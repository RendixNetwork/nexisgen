"""Request authentication for validation evidence API."""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import time
from dataclasses import dataclass

from fastapi import HTTPException, Request, status

from .metagraph_sync import ValidatorAllowlistCache
from .repository import ValidationEvidenceRepository

logger = logging.getLogger(__name__)

HEADER_VALIDATOR_HOTKEY = "x-validator-hotkey"
HEADER_SIGNATURE = "x-signature"
HEADER_TIMESTAMP = "x-timestamp"
HEADER_NONCE = "x-nonce"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def build_auth_message(
    *,
    method: str,
    path: str,
    body_sha256: str,
    timestamp: int,
    nonce: str,
) -> bytes:
    payload = f"{method.upper()}|{path}|{body_sha256}|{timestamp}|{nonce}"
    return payload.encode("utf-8")


def verify_hotkey_signature(
    *,
    hotkey: str,
    signature_hex: str,
    message: bytes,
) -> bool:
    try:
        import bittensor as bt

        clean_signature = signature_hex.removeprefix("0x").removeprefix("0X")
        signature = bytes.fromhex(clean_signature)
        keypair = bt.Keypair(ss58_address=hotkey)
        return bool(keypair.verify(data=message, signature=signature))
    except Exception:
        return False


@dataclass
class AuthContext:
    validator_hotkey: str
    signature: str
    timestamp: int
    nonce: str
    body_sha256: str
    # The exact method/path that were folded into the signed message. Stored
    # alongside the score so an auditor can rebuild the message offline
    # without hardcoding the endpoint.
    method: str = "POST"
    path: str = ""


class RequestAuthenticator:
    """Validate signed headers and replay protection."""

    def __init__(
        self,
        *,
        allowlist_cache: ValidatorAllowlistCache,
        repository: ValidationEvidenceRepository,
        max_time_skew_sec: int,
        nonce_max_age_sec: int,
    ):
        self._allowlist_cache = allowlist_cache
        self._repository = repository
        self._max_time_skew_sec = max(int(max_time_skew_sec), 1)
        self._nonce_max_age_sec = max(int(nonce_max_age_sec), 1)

    async def authenticate(self, request: Request, body: bytes) -> AuthContext:
        hotkey = request.headers.get(HEADER_VALIDATOR_HOTKEY, "").strip()
        signature = request.headers.get(HEADER_SIGNATURE, "").strip()
        timestamp_raw = request.headers.get(HEADER_TIMESTAMP, "").strip()
        nonce = request.headers.get(HEADER_NONCE, "").strip()
        if not hotkey or not signature or not timestamp_raw or not nonce:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="missing required authentication headers",
            )

        try:
            timestamp = int(timestamp_raw)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="invalid timestamp header",
            ) from exc

        now_sec = int(time.time())
        skew = abs(now_sec - timestamp)
        if skew > self._max_time_skew_sec:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="request timestamp outside allowed window",
            )

        if not await self._allowlist_cache.contains(hotkey):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="validator hotkey is not in allowlist",
            )

        body_hash = sha256_hex(body)
        message = build_auth_message(
            method=request.method,
            path=request.url.path,
            body_sha256=body_hash,
            timestamp=timestamp,
            nonce=nonce,
        )
        if not verify_hotkey_signature(
            hotkey=hotkey,
            signature_hex=signature,
            message=message,
        ):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="signature verification failed",
            )

        nonce_inserted = await self._repository.register_nonce_once(
            validator_hotkey=hotkey,
            nonce=nonce,
            signature_timestamp=timestamp,
            max_age_sec=self._nonce_max_age_sec,
        )
        if not nonce_inserted:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="replayed request nonce",
            )
        return AuthContext(
            validator_hotkey=hotkey,
            signature=signature,
            timestamp=timestamp,
            nonce=nonce,
            body_sha256=body_hash,
            method=request.method,
            path=request.url.path,
        )


SCORE_SIGNATURE_SCHEME = "sr25519-hotkey-v1"


def build_score_envelope(
    *,
    validator_hotkey: str,
    scores: dict,
    raw_body: bytes,
    auth: "AuthContext",
) -> dict:
    """Wrap a validator's score submission with everything an auditor needs to
    re-verify it offline.

    `signed_body_b64` is the EXACT bytes the validator signed (the request
    body). It is the cryptographic source of truth; the top-level `scores`
    field is a convenience/aggregator copy and is checked against the signed
    body during verification, so it cannot silently drift.
    """
    return {
        "validator_hotkey": validator_hotkey,
        "cycle_id": None,  # filled by caller if known; not security-relevant
        "scores": scores,
        "auth": {
            "scheme": SCORE_SIGNATURE_SCHEME,
            "signature": auth.signature,
            "timestamp": int(auth.timestamp),
            "nonce": auth.nonce,
            "method": auth.method,
            "path": auth.path,
            "body_sha256": auth.body_sha256,
        },
        "signed_body_b64": base64.b64encode(raw_body).decode("ascii"),
    }


def verify_stored_score(obj: dict) -> tuple[bool, str]:
    """Re-verify a stored validator-score envelope using only the object
    itself + the validator's ss58 hotkey. No server, no DB, no trust in the
    API. Returns (ok, reason).

    Verification chain:
      1. signed_body_b64 decodes to raw bytes
      2. sha256(raw) == auth.body_sha256        (object not tampered)
      3. json(raw)["scores"] == obj["scores"]   (convenience copy matches)
      4. Keypair(ss58=validator_hotkey).verify(
             "{method}|{path}|{body_sha256}|{ts}|{nonce}", signature)
    """
    auth = obj.get("auth")
    if not isinstance(auth, dict):
        return False, "no auth block (legacy/unsigned object)"
    b64 = obj.get("signed_body_b64")
    if not isinstance(b64, str) or not b64:
        return False, "no signed_body_b64"
    try:
        raw = base64.b64decode(b64)
    except Exception as exc:
        return False, f"bad base64: {exc}"
    body_hash = sha256_hex(raw)
    if body_hash != str(auth.get("body_sha256", "")):
        return False, "body hash mismatch (object tampered)"
    try:
        signed = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        return False, f"signed body not JSON: {exc}"
    if obj.get("scores") != signed.get("scores"):
        return False, "scores field drifted from signed body"
    hotkey = str(obj.get("validator_hotkey", "")).strip()
    if not hotkey:
        return False, "no validator_hotkey"
    try:
        timestamp = int(auth.get("timestamp"))
    except (TypeError, ValueError):
        return False, "bad timestamp"
    message = build_auth_message(
        method=str(auth.get("method", "POST")),
        path=str(auth.get("path", "")),
        body_sha256=body_hash,
        timestamp=timestamp,
        nonce=str(auth.get("nonce", "")),
    )
    if not verify_hotkey_signature(
        hotkey=hotkey,
        signature_hex=str(auth.get("signature", "")),
        message=message,
    ):
        return False, "signature does not match validator hotkey"
    return True, "ok"
