"""Validator-side HTTP reporter for signed API requests."""

from __future__ import annotations

import hashlib
import json
import logging
import secrets
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode, urlparse, urlunparse

import httpx

logger = logging.getLogger(__name__)


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def build_auth_message(
    *,
    method: str,
    path: str,
    body_sha256: str,
    timestamp: int,
    nonce: str,
) -> bytes:
    raw = f"{method.upper()}|{path}|{body_sha256}|{timestamp}|{nonce}"
    return raw.encode("utf-8")


@dataclass
class ValidationResultReporter:
    endpoint_url: str
    hotkey_ss58: str
    hotkey_signer: Any
    timeout_sec: float = 60.0

    def _http_timeout(self) -> httpx.Timeout:
        return httpx.Timeout(self.timeout_sec)

    async def _post_async(self, url: str, body: bytes, headers: dict[str, str]) -> int:
        async with httpx.AsyncClient(timeout=self._http_timeout()) as client:
            response = await client.post(url, content=body, headers=headers)
            return int(response.status_code)

    async def _get_async(self, url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        async with httpx.AsyncClient(timeout=self._http_timeout()) as client:
            response = await client.get(url, headers=headers)
            return int(response.status_code), bytes(response.content)

    async def post_training_scores(self, *, payload: dict[str, Any]) -> bool:
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        endpoint = self._join_api_path("/v1/training-scores")
        endpoint_path = "/v1/training-scores"
        headers = self._build_auth_headers(
            method="POST",
            path=endpoint_path,
            body=body,
        )
        headers["Content-Type"] = "application/json"
        try:
            status_code = await self._post_async(endpoint, body, headers)
            if status_code < 200 or status_code >= 300:
                logger.warning(
                    "training-scores POST failed status=%d cycle=%s",
                    status_code,
                    payload.get("cycle_id"),
                )
                return False
            logger.info(
                "training-scores submitted cycle=%s miners=%d",
                payload.get("cycle_id"),
                len(payload.get("scores") or {}),
            )
            return True
        except Exception as exc:
            logger.warning("training-scores POST failed error=%s", exc)
            return False

    async def fetch_invalid_hotkeys(self) -> list[str]:
        """GET /v1/invalid-hotkeys.

        The API returns rich entries `{hotkey, reason, cycle_id}` (see
        InvalidHotkeyEntry), but training logic only needs the hotkey set —
        we flatten to a deduped list[str] here so call sites stay unchanged.
        Legacy bare-string entries (pre-schema migration) are also accepted.
        """
        endpoint = self._join_api_path("/v1/invalid-hotkeys")
        logger.info(f"invalid hotkeys url:{endpoint}")
        headers = {"Accept": "application/json"}
        try:
            status_code, body = await self._get_async(endpoint, headers)
            if status_code < 200 or status_code >= 300:
                logger.warning("invalid-hotkeys fetch failed status=%d", status_code)
                return []
            parsed = json.loads(body.decode("utf-8"))
            values = parsed.get("invalid_hotkeys", [])
            if not isinstance(values, list):
                return []
            deduped: list[str] = []
            for item in values:
                if isinstance(item, dict):
                    hotkey = str(item.get("hotkey", "")).strip()
                else:
                    hotkey = str(item).strip()
                if hotkey and hotkey not in deduped:
                    deduped.append(hotkey)
            return deduped
        except Exception as exc:
            logger.warning("invalid-hotkeys fetch failed error=%s", exc)
            return []

    async def post_invalid_hotkeys(
        self, *, entries: list[dict[str, Any]]
    ) -> bool:
        """POST /v1/invalid-hotkeys.

        `entries` is a list of `{hotkey, reason, cycle_id}` dicts. The API
        upserts on `hotkey`, so re-posting an existing hotkey overwrites its
        reason/cycle_id (and bumps updated_at). Training uses two reasons:
            * "selected"        — miner passed validation and was picked
            * "<failure tags>"  — miner rejected by strict validation
              (semicolon-joined DatasetCheckOutcome.failures)
        """
        endpoint = self._join_api_path("/v1/invalid-hotkeys")
        cleaned: list[dict[str, Any]] = []
        seen: set[str] = set()
        for entry in entries:
            hotkey = str(entry.get("hotkey", "")).strip()
            if not hotkey or hotkey in seen:
                continue
            seen.add(hotkey)
            reason = str(entry.get("reason", "") or "")
            raw_cycle = entry.get("cycle_id")
            try:
                cycle_id = int(raw_cycle) if raw_cycle is not None else None
            except (TypeError, ValueError):
                cycle_id = None
            cleaned.append(
                {"hotkey": hotkey, "reason": reason, "cycle_id": cycle_id}
            )
        cleaned.sort(key=lambda e: e["hotkey"])
        body = json.dumps(
            {"invalid_hotkeys": cleaned},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        headers = self._build_auth_headers(method="POST", path="/v1/invalid-hotkeys", body=body)
        headers["Content-Type"] = "application/json"
        try:
            status_code = await self._post_async(endpoint, body, headers)
            if status_code < 200 or status_code >= 300:
                logger.warning(
                    "invalid-hotkeys POST failed status=%d count=%d",
                    status_code,
                    len(cleaned),
                )
                return False
            return True
        except Exception as exc:
            logger.warning(
                "invalid-hotkeys POST failed error=%s count=%d",
                exc,
                len(cleaned),
            )
            return False

    async def fetch_blacklist_hotkeys(self) -> list[str]:
        endpoint = self._join_api_path("/v1/get_blacklist")
        logger.info(f"blacklist hotkeys url: {endpoint}")
        headers = {"Accept": "application/json"}
        try:
            status_code, body = await self._get_async(endpoint, headers)
            if status_code < 200 or status_code >= 300:
                logger.warning("blacklist hotkeys fetch failed status=%d", status_code)
                return []
            parsed = json.loads(body.decode("utf-8"))
            values = parsed.get("blacklist_hotkeys", [])
            if not isinstance(values, list):
                return []
            deduped: list[str] = []
            for item in values:
                hotkey = str(item).strip()
                if hotkey and hotkey not in deduped:
                    deduped.append(hotkey)
            return deduped
        except Exception as exc:
            logger.warning("blacklist hotkeys fetch failed error=%s", exc)
            return []

    def _join_api_path(self, path: str) -> str:
        parsed = urlparse(self.endpoint_url)
        if parsed.scheme and parsed.netloc:
            return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))
        base = self.endpoint_url.rstrip("/")
        return f"{base}{path}"

    def _build_auth_headers(self, *, method: str, path: str, body: bytes) -> dict[str, str]:
        timestamp = int(time.time())
        nonce = secrets.token_hex(16)
        body_sha256 = _sha256_hex(body)
        message = build_auth_message(
            method=method,
            path=path,
            body_sha256=body_sha256,
            timestamp=timestamp,
            nonce=nonce,
        )
        signature = self.hotkey_signer.sign(data=message).hex()
        return {
            "X-Validator-Hotkey": self.hotkey_ss58,
            "X-Signature": signature,
            "X-Timestamp": str(timestamp),
            "X-Nonce": nonce,
        }


# Quiet `urlencode` import-only warning (used for legacy callers).
_ = urlencode
