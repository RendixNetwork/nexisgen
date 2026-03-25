"""Validator-side HTTP reporter for signed interval decisions."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import secrets
import time
from dataclasses import dataclass
from typing import Any
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import urlencode, urlparse, urlunparse

from ..models import ValidationDecision

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


def decision_to_payload(decision: ValidationDecision) -> dict[str, Any]:
    notes = decision.notes or {}
    return {
        "miner_hotkey": decision.miner_hotkey,
        "accepted": bool(decision.accepted),
        "failures": list(decision.failures),
        "record_count": int(decision.record_count),
        "global_overlap_pruned_count": int(notes.get("global_overlap_pruned_count", 0) or 0),
        "cross_miner_overlap_pruned_count": int(
            notes.get("cross_miner_overlap_pruned_count", 0) or 0
        ),
    }


def build_interval_payload(interval_id: int, decisions: list[ValidationDecision]) -> bytes:
    payload = {
        "interval_id": int(interval_id),
        "decisions": [decision_to_payload(item) for item in decisions],
    }
    # Compact JSON keeps body hash deterministic.
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


@dataclass
class ValidationResultReporter:
    endpoint_url: str
    hotkey_ss58: str
    hotkey_signer: Any
    timeout_sec: float = 10.0

    async def report_interval(
        self,
        *,
        interval_id: int,
        decisions: list[ValidationDecision],
    ) -> bool:
        if not self.endpoint_url or not decisions:
            return False

        body = build_interval_payload(interval_id, decisions)
        endpoint = self._resolve_validation_results_url()
        endpoint_path = self._endpoint_path(endpoint, default="/v1/validation-results")
        headers = self._build_auth_headers(
            method="POST",
            path=endpoint_path,
            body=body,
        )
        headers["Content-Type"] = "application/json"
        try:
            status_code = await asyncio.to_thread(self._post_sync, endpoint, body, headers)
            if status_code < 200 or status_code >= 300:
                logger.warning(
                    "validation evidence POST failed interval=%d status=%d",
                    interval_id,
                    status_code,
                )
                return False
            logger.info(
                "validation evidence submitted interval=%d decisions=%d",
                interval_id,
                len(decisions),
            )
            return True
        except Exception as exc:
            logger.warning("validation evidence report failed interval=%d error=%s", interval_id, exc)
            return False

    async def fetch_invalid_hotkeys(self, *, interval_id: int) -> list[str]:
        endpoint = self._join_api_path("/v1/invalid-hotkeys")
        query = urlencode({"interval_id": int(interval_id)})
        url = f"{endpoint}?{query}"
        headers = {
            "Accept": "application/json",
        }
        try:
            status_code, body = await asyncio.to_thread(self._get_sync, url, headers)
            if status_code < 200 or status_code >= 300:
                logger.warning(
                    "invalid hotkeys fetch failed interval=%d status=%d",
                    interval_id,
                    status_code,
                )
                return []
            parsed = json.loads(body.decode("utf-8"))
            values = parsed.get("invalid_hotkeys", [])
            if not isinstance(values, list):
                return []
            deduped: list[str] = []
            for item in values:
                hotkey = str(item).strip()
                if hotkey and hotkey not in deduped:
                    deduped.append(hotkey)
            return deduped
        except Exception as exc:
            logger.warning("invalid hotkeys fetch failed interval=%d error=%s", interval_id, exc)
            return []

    async def post_invalid_hotkeys(self, *, interval_id: int, invalid_hotkeys: list[str]) -> bool:
        endpoint = self._join_api_path("/v1/invalid-hotkeys")
        body = json.dumps(
            {
                "interval_id": int(interval_id),
                "invalid_hotkeys": sorted({item.strip() for item in invalid_hotkeys if item.strip()}),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        endpoint_path = self._endpoint_path(endpoint, default="/v1/invalid-hotkeys")
        headers = self._build_auth_headers(
            method="POST",
            path=endpoint_path,
            body=body,
        )
        headers["Content-Type"] = "application/json"
        try:
            status_code = await asyncio.to_thread(self._post_sync, endpoint, body, headers)
            if status_code < 200 or status_code >= 300:
                logger.warning(
                    "invalid hotkeys POST failed interval=%d status=%d count=%d",
                    interval_id,
                    status_code,
                    len(invalid_hotkeys),
                )
                return False
            return True
        except Exception as exc:
            logger.warning(
                "invalid hotkeys POST failed interval=%d error=%s count=%d",
                interval_id,
                exc,
                len(invalid_hotkeys),
            )
            return False

    def _resolve_validation_results_url(self) -> str:
        parsed = urlparse(self.endpoint_url)
        if parsed.scheme and parsed.netloc:
            if parsed.path and parsed.path != "/":
                return self.endpoint_url
            return self._join_api_path("/v1/validation-results")
        return self.endpoint_url

    def _join_api_path(self, path: str) -> str:
        parsed = urlparse(self.endpoint_url)
        if parsed.scheme and parsed.netloc:
            return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))
        base = self.endpoint_url.rstrip("/")
        return f"{base}{path}"

    def _endpoint_path(self, endpoint_url: str, *, default: str) -> str:
        try:
            parsed = urlparse(endpoint_url)
            return parsed.path or default
        except Exception:
            return default

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

    def _post_sync(self, url: str, body: bytes, headers: dict[str, str]) -> int:
        req = urllib_request.Request(
            url,
            data=body,
            headers=headers,
            method="POST",
        )
        try:
            with urllib_request.urlopen(req, timeout=float(self.timeout_sec)) as response:
                return int(getattr(response, "status", 200))
        except urllib_error.HTTPError as exc:
            return int(exc.code)

    def _get_sync(self, url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        req = urllib_request.Request(
            url,
            data=None,
            headers=headers,
            method="GET",
        )
        try:
            with urllib_request.urlopen(req, timeout=float(self.timeout_sec)) as response:
                status_code = int(getattr(response, "status", 200))
                return status_code, response.read()
        except urllib_error.HTTPError as exc:
            return int(exc.code), exc.read()
