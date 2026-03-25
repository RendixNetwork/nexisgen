from __future__ import annotations

import hashlib
import json

from nexis.models import ValidationDecision
from nexis.validator.reporting import (
    ValidationResultReporter,
    build_auth_message,
    build_interval_payload,
)
from .helpers import run_async


class _Signer:
    def __init__(self):
        self.messages: list[bytes] = []

    def sign(self, *, data: bytes) -> bytes:
        self.messages.append(data)
        return b"\xAA\xBB"


def test_build_interval_payload_uses_overlap_counts_from_notes() -> None:
    payload = build_interval_payload(
        50,
        [
            ValidationDecision(
                miner_hotkey="miner1",
                interval_id=50,
                accepted=False,
                failures=["x"],
                record_count=10,
                notes={
                    "global_overlap_pruned_count": 3,
                    "cross_miner_overlap_pruned_count": 1,
                },
            )
        ],
    )

    parsed = json.loads(payload.decode("utf-8"))
    assert parsed["interval_id"] == 50
    assert parsed["decisions"][0]["global_overlap_pruned_count"] == 3
    assert parsed["decisions"][0]["cross_miner_overlap_pruned_count"] == 1


def test_report_interval_signs_and_posts(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    signer = _Signer()
    reporter = ValidationResultReporter(
        endpoint_url="http://127.0.0.1:8080/v1/validation-results",
        hotkey_ss58="validator-hotkey",
        hotkey_signer=signer,
        timeout_sec=2.0,
    )

    captured: dict[str, object] = {}

    def fake_post_sync(url: str, body: bytes, headers: dict[str, str]) -> int:
        captured["url"] = url
        captured["body"] = body
        captured["headers"] = dict(headers)
        return 200

    monkeypatch.setattr("nexis.validator.reporting.time.time", lambda: 1710000000)
    monkeypatch.setattr("nexis.validator.reporting.secrets.token_hex", lambda _n: "nonce-fixed")
    monkeypatch.setattr(reporter, "_post_sync", fake_post_sync)

    decision = ValidationDecision(
        miner_hotkey="miner1",
        interval_id=0,
        accepted=True,
        record_count=8,
        notes={
            "global_overlap_pruned_count": 2,
            "cross_miner_overlap_pruned_count": 0,
        },
    )
    ok = run_async(reporter.report_interval(interval_id=0, decisions=[decision]))
    assert ok is True
    assert "headers" in captured
    headers = captured["headers"]
    assert isinstance(headers, dict)
    assert headers["X-Validator-Hotkey"] == "validator-hotkey"
    assert headers["X-Nonce"] == "nonce-fixed"
    assert headers["X-Signature"] == "aabb"
    assert len(signer.messages) == 1
    body = captured["body"]
    assert isinstance(body, bytes)
    expected_message = build_auth_message(
        method="POST",
        path="/v1/validation-results",
        body_sha256=hashlib.sha256(body).hexdigest(),
        timestamp=1710000000,
        nonce="nonce-fixed",
    )
    assert signer.messages[0] == expected_message
    assert captured["url"] == "http://127.0.0.1:8080/v1/validation-results"


def test_fetch_invalid_hotkeys(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    reporter = ValidationResultReporter(
        endpoint_url="http://127.0.0.1:8080/v1/validation-results",
        hotkey_ss58="validator-hotkey",
        hotkey_signer=_Signer(),
        timeout_sec=2.0,
    )

    def fake_get_sync(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        _ = headers
        assert url.endswith("/v1/invalid-hotkeys?interval_id=500")
        return 200, b'{"invalid_hotkeys":["hk1","hk2","hk1"]}'

    monkeypatch.setattr(reporter, "_get_sync", fake_get_sync)
    hotkeys = run_async(reporter.fetch_invalid_hotkeys(interval_id=500))
    assert hotkeys == ["hk1", "hk2"]


def test_post_invalid_hotkeys_signs_request(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    signer = _Signer()
    reporter = ValidationResultReporter(
        endpoint_url="http://127.0.0.1:8080/v1/validation-results",
        hotkey_ss58="validator-hotkey",
        hotkey_signer=signer,
        timeout_sec=2.0,
    )
    captured: dict[str, object] = {}

    def fake_post_sync(url: str, body: bytes, headers: dict[str, str]) -> int:
        captured["url"] = url
        captured["body"] = body
        captured["headers"] = headers
        return 200

    monkeypatch.setattr("nexis.validator.reporting.time.time", lambda: 1710000000)
    monkeypatch.setattr("nexis.validator.reporting.secrets.token_hex", lambda _n: "nonce-fixed")
    monkeypatch.setattr(reporter, "_post_sync", fake_post_sync)

    ok = run_async(
        reporter.post_invalid_hotkeys(interval_id=77, invalid_hotkeys=["m2", "m1", "m1"])
    )
    assert ok is True
    assert captured["url"] == "http://127.0.0.1:8080/v1/invalid-hotkeys"
    assert captured["body"] == b'{"interval_id":77,"invalid_hotkeys":["m1","m2"]}'
    assert len(signer.messages) == 1
    expected_message = build_auth_message(
        method="POST",
        path="/v1/invalid-hotkeys",
        body_sha256=hashlib.sha256(captured["body"]).hexdigest(),  # type: ignore[arg-type]
        timestamp=1710000000,
        nonce="nonce-fixed",
    )
    assert signer.messages[0] == expected_message

