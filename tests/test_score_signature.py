"""End-to-end proof that a stored validator score is independently verifiable.

Signs a score body with a real sr25519 keypair exactly like the validator's
reporter does, wraps it via the API's `build_score_envelope`, then re-verifies
offline via `verify_stored_score` — the same path `verify_validator_score.py`
uses. Also proves every tamper vector fails closed.
"""

from __future__ import annotations

import hashlib
import json

import pytest

from nexis.api.auth import (
    AuthContext,
    build_auth_message,
    build_score_envelope,
    verify_stored_score,
)

bt = pytest.importorskip("bittensor")


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sign_like_validator(keypair, cycle_id: int, scores: dict):
    """Mirror reporting.ValidationResultReporter: compact JSON body, sign
    POST|path|sha256(body)|ts|nonce."""
    body = json.dumps(
        {"cycle_id": cycle_id, "scores": scores},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    timestamp = 1_700_000_000
    nonce = "deadbeefdeadbeefdeadbeefdeadbeef"
    path = "/v1/training-scores"
    body_sha256 = _sha256_hex(body)
    message = build_auth_message(
        method="POST",
        path=path,
        body_sha256=body_sha256,
        timestamp=timestamp,
        nonce=nonce,
    )
    signature = keypair.sign(data=message).hex()
    auth = AuthContext(
        validator_hotkey=keypair.ss58_address,
        signature=signature,
        timestamp=timestamp,
        nonce=nonce,
        body_sha256=body_sha256,
        method="POST",
        path=path,
    )
    return body, auth


def _make_envelope():
    keypair = bt.Keypair.create_from_uri("//Alice")
    scores = {
        "5MinerA": {"aggregate": 0.8128, "miner_interval_id": 2},
        "5MinerB": {"aggregate": 0.7422, "miner_interval_id": 7},
    }
    body, auth = _sign_like_validator(keypair, 56, scores)
    envelope = build_score_envelope(
        validator_hotkey=keypair.ss58_address,
        scores=scores,
        raw_body=body,
        auth=auth,
    )
    envelope["cycle_id"] = 56
    return keypair, envelope


def test_stored_score_verifies_offline() -> None:
    _, envelope = _make_envelope()
    ok, reason = verify_stored_score(envelope)
    assert ok, reason


def test_tampered_scores_field_fails() -> None:
    _, envelope = _make_envelope()
    # Forge a higher aggregate in the convenience copy but leave the signed
    # body intact -> caught by the scores-drift check.
    envelope["scores"]["5MinerB"]["aggregate"] = 0.99
    ok, reason = verify_stored_score(envelope)
    assert not ok
    assert "drift" in reason


def test_tampered_signed_body_fails() -> None:
    import base64

    _, envelope = _make_envelope()
    forged = json.dumps(
        {"cycle_id": 56, "scores": {"5MinerB": {"aggregate": 0.99}}},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    # Rewrite the signed bytes to match the forged scores; sha256 no longer
    # equals auth.body_sha256 -> caught.
    envelope["signed_body_b64"] = base64.b64encode(forged).decode("ascii")
    envelope["scores"] = {"5MinerB": {"aggregate": 0.99}}
    ok, reason = verify_stored_score(envelope)
    assert not ok
    assert "hash mismatch" in reason


def test_wrong_hotkey_fails() -> None:
    _, envelope = _make_envelope()
    # Claim a different validator signed it -> signature check fails.
    envelope["validator_hotkey"] = bt.Keypair.create_from_uri("//Bob").ss58_address
    ok, reason = verify_stored_score(envelope)
    assert not ok
    assert "signature" in reason


def test_legacy_unsigned_object_reports_clearly() -> None:
    ok, reason = verify_stored_score({"cycle_id": 56, "scores": {}})
    assert not ok
    assert "legacy" in reason or "auth" in reason
