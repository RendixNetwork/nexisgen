"""Validate top-K weight computation."""

from __future__ import annotations

import pytest

from nexis.scoring import compute_top_k_weights, parse_score_payload


def test_top5_weights_normalize() -> None:
    scores = {f"miner_{i}": 1.0 - i * 0.1 for i in range(7)}
    weights = compute_top_k_weights(scores, top_k=5)
    assert len(weights) == 5
    assert pytest.approx(sum(weights.values()), abs=1e-9) == 1.0
    ordered = sorted(weights.items(), key=lambda kv: -kv[1])
    expected = [1, 0.5, 0.25, 0.125, 0.0625]
    total = sum(expected)
    for (_, weight), expect in zip(ordered, expected):
        assert pytest.approx(weight, abs=1e-9) == expect / total


def test_top5_weights_handles_fewer_than_k() -> None:
    scores = {"a": 1.0, "b": 0.5}
    weights = compute_top_k_weights(scores, top_k=5)
    assert len(weights) == 2
    assert pytest.approx(sum(weights.values()), abs=1e-9) == 1.0


def test_top5_weights_skip_zero_scores() -> None:
    scores = {"a": 0.0, "b": 0.0}
    weights = compute_top_k_weights(scores, top_k=5)
    assert weights == {}


def test_parse_score_payload() -> None:
    payload = {
        "cycle_id": 3,
        "scores": {
            "alice": {"aggregate": 0.92, "validator_count": 2},
            "bob": {"aggregate": 0.88},
            "carol": 0.85,
        },
    }
    parsed = parse_score_payload(payload)
    assert parsed == {"alice": 0.92, "bob": 0.88, "carol": 0.85}


def test_parse_score_handles_missing() -> None:
    assert parse_score_payload(None) == {}
    assert parse_score_payload({}) == {}
    assert parse_score_payload({"scores": "not_a_dict"}) == {}
