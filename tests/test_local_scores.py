"""LocalScoreStore: per-cycle save/has/load/latest, atomic + parser-compatible."""

from __future__ import annotations

import json
from pathlib import Path

from nexis.scoring import parse_score_payload
from nexis.validator.local_scores import LocalScoreStore


def _payload(cycle_id: int) -> dict:
    return {
        "cycle_id": cycle_id,
        "scores": {
            "m1": {"aggregate": 0.8, "dimensions": {}, "miner_interval_id": 3},
            "m2": {"aggregate": 0.6, "dimensions": {}, "miner_interval_id": 7},
        },
    }


def test_save_has_load_latest(tmp_path: Path) -> None:
    s = LocalScoreStore(score_dir=tmp_path / "scores")
    assert s.has(4) is False
    assert s.latest() is None

    s.save(4, _payload(4))
    s.save(7, _payload(7))
    s.save(5, _payload(5))

    assert s.has(4) and s.has(7) and s.has(5)
    assert s.has(6) is False
    assert s.list_cycle_ids() == [4, 5, 7]

    cycle_id, payload = s.latest()
    assert cycle_id == 7
    assert payload["cycle_id"] == 7

    # On-disk file is the API-mirrored shape; parse_score_payload works on it.
    loaded = s.load(7)
    assert parse_score_payload(loaded) == {"m1": 0.8, "m2": 0.6}


def test_missing_is_none_not_error(tmp_path: Path) -> None:
    s = LocalScoreStore(score_dir=tmp_path / "nope")
    assert s.has(1) is False
    assert s.load(1) is None
    assert s.list_cycle_ids() == []
    assert s.latest() is None


def test_persists_across_instances(tmp_path: Path) -> None:
    LocalScoreStore(score_dir=tmp_path / "scores").save(9, _payload(9))
    # Fresh instance reads the same dir.
    s2 = LocalScoreStore(score_dir=tmp_path / "scores")
    assert s2.has(9)
    assert s2.latest()[0] == 9
    # No leftover .tmp file from the atomic write.
    assert not list((tmp_path / "scores").glob("*.tmp"))
    raw = json.loads((tmp_path / "scores" / "9.json").read_text())
    assert raw["cycle_id"] == 9
