"""Verify total_score aggregation includes per-dimension means + miner_interval_id."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

from nexis.api.app import TotalScoreCoordinator


class _FakeBucket:
    """Minimal stand-in for NexisMinerBucket used by the coordinator."""

    def __init__(self, root: Path):
        self.root = root
        root.mkdir(parents=True, exist_ok=True)
        self.uploaded_total: dict[int, dict] = {}

    async def upload_validator_score(self, *, cycle_id, validator_hotkey, payload, workdir):
        path = self.root / str(cycle_id) / f"{validator_hotkey}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    async def list_validator_score_keys(self, cycle_id):
        cycle_dir = self.root / str(cycle_id)
        if not cycle_dir.exists():
            return []
        return [
            f"{cycle_id}/{p.name}"
            for p in cycle_dir.iterdir()
            if p.name.endswith(".json") and p.name != "total_score.json"
        ]

    async def download_keys(self, keys, workdir):
        out = {}
        for key in keys:
            parts = key.split("/", 1)
            local = self.root / parts[0] / parts[1]
            if local.exists():
                out[key] = local
        return out

    async def upload_total_score(self, cycle_id, payload, workdir):
        self.uploaded_total[cycle_id] = payload


def test_total_score_includes_dimensions_and_interval_id(tmp_path: Path) -> None:
    bucket = _FakeBucket(tmp_path / "bucket")
    coord = TotalScoreCoordinator(bucket=bucket, workdir=tmp_path / "work")  # type: ignore[arg-type]

    # Two validators score the same miner.
    payload_a = {
        "cycle_id": 7,
        "scores": {
            "miner1": {
                "aggregate": 0.8,
                "dimensions": {"aesthetic_quality": 0.6, "imaging_quality": 1.0},
                "miner_interval_id": 4,
            }
        },
    }
    payload_b = {
        "cycle_id": 7,
        "scores": {
            "miner1": {
                "aggregate": 0.6,
                "dimensions": {"aesthetic_quality": 0.7, "imaging_quality": 0.5},
                "miner_interval_id": 4,
            }
        },
    }

    asyncio.run(
        coord.update_for_cycle(cycle_id=7, validator_hotkey="vA", payload=payload_a)
    )
    miner_count, total = asyncio.run(
        coord.update_for_cycle(cycle_id=7, validator_hotkey="vB", payload=payload_b)
    )

    assert miner_count == 1
    entry = total["scores"]["miner1"]
    assert abs(entry["aggregate"] - (0.8 + 0.6) / 2) < 1e-9
    assert entry["validator_count"] == 2
    assert entry["miner_interval_id"] == 4
    assert abs(entry["dimensions"]["aesthetic_quality"] - (0.6 + 0.7) / 2) < 1e-9
    assert abs(entry["dimensions"]["imaging_quality"] - (1.0 + 0.5) / 2) < 1e-9


def test_total_score_preserves_interval_when_one_validator_omits(tmp_path: Path) -> None:
    bucket = _FakeBucket(tmp_path / "bucket")
    coord = TotalScoreCoordinator(bucket=bucket, workdir=tmp_path / "work")  # type: ignore[arg-type]

    payload_a = {
        "cycle_id": 7,
        "scores": {
            "miner1": {"aggregate": 0.5, "miner_interval_id": 9},
        },
    }
    payload_b = {
        "cycle_id": 7,
        "scores": {"miner1": {"aggregate": 0.7}},  # no interval_id
    }
    asyncio.run(coord.update_for_cycle(cycle_id=7, validator_hotkey="vA", payload=payload_a))
    _, total = asyncio.run(
        coord.update_for_cycle(cycle_id=7, validator_hotkey="vB", payload=payload_b)
    )

    assert total["scores"]["miner1"]["miner_interval_id"] == 9


_ = SimpleNamespace  # keep ruff happy if it ever inspects imports
