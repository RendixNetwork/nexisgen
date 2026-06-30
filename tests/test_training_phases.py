"""Verify training cycle trains all miners before any uploads."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from nexis.validator.training import (
    TrainingCandidate,
    determine_next_cycle_id,
    parse_last_winners,
    select_eligible_hotkeys,
)


def test_determine_next_cycle_id_gates_on_validator_score() -> None:
    """Cycle advances only when THIS validator's own score file exists for the
    latest cycle."""
    HK = "5ValidatorHotkeyAddress"

    class _Bucket:
        def __init__(self, latest, scored_for):
            self._latest = latest
            self._scored_for = scored_for  # set of (cycle, hotkey)

        async def latest_cycle_id(self):
            return self._latest

        async def has_validator_score(self, cycle_id, validator_hotkey):
            return (cycle_id, validator_hotkey) in self._scored_for

    # Empty bucket -> bootstrap to cycle 1.
    assert asyncio.run(determine_next_cycle_id(_Bucket(None, set()), HK)) == 1
    # Latest cycle 5 not yet scored by this validator -> wait (None).
    assert asyncio.run(determine_next_cycle_id(_Bucket(5, set()), HK)) is None
    # Latest cycle 5 scored by this validator -> advance to 6.
    assert asyncio.run(determine_next_cycle_id(_Bucket(5, {(5, HK)}), HK)) == 6
    # Scored by a DIFFERENT validator only -> still wait for ours.
    assert (
        asyncio.run(determine_next_cycle_id(_Bucket(5, {(5, "5Other")}), HK))
        is None
    )


def test_parse_last_winners_top5() -> None:
    payload = {
        "scores": {
            f"hk{i}": {"aggregate": float(10 - i)} for i in range(8)
        },
    }
    winners = parse_last_winners(payload, top_k=5)
    assert winners == {"hk0", "hk1", "hk2", "hk3", "hk4"}


def test_parse_last_winners_handles_flat_floats() -> None:
    payload = {"scores": {"a": 0.9, "b": 0.5, "c": 0.7}}
    winners = parse_last_winners(payload, top_k=2)
    assert winners == {"a", "c"}


def test_parse_last_winners_empty() -> None:
    assert parse_last_winners(None) == set()
    assert parse_last_winners({}) == set()
    assert parse_last_winners({"scores": "bad"}) == set()


@pytest.mark.parametrize(
    "candidates,invalid,blacklist,winners,expected",
    [
        # Non-invalid candidates pass.
        (["a", "b"], set(), set(), set(), ["a", "b"]),
        # Invalid candidates are dropped.
        (["a", "b", "c"], {"b"}, set(), set(), ["a", "c"]),
        # Last-winners override the invalid mark.
        (["a", "b"], {"a", "b"}, set(), {"a"}, ["a"]),
        # Blacklist excludes even when it would otherwise be eligible.
        (["a", "b", "c"], set(), {"b"}, set(), ["a", "c"]),
        # Blacklist beats last-winner exemption (a is winner+blacklisted -> out;
        # b is invalid and not a winner -> out).
        (["a", "b"], {"a", "b"}, {"a"}, {"a"}, []),
    ],
)
def test_eligibility_rules(
    candidates: list[str],
    invalid: set[str],
    blacklist: set[str],
    winners: set[str],
    expected: list[str],
) -> None:
    eligible = asyncio.run(
        select_eligible_hotkeys(
            candidate_hotkeys=candidates,
            invalid_hotkeys=invalid,
            blacklist_hotkeys=blacklist,
            last_winners=winners,
        )
    )
    assert eligible == expected


def test_run_training_cycle_uploads_after_all_training(tmp_path: Path, monkeypatch) -> None:
    """All `run_train_container` calls finish before any `upload_miner_outputs` call."""
    from nexis.validator import training as training_mod

    timeline: list[tuple[str, str]] = []  # (event, miner_hotkey)

    async def fake_gather_candidates(**kwargs: Any) -> tuple[list[TrainingCandidate], list[Any]]:
        cycle_id = kwargs["cycle_id"]
        cycle_dir = kwargs["workdir"] / "cycle" / str(cycle_id)
        candidates: list[TrainingCandidate] = []
        for hk in kwargs["eligible_hotkeys"]:
            d = cycle_dir / hk / "1"
            d.mkdir(parents=True, exist_ok=True)
            (d / "dataset.parquet").write_bytes(b"")
            candidates.append(TrainingCandidate(miner_hotkey=hk, interval_id=1, miner_dir=d))
        return candidates, []

    async def fake_run_train(**kwargs: Any) -> Path | None:
        candidate: TrainingCandidate = kwargs["candidate"]
        timeline.append(("train_start", candidate.miner_hotkey))
        await asyncio.sleep(0)
        outputs = kwargs["workdir"] / "outputs" / candidate.miner_hotkey
        outputs.mkdir(parents=True, exist_ok=True)
        (outputs / "video.mp4").write_bytes(b"x")
        timeline.append(("train_end", candidate.miner_hotkey))
        return outputs

    async def fake_upload(**kwargs: Any) -> bool:
        trained = kwargs["trained"]
        timeline.append(("upload", trained.miner_hotkey))
        return True

    async def noop_cleanup(_path: Path) -> None:
        return None

    monkeypatch.setattr(training_mod, "gather_candidates", fake_gather_candidates)
    monkeypatch.setattr(training_mod, "run_train_container", fake_run_train)
    monkeypatch.setattr(training_mod, "upload_miner_outputs", fake_upload)
    monkeypatch.setattr(training_mod, "cleanup_workdir", noop_cleanup)

    fake_pool = SimpleNamespace(num_gpus=8, run=lambda **kw: asyncio.sleep(0))
    fake_bucket = SimpleNamespace(upload_path=lambda *a, **kw: asyncio.sleep(0))
    fake_settings = SimpleNamespace()

    result = asyncio.run(
        training_mod.run_training_cycle(
            settings=fake_settings,
            candidate_hotkeys=["a", "b", "c"],
            invalid_hotkeys=set(),
            blacklist_hotkeys=set(),
            last_score=None,
            store_for_hotkey=lambda hk: SimpleNamespace(),
            nexis_miner=fake_bucket,
            pool=fake_pool,
            cycle_id=1,
            workdir=tmp_path,
            global_record_index={},
            eval_data_dir=tmp_path / "eval_data",
        )
    )

    assert sorted(result.trained) == ["a", "b", "c"]
    assert sorted(result.uploaded) == ["a", "b", "c"]

    # Last training event must come BEFORE the first upload event.
    last_train_idx = max(i for i, e in enumerate(timeline) if e[0] == "train_end")
    first_upload_idx = min(i for i, e in enumerate(timeline) if e[0] == "upload")
    assert last_train_idx < first_upload_idx, f"timeline: {timeline}"
