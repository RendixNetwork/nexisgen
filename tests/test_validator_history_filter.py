from __future__ import annotations

import pytest

from nexis.validator.pipeline import ValidatorPipeline
from .helpers import run_async


def test_validator_skips_selected_miner_with_api_invalid_hotkeys() -> None:
    async def run() -> None:
        def _store_for_hotkey(_hotkey: str) -> object:
            raise AssertionError("store lookup should not happen for history-rejected miner")

        pipeline = ValidatorPipeline(store_for_hotkey=_store_for_hotkey)
        decisions, weights = await pipeline.validate_interval(
            candidate_hotkeys=["hk1"],
            interval_id=100,
            invalid_hotkeys={"hk1"},
        )
        assert decisions == []
        assert weights == {}

    run_async(run())


def test_sampling_runs_on_api_eligible_hotkeys_only(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        observed_sampling_input: list[str] = []

        def fake_select_miners(hotkeys: list[str], _seed: str) -> list[str]:
            observed_sampling_input.extend(hotkeys)
            return list(hotkeys)

        async def fake_load_submission(*, hotkey: str, interval_id: int, workdir):  # type: ignore[no-untyped-def]
            from nexis.models import ValidationDecision

            _ = interval_id, workdir
            return None, ValidationDecision(
                miner_hotkey=hotkey,
                interval_id=101,
                accepted=False,
                failures=["test_skip"],
                sampled_rows=0,
            )

        pipeline = ValidatorPipeline(store_for_hotkey=lambda _hotkey: object())
        monkeypatch.setattr("nexis.validator.pipeline.select_miners", fake_select_miners)
        monkeypatch.setattr(pipeline, "_load_submission", fake_load_submission)

        decisions, _weights = await pipeline.validate_interval(
            candidate_hotkeys=["bad_hk", "good_hk_1", "good_hk_2"],
            interval_id=101,
            invalid_hotkeys={"bad_hk"},
        )
        assert observed_sampling_input == ["good_hk_1", "good_hk_2"]
        assert len(decisions) == 2
        assert {item.miner_hotkey for item in decisions} == {"good_hk_1", "good_hk_2"}

    run_async(run())
