from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace

from nexis.cli import (
    _fetch_hotkeys_with_commitments,
    _load_record_info_snapshot,
    _resolve_llm_runtime,
    _run_miner_loop,
    _run_validator_loop,
)
from nexis.models import ValidationDecision
from nexis.config import Settings
from .helpers import LocalObjectStore, run_async


def test_resolve_llm_runtime_prefers_openai_when_present() -> None:
    settings = Settings()
    settings.openai_api_key = "openai-key"
    settings.gemini_api_key = "gemini-key"

    provider, api_key, model, base_url, route = _resolve_llm_runtime(
        settings,
        openai_model="gpt-4o",
    )

    assert provider == "openai"
    assert api_key == "openai-key"
    assert model == "gpt-4o"
    assert base_url is None
    assert route == "openai_key"


def test_resolve_llm_runtime_uses_gemini_when_openai_missing() -> None:
    settings = Settings()
    settings.openai_api_key = ""
    settings.gemini_api_key = "gemini-key"

    provider, api_key, model, base_url, route = _resolve_llm_runtime(
        settings,
        openai_model="gpt-4o",
    )

    assert provider == "gemini"
    assert api_key == "gemini-key"
    assert model == "gemini-3.1-flash-lite-preview"
    assert base_url == "https://generativelanguage.googleapis.com/v1beta/openai/"
    assert route == "gemini_key"


def test_resolve_llm_runtime_upgrades_legacy_openai_model() -> None:
    settings = Settings()
    settings.openai_api_key = "openai-key"
    settings.gemini_api_key = ""

    provider, api_key, model, base_url, route = _resolve_llm_runtime(
        settings,
        openai_model="gpt-4o-mini",
    )

    assert provider == "openai"
    assert api_key == "openai-key"
    assert model == "gpt-4o"
    assert base_url is None
    assert route == "openai_key"


def test_resolve_llm_runtime_without_any_key() -> None:
    settings = Settings()
    settings.openai_api_key = ""
    settings.gemini_api_key = ""

    provider, api_key, model, base_url, route = _resolve_llm_runtime(
        settings,
        openai_model="gpt-4o",
    )

    assert provider == "openai"
    assert api_key == ""
    assert model == "gpt-4o"
    assert base_url is None
    assert route == "no_api_key"


def test_fetch_hotkeys_with_commitments_filters_excluded_and_missing(
    monkeypatch,
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    async def fake_fetch_hotkeys_from_metagraph_async(*, netuid: int, network: str, subtensor: object) -> list[str]:
        assert netuid == 9
        assert network == "test"
        assert subtensor is fake_subtensor
        return ["hk1", "hk2", "hk3"]

    class FakeManager:
        async def get_all_credentials_async(self, subtensor: object) -> dict[str, dict]:
            assert subtensor is fake_subtensor
            return {
                "hk1": {"account_id": "a" * 32},
                "hk3": {"account_id": "b" * 32},
            }

    settings = Settings()
    settings.netuid = 9
    settings.bt_network = "test"
    settings.validator_blacklist_file = tmp_path / "blacklist.txt"
    settings.validator_blacklist_file.write_text("hk3\n", encoding="utf-8")
    fake_subtensor = object()

    monkeypatch.setattr(
        "nexis.cli.fetch_hotkeys_from_metagraph_async",
        fake_fetch_hotkeys_from_metagraph_async,
    )

    hotkeys, payload = run_async(
        _fetch_hotkeys_with_commitments(
            settings=settings,
            manager=FakeManager(),  # type: ignore[arg-type]
            blacklist_file=None,
            exclude_hotkeys="hk2",
            subtensor=fake_subtensor,
        )
    )

    assert hotkeys == ["hk1"]
    assert sorted(payload.keys()) == ["hk1", "hk3"]


def test_load_record_info_snapshot_trust_flags(tmp_path) -> None:  # type: ignore[no-untyped-def]
    async def run() -> tuple[tuple[dict[str, list[float]], bool], tuple[dict[str, list[float]], bool]]:
        store = LocalObjectStore(tmp_path / "record-info")
        missing = await _load_record_info_snapshot(
            record_info_store=store,  # type: ignore[arg-type]
            object_key="snapshot.json",
            workdir=tmp_path / "work-missing",
        )

        invalid_local = tmp_path / "invalid.json"
        invalid_local.write_text("{not valid json", encoding="utf-8")
        await store.upload_file("snapshot.json", invalid_local)

        invalid = await _load_record_info_snapshot(
            record_info_store=store,  # type: ignore[arg-type]
            object_key="snapshot.json",
            workdir=tmp_path / "work-invalid",
        )
        return missing, invalid

    missing, invalid = run_async(run())
    assert missing == ({}, True)
    assert invalid == ({}, False)


def test_run_validator_loop_submits_epoch_weights_with_shared_subtensor(
    monkeypatch,
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    fake_subtensor = object()
    submit_calls: list[dict[str, object]] = []

    @asynccontextmanager
    async def fake_open_subtensor(network: str):  # type: ignore[no-untyped-def]
        assert network == "test"
        yield fake_subtensor

    async def fake_fetch_current_block_async(*, network: str, subtensor: object | None = None) -> int:
        assert network == "test"
        assert subtensor is fake_subtensor
        return 250

    async def fake_fetch_hotkeys_with_commitments(**kwargs) -> tuple[list[str], dict[str, dict]]:  # type: ignore[no-untyped-def]
        assert kwargs["subtensor"] is fake_subtensor
        return [], {}

    async def fake_submit_weights_to_chain_async(**kwargs) -> object:  # type: ignore[no-untyped-def]
        submit_calls.append(kwargs)
        return SimpleNamespace(submitted=True, reason="", unknown_hotkeys=[])

    async def fake_sleep_poll(seconds: float) -> None:
        _ = seconds
        raise KeyboardInterrupt

    monkeypatch.setattr("nexis.cli._open_subtensor", fake_open_subtensor)
    monkeypatch.setattr("nexis.cli.fetch_current_block_async", fake_fetch_current_block_async)
    monkeypatch.setattr("nexis.cli._fetch_hotkeys_with_commitments", fake_fetch_hotkeys_with_commitments)
    monkeypatch.setattr("nexis.cli.submit_weights_to_chain_async", fake_submit_weights_to_chain_async)
    monkeypatch.setattr("nexis.cli._sleep_poll", fake_sleep_poll)

    class FakeWeightComputer:
        def score_from_sample_count(self, sample_count: int) -> float:
            return float(sample_count)

        def compute_weights_from_totals(self, score_totals: dict[str, float]) -> dict[str, float]:
            return dict(score_totals)

    class FakeValidator:
        weight_computer = FakeWeightComputer()

        async def validate_interval(self, **kwargs) -> tuple[list[object], dict[str, float]]:  # type: ignore[no-untyped-def]
            _ = kwargs
            raise AssertionError("validate_interval should not be called when no hotkeys")

    settings = Settings()
    settings.netuid = 9
    settings.bt_network = "test"
    settings.bt_wallet_name = "wallet"
    settings.bt_wallet_hotkey = "hotkey"
    settings.bt_wallet_path = tmp_path / "wallets"

    try:
        run_async(
            _run_validator_loop(
                settings=settings,
                poll_seconds=0.01,
                enabled_specs=["video_v1"],
                manager=object(),  # type: ignore[arg-type]
                store_cache={},
                committed_payload={},
                validator=FakeValidator(),  # type: ignore[arg-type]
                is_owner_validator=False,
                record_info_read_store=None,
                record_info_write_store=None,
                store_for_hotkey=lambda _hotkey: LocalObjectStore(tmp_path / "unused"),  # type: ignore[return-value]
                blacklist_file=None,
                exclude_hotkeys="",
            )
        )
    except KeyboardInterrupt:
        pass

    assert len(submit_calls) == 1
    assert submit_calls[0]["subtensor"] is fake_subtensor
    assert submit_calls[0]["weights_by_hotkey"] == {}
    assert submit_calls[0]["netuid"] == 9


def test_run_miner_loop_uses_shared_subtensor_and_executes_interval(
    monkeypatch,
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    fake_subtensor = object()
    fetch_calls: list[object] = []
    pipeline_calls: list[int] = []

    @asynccontextmanager
    async def fake_open_subtensor(network: str):  # type: ignore[no-untyped-def]
        assert network == "test"
        yield fake_subtensor

    async def fake_fetch_current_block_async(*, network: str, subtensor: object | None = None) -> int:
        assert network == "test"
        fetch_calls.append(subtensor)
        return 49

    async def fake_sleep_poll(seconds: float) -> None:
        _ = seconds
        raise KeyboardInterrupt

    class FakeStore:
        async def object_exists(self, key: str) -> bool:
            assert key == "0/manifest.json"
            return False

    class FakePipeline:
        async def run_interval(self, **kwargs) -> tuple[object, object]:  # type: ignore[no-untyped-def]
            pipeline_calls.append(int(kwargs["interval_id"]))
            return tmp_path / "dataset.parquet", tmp_path / "manifest.json"

    monkeypatch.setattr("nexis.cli._open_subtensor", fake_open_subtensor)
    monkeypatch.setattr("nexis.cli.fetch_current_block_async", fake_fetch_current_block_async)
    monkeypatch.setattr("nexis.cli._sleep_poll", fake_sleep_poll)

    settings = Settings()
    settings.bt_network = "test"
    settings.netuid = 9
    settings.sources_file = tmp_path / "sources.txt"
    settings.workdir = tmp_path / "workdir"

    try:
        run_async(
            _run_miner_loop(
                settings=settings,
                store=FakeStore(),  # type: ignore[arg-type]
                pipeline=FakePipeline(),  # type: ignore[arg-type]
                hotkey_ss58="hk-test",
                poll_seconds=0.01,
                active_spec="video_v1",
            )
        )
    except KeyboardInterrupt:
        pass

    assert fetch_calls == [fake_subtensor]
    assert pipeline_calls == [0]


def test_run_validator_loop_reports_interval_results(monkeypatch, tmp_path) -> None:  # type: ignore[no-untyped-def]
    fake_subtensor = object()
    reported: list[tuple[int, int]] = []

    @asynccontextmanager
    async def fake_open_subtensor(network: str):  # type: ignore[no-untyped-def]
        assert network == "test"
        yield fake_subtensor

    fetch_blocks = iter([52, 52])

    async def fake_fetch_current_block_async(*, network: str, subtensor: object | None = None) -> int:
        assert network == "test"
        assert subtensor is fake_subtensor
        return next(fetch_blocks)

    async def fake_fetch_hotkeys_with_commitments(**kwargs) -> tuple[list[str], dict[str, dict]]:  # type: ignore[no-untyped-def]
        assert kwargs["subtensor"] is fake_subtensor
        return ["miner-1"], {"miner-1": {"account_id": "a" * 32}}

    async def fake_sleep_poll(seconds: float) -> None:
        _ = seconds
        raise KeyboardInterrupt

    monkeypatch.setattr("nexis.cli._open_subtensor", fake_open_subtensor)
    monkeypatch.setattr("nexis.cli.fetch_current_block_async", fake_fetch_current_block_async)
    monkeypatch.setattr("nexis.cli._fetch_hotkeys_with_commitments", fake_fetch_hotkeys_with_commitments)
    monkeypatch.setattr("nexis.cli._sleep_poll", fake_sleep_poll)

    class FakeWeightComputer:
        def score_from_sample_count(self, sample_count: int) -> float:
            return float(sample_count)

        def compute_weights_from_totals(self, score_totals: dict[str, float]) -> dict[str, float]:
            return dict(score_totals)

    class FakeValidator:
        weight_computer = FakeWeightComputer()

        async def validate_interval(self, **kwargs) -> tuple[list[ValidationDecision], dict[str, float]]:  # type: ignore[no-untyped-def]
            _ = kwargs
            return (
                [
                    ValidationDecision(
                        miner_hotkey="miner-1",
                        interval_id=0,
                        accepted=True,
                        record_count=10,
                        notes={},
                    )
                ],
                {"miner-1": 1.0},
            )

    class FakeReporter:
        async def fetch_invalid_hotkeys(self, *, interval_id: int) -> list[str]:
            _ = interval_id
            return []

        async def report_interval(self, *, interval_id: int, decisions: list[ValidationDecision]) -> bool:
            reported.append((interval_id, len(decisions)))
            return True

    settings = Settings()
    settings.netuid = 9
    settings.bt_network = "test"
    settings.bt_wallet_name = "wallet"
    settings.bt_wallet_hotkey = "hotkey"
    settings.bt_wallet_path = tmp_path / "wallets"

    try:
        run_async(
            _run_validator_loop(
                settings=settings,
                poll_seconds=0.01,
                enabled_specs=["video_v1"],
                manager=object(),  # type: ignore[arg-type]
                store_cache={},
                committed_payload={},
                validator=FakeValidator(),  # type: ignore[arg-type]
                is_owner_validator=False,
                record_info_read_store=None,
                record_info_write_store=None,
                store_for_hotkey=lambda _hotkey: LocalObjectStore(tmp_path / "unused"),  # type: ignore[return-value]
                blacklist_file=None,
                exclude_hotkeys="",
                reporter=FakeReporter(),  # type: ignore[arg-type]
            )
        )
    except KeyboardInterrupt:
        pass

    assert reported == [(0, 1)]
