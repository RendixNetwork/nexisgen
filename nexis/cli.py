"""Nexis CLI: mine, train, validate, commit-credentials."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any, TYPE_CHECKING

import typer
from rich.console import Console
from rich.logging import RichHandler

from .chain.metagraph import (
    _open_subtensor,
    fetch_current_block_async,
    fetch_hotkeys_from_metagraph_async,
)
from .chain.weights import build_chain_weight_payload, submit_weights_to_chain_async
from .config import Settings, load_settings
from .miner.captioner import Captioner
from .miner.pipeline import MinerPipeline
from .protocol import WEIGHT_SUBMISSION_INTERVAL_BLOCKS, WEIGHT_TOP_K
from .scoring import compute_top_k_weights, parse_total_score_payload
from .storage.eval_data import build_eval_data_store, sync_eval_data
from .storage.r2 import R2Credentials, R2S3Store, bucket_name_for_hotkey
from .storage.shared_bucket import (
    NexisMinerBucket,
    build_nexis_miner_credentials,
)
from .validator.dataset_check import latest_complete_interval_id, list_miner_interval_ids
from .validator.docker_runner import DockerGPUPool
from .validator.reporting import ValidationResultReporter
from .validator.training import (
    determine_next_cycle_id,
    run_training_cycle,
)
from .validator.vbench_scorer import (
    cleanup_score_workdir,
    score_cycle,
    submit_scores,
)

if TYPE_CHECKING:
    from .chain.credentials import ReadCredentialCommitmentManager

app = typer.Typer(name="nexis", no_args_is_help=True)
console = Console()
logger = logging.getLogger(__name__)

_WEIGHT_RETRY_BACKOFF_BASE_SEC = 10
_WEIGHT_RETRY_BACKOFF_MAX_SEC = 300


def _configure_logging(level: str, *, debug: bool = False) -> None:
    configured_level = logging.DEBUG if debug else getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=configured_level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[RichHandler(console=console, show_time=False, show_level=False, show_path=False)],
        force=True,
    )
    namespace = __name__.split(".", maxsplit=1)[0]
    namespace_prefix = f"{namespace}."
    for logger_name in list(logging.root.manager.loggerDict.keys()):
        if logger_name == namespace or logger_name.startswith(namespace_prefix):
            app_logger = logging.getLogger(logger_name)
            app_logger.setLevel(configured_level)
            app_logger.propagate = True
    logging.getLogger(namespace).setLevel(configured_level)
    logger.debug("logging configured level=%s debug=%s", logging.getLevelName(configured_level), debug)


def _resolve_hotkey_ss58_from_wallet(settings: Settings) -> str:
    import bittensor as bt

    wallet = bt.wallet(
        name=settings.bt_wallet_name,
        hotkey=settings.bt_wallet_hotkey,
        path=str(settings.bt_wallet_path.expanduser()),
    )
    hotkey = str(getattr(getattr(wallet, "hotkey", None), "ss58_address", "")).strip()
    if hotkey:
        return hotkey
    fallback = str(getattr(wallet, "hotkey_str", "")).strip()
    if fallback:
        return fallback
    raise typer.BadParameter(
        "Unable to resolve wallet hotkey SS58 address; check BT_WALLET_NAME, "
        "BT_WALLET_HOTKEY, and BT_WALLET_PATH."
    )


def _build_miner_credentials(settings: Settings, *, hotkey: str) -> R2Credentials:
    return R2Credentials(
        account_id=settings.r2_account_id,
        bucket_name=bucket_name_for_hotkey(hotkey),
        region=settings.r2_region,
        read_access_key=settings.r2_read_access_key,
        read_secret_key=settings.r2_read_secret_key,
        write_access_key=settings.r2_write_access_key,
        write_secret_key=settings.r2_write_secret_key,
    )


def _build_captioner(settings: Settings) -> Captioner:
    """Build a captioner from settings; OpenAI is preferred when both keys set.

    If neither key is set the returned captioner is disabled and emits
    empty captions (the trainer falls back to NEXIS_TRAINER_DEFAULT_PROMPT).
    """
    openai_key = settings.openai_api_key.strip()
    gemini_key = settings.gemini_api_key.strip()
    if openai_key:
        return Captioner(
            api_key=openai_key,
            model=settings.caption_model,
            timeout_sec=settings.caption_timeout_sec,
        )
    if gemini_key:
        return Captioner(
            api_key=gemini_key,
            model=settings.caption_model,
            base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
            timeout_sec=settings.caption_timeout_sec,
        )
    return Captioner()


def _eval_data_local_dir(settings: Settings) -> Path:
    """Local directory under the workdir where the eval bucket is synced.

    Lives at `<workdir>/eval_data/`. The workdir is bind-mounted 1:1 between
    host and validator/trainer container in docker compose, so this path is
    valid to pass into sibling-container `-v` mounts as-is.
    """
    return (settings.workdir / "eval_data").resolve()


async def _refresh_eval_data(settings: Settings) -> Path:
    """Download the latest eval dataset from the nexis-eval bucket.

    Called by `nexis train` and `nexis validate` before each cycle. Raises
    `RuntimeError` on credential failure or a complete download failure —
    cycles cannot proceed with empty/stale eval data, and bubbling up lets
    the loop's outer try/except handle the retry + sleep.
    """
    local_dir = _eval_data_local_dir(settings)
    store = build_eval_data_store(
        account_id=settings.nexis_eval_account_id,
        bucket_name=settings.nexis_eval_bucket,
        region=settings.r2_region,
        read_access_key=settings.nexis_eval_read_access_key,
        read_secret_key=settings.nexis_eval_read_secret_key,
    )
    if store is None:
        raise RuntimeError(
            "eval-data bucket credentials are incomplete; "
            "set NEXIS_EVAL_ACCOUNT_ID / NEXIS_EVAL_READ_ACCESS_KEY / "
            "NEXIS_EVAL_READ_SECRET_KEY"
        )
    count = await sync_eval_data(
        store=store,
        prefix=settings.nexis_eval_prefix,
        local_dir=local_dir,
    )
    if count == 0:
        raise RuntimeError(
            f"eval-data sync downloaded 0 files from {settings.nexis_eval_bucket}"
            f"/{settings.nexis_eval_prefix} into {local_dir}"
        )
    return local_dir


def _build_record_info_store(settings: Settings) -> R2S3Store | None:
    creds = build_nexis_miner_credentials(
        account_id=settings.record_info_account_id,
        bucket_name=settings.record_info_bucket,
        region=settings.r2_region,
        read_access_key=settings.record_info_read_access_key,
        read_secret_key=settings.record_info_read_secret_key,
        write_access_key=settings.record_info_write_access_key,
        write_secret_key=settings.record_info_write_secret_key,
    )
    if creds is None:
        return None
    return R2S3Store(creds)


def _build_nexis_miner_bucket(
    settings: Settings,
    *,
    require_write: bool,
) -> NexisMinerBucket | None:
    creds = build_nexis_miner_credentials(
        account_id=settings.nexis_miner_account_id,
        bucket_name=settings.nexis_miner_bucket,
        region=settings.r2_region,
        read_access_key=settings.nexis_miner_read_access_key,
        read_secret_key=settings.nexis_miner_read_secret_key,
        write_access_key=settings.nexis_miner_write_access_key,
        write_secret_key=settings.nexis_miner_write_secret_key,
    )
    if creds is None:
        return None
    if require_write and (
        not settings.nexis_miner_write_access_key.strip()
        or not settings.nexis_miner_write_secret_key.strip()
    ):
        return None
    return NexisMinerBucket(R2S3Store(creds))


async def _sleep_poll(seconds: float) -> None:
    await asyncio.sleep(max(seconds, 1.0))


# -------------- commit-credentials --------------


@app.command("commit-credentials")
def commit_credentials() -> None:
    from .chain.credentials import ReadCredentialCommitmentManager

    settings = load_settings()
    hotkey_ss58 = _resolve_hotkey_ss58_from_wallet(settings)
    _configure_logging(settings.log_level)
    creds = _build_miner_credentials(settings, hotkey=hotkey_ss58)
    manager = ReadCredentialCommitmentManager(
        netuid=settings.netuid,
        network=settings.bt_network,
        wallet_name=settings.bt_wallet_name,
        wallet_hotkey=settings.bt_wallet_hotkey,
        wallet_path=settings.bt_wallet_path,
        r2_region=settings.r2_region,
    )
    commitment = manager.commit_read_credentials(hotkey_ss58, creds)
    logger.info("credentials committed for hotkey=%s", hotkey_ss58)
    console.print(f"read credentials committed: {commitment}")


# -------------- mine --------------


@app.command("mine")
def mine(
    debug: bool = typer.Option(False, "--debug", help="Enable verbose debug logging."),
) -> None:
    settings = load_settings()
    hotkey_ss58 = _resolve_hotkey_ss58_from_wallet(settings)
    _configure_logging("INFO", debug=debug)
    creds = _build_miner_credentials(settings, hotkey=hotkey_ss58)
    creds.validate_account_id()
    creds.validate_read_key_lengths()
    creds.validate_bucket_name()
    creds.validate_bucket_for_hotkey(hotkey_ss58)
    store = R2S3Store(creds)
    captioner = _build_captioner(settings)
    if not captioner.enabled:
        logger.warning(
            "captioning disabled: no OPENAI_API_KEY or GEMINI_API_KEY set; "
            "miners will produce empty captions and trainer will fall back to "
            "NEXIS_TRAINER_DEFAULT_PROMPT for every clip"
        )
    pipeline = MinerPipeline(store=store, captioner=captioner)
    try:
        asyncio.run(
            _run_miner_loop(
                settings=settings,
                store=store,
                pipeline=pipeline,
                hotkey_ss58=hotkey_ss58,
            )
        )
    except KeyboardInterrupt:
        console.print("miner loop stopped")


async def _run_miner_loop(
    *,
    settings: Settings,
    store: R2S3Store,
    pipeline: MinerPipeline,
    hotkey_ss58: str,
) -> None:
    console.print(f"miner loop started: sleep_sec={settings.miner_loop_sleep_sec}")
    while True:
        try:
            existing = await list_miner_interval_ids(store)
            next_interval_id = (max(existing) + 1) if existing else 1
            console.print(f"mining interval_id={next_interval_id}")
            dataset_path, manifest_path = await pipeline.run_interval(
                sources_file=settings.sources_file,
                netuid=settings.netuid,
                miner_hotkey=hotkey_ss58,
                interval_id=next_interval_id,
                workdir=settings.workdir / "miner",
            )
            console.print(
                f"mined interval={next_interval_id} "
                f"dataset={dataset_path} manifest={manifest_path}"
            )
        except Exception as exc:
            logger.exception("miner iteration failed: %s", exc)
        await _sleep_poll(settings.miner_loop_sleep_sec)


# -------------- train --------------


@app.command("train")
def train(
    num_gpus: int = typer.Option(0, "--num-gpus", help="GPU count (0 = read from settings)."),
    debug: bool = typer.Option(False, "--debug", help="Enable verbose debug logging."),
) -> None:
    settings = load_settings()
    validator_hotkey = _resolve_hotkey_ss58_from_wallet(settings)
    _configure_logging("INFO", debug=debug)
    is_owner = validator_hotkey == settings.owner_validator_hotkey.strip()
    if not is_owner:
        raise typer.BadParameter(
            f"`nexis train` is reserved for the owner validator "
            f"({settings.owner_validator_hotkey}); your hotkey={validator_hotkey}"
        )
    nexis_miner = _build_nexis_miner_bucket(settings, require_write=True)
    if nexis_miner is None:
        raise typer.BadParameter(
            "nexis_miner bucket write credentials are required for `nexis train`. "
            "Set NEXIS_MINER_ACCOUNT_ID and NEXIS_MINER_WRITE_ACCESS_KEY/SECRET."
        )
    record_info_store = _build_record_info_store(settings)
    pool_size = num_gpus if num_gpus > 0 else settings.trainer_num_gpus
    pool = DockerGPUPool(num_gpus=pool_size)
    try:
        asyncio.run(
            _run_train_loop(
                settings=settings,
                validator_hotkey=validator_hotkey,
                nexis_miner=nexis_miner,
                record_info_store=record_info_store,
                pool=pool,
            )
        )
    except KeyboardInterrupt:
        console.print("trainer loop stopped")


async def _load_global_record_index(
    *,
    record_info_store: R2S3Store | None,
    object_key: str,
    workdir: Path,
) -> dict[str, list[float]]:
    if record_info_store is None:
        return {}
    try:
        exists = await record_info_store.object_exists(object_key)
    except Exception:
        return {}
    if not exists:
        return {}
    local = workdir / "record-info" / "snapshot.json"
    ok = await record_info_store.download_file(object_key, local)
    if not ok or not local.exists():
        return {}
    try:
        payload = json.loads(local.read_text(encoding="utf-8"))
    except Exception:
        return {}
    entries = payload.get("video_v1") if isinstance(payload, dict) else None
    if not isinstance(entries, dict):
        entries = payload if isinstance(payload, dict) else {}
    result: dict[str, list[float]] = {}
    for source_url, values in entries.items():
        if not isinstance(values, list):
            continue
        floats: list[float] = []
        for item in values:
            try:
                floats.append(float(item))
            except (TypeError, ValueError):
                continue
        if floats:
            result[str(source_url)] = sorted(set(floats))
    return result


async def _run_train_loop(
    *,
    settings: Settings,
    validator_hotkey: str,
    nexis_miner: NexisMinerBucket,
    record_info_store: R2S3Store | None,
    pool: DockerGPUPool,
) -> None:
    from .chain.credentials import ReadCredentialCommitmentManager

    manager = ReadCredentialCommitmentManager(
        netuid=settings.netuid,
        network=settings.bt_network,
        wallet_name=settings.bt_wallet_name,
        wallet_hotkey=settings.bt_wallet_hotkey,
        wallet_path=settings.bt_wallet_path,
        r2_region=settings.r2_region,
    )

    reporter = _build_reporter(settings, validator_hotkey)

    console.print(
        f"trainer loop started owner={validator_hotkey} num_gpus={pool.num_gpus} "
        f"poll_sec={settings.train_poll_sec}"
    )
    async with _open_subtensor(settings.bt_network) as subtensor:
        while True:
            try:
                cycle_id = await determine_next_cycle_id(nexis_miner)
                if cycle_id is None:
                    logger.info("waiting for previous cycle scoring to finish")
                    await _sleep_poll(settings.train_poll_sec)
                    continue

                hotkeys = await fetch_hotkeys_from_metagraph_async(
                    netuid=settings.netuid,
                    network=settings.bt_network,
                    subtensor=subtensor,
                )
                committed_payload = await manager.get_all_credentials_async(subtensor=subtensor)
                committed_hotkeys = [hk for hk in hotkeys if committed_payload.get(hk)]

                invalid_hotkeys: set[str] = set()
                blacklist_hotkeys: set[str] = set()
                if reporter is not None:
                    invalid_hotkeys = {
                        item for item in await reporter.fetch_invalid_hotkeys() if item
                    }
                    blacklist_hotkeys = {
                        item for item in await reporter.fetch_blacklist_hotkeys() if item
                    }

                last_total_score = None
                if cycle_id > 1:
                    last_total_score = await nexis_miner.download_total_score(
                        cycle_id - 1,
                        settings.workdir / "trainer" / f"prev_total_{cycle_id - 1}.json",
                    )

                global_record_index = await _load_global_record_index(
                    record_info_store=record_info_store,
                    object_key=settings.record_info_object_key,
                    workdir=settings.workdir / "trainer",
                )

                store_cache: dict[str, R2S3Store] = {}

                def store_for_hotkey(hotkey: str) -> R2S3Store:
                    cached = store_cache.get(hotkey)
                    if cached is not None:
                        return cached
                    payload = committed_payload.get(hotkey)
                    creds = manager.build_r2_credentials(payload, hotkey=hotkey)
                    if creds is None:
                        raise RuntimeError(f"missing committed read credentials for {hotkey}")
                    store = R2S3Store(creds)
                    store_cache[hotkey] = store
                    return store

                async def on_select(selected: list[str], cycle: int) -> None:
                    if reporter is None or not selected:
                        return
                    await reporter.post_invalid_hotkeys(invalid_hotkeys=selected)
                    logger.info(
                        "submitted %d hotkeys to invalid list cycle=%d",
                        len(selected),
                        cycle,
                    )

                cycle_workdir = settings.workdir / "trainer"
                cycle_workdir.mkdir(parents=True, exist_ok=True)

                # Refresh the canonical eval dataset from R2 before every
                # cycle so the trainer always uses the latest evaluation
                # prompts/images. Raises if the bucket is misconfigured —
                # the outer try/except logs and retries on the next tick.
                eval_data_dir = await _refresh_eval_data(settings)

                result = await run_training_cycle(
                    settings=settings,
                    candidate_hotkeys=committed_hotkeys,
                    invalid_hotkeys=invalid_hotkeys,
                    blacklist_hotkeys=blacklist_hotkeys,
                    last_total_score=last_total_score,
                    store_for_hotkey=store_for_hotkey,
                    nexis_miner=nexis_miner,
                    pool=pool,
                    cycle_id=cycle_id,
                    workdir=cycle_workdir,
                    global_record_index=global_record_index,
                    eval_data_dir=eval_data_dir,
                    on_select=on_select,
                )
                console.print(
                    f"cycle={result.cycle_id} accepted={len(result.accepted)} "
                    f"rejected={len(result.rejected)} trained={len(result.trained)} "
                    f"failed={len(result.failed_training)}"
                )
            except Exception as exc:
                logger.exception("trainer iteration failed: %s", exc)
            await _sleep_poll(settings.train_poll_sec)


def _build_reporter(settings: Settings, validator_hotkey: str) -> ValidationResultReporter | None:
    import bittensor as bt

    api_url = settings.validation_api_url.strip()
    if not api_url:
        return None
    wallet = bt.wallet(
        name=settings.bt_wallet_name,
        hotkey=settings.bt_wallet_hotkey,
        path=str(settings.bt_wallet_path.expanduser()),
    )
    return ValidationResultReporter(
        endpoint_url=api_url,
        hotkey_ss58=validator_hotkey,
        hotkey_signer=wallet.hotkey,
        timeout_sec=settings.validation_api_timeout_sec,
    )


# -------------- validate --------------


@app.command("validate")
def validate(
    debug: bool = typer.Option(False, "--debug", help="Enable verbose debug logging."),
) -> None:
    settings = load_settings()
    validator_hotkey = _resolve_hotkey_ss58_from_wallet(settings)
    _configure_logging("INFO", debug=debug)
    nexis_miner = _build_nexis_miner_bucket(settings, require_write=False)
    if nexis_miner is None:
        raise typer.BadParameter(
            "nexis_miner bucket read credentials are required. "
            "Set NEXIS_MINER_ACCOUNT_ID and NEXIS_MINER_READ_ACCESS_KEY/SECRET."
        )
    try:
        asyncio.run(
            _run_validate_loop(
                settings=settings,
                validator_hotkey=validator_hotkey,
                nexis_miner=nexis_miner,
            )
        )
    except KeyboardInterrupt:
        console.print("validator loop stopped")


async def _run_validate_loop(
    *,
    settings: Settings,
    validator_hotkey: str,
    nexis_miner: NexisMinerBucket,
) -> None:
    reporter = _build_reporter(settings, validator_hotkey)
    scoring_task = asyncio.create_task(
        _scoring_loop(
            settings=settings,
            validator_hotkey=validator_hotkey,
            nexis_miner=nexis_miner,
            reporter=reporter,
        ),
        name="scoring-loop",
    )
    set_weight_task = asyncio.create_task(
        _set_weight_loop(
            settings=settings,
            validator_hotkey=validator_hotkey,
            nexis_miner=nexis_miner,
        ),
        name="set-weight-loop",
    )
    console.print("validator loop started: scoring + set-weight")
    try:
        await asyncio.gather(scoring_task, set_weight_task)
    finally:
        for task in (scoring_task, set_weight_task):
            if not task.done():
                task.cancel()


async def _scoring_loop(
    *,
    settings: Settings,
    validator_hotkey: str,
    nexis_miner: NexisMinerBucket,
    reporter: ValidationResultReporter | None,
) -> None:
    last_scored_cycle: int | None = None
    while True:
        try:
            cycle_id = await nexis_miner.latest_cycle_id()
            if cycle_id is None:
                logger.info("scoring: no cycles yet")
            elif last_scored_cycle == cycle_id:
                logger.debug("scoring: cycle %d already scored locally", cycle_id)
            elif await nexis_miner.has_validator_score(cycle_id, validator_hotkey):
                logger.info("scoring: cycle %d already has my score", cycle_id)
                last_scored_cycle = cycle_id
            else:
                workdir = settings.workdir / "scorer" / str(cycle_id)
                workdir.mkdir(parents=True, exist_ok=True)
                # Pull the canonical eval dataset before scoring so VBench
                # uses the same prompts/images the trainer just used.
                eval_data_dir = await _refresh_eval_data(settings)
                scores = await score_cycle(
                    settings=settings,
                    cycle_id=cycle_id,
                    nexis_miner=nexis_miner,
                    workdir=workdir,
                    eval_data_dir=eval_data_dir,
                )
                console.print(f"scored cycle={cycle_id} miners={len(scores)}")
                if reporter is not None and scores:
                    ok = await submit_scores(
                        reporter=reporter,
                        cycle_id=cycle_id,
                        scores=scores,
                    )
                    if ok:
                        last_scored_cycle = cycle_id
                await cleanup_score_workdir(workdir)
        except Exception as exc:
            logger.exception("scoring iteration failed: %s", exc)
        await _sleep_poll(settings.score_poll_sec)


async def _find_total_score_cycle(
    nexis_miner: NexisMinerBucket,
    *,
    max_lookback: int = 10,
) -> tuple[int, dict[str, Any]] | None:
    cycles = await nexis_miner.list_cycle_ids()
    for cycle_id in reversed(cycles[-max_lookback:]):
        if not await nexis_miner.has_total_score(cycle_id):
            continue
        local = Path("/tmp") / f"nexis_total_score_{cycle_id}.json"
        payload = await nexis_miner.download_total_score(cycle_id, local)
        if isinstance(payload, dict):
            return cycle_id, payload
    return None


async def _set_weight_loop(
    *,
    settings: Settings,
    validator_hotkey: str,
    nexis_miner: NexisMinerBucket,
) -> None:
    last_submitted_epoch: int | None = None
    weight_failure_count = 0
    next_retry_ts = 0.0
    async with _open_subtensor(settings.bt_network) as subtensor:
        while True:
            try:
                current_block = await fetch_current_block_async(
                    network=settings.bt_network,
                    subtensor=subtensor,
                )
                current_epoch = current_block // WEIGHT_SUBMISSION_INTERVAL_BLOCKS
                if (
                    last_submitted_epoch is not None
                    and current_epoch <= last_submitted_epoch
                ) or time.monotonic() < next_retry_ts:
                    await _sleep_poll(settings.block_poll_sec)
                    continue

                found = await _find_total_score_cycle(nexis_miner)
                if found is None:
                    logger.info(
                        "set-weight: no total_score.json found; burning to UID 0"
                    )
                    weights_by_hotkey: dict[str, float] = {}
                else:
                    cycle_id, payload = found
                    miner_scores = parse_total_score_payload(payload)
                    weights_by_hotkey = compute_top_k_weights(
                        miner_scores,
                        top_k=WEIGHT_TOP_K,
                    )
                    logger.info(
                        "set-weight cycle=%d top_k=%d weights=%s",
                        cycle_id,
                        len(weights_by_hotkey),
                        weights_by_hotkey,
                    )

                submission = await submit_weights_to_chain_async(
                    netuid=settings.netuid,
                    network=settings.bt_network,
                    wallet_name=settings.bt_wallet_name,
                    wallet_hotkey=settings.bt_wallet_hotkey,
                    wallet_path=settings.bt_wallet_path,
                    weights_by_hotkey=weights_by_hotkey,
                    subtensor=subtensor,
                )
                if submission.submitted:
                    console.print(f"set_weights submitted epoch={current_epoch}")
                    last_submitted_epoch = current_epoch
                    weight_failure_count = 0
                    next_retry_ts = 0.0
                else:
                    weight_failure_count += 1
                    backoff = min(
                        _WEIGHT_RETRY_BACKOFF_MAX_SEC,
                        _WEIGHT_RETRY_BACKOFF_BASE_SEC * (2 ** max(weight_failure_count - 1, 0)),
                    )
                    next_retry_ts = time.monotonic() + float(backoff)
                    logger.error(
                        "set_weights failed reason=%s retry_in=%ds",
                        submission.reason,
                        backoff,
                    )
            except Exception as exc:
                logger.exception("set-weight iteration failed: %s", exc)
            await _sleep_poll(settings.block_poll_sec)


# Keep a reference so the unused-import linter doesn't trip.
_ = build_chain_weight_payload


def main() -> None:
    app()


if __name__ == "__main__":
    main()
