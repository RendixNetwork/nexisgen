"""Nexis CLI."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict
from pathlib import Path
from typing import Any
from typing import Callable
from typing import TYPE_CHECKING
from urllib.parse import urlparse
from urllib.parse import urlunparse

import httpx
import typer
from rich.console import Console
from rich.logging import RichHandler

from .chain.metagraph import (
    _open_subtensor,
    fetch_current_block_async,
    fetch_hotkeys_from_metagraph_async,
)
from .chain.weights import submit_weights_to_chain_async
from .config import Settings, load_settings
from .miner.captioner import Captioner
from .miner.pipeline import MinerPipeline
from .models import ClipRecord
from .models import ValidationDecision
from .protocol import (
    INTERVAL_LENGTH_BLOCKS,
    UPLOAD_DEADLINE_RESERVED_BLOCKS,
    WEIGHT_SUBMISSION_INTERVAL_BLOCKS,
)
from .specs import DEFAULT_SPEC_ID, DatasetSpecRegistry
from .storage.r2 import R2Credentials, R2S3Store, bucket_name_for_hotkey
from .validator.caption_semantic import CaptionSemanticChecker
from .validator.category_check import NatureCategoryChecker
from .validator.owner_sync import (
    merge_records_into_index as owner_merge_records_into_index,
    parse_record_info as owner_parse_record_info,
    serialize_record_info as owner_serialize_record_info,
    sync_record_info_to_owner_bucket_async as owner_sync_record_info_to_owner_bucket_async,
    upload_record_info_snapshot_async as owner_upload_record_info_snapshot_async,
    upload_validated_datasets_to_owner_bucket_async as owner_upload_validated_datasets_to_owner_bucket_async,
)
from .validator.pipeline import ValidatorPipeline
from .validator.reporting import ValidationResultReporter

if TYPE_CHECKING:
    from .chain.credentials import ReadCredentialCommitmentManager
    from .scoring import WeightComputer

app = typer.Typer(name="nexis", no_args_is_help=True)
console = Console()
logger = logging.getLogger(__name__)
_WEIGHT_RETRY_BACKOFF_BASE_SEC = 10
_WEIGHT_RETRY_BACKOFF_MAX_SEC = 300
_OPENAI_PRIMARY_MODEL = "gpt-4o"
_GEMINI_PRIMARY_MODEL = "gemini-3.1-flash-lite-preview"
_GEMINI_OPENAI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"
_SUPPORTED_CATEGORY = "nature_landscape_scenery"


def _configure_logging(level: str, *, debug: bool = False) -> None:
    configured_level = logging.DEBUG if debug else getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=configured_level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[RichHandler(console=console, show_time=False, show_level=False, show_path=False)],
        force=True,
    )
    # Bittensor can raise third-party logger levels to CRITICAL; force our namespace back.
    namespace = __name__.split(".", maxsplit=1)[0]
    namespace_prefix = f"{namespace}."
    for logger_name in list(logging.root.manager.loggerDict.keys()):
        if logger_name == namespace or logger_name.startswith(namespace_prefix):
            app_logger = logging.getLogger(logger_name)
            app_logger.setLevel(configured_level)
            app_logger.propagate = True
    logging.getLogger(namespace).setLevel(configured_level)
    logger.debug("logging configured level=%s debug=%s", logging.getLevelName(configured_level), debug)


def _build_remote_credentials(settings: Settings, *, hotkey: str) -> R2Credentials:
    creds = R2Credentials(
        account_id=settings.r2_account_id,
        bucket_name=bucket_name_for_hotkey(hotkey),
        region=settings.r2_region,
        read_access_key=settings.r2_read_access_key,
        read_secret_key=settings.r2_read_secret_key,
        write_access_key=settings.r2_write_access_key,
        write_secret_key=settings.r2_write_secret_key,
    )
    logger.debug(
        "constructed r2 credentials account_id=%s bucket=%s endpoint=%s region=%s",
        creds.account_id,
        creds.bucket_name,
        creds.endpoint_url,
        creds.region,
    )
    return creds


def _build_shared_bucket_credentials(
    *,
    settings: Settings,
    account_id: str,
    bucket_name: str,
    read_access_key: str,
    read_secret_key: str,
    write_access_key: str,
    write_secret_key: str,
) -> R2Credentials | None:
    resolved_account_id = account_id.strip()
    bucket = bucket_name.strip()
    read_key = read_access_key.strip()
    read_secret = read_secret_key.strip()
    if not resolved_account_id or not bucket or not read_key or not read_secret:
        return None
    effective_write_key = write_access_key.strip() or read_key
    effective_write_secret = write_secret_key.strip() or read_secret
    return R2Credentials(
        account_id=resolved_account_id,
        bucket_name=bucket,
        region=settings.r2_region,
        read_access_key=read_key,
        read_secret_key=read_secret,
        write_access_key=effective_write_key,
        write_secret_key=effective_write_secret,
    )


def _parse_record_info(raw: str) -> dict[str, list[float]]:
    return owner_parse_record_info(raw)


def _serialize_record_info(record_index: dict[str, list[float]]) -> str:
    return owner_serialize_record_info(record_index)


async def _load_record_info_snapshot(
    *,
    record_info_store: R2S3Store | None,
    object_key: str,
    workdir: Path,
) -> tuple[dict[str, list[float]], bool]:
    if record_info_store is None:
        return {}, True
    try:
        exists = await record_info_store.object_exists(object_key)
    except Exception:
        return {}, False
    if not exists:
        return {}, True
    local = workdir / "record-info" / "snapshot.json"
    ok = await record_info_store.download_file(object_key, local)
    if not ok or not local.exists():
        return {}, False
    raw = local.read_text(encoding="utf-8")
    try:
        json.loads(raw)
    except Exception:
        logger.warning("record-info snapshot invalid JSON; rebuilding from empty index")
        return {}, True
    return _parse_record_info(raw), True


def _merge_records_into_index(
    *,
    record_index: dict[str, list[float]],
    records: list[ClipRecord],
) -> None:
    owner_merge_records_into_index(
        record_index=record_index,
        records=records,
    )


async def _upload_record_info_snapshot(
    *,
    record_info_store: R2S3Store | None,
    object_key: str,
    workdir: Path,
    record_index: dict[str, list[float]],
) -> None:
    await owner_upload_record_info_snapshot_async(
        record_info_store=record_info_store,
        object_key=object_key,
        workdir=workdir,
        record_index=record_index,
    )


async def _upload_validated_datasets_to_owner_bucket(
    *,
    owner_store: R2S3Store | None,
    source_store_for_hotkey: Callable[[str], R2S3Store],
    validator: ValidatorPipeline,
    decisions: list[ValidationDecision],
    interval_id: int,
    workdir: Path,
) -> dict[str, list[ClipRecord]]:
    return await owner_upload_validated_datasets_to_owner_bucket_async(
        owner_store=owner_store,
        source_store_for_hotkey=source_store_for_hotkey,
        validator=validator,
        decisions=decisions,
        interval_id=interval_id,
        workdir=workdir,
    )


async def _sync_record_info_to_owner_bucket(
    *,
    record_info_store: R2S3Store | None,
    owner_store: R2S3Store | None,
    source_store_for_hotkey: Callable[[str], R2S3Store],
    workdir: Path,
    spec_registry: DatasetSpecRegistry,
) -> dict[str, int]:
    return await owner_sync_record_info_to_owner_bucket_async(
        record_info_store=record_info_store,
        owner_store=owner_store,
        source_store_for_hotkey=source_store_for_hotkey,
        workdir=workdir,
        spec_registry=spec_registry,
    )


def _resolve_hotkey_ss58_from_wallet(settings: Settings) -> str:
    import bittensor as bt

    wallet = bt.wallet(
        name=settings.bt_wallet_name,
        hotkey=settings.bt_wallet_hotkey,
        path=str(settings.bt_wallet_path.expanduser()),
    )

    hotkey = str(getattr(getattr(wallet, "hotkey", None), "ss58_address", "")).strip()
    if hotkey:
        logger.debug("resolved wallet hotkey ss58 from wallet.hotkey.ss58_address")
        return hotkey

    fallback = str(getattr(wallet, "hotkey_str", "")).strip()
    if fallback:
        logger.debug("resolved wallet hotkey from wallet.hotkey_str fallback")
        return fallback

    raise typer.BadParameter(
        "Unable to resolve wallet hotkey SS58 address; check BT_WALLET_NAME, "
        "BT_WALLET_HOTKEY, and BT_WALLET_PATH."
    )


def _load_hotkeys_from_file(path: Path) -> set[str]:
    if not path.exists():
        logger.debug("hotkey file not found path=%s", path)
        return set()
    values: set[str] = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        values.add(line)
    return values


def _parse_exclude_hotkeys(text: str) -> set[str]:
    if not text.strip():
        return set()
    return {part.strip() for part in text.split(",") if part.strip()}


def _parse_spec_list(text: str) -> list[str]:
    values = [item.strip() for item in text.split(",") if item.strip()]
    deduped: list[str] = []
    for value in values:
        if value not in deduped:
            deduped.append(value)
    return deduped


def _resolve_enabled_specs(text: str, registry: DatasetSpecRegistry) -> list[str]:
    values = _parse_spec_list(text)
    if not values:
        values = [DEFAULT_SPEC_ID]
    unknown = [value for value in values if value not in registry.list_spec_ids()]
    if unknown:
        raise typer.BadParameter(f"Unknown dataset specs: {','.join(sorted(unknown))}")
    return values


def _current_interval_start(block: int) -> int:
    return block - (block % INTERVAL_LENGTH_BLOCKS)


def _interval_label(interval_start: int) -> str:
    interval_end = interval_start + INTERVAL_LENGTH_BLOCKS
    return f"{interval_start}-{interval_end}"


def _initial_miner_interval_start(current_block: int) -> int:
    current_interval_start = _current_interval_start(current_block)
    return max(current_interval_start - INTERVAL_LENGTH_BLOCKS, 0)


def _latest_eligible_validation_interval_start(current_block: int) -> int | None:
    """Return latest interval start eligible for validation.

    Interval becomes eligible only after upload reserve blocks have elapsed
    past interval close.
    """
    latest_eligible_end = current_block - UPLOAD_DEADLINE_RESERVED_BLOCKS
    if latest_eligible_end < INTERVAL_LENGTH_BLOCKS:
        return None
    latest_closed_end = (latest_eligible_end // INTERVAL_LENGTH_BLOCKS) * INTERVAL_LENGTH_BLOCKS
    latest_start = latest_closed_end - INTERVAL_LENGTH_BLOCKS
    return latest_start if latest_start >= 0 else None


def _weight_retry_backoff_sec(failure_count: int) -> int:
    exponent = max(failure_count - 1, 0)
    return min(_WEIGHT_RETRY_BACKOFF_MAX_SEC, _WEIGHT_RETRY_BACKOFF_BASE_SEC * (2**exponent))


def _resolve_llm_runtime(
    settings: Settings,
    *,
    openai_model: str,
) -> tuple[str, str, str, str | None, str]:
    resolved_openai_model = openai_model.strip()
    # Auto-upgrade legacy default to requested OpenAI model.
    if not resolved_openai_model or resolved_openai_model == "gpt-4o-mini":
        resolved_openai_model = _OPENAI_PRIMARY_MODEL
    openai_api_key = settings.openai_api_key.strip()
    gemini_api_key = settings.gemini_api_key.strip()
    if openai_api_key and gemini_api_key:
        logger.info("both OPENAI_API_KEY and GEMINI_API_KEY are set; preferring OpenAI")
    if openai_api_key:
        return (
            "openai",
            openai_api_key,
            resolved_openai_model,
            None,
            "openai_key",
        )
    if gemini_api_key:
        return (
            "gemini",
            gemini_api_key,
            _GEMINI_PRIMARY_MODEL,
            _GEMINI_OPENAI_BASE_URL,
            "gemini_key",
        )
    return (
        "openai",
        "",
        resolved_openai_model,
        None,
        "no_api_key",
    )


async def _fetch_hotkeys_with_commitments(
    *,
    settings: Settings,
    manager: "ReadCredentialCommitmentManager",
    blacklist_file: Path | None,
    exclude_hotkeys: str,
    subtensor: object,
) -> tuple[list[str], dict[str, dict]]:
    hotkeys = await fetch_hotkeys_from_metagraph_async(
        netuid=settings.netuid,
        network=settings.bt_network,
        subtensor=subtensor,
    )
    console.print(f"metagraph hotkeys discovered: {len(hotkeys)}")
    active_blacklist_file = blacklist_file or settings.validator_blacklist_file
    file_blacklist = _load_hotkeys_from_file(active_blacklist_file)
    runtime_excludes = _parse_exclude_hotkeys(exclude_hotkeys)
    excluded = file_blacklist | runtime_excludes
    if file_blacklist:
        console.print(f"blacklist file exclusions loaded: {len(file_blacklist)}")
    if runtime_excludes:
        console.print(f"runtime exclusions loaded: {len(runtime_excludes)}")

    committed_payload = await manager.get_all_credentials_async(subtensor=subtensor)
    committed_hotkeys: list[str] = []
    for hotkey in hotkeys:
        if hotkey in excluded:
            console.print(f"skipping {hotkey}: in validator exclusion list")
            continue
        if committed_payload.get(hotkey) is None:
            console.print(f"skipping {hotkey}: no committed read credentials found")
            continue
        committed_hotkeys.append(hotkey)
    logger.debug(
        "hotkey filtering complete discovered=%d committed=%d excluded=%d",
        len(hotkeys),
        len(committed_hotkeys),
        len(excluded),
    )
    return committed_hotkeys, committed_payload


async def _sleep_poll(seconds: float) -> None:
    await asyncio.sleep(max(seconds, 0.5))


def _resolve_latest_result_url(validation_api_url: str) -> str:
    """Resolve the API endpoint used to fetch cached latest results."""
    raw_url = validation_api_url.strip()
    if not raw_url:
        return raw_url
    parsed = urlparse(raw_url)
    if parsed.scheme and parsed.netloc:
        if parsed.path in {"/v1/get_latest_result", "/get_latest_result"}:
            return raw_url
        return urlunparse((parsed.scheme, parsed.netloc, "/v1/get_latest_result", "", "", ""))
    if raw_url.endswith("/v1/get_latest_result") or raw_url.endswith("/get_latest_result"):
        return raw_url
    return f"{raw_url.rstrip('/')}/v1/get_latest_result"


async def fetch_latest_result_snapshot(*, url: str, timeout_sec: float) -> dict[str, Any]:
    headers = {"Accept": "application/json"}
    read = max(float(timeout_sec), 1.0)
    connect = min(30.0, read)
    timeout = httpx.Timeout(connect=connect, read=read, write=read, pool=read)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(url, headers=headers)
    except httpx.RequestError as exc:
        name = type(exc).__name__
        detail = str(exc).strip() or repr(exc)
        raise RuntimeError(f"latest-result request failed ({name}): {detail}") from exc

    status_code = int(response.status_code)
    if status_code < 200 or status_code >= 300:
        raise RuntimeError(f"latest-result request failed status={status_code}")

    try:
        parsed = response.json()
    except Exception as exc:
        raise RuntimeError("latest-result response is not valid JSON") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("latest-result response must be a JSON object")
    return parsed



def compute_score_totals_from_decisions(
    *,
    decisions: list[dict[str, Any]],
    interval_start: int,
    interval_end: int,
    weight_computer: WeightComputer,
) -> dict[str, float]:
    _ = weight_computer
    totals_by_hotkey: dict[str, int] = {}
    seen_keys: set[tuple[str, int, str]] = set()

    for row in decisions:
        if row.get("accepted") is not True:
            continue

        miner_hotkey = str(row.get("miner_hotkey", "")).strip()
        if not miner_hotkey:
            continue

        validator_hotkey = str(row.get("validator_hotkey", "")).strip()
        try:
            interval_id = int(row.get("interval_id"))
        except (TypeError, ValueError):
            continue
        if interval_id < interval_start or interval_id >= interval_end:
            continue

        dedupe_key = (validator_hotkey, interval_id, miner_hotkey)
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)

        try:
            record_count = int(row.get("record_count", 0))
        except (TypeError, ValueError):
            record_count = 0
        if record_count <= 0:
            continue
        totals_by_hotkey[miner_hotkey] = totals_by_hotkey.get(miner_hotkey, 0) + record_count

    if not totals_by_hotkey:
        return {}

    ranked = sorted(
        totals_by_hotkey.items(),
        key=lambda item: (-item[1], item[0]),
    )
    scores_by_hotkey: dict[str, float] = {}
    previous_count: int | None = None
    rank = 0
    for hotkey, total_count in ranked:
        if previous_count is None or total_count < previous_count:
            rank += 1
            previous_count = total_count
        scores_by_hotkey[hotkey] = 1.0 / float(2 ** (rank - 1))

    return scores_by_hotkey


@app.command("commit-credentials")
def commit_credentials(
) -> None:
    from .chain.credentials import ReadCredentialCommitmentManager

    settings = load_settings()
    hotkey_ss58 = _resolve_hotkey_ss58_from_wallet(settings)
    _configure_logging(settings.log_level)
    creds = _build_remote_credentials(settings, hotkey=hotkey_ss58)
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


@app.command("mine")
def mine(
    poll_sec: float | None = typer.Option(
        None,
        "--poll-sec",
        help="Block polling interval in seconds (default from NEXIS_BLOCK_POLL_SEC).",
    ),
    debug: bool = typer.Option(
        False,
        "--debug",
        help="Enable verbose debug logging for miner loop.",
    ),
    spec: str = typer.Option(
        "",
        "--spec",
        help="Dataset spec ID to mine (defaults to NEXIS_DATASET_SPEC_DEFAULT).",
    ),
) -> None:
    settings = load_settings()
    spec_registry = DatasetSpecRegistry.with_defaults()
    enabled_specs = _resolve_enabled_specs(settings.miner_enabled_specs, spec_registry)
    active_spec = spec.strip() or settings.dataset_spec_default.strip() or DEFAULT_SPEC_ID
    active_category = settings.dataset_category.strip()
    if not active_category:
        raise typer.BadParameter("NEXIS_DATASET_CATEGORY must not be empty")
    if active_spec not in enabled_specs:
        raise typer.BadParameter(
            f"Spec '{active_spec}' is not enabled for miner (enabled: {', '.join(enabled_specs)})"
        )
    if active_spec not in spec_registry.list_spec_ids():
        raise typer.BadParameter(f"Unknown dataset spec: {active_spec}")
    hotkey_ss58 = _resolve_hotkey_ss58_from_wallet(settings)
    _configure_logging("INFO", debug=debug)
    poll_seconds = settings.block_poll_sec if poll_sec is None else poll_sec

    creds = _build_remote_credentials(settings, hotkey=hotkey_ss58)
    creds.validate_account_id()
    creds.validate_read_key_lengths()
    creds.validate_bucket_name()
    creds.validate_bucket_for_hotkey(hotkey_ss58)
    store = R2S3Store(creds)
    (
        caption_provider,
        caption_api_key,
        caption_model,
        caption_base_url,
        caption_route,
    ) = _resolve_llm_runtime(
        settings,
        openai_model=settings.caption_model,
    )
    logger.info(
        "miner caption runtime provider=%s model=%s route=%s",
        caption_provider,
        caption_model,
        caption_route,
    )
    if caption_route == "no_api_key":
        logger.warning(
            "miner caption key missing; fallback captions will be used until OPENAI_API_KEY or GEMINI_API_KEY is set"
        )
    captioner = Captioner(
        api_key=caption_api_key,
        model=caption_model,
        timeout_sec=settings.caption_timeout_sec,
        provider=caption_provider,
        base_url=caption_base_url,
    )
    pipeline = MinerPipeline(  # type: ignore[arg-type]
        store=store,
        captioner=captioner,
        spec_id=active_spec,
        dataset_category=active_category,
    )
    try:
        asyncio.run(
            _run_miner_loop(
                settings=settings,
                store=store,
                pipeline=pipeline,
                hotkey_ss58=hotkey_ss58,
                poll_seconds=poll_seconds,
                active_spec=active_spec,
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
    poll_seconds: float,
    active_spec: str,
) -> None:
    last_mined_interval_start: int | None = None
    console.print(
        f"miner loop started: interval={INTERVAL_LENGTH_BLOCKS} blocks, poll={poll_seconds:.1f}s"
    )
    logger.info(
        "miner loop initialized hotkey=%s interval_blocks=%d poll_sec=%.1f spec=%s",
        hotkey_ss58,
        INTERVAL_LENGTH_BLOCKS,
        poll_seconds,
        active_spec,
    )
    async with _open_subtensor(settings.bt_network) as subtensor:
        while True:
            try:
                current_block = await fetch_current_block_async(
                    network=settings.bt_network,
                    subtensor=subtensor,
                )
                next_interval_start = (
                    _initial_miner_interval_start(current_block)
                    if last_mined_interval_start is None
                    else last_mined_interval_start + INTERVAL_LENGTH_BLOCKS
                )
                logger.debug(
                    "miner tick current_block=%d next_interval_start=%d last_mined=%s",
                    current_block,
                    next_interval_start,
                    str(last_mined_interval_start),
                )

                while next_interval_start <= _current_interval_start(current_block):
                    manifest_key = f"{next_interval_start}/manifest.json"
                    already_uploaded = await store.object_exists(manifest_key)
                    if already_uploaded:
                        logger.info(
                            "skip mining interval=%s reason=already_uploaded",
                            _interval_label(next_interval_start),
                        )
                        console.print(
                            f"skip interval {_interval_label(next_interval_start)}: already uploaded"
                        )
                    else:
                        logger.info("starting mining interval=%s", _interval_label(next_interval_start))
                        dataset_path, manifest_path = await pipeline.run_interval(
                            sources_file=settings.sources_file,
                            netuid=settings.netuid,
                            miner_hotkey=hotkey_ss58,
                            interval_id=next_interval_start,
                            workdir=settings.workdir,
                        )
                        console.print(
                            f"mined interval {_interval_label(next_interval_start)} "
                            f"dataset={dataset_path} manifest={manifest_path}"
                        )
                        logger.info(
                            "completed mining interval=%s dataset=%s manifest=%s",
                            _interval_label(next_interval_start),
                            dataset_path,
                            manifest_path,
                        )
                    last_mined_interval_start = next_interval_start
                    next_interval_start += INTERVAL_LENGTH_BLOCKS
            except Exception as exc:
                logger.exception("miner loop iteration failed: %s", exc)

            await _sleep_poll(poll_seconds)


@app.command("validate")
def validate(
    blacklist_file: Path | None = typer.Option(
        None,
        "--blacklist-file",
        help="Blacklist file path override (always enforced when present).",
    ),
    exclude_hotkeys: str = typer.Option(
        "",
        "--exclude-hotkeys",
        help="Additional comma-separated hotkeys to exclude for this run.",
    ),
    poll_sec: float | None = typer.Option(
        None,
        "--poll-sec",
        help="Block polling interval in seconds (default from NEXIS_BLOCK_POLL_SEC).",
    ),
    debug: bool = typer.Option(
        False,
        "--debug",
        help="Enable verbose debug logging for validator loop.",
    ),
    specs: str = typer.Option(
        "",
        "--specs",
        help="Comma-separated dataset spec IDs to validate (defaults to NEXIS_VALIDATOR_ENABLED_SPECS).",
    ),
) -> None:
    from .chain.credentials import ReadCredentialCommitmentManager
    import bittensor as bt

    settings = load_settings()
    spec_registry = DatasetSpecRegistry.with_defaults()
    enabled_specs = _resolve_enabled_specs(
        specs.strip() or settings.validator_enabled_specs,
        spec_registry,
    )
    expected_category = settings.dataset_category.strip()
    if not expected_category:
        raise typer.BadParameter("NEXIS_DATASET_CATEGORY must not be empty")
    if settings.validator_category_check_enabled and expected_category != _SUPPORTED_CATEGORY:
        raise typer.BadParameter(
            f"Unsupported NEXIS_DATASET_CATEGORY '{expected_category}'. "
            f"Current validator category checks support only '{_SUPPORTED_CATEGORY}'."
        )
    poll_seconds = settings.block_poll_sec if poll_sec is None else poll_sec
    validator_hotkey = _resolve_hotkey_ss58_from_wallet(settings)
    _configure_logging("INFO", debug=debug)
    is_owner_validator = validator_hotkey == settings.owner_validator_hotkey.strip()

    manager = ReadCredentialCommitmentManager(
        netuid=settings.netuid,
        network=settings.bt_network,
        wallet_name=settings.bt_wallet_name,
        wallet_hotkey=settings.bt_wallet_hotkey,
        wallet_path=settings.bt_wallet_path,
        r2_region=settings.r2_region,
    )
    store_cache: dict[str, R2S3Store] = {}
    committed_payload: dict[str, dict] = {}

    def store_for_hotkey(hotkey: str) -> R2S3Store:
        cached = store_cache.get(hotkey)
        if cached is not None:
            return cached
        creds = manager.build_r2_credentials(committed_payload.get(hotkey), hotkey=hotkey)
        if creds is None:
            raise RuntimeError(f"missing committed read credentials for {hotkey}")
        store = R2S3Store(creds)
        store_cache[hotkey] = store
        return store

    (
        semantic_provider,
        semantic_api_key,
        semantic_model,
        semantic_base_url,
        semantic_route,
    ) = _resolve_llm_runtime(
        settings,
        openai_model=settings.validator_semantic_model,
    )
    logger.info(
        "validator semantic runtime provider=%s model=%s route=%s",
        semantic_provider,
        semantic_model,
        semantic_route,
    )
    if semantic_route == "no_api_key" and settings.validator_semantic_check_enabled:
        logger.warning(
            "validator semantic checks enabled but no API key configured; semantic checker will fail-open"
        )
    semantic_checker = CaptionSemanticChecker(
        enabled=settings.validator_semantic_check_enabled,
        api_key=semantic_api_key,
        model=semantic_model,
        timeout_sec=settings.validator_semantic_timeout_sec,
        max_samples=settings.validator_semantic_max_samples,
        provider=semantic_provider,
        base_url=semantic_base_url,
    )
    (
        category_provider,
        category_api_key,
        category_model,
        category_base_url,
        category_route,
    ) = _resolve_llm_runtime(
        settings,
        openai_model=settings.validator_category_model,
    )
    logger.info(
        "validator category runtime provider=%s model=%s route=%s",
        category_provider,
        category_model,
        category_route,
    )
    if settings.validator_category_check_enabled and category_route == "no_api_key":
        raise typer.BadParameter(
            "NEXIS_VALIDATOR_CATEGORY_CHECK_ENABLED=true requires OPENAI_API_KEY or GEMINI_API_KEY"
        )
    category_checker = NatureCategoryChecker(
        enabled=settings.validator_category_check_enabled,
        api_key=category_api_key,
        timeout_sec=settings.validator_category_timeout_sec,
        max_samples=settings.validator_category_max_samples,
        base_url=category_base_url,
        model=category_model,
    )
    record_info_read_store: R2S3Store | None = None
    record_info_read_creds = _build_shared_bucket_credentials(
        settings=settings,
        account_id=settings.record_info_account_id,
        bucket_name=settings.record_info_bucket,
        read_access_key=settings.record_info_read_access_key,
        read_secret_key=settings.record_info_read_secret_key,
        write_access_key="",
        write_secret_key="",
    )
    if record_info_read_creds is not None:
        record_info_read_creds.validate_account_id()
        record_info_read_store = R2S3Store(record_info_read_creds)
    else:
        logger.warning("record info read credentials/account_id missing; overlap index sync disabled")

    record_info_write_store: R2S3Store | None = None
    if is_owner_validator:
        record_info_write_creds = _build_shared_bucket_credentials(
            settings=settings,
            account_id=settings.record_info_account_id,
            bucket_name=settings.record_info_bucket,
            read_access_key=settings.record_info_read_access_key,
            read_secret_key=settings.record_info_read_secret_key,
            write_access_key=settings.record_info_write_access_key,
            write_secret_key=settings.record_info_write_secret_key,
        )
        if record_info_write_creds is not None:
            record_info_write_creds.validate_account_id()
            record_info_write_store = R2S3Store(record_info_write_creds)
        else:
            logger.warning(
                "owner validator mode active but record info write credentials/account_id are missing"
            )

    validator = ValidatorPipeline(
        store_for_hotkey=store_for_hotkey,
        caption_semantic_checker=semantic_checker,
        category_checker=category_checker,
        expected_category=expected_category,
        source_authenticity_enabled=False,
        spec_registry=spec_registry,
        enabled_specs=enabled_specs,
    )
    reporter: ValidationResultReporter | None = None
    evidence_url = settings.validation_api_url.strip()
    if evidence_url:
        wallet = bt.wallet(
            name=settings.bt_wallet_name,
            hotkey=settings.bt_wallet_hotkey,
            path=str(settings.bt_wallet_path.expanduser()),
        )
        hotkey_signer = getattr(wallet, "hotkey", None)
        if hotkey_signer is None or not hasattr(hotkey_signer, "sign"):
            raise typer.BadParameter("wallet hotkey signer is unavailable for validation API reporting")
        reporter = ValidationResultReporter(
            endpoint_url=evidence_url,
            hotkey_ss58=validator_hotkey,
            hotkey_signer=hotkey_signer,
            timeout_sec=settings.validation_api_timeout_sec,
        )
    try:
        asyncio.run(
            _run_validator_loop(
                settings=settings,
                poll_seconds=poll_seconds,
                enabled_specs=enabled_specs,
                manager=manager,
                store_cache=store_cache,
                committed_payload=committed_payload,
                validator=validator,
                is_owner_validator=is_owner_validator,
                record_info_read_store=record_info_read_store,
                record_info_write_store=record_info_write_store,
                store_for_hotkey=store_for_hotkey,
                blacklist_file=blacklist_file,
                exclude_hotkeys=exclude_hotkeys,
                reporter=reporter,
            )
        )
    except KeyboardInterrupt:
        console.print("validator loop stopped")


@app.command("validate-source-auth")
def validate_source_auth(
    blacklist_file: Path | None = typer.Option(
        None,
        "--blacklist-file",
        help="Blacklist file path override (always enforced when present).",
    ),
    exclude_hotkeys: str = typer.Option(
        "",
        "--exclude-hotkeys",
        help="Additional comma-separated hotkeys to exclude for this run.",
    ),
    poll_sec: float | None = typer.Option(
        None,
        "--poll-sec",
        help="Block polling interval in seconds (default from NEXIS_BLOCK_POLL_SEC).",
    ),
    debug: bool = typer.Option(
        False,
        "--debug",
        help="Enable verbose debug logging for source-auth validator loop.",
    ),
    specs: str = typer.Option(
        "",
        "--specs",
        help="Comma-separated dataset spec IDs to validate (defaults to NEXIS_VALIDATOR_ENABLED_SPECS).",
    ),
) -> None:
    from .chain.credentials import ReadCredentialCommitmentManager
    import bittensor as bt

    settings = load_settings()
    evidence_url = settings.validation_api_url.strip()
    if not evidence_url:
        raise typer.BadParameter(
            "NEXIS_VALIDATION_API_URL is required for validate-source-auth mode"
        )
    spec_registry = DatasetSpecRegistry.with_defaults()
    enabled_specs = _resolve_enabled_specs(
        specs.strip() or settings.validator_enabled_specs,
        spec_registry,
    )
    expected_category = settings.dataset_category.strip()
    if not expected_category:
        raise typer.BadParameter("NEXIS_DATASET_CATEGORY must not be empty")
    poll_seconds = settings.block_poll_sec if poll_sec is None else poll_sec
    validator_hotkey = _resolve_hotkey_ss58_from_wallet(settings)
    _configure_logging("INFO", debug=debug)

    manager = ReadCredentialCommitmentManager(
        netuid=settings.netuid,
        network=settings.bt_network,
        wallet_name=settings.bt_wallet_name,
        wallet_hotkey=settings.bt_wallet_hotkey,
        wallet_path=settings.bt_wallet_path,
        r2_region=settings.r2_region,
    )
    store_cache: dict[str, R2S3Store] = {}
    committed_payload: dict[str, dict] = {}

    def store_for_hotkey(hotkey: str) -> R2S3Store:
        cached = store_cache.get(hotkey)
        if cached is not None:
            return cached
        creds = manager.build_r2_credentials(committed_payload.get(hotkey), hotkey=hotkey)
        if creds is None:
            raise RuntimeError(f"missing committed read credentials for {hotkey}")
        store = R2S3Store(creds)
        store_cache[hotkey] = store
        return store

    record_info_read_store: R2S3Store | None = None
    record_info_read_creds = _build_shared_bucket_credentials(
        settings=settings,
        account_id=settings.record_info_account_id,
        bucket_name=settings.record_info_bucket,
        read_access_key=settings.record_info_read_access_key,
        read_secret_key=settings.record_info_read_secret_key,
        write_access_key="",
        write_secret_key="",
    )
    if record_info_read_creds is not None:
        record_info_read_creds.validate_account_id()
        record_info_read_store = R2S3Store(record_info_read_creds)
    else:
        logger.warning("record info read credentials/account_id missing; overlap index sync disabled")

    validator = ValidatorPipeline(
        store_for_hotkey=store_for_hotkey,
        caption_semantic_checker=None,
        expected_category=expected_category,
        source_authenticity_enabled=True,
        source_auth_only=True,
        spec_registry=spec_registry,
        enabled_specs=enabled_specs,
    )
    wallet = bt.wallet(
        name=settings.bt_wallet_name,
        hotkey=settings.bt_wallet_hotkey,
        path=str(settings.bt_wallet_path.expanduser()),
    )
    hotkey_signer = getattr(wallet, "hotkey", None)
    if hotkey_signer is None or not hasattr(hotkey_signer, "sign"):
        raise typer.BadParameter("wallet hotkey signer is unavailable for validation API reporting")
    reporter = ValidationResultReporter(
        endpoint_url=evidence_url,
        hotkey_ss58=validator_hotkey,
        hotkey_signer=hotkey_signer,
        timeout_sec=settings.validation_api_timeout_sec,
    )
    try:
        asyncio.run(
            _run_source_auth_validator_loop(
                settings=settings,
                poll_seconds=poll_seconds,
                enabled_specs=enabled_specs,
                manager=manager,
                store_cache=store_cache,
                committed_payload=committed_payload,
                validator=validator,
                blacklist_file=blacklist_file,
                exclude_hotkeys=exclude_hotkeys,
                reporter=reporter,
                record_info_read_store=record_info_read_store,
            )
        )
    except KeyboardInterrupt:
        console.print("source-auth validator loop stopped")


@app.command("sync-owner-datasets")
def sync_owner_datasets(
    blacklist_file: Path | None = typer.Option(
        None,
        "--blacklist-file",
        help="Blacklist file path override (always enforced when present).",
    ),
    exclude_hotkeys: str = typer.Option(
        "",
        "--exclude-hotkeys",
        help="Additional comma-separated hotkeys to exclude for this run.",
    ),
    poll_sec: float = typer.Option(
        60.0,
        "--poll-sec",
        help="Polling interval in seconds for metadata scan.",
    ),
    debug: bool = typer.Option(
        False,
        "--debug",
        help="Enable verbose debug logging for owner sync worker loop.",
    ),
) -> None:
    from .chain.credentials import ReadCredentialCommitmentManager

    settings = load_settings()
    _configure_logging("INFO", debug=debug)
    manager = ReadCredentialCommitmentManager(
        netuid=settings.netuid,
        network=settings.bt_network,
        wallet_name=settings.bt_wallet_name,
        wallet_hotkey=settings.bt_wallet_hotkey,
        wallet_path=settings.bt_wallet_path,
        r2_region=settings.r2_region,
    )
    store_cache: dict[str, R2S3Store] = {}
    committed_payload: dict[str, dict] = {}

    owner_db_creds = _build_shared_bucket_credentials(
        settings=settings,
        account_id=settings.owner_db_account_id,
        bucket_name=settings.owner_db_bucket,
        read_access_key=settings.owner_db_read_access_key,
        read_secret_key=settings.owner_db_read_secret_key,
        write_access_key=settings.owner_db_write_access_key,
        write_secret_key=settings.owner_db_write_secret_key,
    )
    if owner_db_creds is None:
        raise typer.BadParameter("owner dataset bucket credentials and NEXIS_OWNER_DB_ACCOUNT_ID are required")
    owner_db_creds.validate_account_id()
    owner_db_store = R2S3Store(owner_db_creds)

    record_info_creds = _build_shared_bucket_credentials(
        settings=settings,
        account_id=settings.record_info_account_id,
        bucket_name=settings.record_info_bucket,
        read_access_key=settings.record_info_read_access_key,
        read_secret_key=settings.record_info_read_secret_key,
        write_access_key=settings.record_info_write_access_key,
        write_secret_key=settings.record_info_write_secret_key,
    )
    if record_info_creds is None:
        raise typer.BadParameter(
            "record-info bucket read credentials and NEXIS_RECORD_INFO_ACCOUNT_ID are required"
        )
    record_info_creds.validate_account_id()
    record_info_read_store = R2S3Store(record_info_creds)

    spec_registry = DatasetSpecRegistry.with_defaults()

    def store_for_hotkey(hotkey: str) -> R2S3Store:
        cached = store_cache.get(hotkey)
        if cached is not None:
            return cached
        creds = manager.build_r2_credentials(committed_payload.get(hotkey), hotkey=hotkey)
        if creds is None:
            raise RuntimeError(f"missing committed read credentials for {hotkey}")
        store = R2S3Store(creds)
        store_cache[hotkey] = store
        return store

    try:
        asyncio.run(
            _run_owner_sync_worker_loop(
                settings=settings,
                poll_seconds=max(poll_sec, 1.0),
                manager=manager,
                store_cache=store_cache,
                committed_payload=committed_payload,
                owner_db_store=owner_db_store,
                record_info_read_store=record_info_read_store,
                store_for_hotkey=store_for_hotkey,
                blacklist_file=blacklist_file,
                exclude_hotkeys=exclude_hotkeys,
                spec_registry=spec_registry,
            )
        )
    except KeyboardInterrupt:
        console.print("owner dataset sync worker stopped")


async def _run_validator_loop(
    *,
    settings: Settings,
    poll_seconds: float,
    enabled_specs: list[str],
    manager: "ReadCredentialCommitmentManager",
    store_cache: dict[str, R2S3Store],
    committed_payload: dict[str, dict],
    validator: ValidatorPipeline,
    is_owner_validator: bool,
    record_info_read_store: R2S3Store | None,
    record_info_write_store: R2S3Store | None,
    store_for_hotkey: Callable[[str], R2S3Store],
    blacklist_file: Path | None,
    exclude_hotkeys: str,
    reporter: ValidationResultReporter | None = None,
) -> None:
    async with _open_subtensor(settings.bt_network) as subtensor:
        start_block = await fetch_current_block_async(
            network=settings.bt_network,
            subtensor=subtensor,
        )
        initial_latest_eligible = _latest_eligible_validation_interval_start(start_block)
        if initial_latest_eligible is None:
            last_validated_interval_start = -INTERVAL_LENGTH_BLOCKS
        else:
            last_validated_interval_start = initial_latest_eligible - INTERVAL_LENGTH_BLOCKS
        start_weight_epoch = start_block // WEIGHT_SUBMISSION_INTERVAL_BLOCKS
        last_submitted_weight_epoch = (
            start_weight_epoch - 1
            if start_block % WEIGHT_SUBMISSION_INTERVAL_BLOCKS == 0
            else start_weight_epoch
        )
        last_failed_weight_epoch: int | None = None
        weight_failure_count = 0
        next_weight_retry_ts = 0.0
        epoch_score_totals: dict[str, float] = defaultdict(float)
        frozen_epoch_weights: dict[str, float] | None = None
        frozen_epoch: int | None = None
        console.print(
            "validator loop started: "
            f"validate_every={INTERVAL_LENGTH_BLOCKS} blocks, "
            f"set_weights_every={WEIGHT_SUBMISSION_INTERVAL_BLOCKS} blocks, "
            f"poll={poll_seconds:.1f}s, reserve={UPLOAD_DEADLINE_RESERVED_BLOCKS} blocks"
        )
        logger.info(
            "validator loop initialized validate_blocks=%d weight_blocks=%d poll_sec=%.1f reserve_blocks=%d specs=%s",
            INTERVAL_LENGTH_BLOCKS,
            WEIGHT_SUBMISSION_INTERVAL_BLOCKS,
            poll_seconds,
            UPLOAD_DEADLINE_RESERVED_BLOCKS,
            ",".join(enabled_specs),
        )
        latest_result_url = _resolve_latest_result_url(settings.validation_api_url)
        if latest_result_url:
            logger.info("latest-result endpoint resolved to %s", latest_result_url)

        while True:
            try:
                current_block = await fetch_current_block_async(
                    network=settings.bt_network,
                    subtensor=subtensor,
                )
                latest_eligible_interval_start = _latest_eligible_validation_interval_start(current_block)
                next_interval_start = last_validated_interval_start + INTERVAL_LENGTH_BLOCKS
                logger.debug(
                    "validator tick current_block=%d eligible_start=%s next_interval=%d last_validated=%d",
                    current_block,
                    str(latest_eligible_interval_start),
                    next_interval_start,
                    last_validated_interval_start,
                )

                if latest_eligible_interval_start is not None:
                    if next_interval_start > latest_eligible_interval_start:
                        logger.info(
                            "waiting for next interval to be eligible, current_block=%d, next_interval=(%d, %d)",
                            current_block,
                            next_interval_start,
                            next_interval_start + INTERVAL_LENGTH_BLOCKS,
                        )
                        await _sleep_poll(poll_seconds)
                        continue
                    store_cache.clear()
                    hotkeys, payload = await _fetch_hotkeys_with_commitments(
                        settings=settings,
                        manager=manager,
                        blacklist_file=blacklist_file,
                        exclude_hotkeys=exclude_hotkeys,
                        subtensor=subtensor,
                    )
                    committed_payload.clear()
                    committed_payload.update(payload)
                    while next_interval_start <= latest_eligible_interval_start:
                        if hotkeys:
                            logger.info(
                                "validating interval=%s miners=%d",
                                _interval_label(next_interval_start),
                                len(hotkeys),
                            )
                            invalid_hotkeys_for_interval: set[str] = set()
                            if reporter is not None:
                                invalid_hotkeys_for_interval = set(
                                    await reporter.fetch_invalid_hotkeys(
                                        interval_id=next_interval_start
                                    )
                                )
                                if invalid_hotkeys_for_interval:
                                    logger.info(
                                        "api invalid hotkeys interval=%s count=%d",
                                        _interval_label(next_interval_start),
                                        len(invalid_hotkeys_for_interval),
                                    )
                            global_record_index, record_info_loaded = await _load_record_info_snapshot(
                                record_info_store=record_info_read_store,
                                object_key=settings.record_info_object_key,
                                workdir=settings.workdir / "validator",
                            )
                            decisions, interval_weights = await validator.validate_interval(
                                candidate_hotkeys=hotkeys,
                                interval_id=next_interval_start,
                                workdir=settings.workdir / "validator",
                                global_record_index=global_record_index,
                                invalid_hotkeys=invalid_hotkeys_for_interval,
                            )
                            if reporter is not None:
                                await reporter.report_interval(
                                    interval_id=next_interval_start,
                                    decisions=decisions,
                                )
                            for decision in decisions:
                                if not decision.accepted:
                                    continue
                                epoch_score_totals[decision.miner_hotkey] += (
                                    validator.weight_computer.score_from_sample_count(decision.record_count)
                                )
                            # New score material was added; refresh frozen snapshot on next submit.
                            frozen_epoch_weights = None
                            frozen_epoch = None
                            payload_json = [d.model_dump(mode="json") for d in decisions]
                            console.print(f"validated interval {_interval_label(next_interval_start)}")
                            console.print_json(json.dumps(payload_json))
                            console.print("interval weights:")
                            console.print_json(json.dumps(interval_weights))
                            console.print("epoch accumulated scores:")
                            console.print_json(json.dumps(dict(epoch_score_totals)))
                            logger.debug(
                                "interval=%s decisions=%d interval_weight_entries=%d epoch_score_entries=%d",
                                _interval_label(next_interval_start),
                                len(decisions),
                                len(interval_weights),
                                len(epoch_score_totals),
                            )
                            if is_owner_validator:
                                try:
                                    published_rows_by_hotkey = await _upload_validated_datasets_to_owner_bucket(
                                        owner_store=record_info_write_store,
                                        source_store_for_hotkey=store_for_hotkey,
                                        validator=validator,
                                        decisions=decisions,
                                        interval_id=next_interval_start,
                                        workdir=settings.workdir / "validator",
                                    )
                                    for rows in published_rows_by_hotkey.values():
                                        if not rows:
                                            continue
                                        _merge_records_into_index(
                                            record_index=global_record_index,
                                            records=rows,
                                        )
                                    if record_info_loaded:
                                        await _upload_record_info_snapshot(
                                            record_info_store=record_info_write_store,
                                            object_key=settings.record_info_object_key,
                                            workdir=settings.workdir / "validator",
                                            record_index=global_record_index,
                                        )
                                    else:
                                        logger.warning(
                                            "skipping record-info write interval=%s reason=record_info_read_untrusted",
                                            _interval_label(next_interval_start),
                                        )
                                except Exception as exc:
                                    logger.exception(
                                        "owner sync failed for interval=%s: %s",
                                        _interval_label(next_interval_start),
                                        exc,
                                    )
                        else:
                            console.print(
                                "no miners with committed read credentials; "
                                f"interval {_interval_label(next_interval_start)} skipped"
                            )

                        last_validated_interval_start = next_interval_start
                        next_interval_start += INTERVAL_LENGTH_BLOCKS

                current_weight_epoch = current_block // WEIGHT_SUBMISSION_INTERVAL_BLOCKS
                should_try_submit = (
                    current_weight_epoch > last_submitted_weight_epoch
                    and time.monotonic() >= next_weight_retry_ts
                )
                logger.debug(
                    "weight submission check epoch=%d last_submitted=%d should_try=%s",
                    current_weight_epoch,
                    last_submitted_weight_epoch,
                    str(should_try_submit),
                )
                if should_try_submit:
                    submission_block = current_weight_epoch * WEIGHT_SUBMISSION_INTERVAL_BLOCKS
                    snapshot: dict[str, Any]
                    if latest_result_url:
                        try:
                            snapshot = await fetch_latest_result_snapshot(
                                url=latest_result_url,
                                timeout_sec=settings.latest_result_timeout_sec,
                            )
                        except RuntimeError as exc:
                            logger.warning(
                                "latest-result fetch failed epoch=%d url=%s: %s; "
                                "continuing with empty API decisions for this weight pass",
                                current_weight_epoch,
                                latest_result_url,
                                exc,
                            )
                            snapshot = {"decisions": []}
                    else:
                        snapshot = {"decisions": []}

                    decisions_raw = snapshot.get("decisions")
                    if not isinstance(decisions_raw, list):
                        decisions_raw = []
                    decisions: list[dict[str, Any]] = [
                        row for row in decisions_raw if isinstance(row, dict)
                    ]
                    score_totals = compute_score_totals_from_decisions(
                        decisions=decisions,
                        interval_start=submission_block - 1500 - INTERVAL_LENGTH_BLOCKS,
                        interval_end=submission_block,
                        weight_computer=validator.weight_computer,
                    )
                    if not score_totals:
                        logger.info(
                            "no valid miner hotkeys in epoch=%d; forcing burn fallback to UID0",
                            current_weight_epoch,
                        )
                        frozen_epoch_weights = {}
                    else:
                        frozen_epoch_weights = validator.weight_computer.compute_weights_from_totals(dict(score_totals))
                    current_interval_id = _current_interval_start(current_block)
                    invalid_for_submission: set[str] = set()
                    if reporter is not None:
                        invalid_for_submission = set(
                            await reporter.fetch_invalid_hotkeys(interval_id=current_interval_id)
                        )
                    if invalid_for_submission:
                        for hotkey in invalid_for_submission:
                            if hotkey in frozen_epoch_weights:
                                frozen_epoch_weights[hotkey] = 0.0
                        frozen_epoch_weights = validator.weight_computer.normalize_weights(
                            frozen_epoch_weights
                        )
                        logger.info(
                            "zeroed invalid hotkeys before set_weights count=%d",
                            len(invalid_for_submission),
                        )
                    logger.info("frozen_epoch_weights: %s", frozen_epoch_weights)

                    submission = await submit_weights_to_chain_async(
                        netuid=settings.netuid,
                        network=settings.bt_network,
                        wallet_name=settings.bt_wallet_name,
                        wallet_hotkey=settings.bt_wallet_hotkey,
                        wallet_path=settings.bt_wallet_path,
                        weights_by_hotkey=frozen_epoch_weights,
                        subtensor=subtensor,
                    )
                    if submission.unknown_hotkeys:
                        console.print(
                            "weights skipped for unknown metagraph hotkeys: "
                            f"{len(submission.unknown_hotkeys)}"
                        )
                    if submission.submitted:
                        console.print(f"set_weights submitted at block boundary {submission_block}")
                        non_zero_weights = [
                            (hotkey, weight)
                            for hotkey, weight in frozen_epoch_weights.items()
                            if weight > 0
                        ]
                        logger.info(
                            "set_weights submitted block=%d weights=%s",
                            submission_block,
                            json.dumps(non_zero_weights, indent=2)
                            if non_zero_weights
                            else "no valid miners in epoch, burning to UID 0",
                        )
                        last_submitted_weight_epoch = current_weight_epoch
                        last_failed_weight_epoch = None
                        weight_failure_count = 0
                        next_weight_retry_ts = 0.0
                        epoch_score_totals = defaultdict(float)
                        frozen_epoch_weights = None
                        frozen_epoch = None
                    else:
                        if last_failed_weight_epoch != current_weight_epoch:
                            weight_failure_count = 0
                        last_failed_weight_epoch = current_weight_epoch
                        weight_failure_count += 1
                        backoff = _weight_retry_backoff_sec(weight_failure_count)
                        next_weight_retry_ts = time.monotonic() + float(backoff)
                        logger.error("set_weights failed: %s", submission.reason)
            except Exception as exc:
                logger.exception("validator loop iteration failed: %s", exc)

            await _sleep_poll(poll_seconds)


async def _run_source_auth_validator_loop(
    *,
    settings: Settings,
    poll_seconds: float,
    enabled_specs: list[str],
    manager: "ReadCredentialCommitmentManager",
    store_cache: dict[str, R2S3Store],
    committed_payload: dict[str, dict],
    validator: ValidatorPipeline,
    blacklist_file: Path | None,
    exclude_hotkeys: str,
    reporter: ValidationResultReporter,
    record_info_read_store: R2S3Store | None,
) -> None:
    async with _open_subtensor(settings.bt_network) as subtensor:
        start_block = await fetch_current_block_async(
            network=settings.bt_network,
            subtensor=subtensor,
        )
        initial_latest_eligible = _latest_eligible_validation_interval_start(start_block)
        if initial_latest_eligible is None:
            last_validated_interval_start = -INTERVAL_LENGTH_BLOCKS
        else:
            last_validated_interval_start = initial_latest_eligible - INTERVAL_LENGTH_BLOCKS
        logger.info(
            "source-auth validator loop initialized validate_blocks=%d poll_sec=%.1f specs=%s",
            INTERVAL_LENGTH_BLOCKS,
            poll_seconds,
            ",".join(enabled_specs),
        )
        while True:
            try:
                current_block = await fetch_current_block_async(
                    network=settings.bt_network,
                    subtensor=subtensor,
                )
                latest_eligible_interval_start = _latest_eligible_validation_interval_start(current_block)
                next_interval_start = last_validated_interval_start + INTERVAL_LENGTH_BLOCKS
                if latest_eligible_interval_start is not None:
                    if next_interval_start > latest_eligible_interval_start:
                        logger.info(
                            "waiting for next interval to be eligible, current_block=%d, next_interval=(%d, %d)",
                            current_block,
                            next_interval_start,
                            next_interval_start + INTERVAL_LENGTH_BLOCKS,
                        )
                        await _sleep_poll(poll_seconds)
                        continue
                    store_cache.clear()
                    hotkeys, payload = await _fetch_hotkeys_with_commitments(
                        settings=settings,
                        manager=manager,
                        blacklist_file=blacklist_file,
                        exclude_hotkeys=exclude_hotkeys,
                        subtensor=subtensor,
                    )
                    committed_payload.clear()
                    committed_payload.update(payload)
                    while next_interval_start <= latest_eligible_interval_start:
                        if hotkeys:
                            invalid_hotkeys_for_interval = set(
                                await reporter.fetch_invalid_hotkeys(interval_id=next_interval_start)
                            )
                            global_record_index, _record_info_loaded = await _load_record_info_snapshot(
                                record_info_store=record_info_read_store,
                                object_key=settings.record_info_object_key,
                                workdir=settings.workdir / "validator-source-auth",
                            )
                            decisions, _weights = await validator.validate_interval(
                                candidate_hotkeys=hotkeys,
                                interval_id=next_interval_start,
                                workdir=settings.workdir / "validator-source-auth",
                                global_record_index=global_record_index,
                                invalid_hotkeys=invalid_hotkeys_for_interval,
                            )
                            invalid_hotkeys = sorted(
                                {
                                    decision.miner_hotkey
                                    for decision in decisions
                                    if any(
                                        failure.startswith("source_frame_")
                                        for failure in decision.failures
                                    )
                                }
                            )
                            if invalid_hotkeys:
                                await reporter.post_invalid_hotkeys(
                                    interval_id=next_interval_start,
                                    invalid_hotkeys=invalid_hotkeys,
                                )
                                logger.info(
                                    "source-auth invalid hotkeys posted interval=%s count=%d",
                                    _interval_label(next_interval_start),
                                    len(invalid_hotkeys),
                                )
                        last_validated_interval_start = next_interval_start
                        next_interval_start += INTERVAL_LENGTH_BLOCKS
            except Exception as exc:
                logger.exception("source-auth validator loop iteration failed: %s", exc)
            await _sleep_poll(poll_seconds)


async def _run_owner_sync_worker_loop(
    *,
    settings: Settings,
    poll_seconds: float,
    manager: "ReadCredentialCommitmentManager",
    store_cache: dict[str, R2S3Store],
    committed_payload: dict[str, dict],
    owner_db_store: R2S3Store,
    record_info_read_store: R2S3Store,
    store_for_hotkey: Callable[[str], R2S3Store],
    blacklist_file: Path | None,
    exclude_hotkeys: str,
    spec_registry: DatasetSpecRegistry,
) -> None:
    async with _open_subtensor(settings.bt_network) as subtensor:
        logger.info("owner dataset sync worker started poll_sec=%.1f", poll_seconds)
        while True:
            try:
                store_cache.clear()
                _hotkeys, payload = await _fetch_hotkeys_with_commitments(
                    settings=settings,
                    manager=manager,
                    blacklist_file=blacklist_file,
                    exclude_hotkeys=exclude_hotkeys,
                    subtensor=subtensor,
                )
                committed_payload.clear()
                committed_payload.update(payload)
                summary = await _sync_record_info_to_owner_bucket(
                    record_info_store=record_info_read_store,
                    owner_store=owner_db_store,
                    source_store_for_hotkey=store_for_hotkey,
                    workdir=settings.workdir / "owner-sync-worker",
                    spec_registry=spec_registry,
                )
                logger.info(
                    "owner dataset sync iteration processed=%d copied=%d skipped=%d",
                    summary["processed"],
                    summary["copied"],
                    summary["skipped"],
                )
            except Exception as exc:
                logger.exception("owner dataset sync worker iteration failed: %s", exc)
            await _sleep_poll(poll_seconds)


def main() -> None:
    app()


if __name__ == "__main__":
    main()

