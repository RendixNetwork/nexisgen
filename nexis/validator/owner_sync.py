"""Owner validator sync helpers for shared Hippius buckets."""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
from pathlib import Path
from pathlib import PurePosixPath
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from ..hash_utils import sha256_file
from ..models import ClipRecord, ValidationDecision
from ..serialization import (
    read_dataset_parquet_as_model,
    read_manifest,
    write_dataset_parquet,
    write_manifest,
)
from ..specs import DEFAULT_SPEC_ID
from ..specs import DatasetSpecRegistry
from .pipeline import ValidatorPipeline

logger = logging.getLogger(__name__)


def _run_async(coro: Any) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    close_coro = getattr(coro, "close", None)
    if callable(close_coro):
        close_coro()
    raise RuntimeError("Synchronous owner sync helper cannot run inside an active event loop")


def normalize_relative_uri(value: str) -> str | None:
    text = value.strip().lstrip("/")
    if not text:
        return None
    parts = PurePosixPath(text).parts
    if any(part in {"", ".", ".."} for part in parts):
        return None
    return "/".join(parts)


def parse_record_info(raw: str) -> dict[str, list[float]]:
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    entries: dict[str, Any] | None = None
    if isinstance(payload, dict):
        if isinstance(payload.get(DEFAULT_SPEC_ID), dict):
            entries = payload[DEFAULT_SPEC_ID]
        elif isinstance(payload.get("entries"), dict):
            entries = payload["entries"]
        else:
            entries = payload
    if not isinstance(entries, dict):
        return {}
    parsed: dict[str, list[float]] = {}
    for source_key, values in entries.items():
        if not isinstance(source_key, str) or not isinstance(values, list):
            continue
        normalized_source_url = _normalize_record_info_source_key(source_key)
        if normalized_source_url is None:
            continue
        starts: list[float] = []
        for item in values:
            try:
                starts.append(float(item))
            except Exception:
                continue
        if starts:
            existing = parsed.setdefault(normalized_source_url, [])
            existing.extend(starts)
    for source_url, values in parsed.items():
        parsed[source_url] = sorted(set(values))
    return parsed


def canonical_source_key_from_url(source_video_url: str) -> str:
    return _normalize_record_info_source_key(source_video_url) or source_video_url.strip()


def _normalize_record_info_source_key(value: str) -> str | None:
    text = value.strip()
    if not text:
        return None
    if text.startswith(f"{DEFAULT_SPEC_ID}:"):
        text = text.split(":", maxsplit=1)[1].strip()
        if not text:
            return None
    parsed = urlparse(text)
    host = (parsed.hostname or "").lower()
    if host:
        if host == "youtu.be":
            video_id = parsed.path.strip("/")
            if video_id:
                return f"https://www.youtube.com/watch?v={video_id}"
        if host == "youtube.com" or host.endswith(".youtube.com"):
            query = parse_qs(parsed.query)
            values = query.get("v", [])
            if values and values[0].strip():
                return f"https://www.youtube.com/watch?v={values[0].strip()}"
            parts = [part for part in parsed.path.split("/") if part]
            if len(parts) >= 2 and parts[0] in {"shorts", "embed", "v"} and parts[1].strip():
                return f"https://www.youtube.com/watch?v={parts[1].strip()}"
        return text
    return f"https://www.youtube.com/watch?v={text}"


def serialize_record_info(record_index: dict[str, list[float]]) -> str:
    payload = {
        f"{DEFAULT_SPEC_ID}": {
            source_url: [f"{value:.3f}" for value in sorted(values)]
            for source_url, values in sorted(record_index.items())
        },
    }
    return json.dumps(payload, indent=2)


def load_record_info_snapshot(
    *,
    record_info_store: Any | None,
    object_key: str,
    workdir: Path,
) -> dict[str, list[float]]:
    return _run_async(
        load_record_info_snapshot_async(
            record_info_store=record_info_store,
            object_key=object_key,
            workdir=workdir,
        )
    )


async def load_record_info_snapshot_async(
    *,
    record_info_store: Any | None,
    object_key: str,
    workdir: Path,
) -> dict[str, list[float]]:
    if record_info_store is None:
        return {}
    local = workdir / "record-info" / "snapshot.json"
    ok = await record_info_store.download_file(object_key, local)
    if not ok or not local.exists():
        return {}
    return parse_record_info(local.read_text(encoding="utf-8"))


def merge_records_into_index(
    *,
    record_index: dict[str, list[float]],
    records: list[ClipRecord],
) -> None:
    for row in records:
        source_url = row.source_video_url
        values = record_index.setdefault(source_url, [])
        values.append(float(row.clip_start_sec))
    for source_url, values in record_index.items():
        deduped = sorted(set(round(value, 3) for value in values))
        record_index[source_url] = deduped


def upload_record_info_snapshot(
    *,
    record_info_store: Any | None,
    object_key: str,
    workdir: Path,
    record_index: dict[str, list[float]],
) -> None:
    _run_async(
        upload_record_info_snapshot_async(
            record_info_store=record_info_store,
            object_key=object_key,
            workdir=workdir,
            record_index=record_index,
        )
    )


async def upload_record_info_snapshot_async(
    *,
    record_info_store: Any | None,
    object_key: str,
    workdir: Path,
    record_index: dict[str, list[float]],
) -> None:
    if record_info_store is None:
        return
    local = workdir / "record-info" / "snapshot.json"
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_text(serialize_record_info(record_index), encoding="utf-8")
    await record_info_store.upload_file(object_key, local, use_write=True)


def upload_validated_datasets_to_owner_bucket(
    *,
    owner_store: Any | None,
    source_store_for_hotkey: Callable[[str], Any],
    validator: ValidatorPipeline,
    decisions: list[ValidationDecision],
    interval_id: int,
    workdir: Path,
) -> dict[str, list[ClipRecord]]:
    return _run_async(
        upload_validated_datasets_to_owner_bucket_async(
            owner_store=owner_store,
            source_store_for_hotkey=source_store_for_hotkey,
            validator=validator,
            decisions=decisions,
            interval_id=interval_id,
            workdir=workdir,
        )
    )


async def upload_validated_datasets_to_owner_bucket_async(
    *,
    owner_store: Any | None,
    source_store_for_hotkey: Callable[[str], Any],
    validator: ValidatorPipeline,
    decisions: list[ValidationDecision],
    interval_id: int,
    workdir: Path,
) -> dict[str, list[ClipRecord]]:
    """Upload metadata-only interval bundles to the record-info bucket.

    Despite the historical function name, this now publishes only:
    - {interval_id}/{hotkey}/dataset.parquet
    - {interval_id}/{hotkey}/manifest.json
    """
    published_rows_by_hotkey: dict[str, list[ClipRecord]] = {}
    _ = source_store_for_hotkey
    if owner_store is None:
        return published_rows_by_hotkey
    artifacts = validator.last_interval_artifacts
    if artifacts.interval_id != interval_id:
        return published_rows_by_hotkey
    for decision in decisions:
        if not decision.accepted:
            continue
        hotkey = decision.miner_hotkey
        records = artifacts.records_by_hotkey.get(hotkey, [])
        manifest = artifacts.manifests_by_hotkey.get(hotkey)
        if manifest is None:
            continue

        out_dir = workdir / "owner-upload" / str(interval_id) / hotkey
        out_dir.mkdir(parents=True, exist_ok=True)
        key_prefix = f"{interval_id}/{hotkey}"
        try:
            if not records:
                continue

            dataset_path = out_dir / "dataset.parquet"
            manifest_path = out_dir / "manifest.json"
            write_dataset_parquet(records, dataset_path)
            published_manifest = manifest.model_copy(deep=True)
            published_manifest.record_count = len(records)
            published_manifest.dataset_sha256 = sha256_file(dataset_path)
            write_manifest(published_manifest, manifest_path)

            await owner_store.upload_file(
                f"{key_prefix}/dataset.parquet",
                dataset_path,
                use_write=True,
            )
            await owner_store.upload_file(
                f"{key_prefix}/manifest.json",
                manifest_path,
                use_write=True,
            )
            published_rows_by_hotkey[hotkey] = list(records)
        finally:
            _cleanup_owner_upload_workdir(out_dir)
    return published_rows_by_hotkey


async def sync_record_info_to_owner_bucket_async(
    *,
    record_info_store: Any | None,
    owner_store: Any | None,
    source_store_for_hotkey: Callable[[str], Any],
    workdir: Path,
    spec_registry: DatasetSpecRegistry | None = None,
) -> dict[str, int]:
    """Copy metadata-published bundles into owner dataset bucket with assets.

    Scans record-info bucket for {interval}/{hotkey}/manifest.json and mirrors to owner bucket.
    """
    summary = {"processed": 0, "copied": 0, "skipped": 0}
    if record_info_store is None or owner_store is None:
        return summary
    keys = await record_info_store.list_prefix("")
    targets = _discover_manifest_targets(keys)
    registry = spec_registry or DatasetSpecRegistry.with_defaults()
    for interval_id, hotkey in sorted(targets):
        summary["processed"] += 1
        marker_key = f"{interval_id}/{hotkey}/copied_to_owner.marker"
        if await record_info_store.object_exists(marker_key):
            summary["skipped"] += 1
            continue
        copied = await _copy_metadata_bundle_to_owner_bucket(
            interval_id=interval_id,
            hotkey=hotkey,
            record_info_store=record_info_store,
            owner_store=owner_store,
            source_store_for_hotkey=source_store_for_hotkey,
            workdir=workdir,
            spec_registry=registry,
        )
        if copied:
            marker_local = workdir / "owner-sync-worker" / str(interval_id) / hotkey / ".copied"
            marker_local.parent.mkdir(parents=True, exist_ok=True)
            marker_local.write_text("ok\n", encoding="utf-8")
            await record_info_store.upload_file(marker_key, marker_local, use_write=True)
            summary["copied"] += 1
        else:
            summary["skipped"] += 1
    return summary


def _discover_manifest_targets(keys: list[str]) -> set[tuple[int, str]]:
    targets: set[tuple[int, str]] = set()
    for key in keys:
        parts = key.strip("/").split("/")
        if len(parts) != 3:
            continue
        if parts[2] != "manifest.json":
            continue
        try:
            interval_id = int(parts[0])
        except ValueError:
            continue
        hotkey = parts[1].strip()
        if not hotkey:
            continue
        targets.add((interval_id, hotkey))
    return targets


async def _copy_metadata_bundle_to_owner_bucket(
    *,
    interval_id: int,
    hotkey: str,
    record_info_store: Any,
    owner_store: Any,
    source_store_for_hotkey: Callable[[str], Any],
    workdir: Path,
    spec_registry: DatasetSpecRegistry,
) -> bool:
    base_dir = workdir / "owner-sync-worker" / str(interval_id) / hotkey
    key_prefix = f"{interval_id}/{hotkey}"
    metadata_manifest = base_dir / "manifest.json"
    metadata_dataset = base_dir / "dataset.parquet"
    try:
        ok_manifest = await record_info_store.download_file(f"{key_prefix}/manifest.json", metadata_manifest)
        ok_dataset = await record_info_store.download_file(f"{key_prefix}/dataset.parquet", metadata_dataset)
        if not ok_manifest or not ok_dataset:
            return False
        manifest = read_manifest(metadata_manifest)
        spec = spec_registry.get(manifest.spec_id)
        records = read_dataset_parquet_as_model(metadata_dataset, spec.row_model)
        if not records:
            return False
        source_store = source_store_for_hotkey(hotkey)
        publishable_rows: list[ClipRecord] = []
        for row in records:
            pending_assets: list[tuple[str, Path]] = []
            row_asset_missing = False
            for relative_uri in (row.clip_uri, row.first_frame_uri):
                safe_uri = normalize_relative_uri(relative_uri)
                if safe_uri is None:
                    row_asset_missing = True
                    break
                src_key = f"{interval_id}/{safe_uri}"
                local_asset = base_dir / "assets" / safe_uri
                ok = await source_store.download_file(src_key, local_asset)
                if not ok:
                    row_asset_missing = True
                    break
                expected_sha = row.clip_sha256 if relative_uri == row.clip_uri else row.first_frame_sha256
                if sha256_file(local_asset) != expected_sha:
                    row_asset_missing = True
                    break
                pending_assets.append((safe_uri, local_asset))
            if row_asset_missing:
                continue
            for safe_uri, local_asset in pending_assets:
                await owner_store.upload_file(
                    f"{key_prefix}/{safe_uri}",
                    local_asset,
                    use_write=True,
                )
            publishable_rows.append(row)
        if not publishable_rows:
            return False
        dataset_path = base_dir / "owner-dataset.parquet"
        manifest_path = base_dir / "owner-manifest.json"
        write_dataset_parquet(publishable_rows, dataset_path)
        published_manifest = manifest.model_copy(deep=True)
        published_manifest.record_count = len(publishable_rows)
        published_manifest.dataset_sha256 = sha256_file(dataset_path)
        write_manifest(published_manifest, manifest_path)
        await owner_store.upload_file(
            f"{key_prefix}/dataset.parquet",
            dataset_path,
            use_write=True,
        )
        await owner_store.upload_file(
            f"{key_prefix}/manifest.json",
            manifest_path,
            use_write=True,
        )
        return True
    except Exception as exc:
        logger.warning(
            "owner sync worker failed interval=%d hotkey=%s error=%s",
            interval_id,
            hotkey,
            exc,
        )
        return False
    finally:
        _cleanup_owner_upload_workdir(base_dir)


def _cleanup_owner_upload_workdir(out_dir: Path) -> None:
    try:
        if out_dir.exists():
            shutil.rmtree(out_dir)
        interval_dir = out_dir.parent
        try:
            interval_dir.rmdir()
        except OSError:
            pass
        try:
            interval_dir.parent.rmdir()
        except OSError:
            pass
    except Exception as exc:
        logger.warning("failed to remove owner upload cache dir=%s error=%s", out_dir, exc)
