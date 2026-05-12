"""Strict dataset validation for v2 protocol (spec + global overlap)."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from ..hash_utils import sha256_file
from ..miner.youtube import probe_video
from ..models import ClipRecord, IntervalManifest
from ..protocol import (
    CLIP_DURATION_SEC,
    CLIP_DURATION_TOLERANCE_SEC,
    FPS_TOLERANCE,
    GLOBAL_OVERLAP_REJECT_THRESHOLD,
    OVERLAP_WINDOW_SEC,
    SAMPLE_COUNT,
    TARGET_FPS,
    TARGET_HEIGHT,
    TARGET_NUM_FRAMES,
    TARGET_WIDTH,
)
from ..serialization import read_dataset_parquet, read_manifest

logger = logging.getLogger(__name__)


@dataclass
class DatasetCheckOutcome:
    accepted: bool
    miner_hotkey: str
    interval_id: int
    record_count: int = 0
    global_overlap_count: int = 0
    failures: list[str] = field(default_factory=list)
    notes: dict[str, Any] = field(default_factory=dict)


def canonical_source_key(url: str) -> str:
    parsed = urlparse(url.strip())
    host = (parsed.hostname or "").lower()
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
    return url.strip()


def _check_record_specs(record: ClipRecord) -> str | None:
    if record.width != TARGET_WIDTH:
        return f"width:{record.width}!={TARGET_WIDTH}"
    if record.height != TARGET_HEIGHT:
        return f"height:{record.height}!={TARGET_HEIGHT}"
    if abs(record.fps - TARGET_FPS) > FPS_TOLERANCE:
        return f"fps:{record.fps}!={TARGET_FPS}"
    if record.num_frames != TARGET_NUM_FRAMES:
        return f"num_frames:{record.num_frames}!={TARGET_NUM_FRAMES}"
    if abs(record.duration_sec - CLIP_DURATION_SEC) > CLIP_DURATION_TOLERANCE_SEC:
        return f"duration_sec:{record.duration_sec:.3f}"
    return None


def _within_dataset_overlap(records: list[ClipRecord]) -> str | None:
    seen: dict[str, list[float]] = {}
    for row in records:
        key = canonical_source_key(row.source_video_url)
        positions = seen.setdefault(key, [])
        if any(abs(row.clip_start_sec - prev) < OVERLAP_WINDOW_SEC for prev in positions):
            return f"within_dataset_overlap:{row.clip_id}"
        positions.append(row.clip_start_sec)
    return None


def _count_global_overlap(
    records: list[ClipRecord],
    global_record_index: dict[str, list[float]],
) -> int:
    if not global_record_index:
        return 0
    count = 0
    for row in records:
        key = canonical_source_key(row.source_video_url)
        positions = global_record_index.get(key)
        if not positions:
            continue
        if any(abs(row.clip_start_sec - prev) < OVERLAP_WINDOW_SEC for prev in positions):
            count += 1
    return count


def _ffprobe_metadata(path: Path) -> tuple[int, int, float, int]:
    info = probe_video(path)
    stream: dict[str, Any] | None = None
    for s in info.get("streams", []):
        if s.get("codec_type") == "video":
            stream = s
            break
    if stream is None:
        raise ValueError("no video stream")
    width = int(stream.get("width") or 0)
    height = int(stream.get("height") or 0)
    r_frame_rate = stream.get("r_frame_rate", "0/1")
    try:
        num, den = r_frame_rate.split("/")
        fps = float(num) / max(float(den), 1.0)
    except Exception:
        fps = 0.0
    nb_frames = stream.get("nb_frames")
    try:
        num_frames = int(nb_frames) if nb_frames is not None else 0
    except Exception:
        num_frames = 0
    if num_frames <= 0:
        # Fallback: derive from duration * fps.
        duration = float(stream.get("duration") or info.get("format", {}).get("duration") or 0.0)
        num_frames = int(round(duration * fps)) if duration > 0 else 0
    return width, height, fps, num_frames


async def _download_with_sem(
    sem: asyncio.Semaphore,
    miner_store: Any,
    key: str,
    dst: Path,
) -> bool:
    async with sem:
        try:
            return bool(await miner_store.download_file(key, dst))
        except Exception as exc:
            logger.warning("download exception key=%s err=%s", key, exc)
            return False


async def validate_miner_dataset(
    *,
    miner_hotkey: str,
    interval_id: int,
    miner_store: Any,
    workdir: Path,
    global_record_index: dict[str, list[float]] | None = None,
    download_concurrency: int = 16,
) -> DatasetCheckOutcome:
    """Download miner dataset for `interval_id`, validate strict spec + overlaps.

    Returns DatasetCheckOutcome. Caller is responsible for the local files
    in `workdir/{miner_hotkey}/{interval_id}/` after this returns.
    """
    out = DatasetCheckOutcome(
        accepted=False,
        miner_hotkey=miner_hotkey,
        interval_id=interval_id,
    )
    miner_dir = workdir / miner_hotkey / str(interval_id)
    miner_dir.mkdir(parents=True, exist_ok=True)
    base_key = f"{interval_id}"

    # 1) manifest
    manifest_local = miner_dir / "manifest.json"
    ok = await miner_store.download_file(f"{base_key}/manifest.json", manifest_local)
    if not ok or not manifest_local.exists():
        out.failures.append("manifest_missing")
        return out
    try:
        manifest = read_manifest(manifest_local)
    except Exception as exc:
        out.failures.append(f"manifest_parse_error:{exc}")
        return out
    if manifest.miner_hotkey.strip() != miner_hotkey:
        out.failures.append("manifest_hotkey_mismatch")
        return out
    if manifest.interval_id != interval_id:
        out.failures.append("manifest_interval_mismatch")
        return out
    if manifest.record_count != SAMPLE_COUNT:
        out.failures.append(f"record_count:{manifest.record_count}!={SAMPLE_COUNT}")
        return out

    # 2) dataset.parquet
    dataset_local = miner_dir / "dataset.parquet"
    ok = await miner_store.download_file(f"{base_key}/dataset.parquet", dataset_local)
    if not ok or not dataset_local.exists():
        out.failures.append("dataset_missing")
        return out
    if sha256_file(dataset_local) != manifest.dataset_sha256:
        out.failures.append("dataset_sha256_mismatch")
        return out
    try:
        records = read_dataset_parquet(dataset_local)
    except Exception as exc:
        out.failures.append(f"dataset_parse_error:{exc}")
        return out
    if len(records) != SAMPLE_COUNT:
        out.failures.append(f"records_len:{len(records)}!={SAMPLE_COUNT}")
        return out

    # 3) per-row spec
    for row in records:
        reason = _check_record_specs(row)
        if reason is not None:
            out.failures.append(f"spec:{reason}")
            return out

    # 4) within-dataset overlap
    overlap_reason = _within_dataset_overlap(records)
    if overlap_reason is not None:
        out.failures.append(overlap_reason)
        return out

    # 4.5) every row must carry a non-empty caption. The trainer image needs
    # a `prompt` per clip; rejecting here (rather than substituting a default
    # at conversion time) makes captioning an explicit miner responsibility.
    for row in records:
        if not (getattr(row, "caption", "") or "").strip():
            out.failures.append(f"caption_missing:{row.clip_id}")
            return out

    # 5) parallel download of all 800 assets (400 clips + 400 frames), then
    # serial sha256/ffprobe verification.  Downloads dominate wall-clock — they
    # benefit from concurrency; sha+probe are CPU-bound and stay sequential.
    sem = asyncio.Semaphore(max(int(download_concurrency), 1))
    download_specs: list[tuple[ClipRecord, Path, Path]] = []
    download_tasks: list[asyncio.Task[bool]] = []
    for row in records:
        clip_uri = row.clip_uri.lstrip("/")
        frame_uri = row.first_frame_uri.lstrip("/")
        clip_local = miner_dir / clip_uri
        frame_local = miner_dir / frame_uri
        download_specs.append((row, clip_local, frame_local))
        download_tasks.append(
            asyncio.create_task(
                _download_with_sem(sem, miner_store, f"{base_key}/{clip_uri}", clip_local)
            )
        )
        download_tasks.append(
            asyncio.create_task(
                _download_with_sem(sem, miner_store, f"{base_key}/{frame_uri}", frame_local)
            )
        )
    download_results = await asyncio.gather(*download_tasks)

    for idx, (row, clip_local, frame_local) in enumerate(download_specs):
        clip_ok = download_results[idx * 2]
        frame_ok = download_results[idx * 2 + 1]
        if not clip_ok or not clip_local.exists():
            out.failures.append(f"clip_missing:{row.clip_id}")
            return out
        if sha256_file(clip_local) != row.clip_sha256:
            out.failures.append(f"clip_sha256_mismatch:{row.clip_id}")
            return out
        try:
            width, height, fps, num_frames = _ffprobe_metadata(clip_local)
        except Exception as exc:
            out.failures.append(f"clip_probe_error:{row.clip_id}:{exc}")
            return out
        if width != TARGET_WIDTH or height != TARGET_HEIGHT:
            out.failures.append(f"clip_resolution:{row.clip_id}:{width}x{height}")
            return out
        if abs(fps - TARGET_FPS) > FPS_TOLERANCE:
            out.failures.append(f"clip_fps:{row.clip_id}:{fps:.3f}")
            return out
        if num_frames and abs(num_frames - TARGET_NUM_FRAMES) > 1:
            # Allow ±1 because container metadata can be off by one.
            out.failures.append(f"clip_num_frames:{row.clip_id}:{num_frames}")
            return out
        if not frame_ok or not frame_local.exists():
            out.failures.append(f"frame_missing:{row.clip_id}")
            return out
        if sha256_file(frame_local) != row.first_frame_sha256:
            out.failures.append(f"frame_sha256_mismatch:{row.clip_id}")
            return out

    # 6) global overlap count
    global_overlap = _count_global_overlap(records, global_record_index or {})
    out.global_overlap_count = global_overlap
    if global_overlap > GLOBAL_OVERLAP_REJECT_THRESHOLD:
        out.failures.append(
            f"global_overlap_exceeded:{global_overlap}>{GLOBAL_OVERLAP_REJECT_THRESHOLD}"
        )
        return out

    out.record_count = len(records)
    out.accepted = True
    out.notes = {
        "manifest_protocol": manifest.protocol_version,
        "manifest_schema": manifest.schema_version,
    }
    return out


async def list_miner_interval_ids(miner_store: Any) -> list[int]:
    """Return integer prefixes (interval_ids) present in the miner bucket."""
    keys = await miner_store.list_prefix("")
    seen: set[int] = set()
    for key in keys:
        head = key.split("/", 1)[0]
        if head.isdigit():
            seen.add(int(head))
    return sorted(seen)


async def latest_complete_interval_id(miner_store: Any) -> int | None:
    """Return the largest integer interval_id with both manifest.json + dataset.parquet."""
    candidates = await list_miner_interval_ids(miner_store)
    for interval_id in reversed(candidates):
        if await miner_store.object_exists(f"{interval_id}/manifest.json") and \
           await miner_store.object_exists(f"{interval_id}/dataset.parquet"):
            return interval_id
    return None


def manifest_for_interval(local_manifest: Path) -> IntervalManifest:
    return read_manifest(local_manifest)
