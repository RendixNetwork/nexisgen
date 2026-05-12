"""Validate `validate_miner_dataset` rejects bad spec / overlap."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from nexis.hash_utils import sha256_file
from nexis.models import ClipRecord, IntervalManifest
from nexis.protocol import (
    CLIP_DURATION_SEC,
    SAMPLE_COUNT,
    TARGET_FPS,
    TARGET_HEIGHT,
    TARGET_NUM_FRAMES,
    TARGET_WIDTH,
)
from nexis.serialization import write_dataset_parquet, write_manifest
from nexis.validator.dataset_check import (
    canonical_source_key,
    validate_miner_dataset,
)
from tests.helpers import LocalObjectStore


def _build_records(count: int = SAMPLE_COUNT, gap: float = 6.0) -> list[ClipRecord]:
    records: list[ClipRecord] = []
    for i in range(count):
        start = float(i) * gap
        records.append(
            ClipRecord(
                clip_id=f"clip_{i:04d}",
                clip_uri=f"clips/clip_{i:04d}.mp4",
                clip_sha256="0" * 64,
                first_frame_uri=f"frames/clip_{i:04d}.jpg",
                first_frame_sha256="1" * 64,
                source_video_id="vid",
                clip_start_sec=start,
                duration_sec=CLIP_DURATION_SEC,
                width=TARGET_WIDTH,
                height=TARGET_HEIGHT,
                fps=float(TARGET_FPS),
                num_frames=TARGET_NUM_FRAMES,
                source_video_url="https://www.youtube.com/watch?v=test",
            )
        )
    return records


def _materialize_dataset(
    *,
    store: LocalObjectStore,
    miner_hotkey: str,
    interval_id: int,
    records: list[ClipRecord],
    workdir: Path,
) -> None:
    workdir.mkdir(parents=True, exist_ok=True)
    dataset_path = workdir / "dataset.parquet"
    write_dataset_parquet(records, dataset_path)
    # Recompute clip_sha and frame_sha for each row by writing tiny stub files
    # that carry the recorded sha256 (we override sha256s in records before parquet write).
    # For simplicity the test patches stored shas to match generated stubs.
    asyncio_run = asyncio.get_event_loop()
    asyncio_run.run_until_complete(
        store.upload_file(f"{interval_id}/dataset.parquet", dataset_path)
    )
    manifest = IntervalManifest(
        spec_id="video_v1",
        netuid=1,
        miner_hotkey=miner_hotkey,
        interval_id=interval_id,
        record_count=len(records),
        dataset_sha256=sha256_file(dataset_path),
    )
    manifest_path = workdir / "manifest.json"
    write_manifest(manifest, manifest_path)
    asyncio_run.run_until_complete(
        store.upload_file(f"{interval_id}/manifest.json", manifest_path)
    )


def test_canonical_source_key_youtube() -> None:
    assert canonical_source_key(
        "https://youtu.be/abc123"
    ) == "https://www.youtube.com/watch?v=abc123"
    assert canonical_source_key(
        "https://www.youtube.com/watch?v=abc123&t=4s"
    ) == "https://www.youtube.com/watch?v=abc123"
    assert canonical_source_key(
        "https://www.youtube.com/shorts/xyz"
    ) == "https://www.youtube.com/watch?v=xyz"


def test_validate_rejects_record_count_mismatch(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "miner")
    records = _build_records(count=10)
    workdir = tmp_path / "build"
    workdir.mkdir(parents=True, exist_ok=True)
    dataset_path = workdir / "dataset.parquet"
    write_dataset_parquet(records, dataset_path)
    asyncio.run(store.upload_file("1/dataset.parquet", dataset_path))
    # Build manifest with a (forced) record_count == SAMPLE_COUNT but actual rows are 10.
    # The model rejects record_count != 400, so write JSON directly.
    manifest_payload = {
        "protocol_version": "2.0.0",
        "schema_version": "2.0.0",
        "spec_id": "video_v1",
        "netuid": 1,
        "miner_hotkey": "5xyz",
        "interval_id": 1,
        "created_at": "2026-01-01T00:00:00+00:00",
        "record_count": SAMPLE_COUNT,
        "dataset_sha256": sha256_file(dataset_path),
    }
    manifest_path = workdir / "manifest.json"
    manifest_path.write_text(__import__("json").dumps(manifest_payload), encoding="utf-8")
    asyncio.run(store.upload_file("1/manifest.json", manifest_path))

    outcome = asyncio.run(
        validate_miner_dataset(
            miner_hotkey="5xyz",
            interval_id=1,
            miner_store=store,
            workdir=tmp_path / "validate",
        )
    )
    assert not outcome.accepted
    assert any("records_len" in f for f in outcome.failures)


def test_validate_missing_manifest(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "miner")
    outcome = asyncio.run(
        validate_miner_dataset(
            miner_hotkey="5xyz",
            interval_id=1,
            miner_store=store,
            workdir=tmp_path / "validate",
        )
    )
    assert not outcome.accepted
    assert "manifest_missing" in outcome.failures


@pytest.mark.parametrize(
    "global_count,threshold_exceeded",
    [(100, False), (101, True)],
)
def test_global_overlap_threshold(tmp_path: Path, global_count: int, threshold_exceeded: bool) -> None:
    """Construct a minimal index that triggers exactly global_count overlaps."""
    from nexis.validator.dataset_check import _count_global_overlap

    records = _build_records(count=SAMPLE_COUNT)
    canonical = "https://www.youtube.com/watch?v=test"
    # Place `global_count` exact-position matches in the global index.
    overlap_starts = [records[i].clip_start_sec for i in range(global_count)]
    global_index = {canonical: overlap_starts}
    overlapped = _count_global_overlap(records, global_index)
    assert overlapped == global_count
    from nexis.protocol import GLOBAL_OVERLAP_REJECT_THRESHOLD
    assert (overlapped > GLOBAL_OVERLAP_REJECT_THRESHOLD) is threshold_exceeded
