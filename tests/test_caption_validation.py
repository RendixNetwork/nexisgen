"""Validator rejects miners with any empty caption."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from nexis.hash_utils import sha256_file
from nexis.models import ClipRecord
from nexis.protocol import (
    CLIP_DURATION_SEC,
    SAMPLE_COUNT,
    TARGET_FPS,
    TARGET_HEIGHT,
    TARGET_NUM_FRAMES,
    TARGET_WIDTH,
)
from nexis.serialization import write_dataset_parquet
from nexis.validator.dataset_check import validate_miner_dataset
from tests.helpers import LocalObjectStore


def _record(idx: int, *, caption: str) -> ClipRecord:
    start = float(idx) * 6.0
    return ClipRecord(
        clip_id=f"clip_{idx:04d}",
        clip_uri=f"clips/clip_{idx:04d}.mp4",
        clip_sha256="0" * 64,
        first_frame_uri=f"frames/clip_{idx:04d}.jpg",
        first_frame_sha256="1" * 64,
        source_video_id="src",
        clip_start_sec=start,
        duration_sec=CLIP_DURATION_SEC,
        width=TARGET_WIDTH,
        height=TARGET_HEIGHT,
        fps=float(TARGET_FPS),
        num_frames=TARGET_NUM_FRAMES,
        source_video_url="https://www.youtube.com/watch?v=t",
        caption=caption,
    )


def _publish(store: LocalObjectStore, records: list[ClipRecord], workdir: Path) -> None:
    workdir.mkdir(parents=True, exist_ok=True)
    parquet = workdir / "dataset.parquet"
    write_dataset_parquet(records, parquet)
    asyncio.run(store.upload_file("1/dataset.parquet", parquet))
    manifest_payload = {
        "protocol_version": "2.0.0",
        "schema_version": "2.0.0",
        "spec_id": "video_v1",
        "netuid": 1,
        "miner_hotkey": "5xyz",
        "interval_id": 1,
        "created_at": "2026-01-01T00:00:00+00:00",
        "record_count": SAMPLE_COUNT,
        "dataset_sha256": sha256_file(parquet),
    }
    manifest = workdir / "manifest.json"
    manifest.write_text(json.dumps(manifest_payload), encoding="utf-8")
    asyncio.run(store.upload_file("1/manifest.json", manifest))


def test_reject_when_any_caption_empty(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "miner")
    records = [_record(i, caption="a clip") for i in range(SAMPLE_COUNT)]
    records[42] = _record(42, caption="")  # one empty caption out of 400
    _publish(store, records, tmp_path / "build")

    outcome = asyncio.run(
        validate_miner_dataset(
            miner_hotkey="5xyz",
            interval_id=1,
            miner_store=store,
            workdir=tmp_path / "validate",
        )
    )
    assert not outcome.accepted
    assert any(f.startswith("caption_missing:") for f in outcome.failures), outcome.failures


def test_caption_check_fires_before_asset_download(tmp_path: Path) -> None:
    """Caption check happens before the (expensive) clip download phase."""
    store = LocalObjectStore(tmp_path / "miner")
    records = [_record(i, caption="" if i == 0 else "ok") for i in range(SAMPLE_COUNT)]
    _publish(store, records, tmp_path / "build")
    # No clip files were ever uploaded to the store; if the caption check
    # didn't short-circuit, we'd get a clip_missing failure instead.
    outcome = asyncio.run(
        validate_miner_dataset(
            miner_hotkey="5xyz",
            interval_id=1,
            miner_store=store,
            workdir=tmp_path / "validate",
        )
    )
    assert not outcome.accepted
    assert outcome.failures[0].startswith("caption_missing:")
