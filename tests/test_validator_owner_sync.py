from __future__ import annotations

import json
from pathlib import Path

from nexis.hash_utils import sha256_file
from nexis.models import ClipRecord, IntervalManifest, ValidationDecision
from nexis.serialization import write_dataset_parquet, write_manifest
from nexis.validator.owner_sync import (
    load_record_info_snapshot_async,
    merge_records_into_index,
    parse_record_info,
    serialize_record_info,
    sync_record_info_to_owner_bucket_async,
    upload_record_info_snapshot_async,
    upload_validated_datasets_to_owner_bucket,
)
from nexis.validator.pipeline import ValidatorPipeline
from .helpers import LocalObjectStore, run_async


def _row(clip_id: str, start: float) -> ClipRecord:
    return ClipRecord(
        clip_id=clip_id,
        clip_uri=f"clips/{clip_id}.mp4",
        clip_sha256="a" * 64,
        first_frame_uri=f"frames/{clip_id}.jpg",
        first_frame_sha256="b" * 64,
        source_video_id="bQO8kMZwWuQ",
        split_group_id="bQO8kMZwWuQ:1",
        split="train",
        clip_start_sec=start,
        duration_sec=5.0,
        width=1280,
        height=720,
        fps=30.0,
        num_frames=150,
        has_audio=True,
        caption="A moving object.",
        source_video_url="https://www.youtube.com/watch?v=bQO8kMZwWuQ",
        source_proof={"extractor": "yt-dlp"},
    )


def test_record_info_round_trip_and_merge() -> None:
    index: dict[str, list[float]] = {}
    rows = [_row("c1", 0.0), _row("c2", 8.0)]
    merge_records_into_index(record_index=index, records=rows)
    merge_records_into_index(record_index=index, records=[_row("c3", 8.0)])
    payload = serialize_record_info(index)
    assert "\"entries\"" not in payload
    assert "\"version\"" not in payload
    parsed = parse_record_info(payload)
    values = parsed["https://www.youtube.com/watch?v=bQO8kMZwWuQ"]
    assert values == [0.0, 8.0]


def test_merge_records_normalizes_source_urls() -> None:
    index: dict[str, list[float]] = {}
    row = _row("c1", 0.0)
    row.source_video_url = "https://youtu.be/bQO8kMZwWuQ"
    merge_records_into_index(record_index=index, records=[row])
    assert index == {"https://www.youtube.com/watch?v=bQO8kMZwWuQ": [0.0]}


def test_parse_record_info_legacy_payload_normalizes_to_url_only() -> None:
    payload = json.dumps(
        {
            "version": 2,
            "entries": {
                "bQO8kMZwWuQ": ["0.000"],
                "video_v1:bQO8kMZwWuQ": ["5.000"],
                "video_v1:https://www.youtube.com/watch?v=bQO8kMZwWuQ": ["10.000"],
                "https://www.youtube.com/watch?v=bQO8kMZwWuQ": ["15.000"],
            },
        }
    )
    parsed = parse_record_info(payload)
    assert parsed == {
        "https://www.youtube.com/watch?v=bQO8kMZwWuQ": [0.0, 5.0, 10.0, 15.0]
    }


def test_record_info_async_roundtrip_with_store(tmp_path: Path) -> None:
    async def run() -> dict[str, list[float]]:
        store = LocalObjectStore(tmp_path / "record-info")
        payload = {
            "https://www.youtube.com/watch?v=bQO8kMZwWuQ": [0.0, 8.0],
        }
        await upload_record_info_snapshot_async(
            record_info_store=store,
            object_key="snapshot.json",
            workdir=tmp_path / "work",
            record_index=payload,
        )
        return await load_record_info_snapshot_async(
            record_info_store=store,
            object_key="snapshot.json",
            workdir=tmp_path / "work-2",
        )

    loaded = run_async(run())
    assert loaded["https://www.youtube.com/watch?v=bQO8kMZwWuQ"] == [0.0, 8.0]


def test_owner_upload_publishes_validated_dataset_bundle(tmp_path: Path) -> None:
    async def run_validation() -> tuple[ValidatorPipeline, list]:
        source_store = LocalObjectStore(tmp_path / "source")
        interval_id = 90
        hotkey = "miner1"
        key_base = f"{interval_id}"

        clip = tmp_path / "clip.mp4"
        frame = tmp_path / "frame.jpg"
        clip.write_bytes(b"clip")
        frame.write_bytes(b"frame")
        row = _row("c1", 0.0)
        row.clip_sha256 = sha256_file(clip)
        row.first_frame_sha256 = sha256_file(frame)
        dataset = tmp_path / "dataset.parquet"
        manifest = tmp_path / "manifest.json"
        write_dataset_parquet([row], dataset)
        write_manifest(
            IntervalManifest(
                netuid=1,
                miner_hotkey=hotkey,
                interval_id=interval_id,
                record_count=1,
                dataset_sha256=sha256_file(dataset),
            ),
            manifest,
        )
        await source_store.upload_file(f"{key_base}/dataset.parquet", dataset)
        await source_store.upload_file(f"{key_base}/manifest.json", manifest)
        await source_store.upload_file(f"{key_base}/{row.clip_uri}", clip)
        await source_store.upload_file(f"{key_base}/{row.first_frame_uri}", frame)

        pipeline = ValidatorPipeline(store_for_hotkey=lambda _: source_store)
        decisions, _weights = await pipeline.validate_interval(
            candidate_hotkeys=[hotkey],
            interval_id=interval_id,
        )
        assert decisions[0].accepted is True
        return pipeline, decisions

    owner_store = LocalObjectStore(tmp_path / "owner")
    source_store = LocalObjectStore(tmp_path / "source")
    pipeline, decisions = run_async(run_validation())
    interval_id = 90
    hotkey = "miner1"

    upload_validated_datasets_to_owner_bucket(
        owner_store=owner_store,
        source_store_for_hotkey=lambda _: source_store,
        validator=pipeline,
        decisions=decisions,
        interval_id=interval_id,
        workdir=tmp_path / "work",
    )

    expected_prefix = tmp_path / "owner" / f"{interval_id}/{hotkey}"
    assert (expected_prefix / "dataset.parquet").exists()
    assert (expected_prefix / "manifest.json").exists()
    assert not (expected_prefix / "clips/c1.mp4").exists()
    assert not (expected_prefix / "frames/c1.jpg").exists()
    assert not (tmp_path / "work" / "owner-upload" / str(interval_id) / hotkey).exists()

    parsed_manifest = IntervalManifest.model_validate_json(
        (expected_prefix / "manifest.json").read_text(encoding="utf-8")
    )
    assert parsed_manifest.record_count == 1


def test_owner_upload_skips_rows_with_missing_assets(tmp_path: Path) -> None:
    async def run_validation() -> tuple[ValidatorPipeline, list]:
        source_store = LocalObjectStore(tmp_path / "source")
        interval_id = 91
        hotkey = "miner1"
        key_base = f"{interval_id}"

        clip = tmp_path / "clip.mp4"
        frame = tmp_path / "frame.jpg"
        clip.write_bytes(b"clip")
        frame.write_bytes(b"frame")
        row = _row("c1", 0.0)
        row.clip_sha256 = sha256_file(clip)
        row.first_frame_sha256 = sha256_file(frame)
        dataset = tmp_path / "dataset.parquet"
        manifest = tmp_path / "manifest.json"
        write_dataset_parquet([row], dataset)
        write_manifest(
            IntervalManifest(
                netuid=1,
                miner_hotkey=hotkey,
                interval_id=interval_id,
                record_count=1,
                dataset_sha256=sha256_file(dataset),
            ),
            manifest,
        )
        await source_store.upload_file(f"{key_base}/dataset.parquet", dataset)
        await source_store.upload_file(f"{key_base}/manifest.json", manifest)
        # Intentionally upload only clip, not first_frame.
        await source_store.upload_file(f"{key_base}/{row.clip_uri}", clip)

        pipeline = ValidatorPipeline(store_for_hotkey=lambda _: source_store)
        # Build artifact cache manually to test owner publish filtering behavior.
        pipeline.last_interval_artifacts.interval_id = interval_id
        pipeline.last_interval_artifacts.records_by_hotkey = {hotkey: [row]}
        pipeline.last_interval_artifacts.manifests_by_hotkey = {
            hotkey: IntervalManifest(
                netuid=1,
                miner_hotkey=hotkey,
                interval_id=interval_id,
                record_count=1,
                dataset_sha256=sha256_file(dataset),
            )
        }
        return pipeline, []

    owner_store = LocalObjectStore(tmp_path / "owner")
    source_store = LocalObjectStore(tmp_path / "source")
    pipeline, _ = run_async(run_validation())
    interval_id = 91
    hotkey = "miner1"

    upload_validated_datasets_to_owner_bucket(
        owner_store=owner_store,
        source_store_for_hotkey=lambda _: source_store,
        validator=pipeline,
        decisions=[
            ValidationDecision(
                miner_hotkey=hotkey,
                interval_id=interval_id,
                accepted=True,
                failures=[],
                sampled_rows=0,
            )
        ],
        interval_id=interval_id,
        workdir=tmp_path / "work",
    )

    expected_prefix = tmp_path / "owner" / f"{interval_id}/{hotkey}"
    assert (expected_prefix / "dataset.parquet").exists()
    assert (expected_prefix / "manifest.json").exists()
    assert not (tmp_path / "work" / "owner-upload" / str(interval_id) / hotkey).exists()


def test_owner_sync_worker_copies_assets_to_owner_bucket(tmp_path: Path) -> None:
    async def run_sync() -> None:
        record_info_store = LocalObjectStore(tmp_path / "record-info")
        owner_store = LocalObjectStore(tmp_path / "owner-db")
        source_store = LocalObjectStore(tmp_path / "source")
        interval_id = 92
        hotkey = "miner1"
        key_base = f"{interval_id}"

        clip = tmp_path / "clip.mp4"
        frame = tmp_path / "frame.jpg"
        clip.write_bytes(b"clip")
        frame.write_bytes(b"frame")
        row = _row("c1", 0.0)
        row.clip_sha256 = sha256_file(clip)
        row.first_frame_sha256 = sha256_file(frame)
        dataset = tmp_path / "dataset.parquet"
        manifest = tmp_path / "manifest.json"
        write_dataset_parquet([row], dataset)
        write_manifest(
            IntervalManifest(
                netuid=1,
                miner_hotkey=hotkey,
                interval_id=interval_id,
                record_count=1,
                dataset_sha256=sha256_file(dataset),
            ),
            manifest,
        )
        # metadata bundle in record-info bucket
        await record_info_store.upload_file(f"{key_base}/{hotkey}/dataset.parquet", dataset)
        await record_info_store.upload_file(f"{key_base}/{hotkey}/manifest.json", manifest)
        # original miner assets in source bucket
        await source_store.upload_file(f"{key_base}/{row.clip_uri}", clip)
        await source_store.upload_file(f"{key_base}/{row.first_frame_uri}", frame)

        summary = await sync_record_info_to_owner_bucket_async(
            record_info_store=record_info_store,
            owner_store=owner_store,
            source_store_for_hotkey=lambda _hotkey: source_store,
            workdir=tmp_path / "work-sync",
        )
        assert summary["processed"] == 1
        assert summary["copied"] == 1

    run_async(run_sync())

    expected_prefix = tmp_path / "owner-db" / "92/miner1"
    assert (expected_prefix / "dataset.parquet").exists()
    assert (expected_prefix / "manifest.json").exists()
    assert (expected_prefix / "clips/c1.mp4").exists()
    assert (expected_prefix / "frames/c1.jpg").exists()