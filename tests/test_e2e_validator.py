from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip("pyarrow")
import pyarrow as pa
import pyarrow.parquet as pq

from nexis.hash_utils import sha256_file
from nexis.models import ClipRecord, IntervalManifest
from nexis.serialization import write_dataset_parquet, write_manifest
from nexis.validator.pipeline import ValidatorPipeline
from .helpers import LocalObjectStore, run_async


def _record(
    clip_id: str,
    start: float,
    *,
    clip_sha256: str = "a" * 64,
    frame_sha256: str = "b" * 64,
) -> ClipRecord:
    return ClipRecord(
        clip_id=clip_id,
        clip_uri=f"clips/{clip_id}.mp4",
        clip_sha256=clip_sha256,
        first_frame_uri=f"frames/{clip_id}.jpg",
        first_frame_sha256=frame_sha256,
        source_video_id="ytid",
        split_group_id="ytid:1",
        split="train",
        clip_start_sec=start,
        duration_sec=5.0,
        width=1280,
        height=720,
        fps=30.0,
        num_frames=150,
        has_audio=True,
        caption="A moving car in a city scene.",
        source_video_url="https://youtube.com/watch?v=abc",
        source_proof={"extractor": "yt-dlp"},
    )


class _AlwaysMismatchChecker:
    def check(
        self,
        *,
        sampled: list[ClipRecord],
        frame_paths_by_clip_id: dict[str, list[Path]],
    ) -> list[str]:
        _ = frame_paths_by_clip_id
        if not sampled:
            return []
        return [f"caption_semantic_mismatch:{sampled[0].clip_id}"]


def test_validator_e2e(tmp_path: Path) -> None:
    async def run() -> None:
        store = LocalObjectStore(tmp_path / "store")
        hotkey = "miner1"
        interval_id = 12
        key_base = f"{interval_id}"

        assets_dir = tmp_path / "assets"
        assets_dir.mkdir(parents=True, exist_ok=True)
        records: list[ClipRecord] = []
        for clip_id, start in [("c1", 0.0), ("c2", 5.0), ("c3", 10.0)]:
            clip_file = assets_dir / f"{clip_id}.mp4"
            frame_file = assets_dir / f"{clip_id}.jpg"
            clip_file.write_bytes(f"clip-{clip_id}".encode("utf-8"))
            frame_file.write_bytes(f"frame-{clip_id}".encode("utf-8"))
            records.append(
                _record(
                    clip_id,
                    start,
                    clip_sha256=sha256_file(clip_file),
                    frame_sha256=sha256_file(frame_file),
                )
            )

        dataset = tmp_path / "dataset.parquet"
        manifest = tmp_path / "manifest.json"
        write_dataset_parquet(records, dataset)
        write_manifest(
            IntervalManifest(
                netuid=1,
                miner_hotkey=hotkey,
                interval_id=interval_id,
                record_count=len(records),
                dataset_sha256=sha256_file(dataset),
            ),
            manifest,
        )
        await store.upload_file(f"{key_base}/dataset.parquet", dataset)
        await store.upload_file(f"{key_base}/manifest.json", manifest)
        for row in records:
            clip_file = assets_dir / f"{row.clip_id}.mp4"
            frame_file = assets_dir / f"{row.clip_id}.jpg"
            await store.upload_file(f"{key_base}/{row.clip_uri}", clip_file)
            await store.upload_file(f"{key_base}/{row.first_frame_uri}", frame_file)

        pipeline = ValidatorPipeline(store_for_hotkey=lambda _: store)
        decisions, weights = await pipeline.validate_interval(
            candidate_hotkeys=[hotkey],
            interval_id=interval_id,
        )
        assert len(decisions) == 1
        assert decisions[0].accepted is True
        assert hotkey in weights

    run_async(run())


def test_validator_cleans_interval_downloaded_assets(tmp_path: Path) -> None:
    async def run() -> None:
        store = LocalObjectStore(tmp_path / "store")
        hotkey = "miner1"
        interval_id = 13
        key_base = f"{interval_id}"
        workdir = tmp_path / "validator-workdir"

        clip_file = tmp_path / "clip.mp4"
        frame_file = tmp_path / "frame.jpg"
        clip_file.write_bytes(b"clip-data")
        frame_file.write_bytes(b"frame-data")
        row = _record(
            "c1",
            0.0,
            clip_sha256=sha256_file(clip_file),
            frame_sha256=sha256_file(frame_file),
        )

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
        await store.upload_file(f"{key_base}/dataset.parquet", dataset)
        await store.upload_file(f"{key_base}/manifest.json", manifest)
        await store.upload_file(f"{key_base}/{row.clip_uri}", clip_file)
        await store.upload_file(f"{key_base}/{row.first_frame_uri}", frame_file)

        pipeline = ValidatorPipeline(store_for_hotkey=lambda _: store)
        decisions, _weights = await pipeline.validate_interval(
            candidate_hotkeys=[hotkey],
            interval_id=interval_id,
            workdir=workdir,
        )
        assert decisions[0].accepted is True
        assert not (workdir / hotkey / str(interval_id)).exists()

    run_async(run())


def test_validator_rejects_semantic_caption_mismatch(tmp_path: Path) -> None:
    async def run() -> None:
        store = LocalObjectStore(tmp_path / "store")
        hotkey = "miner1"
        interval_id = 12
        key_base = f"{interval_id}"

        clip_id = "c1"
        clip_file = tmp_path / f"{clip_id}.mp4"
        frame_file = tmp_path / f"{clip_id}.jpg"
        clip_file.write_bytes(b"clip-data")
        frame_file.write_bytes(b"frame-data")
        records = [
            _record(
                clip_id,
                0.0,
                clip_sha256=sha256_file(clip_file),
                frame_sha256=sha256_file(frame_file),
            )
        ]
        dataset = tmp_path / "dataset.parquet"
        manifest = tmp_path / "manifest.json"
        write_dataset_parquet(records, dataset)
        write_manifest(
            IntervalManifest(
                netuid=1,
                miner_hotkey=hotkey,
                interval_id=interval_id,
                record_count=len(records),
                dataset_sha256=sha256_file(dataset),
            ),
            manifest,
        )
        await store.upload_file(f"{key_base}/dataset.parquet", dataset)
        await store.upload_file(f"{key_base}/manifest.json", manifest)
        await store.upload_file(f"{key_base}/{records[0].clip_uri}", clip_file)
        await store.upload_file(f"{key_base}/{records[0].first_frame_uri}", frame_file)

        pipeline = ValidatorPipeline(
            store_for_hotkey=lambda _: store,
            caption_semantic_checker=_AlwaysMismatchChecker(),
        )
        decisions, _weights = await pipeline.validate_interval(
            candidate_hotkeys=[hotkey],
            interval_id=interval_id,
        )
        assert len(decisions) == 1
        assert decisions[0].accepted is False
        assert any(
            item.startswith("caption_semantic_mismatch:")
            for item in decisions[0].failures
        )

    run_async(run())


def test_validator_rejects_dataset_sha_mismatch(tmp_path: Path) -> None:
    async def run() -> None:
        store = LocalObjectStore(tmp_path / "store")
        hotkey = "miner1"
        interval_id = 12
        key_base = f"{interval_id}"

        records = [_record("c1", 0.0)]
        dataset = tmp_path / "dataset.parquet"
        manifest = tmp_path / "manifest.json"
        write_dataset_parquet(records, dataset)
        write_manifest(
            IntervalManifest(
                netuid=1,
                miner_hotkey=hotkey,
                interval_id=interval_id,
                record_count=len(records),
                dataset_sha256="f" * 64,
            ),
            manifest,
        )
        await store.upload_file(f"{key_base}/dataset.parquet", dataset)
        await store.upload_file(f"{key_base}/manifest.json", manifest)

        pipeline = ValidatorPipeline(store_for_hotkey=lambda _: store)
        decisions, _weights = await pipeline.validate_interval(
            candidate_hotkeys=[hotkey],
            interval_id=interval_id,
        )
        assert len(decisions) == 1
        assert decisions[0].accepted is False
        assert "dataset_sha256_mismatch" in decisions[0].failures

    run_async(run())


def test_validator_rejects_missing_sampled_assets(tmp_path: Path) -> None:
    async def run() -> None:
        store = LocalObjectStore(tmp_path / "store")
        hotkey = "miner1"
        interval_id = 12
        key_base = f"{interval_id}"

        records = [_record("c1", 0.0)]
        dataset = tmp_path / "dataset.parquet"
        manifest = tmp_path / "manifest.json"
        write_dataset_parquet(records, dataset)
        write_manifest(
            IntervalManifest(
                netuid=1,
                miner_hotkey=hotkey,
                interval_id=interval_id,
                record_count=len(records),
                dataset_sha256=sha256_file(dataset),
            ),
            manifest,
        )
        await store.upload_file(f"{key_base}/dataset.parquet", dataset)
        await store.upload_file(f"{key_base}/manifest.json", manifest)

        pipeline = ValidatorPipeline(store_for_hotkey=lambda _: store)
        decisions, _weights = await pipeline.validate_interval(
            candidate_hotkeys=[hotkey],
            interval_id=interval_id,
        )
        assert len(decisions) == 1
        assert decisions[0].accepted is False
        assert any(
            item.startswith("missing_clip_asset:") for item in decisions[0].failures
        )

    run_async(run())


def test_validator_rejects_malformed_manifest_as_schema_invalid(tmp_path: Path) -> None:
    async def run() -> None:
        store = LocalObjectStore(tmp_path / "store")
        hotkey = "miner1"
        interval_id = 12
        key_base = f"{interval_id}"

        dataset = tmp_path / "dataset.parquet"
        write_dataset_parquet([_record("c1", 0.0)], dataset)
        bad_manifest = tmp_path / "manifest.json"
        bad_manifest.write_text("{bad json", encoding="utf-8")
        await store.upload_file(f"{key_base}/dataset.parquet", dataset)
        await store.upload_file(f"{key_base}/manifest.json", bad_manifest)

        pipeline = ValidatorPipeline(store_for_hotkey=lambda _: store)
        decisions, _weights = await pipeline.validate_interval(
            candidate_hotkeys=[hotkey],
            interval_id=interval_id,
        )
        assert len(decisions) == 1
        assert decisions[0].accepted is False
        assert "manifest_schema_invalid" in decisions[0].failures

    run_async(run())


def test_validator_rejects_invalid_dataset_schema(tmp_path: Path) -> None:
    async def run() -> None:
        store = LocalObjectStore(tmp_path / "store")
        hotkey = "miner1"
        interval_id = 12
        key_base = f"{interval_id}"

        # Write an intentionally invalid row directly to parquet to bypass ClipRecord validation
        # on the miner side and simulate malformed miner submissions.
        invalid_row = {
            "clip_id": "c1",
            "clip_uri": "clips/c1.mp4",
            "clip_sha256": "a" * 64,
            "first_frame_uri": "frames/c1.jpg",
            "first_frame_sha256": "b" * 64,
            "source_video_id": "ytid",
            "split_group_id": "ytid:1",
            "split": "train",
            "clip_start_sec": 0.0,
            "duration_sec": 4.0,  # invalid: outside 5s +/- 0.15 tolerance
            "width": 640,
            "height": 360,
            "fps": 30.0,
            "num_frames": 120,
            "has_audio": True,
            "caption": "A moving car in a city scene.",
            "source_video_url": "https://youtube.com/watch?v=abc",
            "source_proof": {"extractor": "yt-dlp"},
        }
        dataset = tmp_path / "dataset.parquet"
        table = pa.Table.from_pylist([invalid_row])
        pq.write_table(table, dataset)

        manifest = tmp_path / "manifest.json"
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
        await store.upload_file(f"{key_base}/dataset.parquet", dataset)
        await store.upload_file(f"{key_base}/manifest.json", manifest)

        pipeline = ValidatorPipeline(store_for_hotkey=lambda _: store)
        decisions, _weights = await pipeline.validate_interval(
            candidate_hotkeys=[hotkey],
            interval_id=interval_id,
        )
        assert len(decisions) == 1
        assert decisions[0].accepted is False
        assert "dataset_schema_invalid" in decisions[0].failures

    run_async(run())


def test_validator_prunes_rows_using_global_record_index(tmp_path: Path) -> None:
    async def run() -> None:
        store = LocalObjectStore(tmp_path / "store")
        hotkey = "miner1"
        interval_id = 50
        key_base = f"{interval_id}"
        url = "https://youtube.com/watch?v=abc"

        clip_a = tmp_path / "a.mp4"
        frame_a = tmp_path / "a.jpg"
        clip_b = tmp_path / "b.mp4"
        frame_b = tmp_path / "b.jpg"
        clip_a.write_bytes(b"clip-a")
        frame_a.write_bytes(b"frame-a")
        clip_b.write_bytes(b"clip-b")
        frame_b.write_bytes(b"frame-b")
        rows = [
            _record("c1", 0.0, clip_sha256=sha256_file(clip_a), frame_sha256=sha256_file(frame_a)),
            _record("c2", 8.0, clip_sha256=sha256_file(clip_b), frame_sha256=sha256_file(frame_b)),
        ]
        rows[0].source_video_url = url
        rows[1].source_video_url = url
        dataset = tmp_path / "dataset.parquet"
        manifest = tmp_path / "manifest.json"
        write_dataset_parquet(rows, dataset)
        write_manifest(
            IntervalManifest(
                netuid=1,
                miner_hotkey=hotkey,
                interval_id=interval_id,
                record_count=2,
                dataset_sha256=sha256_file(dataset),
            ),
            manifest,
        )
        await store.upload_file(f"{key_base}/dataset.parquet", dataset)
        await store.upload_file(f"{key_base}/manifest.json", manifest)
        await store.upload_file(f"{key_base}/{rows[0].clip_uri}", clip_a)
        await store.upload_file(f"{key_base}/{rows[0].first_frame_uri}", frame_a)
        await store.upload_file(f"{key_base}/{rows[1].clip_uri}", clip_b)
        await store.upload_file(f"{key_base}/{rows[1].first_frame_uri}", frame_b)

        pipeline = ValidatorPipeline(store_for_hotkey=lambda _: store)
        decisions, _weights = await pipeline.validate_interval(
            candidate_hotkeys=[hotkey],
            interval_id=interval_id,
            global_record_index={url: [0.0]},
        )
        assert len(decisions) == 1
        decision = decisions[0]
        assert decision.accepted is True
        assert decision.notes["global_overlap_pruned_count"] == 1
        assert decision.notes["record_count"] == 1

    run_async(run())


def test_validator_cross_miner_overlap_uses_earliest_manifest(tmp_path: Path) -> None:
    async def run() -> None:
        stores = {
            "miner1": LocalObjectStore(tmp_path / "store1"),
            "miner2": LocalObjectStore(tmp_path / "store2"),
        }
        interval_id = 60
        key_base = f"{interval_id}"

        def _store_for_hotkey(hotkey: str) -> LocalObjectStore:
            return stores[hotkey]

        for hotkey, created_at, start in [
            ("miner1", datetime(2026, 1, 1, tzinfo=timezone.utc), 0.0),
            ("miner2", datetime(2026, 1, 2, tzinfo=timezone.utc), 2.0),
        ]:
            clip = tmp_path / f"{hotkey}.mp4"
            frame = tmp_path / f"{hotkey}.jpg"
            clip.write_bytes(f"clip-{hotkey}".encode("utf-8"))
            frame.write_bytes(f"frame-{hotkey}".encode("utf-8"))
            row = _record(
                f"{hotkey}-c1",
                start,
                clip_sha256=sha256_file(clip),
                frame_sha256=sha256_file(frame),
            )
            row.source_video_url = "https://youtube.com/watch?v=same"
            dataset = tmp_path / f"{hotkey}-dataset.parquet"
            manifest = tmp_path / f"{hotkey}-manifest.json"
            write_dataset_parquet([row], dataset)
            write_manifest(
                IntervalManifest(
                    netuid=1,
                    miner_hotkey=hotkey,
                    interval_id=interval_id,
                    record_count=1,
                    dataset_sha256=sha256_file(dataset),
                    created_at=created_at,
                ),
                manifest,
            )
            await stores[hotkey].upload_file(f"{key_base}/dataset.parquet", dataset)
            await stores[hotkey].upload_file(f"{key_base}/manifest.json", manifest)
            await stores[hotkey].upload_file(f"{key_base}/{row.clip_uri}", clip)
            await stores[hotkey].upload_file(f"{key_base}/{row.first_frame_uri}", frame)

        pipeline = ValidatorPipeline(store_for_hotkey=_store_for_hotkey)
        decisions, _weights = await pipeline.validate_interval(
            candidate_hotkeys=["miner1", "miner2"],
            interval_id=interval_id,
        )
        by_hotkey = {item.miner_hotkey: item for item in decisions}
        assert by_hotkey["miner1"].notes["cross_miner_overlap_pruned_count"] == 0
        assert by_hotkey["miner2"].notes["cross_miner_overlap_pruned_count"] == 1

    run_async(run())


def test_validator_cross_miner_overlap_keeps_non_overlapping_transitive_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    async def run() -> None:
        stores = {
            "miner1": LocalObjectStore(tmp_path / "store1"),
            "miner2": LocalObjectStore(tmp_path / "store2"),
            "miner3": LocalObjectStore(tmp_path / "store3"),
        }
        interval_id = 61
        key_base = f"{interval_id}"
        source_url = "https://youtube.com/watch?v=same"

        def _store_for_hotkey(hotkey: str) -> LocalObjectStore:
            return stores[hotkey]

        # miner1(0s) overlaps miner2(2s), miner2(2s) overlaps miner3(5s),
        # but miner1(0s) does NOT overlap miner3(5s). miner3 should remain valid.
        rows_meta = [
            ("miner1", datetime(2026, 1, 1, tzinfo=timezone.utc), 0.0),
            ("miner2", datetime(2026, 1, 2, tzinfo=timezone.utc), 2.0),
            ("miner3", datetime(2026, 1, 3, tzinfo=timezone.utc), 5.0),
        ]
        for hotkey, created_at, start in rows_meta:
            clip = tmp_path / f"{hotkey}-clip.mp4"
            frame = tmp_path / f"{hotkey}-frame.jpg"
            clip.write_bytes(f"clip-{hotkey}".encode("utf-8"))
            frame.write_bytes(f"frame-{hotkey}".encode("utf-8"))
            row = _record(
                f"{hotkey}-c1",
                start,
                clip_sha256=sha256_file(clip),
                frame_sha256=sha256_file(frame),
            )
            row.source_video_url = source_url
            row.source_video_id = "canonical-id"
            dataset = tmp_path / f"{hotkey}-dataset.parquet"
            manifest = tmp_path / f"{hotkey}-manifest.json"
            write_dataset_parquet([row], dataset)
            write_manifest(
                IntervalManifest(
                    netuid=1,
                    miner_hotkey=hotkey,
                    interval_id=interval_id,
                    record_count=1,
                    dataset_sha256=sha256_file(dataset),
                    created_at=created_at,
                ),
                manifest,
            )
            await stores[hotkey].upload_file(f"{key_base}/dataset.parquet", dataset)
            await stores[hotkey].upload_file(f"{key_base}/manifest.json", manifest)
            await stores[hotkey].upload_file(f"{key_base}/{row.clip_uri}", clip)
            await stores[hotkey].upload_file(f"{key_base}/{row.first_frame_uri}", frame)

        monkeypatch.setattr("nexis.validator.pipeline.select_miners", lambda hotkeys, _seed: list(hotkeys))
        pipeline = ValidatorPipeline(store_for_hotkey=_store_for_hotkey)
        decisions, _weights = await pipeline.validate_interval(
            candidate_hotkeys=["miner1", "miner2", "miner3"],
            interval_id=interval_id,
        )
        by_hotkey = {item.miner_hotkey: item for item in decisions}
        assert by_hotkey["miner1"].notes["cross_miner_overlap_pruned_count"] == 0
        assert by_hotkey["miner2"].notes["cross_miner_overlap_pruned_count"] == 1
        assert by_hotkey["miner3"].notes["cross_miner_overlap_pruned_count"] == 0

    run_async(run())


def test_validator_runs_hard_checks_on_full_records(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    async def run() -> None:
        store = LocalObjectStore(tmp_path / "store")
        hotkey = "miner1"
        interval_id = 70
        key_base = f"{interval_id}"

        clip1 = tmp_path / "c1.mp4"
        frame1 = tmp_path / "c1.jpg"
        clip2 = tmp_path / "c2.mp4"
        frame2 = tmp_path / "c2.jpg"
        clip1.write_bytes(b"clip1")
        frame1.write_bytes(b"frame1")
        clip2.write_bytes(b"clip2")
        frame2.write_bytes(b"frame2")
        rows = [
            _record("c1", 0.0, clip_sha256=sha256_file(clip1), frame_sha256=sha256_file(frame1)),
            _record("c2", 8.0, clip_sha256=sha256_file(clip2), frame_sha256=sha256_file(frame2)),
        ]
        rows[1].source_video_url = "https://example.com/not-youtube"
        dataset = tmp_path / "dataset.parquet"
        manifest = tmp_path / "manifest.json"
        write_dataset_parquet(rows, dataset)
        write_manifest(
            IntervalManifest(
                netuid=1,
                miner_hotkey=hotkey,
                interval_id=interval_id,
                record_count=2,
                dataset_sha256=sha256_file(dataset),
            ),
            manifest,
        )
        await store.upload_file(f"{key_base}/dataset.parquet", dataset)
        await store.upload_file(f"{key_base}/manifest.json", manifest)
        for row, clip, frame in [(rows[0], clip1, frame1), (rows[1], clip2, frame2)]:
            await store.upload_file(f"{key_base}/{row.clip_uri}", clip)
            await store.upload_file(f"{key_base}/{row.first_frame_uri}", frame)

        monkeypatch.setattr("nexis.validator.pipeline.select_row_indices", lambda *_args, **_kwargs: [0])
        pipeline = ValidatorPipeline(store_for_hotkey=lambda _: store)
        decisions, _weights = await pipeline.validate_interval(
            candidate_hotkeys=[hotkey],
            interval_id=interval_id,
        )
        assert decisions[0].accepted is False
        assert any(item.startswith("non_youtube_source:") for item in decisions[0].failures)

    run_async(run())


def test_source_auth_only_validator_rejects_on_source_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    async def run() -> None:
        store = LocalObjectStore(tmp_path / "store")
        hotkey = "miner1"
        interval_id = 80
        key_base = f"{interval_id}"

        clip = tmp_path / "clip.mp4"
        frame = tmp_path / "frame.jpg"
        clip.write_bytes(b"clip")
        frame.write_bytes(b"frame")
        row = _record(
            "c1",
            0.0,
            clip_sha256=sha256_file(clip),
            frame_sha256=sha256_file(frame),
        )
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
        await store.upload_file(f"{key_base}/dataset.parquet", dataset)
        await store.upload_file(f"{key_base}/manifest.json", manifest)
        await store.upload_file(f"{key_base}/{row.clip_uri}", clip)
        await store.upload_file(f"{key_base}/{row.first_frame_uri}", frame)

        monkeypatch.setattr(
            "nexis.validator.pipeline.ValidatorPipeline._check_source_authenticity",
            lambda *_args, **_kwargs: ["source_frame_mismatch:c1"],
        )
        pipeline = ValidatorPipeline(
            store_for_hotkey=lambda _: store,
            source_authenticity_enabled=True,
            source_auth_only=True,
        )
        decisions, _weights = await pipeline.validate_interval(
            candidate_hotkeys=[hotkey],
            interval_id=interval_id,
        )
        assert decisions[0].accepted is False
        assert "source_frame_mismatch:c1" in decisions[0].failures

    run_async(run())


def test_source_auth_only_fail_open_on_download_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    async def run() -> None:
        store = LocalObjectStore(tmp_path / "store")
        hotkey = "miner1"
        interval_id = 81
        key_base = f"{interval_id}"

        clip = tmp_path / "clip.mp4"
        frame = tmp_path / "frame.jpg"
        clip.write_bytes(b"clip")
        frame.write_bytes(b"frame")
        row = _record(
            "c1",
            0.0,
            clip_sha256=sha256_file(clip),
            frame_sha256=sha256_file(frame),
        )
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
        await store.upload_file(f"{key_base}/dataset.parquet", dataset)
        await store.upload_file(f"{key_base}/manifest.json", manifest)
        await store.upload_file(f"{key_base}/{row.first_frame_uri}", frame)

        monkeypatch.setattr(
            "nexis.validator.pipeline.download_youtube_video",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("anti bot")),
        )
        pipeline = ValidatorPipeline(
            store_for_hotkey=lambda _: store,
            source_authenticity_enabled=True,
            source_auth_only=True,
        )
        decisions, _weights = await pipeline.validate_interval(
            candidate_hotkeys=[hotkey],
            interval_id=interval_id,
        )
        assert len(decisions) == 1
        assert decisions[0].accepted is True

    run_async(run())


def test_validator_pipeline_skips_api_invalid_hotkeys(tmp_path: Path) -> None:
    async def run() -> None:
        store = LocalObjectStore(tmp_path / "store")
        pipeline = ValidatorPipeline(store_for_hotkey=lambda _: store)
        decisions, weights = await pipeline.validate_interval(
            candidate_hotkeys=["hk1", "hk2"],
            interval_id=99,
            invalid_hotkeys={"hk1", "hk2"},
        )
        assert decisions == []
        assert weights == {}

    run_async(run())

