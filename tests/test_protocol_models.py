"""Validate v2 spec-tightening on ClipRecord and IntervalManifest."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from nexis.models import ClipRecord, IntervalManifest
from nexis.protocol import (
    CLIP_DURATION_SEC,
    SAMPLE_COUNT,
    TARGET_FPS,
    TARGET_HEIGHT,
    TARGET_NUM_FRAMES,
    TARGET_WIDTH,
)


def _good_record() -> dict:
    return {
        "clip_id": "abc123",
        "clip_uri": "clips/abc.mp4",
        "clip_sha256": "f" * 64,
        "first_frame_uri": "frames/abc.jpg",
        "first_frame_sha256": "e" * 64,
        "source_video_id": "vid",
        "clip_start_sec": 0.0,
        "duration_sec": CLIP_DURATION_SEC,
        "width": TARGET_WIDTH,
        "height": TARGET_HEIGHT,
        "fps": float(TARGET_FPS),
        "num_frames": TARGET_NUM_FRAMES,
        "source_video_url": "https://example.com/v",
    }


def test_clip_record_accepts_target_spec() -> None:
    ClipRecord(**_good_record())


def test_clip_record_rejects_bad_width() -> None:
    payload = _good_record() | {"width": 1920}
    with pytest.raises(ValidationError):
        ClipRecord(**payload)


def test_clip_record_rejects_bad_height() -> None:
    payload = _good_record() | {"height": 720}
    with pytest.raises(ValidationError):
        ClipRecord(**payload)


def test_clip_record_rejects_bad_num_frames() -> None:
    payload = _good_record() | {"num_frames": 120}
    with pytest.raises(ValidationError):
        ClipRecord(**payload)


def test_clip_record_rejects_bad_fps() -> None:
    payload = _good_record() | {"fps": 30.0}
    with pytest.raises(ValidationError):
        ClipRecord(**payload)


def test_manifest_requires_400_records() -> None:
    payload = {
        "spec_id": "video_v1",
        "netuid": 1,
        "miner_hotkey": "5xyz",
        "interval_id": 1,
        "record_count": SAMPLE_COUNT - 1,
        "dataset_sha256": "a" * 64,
    }
    with pytest.raises(ValidationError):
        IntervalManifest(**payload)


def test_manifest_accepts_400_records() -> None:
    payload = {
        "spec_id": "video_v1",
        "netuid": 1,
        "miner_hotkey": "5xyz",
        "interval_id": 1,
        "record_count": SAMPLE_COUNT,
        "dataset_sha256": "a" * 64,
    }
    IntervalManifest(**payload)


def test_manifest_interval_id_must_be_positive() -> None:
    payload = {
        "spec_id": "video_v1",
        "netuid": 1,
        "miner_hotkey": "5xyz",
        "interval_id": 0,
        "record_count": SAMPLE_COUNT,
        "dataset_sha256": "a" * 64,
    }
    with pytest.raises(ValidationError):
        IntervalManifest(**payload)
