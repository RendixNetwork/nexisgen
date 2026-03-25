from __future__ import annotations

from pathlib import Path

from nexis.models import ClipRecord
from nexis.validator.assets import VideoAssetVerifier


def _row() -> ClipRecord:
    return ClipRecord(
        clip_id="c1",
        clip_uri="clips/c1.mp4",
        clip_sha256="a" * 64,
        first_frame_uri="frames/c1.jpg",
        first_frame_sha256="b" * 64,
        source_video_id="ytid",
        split_group_id="ytid:1",
        split="train",
        clip_start_sec=0.0,
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


def test_resolution_check_accepts_1280x720(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    verifier = VideoAssetVerifier()
    row = _row()

    monkeypatch.setattr(
        "nexis.validator.assets.probe_video",
        lambda _path: {"streams": [{"codec_type": "video", "width": 1280, "height": 720}]},
    )

    assert verifier._verify_resolution(row=row, clip_path=Path("dummy.mp4")) is None


def test_resolution_check_rejects_non_1280x720(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    verifier = VideoAssetVerifier()
    row = _row()

    monkeypatch.setattr(
        "nexis.validator.assets.probe_video",
        lambda _path: {"streams": [{"codec_type": "video", "width": 640, "height": 360}]},
    )

    assert (
        verifier._verify_resolution(row=row, clip_path=Path("dummy.mp4"))
        == "invalid_resolution:c1:640x360"
    )
