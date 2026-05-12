"""Source acquisition and clip extraction utilities for miners.

Despite the historical name, this module supports any URL handled by yt-dlp
(YouTube, Vimeo, Twitch, etc.) and re-encodes clips to the strict v2 spec
(1280x704, 24fps, 121 frames).
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
from pathlib import Path

from ..protocol import (
    CLIP_DURATION_SEC,
    TARGET_FPS,
    TARGET_HEIGHT,
    TARGET_NUM_FRAMES,
    TARGET_WIDTH,
)

YT_DLP_DOWNLOAD_TIMEOUT_SECONDS = 600
FFPROBE_TIMEOUT_SEC = 30
FFMPEG_TIMEOUT_SEC = 240
YTDLP_RETRIES = 2
logger = logging.getLogger(__name__)


def _build_yt_dlp_cmd(args: list[str]) -> list[str]:
    return ["yt-dlp", *args]


def _run_command(
    cmd: list[str],
    *,
    timeout_sec: int,
    capture_output: bool = False,
    text: bool = False,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        check=True,
        timeout=timeout_sec,
        capture_output=capture_output,
        text=text,
    )


def _run_subprocess(
    cmd: list[str],
    *,
    timeout: int,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        timeout=timeout,
        capture_output=True,
        text=True,
        check=False,
    )


def read_sources(path: Path) -> list[str]:
    lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]
    return [line for line in lines if line and not line.startswith("#")]


def download_source_video(url: str, output_dir: Path) -> Path:
    """Download a video from any yt-dlp supported URL."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_template = str(output_dir / "%(extractor)s_%(id)s.%(ext)s")
    # Source resolution should be at least the target resolution; downscale later in ffmpeg.
    height_floor = TARGET_HEIGHT
    cmd = _build_yt_dlp_cmd(
        [
            "--no-simulate",
            "-f",
            (
                f"bestvideo[height>={height_floor}]+bestaudio/"
                f"bestvideo+bestaudio/best"
            ),
            "--merge-output-format",
            "mp4",
            "--recode-video",
            "mp4",
            "-o",
            output_template,
            "--no-playlist",
            "--no-overwrites",
            "--print",
            "after_move:filepath",
            url,
        ]
    )
    last_error: Exception | None = None
    for attempt in range(1, YTDLP_RETRIES + 1):
        try:
            logger.info("yt-dlp download start url=%s attempt=%d/%d", url, attempt, YTDLP_RETRIES)
            result = _run_subprocess(
                cmd,
                timeout=YT_DLP_DOWNLOAD_TIMEOUT_SECONDS,
            )
            if result.returncode != 0:
                logger.warning(
                    "yt-dlp download failed attempt=%d/%d rc=%d err=%s",
                    attempt,
                    YTDLP_RETRIES,
                    result.returncode,
                    (result.stderr or "")[:200],
                )
                last_error = RuntimeError(
                    f"yt-dlp download failed: {(result.stderr or '')[:200]}"
                )
                continue

            for line in reversed(result.stdout.splitlines()):
                candidate = line.strip()
                if candidate and Path(candidate).exists():
                    logger.info("yt-dlp download complete url=%s path=%s", url, candidate)
                    return Path(candidate)

            mp4_candidates = sorted(
                output_dir.glob("*.mp4"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            if mp4_candidates:
                logger.info(
                    "yt-dlp download complete (fallback) url=%s path=%s",
                    url,
                    mp4_candidates[0],
                )
                return mp4_candidates[0]
            last_error = RuntimeError("yt-dlp completed but output file was not found")
        except (subprocess.TimeoutExpired, OSError) as exc:
            logger.warning("yt-dlp download exception attempt=%d/%d: %s", attempt, YTDLP_RETRIES, exc)
            last_error = exc
    raise RuntimeError(f"failed to download source video for url={url}") from last_error


# Back-compat alias retained for any external callers.
download_youtube_video = download_source_video


def probe_video(path: Path) -> dict:
    logger.debug("ffprobe start path=%s", path)
    cmd = [
        "ffprobe",
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_streams",
        "-show_format",
        str(path),
    ]
    proc = _run_command(
        cmd,
        timeout_sec=FFPROBE_TIMEOUT_SEC,
        capture_output=True,
        text=True,
    )
    payload = json.loads(proc.stdout)
    logger.debug("ffprobe complete path=%s", path)
    return payload


def create_clip(src: Path, dst: Path, start_sec: float) -> None:
    """Create a clip re-encoded to the strict spec (1280x704, 24fps, 121 frames, no audio)."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    # Crop-to-fit then scale to TARGET_WIDTH x TARGET_HEIGHT to preserve aspect on most sources.
    vf = (
        f"scale={TARGET_WIDTH}:{TARGET_HEIGHT}:force_original_aspect_ratio=increase,"
        f"crop={TARGET_WIDTH}:{TARGET_HEIGHT},"
        f"fps={TARGET_FPS}"
    )
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-ss",
        f"{start_sec:.3f}",
        "-i",
        str(src),
        "-frames:v",
        str(TARGET_NUM_FRAMES),
        "-vf",
        vf,
        "-an",
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-preset",
        "veryfast",
        "-crf",
        "20",
        str(dst),
    ]
    logger.debug(
        "ffmpeg create clip src=%s dst=%s start=%.3f frames=%d",
        src,
        dst,
        start_sec,
        TARGET_NUM_FRAMES,
    )
    _run_command(cmd, timeout_sec=FFMPEG_TIMEOUT_SEC)


def extract_first_frame(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        str(src),
        "-vf",
        "select=eq(n\\,0)",
        "-frames:v",
        "1",
        str(dst),
    ]
    logger.debug("ffmpeg extract first frame src=%s dst=%s", src, dst)
    _run_command(cmd, timeout_sec=FFMPEG_TIMEOUT_SEC)


def get_video_duration_sec(path: Path) -> float:
    info = probe_video(path)
    duration_str = info.get("format", {}).get("duration")
    try:
        return float(duration_str)
    except (TypeError, ValueError):
        return 0.0


_ = (CLIP_DURATION_SEC, os)  # quiet unused import warnings; CLIP_DURATION_SEC kept for callers
