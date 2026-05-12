"""Miner interval pipeline: build a 400-sample dataset and upload it."""

from __future__ import annotations

import logging
import math
import shutil
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from ..hash_utils import deterministic_clip_id, sha256_file
from ..models import ClipRecord, IntervalManifest
from ..protocol import (
    CLIP_DURATION_SEC,
    OVERLAP_WINDOW_SEC,
    PROTOCOL_VERSION,
    SAMPLE_COUNT,
    SCHEMA_VERSION,
    TARGET_FPS,
    TARGET_HEIGHT,
    TARGET_NUM_FRAMES,
    TARGET_WIDTH,
)
from ..serialization import write_dataset_parquet, write_manifest
from ..specs import DEFAULT_SPEC_ID
from .captioner import Captioner
from .providers import GenericSourceProvider, SourceProvider

logger = logging.getLogger(__name__)


def _video_stream(info: dict[str, Any]) -> dict[str, Any]:
    for stream in info.get("streams", []):
        if stream.get("codec_type") == "video":
            return stream
    raise ValueError("video stream not found")


def _canonical_url(url: str) -> str:
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


class MinerPipeline:
    """Build a strict-spec 400-sample dataset from public video sources."""

    def __init__(
        self,
        store: Any,
        source_provider: SourceProvider | None = None,
        spec_id: str = DEFAULT_SPEC_ID,
        sample_count: int = SAMPLE_COUNT,
        captioner: Captioner | None = None,
    ):
        self.store = store
        self.source_provider = source_provider or GenericSourceProvider()
        self.spec_id = spec_id
        self.sample_count = sample_count
        # No-op captioner if none supplied / no API key — produces empty
        # captions and the trainer falls back to its default prompt.
        self.captioner = captioner or Captioner()

    async def run_interval(
        self,
        *,
        sources_file: Path,
        netuid: int,
        miner_hotkey: str,
        interval_id: int,
        workdir: Path,
    ) -> tuple[Path, Path]:
        if interval_id < 1:
            raise ValueError("interval_id must be >= 1")
        logger.info(
            "miner pipeline start interval_id=%d hotkey=%s sample_count=%d",
            interval_id,
            miner_hotkey,
            self.sample_count,
        )
        workdir.mkdir(parents=True, exist_ok=True)
        raw_dir = workdir / "raw"
        clips_dir = workdir / "clips"
        frames_dir = workdir / "frames"
        out_dir = workdir / "out" / str(interval_id)
        if out_dir.exists():
            shutil.rmtree(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        records: list[ClipRecord] = []
        assets_to_upload: dict[str, Path] = {}
        seen_positions: dict[str, list[float]] = {}

        urls = list(self.source_provider.read_sources(sources_file))
        if not urls:
            raise RuntimeError(f"no sources defined in {sources_file}")

        for url in urls:
            if len(records) >= self.sample_count:
                break
            canonical = _canonical_url(url)
            source_id = self.source_provider.source_video_id(url)
            logger.info("processing source source_id=%s url=%s", source_id, url)
            try:
                raw_path = self.source_provider.download(url, raw_dir)
            except Exception as exc:
                logger.warning("source download failed url=%s err=%s", url, exc)
                continue

            try:
                probe = self.source_provider.probe(raw_path)
            except Exception as exc:
                logger.warning("source probe failed path=%s err=%s", raw_path, exc)
                continue

            duration = float(probe.get("format", {}).get("duration") or 0.0)
            total_segments = int(math.floor(duration / CLIP_DURATION_SEC))
            if total_segments <= 0:
                logger.warning("source has no usable segments source_id=%s", source_id)
                continue

            stream = _video_stream(probe)
            src_width = int(stream.get("width", 0) or 0)
            src_height = int(stream.get("height", 0) or 0)
            if src_width < TARGET_WIDTH or src_height < TARGET_HEIGHT:
                logger.warning(
                    "source resolution below target (got %dx%d, need >= %dx%d) source_id=%s",
                    src_width,
                    src_height,
                    TARGET_WIDTH,
                    TARGET_HEIGHT,
                    source_id,
                )
                continue

            for idx in range(total_segments):
                if len(records) >= self.sample_count:
                    break
                start = float(idx) * CLIP_DURATION_SEC

                # Within-dataset overlap protection.
                positions = seen_positions.setdefault(canonical, [])
                if any(abs(start - prev) < OVERLAP_WINDOW_SEC for prev in positions):
                    continue

                clip_id = deterministic_clip_id(source_id, start, CLIP_DURATION_SEC)
                clip_path = clips_dir / f"{clip_id}.mp4"
                frame_path = frames_dir / f"{clip_id}.jpg"
                try:
                    self.source_provider.create_clip(raw_path, clip_path, start)
                    self.source_provider.extract_first_frame(clip_path, frame_path)
                except Exception as exc:
                    logger.warning(
                        "clip extraction failed source_id=%s start=%.3f err=%s",
                        source_id,
                        start,
                        exc,
                    )
                    continue

                positions.append(start)
                caption = self.captioner.caption_frame(frame_path) if self.captioner.enabled else ""
                record = ClipRecord(
                    clip_id=clip_id,
                    clip_uri=f"clips/{clip_path.name}",
                    clip_sha256=sha256_file(clip_path),
                    first_frame_uri=f"frames/{frame_path.name}",
                    first_frame_sha256=sha256_file(frame_path),
                    source_video_id=source_id,
                    clip_start_sec=start,
                    duration_sec=CLIP_DURATION_SEC,
                    caption=caption,
                    width=TARGET_WIDTH,
                    height=TARGET_HEIGHT,
                    fps=float(TARGET_FPS),
                    num_frames=TARGET_NUM_FRAMES,
                    source_video_url=canonical,
                )
                records.append(record)
                assets_to_upload[record.clip_uri] = clip_path
                assets_to_upload[record.first_frame_uri] = frame_path

        if len(records) != self.sample_count:
            raise RuntimeError(
                f"failed to produce {self.sample_count} samples from sources "
                f"(got {len(records)}); add more URLs to sources.txt"
            )

        dataset_path = out_dir / "dataset.parquet"
        write_dataset_parquet(records, dataset_path)
        logger.info("dataset written interval=%d records=%d", interval_id, len(records))
        manifest = IntervalManifest(
            protocol_version=PROTOCOL_VERSION,
            schema_version=SCHEMA_VERSION,
            spec_id=self.spec_id,
            netuid=netuid,
            miner_hotkey=miner_hotkey,
            interval_id=interval_id,
            record_count=len(records),
            dataset_sha256=sha256_file(dataset_path),
        )
        manifest_path = out_dir / "manifest.json"
        write_manifest(manifest, manifest_path)

        base_key = f"{interval_id}"
        await self.store.upload_file(f"{base_key}/dataset.parquet", dataset_path, use_write=True)
        for relative_uri, local_path in assets_to_upload.items():
            await self.store.upload_file(
                f"{base_key}/{relative_uri.lstrip('/')}",
                local_path,
                use_write=True,
            )
        # Manifest last: signals the upload is complete.
        await self.store.upload_file(f"{base_key}/manifest.json", manifest_path, use_write=True)
        logger.info("uploaded interval package hotkey=%s interval=%d", miner_hotkey, interval_id)
        return dataset_path, manifest_path
