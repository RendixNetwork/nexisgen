"""Source provider abstraction for miner ingestion."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol
from urllib.parse import parse_qs, urlparse

from .youtube import (
    create_clip,
    download_source_video,
    extract_first_frame,
    probe_video,
    read_sources,
)


class SourceProvider(Protocol):
    def read_sources(self, path: Path) -> list[str]: ...

    def source_video_id(self, url: str) -> str: ...

    def download(self, url: str, output_dir: Path) -> Path: ...

    def probe(self, path: Path) -> dict[str, Any]: ...

    def create_clip(self, src: Path, dst: Path, start_sec: float) -> None: ...

    def extract_first_frame(self, src: Path, dst: Path) -> None: ...


class GenericSourceProvider:
    """yt-dlp-backed provider that supports any public video platform."""

    def read_sources(self, path: Path) -> list[str]:
        return read_sources(path)

    def source_video_id(self, url: str) -> str:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        if host == "youtu.be":
            return parsed.path.strip("/") or url
        if host == "youtube.com" or host.endswith(".youtube.com"):
            query = parse_qs(parsed.query)
            values = query.get("v", [])
            if values and values[0].strip():
                return values[0].strip()
            parts = [part for part in parsed.path.split("/") if part]
            if len(parts) >= 2 and parts[0] in {"shorts", "embed", "v"}:
                return parts[1]
        if host:
            return f"{host}_{parsed.path.strip('/').replace('/', '_') or 'root'}"
        return url

    def download(self, url: str, output_dir: Path) -> Path:
        return download_source_video(url, output_dir)

    def probe(self, path: Path) -> dict[str, Any]:
        return probe_video(path)

    def create_clip(self, src: Path, dst: Path, start_sec: float) -> None:
        create_clip(src, dst, start_sec)

    def extract_first_frame(self, src: Path, dst: Path) -> None:
        extract_first_frame(src, dst)


# Back-compat alias.
YouTubeSourceProvider = GenericSourceProvider
