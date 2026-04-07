"""Anti-cheat checks for sampled clip rows."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Callable
from urllib.parse import urlparse

from ..models import ClipRecord
from ..protocol import MIN_CLIP_GAP_SEC


@dataclass
class CheckResult:
    failures: list[str]


HardCheckRule = Callable[[list[ClipRecord]], list[str]]
_MAX_CLIP_START_SEC = 5000.0


def default_hard_check_rules() -> list[HardCheckRule]:
    return [
        _check_youtube_sources,
        _check_overlap,
        _check_caption_alignment,
        _check_clip_start_limit,
    ]


def run_hard_checks(records: list[ClipRecord], rules: list[HardCheckRule] | None = None) -> CheckResult:
    failures: list[str] = []
    for rule in (rules or default_hard_check_rules()):
        failures.extend(rule(records))
    return CheckResult(failures=failures)


def _check_youtube_sources(records: list[ClipRecord]) -> list[str]:
    failures: list[str] = []
    for row in records:
        host = (urlparse(row.source_video_url).hostname or "").lower()
        if not _is_allowed_youtube_host(host):
            failures.append(f"non_youtube_source:{row.clip_id}")
    return failures


def _is_allowed_youtube_host(host: str) -> bool:
    if not host:
        return False
    if host == "youtube.com" or host.endswith(".youtube.com"):
        return True
    if host == "youtu.be":
        return True
    return False


def _check_overlap(records: list[ClipRecord]) -> list[str]:
    by_source: dict[str, list[ClipRecord]] = defaultdict(list)
    for row in records:
        source_key = row.source_video_id.strip() or row.source_video_url
        by_source[source_key].append(row)

    failures: list[str] = []
    for source, clips in by_source.items():
        ordered = sorted(clips, key=lambda c: c.clip_start_sec)
        for prev, curr in zip(ordered, ordered[1:]):
            if curr.clip_start_sec - prev.clip_start_sec < (MIN_CLIP_GAP_SEC - 0.5):
                failures.append(
                    f"overlap_lt_5s:{source}:{prev.clip_id}:{curr.clip_id}"
                )
    return failures


def _check_caption_alignment(records: list[ClipRecord]) -> list[str]:
    """Simple lexical guardrail.

    This is intentionally conservative for v1:
    - empty captions fail
    - extremely short captions fail
    - captions that only repeat URL-like strings fail
    """
    failures: list[str] = []
    for row in records:
        text = row.caption.strip().lower()
        if not text:
            failures.append(f"empty_caption:{row.clip_id}")
            continue
        if len(text.split()) < 20:
            failures.append(f"short_caption:{row.clip_id}")
            continue
        if "http://" in text or "https://" in text:
            failures.append(f"url_like_caption:{row.clip_id}")
    return failures


def _check_clip_start_limit(records: list[ClipRecord]) -> list[str]:
    failures: list[str] = []
    for row in records:
        if row.clip_start_sec > _MAX_CLIP_START_SEC:
            failures.append(f"clip_start_gt_5000:{row.clip_id}")
    return failures

