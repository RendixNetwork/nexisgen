"""VBench-based scoring of trained-model outputs."""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..config import Settings
from ..storage.shared_bucket import NexisMinerBucket
from .docker_runner import DockerRunResult, run_docker_one_off

logger = logging.getLogger(__name__)


@dataclass
class MinerScore:
    miner_hotkey: str
    aggregate: float
    # `dimensions`: per-dimension aggregate score (the VBench `[0]` value).
    dimensions: dict[str, float] = field(default_factory=dict)
    # `full_dimensions`: raw VBench output keyed by dimension name.  Carries
    # the whole `[aggregate, [{video_path, video_results}, ...]]` blob so the
    # per-validator JSON written to the shared bucket has the full breakdown,
    # not just the aggregates.
    full_dimensions: dict[str, Any] = field(default_factory=dict)
    # Miner-side interval_id of the dataset that produced these outputs.
    # Pulled from `_done.json` written by the trainer; None if missing.
    miner_interval_id: int | None = None


async def _read_trained_interval_id(
    *,
    nexis_miner: NexisMinerBucket,
    cycle_id: int,
    miner_hotkey: str,
    workdir: Path,
) -> int | None:
    """Return the miner-side interval_id that THIS cycle actually trained on.

    The trainer wrote `nexis_miner/{cycle_id}/{miner_hotkey}/_done.json` with
    `miner_interval_id = <the interval the trainer used>` at the moment of
    upload. We read it here to attach the same value to the score we POST,
    independent of whatever interval may now be the latest in the miner's
    own R2 bucket.
    """
    local = workdir / "done" / f"{miner_hotkey}_done.json"
    local.parent.mkdir(parents=True, exist_ok=True)
    try:
        ok = await nexis_miner.store.download_file(
            f"{cycle_id}/{miner_hotkey}/_done.json", local
        )
    except Exception as exc:
        logger.warning("_done.json fetch failed miner=%s err=%s", miner_hotkey, exc)
        return None
    if not ok or not local.exists():
        return None
    try:
        data = json.loads(local.read_text(encoding="utf-8"))
        return int(data.get("miner_interval_id"))
    except Exception:
        return None


def parse_vbench_results(results_dir: Path) -> dict[str, float]:
    """Aggregate VBench result JSONs in `results_dir` into {dimension: score}.

    VBench writes each dimension as a 2-element list::

        {"<dimension_name>": [aggregate_score, [{video_path, video_results}, ...]]}

    We extract only `aggregate_score` (index 0); the per-video list at index 1
    is debug detail and intentionally ignored so miners with more eval videos
    don't dominate the mean. When VBench wrote the same dimension into
    multiple result files, we average the per-file aggregates.
    """
    dimensions: dict[str, list[float]] = {}
    for path in results_dir.rglob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        for dim_name, dim_value in payload.items():
            score = _extract_dimension_aggregate(dim_value)
            if score is None:
                continue
            dimensions.setdefault(str(dim_name), []).append(score)
    return {k: sum(vals) / len(vals) for k, vals in dimensions.items() if vals}


def _extract_dimension_aggregate(value: Any) -> float | None:
    """Pull the dimension aggregate from VBench's `[score, [per_video...]]` shape.

    Falls back to treating `value` as a raw float, since some VBench builds
    write bare numbers for simpler dimensions.
    """
    if isinstance(value, list) and value:
        head = value[0]
        if isinstance(head, (int, float)) and not isinstance(head, bool):
            return float(head)
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return None


def aggregate_score(dimensions: dict[str, float]) -> float:
    if not dimensions:
        return 0.0
    return sum(dimensions.values()) / float(len(dimensions))


# VBench iterates `os.listdir(videos_path)` and takes the suffix of the first
# file it finds to figure out what extension every "video" in the directory
# has.  If even one non-video file (e.g. `_done.json`, `dataset_index.json`)
# lands in the same dir and happens to come first in directory-enumeration
# order, VBench searches for `<prompt>.json` files, finds none, processes 0
# videos, and later crashes with ZeroDivisionError in subject_consistency.
# Restricting the download to known video extensions sidesteps the bug.
_VIDEO_EXTENSIONS = frozenset({".mp4", ".avi", ".mov", ".webm", ".gif"})


async def download_miner_videos(
    *,
    nexis_miner: NexisMinerBucket,
    cycle_id: int,
    miner_hotkey: str,
    dest_dir: Path,
    download_concurrency: int = 16,
) -> Path:
    keys = await nexis_miner.list_miner_files(cycle_id, miner_hotkey)
    dest_dir.mkdir(parents=True, exist_ok=True)
    sem = asyncio.Semaphore(max(int(download_concurrency), 1))

    async def _fetch(key: str) -> None:
        rel = "/".join(key.split("/")[2:])  # strip "{cycle}/{hotkey}/"
        if not rel:
            return
        if Path(rel).suffix.lower() not in _VIDEO_EXTENSIONS:
            # Skip _done.json, dataset_index.json, and any other sidecar
            # files the trainer uploaded — they confuse VBench's
            # extension-sniffing in build_full_info_json.
            return
        local = dest_dir / rel
        async with sem:
            try:
                ok = await nexis_miner.store.download_file(key, local)
            except Exception as exc:
                logger.warning("download exception key=%s err=%s", key, exc)
                ok = False
        if not ok:
            logger.warning("download failed key=%s", key)

    await asyncio.gather(*[_fetch(k) for k in keys])
    return dest_dir


async def score_miner(
    *,
    settings: Settings,
    cycle_id: int,
    miner_hotkey: str,
    nexis_miner: NexisMinerBucket,
    workdir: Path,
    eval_data_dir: Path,
) -> MinerScore | None:
    miner_videos_dir = workdir / "videos" / miner_hotkey
    results_dir = workdir / "results" / miner_hotkey
    if results_dir.exists():
        shutil.rmtree(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    await download_miner_videos(
        nexis_miner=nexis_miner,
        cycle_id=cycle_id,
        miner_hotkey=miner_hotkey,
        dest_dir=miner_videos_dir,
        download_concurrency=int(getattr(settings, "download_concurrency", 16)),
    )

    if not any(miner_videos_dir.rglob("*")):
        logger.warning("no videos for miner=%s cycle=%d; skipping", miner_hotkey, cycle_id)
        return None

    dimensions = [d.strip() for d in settings.vbench_dimensions.split(",") if d.strip()]
    extra: list[str] = ["--dimension", *dimensions, "--load_ckpt_from_local", "True"]
    # Resolve to absolute host paths: docker treats relative left-side as a
    # named-volume identifier and would silently create empty volumes.
    volumes = [
        (Path(eval_data_dir).resolve(), "/eval_data", "ro"),
        (Path(miner_videos_dir).resolve(), "/videos", "ro"),
        (Path(results_dir).resolve(), "/results", ""),
    ]
    env = {
        "EVAL_DATA": "/eval_data",
        "GENERATED_VIDEOS": "/videos",
        "RESULTS": "/results",
    }
    result: DockerRunResult = await run_docker_one_off(
        image=settings.vbench_docker_image,
        command=extra,
        volumes=volumes,
        env=env,
        gpu_spec="all",
        timeout_sec=settings.vbench_timeout_sec,
    )
    if not result.success:
        logger.error(
            "vbench failed miner=%s cycle=%d rc=%d\nstderr:\n%s\nstdout:\n%s",
            miner_hotkey,
            cycle_id,
            result.returncode,
            result.stderr,
            result.stdout[-2000:],
        )
        return None

    dim_scores = parse_vbench_results(results_dir)
    aggregate = aggregate_score(dim_scores)
    # Merge every VBench result JSON's top-level keys into one map of
    # dimension_name -> raw VBench value (`[aggregate, [per_video...]]`).
    # If multiple files report the same dimension, the latest wins — which
    # is fine because VBench writes one file per dimension by default.
    full_dimensions: dict[str, Any] = {}
    for path in sorted(results_dir.rglob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        for dim_name, dim_value in payload.items():
            full_dimensions[str(dim_name)] = dim_value

    miner_interval_id = await _read_trained_interval_id(
        nexis_miner=nexis_miner,
        cycle_id=cycle_id,
        miner_hotkey=miner_hotkey,
        workdir=workdir,
    )
    return MinerScore(
        miner_hotkey=miner_hotkey,
        aggregate=aggregate,
        dimensions=dim_scores,
        full_dimensions=full_dimensions,
        miner_interval_id=miner_interval_id,
    )


async def score_cycle(
    *,
    settings: Settings,
    cycle_id: int,
    nexis_miner: NexisMinerBucket,
    workdir: Path,
    eval_data_dir: Path,
) -> dict[str, MinerScore]:
    miner_hotkeys = await nexis_miner.list_miner_dirs(cycle_id)
    logger.info("scoring cycle=%d miners=%d", cycle_id, len(miner_hotkeys))
    scores: dict[str, MinerScore] = {}
    for miner_hotkey in miner_hotkeys:
        try:
            ms = await score_miner(
                settings=settings,
                cycle_id=cycle_id,
                miner_hotkey=miner_hotkey,
                nexis_miner=nexis_miner,
                workdir=workdir,
                eval_data_dir=eval_data_dir,
            )
        except Exception as exc:
            logger.exception(
                "scoring exception miner=%s cycle=%d: %s",
                miner_hotkey,
                cycle_id,
                exc,
            )
            ms = None
        if ms is not None:
            scores[miner_hotkey] = ms
    return scores


async def cleanup_score_workdir(path: Path) -> None:
    if not path.exists():
        return
    try:
        shutil.rmtree(path)
    except Exception as exc:
        logger.warning("score workdir cleanup failed path=%s err=%s", path, exc)


def build_score_payload(cycle_id: int, scores: dict[str, MinerScore]) -> dict:
    """Build the `{cycle_id, scores: {...}}` payload that is both saved locally
    (LocalScoreStore) and POSTed to the API — identical shape so downstream
    parsers (`parse_score_payload`) work on either source."""
    return {
        "cycle_id": int(cycle_id),
        "scores": {
            hotkey: {
                "aggregate": float(ms.aggregate),
                "dimensions": ms.dimensions,
                "full_dimensions": ms.full_dimensions,
                "miner_interval_id": ms.miner_interval_id,
            }
            for hotkey, ms in scores.items()
        },
    }


async def submit_scores(
    *,
    reporter: Any,
    cycle_id: int,
    scores: dict[str, MinerScore],
) -> bool:
    """POST /v1/training-scores via the reporter."""
    return await reporter.post_training_scores(
        payload=build_score_payload(cycle_id, scores)
    )


# Light no-op so flake8 doesn't complain about asyncio import-only usage in some runs.
_ = asyncio
