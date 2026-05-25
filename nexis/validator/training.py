"""Owner-trainer orchestrator for the `nexis train` command.

Trainer container expectations (matching `rendixnetwork/train:latest`):

  Internal (container-side) paths the image expects:
    - /workspace/training/Wan2.2DatasetAnalsis/h100_dataset_training/models
    - /workspace/training/Wan2.2DatasetAnalsis/h100_dataset_training/runs
    - /workspace/training/Wan2.2DatasetAnalsis/h100_dataset_training/config.json
    - /workspace/training/<dataset_name>          (read-only, ${DATASET_MANIFEST} points inside)
    - /workspace/eval_data                        (read-only)
    - /workspace/outputs                          (writable; final eval output dir)

The host paths are configurable via NEXIS_TRAINER_* env vars. Per-cycle
miner-specific dirs (runs, outputs, dataset) live inside the cycle workdir.

Phase ordering:
  1. Validate datasets, gather candidates.
  2. Train ALL accepted miners (8-GPU pool by default).
  3. After every training container exits, upload all miners' outputs to
     `nexis_miner/{cycle_id}/{miner_hotkey}/...` sequentially.
  4. Persist training_state.json and clean up the cycle scratch dir.
"""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from ..config import Settings
from ..protocol import CROSS_MINER_OVERLAP_REJECT_THRESHOLD
from ..serialization import read_dataset_parquet, read_manifest
from ..storage.r2 import R2S3Store
from ..storage.shared_bucket import NexisMinerBucket
from .dataset_check import (
    DatasetCheckOutcome,
    build_overlap_index,
    count_index_overlap,
    latest_complete_interval_id,
    validate_miner_dataset,
)
from .dataset_convert import convert_to_trainer_manifest
from .docker_runner import DockerGPUPool, DockerRunResult

logger = logging.getLogger(__name__)


# Trainer container internal paths (do NOT change without coordinating with the image).
TRAIN_CONTAINER_MODELS_DIR = "/workspace/training/Wan2.2DatasetAnalsis/h100_dataset_training/models"
TRAIN_CONTAINER_RUNS_DIR = "/workspace/training/Wan2.2DatasetAnalsis/h100_dataset_training/runs"
TRAIN_CONTAINER_CONFIG_JSON = "/workspace/training/Wan2.2DatasetAnalsis/h100_dataset_training/config.json"
TRAIN_CONTAINER_DATASET_BASE = "/workspace/training"
TRAIN_CONTAINER_EVAL_DATA = "/workspace/eval_data"
TRAIN_CONTAINER_OUTPUTS = "/workspace/outputs"


@dataclass
class TrainingCandidate:
    miner_hotkey: str
    interval_id: int
    miner_dir: Path


@dataclass
class TrainedMiner:
    miner_hotkey: str
    interval_id: int
    outputs_dir: Path
    # Path to the per-miner dataset dir (containing dataset.parquet + clips/).
    # Kept around through the upload phase so dataset_index.json can be
    # generated from the same parquet the trainer consumed.
    miner_dir: Path


@dataclass
class TrainingCycleResult:
    cycle_id: int
    accepted: list[str] = field(default_factory=list)
    rejected: list[str] = field(default_factory=list)
    trained: list[str] = field(default_factory=list)
    failed_training: list[str] = field(default_factory=list)
    uploaded: list[str] = field(default_factory=list)
    failed_upload: list[str] = field(default_factory=list)


def _train_state_path(workdir: Path) -> Path:
    return workdir / "training_state.json"


def load_training_state(workdir: Path) -> dict[str, int]:
    """Return dict[miner_hotkey] -> last trained interval_id."""
    path = _train_state_path(workdir)
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    return {str(k): int(v) for k, v in raw.items() if isinstance(v, int)}


def save_training_state(workdir: Path, state: dict[str, int]) -> None:
    path = _train_state_path(workdir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


async def select_eligible_hotkeys(
    *,
    candidate_hotkeys: list[str],
    invalid_hotkeys: set[str],
    blacklist_hotkeys: set[str],
    last_winners: set[str],
) -> list[str]:
    """Pick miners to validate this cycle.

    Eligibility rule:
        (last_winner OR not in invalid_hotkeys) AND not in blacklist_hotkeys

    Blacklisted hotkeys are unconditionally excluded — winning the previous
    cycle does not grant an exemption from the blacklist. `invalid_hotkeys`
    is the soft "already-selected" set that last cycle's winners can override.
    """
    eligible: list[str] = []
    for hotkey in candidate_hotkeys:
        if hotkey in blacklist_hotkeys:
            continue
        if hotkey in last_winners or hotkey not in invalid_hotkeys:
            eligible.append(hotkey)
    return eligible


def parse_last_winners(total_score_payload: dict[str, Any] | None, top_k: int = 5) -> set[str]:
    """Top-K hotkeys by aggregate score from a `total_score.json` payload."""
    if not total_score_payload:
        return set()
    scores = total_score_payload.get("scores")
    if not isinstance(scores, dict):
        return set()
    flat: list[tuple[str, float]] = []
    for hotkey, entry in scores.items():
        if isinstance(entry, dict):
            value = entry.get("aggregate", entry.get("score"))
        else:
            value = entry
        try:
            score = float(value)
        except (TypeError, ValueError):
            continue
        flat.append((str(hotkey), score))
    flat.sort(key=lambda pair: (-pair[1], pair[0]))
    return {hotkey for hotkey, _ in flat[:top_k]}


def build_train_volumes(
    *,
    settings: Settings,
    miner_dir: Path,
    miner_hotkey: str,
    runs_dir: Path,
    outputs_dir: Path,
    eval_data_dir: Path,
    config_json: Path | None = None,
) -> list[tuple[Path | str, Path | str, str]]:
    """Build the docker -v mounts for the trainer container.

    Container-side paths are fixed (the trainer image hardcodes them); host
    paths come from settings + per-miner workdir.

    `eval_data_dir` is the freshly-synced local copy of the network's eval
    dataset (typically `<workdir>/eval_data`). The caller is responsible
    for downloading it from the nexis-eval bucket before invoking this.

    All host paths are resolved to absolute paths because docker treats a
    relative path on the left of `-v` as a named-volume identifier, which
    silently creates an empty volume instead of bind-mounting the directory.
    """
    dataset_container_path = f"{TRAIN_CONTAINER_DATASET_BASE}/{miner_hotkey}"
    return [
        (Path(settings.trainer_models_dir).resolve(), TRAIN_CONTAINER_MODELS_DIR, ""),
        (Path(runs_dir).resolve(), TRAIN_CONTAINER_RUNS_DIR, ""),
        (Path(settings.trainer_config_json).resolve(), TRAIN_CONTAINER_CONFIG_JSON, ""),
        (Path(miner_dir).resolve(), dataset_container_path, "ro"),
        (Path(eval_data_dir).resolve(), TRAIN_CONTAINER_EVAL_DATA, "ro"),
        (Path(outputs_dir).resolve(), TRAIN_CONTAINER_OUTPUTS, ""),
    ]


def trainer_command() -> list[str]:
    return [
        "bash",
        "-c",
        (
            "python 02_train_dataset.py && "
            "python 05_eval_with_images.py "
            f"--eval_manifest {TRAIN_CONTAINER_EVAL_DATA}/manifest.jsonl "
            f"--output_dir {TRAIN_CONTAINER_OUTPUTS}"
        ),
    ]


async def run_train_container(
    *,
    settings: Settings,
    candidate: TrainingCandidate,
    pool: DockerGPUPool,
    cycle_id: int,
    workdir: Path,
    eval_data_dir: Path,
) -> Path | None:
    """Train one miner. Returns the local outputs_dir if successful, else None.

    NB: this function does NOT upload anything. Uploads are deferred until
    every container in the cycle has finished.
    """
    miner_dir = candidate.miner_dir
    miner_hotkey = candidate.miner_hotkey

    # Convert parquet -> manifest.jsonl in-place (the trainer image reads it via
    # DATASET_MANIFEST). Paths inside the manifest must be CONTAINER paths, since
    # the trainer image opens them after the host bind-mount remaps the location.
    # Captions are guaranteed non-empty by dataset_check.validate_miner_dataset.
    container_dataset_dir = f"{TRAIN_CONTAINER_DATASET_BASE}/{miner_hotkey}"
    convert_to_trainer_manifest(
        miner_dir=miner_dir,
        container_dataset_dir=container_dataset_dir,
    )

    runs_dir = workdir / "runs" / miner_hotkey
    outputs_dir = workdir / "outputs" / miner_hotkey
    runs_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir.mkdir(parents=True, exist_ok=True)

    # Per-miner config.json copy: the trainer image rewrites this file
    # in place at startup, so a single shared inode across parallel
    # containers races (miner A clobbers the manifest field that miner
    # B's entrypoint just wrote, then B's python step opens config.json
    # and tries to load A's manifest). Each miner gets its own copy.
    config_src = Path(settings.trainer_config_json).resolve()
    miner_config_path = workdir / "configs" / miner_hotkey / "config.json"
    miner_config_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(config_src, miner_config_path)

    volumes = build_train_volumes(
        settings=settings,
        miner_dir=miner_dir,
        miner_hotkey=miner_hotkey,
        runs_dir=runs_dir,
        outputs_dir=outputs_dir,
        eval_data_dir=eval_data_dir,
        config_json=miner_config_path,
    )
    env = {
        "DATASET_MANIFEST": f"{TRAIN_CONTAINER_DATASET_BASE}/{miner_hotkey}/manifest.jsonl",
    }
    result: DockerRunResult = await pool.run(
        image=settings.trainer_docker_image,
        command=trainer_command(),
        volumes=volumes,
        env=env,
        shm_size=settings.trainer_shm_size,
        timeout_sec=settings.trainer_timeout_sec,
    )
    if not result.success:
        logger.error(
            "training failed miner=%s cycle=%d rc=%d\nstderr:\n%s\nstdout:\n%s",
            miner_hotkey,
            cycle_id,
            result.returncode,
            result.stderr,
            result.stdout[-2000:],
        )
        return None

    if not any(outputs_dir.rglob("*")):
        logger.error(
            "training produced no outputs miner=%s cycle=%d outputs_dir=%s",
            miner_hotkey,
            cycle_id,
            outputs_dir,
        )
        return None

    logger.info(
        "training complete miner=%s cycle=%d outputs_dir=%s",
        miner_hotkey,
        cycle_id,
        outputs_dir,
    )
    return outputs_dir


async def upload_miner_outputs(
    *,
    nexis_miner: NexisMinerBucket,
    trained: TrainedMiner,
    cycle_id: int,
    workdir: Path,
    upload_concurrency: int = 8,
) -> bool:
    miner_hotkey = trained.miner_hotkey
    outputs_dir = trained.outputs_dir
    files = sorted(p for p in outputs_dir.rglob("*") if p.is_file())
    sem = asyncio.Semaphore(max(int(upload_concurrency), 1))

    async def _upload_one(path: Path) -> bool:
        rel = path.relative_to(outputs_dir)
        key = f"{cycle_id}/{miner_hotkey}/{rel.as_posix()}"
        async with sem:
            try:
                await nexis_miner.upload_path(key, path)
                return True
            except Exception as exc:
                logger.warning(
                    "upload failed miner=%s key=%s err=%s",
                    miner_hotkey,
                    key,
                    exc,
                )
                return False

    results = await asyncio.gather(*[_upload_one(p) for p in files])
    uploaded = sum(1 for ok in results if ok)
    if uploaded != len(files):
        logger.warning(
            "partial upload miner=%s cycle=%d uploaded=%d of=%d",
            miner_hotkey,
            cycle_id,
            uploaded,
            len(files),
        )
        return False
    if uploaded == 0:
        logger.warning(
            "no files to upload miner=%s cycle=%d outputs_dir=%s",
            miner_hotkey,
            cycle_id,
            outputs_dir,
        )
        return False
    done_marker = workdir / "done" / miner_hotkey / "_done.json"
    done_marker.parent.mkdir(parents=True, exist_ok=True)
    done_marker.write_text(
        json.dumps(
            {
                "miner_hotkey": miner_hotkey,
                "cycle_id": cycle_id,
                "miner_interval_id": trained.interval_id,
                "uploaded_files": uploaded,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    await nexis_miner.upload_path(f"{cycle_id}/{miner_hotkey}/_done.json", done_marker)

    # dataset_index.json: list of (source_url, clip_start_sec) for every row in
    # the dataset we just trained on. The API uses this when the OWNER posts
    # scores to update `record_info.json` (top-5 miners feed the global
    # overlap snapshot). Built from the same parquet the trainer consumed,
    # so it's authoritative for THIS cycle's training data.
    parquet_path = trained.miner_dir / "dataset.parquet"
    if parquet_path.exists():
        try:
            records = read_dataset_parquet(parquet_path)
        except Exception as exc:
            logger.warning(
                "dataset_index skipped miner=%s cycle=%d err=%s",
                miner_hotkey,
                cycle_id,
                exc,
            )
        else:
            index_payload = [
                {
                    "source_url": row.source_video_url,
                    "clip_start_sec": float(row.clip_start_sec),
                }
                for row in records
            ]
            index_local = workdir / "index" / f"{miner_hotkey}_dataset_index.json"
            index_local.parent.mkdir(parents=True, exist_ok=True)
            index_local.write_text(
                json.dumps(index_payload, ensure_ascii=True),
                encoding="utf-8",
            )
            await nexis_miner.upload_path(
                f"{cycle_id}/{miner_hotkey}/dataset_index.json",
                index_local,
            )
            logger.info(
                "dataset_index uploaded miner=%s cycle=%d rows=%d",
                miner_hotkey,
                cycle_id,
                len(index_payload),
            )

    logger.info(
        "uploaded miner=%s cycle=%d files=%d",
        miner_hotkey,
        cycle_id,
        uploaded,
    )
    return True


async def _miner_upload_time(
    store: R2S3Store, interval_id: int, manifest_path: Path
) -> Any:
    """Return the most-trustworthy timestamp we have for when this miner
    uploaded its dataset. Prefer R2's LastModified on dataset.parquet (set
    by R2 at PUT time, unspoofable by the miner); fall back to the manifest's
    self-reported `created_at` if the head request fails."""
    try:
        ts = await store.get_object_last_modified(f"{interval_id}/dataset.parquet")
        if ts is not None:
            return ts
    except Exception as exc:
        logger.warning(
            "last-modified lookup failed interval=%d err=%s", interval_id, exc
        )
    try:
        return read_manifest(manifest_path).created_at
    except Exception:
        return None


async def _filter_cross_miner_overlap(
    candidates: list[TrainingCandidate],
    store_for_hotkey: Callable[[str], R2S3Store],
) -> tuple[list[TrainingCandidate], list[DatasetCheckOutcome]]:
    """Reject candidates whose dataset overlaps an *earlier-uploaded* candidate
    by more than CROSS_MINER_OVERLAP_REJECT_THRESHOLD rows.

    Overlap is measured on (canonical_source_url, clip_start_sec) within
    ±OVERLAP_WINDOW_SEC. The earlier uploader (by R2 LastModified on
    dataset.parquet) keeps its slot; later uploaders that exceed the
    threshold against any kept candidate are dropped.
    """
    if len(candidates) < 2:
        return candidates, []

    enriched: list[tuple[TrainingCandidate, Any, dict[str, list[float]], int]] = []
    for cand in candidates:
        parquet_path = cand.miner_dir / "dataset.parquet"
        manifest_path = cand.miner_dir / "manifest.json"
        try:
            records = read_dataset_parquet(parquet_path)
        except Exception as exc:
            logger.warning(
                "cross-miner: parquet re-read failed hotkey=%s err=%s",
                cand.miner_hotkey,
                exc,
            )
            continue
        try:
            store = store_for_hotkey(cand.miner_hotkey)
            upload_time = await _miner_upload_time(
                store, cand.interval_id, manifest_path
            )
        except Exception:
            upload_time = None
        enriched.append((cand, upload_time, build_overlap_index(records), len(records)))

    # Sort by upload_time ascending; ties broken by hotkey for determinism.
    # Candidates with no resolvable timestamp sort last (they have no priority
    # claim and cannot displace anyone who does).
    enriched.sort(
        key=lambda t: (t[1] is None, t[1], t[0].miner_hotkey)
    )

    kept: list[tuple[TrainingCandidate, Any, dict[str, list[float]], int]] = []
    rejections: list[DatasetCheckOutcome] = []
    for cand, upload_time, index, record_count in enriched:
        rejected_by: tuple[str, int] | None = None
        for kept_cand, _, kept_index, _ in kept:
            count = count_index_overlap(index, kept_index)
            if count > CROSS_MINER_OVERLAP_REJECT_THRESHOLD:
                rejected_by = (kept_cand.miner_hotkey, count)
                break
        if rejected_by is not None:
            other_hk, count = rejected_by
            logger.warning(
                "cross-miner overlap reject hotkey=%s vs earlier=%s count=%d > %d",
                cand.miner_hotkey,
                other_hk,
                count,
                CROSS_MINER_OVERLAP_REJECT_THRESHOLD,
            )
            rejections.append(
                DatasetCheckOutcome(
                    accepted=False,
                    miner_hotkey=cand.miner_hotkey,
                    interval_id=cand.interval_id,
                    record_count=record_count,
                    failures=[
                        f"cross_miner_overlap:{other_hk}:{count}"
                    ],
                )
            )
        else:
            kept.append((cand, upload_time, index, record_count))

    return [t[0] for t in kept], rejections


async def gather_candidates(
    *,
    eligible_hotkeys: list[str],
    store_for_hotkey: Callable[[str], R2S3Store],
    workdir: Path,
    cycle_id: int,
    training_state: dict[str, int],
    global_record_index: dict[str, list[float]],
    miner_concurrency: int = 4,
    download_concurrency: int = 16,
) -> tuple[list[TrainingCandidate], list[DatasetCheckOutcome]]:
    """Validate candidate miners in parallel.

    Each miner runs `validate_miner_dataset` independently behind a
    semaphore; within a single miner, asset downloads are themselves
    parallelized via `download_concurrency`.  Effective concurrent GETs
    against R2 ≈ miner_concurrency × download_concurrency.
    """
    cycle_workdir = workdir / "cycle" / str(cycle_id)
    miner_sem = asyncio.Semaphore(max(int(miner_concurrency), 1))

    async def _process(
        hotkey: str,
    ) -> tuple[str, TrainingCandidate | None, DatasetCheckOutcome | None]:
        async with miner_sem:
            try:
                miner_store = store_for_hotkey(hotkey)
            except Exception as exc:
                logger.warning("store unavailable for hotkey=%s err=%s", hotkey, exc)
                return hotkey, None, None
            try:
                interval_id = await latest_complete_interval_id(miner_store)
            except Exception as exc:
                logger.warning("interval lookup failed hotkey=%s err=%s", hotkey, exc)
                return hotkey, None, None
            if interval_id is None:
                logger.info("hotkey=%s has no uploaded interval; skipping", hotkey)
                return hotkey, None, None
            last_seen = training_state.get(hotkey)
            if last_seen is not None and interval_id <= last_seen:
                logger.info(
                    "hotkey=%s latest interval %d already trained at cycle %d; skipping",
                    hotkey,
                    interval_id,
                    last_seen,
                )
                return hotkey, None, None
            outcome = await validate_miner_dataset(
                miner_hotkey=hotkey,
                interval_id=interval_id,
                miner_store=miner_store,
                workdir=cycle_workdir,
                global_record_index=global_record_index,
                download_concurrency=download_concurrency,
            )
            if not outcome.accepted:
                logger.warning(
                    "dataset rejected hotkey=%s interval=%d failures=%s",
                    hotkey,
                    interval_id,
                    outcome.failures,
                )
                return hotkey, None, outcome
            return (
                hotkey,
                TrainingCandidate(
                    miner_hotkey=hotkey,
                    interval_id=interval_id,
                    miner_dir=cycle_workdir / hotkey / str(interval_id),
                ),
                None,
            )

    results = await asyncio.gather(*[_process(hk) for hk in eligible_hotkeys])
    candidates: list[TrainingCandidate] = []
    rejections: list[DatasetCheckOutcome] = []
    for _, cand, outcome in results:
        if cand is not None:
            candidates.append(cand)
        elif outcome is not None:
            rejections.append(outcome)

    # Cross-miner overlap: drop later-uploaders whose datasets duplicate an
    # earlier accepted miner's by > CROSS_MINER_OVERLAP_REJECT_THRESHOLD rows.
    # First-uploader wins by R2's LastModified on dataset.parquet (with the
    # manifest's created_at as a fallback).
    candidates, cross_miner_rejections = await _filter_cross_miner_overlap(
        candidates, store_for_hotkey
    )
    rejections.extend(cross_miner_rejections)
    return candidates, rejections


async def determine_next_cycle_id(nexis_miner: NexisMinerBucket) -> int | None:
    """Return the cycle_id to train, or None if the previous cycle hasn't finished scoring."""
    latest = await nexis_miner.latest_cycle_id()
    if latest is None:
        return 1
    if not await nexis_miner.has_total_score(latest):
        return None
    return latest + 1


async def cleanup_workdir(path: Path) -> None:
    if not path.exists():
        return
    try:
        shutil.rmtree(path)
    except Exception as exc:
        logger.warning("workdir cleanup failed path=%s err=%s", path, exc)


async def run_training_cycle(
    *,
    settings: Settings,
    candidate_hotkeys: list[str],
    invalid_hotkeys: set[str],
    blacklist_hotkeys: set[str],
    last_total_score: dict[str, Any] | None,
    store_for_hotkey: Callable[[str], R2S3Store],
    nexis_miner: NexisMinerBucket,
    pool: DockerGPUPool,
    cycle_id: int,
    workdir: Path,
    global_record_index: dict[str, list[float]],
    eval_data_dir: Path,
    on_select: Callable[[list[str], int], Any] | None = None,
) -> TrainingCycleResult:
    last_winners = parse_last_winners(last_total_score)
    eligible = await select_eligible_hotkeys(
        candidate_hotkeys=candidate_hotkeys,
        invalid_hotkeys=invalid_hotkeys,
        blacklist_hotkeys=blacklist_hotkeys,
        last_winners=last_winners,
    )
    logger.info(
        "training cycle=%d candidates=%d eligible=%d invalid=%d blacklist=%d "
        "last_winners=%d",
        cycle_id,
        len(candidate_hotkeys),
        len(eligible),
        len(invalid_hotkeys),
        len(blacklist_hotkeys),
        len(last_winners),
    )

    training_state = load_training_state(workdir)
    candidates, rejections = await gather_candidates(
        eligible_hotkeys=eligible,
        store_for_hotkey=store_for_hotkey,
        workdir=workdir,
        cycle_id=cycle_id,
        training_state=training_state,
        global_record_index=global_record_index,
        miner_concurrency=getattr(settings, "miner_gather_concurrency", 4),
        download_concurrency=getattr(settings, "download_concurrency", 16),
    )

    selected_hotkeys = [c.miner_hotkey for c in candidates]
    rejected_hotkeys = [
        outcome.miner_hotkey for outcome in rejections if outcome.miner_hotkey
    ]
    # Mark BOTH accepted and rejected miners as invalid for this network:
    #   * accepted   → "we already trained on this miner; don't pick again
    #                   unless they win a top-5 slot"
    #   * rejected   → "this miner's last upload failed strict validation;
    #                   don't waste time re-validating the same bad dataset"
    # Either path keeps a miner out of the eligibility pool until they make
    # the previous-cycle's top-5 (the only re-entry route).
    hotkeys_to_invalidate = sorted({*selected_hotkeys, *rejected_hotkeys})
    if on_select and hotkeys_to_invalidate:
        maybe = on_select(hotkeys_to_invalidate, cycle_id)
        if asyncio.iscoroutine(maybe):
            await maybe

    cycle_result = TrainingCycleResult(
        cycle_id=cycle_id,
        accepted=selected_hotkeys,
        rejected=rejected_hotkeys,
    )

    cycle_scratch = workdir / "cycle" / str(cycle_id)

    # Phase 2: TRAIN ALL miners (no uploads yet). 8 in parallel via GPU pool.
    async def _train(candidate: TrainingCandidate) -> tuple[TrainingCandidate, Path | None]:
        outputs_dir = await run_train_container(
            settings=settings,
            candidate=candidate,
            pool=pool,
            cycle_id=cycle_id,
            workdir=cycle_scratch,
            eval_data_dir=eval_data_dir,
        )
        return candidate, outputs_dir

    trained_miners: list[TrainedMiner] = []
    if candidates:
        results = await asyncio.gather(*[_train(c) for c in candidates])
        for candidate, outputs_dir in results:
            if outputs_dir is None:
                cycle_result.failed_training.append(candidate.miner_hotkey)
                continue
            cycle_result.trained.append(candidate.miner_hotkey)
            trained_miners.append(
                TrainedMiner(
                    miner_hotkey=candidate.miner_hotkey,
                    interval_id=candidate.interval_id,
                    outputs_dir=outputs_dir,
                    miner_dir=candidate.miner_dir,
                )
            )
        logger.info(
            "training phase complete cycle=%d trained=%d failed=%d",
            cycle_id,
            len(cycle_result.trained),
            len(cycle_result.failed_training),
        )

    # Phase 3: UPLOAD all successful outputs.  Per-miner uploads run in
    # parallel (one task per miner); within each miner, individual files
    # are also uploaded concurrently up to `upload_concurrency`.
    upload_conc = max(int(getattr(settings, "upload_concurrency", 8)), 1)

    async def _upload_one(trained: TrainedMiner) -> tuple[str, bool]:
        try:
            ok = await upload_miner_outputs(
                nexis_miner=nexis_miner,
                trained=trained,
                cycle_id=cycle_id,
                workdir=cycle_scratch,
                upload_concurrency=upload_conc,
            )
        except Exception as exc:
            logger.exception(
                "upload exception miner=%s cycle=%d: %s",
                trained.miner_hotkey,
                cycle_id,
                exc,
            )
            ok = False
        return trained.miner_hotkey, ok

    if trained_miners:
        upload_results = await asyncio.gather(*[_upload_one(t) for t in trained_miners])
        for hotkey, ok in upload_results:
            if ok:
                cycle_result.uploaded.append(hotkey)
            else:
                cycle_result.failed_upload.append(hotkey)

    # Persist training_state only for fully-successful miners (trained + uploaded).
    for trained in trained_miners:
        if trained.miner_hotkey in cycle_result.uploaded:
            training_state[trained.miner_hotkey] = trained.interval_id
    save_training_state(workdir, training_state)

    # Cleanup scratch (cycle workdir).
    await cleanup_workdir(cycle_scratch)
    return cycle_result
