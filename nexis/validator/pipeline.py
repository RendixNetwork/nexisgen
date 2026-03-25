"""Validator pipeline: discover, sample, validate, score."""

from __future__ import annotations

import hashlib
import inspect
import logging
import shutil
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from pathlib import PurePosixPath
from typing import Any, Callable, Iterable, Protocol

from ..hash_utils import sha256_file
from ..miner.youtube import download_youtube_video
from ..models import ClipRecord, IntervalManifest, ValidationDecision
from ..protocol import MIN_CLIP_GAP_SEC
from ..scoring import MinerIntervalScore, WeightComputer
from ..serialization import read_dataset_parquet_as_model, read_manifest
from ..specs import DEFAULT_SPEC_ID, DatasetSpecRegistry
from .sampling import select_miners, select_row_indices

logger = logging.getLogger(__name__)
_OVERLAP_WINDOW_SEC = MIN_CLIP_GAP_SEC - 0.5
_SOURCE_AUTH_MAX_ROWS = 3
_SOURCE_FRAME_COMPARE_SIZE = 32
_SOURCE_FRAME_MAE_THRESHOLD = 18.0


@dataclass
class LoadedMinerSubmission:
    hotkey: str
    interval_id: int
    key_base: str
    store: Any
    miner_dir: Path
    dataset_local: Path
    manifest_local: Path
    manifest_last_modified: datetime | None
    manifest: IntervalManifest
    records: list[ClipRecord]
    spec_id: str


@dataclass
class IntervalArtifacts:
    interval_id: int
    records_by_hotkey: dict[str, list[ClipRecord]]
    manifests_by_hotkey: dict[str, IntervalManifest]


class CaptionSemanticCheckerLike(Protocol):
    def check(
        self,
        *,
        sampled: list[ClipRecord],
        frame_paths_by_clip_id: dict[str, list[Path]],
    ) -> list[str]: ...


class ValidatorPipeline:
    def __init__(
        self,
        store_for_hotkey: Callable[[str], Any],
        weight_computer: WeightComputer | None = None,
        caption_semantic_checker: CaptionSemanticCheckerLike | None = None,
        source_authenticity_enabled: bool = False,
        source_auth_only: bool = False,
        spec_registry: DatasetSpecRegistry | None = None,
        enabled_specs: list[str] | None = None,
    ):
        self._store_for_hotkey = store_for_hotkey
        self.weight_computer = weight_computer or WeightComputer()
        self._caption_semantic_checker = caption_semantic_checker
        self._source_authenticity_enabled = source_authenticity_enabled
        self._source_auth_only = source_auth_only
        self._spec_registry = spec_registry or DatasetSpecRegistry.with_defaults()
        if enabled_specs:
            self._enabled_specs = {item.strip() for item in enabled_specs if item.strip()}
        else:
            self._enabled_specs = set(self._spec_registry.list_spec_ids())
        if not self._enabled_specs:
            self._enabled_specs = {DEFAULT_SPEC_ID}
        self.last_interval_artifacts = IntervalArtifacts(
            interval_id=-1,
            records_by_hotkey={},
            manifests_by_hotkey={},
        )

    async def discover_active_miners(self, hotkeys: Iterable[str], interval_id: int) -> list[str]:
        active: list[str] = []
        for hotkey in hotkeys:
            store = self._store_for_hotkey(hotkey)
            manifest_key = f"{interval_id}/manifest.json"
            if await store.object_exists(manifest_key):
                active.append(hotkey)
            else:
                logger.debug(
                    "inactive miner hotkey=%s interval=%d reason=manifest_missing",
                    hotkey,
                    interval_id,
                )
        logger.info("active miners discovered interval=%d active=%d", interval_id, len(active))
        return active

    async def validate_interval(
        self,
        *,
        candidate_hotkeys: list[str],
        interval_id: int,
        interval_seed: str | None = None,
        workdir: Path | None = None,
        global_record_index: dict[str, list[float]] | None = None,
        invalid_hotkeys: set[str] | None = None,
    ) -> tuple[list[ValidationDecision], dict[str, float]]:
        interval_seed = interval_seed or self._default_interval_seed(interval_id)
        invalid_hotkey_set = set(invalid_hotkeys or set())
        eligible_hotkeys = [
            hotkey
            for hotkey in candidate_hotkeys
            if hotkey not in invalid_hotkey_set
        ]
        sampled_hotkeys = select_miners(eligible_hotkeys, interval_seed)
        selected = list(sampled_hotkeys)
        rejected_from_invalids = [hotkey for hotkey in candidate_hotkeys if hotkey not in eligible_hotkeys]
        logger.info(
            "selected %d miners from %d candidates (%d excluded by API invalid list)",
            len(selected),
            len(candidate_hotkeys),
            len(rejected_from_invalids),
        )
        logger.debug(
            "validator interval_seed=%s eligible_hotkeys=%s sampled_hotkeys=%s selected_hotkeys=%s",
            interval_seed,
            ",".join(eligible_hotkeys),
            ",".join(sampled_hotkeys),
            ",".join(selected),
        )

        base_workdir = workdir or Path(".nexis/validator")
        interval_dirs_to_cleanup = {
            base_workdir / hotkey / str(interval_id) for hotkey in selected
        }
        decisions: list[ValidationDecision] = []
        loaded_submissions: list[LoadedMinerSubmission] = []
        acceptance_map: dict[str, bool] = {}

        try:
            for hotkey in selected:
                loaded, failed = await self._load_submission(
                    hotkey=hotkey,
                    interval_id=interval_id,
                    workdir=base_workdir,
                )
                if failed is not None:
                    decisions.append(failed)
                    acceptance_map[hotkey] = failed.accepted
                    continue
                if loaded is not None:
                    loaded_submissions.append(loaded)

            global_pruned = self._prune_overlaps_with_global_index(
                loaded_submissions,
                global_record_index or {},
            )
            cross_miner_pruned = self._prune_cross_miner_overlaps(loaded_submissions)

            scores: list[MinerIntervalScore] = []
            for loaded in loaded_submissions:
                decision = await self._evaluate_loaded_submission(
                    loaded=loaded,
                    interval_seed=interval_seed,
                    global_pruned_count=global_pruned.get(loaded.hotkey, 0),
                    cross_miner_pruned_count=cross_miner_pruned.get(loaded.hotkey, 0),
                )
                decisions.append(decision)
                acceptance_map[loaded.hotkey] = decision.accepted
                logger.info(
                    "decision hotkey=%s interval=%d accepted=%s sampled_rows=%d failures=%d",
                    loaded.hotkey,
                    interval_id,
                    str(decision.accepted),
                    decision.sampled_rows,
                    len(decision.failures),
                )
                scores.append(
                    MinerIntervalScore(
                        miner_hotkey=loaded.hotkey,
                        interval_id=interval_id,
                        accepted=decision.accepted,
                        passed_sample_count=decision.sampled_rows if decision.accepted else 0,
                    )
                )

            self.last_interval_artifacts = IntervalArtifacts(
                interval_id=interval_id,
                records_by_hotkey={entry.hotkey: list(entry.records) for entry in loaded_submissions},
                manifests_by_hotkey={entry.hotkey: entry.manifest for entry in loaded_submissions},
            )

            weights = self.weight_computer.compute_weights(scores)
            logger.debug("computed weights interval=%d entries=%d", interval_id, len(weights))
            return decisions, weights
        finally:
            self._cleanup_interval_dirs(interval_dirs_to_cleanup)

    def _cleanup_interval_dirs(self, interval_dirs: set[Path]) -> None:
        for interval_dir in interval_dirs:
            try:
                if interval_dir.exists():
                    shutil.rmtree(interval_dir)
                try:
                    interval_dir.parent.rmdir()
                except OSError:
                    pass
            except Exception as exc:
                logger.warning(
                    "failed to remove validator interval cache dir=%s error=%s",
                    interval_dir,
                    exc,
                )

    async def _load_submission(
        self,
        *,
        hotkey: str,
        interval_id: int,
        workdir: Path,
    ) -> tuple[LoadedMinerSubmission | None, ValidationDecision | None]:
        store = self._store_for_hotkey(hotkey)
        key_base = f"{interval_id}"
        dataset_key = f"{key_base}/dataset.parquet"
        manifest_key = f"{key_base}/manifest.json"
        miner_dir = workdir / hotkey / str(interval_id)
        miner_dir.mkdir(parents=True, exist_ok=True)
        dataset_local = miner_dir / "dataset.parquet"
        manifest_local = miner_dir / "manifest.json"

        try:
            has_manifest = await store.download_file(manifest_key, manifest_local)
            has_dataset = await store.download_file(dataset_key, dataset_local)
            if not has_manifest or not has_dataset:
                logger.warning(
                    "missing submission files hotkey=%s interval=%d manifest=%s dataset=%s",
                    hotkey,
                    interval_id,
                    str(has_manifest),
                    str(has_dataset),
                )
                return None, ValidationDecision(
                    miner_hotkey=hotkey,
                    interval_id=interval_id,
                    accepted=False,
                    failures=["missing_submission_files"],
                    sampled_rows=0,
                )

            try:
                manifest = read_manifest(manifest_local)
            except Exception as exc:
                logger.warning(
                    "manifest schema invalid hotkey=%s interval=%d error=%s",
                    hotkey,
                    interval_id,
                    exc,
                )
                return None, ValidationDecision(
                    miner_hotkey=hotkey,
                    interval_id=interval_id,
                    accepted=False,
                    failures=["manifest_schema_invalid"],
                    sampled_rows=0,
                    notes={"error": str(exc)},
                )
            if manifest.miner_hotkey != hotkey or manifest.interval_id != interval_id:
                logger.warning(
                    "manifest identity mismatch hotkey=%s interval=%d manifest_hotkey=%s manifest_interval=%d",
                    hotkey,
                    interval_id,
                    manifest.miner_hotkey,
                    manifest.interval_id,
                )
                return None, ValidationDecision(
                    miner_hotkey=hotkey,
                    interval_id=interval_id,
                    accepted=False,
                    failures=["manifest_identity_mismatch"],
                    sampled_rows=0,
                )
            compatibility = self._spec_registry.compatibility(
                spec_id=manifest.spec_id,
                protocol_version=manifest.protocol_version,
                schema_version=manifest.schema_version,
            )
            if not compatibility.compatible:
                logger.warning(
                    "manifest compatibility failure hotkey=%s interval=%d reason=%s",
                    hotkey,
                    interval_id,
                    compatibility.reason,
                )
                return None, ValidationDecision(
                    miner_hotkey=hotkey,
                    interval_id=interval_id,
                    accepted=False,
                    failures=[compatibility.reason],
                    sampled_rows=0,
                )
            if manifest.spec_id not in self._enabled_specs:
                logger.warning(
                    "manifest spec not enabled hotkey=%s interval=%d spec_id=%s",
                    hotkey,
                    interval_id,
                    manifest.spec_id,
                )
                return None, ValidationDecision(
                    miner_hotkey=hotkey,
                    interval_id=interval_id,
                    accepted=False,
                    failures=[f"spec_not_enabled:{manifest.spec_id}"],
                    sampled_rows=0,
                )
            spec = self._spec_registry.get(manifest.spec_id)
            actual_dataset_sha256 = sha256_file(dataset_local)
            if manifest.dataset_sha256 != actual_dataset_sha256:
                logger.warning("dataset sha mismatch hotkey=%s interval=%d", hotkey, interval_id)
                return None, ValidationDecision(
                    miner_hotkey=hotkey,
                    interval_id=interval_id,
                    accepted=False,
                    failures=["dataset_sha256_mismatch"],
                    sampled_rows=0,
                    notes={
                        "expected_dataset_sha256": manifest.dataset_sha256,
                        "actual_dataset_sha256": actual_dataset_sha256,
                    },
                )

            try:
                records = read_dataset_parquet_as_model(dataset_local, spec.row_model)
            except Exception as exc:
                logger.warning(
                    "dataset schema invalid hotkey=%s interval=%d error=%s",
                    hotkey,
                    interval_id,
                    exc,
                )
                return None, ValidationDecision(
                    miner_hotkey=hotkey,
                    interval_id=interval_id,
                    accepted=False,
                    failures=["dataset_schema_invalid"],
                    sampled_rows=0,
                    notes={"error": str(exc)},
                )
            if manifest.record_count != len(records):
                logger.warning(
                    "record count mismatch hotkey=%s interval=%d manifest=%d dataset=%d",
                    hotkey,
                    interval_id,
                    manifest.record_count,
                    len(records),
                )
                return None, ValidationDecision(
                    miner_hotkey=hotkey,
                    interval_id=interval_id,
                    accepted=False,
                    failures=["manifest_record_count_mismatch"],
                    sampled_rows=0,
                    notes={
                        "manifest_record_count": manifest.record_count,
                        "dataset_record_count": len(records),
                    },
                )
            return (
                LoadedMinerSubmission(
                    hotkey=hotkey,
                    interval_id=interval_id,
                    key_base=key_base,
                    store=store,
                    miner_dir=miner_dir,
                    dataset_local=dataset_local,
                    manifest_local=manifest_local,
                    manifest_last_modified=await self._manifest_last_modified(
                        store=store,
                        manifest_key=manifest_key,
                    ),
                    manifest=manifest,
                    records=records,
                    spec_id=manifest.spec_id,
                ),
                None,
            )
        except Exception as exc:
            logger.exception(
                "validator miner load failed hotkey=%s interval=%d: %s",
                hotkey,
                interval_id,
                exc,
            )
            return None, ValidationDecision(
                miner_hotkey=hotkey,
                interval_id=interval_id,
                accepted=False,
                failures=["validation_exception"],
                sampled_rows=0,
                notes={"error": str(exc)},
            )

    async def _evaluate_loaded_submission(
        self,
        *,
        loaded: LoadedMinerSubmission,
        interval_seed: str,
        global_pruned_count: int,
        cross_miner_pruned_count: int,
    ) -> ValidationDecision:
        try:
            spec = self._spec_registry.get(loaded.spec_id)
            records = loaded.records
            indices = select_row_indices(len(records), loaded.hotkey, interval_seed)
            sampled = [records[i] for i in indices]
            logger.info(
                "sampling hotkey=%s interval=%d total_records=%d sampled=%d",
                loaded.hotkey,
                loaded.interval_id,
                len(records),
                len(sampled),
            )

            if self._source_auth_only:
                check_result = spec.run_hard_checks([])
            else:
                # Full-record hard checks run across the complete interval payload.
                check_result = spec.run_hard_checks(records)
            semantic_frames: dict[str, list[Path]] = {}
            frame_paths: dict[str, Path] = {}
            if self._source_auth_only:
                frame_paths = await self._download_sampled_source_auth_frames(
                    store=loaded.store,
                    key_base=loaded.key_base,
                    sampled=sampled,
                    miner_dir=loaded.miner_dir / "source-auth-frames",
                )
            else:
                verifier = spec.build_asset_verifier()
                if verifier is not None:
                    asset_result = await verifier.verify(
                        store=loaded.store,
                        key_base=loaded.key_base,
                        sampled=sampled,
                        miner_dir=loaded.miner_dir / "assets",
                    )
                    check_result.failures.extend(asset_result.failures)
                    semantic_frames = asset_result.semantic_frames_by_clip_id
                    frame_paths = asset_result.first_frames_by_clip_id

            if self._source_authenticity_enabled and self._source_auth_only:
                source_failures = self._check_source_authenticity(
                    sampled=sampled,
                    frame_paths_by_clip_id=frame_paths,
                    source_cache_dir=loaded.miner_dir / "source_cache",
                    fail_open_on_error=True,
                )
                check_result.failures.extend(source_failures)

            if self._caption_semantic_checker is not None and not self._source_auth_only:
                semantic_failures = self._caption_semantic_checker.check(
                    sampled=sampled,
                    frame_paths_by_clip_id=semantic_frames,
                )
                check_result.failures.extend(semantic_failures)
                if semantic_failures:
                    logger.warning(
                        "semantic caption mismatches hotkey=%s interval=%d count=%d",
                        loaded.hotkey,
                        loaded.interval_id,
                        len(semantic_failures),
                    )

            accepted = len(check_result.failures) == 0
            return ValidationDecision(
                miner_hotkey=loaded.hotkey,
                interval_id=loaded.interval_id,
                accepted=accepted,
                failures=check_result.failures,
                record_count=len(records),
                sampled_rows=len(sampled),
                notes={
                    "record_count": len(records),
                    "sampled_record_count": len(sampled),
                    "global_overlap_pruned_count": global_pruned_count,
                    "cross_miner_overlap_pruned_count": cross_miner_pruned_count,
                    "spec_id": loaded.spec_id,
                },
            )
        except Exception as exc:
            logger.exception(
                "validator miner check failed hotkey=%s interval=%d: %s",
                loaded.hotkey,
                loaded.interval_id,
                exc,
            )
            return ValidationDecision(
                miner_hotkey=loaded.hotkey,
                interval_id=loaded.interval_id,
                accepted=False,
                failures=["validation_exception"],
                sampled_rows=0,
                notes={"error": str(exc)},
            )

    def _prune_overlaps_with_global_index(
        self,
        submissions: list[LoadedMinerSubmission],
        global_record_index: dict[str, list[float]],
    ) -> dict[str, int]:
        pruned: dict[str, int] = defaultdict(int)
        if not global_record_index:
            return pruned
        for submission in submissions:
            spec = self._spec_registry.get(submission.spec_id)
            kept: list[ClipRecord] = []
            for row in submission.records:
                seen_positions: list[float] = []
                for source_key in spec.overlap_index_keys(row):
                    seen_positions.extend(global_record_index.get(source_key, []))
                overlapped = any(
                    abs(row.clip_start_sec - value) < _OVERLAP_WINDOW_SEC
                    for value in seen_positions
                )
                if overlapped:
                    pruned[submission.hotkey] += 1
                    continue
                kept.append(row)
            submission.records = kept
        return pruned

    def _prune_cross_miner_overlaps(
        self,
        submissions: list[LoadedMinerSubmission],
    ) -> dict[str, int]:
        pruned: dict[str, int] = defaultdict(int)
        if len(submissions) < 2:
            return pruned

        by_source: dict[str, list[tuple[LoadedMinerSubmission, ClipRecord]]] = defaultdict(list)
        for submission in submissions:
            spec = self._spec_registry.get(submission.spec_id)
            for row in submission.records:
                by_source[f"{submission.spec_id}:{spec.source_identity_key(row)}"].append(
                    (submission, row)
                )

        rows_to_drop: dict[str, set[int]] = defaultdict(set)
        for source_rows in by_source.values():
            ordered = sorted(
                source_rows,
                key=lambda item: (
                    self._cross_miner_tiebreak(item[0]),
                    item[1].clip_start_sec,
                    item[1].clip_id,
                ),
            )
            accepted: list[tuple[LoadedMinerSubmission, ClipRecord]] = []
            for submission, row in ordered:
                conflicts = any(
                    (
                        submission.hotkey != kept_submission.hotkey
                        and abs(row.clip_start_sec - kept_row.clip_start_sec) < _OVERLAP_WINDOW_SEC
                    )
                    for kept_submission, kept_row in accepted
                )
                if conflicts:
                    rows_to_drop[submission.hotkey].add(id(row))
                    pruned[submission.hotkey] += 1
                    continue
                accepted.append((submission, row))

        for submission in submissions:
            drop_ids = rows_to_drop.get(submission.hotkey, set())
            if not drop_ids:
                continue
            submission.records = [row for row in submission.records if id(row) not in drop_ids]
        return pruned

    def _cross_miner_tiebreak(self, submission: LoadedMinerSubmission) -> tuple[datetime, str]:
        if submission.manifest_last_modified is not None:
            trusted = submission.manifest_last_modified
            if trusted.tzinfo is None:
                trusted = trusted.replace(tzinfo=timezone.utc)
            return trusted, submission.hotkey
        created_at = submission.manifest.created_at
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        return created_at, submission.hotkey

    async def _manifest_last_modified(
        self,
        *,
        store: Any,
        manifest_key: str,
    ) -> datetime | None:
        getter = getattr(store, "get_object_last_modified", None)
        if getter is None:
            return None
        try:
            maybe = getter(manifest_key)
            if inspect.isawaitable(maybe):
                value = await maybe
            else:
                value = maybe
            if isinstance(value, datetime):
                return value
        except Exception:
            return None
        return None

    def _check_source_authenticity(
        self,
        *,
        sampled: list[ClipRecord],
        frame_paths_by_clip_id: dict[str, Path],
        source_cache_dir: Path,
        fail_open_on_error: bool,
    ) -> list[str]:
        failures: list[str] = []
        checked = 0
        source_video_cache: dict[str, Path] = {}
        for row in sampled:
            if checked >= _SOURCE_AUTH_MAX_ROWS:
                break
            validator_frame = frame_paths_by_clip_id.get(row.clip_id)
            if validator_frame is None or not validator_frame.exists():
                continue
            checked += 1
            try:
                source_video = source_video_cache.get(row.source_video_url)
                if source_video is None:
                    source_video = download_youtube_video(
                        row.source_video_url,
                        source_cache_dir / "raw",
                    )
                    source_video_cache[row.source_video_url] = source_video
                source_frame = source_cache_dir / "frames" / f"{row.clip_id}.jpg"
                self._extract_source_frame(
                    source_video=source_video,
                    second=max(0.0, row.clip_start_sec),
                    target=source_frame,
                )
                if not self._frames_are_similar(validator_frame, source_frame):
                    failures.append(f"source_frame_mismatch:{row.clip_id}")
                    break
            except Exception as exc:
                if not fail_open_on_error:
                    failures.append(f"source_frame_validation_error:{row.clip_id}")
                    break
                logger.warning(
                    "source auth check fail-open clip_id=%s error=%s",
                    row.clip_id,
                    exc,
                )
        return failures

    async def _download_sampled_source_auth_frames(
        self,
        *,
        store: Any,
        key_base: str,
        sampled: list[ClipRecord],
        miner_dir: Path,
    ) -> dict[str, Path]:
        frames_by_clip_id: dict[str, Path] = {}
        for row in sampled:
            safe_uri = self._normalize_relative_uri(row.first_frame_uri)
            if safe_uri is None:
                continue
            frame_path = miner_dir / safe_uri
            ok = await store.download_file(f"{key_base}/{safe_uri}", frame_path)
            if not ok:
                continue
            if sha256_file(frame_path) != row.first_frame_sha256:
                continue
            frames_by_clip_id[row.clip_id] = frame_path
        return frames_by_clip_id

    def _normalize_relative_uri(self, value: str) -> str | None:
        text = value.strip().lstrip("/")
        if not text:
            return None
        parts = PurePosixPath(text).parts
        if any(part in {"", ".", ".."} for part in parts):
            return None
        return "/".join(parts)

    def _extract_source_frame(self, *, source_video: Path, second: float, target: Path) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-ss",
            f"{second:.3f}",
            "-i",
            str(source_video),
            "-frames:v",
            "1",
            str(target),
        ]
        subprocess.run(cmd, check=True, timeout=90, capture_output=True, text=True)

    def _frames_are_similar(self, left: Path, right: Path) -> bool:
        left_pixels = self._frame_signature(left)
        right_pixels = self._frame_signature(right)
        if len(left_pixels) != len(right_pixels) or not left_pixels:
            return False
        diff_sum = 0
        for index, value in enumerate(left_pixels):
            diff_sum += abs(value - right_pixels[index])
        mae = diff_sum / len(left_pixels)
        return mae <= _SOURCE_FRAME_MAE_THRESHOLD

    def _frame_signature(self, image_path: Path) -> bytes:
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            str(image_path),
            "-vf",
            f"scale={_SOURCE_FRAME_COMPARE_SIZE}:{_SOURCE_FRAME_COMPARE_SIZE},format=gray",
            "-frames:v",
            "1",
            "-f",
            "rawvideo",
            "-",
        ]
        proc = subprocess.run(cmd, check=True, timeout=30, capture_output=True)
        return proc.stdout

    def _default_interval_seed(self, interval_id: int) -> str:
        return hashlib.sha256(f"interval:{interval_id}".encode("utf-8")).hexdigest()

