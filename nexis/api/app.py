"""FastAPI application for the validator API (v2)."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
import tempfile

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from ..config import load_settings
from ..storage.r2 import R2S3Store
from ..storage.shared_bucket import (
    NexisMinerBucket,
    TOTAL_SCORE_OBJECT,
    build_nexis_miner_credentials,
)
from ..validator.dataset_check import canonical_source_key
from .auth import RequestAuthenticator
from .db import Database
from .metagraph_sync import MetagraphAllowlistSync, ValidatorAllowlistCache
from .repository import ValidationEvidenceRepository
from .schemas import (
    BlacklistResponse,
    InvalidHotkeysIngestRequest,
    InvalidHotkeysIngestResponse,
    InvalidHotkeysListResponse,
    InvalidHotkeysResetResponse,
    TrainingScoresIngestRequest,
    TrainingScoresIngestResponse,
)

logger = logging.getLogger(__name__)


class RecordInfoCoordinator:
    """Owner-only updater for the global overlap snapshot (`record_info.json`).

    When the OWNER validator POSTs training scores, we pick the top-5 miners
    by aggregate from the freshly-computed total_score, fetch each one's
    `dataset_index.json` from the nexis_miner bucket, canonicalize the source
    URLs, and merge `(canonical_url, clip_start_sec)` pairs into the existing
    snapshot.  POSTs from any non-owner validator are no-ops.
    """

    def __init__(
        self,
        *,
        nexis_miner: NexisMinerBucket,
        record_info_store: R2S3Store | None,
        record_info_object_key: str,
        owner_hotkey: str,
        workdir: Path,
        top_k: int = 5,
    ):
        self._nexis_miner = nexis_miner
        self._record_info_store = record_info_store
        self._record_info_object_key = record_info_object_key
        self._owner_hotkey = owner_hotkey.strip()
        self._workdir = workdir
        self._top_k = max(int(top_k), 1)
        self._lock = asyncio.Lock()
        # Strong references to spawned background tasks so the Python GC
        # doesn't cancel them before they finish.  Cleared as each completes.
        self._tasks: set[asyncio.Task[bool]] = set()

    @property
    def enabled(self) -> bool:
        return (
            bool(self._owner_hotkey)
            and self._record_info_store is not None
        )

    def disabled_reason(self) -> str:
        if not self._owner_hotkey:
            return "owner_hotkey not configured (NEXIS_OWNER_VALIDATOR_HOTKEY)"
        if self._record_info_store is None:
            return (
                "record_info store unavailable — missing "
                "NEXIS_RECORD_INFO_ACCOUNT_ID and/or WRITE keys"
            )
        return "enabled"

    @property
    def owner_hotkey(self) -> str:
        return self._owner_hotkey

    def schedule(
        self,
        *,
        cycle_id: int,
        validator_hotkey: str,
        total_score_payload: dict,
    ) -> bool:
        """Spawn `maybe_update` as a tracked background task.

        Returns True if a task was scheduled, False if the coordinator is
        disabled or the poster is not the owner.  The task itself logs every
        early-return path so operators can see exactly where the update was
        skipped.
        """
        if not self.enabled:
            logger.info(
                "record_info update skipped cycle=%d validator=%s reason=%s",
                cycle_id,
                validator_hotkey,
                self.disabled_reason(),
            )
            return False
        if validator_hotkey.strip() != self._owner_hotkey:
            logger.info(
                "record_info update skipped cycle=%d validator=%s reason=not_owner "
                "(expected=%s)",
                cycle_id,
                validator_hotkey,
                self._owner_hotkey,
            )
            return False
        task = asyncio.create_task(
            self.maybe_update(
                cycle_id=cycle_id,
                validator_hotkey=validator_hotkey,
                total_score_payload=total_score_payload,
            ),
            name=f"record-info-update-cycle-{cycle_id}",
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        logger.info(
            "record_info update scheduled cycle=%d task=%s",
            cycle_id,
            task.get_name(),
        )
        return True

    async def maybe_update(
        self,
        *,
        cycle_id: int,
        validator_hotkey: str,
        total_score_payload: dict,
    ) -> bool:
        if not self.enabled:
            logger.info(
                "record_info maybe_update skipped cycle=%d reason=%s",
                cycle_id,
                self.disabled_reason(),
            )
            return False
        if validator_hotkey.strip() != self._owner_hotkey:
            logger.info(
                "record_info maybe_update skipped cycle=%d reason=not_owner "
                "(got=%s expected=%s)",
                cycle_id,
                validator_hotkey,
                self._owner_hotkey,
            )
            return False
        logger.info(
            "record_info maybe_update start cycle=%d validator=%s",
            cycle_id,
            validator_hotkey,
        )
        async with self._lock:
            try:
                ok = await self._update(
                    cycle_id=cycle_id,
                    total_score_payload=total_score_payload,
                )
            except Exception as exc:
                logger.exception(
                    "record_info update failed cycle=%d: %s", cycle_id, exc
                )
                return False
        logger.info(
            "record_info maybe_update end cycle=%d updated=%s", cycle_id, ok
        )
        return ok

    async def _update(self, *, cycle_id: int, total_score_payload: dict) -> bool:
        scores = total_score_payload.get("scores")
        if not isinstance(scores, dict):
            return False

        ranked: list[tuple[str, float]] = []
        for hotkey, entry in scores.items():
            if not isinstance(entry, dict):
                continue
            try:
                agg = float(entry.get("aggregate", 0.0))
            except (TypeError, ValueError):
                continue
            ranked.append((str(hotkey), agg))
        ranked.sort(key=lambda kv: (-kv[1], kv[0]))
        top = ranked[: self._top_k]
        if not top:
            return False

        cycle_dir = self._workdir / f"cycle_{cycle_id}"
        cycle_dir.mkdir(parents=True, exist_ok=True)

        new_entries: list[tuple[str, float]] = []
        for hotkey, _ in top:
            idx_local = cycle_dir / f"{hotkey}_dataset_index.json"
            try:
                ok = await self._nexis_miner.store.download_file(
                    f"{cycle_id}/{hotkey}/dataset_index.json", idx_local
                )
            except Exception as exc:
                logger.warning(
                    "dataset_index fetch failed miner=%s cycle=%d err=%s",
                    hotkey,
                    cycle_id,
                    exc,
                )
                continue
            if not ok or not idx_local.exists():
                logger.warning(
                    "dataset_index missing miner=%s cycle=%d", hotkey, cycle_id
                )
                continue
            try:
                rows = json.loads(idx_local.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                source_url = str(row.get("source_url", "")).strip()
                if not source_url:
                    continue
                try:
                    clip_start = float(row.get("clip_start_sec"))
                except (TypeError, ValueError):
                    continue
                new_entries.append((source_url, clip_start))

        if not new_entries:
            logger.info(
                "no new record_info entries top_k=%d cycle=%d", len(top), cycle_id
            )
            return False

        # Fetch existing snapshot from record_info bucket.
        existing_local = cycle_dir / "existing_record_info.json"
        existing: dict = {}
        try:
            if await self._record_info_store.download_file(  # type: ignore[union-attr]
                self._record_info_object_key, existing_local
            ):
                existing = json.loads(existing_local.read_text(encoding="utf-8"))
        except Exception:
            existing = {}
        if not isinstance(existing, dict):
            existing = {}

        spec_section = existing.get("video_v1")
        if not isinstance(spec_section, dict):
            spec_section = {}

        added = 0
        for source_url, clip_start in new_entries:
            canonical = canonical_source_key(source_url)
            positions = spec_section.setdefault(canonical, [])
            positions.append(round(clip_start, 3))
            added += 1
        # Dedup + sort each per-source position list.
        for url, positions in list(spec_section.items()):
            spec_section[url] = sorted({float(p) for p in positions})

        existing["video_v1"] = spec_section

        updated_local = cycle_dir / "updated_record_info.json"
        updated_local.write_text(
            json.dumps(existing, indent=2, sort_keys=True), encoding="utf-8"
        )
        await self._record_info_store.upload_file(  # type: ignore[union-attr]
            self._record_info_object_key, updated_local, use_write=True
        )
        logger.info(
            "record_info updated cycle=%d top_k=%d new_pairs=%d",
            cycle_id,
            len(top),
            added,
        )
        return True


class TotalScoreCache:
    """In-memory cache of every cycle's `total_score.json` on the shared bucket.

    A background task refreshes the cache every `refresh_sec`. The two read
    endpoints (`/v1/get_latest_total_score`, `/v1/get_total_score/{cycle_id}`)
    serve from this cache so frontends don't hit R2 on every page load.
    """

    def __init__(
        self,
        *,
        bucket: NexisMinerBucket,
        workdir: Path,
        refresh_sec: int = 300,
    ):
        self._bucket = bucket
        self._workdir = workdir
        self._refresh_sec = max(int(refresh_sec), 10)
        self._cache: dict[int, dict] = {}
        self._latest_cycle: int | None = None
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="total-score-cache")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stop.set()
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.refresh()
            except Exception as exc:
                logger.warning("total_score cache refresh failed: %s", exc)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._refresh_sec)
            except asyncio.TimeoutError:
                continue

    async def refresh(self) -> None:
        """Walk every cycle dir, pull total_score.json for any cycle that has it."""
        cycles = await self._bucket.list_cycle_ids()
        for cycle_id in cycles:
            if not await self._bucket.has_total_score(cycle_id):
                continue
            local = self._workdir / f"total_score_{cycle_id}.json"
            local.parent.mkdir(parents=True, exist_ok=True)
            payload = await self._bucket.download_total_score(cycle_id, local)
            if payload is None:
                continue
            async with self._lock:
                self._cache[cycle_id] = payload
                if self._latest_cycle is None or cycle_id > self._latest_cycle:
                    self._latest_cycle = cycle_id

    async def update_one(self, cycle_id: int, payload: dict) -> None:
        """Drop a known-good payload into the cache without re-fetching."""
        async with self._lock:
            self._cache[int(cycle_id)] = payload
            if self._latest_cycle is None or cycle_id > self._latest_cycle:
                self._latest_cycle = int(cycle_id)

    async def get(self, cycle_id: int) -> dict | None:
        async with self._lock:
            return self._cache.get(int(cycle_id))

    async def get_latest(self) -> tuple[int, dict] | None:
        async with self._lock:
            if self._latest_cycle is None:
                return None
            return self._latest_cycle, self._cache[self._latest_cycle]


class TotalScoreCoordinator:
    """Aggregate per-validator scores into `total_score.json` on the shared bucket."""

    def __init__(self, *, bucket: NexisMinerBucket, workdir: Path):
        self._bucket = bucket
        self._workdir = workdir
        self._lock = asyncio.Lock()

    async def update_for_cycle(
        self, *, cycle_id: int, validator_hotkey: str, payload: dict
    ) -> tuple[int, dict]:
        """Persist the validator's score blob and rebuild total_score.json.

        Returns (miner_count_in_this_post, total_score_payload). The caller
        gets the recomputed total so it can drive downstream side-effects
        (e.g. owner-only record_info update) without re-reading the bucket.
        """
        async with self._lock:
            cycle_dir = self._workdir / str(cycle_id)
            cycle_dir.mkdir(parents=True, exist_ok=True)

            # 1) Upload this validator's raw payload.
            await self._bucket.upload_validator_score(
                cycle_id=cycle_id,
                validator_hotkey=validator_hotkey,
                payload=payload,
                workdir=cycle_dir,
            )

            # 2) Recompute total_score.json from every {validator}.json present.
            score_keys = await self._bucket.list_validator_score_keys(cycle_id)
            local_files = await self._bucket.download_keys(score_keys, workdir=cycle_dir / "incoming")
            per_miner_aggs: dict[str, list[float]] = {}
            per_miner_dims: dict[str, dict[str, list[float]]] = {}
            per_miner_interval: dict[str, int] = {}

            for path in local_files.values():
                try:
                    raw = json.loads(path.read_text(encoding="utf-8"))
                except Exception:
                    continue
                scores = raw.get("scores")
                if not isinstance(scores, dict):
                    continue
                for hotkey, entry in scores.items():
                    if not isinstance(entry, dict):
                        # Legacy bare-float entry; treat as the aggregate.
                        try:
                            per_miner_aggs.setdefault(str(hotkey), []).append(float(entry))
                        except (TypeError, ValueError):
                            pass
                        continue
                    try:
                        agg = float(entry.get("aggregate", entry.get("score", 0)))
                        per_miner_aggs.setdefault(str(hotkey), []).append(agg)
                    except (TypeError, ValueError):
                        pass
                    dims = entry.get("dimensions")
                    if isinstance(dims, dict):
                        bucket = per_miner_dims.setdefault(str(hotkey), {})
                        for dn, dv in dims.items():
                            try:
                                bucket.setdefault(str(dn), []).append(float(dv))
                            except (TypeError, ValueError):
                                continue
                    # Trainer-stamped miner_interval_id is identical across
                    # validators in the same cycle; record the first one seen.
                    if hotkey not in per_miner_interval:
                        miid = entry.get("miner_interval_id")
                        if miid is not None:
                            try:
                                per_miner_interval[str(hotkey)] = int(miid)
                            except (TypeError, ValueError):
                                pass

            total_payload = {
                "cycle_id": int(cycle_id),
                "scores": {
                    hotkey: {
                        "aggregate": sum(values) / len(values),
                        "validator_count": len(values),
                        "dimensions": {
                            dn: sum(dvs) / len(dvs)
                            for dn, dvs in per_miner_dims.get(hotkey, {}).items()
                            if dvs
                        },
                        "miner_interval_id": per_miner_interval.get(hotkey),
                    }
                    for hotkey, values in per_miner_aggs.items()
                    if values
                },
            }
            await self._bucket.upload_total_score(cycle_id, total_payload, workdir=cycle_dir)
            return len(payload.get("scores") or {}), total_payload


def create_app() -> FastAPI:
    settings = load_settings()
    # Ensure our `logger.info(...)` calls reach stdout. uvicorn configures its
    # own loggers but the root logger is left at WARNING by default.
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )
    app = FastAPI(title="Nexis Validator API", version="2.0.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    database = Database(settings.validation_api_postgres_dsn)
    repository = ValidationEvidenceRepository(database)
    allowlist_cache = ValidatorAllowlistCache()
    allowlist_sync = MetagraphAllowlistSync(
        netuid=settings.netuid,
        network=settings.bt_network,
        min_stake=settings.validation_api_min_validator_stake,
        refresh_sec=settings.validation_api_allowlist_refresh_sec,
        cache=allowlist_cache,
    )
    authenticator = RequestAuthenticator(
        allowlist_cache=allowlist_cache,
        repository=repository,
        max_time_skew_sec=settings.validation_api_auth_max_skew_sec,
        nonce_max_age_sec=settings.validation_api_nonce_max_age_sec,
    )

    creds = build_nexis_miner_credentials(
        account_id=settings.nexis_miner_account_id,
        bucket_name=settings.nexis_miner_bucket,
        region=settings.r2_region,
        read_access_key=settings.nexis_miner_read_access_key,
        read_secret_key=settings.nexis_miner_read_secret_key,
        write_access_key=settings.nexis_miner_write_access_key,
        write_secret_key=settings.nexis_miner_write_secret_key,
    )
    if creds is None:
        raise RuntimeError(
            "API requires nexis_miner R2 read+write credentials "
            "(NEXIS_MINER_ACCOUNT_ID/READ/WRITE_*)"
        )
    bucket = NexisMinerBucket(R2S3Store(creds))
    coord = TotalScoreCoordinator(
        bucket=bucket,
        workdir=Path(tempfile.gettempdir()) / "nexis_total_score",
    )
    total_score_cache = TotalScoreCache(
        bucket=bucket,
        workdir=Path(tempfile.gettempdir()) / "nexis_total_score_cache",
        refresh_sec=300,
    )

    # Optional: owner-only updates to the global overlap snapshot.  Skipped
    # silently if the API doesn't have write keys for `nexis-record-info`.
    record_info_creds = build_nexis_miner_credentials(
        account_id=settings.record_info_account_id,
        bucket_name=settings.record_info_bucket,
        region=settings.r2_region,
        read_access_key=settings.record_info_read_access_key,
        read_secret_key=settings.record_info_read_secret_key,
        write_access_key=settings.record_info_write_access_key,
        write_secret_key=settings.record_info_write_secret_key,
    )
    record_info_store = (
        R2S3Store(record_info_creds) if record_info_creds is not None else None
    )
    if (
        record_info_store is not None
        and (
            not settings.record_info_write_access_key.strip()
            or not settings.record_info_write_secret_key.strip()
        )
    ):
        # Read-only creds present but no write keys: can't update the snapshot.
        record_info_store = None
        logger.warning(
            "record_info update disabled: NEXIS_RECORD_INFO_WRITE_* not set"
        )
    record_info_coord = RecordInfoCoordinator(
        nexis_miner=bucket,
        record_info_store=record_info_store,
        record_info_object_key=settings.record_info_object_key,
        owner_hotkey=settings.owner_validator_hotkey,
        workdir=Path(tempfile.gettempdir()) / "nexis_record_info",
    )
    if record_info_coord.enabled:
        logger.info(
            "record_info coordinator ENABLED bucket=%s owner_hotkey=%s object_key=%s",
            settings.record_info_bucket,
            record_info_coord.owner_hotkey,
            settings.record_info_object_key,
        )
    else:
        # Be loud — silent disable was the original bug.
        logger.warning(
            "=" * 72 + "\n"
            "record_info coordinator DISABLED — owner POSTs will be no-ops!\n"
            "  reason: %s\n"
            "  required env vars on the API host:\n"
            "    NEXIS_RECORD_INFO_BUCKET             (default: nexis-record-info)\n"
            "    NEXIS_RECORD_INFO_ACCOUNT_ID         (R2 account id)\n"
            "    NEXIS_RECORD_INFO_READ_ACCESS_KEY\n"
            "    NEXIS_RECORD_INFO_READ_SECRET_KEY\n"
            "    NEXIS_RECORD_INFO_WRITE_ACCESS_KEY\n"
            "    NEXIS_RECORD_INFO_WRITE_SECRET_KEY\n"
            "    NEXIS_OWNER_VALIDATOR_HOTKEY         (the owner ss58)\n"
            + "=" * 72,
            record_info_coord.disabled_reason(),
        )

    @app.on_event("startup")
    async def on_startup() -> None:
        logger.info("nexis API starting up")
        await database.connect()
        logger.info("postgres connected")
        await repository.ensure_schema()
        logger.info("schema ensured")
        # Bound the initial chain refresh so a slow finney connection can't
        # block uvicorn from binding the port and serving /healthz.
        try:
            await asyncio.wait_for(allowlist_sync.refresh_once(), timeout=20)
            logger.info("validator allowlist refreshed")
        except asyncio.TimeoutError:
            logger.warning(
                "initial validator allowlist refresh timed out after 20s; "
                "background sync will retry"
            )
        except Exception as exc:
            logger.warning("initial validator allowlist refresh failed: %s", exc)
        await allowlist_sync.start()

        # Warm the total-score cache once at boot so the first /get_*
        # request doesn't have to wait for the periodic refresh.
        try:
            await asyncio.wait_for(total_score_cache.refresh(), timeout=60)
            logger.info("total_score cache warmed")
        except asyncio.TimeoutError:
            logger.warning(
                "initial total_score cache warm-up timed out after 60s; "
                "background refresh will retry"
            )
        except Exception as exc:
            logger.warning("initial total_score cache warm-up failed: %s", exc)
        await total_score_cache.start()
        logger.info("nexis API started")

    @app.on_event("shutdown")
    async def on_shutdown() -> None:
        await total_score_cache.stop()
        await allowlist_sync.stop()
        await database.close()
        logger.info("nexis API stopped")

    @app.post("/v1/training-scores", response_model=TrainingScoresIngestResponse)
    async def post_training_scores(request: Request) -> TrainingScoresIngestResponse:
        body = await request.body()
        auth = await authenticator.authenticate(request, body)
        try:
            payload = TrainingScoresIngestRequest.model_validate_json(body)
        except ValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=exc.errors(),
            ) from exc
        miner_count, total_payload = await coord.update_for_cycle(
            cycle_id=payload.cycle_id,
            validator_hotkey=auth.validator_hotkey,
            payload=json.loads(body.decode("utf-8")),
        )
        # Push the freshly-computed total_score into the read cache so the
        # next /get_*total_score call sees it without waiting for the
        # periodic refresh.
        await total_score_cache.update_one(payload.cycle_id, total_payload)
        # Owner-only side effect: refresh `record_info.json` from this cycle's
        # top-5 dataset indexes.  `schedule()` returns True if the task was
        # actually launched (owner posted, coordinator enabled); False
        # otherwise — with a log line in either case.  The coordinator holds
        # a strong reference to the task so it can't be GC'd mid-flight.
        record_info_coord.schedule(
            cycle_id=payload.cycle_id,
            validator_hotkey=auth.validator_hotkey,
            total_score_payload=total_payload,
        )
        return TrainingScoresIngestResponse(
            validator_hotkey=auth.validator_hotkey,
            cycle_id=payload.cycle_id,
            miner_count=miner_count,
        )

    @app.get("/v1/get_latest_total_score")
    async def get_latest_total_score() -> JSONResponse:
        result = await total_score_cache.get_latest()
        if result is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="no total_score available yet",
            )
        _, payload = result
        return JSONResponse(content=payload)

    @app.get("/v1/get_total_score/{cycle_id}")
    async def get_total_score_for_cycle(cycle_id: int) -> JSONResponse:
        if cycle_id < 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="cycle_id must be >= 1",
            )
        payload = await total_score_cache.get(cycle_id)
        if payload is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"no total_score for cycle {cycle_id}",
            )
        return JSONResponse(content=payload)

    @app.get("/v1/invalid-hotkeys", response_model=InvalidHotkeysListResponse)
    async def list_invalid_hotkeys() -> InvalidHotkeysListResponse:
        rows = await repository.list_invalid_hotkeys()
        return InvalidHotkeysListResponse(invalid_hotkeys=rows)

    @app.post("/v1/invalid-hotkeys", response_model=InvalidHotkeysIngestResponse)
    async def post_invalid_hotkeys(request: Request) -> InvalidHotkeysIngestResponse:
        body = await request.body()
        auth = await authenticator.authenticate(request, body)
        try:
            payload = InvalidHotkeysIngestRequest.model_validate_json(body)
        except ValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=exc.errors(),
            ) from exc
        inserted = await repository.add_invalid_hotkeys(hotkeys=payload.invalid_hotkeys)
        return InvalidHotkeysIngestResponse(
            validator_hotkey=auth.validator_hotkey,
            saved_count=inserted,
        )

    @app.delete("/v1/invalid-hotkeys", response_model=InvalidHotkeysResetResponse)
    async def reset_invalid_hotkeys(request: Request) -> InvalidHotkeysResetResponse:
        token_required = settings.validation_api_admin_token.strip()
        if not token_required:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="admin token not configured",
            )
        provided = request.headers.get("x-admin-token", "").strip()
        if provided != token_required:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="invalid admin token",
            )
        cleared = await repository.reset_invalid_hotkeys()
        return InvalidHotkeysResetResponse(cleared=cleared)

    @app.get("/v1/get_blacklist", response_model=BlacklistResponse)
    async def get_blacklist() -> BlacklistResponse:
        values = await repository.get_blacklisted_hotkeys()
        return BlacklistResponse(blacklist_hotkeys=values)

    @app.post("/v1/admin/refresh-record-info/{cycle_id}")
    async def admin_refresh_record_info(
        cycle_id: int, request: Request
    ) -> JSONResponse:
        """Manually trigger the record_info refresh for one cycle.

        Bypasses the owner-hotkey check (`maybe_update` does the work directly
        instead of going through `schedule()`).  Used to verify the bucket
        creds + dataset_index pipeline are wired correctly without waiting
        for the owner validator to POST scores.
        """
        token_required = settings.validation_api_admin_token.strip()
        if not token_required:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="admin token not configured",
            )
        provided = request.headers.get("x-admin-token", "").strip()
        if provided != token_required:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="invalid admin token",
            )
        if not record_info_coord.enabled:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=(
                    "record_info coordinator disabled: "
                    f"{record_info_coord.disabled_reason()}"
                ),
            )
        total = await total_score_cache.get(cycle_id)
        if total is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"no cached total_score for cycle {cycle_id}",
            )
        # Run with the owner_hotkey as the "validator" so the owner check passes.
        updated = await record_info_coord.maybe_update(
            cycle_id=cycle_id,
            validator_hotkey=record_info_coord.owner_hotkey,
            total_score_payload=total,
        )
        return JSONResponse(content={"cycle_id": cycle_id, "updated": bool(updated)})

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    _ = TOTAL_SCORE_OBJECT
    return app
