"""FastAPI application for validator evidence storage."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ValidationError

from ..chain.metagraph import fetch_current_block_async
from ..config import load_settings
from .auth import RequestAuthenticator
from .db import Database
from .metagraph_sync import MetagraphAllowlistSync, ValidatorAllowlistCache
from .repository import ValidationEvidenceRepository
from .schemas import (
    IngestResponse,
    InvalidHotkeysIngestRequest,
    InvalidHotkeysIngestResponse,
    InvalidHotkeysWindowResponse,
    LatestResultDecision,
    LatestResultResponse,
    QueryResponse,
    StoredDecision,
    ValidationResultsIngestRequest,
)

logger = logging.getLogger(__name__)


class LatestResultsCache:
    """Background cache for latest validation decisions."""

    def __init__(
        self,
        *,
        repository: ValidationEvidenceRepository,
        network: str,
        window_blocks: int = 25_000,
        refresh_every_blocks: int = 50,
        poll_sec: float = 6.0,
    ):
        self._repository = repository
        self._network = network
        self._window_blocks = max(int(window_blocks), 1)
        self._refresh_every_blocks = max(int(refresh_every_blocks), 1)
        self._poll_sec = max(float(poll_sec), 1.0)

        self._lock = asyncio.Lock()
        self._refresh_lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._snapshot: LatestResultResponse | None = None
        self._last_refreshed_block: int | None = None

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="latest-results-cache")

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

    async def snapshot(self) -> LatestResultResponse | None:
        async with self._lock:
            return self._snapshot

    async def refresh_if_needed(self, *, force: bool = False) -> None:
        current_block = await fetch_current_block_async(network=self._network)
        async with self._refresh_lock:
            async with self._lock:
                last_block = self._last_refreshed_block
            if not force and last_block is not None:
                if current_block - last_block < self._refresh_every_blocks:
                    return

            start_interval_id = max(current_block - self._window_blocks, 0)
            rows = await self._repository.get_decisions_in_interval_range(
                start_interval_id=start_interval_id,
                end_interval_id=current_block,
            )
            decisions = [LatestResultDecision.model_validate(item) for item in rows]
            payload = LatestResultResponse(
                current_block=current_block,
                start_interval_id=start_interval_id,
                end_interval_id=current_block,
                refreshed_every_blocks=self._refresh_every_blocks,
                cached_at=datetime.now(timezone.utc),
                decisions=decisions,
            )
            async with self._lock:
                self._snapshot = payload
                self._last_refreshed_block = current_block

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.refresh_if_needed(force=False)
            except Exception as exc:
                logger.warning("latest results cache refresh failed: %s", exc)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._poll_sec)
            except asyncio.TimeoutError:
                continue


def create_app() -> FastAPI:
    settings = load_settings()
    app = FastAPI(title="Nexis Validator Evidence API", version="1.0.0")
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
    latest_results_cache = LatestResultsCache(
        repository=repository,
        network=settings.bt_network,
        window_blocks=25_000,
        refresh_every_blocks=50,
        poll_sec=settings.block_poll_sec,
    )

    @app.on_event("startup")
    async def on_startup() -> None:
        await database.connect()
        await repository.ensure_schema()
        try:
            await allowlist_sync.refresh_once()
        except Exception as exc:
            logger.warning("initial validator allowlist refresh failed: %s", exc)
        await allowlist_sync.start()
        try:
            await latest_results_cache.refresh_if_needed(force=True)
        except Exception as exc:
            logger.warning("initial latest results cache refresh failed: %s", exc)
        await latest_results_cache.start()
        logger.info("validation evidence API started")

    @app.on_event("shutdown")
    async def on_shutdown() -> None:
        await latest_results_cache.stop()
        await allowlist_sync.stop()
        await database.close()
        logger.info("validation evidence API stopped")

    @app.post("/v1/validation-results", response_model=IngestResponse)
    async def post_validation_results(request: Request) -> IngestResponse:
        body = await request.body()
        auth = await authenticator.authenticate(request, body)
        try:
            payload = ValidationResultsIngestRequest.model_validate_json(body)
        except ValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=exc.errors(),
            ) from exc
        saved = await repository.upsert_interval_decisions(
            validator_hotkey=auth.validator_hotkey,
            interval_id=payload.interval_id,
            decisions=payload.decisions,
            signature=auth.signature,
            signature_timestamp=auth.timestamp,
            signature_nonce=auth.nonce,
            body_sha256=auth.body_sha256,
        )
        return IngestResponse(
            saved=saved,
            validator_hotkey=auth.validator_hotkey,
            interval_id=payload.interval_id,
        )

    @app.get("/v1/validation-results", response_model=QueryResponse)
    async def get_validation_results(
        validator_hotkey: str = Query(min_length=1),
        interval_id: int = Query(ge=0),
    ) -> QueryResponse:
        rows = await repository.get_interval_decisions(
            validator_hotkey=validator_hotkey,
            interval_id=interval_id,
        )
        decisions = [StoredDecision.model_validate(item) for item in rows]
        return QueryResponse(
            validator_hotkey=validator_hotkey,
            interval_id=interval_id,
            decisions=decisions,
        )

    @app.get("/v1/invalid-hotkeys", response_model=InvalidHotkeysWindowResponse)
    async def get_invalid_hotkeys(
        interval_id: int = Query(ge=0),
    ) -> InvalidHotkeysWindowResponse:
        window_start = max(int(interval_id) - 500, 0)
        invalid_hotkeys = await repository.get_invalid_hotkeys_in_interval_range(
            start_interval_id=window_start,
            end_interval_id=int(interval_id),
        )
        return InvalidHotkeysWindowResponse(
            interval_id=int(interval_id),
            window_start_interval_id=window_start,
            window_end_interval_id=int(interval_id),
            invalid_hotkeys=invalid_hotkeys,
        )

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
        deduped = sorted({item.strip() for item in payload.invalid_hotkeys if item.strip()})
        await repository.upsert_interval_invalid_hotkeys(
            interval_id=payload.interval_id,
            invalid_hotkeys=deduped,
        )
        return InvalidHotkeysIngestResponse(
            validator_hotkey=auth.validator_hotkey,
            interval_id=payload.interval_id,
            saved_count=len(deduped),
        )

    @app.get("/v1/get_latest_result", response_model=LatestResultResponse)
    @app.get("/get_latest_result", response_model=LatestResultResponse)
    async def get_latest_result() -> LatestResultResponse:
        snapshot = await latest_results_cache.snapshot()
        if snapshot is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="latest results cache is not ready",
            )
        return snapshot

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app

