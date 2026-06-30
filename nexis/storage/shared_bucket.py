"""Helpers for the shared `nexis_miner` R2 bucket (training outputs + scores)."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from .r2 import R2Credentials, R2S3Store, build_r2_endpoint_url

logger = logging.getLogger(__name__)


def build_nexis_miner_credentials(
    *,
    account_id: str,
    bucket_name: str,
    region: str,
    read_access_key: str,
    read_secret_key: str,
    write_access_key: str = "",
    write_secret_key: str = "",
) -> R2Credentials | None:
    account_id = account_id.strip()
    bucket_name = bucket_name.strip()
    read_access_key = read_access_key.strip()
    read_secret_key = read_secret_key.strip()
    if not account_id or not bucket_name or not read_access_key or not read_secret_key:
        return None
    write_access_key = write_access_key.strip() or read_access_key
    write_secret_key = write_secret_key.strip() or read_secret_key
    return R2Credentials(
        account_id=account_id,
        bucket_name=bucket_name,
        region=region,
        read_access_key=read_access_key,
        read_secret_key=read_secret_key,
        write_access_key=write_access_key,
        write_secret_key=write_secret_key,
    )


class NexisMinerBucket:
    """High-level operations on the shared `nexis_miner` bucket."""

    def __init__(self, store: R2S3Store):
        self._store = store

    @property
    def store(self) -> R2S3Store:
        return self._store

    @property
    def endpoint_url(self) -> str:
        return build_r2_endpoint_url(self._store.credentials.account_id)

    async def list_cycle_ids(self) -> list[int]:
        keys = await self._store.list_prefix("")
        cycles: set[int] = set()
        for key in keys:
            head = key.split("/", 1)[0]
            if head.isdigit():
                cycles.add(int(head))
        return sorted(cycles)

    async def latest_cycle_id(self) -> int | None:
        cycles = await self.list_cycle_ids()
        return cycles[-1] if cycles else None

    async def list_miner_dirs(self, cycle_id: int) -> list[str]:
        keys = await self._store.list_prefix(f"{cycle_id}/")
        miners: set[str] = set()
        for key in keys:
            parts = key.split("/")
            # Any `{cycle}/*.json` (per-validator score files) is excluded by
            # the `.endswith(".json")` guard — miner dirs are subdirectories.
            if len(parts) >= 2 and parts[1] and not parts[1].endswith(".json"):
                miners.add(parts[1])
        return sorted(miners)

    async def list_miner_files(self, cycle_id: int, miner_hotkey: str) -> list[str]:
        keys = await self._store.list_prefix(f"{cycle_id}/{miner_hotkey}/")
        return sorted(keys)

    async def upload_path(self, key: str, local: Path) -> None:
        await self._store.upload_file(key, local, use_write=True)

    async def upload_validator_score(
        self,
        cycle_id: int,
        validator_hotkey: str,
        payload: dict,
        workdir: Path,
        *,
        envelope: dict | None = None,
    ) -> None:
        """Persist a validator's score blob to `{cycle}/{hotkey}.json`.

        If `envelope` is given (a signed envelope from
        `auth.build_score_envelope`), it is stored verbatim — this preserves
        the validator's signature + exact signed bytes so the submission is
        independently auditable. Otherwise the bare `payload` is stored
        (legacy / unsigned path).
        """
        obj = envelope if envelope is not None else payload
        local = workdir / f"{validator_hotkey}.json"
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")
        await self._store.upload_file(
            f"{cycle_id}/{validator_hotkey}.json",
            local,
            use_write=True,
        )

    async def has_validator_score(self, cycle_id: int, validator_hotkey: str) -> bool:
        return await self._store.object_exists(f"{cycle_id}/{validator_hotkey}.json")

    async def download_validator_score(
        self, cycle_id: int, validator_hotkey: str, dst: Path
    ) -> dict | None:
        """Download `{cycle}/{validator_hotkey}.json` and parse it.

        The stored object may be a signed envelope (`build_score_envelope`)
        or a bare payload; either way it carries a top-level `scores` dict,
        which is all `parse_score_payload` needs.
        """
        key = f"{cycle_id}/{validator_hotkey}.json"
        ok = await self._store.download_file(key, dst)
        if not ok or not dst.exists():
            return None
        try:
            return json.loads(dst.read_text(encoding="utf-8"))
        except Exception:
            logger.warning(
                "validator score json invalid cycle=%d hotkey=%s",
                cycle_id,
                validator_hotkey,
            )
            return None
