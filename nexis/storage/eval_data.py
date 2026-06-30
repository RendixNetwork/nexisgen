"""Sync the canonical eval dataset from R2 to a local dir.

The `nexis-eval` bucket holds the network's evaluation dataset under a
configurable prefix (default `eval_data/`). Every validator needs a fresh
copy before each cycle (each runs both training and scoring):

- The trainer mounts the local dir at `/workspace/eval_data` inside
  `rendixnetwork/train:latest`.
- VBench scorers mount it at `/eval_data` inside `rendixnetwork/vbench:latest`.

`sync_eval_data` downloads every object under the prefix, stripping the
prefix from each key on the way to disk (so `eval_data/manifest.jsonl`
lands at `<local_dir>/manifest.jsonl`). It is intentionally idempotent —
re-running overwrites files; no removal of locally-extant-but-bucket-gone
files (callers can wipe the dir first if they want a strict mirror).
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from .r2 import R2S3Store
from .shared_bucket import build_nexis_miner_credentials

logger = logging.getLogger(__name__)


def build_eval_data_store(
    *,
    account_id: str,
    bucket_name: str,
    region: str,
    read_access_key: str,
    read_secret_key: str,
) -> R2S3Store | None:
    """Build a read-only R2S3Store for the eval-data bucket.

    Returns None if any of (account_id, bucket_name, read_access_key,
    read_secret_key) is blank.
    """
    creds = build_nexis_miner_credentials(
        account_id=account_id,
        bucket_name=bucket_name,
        region=region,
        read_access_key=read_access_key,
        read_secret_key=read_secret_key,
    )
    if creds is None:
        return None
    return R2S3Store(creds)


async def sync_eval_data(
    *,
    store: R2S3Store,
    prefix: str,
    local_dir: Path,
    download_concurrency: int = 16,
) -> int:
    """Download every object under `prefix` into `local_dir`.

    Returns the number of objects successfully downloaded. Raises only if
    listing the prefix itself fails; per-object errors are logged and the
    overall sync continues.
    """
    local_dir = local_dir.resolve()
    local_dir.mkdir(parents=True, exist_ok=True)
    norm_prefix = (prefix or "").lstrip("/").rstrip("/")
    list_prefix = f"{norm_prefix}/" if norm_prefix else ""

    keys = await store.list_prefix(list_prefix)
    if not keys:
        logger.warning(
            "eval_data sync: zero objects under bucket=%s prefix=%r",
            store.credentials.bucket_name,
            list_prefix,
        )
        return 0

    sem = asyncio.Semaphore(max(int(download_concurrency), 1))

    async def _fetch(key: str) -> bool:
        rel = key
        if norm_prefix and key.startswith(f"{norm_prefix}/"):
            rel = key[len(norm_prefix) + 1 :]
        if not rel or rel.endswith("/"):
            return False
        target = local_dir / rel
        async with sem:
            try:
                ok = await store.download_file(key, target)
            except Exception as exc:
                logger.warning(
                    "eval_data download exception key=%s err=%s", key, exc
                )
                return False
        if not ok:
            logger.warning("eval_data download failed key=%s", key)
        return bool(ok)

    results = await asyncio.gather(*[_fetch(k) for k in keys])
    count = sum(1 for ok in results if ok)
    logger.info(
        "eval_data sync complete bucket=%s prefix=%r downloaded=%d of=%d local=%s",
        store.credentials.bucket_name,
        list_prefix,
        count,
        len(keys),
        local_dir,
    )
    return count
