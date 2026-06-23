#!/usr/bin/env python3
"""Independently verify validator score submissions in the nexis-miner bucket.

Each `{cycle_id}/{validator_hotkey}.json` written by the API now embeds the
validator's signature plus the EXACT bytes they signed (`signed_body_b64`).
This tool re-checks, using only the object + the validator's ss58 hotkey
(no server, no DB, no trust in the API):

  1. signed_body_b64 decodes and sha256(raw) == auth.body_sha256
  2. the convenience `scores` field matches the signed body's scores
  3. the hotkey's sr25519 signature verifies over
       POST|/v1/training-scores|{body_sha256}|{timestamp}|{nonce}

A PASS means: "validator <hotkey> really did sign exactly these scores at
that timestamp." A FAIL on a non-legacy object means tampering or forgery.

Usage:
    # one validator's submission for a cycle
    python verify_validator_score.py --cycle 56 --hotkey 5G3jpd...

    # every validator submission for a cycle
    python verify_validator_score.py --cycle 56 --all

    # a local file you already downloaded
    python verify_validator_score.py --file ./5G3jpd....json

Source bucket creds come from the normal nexis Settings (NEXIS_MINER_*).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from nexis.api.auth import verify_stored_score
from nexis.config import load_settings
from nexis.storage.r2 import R2S3Store
from nexis.storage.shared_bucket import (
    NexisMinerBucket,
    build_nexis_miner_credentials,
)


def _verify_obj(label: str, obj: dict) -> bool:
    ok, reason = verify_stored_score(obj)
    hotkey = str(obj.get("auth", {}).get("scheme", ""))
    if ok:
        print(f"PASS  {label}  (scheme={hotkey or 'n/a'})")
    else:
        print(f"FAIL  {label}  -> {reason}")
    return ok


def _verify_file(path: Path) -> bool:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"FAIL  {path}  -> unreadable: {exc}")
        return False
    return _verify_obj(str(path), obj)


async def _verify_from_bucket(*, cycle: int, hotkey: str | None, all_: bool) -> int:
    settings = load_settings()
    creds = build_nexis_miner_credentials(
        account_id=settings.nexis_miner_account_id,
        bucket_name=settings.nexis_miner_bucket,
        region=settings.r2_region,
        read_access_key=settings.nexis_miner_read_access_key,
        read_secret_key=settings.nexis_miner_read_secret_key,
    )
    if creds is None:
        print("nexis-miner read credentials incomplete; set NEXIS_MINER_*")
        return 2
    bucket = NexisMinerBucket(R2S3Store(creds))
    workdir = Path(settings.workdir) / "verify_scores" / str(cycle)
    workdir.mkdir(parents=True, exist_ok=True)

    if all_:
        keys = await bucket.list_validator_score_keys(cycle)
        if not keys:
            print(f"cycle={cycle}: no validator score objects found")
            return 1
    else:
        keys = [f"{cycle}/{hotkey}.json"]

    files = await bucket.download_keys(keys, workdir=workdir)
    if not files:
        print(f"cycle={cycle}: nothing downloaded for {keys}")
        return 1

    all_ok = True
    for key in sorted(files):
        all_ok &= _verify_file(files[key])
    return 0 if all_ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--cycle", type=int, help="cycle_id in the nexis-miner bucket")
    ap.add_argument("--hotkey", type=str, help="validator hotkey (single check)")
    ap.add_argument("--all", action="store_true", help="verify every validator in the cycle")
    ap.add_argument("--file", type=str, help="verify a local {hotkey}.json instead of the bucket")
    args = ap.parse_args()

    if args.file:
        return 0 if _verify_file(Path(args.file)) else 1
    if args.cycle is None or (not args.all and not args.hotkey):
        ap.error("provide --file, or --cycle with --hotkey / --all")
    return asyncio.run(
        _verify_from_bucket(cycle=args.cycle, hotkey=args.hotkey, all_=args.all)
    )


if __name__ == "__main__":
    sys.exit(main())
