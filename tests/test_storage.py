from __future__ import annotations

from pathlib import Path

import pytest

from nexis.storage.r2 import R2Credentials
from .helpers import LocalObjectStore, run_async


def test_local_store_upload_download(tmp_path: Path) -> None:
    async def run() -> None:
        store = LocalObjectStore(tmp_path / "store")
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_text("hello", encoding="utf-8")
        await store.upload_file("prefix/file.txt", src)
        assert await store.object_exists("prefix/file.txt")
        ok = await store.download_file("prefix/file.txt", dst)
        assert ok
        assert dst.read_text(encoding="utf-8") == "hello"
        keys = await store.list_prefix("prefix/")
        assert keys == ["prefix/file.txt"]

    run_async(run())


def test_r2_credentials_bucket_name_validation_matches_lowercase_hotkey() -> None:
    hotkey = "5F4fAnX"
    creds = R2Credentials(
        account_id="a" * 32,
        bucket_name=hotkey.lower(),
        region="auto",
        read_access_key="k" * 32,
        read_secret_key="s" * 64,
        write_access_key="k" * 32,
        write_secret_key="s" * 64,
    )
    creds.validate_bucket_name()
    creds.validate_bucket_for_hotkey(hotkey)

    with pytest.raises(ValueError):
        R2Credentials(
            account_id="a" * 32,
            bucket_name="",
            region="auto",
            read_access_key="k" * 32,
            read_secret_key="s" * 64,
            write_access_key="k" * 32,
            write_secret_key="s" * 64,
        ).validate_bucket_name()

    with pytest.raises(ValueError):
        R2Credentials(
            account_id="a" * 32,
            bucket_name="other-bucket",
            region="auto",
            read_access_key="k" * 32,
            read_secret_key="s" * 64,
            write_access_key="k" * 32,
            write_secret_key="s" * 64,
        ).validate_bucket_for_hotkey(hotkey)

