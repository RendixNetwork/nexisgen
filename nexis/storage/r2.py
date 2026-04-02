"""Cloudflare R2 S3-compatible storage client."""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

_R2_ENDPOINT_SUFFIX = "r2.cloudflarestorage.com"
_R2_ACCOUNT_ID_LEN = 32
_R2_READ_ACCESS_KEY_LEN = 32
_R2_READ_SECRET_KEY_LEN = 64
_NOT_FOUND_ERROR_CODES = {"404", "NoSuchKey", "NotFound"}
_R2_ACCOUNT_ID_RE = re.compile(r"^[0-9a-f]{32}$")


def build_r2_endpoint_url(account_id: str) -> str:
    """Build Cloudflare R2 S3 endpoint URL from account ID."""
    normalized = account_id.strip().lower()
    if not is_valid_r2_account_id(normalized):
        raise ValueError("R2 account_id must be 32 lowercase hex characters")
    return f"https://{normalized}.{_R2_ENDPOINT_SUFFIX}"


def bucket_name_for_hotkey(hotkey: str) -> str:
    """Derive miner bucket name from hotkey."""
    return hotkey.strip().lower()


def is_valid_r2_account_id(value: str) -> bool:
    return bool(_R2_ACCOUNT_ID_RE.fullmatch(value.strip().lower()))


@dataclass(frozen=True)
class R2Credentials:
    account_id: str
    bucket_name: str
    region: str
    read_access_key: str
    read_secret_key: str
    write_access_key: str
    write_secret_key: str

    @property
    def endpoint_url(self) -> str:
        return build_r2_endpoint_url(self.account_id)

    def validate_account_id(self) -> None:
        account_id = self.account_id.strip().lower()
        if not is_valid_r2_account_id(account_id):
            raise ValueError("R2 account_id must be 32 lowercase hex characters")

    def validate_read_key_lengths(self) -> None:
        read_key = self.read_access_key.strip()
        read_secret = self.read_secret_key.strip()
        if len(read_key) != _R2_READ_ACCESS_KEY_LEN:
            raise ValueError(
                "invalid R2 read access key length "
                f"({len(read_key)}), expected {_R2_READ_ACCESS_KEY_LEN}"
            )
        if len(read_secret) != _R2_READ_SECRET_KEY_LEN:
            raise ValueError(
                "invalid R2 read secret key length "
                f"({len(read_secret)}), expected {_R2_READ_SECRET_KEY_LEN}"
            )

    def validate_bucket_name(self) -> None:
        if not self.bucket_name.strip():
            raise ValueError("bucket name is required")

    def validate_bucket_for_hotkey(self, hotkey: str) -> None:
        expected = bucket_name_for_hotkey(hotkey)
        actual = self.bucket_name.strip().lower()
        if actual != expected:
            raise ValueError(
                f"bucket name must match lowercase hotkey (expected={expected}, actual={actual})"
            )

    @property
    def read_commitment(self) -> str:
        payload = f"{self.account_id}:{self.read_access_key}:{self.read_secret_key}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class R2S3Store:
    """S3 client wrapper for Cloudflare R2 object storage."""

    def __init__(self, credentials: R2Credentials):
        self.credentials = credentials
        self._session = get_session()
        self._upload_download_config = AioConfig(
            signature_version="s3v4",
            connect_timeout=5,
            read_timeout=30,
            s3={"addressing_style": "path"},
        )
        self._default_config = AioConfig(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        )

    async def upload_file(self, key: str, src: Path, use_write: bool = True) -> None:
        access, secret = self._select_keys(use_write=use_write)
        logger.debug(
            "upload start bucket=%s key=%s use_write=%s size_bytes=%d endpoint=%s",
            self.credentials.bucket_name,
            key,
            str(use_write),
            src.stat().st_size if src.exists() else -1,
            self.credentials.endpoint_url,
        )
        async with self._session.create_client(
            "s3",
            endpoint_url=self.credentials.endpoint_url,
            region_name=self.credentials.region,
            aws_access_key_id=access,
            aws_secret_access_key=secret,
            config=self._upload_download_config,
        ) as client:
            with src.open("rb") as fp:
                await client.put_object(Bucket=self.credentials.bucket_name, Key=key, Body=fp)
        logger.info("upload complete bucket=%s key=%s", self.credentials.bucket_name, key)

    async def download_file(self, key: str, dst: Path) -> bool:
        access, secret = self._select_keys(use_write=False)
        try:
            async with self._session.create_client(
                "s3",
                endpoint_url=self.credentials.endpoint_url,
                region_name=self.credentials.region,
                aws_access_key_id=access,
                aws_secret_access_key=secret,
                config=self._upload_download_config,
            ) as client:
                resp = await client.get_object(Bucket=self.credentials.bucket_name, Key=key)
                body = await resp["Body"].read()
                dst.parent.mkdir(parents=True, exist_ok=True)
                dst.write_bytes(body)
                logger.debug(
                    "download complete bucket=%s key=%s bytes=%d",
                    self.credentials.bucket_name,
                    key,
                    len(body),
                )
                return True
        except ClientError as exc:
            if _is_not_found_error(exc):
                logger.debug("download missing bucket=%s key=%s", self.credentials.bucket_name, key)
                return False
            logger.warning(
                "download failed bucket=%s key=%s code=%s error=%s",
                self.credentials.bucket_name,
                key,
                _client_error_code(exc),
                exc,
            )
            return False
        except Exception as exc:  # pragma: no cover - network service dependent
            logger.warning(
                "download failed bucket=%s key=%s error=%s",
                self.credentials.bucket_name,
                key,
                exc,
            )
            return False

    async def object_exists(self, key: str) -> bool:
        access, secret = self._select_keys(use_write=False)
        try:
            async with self._session.create_client(
                "s3",
                endpoint_url=self.credentials.endpoint_url,
                region_name=self.credentials.region,
                aws_access_key_id=access,
                aws_secret_access_key=secret,
                config=self._default_config,
            ) as client:
                await client.head_object(Bucket=self.credentials.bucket_name, Key=key)
                logger.debug("object exists bucket=%s key=%s", self.credentials.bucket_name, key)
                return True
        except ClientError as exc:
            if _is_not_found_error(exc):
                logger.debug("object missing bucket=%s key=%s", self.credentials.bucket_name, key)
                return False
            logger.warning(
                "head failed bucket=%s key=%s code=%s error=%s",
                self.credentials.bucket_name,
                key,
                _client_error_code(exc),
                exc,
            )
            raise

    async def list_prefix(self, prefix: str) -> list[str]:
        access, secret = self._select_keys(use_write=False)
        async with self._session.create_client(
            "s3",
            endpoint_url=self.credentials.endpoint_url,
            region_name=self.credentials.region,
            aws_access_key_id=access,
            aws_secret_access_key=secret,
            config=self._default_config,
        ) as client:
            paginator = client.get_paginator("list_objects_v2")
            keys: list[str] = []
            async for page in paginator.paginate(Bucket=self.credentials.bucket_name, Prefix=prefix):
                for item in page.get("Contents", []):
                    keys.append(item["Key"])
            return keys

    async def get_object_last_modified(self, key: str) -> datetime | None:
        access, secret = self._select_keys(use_write=False)
        try:
            async with self._session.create_client(
                "s3",
                endpoint_url=self.credentials.endpoint_url,
                region_name=self.credentials.region,
                aws_access_key_id=access,
                aws_secret_access_key=secret,
                config=self._default_config,
            ) as client:
                resp = await client.head_object(Bucket=self.credentials.bucket_name, Key=key)
                value = resp.get("LastModified")
                return value if isinstance(value, datetime) else None
        except ClientError as exc:
            if _is_not_found_error(exc):
                return None
            logger.warning(
                "last-modified lookup failed bucket=%s key=%s code=%s error=%s",
                self.credentials.bucket_name,
                key,
                _client_error_code(exc),
                exc,
            )
            return None
        except Exception:
            return None

    def _select_keys(self, use_write: bool) -> tuple[str, str]:
        if use_write:
            return self.credentials.write_access_key, self.credentials.write_secret_key
        return self.credentials.read_access_key, self.credentials.read_secret_key


def _client_error_code(exc: ClientError) -> str:
    error = exc.response.get("Error", {})
    code = error.get("Code")
    return str(code) if code is not None else ""


def _is_not_found_error(exc: ClientError) -> bool:
    code = _client_error_code(exc)
    if code in _NOT_FOUND_ERROR_CODES:
        return True
    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return status == 404
