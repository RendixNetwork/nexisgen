"""Persistence layer for validator API."""

from __future__ import annotations

from .db import Database


class ValidationEvidenceRepository:
    """Read/write operations for the simplified v2 API.

    Kept the original class name so existing callers continue to work.
    """

    def __init__(self, db: Database):
        self._db = db

    async def ensure_schema(self) -> None:
        await self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS validator_request_nonces (
                validator_hotkey TEXT NOT NULL,
                nonce TEXT NOT NULL,
                signature_timestamp BIGINT NOT NULL,
                received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (validator_hotkey, nonce)
            )
            """
        )
        await self._db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_validator_request_nonces_received_at
                ON validator_request_nonces (received_at)
            """
        )
        await self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS invalid_hotkeys (
                hotkey TEXT PRIMARY KEY,
                added_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS blacklisted_hotkeys (
                hotkey TEXT PRIMARY KEY,
                reason TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )

    async def register_nonce_once(
        self,
        *,
        validator_hotkey: str,
        nonce: str,
        signature_timestamp: int,
        max_age_sec: int,
    ) -> bool:
        await self._db.execute(
            """
            DELETE FROM validator_request_nonces
            WHERE received_at < NOW() - ($1::BIGINT * INTERVAL '1 second')
            """,
            int(max(max_age_sec, 1)),
        )
        inserted = await self._db.fetchval(
            """
            INSERT INTO validator_request_nonces (
                validator_hotkey, nonce, signature_timestamp
            ) VALUES ($1, $2, $3)
            ON CONFLICT (validator_hotkey, nonce) DO NOTHING
            RETURNING 1
            """,
            validator_hotkey,
            nonce,
            int(signature_timestamp),
        )
        return inserted == 1

    async def add_invalid_hotkeys(self, *, hotkeys: list[str]) -> int:
        cleaned = sorted({item.strip() for item in hotkeys if item.strip()})
        if not cleaned:
            return 0
        inserted = 0
        for hotkey in cleaned:
            row = await self._db.fetchval(
                """
                INSERT INTO invalid_hotkeys (hotkey)
                VALUES ($1)
                ON CONFLICT (hotkey) DO NOTHING
                RETURNING 1
                """,
                hotkey,
            )
            if row == 1:
                inserted += 1
        return inserted

    async def list_invalid_hotkeys(self) -> list[str]:
        rows = await self._db.fetch(
            """
            SELECT hotkey FROM invalid_hotkeys ORDER BY hotkey ASC
            """
        )
        return [str(row["hotkey"]).strip() for row in rows if str(row["hotkey"]).strip()]

    async def reset_invalid_hotkeys(self) -> int:
        deleted = await self._db.fetchval(
            """
            WITH d AS (DELETE FROM invalid_hotkeys RETURNING 1)
            SELECT COUNT(*) FROM d
            """
        )
        return int(deleted or 0)

    async def get_blacklisted_hotkeys(self) -> list[str]:
        rows = await self._db.fetch(
            """
            SELECT hotkey FROM blacklisted_hotkeys ORDER BY hotkey ASC
            """
        )
        return [str(row["hotkey"]).strip() for row in rows if str(row["hotkey"]).strip()]
