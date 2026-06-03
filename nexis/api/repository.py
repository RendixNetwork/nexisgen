"""Persistence layer for validator API."""

from __future__ import annotations

from typing import Any

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
        # Schema migration: add reason/cycle_id/updated_at without dropping
        # the existing >100 rows. ALTER ... ADD COLUMN IF NOT EXISTS is
        # idempotent (PG 9.6+). For legacy rows that pre-date these columns,
        # backfill updated_at from added_at so their original timing isn't
        # lost; new rows use NOW().
        await self._db.execute(
            "ALTER TABLE invalid_hotkeys ADD COLUMN IF NOT EXISTS "
            "reason TEXT NOT NULL DEFAULT ''"
        )
        await self._db.execute(
            "ALTER TABLE invalid_hotkeys ADD COLUMN IF NOT EXISTS cycle_id INTEGER"
        )
        await self._db.execute(
            "ALTER TABLE invalid_hotkeys ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ"
        )
        await self._db.execute(
            "UPDATE invalid_hotkeys SET updated_at = added_at WHERE updated_at IS NULL"
        )
        await self._db.execute(
            "ALTER TABLE invalid_hotkeys ALTER COLUMN updated_at SET DEFAULT NOW()"
        )
        await self._db.execute(
            "ALTER TABLE invalid_hotkeys ALTER COLUMN updated_at SET NOT NULL"
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

    async def upsert_invalid_hotkeys(
        self, *, entries: list[dict[str, Any]]
    ) -> int:
        """Insert or update `(hotkey, reason, cycle_id)` rows.

        A repeat post for an existing hotkey overwrites its reason/cycle_id and
        bumps `updated_at` to NOW(). Returns the count of rows touched.
        Last-write-wins within a single batch on hotkey collisions.
        """
        deduped: dict[str, tuple[str, int | None]] = {}
        for entry in entries:
            hotkey = str(entry.get("hotkey", "")).strip()
            if not hotkey:
                continue
            reason = str(entry.get("reason", "") or "").strip()
            raw_cycle = entry.get("cycle_id")
            try:
                cycle_id = int(raw_cycle) if raw_cycle is not None else None
            except (TypeError, ValueError):
                cycle_id = None
            deduped[hotkey] = (reason, cycle_id)
        if not deduped:
            return 0
        affected = 0
        for hotkey, (reason, cycle_id) in sorted(deduped.items()):
            row = await self._db.fetchval(
                """
                INSERT INTO invalid_hotkeys (hotkey, reason, cycle_id)
                VALUES ($1, $2, $3)
                ON CONFLICT (hotkey) DO UPDATE
                  SET reason = EXCLUDED.reason,
                      cycle_id = EXCLUDED.cycle_id,
                      updated_at = NOW()
                RETURNING 1
                """,
                hotkey,
                reason,
                cycle_id,
            )
            if row == 1:
                affected += 1
        return affected

    async def list_invalid_hotkeys(self) -> list[dict[str, Any]]:
        """Return every invalid-hotkey row with reason + cycle_id. Legacy
        rows that pre-date the schema migration have `reason=""` and
        `cycle_id=None`."""
        rows = await self._db.fetch(
            """
            SELECT hotkey, reason, cycle_id
            FROM invalid_hotkeys
            ORDER BY hotkey ASC
            """
        )
        out: list[dict[str, Any]] = []
        for row in rows:
            hotkey = str(row["hotkey"]).strip()
            if not hotkey:
                continue
            reason = str(row["reason"] or "")
            cycle_id = row["cycle_id"]
            out.append(
                {
                    "hotkey": hotkey,
                    "reason": reason,
                    "cycle_id": int(cycle_id) if cycle_id is not None else None,
                }
            )
        return out

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
