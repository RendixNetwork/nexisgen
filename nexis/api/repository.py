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
        # NOTE: invalid + blacklist hotkeys are no longer stored in Postgres.
        # They live as local `eligibility/` JSON files and are served by the
        # GET endpoints via EligibilityCache. The DB is used only for nonce
        # replay protection now.

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
