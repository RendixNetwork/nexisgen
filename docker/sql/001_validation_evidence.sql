-- Nexisgen v2 API schema.
-- These tables are also created at startup by ValidationEvidenceRepository.ensure_schema(),
-- but committing them here means the Postgres container has the schema available
-- before the API service first connects.

CREATE TABLE IF NOT EXISTS validator_request_nonces (
    validator_hotkey TEXT NOT NULL,
    nonce TEXT NOT NULL,
    signature_timestamp BIGINT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (validator_hotkey, nonce)
);

CREATE INDEX IF NOT EXISTS idx_validator_request_nonces_received_at
    ON validator_request_nonces (received_at);

-- Persistent "miners selected for training, do not re-select" list.
-- Cleared via DELETE /v1/invalid-hotkeys (requires X-Admin-Token).
CREATE TABLE IF NOT EXISTS invalid_hotkeys (
    hotkey TEXT PRIMARY KEY,
    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS blacklisted_hotkeys (
    hotkey TEXT PRIMARY KEY,
    reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
