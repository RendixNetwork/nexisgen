"""Runtime configuration for Nexisgen."""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


load_dotenv(override=False)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    netuid: int = Field(default=0, alias="NEXIS_NETUID")
    log_level: str = Field(default="INFO", alias="NEXIS_LOG_LEVEL")
    bt_network: str = Field(default="finney", alias="BT_NETWORK")
    bt_wallet_name: str = Field(default="default", alias="BT_WALLET_NAME")
    bt_wallet_hotkey: str = Field(default="default", alias="BT_WALLET_HOTKEY")
    bt_wallet_path: Path = Field(default=Path("~/.bittensor/wallets"), alias="BT_WALLET_PATH")

    r2_account_id: str = Field(default="", alias="R2_ACCOUNT_ID")
    r2_region: str = Field(default="auto", alias="R2_REGION")
    r2_read_access_key: str = Field(default="", alias="R2_READ_ACCESS_KEY")
    r2_read_secret_key: str = Field(default="", alias="R2_READ_SECRET_KEY")
    r2_write_access_key: str = Field(default="", alias="R2_WRITE_ACCESS_KEY")
    r2_write_secret_key: str = Field(default="", alias="R2_WRITE_SECRET_KEY")

    sources_file: Path = Field(default=Path("sources.txt"), alias="NEXIS_SOURCES_FILE")
    workdir: Path = Field(default=Path(".nexis"), alias="NEXIS_WORKDIR")
    block_poll_sec: float = Field(default=6.0, alias="NEXIS_BLOCK_POLL_SEC")
    dataset_spec_default: str = Field(default="video_v1", alias="NEXIS_DATASET_SPEC_DEFAULT")
    miner_enabled_specs: str = Field(default="video_v1", alias="NEXIS_MINER_ENABLED_SPECS")
    validator_enabled_specs: str = Field(
        default="video_v1",
        alias="NEXIS_VALIDATOR_ENABLED_SPECS",
    )
    validator_blacklist_file: Path = Field(
        default=Path("validator_blacklist_hotkeys.txt"),
        alias="NEXIS_VALIDATOR_BLACKLIST_FILE",
    )

    openai_api_key: str = Field(default="", alias="OPENAI_API_KEY")
    gemini_api_key: str = Field(default="", alias="GEMINI_API_KEY")
    caption_model: str = Field(default="gpt-4o", alias="NEXIS_CAPTION_MODEL")
    caption_timeout_sec: int = Field(default=30, alias="NEXIS_CAPTION_TIMEOUT_SEC")
    validator_semantic_check_enabled: bool = Field(
        default=True,
        alias="NEXIS_VALIDATOR_SEMANTIC_CHECK_ENABLED",
    )
    validator_semantic_model: str = Field(
        default="gpt-4o",
        alias="NEXIS_VALIDATOR_SEMANTIC_MODEL",
    )
    validator_semantic_timeout_sec: int = Field(
        default=20,
        alias="NEXIS_VALIDATOR_SEMANTIC_TIMEOUT_SEC",
    )
    validator_semantic_max_samples: int = Field(
        default=8,
        alias="NEXIS_VALIDATOR_SEMANTIC_MAX_SAMPLES",
    )

    owner_validator_hotkey: str = Field(
        default="5EUdjwHz9pW4ftQQQga9PKq7knGiGW9wcHUjkDSih7zpovPy",
        alias="NEXIS_OWNER_VALIDATOR_HOTKEY",
    )

    owner_db_bucket: str = Field(default="nexis-dataset", alias="NEXIS_OWNER_DB_BUCKET")
    owner_db_account_id: str = Field(default="", alias="NEXIS_OWNER_DB_ACCOUNT_ID")
    owner_db_read_access_key: str = Field(default="", alias="NEXIS_OWNER_DB_READ_ACCESS_KEY")
    owner_db_read_secret_key: str = Field(default="", alias="NEXIS_OWNER_DB_READ_SECRET_KEY")
    owner_db_write_access_key: str = Field(default="", alias="NEXIS_OWNER_DB_WRITE_ACCESS_KEY")
    owner_db_write_secret_key: str = Field(default="", alias="NEXIS_OWNER_DB_WRITE_SECRET_KEY")

    record_info_bucket: str = Field(default="nexis-record-info", alias="NEXIS_RECORD_INFO_BUCKET")
    record_info_account_id: str = Field(default="", alias="NEXIS_RECORD_INFO_ACCOUNT_ID")
    record_info_read_access_key: str = Field(default="", alias="NEXIS_RECORD_INFO_READ_ACCESS_KEY")
    record_info_read_secret_key: str = Field(default="", alias="NEXIS_RECORD_INFO_READ_SECRET_KEY")
    record_info_write_access_key: str = Field(default="", alias="NEXIS_RECORD_INFO_WRITE_ACCESS_KEY")
    record_info_write_secret_key: str = Field(default="", alias="NEXIS_RECORD_INFO_WRITE_SECRET_KEY")
    record_info_object_key: str = Field(default="record_info.json", alias="NEXIS_RECORD_INFO_OBJECT_KEY")

    # Optional validator -> evidence API reporting
    validation_api_url: str = Field(default="https://api.nexisgen.ai/v1/validation-results", alias="NEXIS_VALIDATION_API_URL")
    validation_api_timeout_sec: float = Field(default=120.0, alias="NEXIS_VALIDATION_API_TIMEOUT_SEC")
    latest_result_timeout_sec: float = Field(
        default=120.0,
        alias="NEXIS_LATEST_RESULT_TIMEOUT_SEC",
        description="GET /v1/get_latest_result can return a large JSON window; allow a generous read timeout.",
    )

    # Evidence API server settings
    validation_api_postgres_dsn: str = Field(
        default="postgresql://nexis:nexis@localhost:5432/nexis_validation",
        alias="NEXIS_VALIDATION_API_POSTGRES_DSN",
    )
    validation_api_allowlist_refresh_sec: int = Field(
        default=300,
        alias="NEXIS_VALIDATION_API_ALLOWLIST_REFRESH_SEC",
    )
    validation_api_min_validator_stake: float = Field(
        default=5000.0,
        alias="NEXIS_VALIDATION_API_MIN_VALIDATOR_STAKE",
    )
    validation_api_auth_max_skew_sec: int = Field(
        default=300,
        alias="NEXIS_VALIDATION_API_AUTH_MAX_SKEW_SEC",
    )
    validation_api_nonce_max_age_sec: int = Field(
        default=86400,
        alias="NEXIS_VALIDATION_API_NONCE_MAX_AGE_SEC",
    )


def load_settings() -> Settings:
    return Settings()

