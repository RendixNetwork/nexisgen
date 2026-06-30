"""Runtime configuration for Nexisgen."""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from . import CONFIG_JSON_PATH, MODELS_DIR


load_dotenv(override=False)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    netuid: int = Field(default=70, alias="NEXIS_NETUID")
    log_level: str = Field(default="INFO", alias="NEXIS_LOG_LEVEL")
    bt_network: str = Field(default="finney", alias="BT_NETWORK")
    bt_wallet_name: str = Field(default="default", alias="BT_WALLET_NAME")
    bt_wallet_hotkey: str = Field(default="default", alias="BT_WALLET_HOTKEY")
    bt_wallet_path: Path = Field(default=Path("~/.bittensor/wallets"), alias="BT_WALLET_PATH")

    # Miner R2 (per-hotkey bucket)
    r2_account_id: str = Field(default="", alias="R2_ACCOUNT_ID")
    r2_region: str = Field(default="auto", alias="R2_REGION")
    r2_read_access_key: str = Field(default="", alias="R2_READ_ACCESS_KEY")
    r2_read_secret_key: str = Field(default="", alias="R2_READ_SECRET_KEY")
    r2_write_access_key: str = Field(default="", alias="R2_WRITE_ACCESS_KEY")
    r2_write_secret_key: str = Field(default="", alias="R2_WRITE_SECRET_KEY")

    sources_file: Path = Field(default=Path("sources.txt"), alias="NEXIS_SOURCES_FILE")
    workdir: Path = Field(default=Path(".nexis"), alias="NEXIS_WORKDIR")
    block_poll_sec: float = Field(default=6.0, alias="NEXIS_BLOCK_POLL_SEC")

    miner_loop_sleep_sec: float = Field(default=60.0, alias="NEXIS_MINER_LOOP_SLEEP_SEC")
    train_poll_sec: float = Field(default=30.0, alias="NEXIS_TRAIN_POLL_SEC")
    score_poll_sec: float = Field(default=30.0, alias="NEXIS_SCORE_POLL_SEC")

    # Miner captioning. If neither key is set, miners produce empty captions
    # and the trainer falls back to NEXIS_TRAINER_DEFAULT_PROMPT.
    openai_api_key: str = Field(default="", alias="OPENAI_API_KEY")
    gemini_api_key: str = Field(default="", alias="GEMINI_API_KEY")
    caption_model: str = Field(default="gpt-4o-mini", alias="NEXIS_CAPTION_MODEL")
    caption_timeout_sec: int = Field(default=30, alias="NEXIS_CAPTION_TIMEOUT_SEC")

    # I/O concurrency for R2 batch operations.
    # download_concurrency: max concurrent GETs per miner during dataset/video downloads.
    # upload_concurrency:   max concurrent PUTs when uploading training outputs.
    # miner_gather_concurrency: max miners validated in parallel by `nexis train`.
    download_concurrency: int = Field(default=16, alias="NEXIS_DOWNLOAD_CONCURRENCY")
    upload_concurrency: int = Field(default=8, alias="NEXIS_UPLOAD_CONCURRENCY")
    miner_gather_concurrency: int = Field(default=4, alias="NEXIS_MINER_GATHER_CONCURRENCY")

    owner_validator_hotkey: str = Field(
        default="5EJGfSvRcEGVQtqDuU7YYwuZRHmaktf6JEZDeFPyeXksiHrm",
        alias="NEXIS_OWNER_VALIDATOR_HOTKEY",
    )

    # Shared nexis_miner bucket (training outputs + scores)
    nexis_miner_bucket: str = Field(default="nexis-miner", alias="NEXIS_MINER_BUCKET")
    nexis_miner_account_id: str = Field(default="cce499ad4f3a4703b069771d8ff4215a", alias="NEXIS_MINER_ACCOUNT_ID")
    nexis_miner_read_access_key: str = Field(default="c7df3d75bcf89b19e9fccd2866957922", alias="NEXIS_MINER_READ_ACCESS_KEY")
    nexis_miner_read_secret_key: str = Field(default="d04e506a8a155a5e729ada81d2c54f5397f29e061672f2c78bf7b5a2731eda69", alias="NEXIS_MINER_READ_SECRET_KEY")
    nexis_miner_write_access_key: str = Field(default="", alias="NEXIS_MINER_WRITE_ACCESS_KEY")
    nexis_miner_write_secret_key: str = Field(default="", alias="NEXIS_MINER_WRITE_SECRET_KEY")

    # Global overlap snapshot bucket (legacy record-info)
    record_info_bucket: str = Field(default="nexis-record-info", alias="NEXIS_RECORD_INFO_BUCKET")
    record_info_account_id: str = Field(default="cce499ad4f3a4703b069771d8ff4215a", alias="NEXIS_RECORD_INFO_ACCOUNT_ID")
    record_info_read_access_key: str = Field(default="0fa291e03819c60474fed86a4932e652", alias="NEXIS_RECORD_INFO_READ_ACCESS_KEY")
    record_info_read_secret_key: str = Field(default="7bfbc213f3295c0a7f88db3f069490ce474e82520b4455b6a7bc7aa5e66224ee", alias="NEXIS_RECORD_INFO_READ_SECRET_KEY")
    record_info_write_access_key: str = Field(default="", alias="NEXIS_RECORD_INFO_WRITE_ACCESS_KEY")
    record_info_write_secret_key: str = Field(default="", alias="NEXIS_RECORD_INFO_WRITE_SECRET_KEY")
    record_info_object_key: str = Field(default="record_info.json", alias="NEXIS_RECORD_INFO_OBJECT_KEY")

    # Eval-data bucket (read-only). Default credentials let every validator
    # download the canonical eval dataset out of the box; override via env if
    # the network rotates keys or moves buckets. Synced to
    # `<workdir>/eval_data/` before every training and scoring cycle so each
    # validator's trainer / vbench containers see fresh data.
    nexis_eval_bucket: str = Field(
        default="nexis-eval", alias="NEXIS_EVAL_BUCKET"
    )
    nexis_eval_account_id: str = Field(
        default="cce499ad4f3a4703b069771d8ff4215a",
        alias="NEXIS_EVAL_ACCOUNT_ID",
    )
    nexis_eval_read_access_key: str = Field(
        default="168d66ba8c6cacf91c6374b408a5d593",
        alias="NEXIS_EVAL_READ_ACCESS_KEY",
    )
    nexis_eval_read_secret_key: str = Field(
        default=(
            "3aa4440df9db6f77e8cba83d8d1252666775f282e2c65dcc0ce62b08dba4a8c4"
        ),
        alias="NEXIS_EVAL_READ_SECRET_KEY",
    )
    nexis_eval_prefix: str = Field(
        default="eval_data/", alias="NEXIS_EVAL_PREFIX"
    )

    # Trainer — host-side paths default to repo-root assets; container-side
    # paths are fixed in nexis/validator/training.py to match the trainer image.
    # Override via env vars to point at external locations.
    trainer_num_gpus: int = Field(default=8, alias="NEXIS_TRAINER_NUM_GPUS")
    trainer_models_dir: Path = Field(
        default_factory=lambda: MODELS_DIR,
        alias="NEXIS_TRAINER_MODELS_DIR",
    )
    trainer_config_json: Path = Field(
        default_factory=lambda: CONFIG_JSON_PATH,
        alias="NEXIS_TRAINER_CONFIG_JSON",
    )
    trainer_docker_image: str = Field(
        default="rendixnetwork/train:latest",
        alias="NEXIS_TRAINER_DOCKER_IMAGE",
    )
    trainer_shm_size: str = Field(default="16g", alias="NEXIS_TRAINER_SHM_SIZE")
    trainer_timeout_sec: int = Field(default=24 * 3600, alias="NEXIS_TRAINER_TIMEOUT_SEC")

    # VBench scoring
    vbench_docker_image: str = Field(
        default="rendixnetwork/vbench:latest",
        alias="NEXIS_VBENCH_DOCKER_IMAGE",
    )
    vbench_results_dir: Path = Field(
        default=Path("/workspace/VBench/results"),
        alias="NEXIS_VBENCH_RESULTS_DIR",
    )
    vbench_dimensions: str = Field(
        default=(
            "i2v_subject,i2v_background,subject_consistency,background_consistency,"
            "motion_smoothness,dynamic_degree,aesthetic_quality,imaging_quality"
        ),
        alias="NEXIS_VBENCH_DIMENSIONS",
    )
    vbench_timeout_sec: int = Field(default=6 * 3600, alias="NEXIS_VBENCH_TIMEOUT_SEC")

    # API endpoints / auth
    validation_api_url: str = Field(
        default="https://api.nexisgen.ai/v1/training-scores",
        alias="NEXIS_VALIDATION_API_URL",
    )
    validation_api_timeout_sec: float = Field(default=120.0, alias="NEXIS_VALIDATION_API_TIMEOUT_SEC")

    # Per-validator local miner-eligibility policy. Each validator keeps its
    # own invalid + blacklist hotkey lists as JSON files instead of
    # fetching/posting them to a central API. The validator reads both files
    # and appends newly-invalidated hotkeys to its own copy — it never contacts
    # the API for eligibility. The repo ships seed files in `eligibility/`
    # (committed); each validator uses and updates its own working copy.
    eligibility_dir: Path = Field(
        default=Path("eligibility"), alias="NEXIS_ELIGIBILITY_DIR"
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
    validation_api_admin_token: str = Field(
        default="",
        alias="NEXIS_VALIDATION_API_ADMIN_TOKEN",
    )


def load_settings() -> Settings:
    return Settings()
