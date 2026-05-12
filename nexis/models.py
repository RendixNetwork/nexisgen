"""Core schema models for Nexisgen miner submissions."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .protocol import (
    CLIP_DURATION_SEC,
    CLIP_DURATION_TOLERANCE_SEC,
    FPS_TOLERANCE,
    SAMPLE_COUNT,
    SCHEMA_VERSION,
    TARGET_FPS,
    TARGET_HEIGHT,
    TARGET_NUM_FRAMES,
    TARGET_WIDTH,
)

_DEFAULT_SPEC_ID = "video_v1"


class ClipRecord(BaseModel):
    """Single training clip row submitted by a miner."""

    clip_id: str = Field(min_length=1)
    clip_uri: str = Field(min_length=1)
    clip_sha256: str = Field(min_length=64, max_length=64)
    first_frame_uri: str = Field(min_length=1)
    first_frame_sha256: str = Field(min_length=64, max_length=64)
    source_video_id: str = Field(min_length=1)
    clip_start_sec: float = Field(ge=0.0)
    duration_sec: float = Field(gt=0.0)
    width: int = Field(gt=0)
    height: int = Field(gt=0)
    fps: float = Field(gt=0.0)
    num_frames: int = Field(gt=0)
    source_video_url: str = Field(min_length=1)
    # Per-clip prompt used by the trainer. Empty means "use the trainer's
    # default prompt instead". Validator does not LLM-score these.
    caption: str = Field(default="")

    model_config = ConfigDict(str_strip_whitespace=True)

    @field_validator("duration_sec")
    @classmethod
    def validate_duration(cls, value: float) -> float:
        lower = CLIP_DURATION_SEC - CLIP_DURATION_TOLERANCE_SEC
        upper = CLIP_DURATION_SEC + CLIP_DURATION_TOLERANCE_SEC
        if value < lower or value > upper:
            raise ValueError(
                f"duration_sec must be within ±{CLIP_DURATION_TOLERANCE_SEC}s of "
                f"{CLIP_DURATION_SEC:.4f}"
            )
        return value

    @field_validator("width")
    @classmethod
    def validate_width(cls, value: int) -> int:
        if value != TARGET_WIDTH:
            raise ValueError(f"width must be exactly {TARGET_WIDTH}")
        return value

    @field_validator("height")
    @classmethod
    def validate_height(cls, value: int) -> int:
        if value != TARGET_HEIGHT:
            raise ValueError(f"height must be exactly {TARGET_HEIGHT}")
        return value

    @field_validator("fps")
    @classmethod
    def validate_fps(cls, value: float) -> float:
        if abs(value - TARGET_FPS) > FPS_TOLERANCE:
            raise ValueError(f"fps must be within ±{FPS_TOLERANCE} of {TARGET_FPS}")
        return value

    @field_validator("num_frames")
    @classmethod
    def validate_num_frames(cls, value: int) -> int:
        if value != TARGET_NUM_FRAMES:
            raise ValueError(f"num_frames must be exactly {TARGET_NUM_FRAMES}")
        return value


class IntervalManifest(BaseModel):
    """Interval-level metadata for miner submission package."""

    protocol_version: str = Field(default="2.0.0")
    schema_version: str = Field(default=SCHEMA_VERSION)
    spec_id: str = Field(default=_DEFAULT_SPEC_ID, min_length=1)
    netuid: int = Field(ge=0)
    miner_hotkey: str = Field(min_length=1)
    interval_id: int = Field(ge=1)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    record_count: int = Field(ge=0)
    dataset_sha256: str = Field(min_length=64, max_length=64)

    model_config = ConfigDict(str_strip_whitespace=True)

    @field_validator("record_count")
    @classmethod
    def validate_record_count(cls, value: int) -> int:
        if value != SAMPLE_COUNT:
            raise ValueError(f"record_count must be exactly {SAMPLE_COUNT}")
        return value

    @model_validator(mode="before")
    @classmethod
    def _normalize_spec_metadata(cls, payload: Any) -> Any:
        if not isinstance(payload, dict):
            return payload
        data = dict(payload)
        spec_id = str(data.get("spec_id", "")).strip() or _DEFAULT_SPEC_ID
        data["spec_id"] = spec_id
        return data


class ValidationDecision(BaseModel):
    """Per-miner validator decision for one training cycle."""

    miner_hotkey: str
    interval_id: int
    accepted: bool
    failures: list[str] = Field(default_factory=list)
    record_count: int = 0
    global_overlap_count: int = 0
    checked_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    notes: dict[str, Any] = Field(default_factory=dict)
