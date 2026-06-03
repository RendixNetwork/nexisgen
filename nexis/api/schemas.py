"""Pydantic schemas for the validator API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class TrainingScoreEntry(BaseModel):
    aggregate: float = Field(ge=0.0)
    # Per-dimension aggregate score (VBench `[0]` per dimension), used by the
    # API to roll up into total_score.json.
    dimensions: dict[str, float] = Field(default_factory=dict)
    # Optional: raw VBench dimension blob (`[aggregate, [per_video...]]`)
    # preserved verbatim in the per-validator JSON so consumers can drill
    # into the per-video breakdown.  Not used by total_score aggregation.
    full_dimensions: dict[str, Any] = Field(default_factory=dict)
    # Miner-side interval_id of the dataset that produced these outputs.
    # Stamped by the trainer in `_done.json`; preserved into total_score.json.
    miner_interval_id: int | None = None


class TrainingScoresIngestRequest(BaseModel):
    cycle_id: int = Field(ge=1)
    scores: dict[str, TrainingScoreEntry] = Field(default_factory=dict)


class TrainingScoresIngestResponse(BaseModel):
    validator_hotkey: str
    cycle_id: int = Field(ge=1)
    miner_count: int = Field(ge=0)


class InvalidHotkeyEntry(BaseModel):
    """One row in the invalid_hotkeys table.

    `reason` is free-form; the validator sends "selected" for hotkeys that
    passed validation (and were chosen for training) or a `;`-joined list of
    failure tags for hotkeys rejected by strict validation. `cycle_id` is the
    training cycle that produced the verdict. Both fields are nullable / may
    be empty for legacy rows migrated from the pre-reason schema.
    """

    hotkey: str = Field(min_length=1)
    reason: str = ""
    cycle_id: int | None = None


class InvalidHotkeysListResponse(BaseModel):
    invalid_hotkeys: list[InvalidHotkeyEntry] = Field(default_factory=list)


class InvalidHotkeysIngestRequest(BaseModel):
    invalid_hotkeys: list[InvalidHotkeyEntry] = Field(default_factory=list)


class InvalidHotkeysIngestResponse(BaseModel):
    validator_hotkey: str
    saved_count: int = Field(ge=0)


class InvalidHotkeysResetResponse(BaseModel):
    cleared: int = Field(ge=0)


class BlacklistResponse(BaseModel):
    blacklist_hotkeys: list[str] = Field(default_factory=list)


class TotalScoreResponse(BaseModel):
    cycle_id: int = Field(ge=1)
    scores: dict[str, dict[str, Any]] = Field(default_factory=dict)
