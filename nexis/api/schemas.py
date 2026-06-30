"""Pydantic schemas for the validator API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class TrainingScoreEntry(BaseModel):
    aggregate: float = Field(ge=0.0)
    # Per-dimension aggregate score (VBench `[0]` per dimension).
    dimensions: dict[str, float] = Field(default_factory=dict)
    # Optional: raw VBench dimension blob (`[aggregate, [per_video...]]`)
    # preserved verbatim in the per-validator score file so consumers can
    # drill into the per-video breakdown.
    full_dimensions: dict[str, Any] = Field(default_factory=dict)
    # Miner-side interval_id of the dataset that produced these outputs.
    # Stamped by the trainer in `_done.json`; preserved into the score file.
    miner_interval_id: int | None = None


class TrainingScoresIngestRequest(BaseModel):
    cycle_id: int = Field(ge=1)
    scores: dict[str, TrainingScoreEntry] = Field(default_factory=dict)


class TrainingScoresIngestResponse(BaseModel):
    validator_hotkey: str
    cycle_id: int = Field(ge=1)
    miner_count: int = Field(ge=0)


class InvalidHotkeyEntry(BaseModel):
    """One row in the invalid_hotkeys table (read-only via the API now).

    `reason` is free-form ("selected" for chosen miners, or a `;`-joined list
    of failure tags for rejected ones); `cycle_id` is the training cycle that
    produced the verdict. Both may be empty for legacy rows. The list is no
    longer writable through the API — each validator maintains its own
    eligibility lists locally — so only the read response model remains.
    """

    hotkey: str = Field(min_length=1)
    reason: str = ""
    cycle_id: int | None = None


class InvalidHotkeysListResponse(BaseModel):
    invalid_hotkeys: list[InvalidHotkeyEntry] = Field(default_factory=list)


class BlacklistResponse(BaseModel):
    blacklist_hotkeys: list[str] = Field(default_factory=list)
