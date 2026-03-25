"""Pydantic schemas for validation evidence API."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, model_validator


class DecisionIngestItem(BaseModel):
    miner_hotkey: str = Field(min_length=1)
    accepted: bool
    failures: list[str] = Field(default_factory=list)
    record_count: int = Field(default=0, ge=0)
    global_overlap_pruned_count: int = Field(default=0, ge=0)
    cross_miner_overlap_pruned_count: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def _require_failures_for_reject(self) -> "DecisionIngestItem":
        if not self.accepted and not self.failures:
            raise ValueError("failures are required when accepted is false")
        return self


class ValidationResultsIngestRequest(BaseModel):
    interval_id: int = Field(ge=0)
    decisions: list[DecisionIngestItem] = Field(default_factory=list)

    @model_validator(mode="after")
    def _decisions_non_empty(self) -> "ValidationResultsIngestRequest":
        if not self.decisions:
            raise ValueError("decisions must not be empty")
        return self


class IngestResponse(BaseModel):
    saved: int = Field(ge=0)
    validator_hotkey: str
    interval_id: int = Field(ge=0)


class StoredDecision(BaseModel):
    interval_id: int = Field(ge=0)
    validator_hotkey: str
    miner_hotkey: str
    accepted: bool
    failures: list[str] = Field(default_factory=list)
    record_count: int = Field(default=0, ge=0)
    global_overlap_pruned_count: int = Field(default=0, ge=0)
    cross_miner_overlap_pruned_count: int = Field(default=0, ge=0)
    signature: str
    timestamp: int
    nonce: str
    body_sha256: str
    received_at: datetime


class QueryResponse(BaseModel):
    validator_hotkey: str
    interval_id: int = Field(ge=0)
    decisions: list[StoredDecision] = Field(default_factory=list)


class LatestResultDecision(BaseModel):
    interval_id: int = Field(ge=0)
    validator_hotkey: str
    miner_hotkey: str
    accepted: bool
    failures: list[str] = Field(default_factory=list)
    record_count: int = Field(default=0, ge=0)
    global_overlap_pruned_count: int = Field(default=0, ge=0)
    cross_miner_overlap_pruned_count: int = Field(default=0, ge=0)
    received_at: datetime


class LatestResultResponse(BaseModel):
    current_block: int = Field(ge=0)
    start_interval_id: int = Field(ge=0)
    end_interval_id: int = Field(ge=0)
    refreshed_every_blocks: int = Field(ge=1)
    cached_at: datetime
    decisions: list[LatestResultDecision] = Field(default_factory=list)


class InvalidHotkeysWindowResponse(BaseModel):
    interval_id: int = Field(ge=0)
    window_start_interval_id: int = Field(ge=0)
    window_end_interval_id: int = Field(ge=0)
    invalid_hotkeys: list[str] = Field(default_factory=list)


class InvalidHotkeysIngestRequest(BaseModel):
    interval_id: int = Field(ge=0)
    invalid_hotkeys: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _invalid_hotkeys_non_empty(self) -> "InvalidHotkeysIngestRequest":
        if not self.invalid_hotkeys:
            raise ValueError("invalid_hotkeys must not be empty")
        return self


class InvalidHotkeysIngestResponse(BaseModel):
    validator_hotkey: str
    interval_id: int = Field(ge=0)
    saved_count: int = Field(ge=0)

