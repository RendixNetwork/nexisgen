"""Protocol-level constants and policy decisions for Nexisgen."""

from __future__ import annotations

from dataclasses import dataclass


PROTOCOL_VERSION = "1.0.0"
SCHEMA_VERSION = "1.0.0"

# Interval semantics (frozen for v1)
INTERVAL_MODE = "blocks"
INTERVAL_LENGTH_BLOCKS = 100
WEIGHT_SUBMISSION_INTERVAL_BLOCKS = 300
UPLOAD_DEADLINE_RESERVED_BLOCKS = 2

# Miner sampling semantics
MINER_SAMPLE_RATE = 1
MINER_SAMPLE_MIN = 2
MINER_SAMPLE_MAX = 35

# Per-miner row sampling semantics
ROW_SAMPLE_ALL_THRESHOLD = 3
ROW_SAMPLE_RATE = 0.20
ROW_SAMPLE_MAX = 3

# Scoring
SCORING_EXPONENT = 3
FAILURE_LOOKBACK_INTERVALS = 1

# Data policy
CLIP_DURATION_SEC = 5.0
MIN_CLIP_GAP_SEC = 5.0


@dataclass(frozen=True)
class SoftFailurePolicy:
    """Threshold policy for soft checks."""

    threshold: float = 0.50


@dataclass(frozen=True)
class HardFailurePolicy:
    """Hard checks reject the interval immediately for that miner."""

    reject_on_first_violation: bool = True

