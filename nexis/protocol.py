"""Protocol-level constants and policy decisions for Nexisgen."""

from __future__ import annotations

from dataclasses import dataclass


PROTOCOL_VERSION = "2.0.0"
SCHEMA_VERSION = "2.0.0"

# Dataset spec (frozen for v2 train/validate split).
SAMPLE_COUNT = 400
TARGET_WIDTH = 1280
TARGET_HEIGHT = 704
TARGET_FPS = 24
TARGET_NUM_FRAMES = 121
CLIP_DURATION_SEC = TARGET_NUM_FRAMES / TARGET_FPS  # ≈ 5.0417
CLIP_DURATION_TOLERANCE_SEC = 0.15
FPS_TOLERANCE = 0.05

# Overlap policy
OVERLAP_WINDOW_SEC = 4.5
GLOBAL_OVERLAP_REJECT_THRESHOLD = 100  # > threshold -> reject

# Weights
WEIGHT_SUBMISSION_INTERVAL_BLOCKS = 300
WEIGHT_TOP_K = 5
WEIGHT_DECAY_BASE = 0.5  # 1, 1/2, 1/4, 1/8, 1/16


@dataclass(frozen=True)
class HardFailurePolicy:
    """Hard checks reject the interval immediately for that miner."""

    reject_on_first_violation: bool = True
