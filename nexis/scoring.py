"""Weight computation from total_score.json."""

from __future__ import annotations

import logging
from typing import Any

from .protocol import WEIGHT_DECAY_BASE, WEIGHT_TOP_K

logger = logging.getLogger(__name__)


def parse_total_score_payload(payload: dict[str, Any] | None) -> dict[str, float]:
    """Convert a `total_score.json` payload into {hotkey: aggregate}."""
    if not isinstance(payload, dict):
        return {}
    scores = payload.get("scores")
    if not isinstance(scores, dict):
        return {}
    out: dict[str, float] = {}
    for hotkey, value in scores.items():
        if isinstance(value, dict):
            raw = value.get("aggregate", value.get("score"))
        else:
            raw = value
        try:
            out[str(hotkey)] = float(raw)
        except (TypeError, ValueError):
            continue
    return out


def compute_top_k_weights(
    miner_scores: dict[str, float],
    *,
    top_k: int = WEIGHT_TOP_K,
    decay_base: float = WEIGHT_DECAY_BASE,
) -> dict[str, float]:
    """Top-K miners by score get geometric weights normalized to sum=1.

    Weights before normalization are [1, decay_base, decay_base^2, ...] for ranks 1..K.
    Returns empty dict if no positive scores.
    """
    positives = [(hotkey, score) for hotkey, score in miner_scores.items() if score > 0.0]
    positives.sort(key=lambda item: (-item[1], item[0]))
    selected = positives[:top_k]
    if not selected:
        return {}
    raw_weights: dict[str, float] = {}
    for rank, (hotkey, _) in enumerate(selected):
        raw_weights[hotkey] = decay_base**rank
    total = sum(raw_weights.values())
    if total <= 0:
        return {}
    return {hotkey: value / total for hotkey, value in raw_weights.items()}
