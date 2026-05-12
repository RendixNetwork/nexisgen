"""Thin re-exports for backwards compatibility.

The legacy `ValidatorPipeline` has been replaced by the train/validate split.
This module now exposes the dataset-check primitives used by `nexis train`.
"""

from .dataset_check import (
    DatasetCheckOutcome,
    canonical_source_key,
    latest_complete_interval_id,
    list_miner_interval_ids,
    validate_miner_dataset,
)

__all__ = [
    "DatasetCheckOutcome",
    "canonical_source_key",
    "latest_complete_interval_id",
    "list_miner_interval_ids",
    "validate_miner_dataset",
]
