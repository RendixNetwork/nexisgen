"""Verify the v2 VBench parser extracts only `dim[0]` aggregates."""

from __future__ import annotations

import json
from pathlib import Path

from nexis.validator.vbench_scorer import (
    _extract_dimension_aggregate,
    aggregate_score,
    parse_vbench_results,
)


def test_extract_dimension_aggregate_list_form() -> None:
    assert _extract_dimension_aggregate([0.638, [{"video_path": "x", "video_results": 0.595}]]) == 0.638


def test_extract_dimension_aggregate_bare_float() -> None:
    assert _extract_dimension_aggregate(0.9) == 0.9
    assert _extract_dimension_aggregate(42) == 42.0


def test_extract_dimension_aggregate_non_numeric_head_returns_none() -> None:
    assert _extract_dimension_aggregate([{"video_path": "x"}, 0.6]) is None
    assert _extract_dimension_aggregate([]) is None
    assert _extract_dimension_aggregate("nope") is None
    assert _extract_dimension_aggregate(None) is None


def test_parse_only_picks_index_zero(tmp_path: Path) -> None:
    payload = {
        "aesthetic_quality": [
            0.638,
            [
                {"video_path": "a.mp4", "video_results": 0.10},
                {"video_path": "b.mp4", "video_results": 0.20},
                {"video_path": "c.mp4", "video_results": 0.30},
            ],
        ],
        "imaging_quality": [
            0.85,
            [{"video_path": "d.mp4", "video_results": 0.99}],
        ],
    }
    (tmp_path / "results.json").write_text(json.dumps(payload), encoding="utf-8")
    dims = parse_vbench_results(tmp_path)
    assert dims == {"aesthetic_quality": 0.638, "imaging_quality": 0.85}


def test_aggregate_is_unweighted_mean_of_dimensions() -> None:
    assert aggregate_score({"a": 0.638, "b": 0.85}) == (0.638 + 0.85) / 2
    assert aggregate_score({}) == 0.0


def test_per_video_entries_do_not_inflate_aggregate(tmp_path: Path) -> None:
    # 32 per-video MUSIQ scores hover around 60 — should NOT contribute to mean.
    payload = {
        "aesthetic_quality": [
            0.638,
            [{"video_path": f"x{i}.mp4", "video_results": 60.0} for i in range(32)],
        ]
    }
    (tmp_path / "r.json").write_text(json.dumps(payload), encoding="utf-8")
    dims = parse_vbench_results(tmp_path)
    assert dims == {"aesthetic_quality": 0.638}
    assert aggregate_score(dims) == 0.638
