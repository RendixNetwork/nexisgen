from __future__ import annotations

from nexis.validator.sampling import select_miners, select_row_indices


def test_deterministic_miner_sampling() -> None:
    miners = [f"hk{i}" for i in range(20)]
    seed = "seed1"
    a = select_miners(miners, seed)
    b = select_miners(miners, seed)
    assert a == b


def test_deterministic_row_sampling() -> None:
    rows = 100
    hk = "hk1"
    seed = "seed2"
    a = select_row_indices(rows, hk, seed)
    b = select_row_indices(rows, hk, seed)
    assert a == b
    assert len(a) <= 3

