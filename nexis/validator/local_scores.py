"""Per-validator local store of VBench score results, keyed by cycle_id.

The scoring loop writes a cycle's result here right after scoring; the scoring
loop (dedup), the set-weight loop (latest), and the `nexis train` cycle-gate all
read from HERE instead of the API-written `{cycle}/{validator_hotkey}.json`
bucket object.

This removes the central API from the training + weighting critical path: a
validator advances and sets weight on its OWN locally-recorded scoring result,
not on a file the API has to write first. The API POST is kept as a best-effort
side-effect (so the frontend / collector / record_info still see published
scores), but it is no longer a dependency.

Files live under `<workdir>/scores/{cycle_id}.json`, so the `nexis train` and
`nexis validate` processes on the same host (sharing NEXIS_WORKDIR) see each
other's results. The payload shape mirrors what the API stored, so existing
parsers (`parse_score_payload`) work unchanged.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class LocalScoreStore:
    """File-backed `{cycle_id: score_payload}` store, local to one validator."""

    def __init__(self, *, score_dir: Path):
        self._dir = Path(score_dir)

    @property
    def score_dir(self) -> Path:
        return self._dir

    def _path(self, cycle_id: int) -> Path:
        return self._dir / f"{int(cycle_id)}.json"

    def has(self, cycle_id: int) -> bool:
        return self._path(cycle_id).exists()

    def save(self, cycle_id: int, payload: dict) -> None:
        """Atomically write this cycle's score payload (tmp + rename), so a
        concurrent reader never sees a half-written file."""
        self._dir.mkdir(parents=True, exist_ok=True)
        path = self._path(cycle_id)
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
        )
        tmp.replace(path)

    def load(self, cycle_id: int) -> dict | None:
        path = self._path(cycle_id)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else None
        except Exception as exc:
            logger.warning("local score %s unreadable: %s", path.name, exc)
            return None

    def list_cycle_ids(self) -> list[int]:
        if not self._dir.exists():
            return []
        out: list[int] = []
        for p in self._dir.glob("*.json"):
            if p.stem.isdigit():
                out.append(int(p.stem))
        return sorted(out)

    def latest(self) -> tuple[int, dict] | None:
        """Return (cycle_id, payload) for the highest cycle with a readable
        local score, or None."""
        for cycle_id in reversed(self.list_cycle_ids()):
            payload = self.load(cycle_id)
            if payload is not None:
                return cycle_id, payload
        return None
