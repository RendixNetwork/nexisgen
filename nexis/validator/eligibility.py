"""Per-validator local miner-eligibility policy store.

Each validator keeps its own eligibility lists on its own machine instead of
sharing them through a central API. Replaces the API round-trips
(`GET /v1/invalid-hotkeys`, `GET /v1/get_blacklist`, `POST /v1/invalid-hotkeys`)
with two local JSON files:

    <eligibility_dir>/invalid_hotkeys.json   {"invalid_hotkeys": [{hotkey, reason, cycle_id}, ...]}
    <eligibility_dir>/blacklist_hotkeys.json {"blacklist_hotkeys": ["hk", ...]}

The repo ships seed copies in `eligibility/` (committed, the default
`eligibility_dir`). On clone a validator gets the shared baseline, then uses
and updates its own working copy — it never contacts the API for eligibility.
The on-disk shapes mirror the old API GET responses.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

INVALID_FILE = "invalid_hotkeys.json"
BLACKLIST_FILE = "blacklist_hotkeys.json"


class LocalEligibilityStore:
    """File-backed invalid + blacklist hotkey lists, local to one validator."""

    def __init__(self, *, eligibility_dir: Path):
        self._dir = Path(eligibility_dir)
        self._invalid_path = self._dir / INVALID_FILE
        self._blacklist_path = self._dir / BLACKLIST_FILE

    @property
    def invalid_path(self) -> Path:
        return self._invalid_path

    @property
    def blacklist_path(self) -> Path:
        return self._blacklist_path

    # ── reads ────────────────────────────────────────────────────────────────

    def _read(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            logger.warning("eligibility: %s missing; treating as empty", path)
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception as exc:
            logger.warning("eligibility: %s unreadable: %s", path.name, exc)
            return {}

    def invalid_entries(self) -> list[dict[str, Any]]:
        """Return the raw invalid-hotkey entries (hotkey/reason/cycle_id).

        Tolerates bare-string entries (legacy) by wrapping them as
        `{hotkey, reason="", cycle_id=None}`.
        """
        values = self._read(self._invalid_path).get("invalid_hotkeys", [])
        if not isinstance(values, list):
            return []
        out: list[dict[str, Any]] = []
        for item in values:
            if isinstance(item, dict):
                hotkey = str(item.get("hotkey", "")).strip()
                if hotkey:
                    out.append(
                        {
                            "hotkey": hotkey,
                            "reason": str(item.get("reason", "") or ""),
                            "cycle_id": item.get("cycle_id"),
                        }
                    )
            else:
                hotkey = str(item).strip()
                if hotkey:
                    out.append({"hotkey": hotkey, "reason": "", "cycle_id": None})
        return out

    def invalid_hotkey_set(self) -> set[str]:
        return {e["hotkey"] for e in self.invalid_entries()}

    def blacklist_hotkey_set(self) -> set[str]:
        values = self._read(self._blacklist_path).get("blacklist_hotkeys", [])
        if not isinstance(values, list):
            return set()
        return {str(x).strip() for x in values if str(x).strip()}

    # ── writes ───────────────────────────────────────────────────────────────

    def _write(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(
            json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
        )
        tmp.replace(path)  # atomic on POSIX

    def add_invalid_hotkeys(self, entries: list[dict[str, Any]]) -> int:
        """Upsert `{hotkey, reason, cycle_id}` entries into the local invalid
        list (a repeat hotkey overwrites its reason/cycle_id) and save.

        Returns the number of distinct hotkeys written in this call.
        """
        by_hotkey: dict[str, dict[str, Any]] = {
            e["hotkey"]: e for e in self.invalid_entries()
        }
        written = 0
        for entry in entries:
            hotkey = str(entry.get("hotkey", "")).strip()
            if not hotkey:
                continue
            raw_cycle = entry.get("cycle_id")
            try:
                cycle_id = int(raw_cycle) if raw_cycle is not None else None
            except (TypeError, ValueError):
                cycle_id = None
            by_hotkey[hotkey] = {
                "hotkey": hotkey,
                "reason": str(entry.get("reason", "") or ""),
                "cycle_id": cycle_id,
            }
            written += 1
        merged = sorted(by_hotkey.values(), key=lambda e: e["hotkey"])
        self._write(self._invalid_path, {"invalid_hotkeys": merged})
        return written
