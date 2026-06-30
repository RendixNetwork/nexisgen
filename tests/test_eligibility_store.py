"""Local eligibility store: read seeded files, upsert invalid hotkeys."""

from __future__ import annotations

import json
from pathlib import Path

from nexis.validator.eligibility import LocalEligibilityStore


def _seed(d: Path) -> None:
    (d / "invalid_hotkeys.json").write_text(
        json.dumps(
            {
                "invalid_hotkeys": [
                    {"hotkey": "hkA", "reason": "selected", "cycle_id": 5},
                    {"hotkey": "hkB", "reason": "", "cycle_id": None},
                    "hkC",  # legacy bare-string entry
                ]
            }
        ),
        encoding="utf-8",
    )
    (d / "blacklist_hotkeys.json").write_text(
        json.dumps({"blacklist_hotkeys": ["bad1", "bad2", "bad1"]}),
        encoding="utf-8",
    )


def test_reads_seeded_files(tmp_path: Path) -> None:
    _seed(tmp_path)
    s = LocalEligibilityStore(eligibility_dir=tmp_path)
    assert s.invalid_hotkey_set() == {"hkA", "hkB", "hkC"}
    assert s.blacklist_hotkey_set() == {"bad1", "bad2"}
    entries = {e["hotkey"]: e for e in s.invalid_entries()}
    assert entries["hkA"] == {"hotkey": "hkA", "reason": "selected", "cycle_id": 5}
    assert entries["hkC"] == {"hotkey": "hkC", "reason": "", "cycle_id": None}


def test_missing_files_are_empty_not_error(tmp_path: Path) -> None:
    s = LocalEligibilityStore(eligibility_dir=tmp_path / "nope")
    assert s.invalid_hotkey_set() == set()
    assert s.blacklist_hotkey_set() == set()


def test_add_invalid_hotkeys_upserts_and_persists(tmp_path: Path) -> None:
    _seed(tmp_path)
    s = LocalEligibilityStore(eligibility_dir=tmp_path)
    # New hotkey + overwrite an existing one's reason/cycle.
    written = s.add_invalid_hotkeys(
        [
            {"hotkey": "hkD", "reason": "rejected:x", "cycle_id": 9},
            {"hotkey": "hkA", "reason": "selected", "cycle_id": 9},
        ]
    )
    assert written == 2
    # Re-read from disk via a fresh store to confirm persistence.
    s2 = LocalEligibilityStore(eligibility_dir=tmp_path)
    assert s2.invalid_hotkey_set() == {"hkA", "hkB", "hkC", "hkD"}
    ents = {e["hotkey"]: e for e in s2.invalid_entries()}
    assert ents["hkA"]["cycle_id"] == 9  # upserted
    assert ents["hkD"] == {"hotkey": "hkD", "reason": "rejected:x", "cycle_id": 9}
    # On-disk file is the API-mirrored shape.
    raw = json.loads((tmp_path / "invalid_hotkeys.json").read_text())
    assert "invalid_hotkeys" in raw and isinstance(raw["invalid_hotkeys"], list)
