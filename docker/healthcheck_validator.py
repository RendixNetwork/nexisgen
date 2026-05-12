"""Container healthcheck for the nexis runtime.

Returns 0 when PID 1 is `nexis <subcommand>` (validate, mine, train, etc.).
"""

from __future__ import annotations

from pathlib import Path
import sys

_KNOWN_SUBCOMMANDS = {"validate", "mine", "train", "commit-credentials"}


def main() -> int:
    try:
        raw = Path("/proc/1/cmdline").read_bytes()
    except OSError:
        return 1

    parts = [p for p in raw.replace(b"\x00", b" ").decode("utf-8", errors="ignore").split() if p]
    if not parts:
        return 1
    if not any("nexis" in part.lower() for part in parts):
        return 1
    if not any(sub in parts for sub in _KNOWN_SUBCOMMANDS):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
