from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any


class LocalObjectStore:
    """Filesystem-backed object store for tests."""

    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()

    async def upload_file(self, key: str, src: Path, use_write: bool = True) -> None:
        _ = use_write
        dst = self.root / key
        dst.parent.mkdir(parents=True, exist_ok=True)
        async with self._lock:
            dst.write_bytes(src.read_bytes())

    async def download_file(self, key: str, dst: Path) -> bool:
        src = self.root / key
        if not src.exists():
            return False
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(src.read_bytes())
        return True

    async def object_exists(self, key: str) -> bool:
        return (self.root / key).exists()

    async def list_prefix(self, prefix: str) -> list[str]:
        entries: list[str] = []
        for path in self.root.rglob("*"):
            if path.is_file():
                rel = str(path.relative_to(self.root))
                if rel.startswith(prefix):
                    entries.append(rel)
        return sorted(entries)

    async def get_object_last_modified(self, key: str) -> datetime | None:
        path = self.root / key
        if not path.exists():
            return None
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


def run_async(coro: Any) -> Any:
    """Execute a coroutine in tests."""
    return asyncio.run(coro)


def patch_bittensor_wallet(monkeypatch: Any) -> list[dict[str, str]]:
    """Patch bittensor.Wallet and return captured wallet call args."""
    calls: list[dict[str, str]] = []

    def fake_wallet(*, name: str, hotkey: str, path: str) -> object:
        calls.append({"name": name, "hotkey": hotkey, "path": path})
        return {"wallet": "ok"}

    monkeypatch.setitem(sys.modules, "bittensor", SimpleNamespace(Wallet=fake_wallet))
    return calls


def patch_bittensor_async_subtensor(monkeypatch: Any, factory: Any) -> None:
    """Patch bittensor.AsyncSubtensor factory for tests."""
    monkeypatch.setitem(sys.modules, "bittensor", SimpleNamespace(AsyncSubtensor=factory))


class FakeMetagraphSubtensor:
    """Minimal async subtensor fake for metagraph tests."""

    def __init__(
        self,
        *,
        hotkeys: list[Any] | None = None,
        block: int = 0,
        expected_netuid: int | None = None,
    ):
        self._hotkeys = list(hotkeys or [])
        self._block = int(block)
        self._expected_netuid = expected_netuid

    async def metagraph(self, netuid: int) -> object:
        if self._expected_netuid is not None:
            assert netuid == self._expected_netuid
        return SimpleNamespace(hotkeys=self._hotkeys)

    async def get_current_block(self) -> int:
        return self._block


class FakeWeightSubtensor:
    """Minimal async subtensor fake for chain weight tests."""

    def __init__(
        self,
        *,
        hotkeys: list[str],
        uids: list[int],
        expected_netuid: int | None = None,
        set_weight_results: list[object] | None = None,
    ):
        self._hotkeys = hotkeys
        self._uids = uids
        self._expected_netuid = expected_netuid
        self._set_weight_results = list(set_weight_results or [True])
        self.set_weight_calls: list[dict[str, object]] = []

    async def metagraph(self, netuid: int) -> object:
        if self._expected_netuid is not None:
            assert netuid == self._expected_netuid
        return SimpleNamespace(hotkeys=self._hotkeys, uids=self._uids)

    async def set_weights(self, **kwargs) -> object:  # type: ignore[no-untyped-def]
        self.set_weight_calls.append(kwargs)
        if self._set_weight_results:
            return self._set_weight_results.pop(0)
        return True

