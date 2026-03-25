from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from nexis.api.auth import AuthContext
from nexis.api.app import create_app


class _FakeDatabase:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.connected = False

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False


class _FakeRepository:
    def __init__(self, _db: _FakeDatabase):
        self._rows: dict[tuple[str, int, str], dict[str, Any]] = {}
        self._invalid_by_interval: dict[int, set[str]] = {}

    async def ensure_schema(self) -> None:
        return None

    async def register_nonce_once(
        self,
        *,
        validator_hotkey: str,
        nonce: str,
        signature_timestamp: int,
        max_age_sec: int,
    ) -> bool:
        _ = validator_hotkey
        _ = nonce
        _ = signature_timestamp
        _ = max_age_sec
        return True

    async def upsert_interval_decisions(
        self,
        *,
        validator_hotkey: str,
        interval_id: int,
        decisions: list[Any],
        signature: str,
        signature_timestamp: int,
        signature_nonce: str,
        body_sha256: str,
    ) -> int:
        for decision in decisions:
            self._rows[(validator_hotkey, int(interval_id), decision.miner_hotkey)] = {
                "interval_id": int(interval_id),
                "validator_hotkey": validator_hotkey,
                "miner_hotkey": decision.miner_hotkey,
                "accepted": bool(decision.accepted),
                "failures": list(decision.failures),
                "record_count": int(decision.record_count),
                "global_overlap_pruned_count": int(decision.global_overlap_pruned_count),
                "cross_miner_overlap_pruned_count": int(decision.cross_miner_overlap_pruned_count),
                "signature": signature,
                "timestamp": int(signature_timestamp),
                "nonce": signature_nonce,
                "body_sha256": body_sha256,
                "received_at": "2026-01-01T00:00:00+00:00",
            }
            if not bool(decision.accepted):
                self._invalid_by_interval.setdefault(int(interval_id), set()).add(
                    str(decision.miner_hotkey)
                )
        return len(decisions)

    async def upsert_interval_invalid_hotkeys(
        self,
        *,
        interval_id: int,
        invalid_hotkeys: list[str],
    ) -> None:
        values = self._invalid_by_interval.setdefault(int(interval_id), set())
        for item in invalid_hotkeys:
            hotkey = str(item).strip()
            if hotkey:
                values.add(hotkey)

    async def get_invalid_hotkeys_in_interval_range(
        self,
        *,
        start_interval_id: int,
        end_interval_id: int,
    ) -> list[str]:
        merged: set[str] = set()
        for interval, hotkeys in self._invalid_by_interval.items():
            if start_interval_id <= interval <= end_interval_id:
                merged.update(hotkeys)
        return sorted(merged)

    async def get_interval_decisions(
        self,
        *,
        validator_hotkey: str,
        interval_id: int,
    ) -> list[dict[str, Any]]:
        rows = [
            row
            for row in self._rows.values()
            if row["validator_hotkey"] == validator_hotkey and row["interval_id"] == interval_id
        ]
        rows.sort(key=lambda item: item["miner_hotkey"])
        return rows

    async def get_decisions_in_interval_range(
        self,
        *,
        start_interval_id: int,
        end_interval_id: int,
    ) -> list[dict[str, Any]]:
        rows = [
            row
            for row in self._rows.values()
            if start_interval_id <= row["interval_id"] <= end_interval_id
        ]
        rows.sort(
            key=lambda item: (
                -int(item["interval_id"]),
                str(item["validator_hotkey"]),
                str(item["miner_hotkey"]),
            )
        )
        return rows


class _FakeAllowlistSync:
    def __init__(self, **_kwargs: Any):
        self.started = False

    async def refresh_once(self) -> dict[str, float]:
        return {"vk1": 6000.0}

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False


class _FakeAuthenticator:
    def __init__(self, **_kwargs: Any):
        pass

    async def authenticate(self, request, body: bytes) -> AuthContext:  # type: ignore[no-untyped-def]
        _ = request
        _ = body
        return AuthContext(
            validator_hotkey="vk1",
            signature="sig1",
            timestamp=1700000000,
            nonce="nonce1",
            body_sha256="bodyhash1",
        )


def test_api_post_and_get_roundtrip_and_upsert(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("nexis.api.app.Database", _FakeDatabase)
    monkeypatch.setattr("nexis.api.app.ValidationEvidenceRepository", _FakeRepository)
    monkeypatch.setattr("nexis.api.app.MetagraphAllowlistSync", _FakeAllowlistSync)
    monkeypatch.setattr("nexis.api.app.RequestAuthenticator", _FakeAuthenticator)

    app = create_app()
    with TestClient(app) as client:
        create_resp = client.post(
            "/v1/validation-results",
            json={
                "interval_id": 100,
                "decisions": [
                    {
                        "miner_hotkey": "miner-a",
                        "accepted": False,
                        "failures": ["bad_caption"],
                        "record_count": 9,
                        "global_overlap_pruned_count": 2,
                        "cross_miner_overlap_pruned_count": 1,
                    }
                ],
            },
        )
        assert create_resp.status_code == 200
        assert create_resp.json()["saved"] == 1

        update_resp = client.post(
            "/v1/validation-results",
            json={
                "interval_id": 100,
                "decisions": [
                    {
                        "miner_hotkey": "miner-a",
                        "accepted": True,
                        "failures": [],
                        "record_count": 12,
                        "global_overlap_pruned_count": 0,
                        "cross_miner_overlap_pruned_count": 0,
                    }
                ],
            },
        )
        assert update_resp.status_code == 200

        query_resp = client.get(
            "/v1/validation-results",
            params={"validator_hotkey": "vk1", "interval_id": 100},
        )
        assert query_resp.status_code == 200
        payload = query_resp.json()
        assert payload["validator_hotkey"] == "vk1"
        assert len(payload["decisions"]) == 1
        assert payload["decisions"][0]["accepted"] is True
        assert payload["decisions"][0]["record_count"] == 12
        assert payload["decisions"][0]["signature"] == "sig1"


def test_get_latest_result_returns_cached_window(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    block_state = {"value": 25050}

    async def _fake_current_block(*, network: str, subtensor=None) -> int:  # type: ignore[no-untyped-def]
        _ = network
        _ = subtensor
        value = int(block_state["value"])
        block_state["value"] = value + 50
        return value

    monkeypatch.setattr("nexis.api.app.Database", _FakeDatabase)
    monkeypatch.setattr("nexis.api.app.ValidationEvidenceRepository", _FakeRepository)
    monkeypatch.setattr("nexis.api.app.MetagraphAllowlistSync", _FakeAllowlistSync)
    monkeypatch.setattr("nexis.api.app.RequestAuthenticator", _FakeAuthenticator)
    monkeypatch.setattr("nexis.api.app.fetch_current_block_async", _fake_current_block)

    app = create_app()
    with TestClient(app) as client:
        old_interval = client.post(
            "/v1/validation-results",
            json={
                "interval_id": 20,
                "decisions": [
                    {
                        "miner_hotkey": "miner-old",
                        "accepted": True,
                        "failures": [],
                        "record_count": 1,
                        "global_overlap_pruned_count": 0,
                        "cross_miner_overlap_pruned_count": 0,
                    }
                ],
            },
        )
        assert old_interval.status_code == 200

        latest_interval = client.post(
            "/v1/validation-results",
            json={
                "interval_id": 500,
                "decisions": [
                    {
                        "miner_hotkey": "miner-new",
                        "accepted": False,
                        "failures": ["bad_caption"],
                        "record_count": 7,
                        "global_overlap_pruned_count": 0,
                        "cross_miner_overlap_pruned_count": 0,
                    }
                ],
            },
        )
        assert latest_interval.status_code == 200

        result = client.get("/v1/get_latest_result")
        assert result.status_code == 200
        payload = result.json()
        assert payload["current_block"] >= 25050
        assert payload["start_interval_id"] == payload["current_block"] - 25000
        assert payload["end_interval_id"] == payload["current_block"]
        assert payload["refreshed_every_blocks"] == 50
        # Endpoint must return only cache snapshot and never refresh on request.
        assert payload["decisions"] == []


def test_get_invalid_hotkeys_window_union(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("nexis.api.app.Database", _FakeDatabase)
    monkeypatch.setattr("nexis.api.app.ValidationEvidenceRepository", _FakeRepository)
    monkeypatch.setattr("nexis.api.app.MetagraphAllowlistSync", _FakeAllowlistSync)
    monkeypatch.setattr("nexis.api.app.RequestAuthenticator", _FakeAuthenticator)

    app = create_app()
    with TestClient(app) as client:
        assert client.post(
            "/v1/validation-results",
            json={
                "interval_id": 100,
                "decisions": [
                    {
                        "miner_hotkey": "miner-a",
                        "accepted": False,
                        "failures": ["bad_caption"],
                        "record_count": 1,
                        "global_overlap_pruned_count": 0,
                        "cross_miner_overlap_pruned_count": 0,
                    }
                ],
            },
        ).status_code == 200
        assert client.post(
            "/v1/validation-results",
            json={
                "interval_id": 250,
                "decisions": [
                    {
                        "miner_hotkey": "miner-b",
                        "accepted": False,
                        "failures": ["bad_caption"],
                        "record_count": 1,
                        "global_overlap_pruned_count": 0,
                        "cross_miner_overlap_pruned_count": 0,
                    }
                ],
            },
        ).status_code == 200

        response = client.get("/v1/invalid-hotkeys", params={"interval_id": 600})
        assert response.status_code == 200
        payload = response.json()
        assert payload["window_start_interval_id"] == 100
        assert payload["window_end_interval_id"] == 600
        assert payload["invalid_hotkeys"] == ["miner-a", "miner-b"]


def test_post_invalid_hotkeys_requires_auth_and_merges(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("nexis.api.app.Database", _FakeDatabase)
    monkeypatch.setattr("nexis.api.app.ValidationEvidenceRepository", _FakeRepository)
    monkeypatch.setattr("nexis.api.app.MetagraphAllowlistSync", _FakeAllowlistSync)
    monkeypatch.setattr("nexis.api.app.RequestAuthenticator", _FakeAuthenticator)

    app = create_app()
    with TestClient(app) as client:
        create_resp = client.post(
            "/v1/invalid-hotkeys",
            json={"interval_id": 777, "invalid_hotkeys": ["miner-x", "miner-y", "miner-x"]},
        )
        assert create_resp.status_code == 200
        assert create_resp.json()["validator_hotkey"] == "vk1"
        assert create_resp.json()["saved_count"] == 2

        fetch_resp = client.get("/v1/invalid-hotkeys", params={"interval_id": 777})
        assert fetch_resp.status_code == 200
        assert fetch_resp.json()["invalid_hotkeys"] == ["miner-x", "miner-y"]

