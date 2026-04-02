from __future__ import annotations

from pathlib import Path

from nexis.chain.credentials import ReadCredentialCommitmentManager
from nexis.storage.r2 import R2Credentials, build_r2_endpoint_url
from .helpers import patch_bittensor_wallet, run_async


def _manager() -> ReadCredentialCommitmentManager:
    return ReadCredentialCommitmentManager(
        netuid=1,
        network="finney",
        wallet_name="w",
        wallet_hotkey="h",
        wallet_path=Path("~/.bittensor/wallets"),
        r2_region="auto",
    )


def test_payload_v3_roundtrip_includes_r2_account_and_keys() -> None:
    manager = _manager()
    payload = manager._encode_payload(  # type: ignore[attr-defined]
        account_id="a" * 32,
        read_access_key="k" * 32,
        read_secret_key="s" * 64,
    )
    decoded = manager._decode_payload(payload)  # type: ignore[attr-defined]
    assert decoded is not None
    assert len(payload) == 128
    assert decoded["account_id"] == "a" * 32
    assert decoded["read_access_key"] == "k" * 32
    assert decoded["read_secret_key"] == "s" * 64


def test_invalid_payload_decode_returns_none() -> None:
    manager = _manager()
    decoded = manager._decode_payload("too-short")  # type: ignore[attr-defined]
    assert decoded is None


def test_build_credentials_derives_bucket_from_hotkey() -> None:
    manager = _manager()
    hotkey = "5AbCdEf"
    committed = {
        "account_id": "f" * 32,
        "read_access_key": "k" * 32,
        "read_secret_key": "s" * 64,
    }
    creds = manager.build_r2_credentials(committed, hotkey=hotkey)
    assert creds is not None
    assert creds.bucket_name == hotkey.lower()
    assert creds.write_access_key == committed["read_access_key"]
    assert creds.write_secret_key == committed["read_secret_key"]
    assert creds.endpoint_url == build_r2_endpoint_url(committed["account_id"])


def test_build_credentials_requires_account_id() -> None:
    manager = _manager()
    committed = {
        "account_id": "",
        "read_access_key": "k" * 32,
        "read_secret_key": "s" * 64,
    }
    assert manager.build_r2_credentials(committed, hotkey="hk1") is None


def test_get_all_credentials_async_uses_provided_subtensor() -> None:
    manager = _manager()
    payload = manager._encode_payload(  # type: ignore[attr-defined]
        account_id="a" * 32,
        read_access_key="k" * 32,
        read_secret_key="s" * 64,
    )

    manager._decode_hotkey = lambda key: str(key[0])  # type: ignore[method-assign]
    manager._extract_commitment_string = lambda value: str(value)  # type: ignore[method-assign]

    class FakeSubstrate:
        async def query_map(self, **kwargs) -> list[tuple[tuple[str], str]]:  # type: ignore[no-untyped-def]
            assert kwargs["module"] == "Commitments"
            assert kwargs["storage_function"] == "CommitmentOf"
            return [(("hk1",), payload)]

    class FakeSubtensor:
        substrate = FakeSubstrate()

    committed = run_async(manager.get_all_credentials_async(subtensor=FakeSubtensor()))
    assert "hk1" in committed
    assert committed["hk1"]["account_id"] == "a" * 32
    assert committed["hk1"]["read_access_key"] == "k" * 32
    assert committed["hk1"]["read_secret_key"] == "s" * 64


def test_commit_read_credentials_async_uses_provided_subtensor(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    manager = _manager()
    wallet_calls = patch_bittensor_wallet(monkeypatch)
    commit_calls: list[tuple[object, int, str]] = []

    class FakeSubtensor:
        async def commit(self, wallet: object, netuid: int, payload: str) -> None:
            commit_calls.append((wallet, netuid, payload))

    creds = R2Credentials(
        account_id="f" * 32,
        bucket_name="hk-test",
        region="auto",
        read_access_key="k" * 32,
        read_secret_key="s" * 64,
        write_access_key="w" * 32,
        write_secret_key="x" * 64,
    )

    result = run_async(
        manager.commit_read_credentials_async(
            "hk-test",
            creds,
            subtensor=FakeSubtensor(),
        )
    )

    assert result == creds.read_commitment
    assert len(wallet_calls) == 1
    assert len(commit_calls) == 1
    assert commit_calls[0][1] == manager.netuid
