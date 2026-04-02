"""Read credential commitment manager backed by on-chain commitments."""

from __future__ import annotations

from collections.abc import AsyncIterator
import logging
from pathlib import Path
from typing import Any

from .metagraph import _open_subtensor, _resolve_maybe_awaitable, _run_async
from ..storage.r2 import R2Credentials, bucket_name_for_hotkey, is_valid_r2_account_id

logger = logging.getLogger(__name__)

_R2_ACCOUNT_ID_LEN = 32
_R2_READ_ACCESS_KEY_LEN = 32
_R2_READ_SECRET_KEY_LEN = 64
_R2_COMMITMENT_PAYLOAD_LEN = _R2_ACCOUNT_ID_LEN + _R2_READ_ACCESS_KEY_LEN + _R2_READ_SECRET_KEY_LEN


class ReadCredentialCommitmentManager:
    """Commits and fetches read credentials through chain commitments."""

    def __init__(
        self,
        *,
        netuid: int,
        network: str,
        wallet_name: str,
        wallet_hotkey: str,
        wallet_path: Path,
        r2_region: str,
    ):
        self.netuid = netuid
        self.network = network
        self.wallet_name = wallet_name
        self.wallet_hotkey = wallet_hotkey
        self.wallet_path = wallet_path
        self.r2_region = r2_region

    def commit_read_credentials(self, hotkey: str, credentials: R2Credentials) -> str:
        return _run_async(self.commit_read_credentials_async(hotkey, credentials))

    async def commit_read_credentials_async(
        self,
        hotkey: str,
        credentials: R2Credentials,
        subtensor: Any | None = None,
    ) -> str:
        import bittensor as bt

        credentials.validate_account_id()
        credentials.validate_read_key_lengths()
        credentials.validate_bucket_for_hotkey(hotkey)
        commitment_payload = self._encode_payload(
            account_id=credentials.account_id,
            read_access_key=credentials.read_access_key,
            read_secret_key=credentials.read_secret_key,
        )
        if len(commitment_payload) != _R2_COMMITMENT_PAYLOAD_LEN:
            raise ValueError(
                f"invalid commitment payload length ({len(commitment_payload)}), "
                f"expected {_R2_COMMITMENT_PAYLOAD_LEN}"
            )

        if subtensor is None:
            async with _open_subtensor(self.network) as owned_subtensor:
                return await self.commit_read_credentials_async(
                    hotkey=hotkey,
                    credentials=credentials,
                    subtensor=owned_subtensor,
                )
        wallet = bt.wallet(
            name=self.wallet_name,
            hotkey=self.wallet_hotkey,
            path=str(self.wallet_path.expanduser()),
        )
        await _resolve_maybe_awaitable(subtensor.commit(wallet, self.netuid, commitment_payload))
        logger.info("committed read credentials on-chain for hotkey=%s", hotkey)
        return credentials.read_commitment

    def get_credentials_for_hotkey(self, hotkey: str) -> dict | None:
        payload = self.get_all_credentials()
        return payload.get(hotkey)

    def get_all_credentials(self) -> dict[str, dict]:
        return _run_async(self.get_all_credentials_async())

    async def get_all_credentials_async(self, subtensor: Any | None = None) -> dict[str, dict]:
        commitments: dict[str, dict] = {}
        try:
            if subtensor is None:
                async with _open_subtensor(self.network) as owned_subtensor:
                    return await self.get_all_credentials_async(subtensor=owned_subtensor)
            substrate = getattr(subtensor, "substrate", None)
            if substrate is None:
                return commitments
            query_result = await _resolve_maybe_awaitable(
                substrate.query_map(
                    module="Commitments",
                    storage_function="CommitmentOf",
                    params=[self.netuid],
                    block_hash=None,
                )
            )
            if query_result is None:
                return commitments

            async for key, value in self._iter_query_entries(query_result):
                hotkey = self._decode_hotkey(key)
                if not hotkey:
                    continue
                commitment_str = self._extract_commitment_string(value)
                if not commitment_str:
                    continue
                decoded = self._decode_payload(commitment_str)
                if decoded is None:
                    continue
                commitments[hotkey] = {
                    "account_id": decoded["account_id"],
                    "read_access_key": decoded["read_access_key"],
                    "read_secret_key": decoded["read_secret_key"],
                    "commitment": commitment_str,
                }
        except Exception as exc:
            logger.warning("failed to fetch chain commitments: %s", exc)
        return commitments

    def build_r2_credentials(
        self,
        committed: dict | None,
        *,
        hotkey: str,
    ) -> R2Credentials | None:
        if committed is None:
            return None
        account_id = str(committed.get("account_id", "")).strip()
        read_access_key = str(committed.get("read_access_key", "")).strip()
        read_secret_key = str(committed.get("read_secret_key", "")).strip()
        if not account_id or not read_access_key or not read_secret_key:
            return None
        return R2Credentials(
            account_id=account_id,
            bucket_name=bucket_name_for_hotkey(hotkey),
            region=self.r2_region,
            read_access_key=read_access_key,
            read_secret_key=read_secret_key,
            write_access_key=read_access_key,
            write_secret_key=read_secret_key,
        )

    def _encode_payload(self, *, account_id: str, read_access_key: str, read_secret_key: str) -> str:
        return f"{account_id}{read_access_key}{read_secret_key}"

    def _decode_payload(self, payload: str) -> dict[str, str] | None:
        if len(payload) != _R2_COMMITMENT_PAYLOAD_LEN:
            return None
        account_id = payload[:_R2_ACCOUNT_ID_LEN]
        read_access_key = payload[_R2_ACCOUNT_ID_LEN : _R2_ACCOUNT_ID_LEN + _R2_READ_ACCESS_KEY_LEN]
        read_secret_key = payload[-_R2_READ_SECRET_KEY_LEN:]
        if not account_id or not read_access_key or not read_secret_key:
            return None
        if not is_valid_r2_account_id(account_id):
            return None
        if any(ch.isspace() for ch in read_access_key) or any(ch.isspace() for ch in read_secret_key):
            return None
        return {
            "account_id": account_id.lower(),
            "read_access_key": read_access_key,
            "read_secret_key": read_secret_key,
        }

    def _decode_hotkey(self, key: Any) -> str | None:
        try:
            if isinstance(key, (list, tuple)) and key:
                from bittensor.core.chain_data import decode_account_id

                return decode_account_id(key[0])
        except Exception:
            return None
        return None

    def _extract_commitment_string(self, value: Any) -> str | None:
        try:
            payload = getattr(value, "value", value)
            fields = payload.get("info", {}).get("fields", [])
            if not fields:
                return None
            encoded_map = fields[0][0]
            if not isinstance(encoded_map, dict) or not encoded_map:
                return None
            encoded_value = encoded_map[next(iter(encoded_map.keys()))]
            if isinstance(encoded_value, (list, tuple)) and encoded_value:
                first = encoded_value[0]
                if isinstance(first, (list, tuple)):
                    return bytes(first).decode("utf-8")
                if isinstance(first, (bytes, bytearray)):
                    return bytes(first).decode("utf-8")
            if isinstance(encoded_value, str):
                return encoded_value
        except Exception:
            return None
        return None

    async def _iter_query_entries(self, query_result: Any) -> AsyncIterator[tuple[Any, Any]]:
        if hasattr(query_result, "__aiter__"):
            async for key, value in query_result:
                yield key, value
            return
        for key, value in query_result:
            yield key, value

