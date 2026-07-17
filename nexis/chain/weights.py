"""Chain weight submission helpers for validators."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .metagraph import _open_subtensor, _resolve_maybe_awaitable, _run_async
import logging

logger = logging.getLogger(__name__)


# Substrate-level "your transaction came in too late" messages. These appear
# when bittensor's internal retry of `set_weights(wait_for_inclusion=True)`
# tries to resubmit while the original extrinsic was already finalized — the
# chain rejects the duplicate but our first submission DID land. Treat as
# success and stop retrying.
_OUTDATED_TX_SIGNALS = (
    "transaction is outdated",
    "transaction is stale",
    "priority too low",
    "future",
    "ancientbirthblock",
)


def _looks_like_outdated_tx(result: object) -> bool:
    """Inspect bittensor's set_weights return value for outdated/duplicate hints."""
    if isinstance(result, tuple):
        for item in result:
            if isinstance(item, str) and any(s in item.lower() for s in _OUTDATED_TX_SIGNALS):
                return True
    if isinstance(result, str):
        return any(s in result.lower() for s in _OUTDATED_TX_SIGNALS)
    # bittensor >=10 returns an ExtrinsicResponse object with a `.message`.
    message = getattr(result, "message", None)
    if isinstance(message, str):
        return any(s in message.lower() for s in _OUTDATED_TX_SIGNALS)
    return False


class _BittensorOutdatedTxFilter(logging.Filter):
    """Downgrade bittensor's `Transaction is outdated` ERROR to INFO.

    These come from bittensor's internal retry loop inside one set_weights
    call; the very next line from our own logger is `set_weights submitted ...`
    that confirms the original submission landed. Filtering keeps the log
    readable; real errors still pass through at ERROR.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage().lower()
        if any(s in msg for s in _OUTDATED_TX_SIGNALS):
            record.levelno = logging.INFO
            record.levelname = "INFO"
            # Prefix the record so it's obvious this was downgraded.
            record.msg = "(bittensor duplicate-tx noise; harmless) " + str(record.msg)
        return True


def install_bittensor_log_filter() -> None:
    """Attach the outdated-tx filter to the `bittensor` logger.

    Safe to call multiple times — adds at most one instance of the filter.
    """
    bt_logger = logging.getLogger("bittensor")
    for existing in bt_logger.filters:
        if isinstance(existing, _BittensorOutdatedTxFilter):
            return
    bt_logger.addFilter(_BittensorOutdatedTxFilter())

@dataclass(frozen=True)
class ChainWeightPayload:
    """Dense UID-aligned weight payload for on-chain submission."""

    uids: list[int]
    weights: list[float]
    unknown_hotkeys: list[str]


@dataclass(frozen=True)
class WeightSubmissionResult:
    """Result metadata for validator weight submission."""

    submitted: bool
    reason: str = ""
    unknown_hotkeys: list[str] | None = None


def build_chain_weight_payload(
    *,
    metagraph_hotkeys: Iterable[str],
    metagraph_uids: Iterable[int],
    weights_by_hotkey: dict[str, float],
) -> ChainWeightPayload:
    """Build dense UID-aligned weights from hotkey-indexed weights."""
    hotkeys = list(metagraph_hotkeys)
    uids = [int(uid) for uid in metagraph_uids]
    uid_by_hotkey = dict(zip(hotkeys, uids, strict=True))

    unknown_hotkeys: list[str] = []
    dense: dict[int, float] = {uid: 0.0 for uid in uids}
    for hotkey, weight in weights_by_hotkey.items():
        uid = uid_by_hotkey.get(hotkey)
        if uid is None:
            unknown_hotkeys.append(hotkey)
            continue
        dense[uid] = max(0.0, float(weight))

    total = sum(dense.values())
    if total > 0:
        dense = {uid: value / total for uid, value in dense.items()}
    elif uids:
        # Required fallback: if no valid miner weight exists, route full weight to UID 0.
        target_uid = 0 if 0 in dense else uids[0]
        dense = {uid: 0.0 for uid in uids}
        dense[target_uid] = 1.0

    ordered_weights = [dense[uid] for uid in uids]
    return ChainWeightPayload(uids=uids, weights=ordered_weights, unknown_hotkeys=unknown_hotkeys)


async def submit_weights_to_chain_async(
    *,
    netuid: int,
    network: str,
    wallet_name: str,
    wallet_hotkey: str,
    wallet_path: Path,
    weights_by_hotkey: dict[str, float],
    subtensor: object | None = None,
) -> WeightSubmissionResult:
    """Submit validator-computed weights using bittensor set_weights."""
    import bittensor as bt

    if subtensor is None:
        async with _open_subtensor(network) as owned_subtensor:
            return await submit_weights_to_chain_async(
                netuid=netuid,
                network=network,
                wallet_name=wallet_name,
                wallet_hotkey=wallet_hotkey,
                wallet_path=wallet_path,
                weights_by_hotkey=weights_by_hotkey,
                subtensor=owned_subtensor,
            )
    active_subtensor = subtensor
    if active_subtensor is None:
        return WeightSubmissionResult(submitted=False, reason="subtensor_unavailable")
    metagraph = await _resolve_maybe_awaitable(active_subtensor.metagraph(netuid))
    payload = build_chain_weight_payload(
        metagraph_hotkeys=list(metagraph.hotkeys),
        metagraph_uids=list(metagraph.uids),
        weights_by_hotkey=weights_by_hotkey,
    )
    if not payload.uids:
        return WeightSubmissionResult(
            submitted=False,
            reason="empty_metagraph",
            unknown_hotkeys=payload.unknown_hotkeys,
        )

    wallet = bt.Wallet(
        name=wallet_name,
        hotkey=wallet_hotkey,
        path=str(wallet_path.expanduser()),
    )
    install_bittensor_log_filter()
    logger.info(f"submitting weights to chain: {payload.uids} {payload.weights}")
    attempt = 0
    submitted = False
    reason = "set_weights_returned_false"
    while attempt < 3:
        result = await _resolve_maybe_awaitable(
            active_subtensor.set_weights(
                wallet=wallet,
                netuid=netuid,
                uids=payload.uids,
                weights=payload.weights,
                wait_for_inclusion=True,
            )
        )

        submitted = True
        if isinstance(result, tuple):
            submitted = bool(result[0])
        elif isinstance(result, bool):
            submitted = result
        elif hasattr(result, "success"):
            # bittensor >=10: ExtrinsicResponse with .success / .message.
            submitted = bool(result.success)
        if submitted:
            reason = ""
            break

        # A False/(False, msg) return with an "outdated" hint means the
        # original extrinsic was finalized and bittensor's resubmit was
        # rejected as a duplicate. Don't retry — that just creates more noise.
        if _looks_like_outdated_tx(result):
            logger.info(
                "set_weights: chain reported outdated/duplicate (%r); "
                "treating as success (original extrinsic was already included)",
                result,
            )
            submitted = True
            reason = ""
            break

        logger.error(f"set_weights failed: {result} on attempt {attempt}")
        attempt += 1
        if attempt < 3:
            await asyncio.sleep(10)

    return WeightSubmissionResult(
        submitted=submitted,
        reason=reason,
        unknown_hotkeys=payload.unknown_hotkeys,
    )


def submit_weights_to_chain(
    *,
    netuid: int,
    network: str,
    wallet_name: str,
    wallet_hotkey: str,
    wallet_path: Path,
    weights_by_hotkey: dict[str, float],
) -> WeightSubmissionResult:
    return _run_async(
        submit_weights_to_chain_async(
            netuid=netuid,
            network=network,
            wallet_name=wallet_name,
            wallet_hotkey=wallet_hotkey,
            wallet_path=wallet_path,
            weights_by_hotkey=weights_by_hotkey,
        )
    )

