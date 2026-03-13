"""
Polygon RPC on-chain position monitoring (Method B — backup detection).

Subscribes to Transfer events on the Polymarket CTF contract to detect
whale trades faster than the Data API (within 1-2 blocks ~4s on Polygon).

Contracts:
  CTF: 0x4D97DCd97eC945f40cF65F87097ACe5EA0476045 (Conditional Token Framework)
  USDC: 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174 (USDC on Polygon)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

# ERC1155 TransferSingle event signature
TRANSFER_SINGLE_TOPIC = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"


class PolygonMonitor:
    """
    On-chain whale trade monitor using Polygon RPC.

    Uses web3.py to subscribe to Transfer events on the CTF contract,
    filtering for whale wallet addresses.
    """

    def __init__(self, rpc_url: str, whale_addresses: set[str]):
        self.rpc_url = rpc_url
        self.whale_addresses = {addr.lower() for addr in whale_addresses}
        self._w3 = None
        self._running = False
        self._callback: Optional[Callable] = None

    async def start(self, callback: Callable) -> None:
        """
        Start monitoring Transfer events.

        Args:
            callback: async function called with (wallet, token_id, amount, tx_hash)
        """
        self._callback = callback

        try:
            from web3 import Web3
            from web3.middleware import ExtraDataToPOAMiddleware

            self._w3 = Web3(Web3.HTTPProvider(self.rpc_url))
            self._w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

            if not self._w3.is_connected():
                logger.error("Failed to connect to Polygon RPC: %s", self.rpc_url)
                return

            self._running = True
            logger.info(
                "PolygonMonitor started — watching %d addresses on CTF %s",
                len(self.whale_addresses),
                CTF_ADDRESS[:10],
            )

        except ImportError:
            logger.warning("web3 not installed — on-chain monitoring disabled")
            return

    async def poll_blocks(self) -> None:
        """Poll recent blocks for whale Transfer events."""
        if not self._w3 or not self._running:
            return

        last_block = self._w3.eth.block_number

        while self._running:
            try:
                current_block = self._w3.eth.block_number
                if current_block <= last_block:
                    await asyncio.sleep(2)
                    continue

                # Scan new blocks for TransferSingle events on CTF
                ctf_contract = self._w3.to_checksum_address(CTF_ADDRESS)
                logs = self._w3.eth.get_logs({
                    "fromBlock": last_block + 1,
                    "toBlock": current_block,
                    "address": ctf_contract,
                    "topics": [TRANSFER_SINGLE_TOPIC],
                })

                for log in logs:
                    await self._process_log(log)

                last_block = current_block
                await asyncio.sleep(2)  # ~1 block on Polygon

            except Exception as e:
                logger.error("Block poll error: %s", e)
                await asyncio.sleep(5)

    async def _process_log(self, log: Any) -> None:
        """Process a TransferSingle event log."""
        try:
            # Decode topics: [event_sig, operator, from, to]
            topics = log.get("topics", [])
            if len(topics) < 4:
                return

            from_addr = "0x" + topics[2].hex()[-40:]
            to_addr = "0x" + topics[3].hex()[-40:]

            # Check if either address is a whale
            from_is_whale = from_addr.lower() in self.whale_addresses
            to_is_whale = to_addr.lower() in self.whale_addresses

            if not from_is_whale and not to_is_whale:
                return

            # Decode data: (token_id, amount)
            data = log.get("data", b"")
            if isinstance(data, str):
                data = bytes.fromhex(data[2:])

            if len(data) >= 64:
                token_id = int.from_bytes(data[:32], "big")
                amount = int.from_bytes(data[32:64], "big")
            else:
                return

            tx_hash = log.get("transactionHash", b"").hex()

            whale_addr = to_addr if to_is_whale else from_addr
            is_buy = to_is_whale

            logger.info(
                "ON-CHAIN: %s %s token=%d amount=%d tx=%s",
                "BUY" if is_buy else "SELL",
                whale_addr[:10],
                token_id,
                amount,
                tx_hash[:12],
            )

            if self._callback:
                await self._callback(whale_addr, str(token_id), amount, tx_hash)

        except Exception as e:
            logger.debug("Log processing error: %s", e)

    async def stop(self) -> None:
        self._running = False
        logger.info("PolygonMonitor stopped")
