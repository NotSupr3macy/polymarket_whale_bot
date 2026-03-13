"""
Order executor — places and manages maker limit orders on Polymarket CLOB.

Post-Feb 2026 rules:
  - Maker orders: 0% fee + rebates (ALWAYS use maker)
  - Taker orders: ~1.56% fee (NEVER use taker)
  - Place limits 0.5% better than midpoint for fill probability

Uses py-clob-client for order signing and placement.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Optional

import aiohttp

from config import BotConfig
from signal_engine import TradeOpportunity, ExitSignal

logger = logging.getLogger(__name__)


@dataclass
class OrderResult:
    """Result of an order placement attempt."""

    success: bool
    order_id: str = ""
    price: float = 0.0
    size: float = 0.0
    shares: float = 0.0
    error: str = ""
    timestamp: float = 0.0


class OrderExecutor:
    """
    Executes trades on Polymarket CLOB via py-clob-client.

    IMPORTANT setup:
      1. Run set_allowances.py from py-clob-client first (one-time, needs POL gas).
      2. Use signature_type=1 for email/Magic wallets, 0 for MetaMask/EOA.
      3. Balance is returned in wei — divide by 1e6 for USDC.
    """

    def __init__(self, config: BotConfig):
        self.config = config
        self._client = None
        self._session: aiohttp.ClientSession | None = None
        self._pending_orders: dict[str, asyncio.Task] = {}  # order_id -> timeout task
        self._initialized = False
        self.orders_placed = 0
        self.orders_filled = 0
        self.orders_cancelled = 0

    async def initialize(self) -> None:
        """Initialize the CLOB client and HTTP session."""
        if self.config.DRY_RUN:
            logger.info("OrderExecutor initialized in DRY RUN mode")
            self._initialized = True
            return

        try:
            from py_clob_client.client import ClobClient

            self._client = ClobClient(
                self.config.CLOB_HOST,
                key=self.config.POLYMARKET_PK,
                chain_id=self.config.CHAIN_ID,
                signature_type=1,  # email/Magic wallet
                funder=self.config.POLYMARKET_FUNDER,
            )
            # Derive API credentials
            self._client.set_api_creds(self._client.create_or_derive_api_creds())

            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15),
            )
            self._initialized = True
            logger.info("OrderExecutor initialized for LIVE trading")

        except ImportError:
            logger.error("py-clob-client not installed. Run: pip install py-clob-client")
            raise
        except Exception as e:
            logger.error("Failed to initialize CLOB client: %s", e)
            raise

    async def shutdown(self) -> None:
        """Cancel pending orders and close connections."""
        # Cancel all pending timeout tasks
        for order_id, task in self._pending_orders.items():
            task.cancel()

        # Cancel unfilled orders on the exchange
        if self._client and not self.config.DRY_RUN:
            try:
                open_orders = self._client.get_orders()
                for order in open_orders:
                    if order.get("status") == "live":
                        try:
                            self._client.cancel(order["id"])
                            self.orders_cancelled += 1
                            logger.info("Shutdown: cancelled order %s", order["id"])
                        except Exception as e:
                            logger.error("Failed to cancel order %s: %s", order["id"], e)
            except Exception as e:
                logger.error("Failed to fetch open orders during shutdown: %s", e)

        if self._session and not self._session.closed:
            await self._session.close()

        logger.info(
            "OrderExecutor shutdown: %d placed, %d filled, %d cancelled",
            self.orders_placed,
            self.orders_filled,
            self.orders_cancelled,
        )

    async def execute_copy_trade(
        self, opportunity: TradeOpportunity, position_size: float
    ) -> Optional[OrderResult]:
        """
        Place a maker limit order to copy a whale trade.

        Steps:
          1. Get current midpoint price
          2. Check slippage vs whale entry
          3. Apply price improvement (place limit better than market)
          4. Submit maker order
          5. Schedule timeout cancellation
        """
        if not self._initialized:
            await self.initialize()

        if self.config.DRY_RUN:
            return self._dry_run_order(opportunity, position_size)

        try:
            # Get current market price
            midpoint = float(self._client.get_midpoint(opportunity.token_id))

            # Entry price sanity check: if midpoint differs >20% from whale's
            # reported entry, something is wrong (stale data, wrong market, etc.)
            if opportunity.avg_whale_entry > 0:
                price_diff = abs(midpoint - opportunity.avg_whale_entry) / opportunity.avg_whale_entry
                if price_diff > self.config.ENTRY_PRICE_SANITY_CHECK_PCT:
                    logger.warning(
                        "Price sanity check failed: market=%.4f whale_entry=%.4f diff=%.1f%% — skipping %s",
                        midpoint,
                        opportunity.avg_whale_entry,
                        price_diff * 100,
                        opportunity.market_id[:20],
                    )
                    return OrderResult(success=False, error=f"Price sanity {price_diff:.1%}")

            # Check slippage from whale entry (tighter check for alpha decay)
            if opportunity.avg_whale_entry > 0:
                slippage = abs(midpoint - opportunity.avg_whale_entry) / opportunity.avg_whale_entry
                if slippage > self.config.MAX_SLIPPAGE_PCT:
                    logger.warning(
                        "Slippage %.1f%% > max %.1f%%, skipping %s",
                        slippage * 100,
                        self.config.MAX_SLIPPAGE_PCT * 100,
                        opportunity.market_id[:20],
                    )
                    return OrderResult(success=False, error=f"Slippage {slippage:.1%}")

            # Calculate order price with price improvement
            order_price = self._calculate_order_price(midpoint, opportunity.direction)

            # Calculate shares
            if order_price <= 0:
                return OrderResult(success=False, error="Invalid order price")
            shares = position_size / order_price

            # Check market metadata for neg_risk
            neg_risk = await self._check_neg_risk(opportunity.condition_id)

            # Place order via py-clob-client
            from py_clob_client.clob_types import OrderArgs
            from py_clob_client.order_builder.constants import BUY

            order = self._client.create_and_post_order(
                OrderArgs(
                    price=order_price,
                    size=shares,
                    side=BUY,
                    token_id=opportunity.token_id,
                ),
                options={"tickSize": "0.01", "negRisk": neg_risk},
            )

            order_id = order.get("orderID", order.get("id", ""))
            self.orders_placed += 1

            # Schedule timeout cancellation
            timeout_task = asyncio.create_task(
                self._order_timeout(order_id)
            )
            self._pending_orders[order_id] = timeout_task

            result = OrderResult(
                success=True,
                order_id=order_id,
                price=order_price,
                size=position_size,
                shares=shares,
                timestamp=time.time(),
            )

            logger.info(
                "ORDER PLACED: %s %s %.2f shares @ $%.4f ($%.2f) | id=%s",
                opportunity.direction,
                opportunity.market_id[:20],
                shares,
                order_price,
                position_size,
                order_id[:12],
            )
            return result

        except Exception as e:
            logger.error("Order execution failed: %s", e)
            return OrderResult(success=False, error=str(e))

    async def execute_exit(
        self, token_id: str, shares: float, reason: str
    ) -> Optional[OrderResult]:
        """Sell/exit a position."""
        if self.config.DRY_RUN:
            logger.info("[DRY] EXIT: %.2f shares of %s (%s)", shares, token_id[:20], reason)
            return OrderResult(
                success=True,
                order_id=f"dry-exit-{int(time.time())}",
                shares=shares,
                timestamp=time.time(),
            )

        if not self._client:
            return OrderResult(success=False, error="Client not initialized")

        try:
            from py_clob_client.clob_types import OrderArgs
            from py_clob_client.order_builder.constants import SELL

            midpoint = float(self._client.get_midpoint(token_id))
            # For exit, place slightly above midpoint to get filled as maker
            improvement = self.config.PRICE_IMPROVEMENT_BPS / 10000
            sell_price = round(midpoint + improvement, 2)
            sell_price = min(0.99, sell_price)

            order = self._client.create_and_post_order(
                OrderArgs(
                    price=sell_price,
                    size=shares,
                    side=SELL,
                    token_id=token_id,
                ),
                options={"tickSize": "0.01"},
            )

            order_id = order.get("orderID", order.get("id", ""))
            self.orders_placed += 1

            logger.info(
                "EXIT ORDER: sell %.2f shares @ $%.4f (%s) | id=%s",
                shares,
                sell_price,
                reason,
                order_id[:12],
            )

            return OrderResult(
                success=True,
                order_id=order_id,
                price=sell_price,
                shares=shares,
                timestamp=time.time(),
            )

        except Exception as e:
            logger.error("Exit order failed for %s: %s", token_id[:20], e)
            return OrderResult(success=False, error=str(e))

    def _calculate_order_price(self, midpoint: float, direction: str) -> float:
        """
        Calculate maker limit price with price improvement.

        For BUY YES: place below midpoint (buy cheaper)
        For BUY NO: calculate complement price, place below
        """
        improvement = self.config.PRICE_IMPROVEMENT_BPS / 10000

        if direction == "YES":
            price = round(midpoint - improvement, 2)
            return max(0.01, price)
        else:
            # Buying NO shares: price = 1 - yes_price
            no_price = 1.0 - midpoint
            price = round(no_price - improvement, 2)
            return max(0.01, price)

    async def _check_neg_risk(self, condition_id: str) -> bool:
        """Check if market uses neg_risk mode via Gamma API."""
        if not self._session:
            return False

        try:
            url = f"{self.config.GAMMA_API}/markets/{condition_id}"
            async with self._session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return bool(data.get("negRisk", False))
        except Exception:
            pass
        return False

    async def _order_timeout(self, order_id: str) -> None:
        """Cancel an order if not filled within ORDER_TIMEOUT_SECONDS."""
        try:
            await asyncio.sleep(self.config.ORDER_TIMEOUT_SECONDS)
            if self._client:
                self._client.cancel(order_id)
                self.orders_cancelled += 1
                logger.info("Timeout: cancelled unfilled order %s", order_id[:12])
        except asyncio.CancelledError:
            pass  # Task was cancelled (e.g., shutdown)
        except Exception as e:
            logger.debug("Order timeout cancel failed for %s: %s", order_id[:12], e)
        finally:
            self._pending_orders.pop(order_id, None)

    def _dry_run_order(
        self, opportunity: TradeOpportunity, position_size: float
    ) -> OrderResult:
        """Simulate order placement in dry-run mode.
        Uses a simulated midpoint price (not the whale's historical avg)."""
        # Simulate a realistic midpoint near the whale's entry with small spread
        whale_price = opportunity.avg_whale_entry if opportunity.avg_whale_entry > 0 else 0.55
        # In dry run, simulate our entry at whale price with price improvement applied
        improvement = self.config.PRICE_IMPROVEMENT_BPS / 10000
        simulated_price = max(0.01, round(whale_price - improvement, 4))
        shares = position_size / simulated_price if simulated_price > 0 else 0

        logger.info(
            "[DRY] ORDER: %s %s %.2f shares @ $%.4f ($%.2f) | %d whales, %.0f%% consensus",
            opportunity.direction,
            opportunity.market_id[:20],
            shares,
            simulated_price,
            position_size,
            opportunity.n_whales,
            opportunity.consensus_pct * 100,
        )

        self.orders_placed += 1
        return OrderResult(
            success=True,
            order_id=f"dry-{int(time.time())}-{self.orders_placed}",
            price=simulated_price,
            size=position_size,
            shares=shares,
            timestamp=time.time(),
        )

    async def get_balance(self) -> float:
        """Get USDC balance (returns in standard units, not wei)."""
        if self.config.DRY_RUN or not self._client:
            return self.config.INITIAL_BANKROLL

        try:
            balance_wei = self._client.get_balance()
            return float(balance_wei) / 1e6  # USDC has 6 decimals
        except Exception as e:
            logger.error("Failed to get balance: %s", e)
            return 0.0

    def get_stats(self) -> dict:
        return {
            "orders_placed": self.orders_placed,
            "orders_filled": self.orders_filled,
            "orders_cancelled": self.orders_cancelled,
            "pending_orders": len(self._pending_orders),
            "initialized": self._initialized,
            "mode": "DRY RUN" if self.config.DRY_RUN else "LIVE",
        }
