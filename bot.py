"""
Main bot orchestrator — ties all modules together.

Lifecycle:
  1. Initialize all modules (config, monitor, engine, executor, risk, journal)
  2. Restore open positions from journal (idempotent restart)
  3. Start whale monitor polling (background task)
  4. Main loop: read signals -> consensus check -> risk check -> execute
  5. Periodic stop-loss checking on open positions
  6. Whale exit follow: when a whale exits, 120s cooldown before we follow
  7. Graceful shutdown on SIGINT/SIGTERM: cancel orders, save state, print stats
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

from datetime import datetime, timezone

from config import BotConfig, WHALE_WATCHLIST
from whale_monitor import WhaleMonitor, WhaleSignal
from signal_engine import SignalEngine, TradeOpportunity, ExitSignal
from order_executor import OrderExecutor, OrderResult
from risk_manager import RiskManager, OpenPosition, STOP_LOSS_RESOLUTION_THRESHOLD_HOURS
from trade_journal import TradeJournal
from resolution_tracker import ResolutionTracker, RESOLUTION_CHECK_INTERVAL_SECONDS

logger = logging.getLogger(__name__)

STARTUP_WARNING = """
====================================================================
  POLYMARKET WHALE COPY-TRADING BOT
====================================================================
  This bot trades real money. Past backtest results do not
  guarantee future profits. 87.3%% of Polymarket users lose money.
  Start with small capital you can afford to lose.

  Mode: %s | Bankroll: $%.2f | Whales: %d | Consensus: %.0f%%
====================================================================
"""


@dataclass
class PendingWhaleExit:
    """Tracks a pending exit that is in its cooldown period."""

    trade_id: str
    whale_alias: str
    whale_wallet: str
    condition_id: str
    market_id: str
    direction: str
    exit_pct: float  # From the whale signal (0.0–1.0)
    exit_fraction: float  # What fraction of OUR position to close (0.0–1.0)
    scheduled_time: float  # When the cooldown expires
    task: asyncio.Task | None = None


class WhaleBot:
    """Main orchestrator for the whale copy-trading bot."""

    def __init__(self, config: BotConfig):
        self.config = config
        self.monitor = WhaleMonitor(config, WHALE_WATCHLIST)
        self.engine = SignalEngine(config)
        self.executor = OrderExecutor(config)
        self.risk = RiskManager(config, config.INITIAL_BANKROLL)
        self.journal = TradeJournal(config.DB_PATH, dry_run=config.DRY_RUN)
        self.resolution_tracker = ResolutionTracker(config, self.journal)
        self._running = False
        self._tasks: list[asyncio.Task] = []
        self._start_time = 0.0

        # ── Whale exit follow state ──
        # Pending exits in cooldown: key = "trade_id:whale_alias"
        self._pending_whale_exits: dict[str, PendingWhaleExit] = {}
        # Which whales have exited each of our positions: trade_id -> set of wallet addresses
        self._whale_exit_tracking: dict[str, set[str]] = {}
        # Stats
        self._exits_followed = 0
        self._exits_cancelled = 0

    async def run(self) -> None:
        """Main entry point — runs until interrupted."""
        self._running = True
        self._start_time = time.time()

        # Print startup warning
        mode = "DRY RUN" if self.config.DRY_RUN else "LIVE TRADING"
        print(
            STARTUP_WARNING
            % (
                mode,
                self.config.INITIAL_BANKROLL,
                len(WHALE_WATCHLIST),
                self.config.CONSENSUS_THRESHOLD * 100,
            )
        )

        # Initialize modules
        self.journal.initialize()
        await self.executor.initialize()
        await self.monitor.start()

        # Phase 1+2: Capture baselines with full pagination, then quiet period
        # This MUST complete before we start monitoring or verify wallets.
        wallets = list(self.monitor.watchlist.keys())
        await self.monitor._capture_baselines(wallets)

        # Verify wallet addresses (now baselines are populated)
        self._verify_wallets()

        # Restore open positions from journal (idempotent restart)
        self._restore_positions()

        # Register signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
            except NotImplementedError:
                # Windows doesn't support add_signal_handler
                pass

        # Start resolution tracker
        await self.resolution_tracker.start()

        # Start background tasks (poll_loop skips baseline since _all_baselined=True)
        monitor_task = asyncio.create_task(self.monitor.poll_loop(), name="monitor")
        stop_check_task = asyncio.create_task(self._stop_loss_loop(), name="stop_loss")
        cleanup_task = asyncio.create_task(self._cleanup_loop(), name="cleanup")
        resolution_task = asyncio.create_task(self._resolution_loop(), name="resolution")
        self._tasks = [monitor_task, stop_check_task, cleanup_task, resolution_task]

        logger.info("Bot started — waiting for whale signals...")

        # Main signal processing loop
        try:
            while self._running:
                try:
                    # Wait for signals with timeout (allows periodic checks)
                    signal_obj = await asyncio.wait_for(
                        self.monitor.signal_queue.get(), timeout=5.0
                    )
                except asyncio.TimeoutError:
                    continue

                await self._handle_signal(signal_obj)

        except asyncio.CancelledError:
            logger.info("Main loop cancelled")
        except Exception as e:
            logger.error("Unexpected error in main loop: %s", e, exc_info=True)
        finally:
            await self.shutdown()

    # ── Signal dispatch ────────────────────────────────────────────

    async def _handle_signal(self, signal_obj) -> None:
        """Process a single whale signal through the full pipeline."""
        if not isinstance(signal_obj, WhaleSignal):
            return

        # ── Whale exit follow (runs BEFORE signal engine) ──
        if signal_obj.is_exit:
            await self._evaluate_whale_exit_follow(signal_obj)
        else:
            # Entry signal: check if it cancels any pending exit
            self._check_reentry_cancellation(signal_obj)

        # Run through signal engine (handles consensus, solo trades, fast-track)
        try:
            result = await self.engine.process_signal(signal_obj)
        except Exception as e:
            logger.error("Signal engine error: %s: %s", type(e).__name__, e)
            return

        if result is None:
            return

        if isinstance(result, ExitSignal):
            await self._handle_exit_signal(result)
            return

        if isinstance(result, TradeOpportunity):
            await self._handle_opportunity(result)

    # ── Trade entry ────────────────────────────────────────────────

    async def _enrich_opportunity(self, opportunity: TradeOpportunity) -> None:
        """
        Fetch market metadata from CLOB API to determine hours_to_resolution
        and whether stop-loss should be enabled.

        For sports markets resolving within 24h, disables stop-loss (hold to resolution).
        """
        try:
            url = f"{self.config.CLOB_HOST}/markets/{opportunity.condition_id}"

            if not hasattr(self, '_price_session') or self._price_session is None:
                import aiohttp
                self._price_session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=10),
                )

            async with self._price_session.get(url) as resp:
                if resp.status != 200:
                    return

                data = await resp.json()
                question = data.get("question", "")
                if question:
                    opportunity.market_title = question

                # Parse end date — try end_date_iso first, then game_start_time
                end_date_str = data.get("end_date_iso", "") or data.get("game_start_time", "")
                if end_date_str:
                    try:
                        # Handle various ISO formats
                        end_str = end_date_str.replace("Z", "+00:00")
                        end_dt = datetime.fromisoformat(end_str)
                        now = datetime.now(timezone.utc)
                        if end_dt.tzinfo is None:
                            end_dt = end_dt.replace(tzinfo=timezone.utc)
                        hours = (end_dt - now).total_seconds() / 3600
                        opportunity.hours_to_resolution = max(0, hours)

                        if opportunity.hours_to_resolution <= STOP_LOSS_RESOLUTION_THRESHOLD_HOURS:
                            opportunity.stop_loss_enabled = False
                            logger.info(
                                "Market resolves in %.1fh — stop-loss DISABLED (hold to resolution) | %s",
                                opportunity.hours_to_resolution,
                                question[:50],
                            )
                        else:
                            logger.info(
                                "Market resolves in %.1fh — stop-loss active | %s",
                                opportunity.hours_to_resolution,
                                question[:50],
                            )
                    except (ValueError, TypeError) as e:
                        logger.debug("Could not parse end_date '%s': %s", end_date_str, e)

        except Exception as e:
            logger.warning("Could not enrich opportunity: %s", e)

    async def _handle_opportunity(self, opportunity: TradeOpportunity) -> None:
        """Risk check, size, and execute a trade opportunity."""
        # Enrich with market metadata (hours to resolution, stop-loss decision)
        await self._enrich_opportunity(opportunity)

        # Risk check (pass opportunity for per-whale + consensus slot checks)
        can, reason = self.risk.can_trade(opportunity)
        if not can:
            logger.info("Trade blocked: %s", reason)
            return

        # Calculate position size
        size = self.risk.calculate_position_size(opportunity)
        if size <= 0:
            logger.debug("Position size zero, skipping")
            return

        # Execute
        order = await self.executor.execute_copy_trade(opportunity, size)
        if not order or not order.success:
            logger.warning("Order failed: %s", order.error if order else "None")
            return

        # Calculate stop price (still calculated for logging; ignored if stop_loss_enabled=False)
        stop_price = self.risk.calculate_stop_price(order.price)

        # Log to journal (v4: includes consensus_level; v5: market_title from enrichment)
        trade_id = self.journal.log_entry(
            market_id=opportunity.market_id,
            condition_id=opportunity.condition_id,
            token_id=opportunity.token_id,
            direction=opportunity.direction,
            entry_price=order.price,
            position_size=order.size,
            shares=order.shares,
            whale_signals=opportunity.whale_aliases,
            consensus_pct=opportunity.consensus_pct,
            n_whales=opportunity.n_whales,
            tier_score=opportunity.tier_weighted_score,
            is_fast_track=opportunity.is_fast_track,
            order_id=order.order_id,
            stop_price=stop_price,
            consensus_level=getattr(opportunity, "consensus_level", "EXACT_MARKET"),
            market_title=opportunity.market_title,
        )

        # Register with risk manager (include whale_aliases for per-whale limit tracking)
        position = OpenPosition(
            trade_id=trade_id,
            market_id=opportunity.market_id,
            condition_id=opportunity.condition_id,
            token_id=opportunity.token_id,
            direction=opportunity.direction,
            entry_price=order.price,
            shares=order.shares,
            size_usd=order.size,
            entry_time=time.time(),
            stop_price=stop_price,
            whale_aliases=opportunity.whale_aliases,
            stop_loss_enabled=opportunity.stop_loss_enabled,
            hours_to_resolution=opportunity.hours_to_resolution,
        )
        self.risk.register_entry(position)

        # Send alert if configured
        stop_info = f"Stop: ${stop_price:.4f}" if opportunity.stop_loss_enabled else "HOLD TO RESOLUTION"
        await self._send_alert(
            f"Trade: {opportunity.direction} ${size:.0f} on {opportunity.market_title or opportunity.market_id[:30]}\n"
            f"Whales: {', '.join(opportunity.whale_aliases)} ({opportunity.consensus_pct:.0%})\n"
            f"Price: ${order.price:.4f} | {stop_info}"
        )

    # ── Legacy exit signal handler (engine-level: 2+ whales exit) ──

    async def _handle_exit_signal(self, exit_signal: ExitSignal) -> None:
        """When the signal engine detects mass whale exit, close matching positions."""
        matching = [
            p for p in self.risk.open_positions
            if (p.condition_id == exit_signal.condition_id or p.market_id == exit_signal.market_id)
            and p.direction == exit_signal.direction
        ]

        for pos in matching:
            order = await self.executor.execute_exit(
                pos.token_id, pos.shares, "whale_exit"
            )
            if order and order.success:
                pnl = (order.price - pos.entry_price) * pos.shares if order.price > 0 else 0
                self.journal.log_exit(pos.trade_id, order.price, pnl, "whale_exit")
                self.risk.register_exit(pos.trade_id, order.price, pnl)
                # Clean up any pending exits for this position
                self._cleanup_pending_exits_for(pos.trade_id)

    # ══════════════════════════════════════════════════════════════
    #  WHALE EXIT FOLLOW — per-whale exit tracking with cooldown
    # ══════════════════════════════════════════════════════════════

    async def _evaluate_whale_exit_follow(self, signal: WhaleSignal) -> None:
        """
        When a whale exits a position, check if that whale triggered one of
        our open positions. If so, evaluate whether we should follow the exit.

        Rules:
          - Solo position (1 whale source):
            - Full exit (exit_pct >= 0.95): close with cooldown
            - Partial >50%: reduce proportionally with cooldown
            - Partial <=50%: log warning only
          - Consensus position (2+ whale sources):
            - Only close if >50% of original consensus whales have exited
            - Otherwise: log info only
        """
        # Find our positions on this market/condition matching direction
        matching = [
            p for p in self.risk.open_positions
            if (p.condition_id == signal.condition_id or p.market_id == signal.market_id)
            and p.direction == signal.direction
        ]

        if not matching:
            return

        for pos in matching:
            # Was this whale one of the whales who triggered our entry?
            if signal.alias not in pos.whale_aliases:
                continue

            # Track this whale as having exited this position
            if pos.trade_id not in self._whale_exit_tracking:
                self._whale_exit_tracking[pos.trade_id] = set()
            self._whale_exit_tracking[pos.trade_id].add(signal.wallet)

            is_solo = len(pos.whale_aliases) == 1
            is_full_exit = signal.exit_pct >= 0.95

            if is_solo:
                # ── Solo position: whale was our only signal source ──
                if is_full_exit:
                    # Full exit -> close our entire position
                    logger.info(
                        "WHALE EXIT FOLLOW: %s fully exited %s — scheduling close for %s (cooldown %.0fs)",
                        signal.alias, pos.market_id[:30], pos.trade_id[:12],
                        self.config.WHALE_EXIT_COOLDOWN_SECONDS,
                    )
                    await self._schedule_whale_exit(pos, signal, exit_fraction=1.0)

                elif signal.exit_pct > 0.50:
                    # Partial >50% -> reduce proportionally
                    logger.info(
                        "WHALE EXIT FOLLOW: %s reduced %.0f%% of %s — scheduling proportional reduce for %s",
                        signal.alias, signal.exit_pct * 100,
                        pos.market_id[:30], pos.trade_id[:12],
                    )
                    await self._schedule_whale_exit(pos, signal, exit_fraction=signal.exit_pct)

                else:
                    # Partial <=50% -> log warning only
                    logger.info(
                        "WHALE EXIT WATCH: %s reduced %.0f%% of %s — below 50%% threshold, no action",
                        signal.alias, signal.exit_pct * 100, pos.market_id[:30],
                    )

            else:
                # ── Consensus position: need >50% of original whales to exit ──
                exited_count = self._count_exited_whales(pos)
                total_whales = len(pos.whale_aliases)
                exit_ratio = exited_count / total_whales if total_whales > 0 else 0

                if exit_ratio > 0.50:
                    logger.info(
                        "WHALE EXIT FOLLOW (consensus): %d/%d whales exited %s — scheduling close for %s",
                        exited_count, total_whales, pos.market_id[:30], pos.trade_id[:12],
                    )
                    await self._schedule_whale_exit(pos, signal, exit_fraction=1.0)
                else:
                    logger.info(
                        "WHALE EXIT WATCH (consensus): %s exited, %d/%d whales gone on %s — below majority, no action",
                        signal.alias, exited_count, total_whales, pos.market_id[:30],
                    )

    async def _schedule_whale_exit(
        self, pos: OpenPosition, signal: WhaleSignal, exit_fraction: float,
    ) -> None:
        """
        Start the cooldown timer before executing our exit.
        If the whale re-enters within WHALE_EXIT_COOLDOWN_SECONDS, the exit is cancelled.
        """
        key = f"{pos.trade_id}:{signal.alias}"

        # Cancel any existing pending exit for this position+whale combo
        if key in self._pending_whale_exits:
            existing = self._pending_whale_exits[key]
            if existing.task and not existing.task.done():
                existing.task.cancel()
            del self._pending_whale_exits[key]

        cooldown = self.config.WHALE_EXIT_COOLDOWN_SECONDS
        scheduled_time = time.time() + cooldown

        pending = PendingWhaleExit(
            trade_id=pos.trade_id,
            whale_alias=signal.alias,
            whale_wallet=signal.wallet,
            condition_id=signal.condition_id,
            market_id=signal.market_id,
            direction=signal.direction,
            exit_pct=signal.exit_pct,
            exit_fraction=exit_fraction,
            scheduled_time=scheduled_time,
        )

        # Create the cooldown task
        task = asyncio.create_task(
            self._execute_whale_exit_after_cooldown(pos, exit_fraction, key),
            name=f"exit_cooldown:{key}",
        )
        pending.task = task
        self._pending_whale_exits[key] = pending

        logger.debug(
            "Exit cooldown started: %s for %s (%.0fs, fraction=%.0f%%)",
            key, pos.market_id[:30], cooldown, exit_fraction * 100,
        )

    async def _execute_whale_exit_after_cooldown(
        self, pos: OpenPosition, exit_fraction: float, key: str,
    ) -> None:
        """Wait for cooldown, then execute the exit if not cancelled."""
        try:
            await asyncio.sleep(self.config.WHALE_EXIT_COOLDOWN_SECONDS)
        except asyncio.CancelledError:
            # Cooldown was cancelled (whale re-entered)
            logger.debug("Exit cooldown cancelled for %s", key)
            return

        # Check if position is still open (might have been stopped out or exited already)
        still_open = any(p.trade_id == pos.trade_id for p in self.risk.open_positions)
        if not still_open:
            logger.debug("Position %s already closed, skipping exit follow", pos.trade_id[:12])
            self._pending_whale_exits.pop(key, None)
            return

        # Re-fetch the position in case shares changed (partial reduction)
        current_pos = next(
            (p for p in self.risk.open_positions if p.trade_id == pos.trade_id), None
        )
        if not current_pos:
            self._pending_whale_exits.pop(key, None)
            return

        # Calculate shares to exit
        shares_to_exit = current_pos.shares * exit_fraction

        if exit_fraction >= 0.95:
            # Full close
            shares_to_exit = current_pos.shares
            reason = "whale_exit_follow"
        else:
            reason = "whale_exit_follow_partial"

        logger.info(
            "EXECUTING WHALE EXIT FOLLOW: %s %.0f shares (%.0f%%) on %s",
            reason, shares_to_exit, exit_fraction * 100, current_pos.market_id[:30],
        )

        order = await self.executor.execute_exit(
            current_pos.token_id, shares_to_exit, reason,
        )

        if order and order.success:
            exit_price = order.price if order.price > 0 else current_pos.entry_price
            pnl = (exit_price - current_pos.entry_price) * shares_to_exit

            if exit_fraction >= 0.95:
                # Full exit: close the position
                self.journal.log_exit(current_pos.trade_id, exit_price, pnl, "whale_exit_follow")
                self.risk.register_exit(current_pos.trade_id, exit_price, pnl)
                self._cleanup_pending_exits_for(current_pos.trade_id)
            else:
                # Partial exit: reduce shares on the position
                current_pos.shares -= shares_to_exit
                current_pos.size_usd = current_pos.shares * current_pos.entry_price
                logger.info(
                    "Position %s reduced to %.0f shares (was %.0f)",
                    current_pos.trade_id[:12], current_pos.shares,
                    current_pos.shares + shares_to_exit,
                )

            self._exits_followed += 1

            await self._send_alert(
                f"WHALE EXIT FOLLOW: {reason}\n"
                f"Shares: {shares_to_exit:.0f} | PnL: ${pnl:.2f}\n"
                f"Market: {current_pos.market_id[:30]}"
            )
        else:
            logger.warning(
                "Whale exit follow order failed for %s: %s",
                current_pos.trade_id[:12], order.error if order else "None",
            )

        self._pending_whale_exits.pop(key, None)

    def _check_reentry_cancellation(self, signal: WhaleSignal) -> None:
        """
        On a new entry signal, check if there's a pending exit for the same
        whale + market. If so, cancel the exit — the whale re-entered.
        """
        keys_to_cancel: list[str] = []

        for key, pending in self._pending_whale_exits.items():
            # Same whale + same market/condition + same direction
            if (
                pending.whale_wallet == signal.wallet
                and pending.direction == signal.direction
                and (
                    pending.condition_id == signal.condition_id
                    or pending.market_id == signal.market_id
                )
            ):
                keys_to_cancel.append(key)

        for key in keys_to_cancel:
            pending = self._pending_whale_exits.pop(key)
            if pending.task and not pending.task.done():
                pending.task.cancel()
            self._exits_cancelled += 1

            # Also remove from whale exit tracking
            if pending.trade_id in self._whale_exit_tracking:
                self._whale_exit_tracking[pending.trade_id].discard(pending.whale_wallet)

            logger.info(
                "WHALE EXIT CANCELLED — re-entry detected: %s re-entered %s within %.0fs cooldown",
                pending.whale_alias, pending.market_id[:30],
                self.config.WHALE_EXIT_COOLDOWN_SECONDS,
            )

    def _count_exited_whales(self, pos: OpenPosition) -> int:
        """Count how many of the original consensus whales have exited this position."""
        exited_wallets = self._whale_exit_tracking.get(pos.trade_id, set())
        # Map whale_aliases back to wallets for comparison
        # whale_aliases stores alias names; exited_wallets stores wallet addresses.
        # We need to match by looking up which wallets correspond to which aliases.
        count = 0
        for wallet, info in WHALE_WATCHLIST.items():
            if info["alias"] in pos.whale_aliases and wallet in exited_wallets:
                count += 1
        return count

    def _cleanup_pending_exits_for(self, trade_id: str) -> None:
        """Remove all pending exits for a closed position."""
        keys_to_remove = [
            k for k, v in self._pending_whale_exits.items()
            if v.trade_id == trade_id
        ]
        for key in keys_to_remove:
            pending = self._pending_whale_exits.pop(key)
            if pending.task and not pending.task.done():
                pending.task.cancel()

        self._whale_exit_tracking.pop(trade_id, None)

    # ── Stop-loss loop ─────────────────────────────────────────────

    async def _stop_loss_loop(self) -> None:
        """Periodically check open positions against stop-loss prices."""
        while self._running:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds

                if not self.risk.open_positions:
                    continue

                # Get current prices for all open position tokens
                current_prices = await self._fetch_current_prices()
                if not current_prices:
                    continue

                stopped = self.risk.check_stop_losses(current_prices)
                for pos in stopped:
                    current_price = current_prices.get(pos.token_id, 0)
                    order = await self.executor.execute_exit(
                        pos.token_id, pos.shares, "stop_loss"
                    )
                    if order and order.success:
                        exit_price = order.price if order.price > 0 else current_price
                        pnl = (exit_price - pos.entry_price) * pos.shares
                        self.journal.log_exit(pos.trade_id, exit_price, pnl, "stop_loss")
                        self.risk.register_exit(pos.trade_id, exit_price, pnl)
                        self._cleanup_pending_exits_for(pos.trade_id)

                        await self._send_alert(
                            f"STOP-LOSS: {pos.market_id[:30]}\n"
                            f"PnL: ${pnl:.2f} | Exit: ${exit_price:.4f}"
                        )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Stop-loss check error: %s", e)

    async def _fetch_current_prices(self) -> dict[str, float]:
        """
        Fetch current midpoint prices for all open position tokens.

        CRITICAL: Each position's token_id MUST map to its own unique price.
        Never cross-contaminate prices between different markets.

        In dry-run mode, fetches real midpoints from the CLOB REST API
        (read-only, no auth required). Falls back to entry_price per-position
        only if the API call fails — and ONLY for that specific position.
        """
        prices: dict[str, float] = {}

        if self.config.DRY_RUN:
            # Fetch REAL midpoints from CLOB API (read-only, no auth needed).
            # This lets stop-losses work properly in dry-run mode.
            for pos in self.risk.open_positions:
                if not pos.token_id:
                    # Skip positions with empty token_id (legacy bug)
                    logger.debug(
                        "Skipping stop-loss for %s — no token_id (legacy position)",
                        pos.trade_id[:12],
                    )
                    continue

                # Don't overwrite if we already have a price for this token
                if pos.token_id in prices:
                    continue

                mid = await self._fetch_clob_midpoint(pos.token_id)
                if mid is not None and mid > 0:
                    prices[pos.token_id] = mid
                else:
                    # Fallback: use entry price (stop will never trigger since
                    # entry_price > stop_price by definition — this is safe)
                    prices[pos.token_id] = pos.entry_price

            return prices

        # Live mode: use py-clob-client
        if not self.executor._client:
            return prices

        for pos in self.risk.open_positions:
            if not pos.token_id or pos.token_id in prices:
                continue
            try:
                mid = float(self.executor._client.get_midpoint(pos.token_id))
                prices[pos.token_id] = mid
            except Exception:
                continue

        return prices

    async def _fetch_clob_midpoint(self, token_id: str) -> float | None:
        """
        Fetch a single token's midpoint from the CLOB REST API.
        Read-only endpoint, no authentication required.
        Returns None on failure.
        """
        try:
            url = f"{self.config.CLOB_HOST}/midpoint"
            params = {"token_id": token_id}

            if not hasattr(self, '_price_session') or self._price_session is None:
                import aiohttp
                self._price_session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=5),
                )

            async with self._price_session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    mid = float(data.get("mid", 0))
                    return mid if mid > 0 else None
                else:
                    return None
        except Exception as e:
            logger.debug("CLOB midpoint fetch failed for %s: %s", token_id[:16], e)
            return None

    # ── Resolution loop (Upgrade 3) ──────────────────────────────

    async def _resolution_loop(self) -> None:
        """Periodically check for resolved markets and classify outcomes."""
        while self._running:
            try:
                await asyncio.sleep(RESOLUTION_CHECK_INTERVAL_SECONDS)

                resolved = await self.resolution_tracker.check_resolutions()
                if resolved > 0:
                    logger.info("Resolution check: %d trades resolved", resolved)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Resolution check error: %s", e)

    # ── Cleanup loop ───────────────────────────────────────────────

    async def _cleanup_loop(self) -> None:
        """Periodic cleanup of stale signals and data."""
        while self._running:
            try:
                await asyncio.sleep(60)
                self.engine.cleanup_stale()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug("Cleanup error: %s", e)

    # ── Wallet verification ────────────────────────────────────────

    def _verify_wallets(self) -> None:
        """Verify whale wallet addresses after baseline capture.
        Only flags wallets that returned 0 positions from the Data API."""
        zero_wallets = []
        for wallet, info in self.monitor.watchlist.items():
            positions = self.monitor._confirmed_positions.get(wallet, {})
            alias = info["alias"]
            n = len(positions)
            if n == 0:
                zero_wallets.append(alias)
                logger.warning(
                    "VERIFY: %s has 0 positions — may have closed all, "
                    "or address may be a proxy wallet. Check: "
                    "https://polymarket.com/profile/%s",
                    alias, wallet,
                )
        if zero_wallets:
            logger.warning(
                "VERIFY SUMMARY: %d/%d wallets have 0 positions: %s",
                len(zero_wallets), len(self.monitor.watchlist),
                ", ".join(zero_wallets),
            )
        else:
            logger.info("VERIFY: All %d wallets have positions — addresses look good", len(self.monitor.watchlist))

    # ── Position restoration ───────────────────────────────────────

    def _restore_positions(self) -> None:
        """Restore open positions from journal on restart."""
        open_trades = self.journal.get_open_positions()
        if not open_trades:
            logger.info("No open positions to restore")
            return

        positions = []
        for t in open_trades:
            # Parse whale_aliases from the JSON-encoded whale_signals field
            whale_aliases: list[str] = []
            raw_signals = t.get("whale_signals", "")
            if raw_signals:
                try:
                    whale_aliases = json.loads(raw_signals)
                except (json.JSONDecodeError, TypeError):
                    whale_aliases = []

            pos = OpenPosition(
                trade_id=t["id"],
                market_id=t["market_id"],
                condition_id=t.get("condition_id", ""),
                token_id=t["token_id"],
                direction=t["direction"],
                entry_price=t["entry_price"],
                shares=t["shares"],
                size_usd=t["position_size"],
                entry_time=time.time(),  # Approximate
                stop_price=t.get("stop_price", t["entry_price"] * 0.85),
                whale_aliases=whale_aliases,
            )
            positions.append(pos)

        self.risk.load_positions(positions)

    # ── Alerts ─────────────────────────────────────────────────────

    async def _send_alert(self, message: str) -> None:
        """Send Telegram alert if configured."""
        if not self.config.TELEGRAM_BOT_TOKEN or not self.config.TELEGRAM_CHAT_ID:
            return

        try:
            from utils.telegram_alerts import send_alert
            await send_alert(
                self.config.TELEGRAM_BOT_TOKEN,
                self.config.TELEGRAM_CHAT_ID,
                message,
            )
        except Exception as e:
            logger.debug("Telegram alert failed: %s", e)

    # ── Shutdown ───────────────────────────────────────────────────

    async def shutdown(self) -> None:
        """Graceful shutdown: cancel orders, save state, print stats."""
        if not self._running:
            return
        self._running = False

        logger.info("Shutting down...")

        # Cancel all pending whale exit cooldowns
        for key, pending in list(self._pending_whale_exits.items()):
            if pending.task and not pending.task.done():
                pending.task.cancel()
        self._pending_whale_exits.clear()

        # Cancel background tasks
        for task in self._tasks:
            task.cancel()

        # Wait for tasks to finish
        await asyncio.gather(*self._tasks, return_exceptions=True)

        # Shutdown executor (cancels open orders)
        await self.executor.shutdown()

        # Shutdown signal engine (closes Gamma API session)
        await self.engine.shutdown()

        # Stop resolution tracker
        await self.resolution_tracker.stop()

        # Close price session (dry-run CLOB midpoint fetcher)
        if hasattr(self, '_price_session') and self._price_session:
            await self._price_session.close()
            self._price_session = None

        # Stop monitor
        await self.monitor.stop()

        # Print session summary
        self._print_session_stats()

        # Close journal
        self.journal.close()

        logger.info("Shutdown complete")

    def _print_session_stats(self) -> None:
        """Print session performance summary."""
        runtime = time.time() - self._start_time
        hours = runtime / 3600

        risk_stats = self.risk.get_stats()
        engine_stats = self.engine.get_stats()
        monitor_stats = self.monitor.get_stats()
        executor_stats = self.executor.get_stats()
        journal_stats = self.journal.get_performance_stats()
        resolution_stats = self.resolution_tracker.get_stats()

        print("\n" + "=" * 60)
        print("  SESSION SUMMARY")
        print("=" * 60)
        print(f"  Runtime:          {hours:.1f} hours")
        print(f"  Mode:             {'DRY RUN' if self.config.DRY_RUN else 'LIVE'}")
        print(f"  Polls:            {monitor_stats['poll_count']}")
        print(f"  Signals:          {engine_stats['signals_processed']}")
        print(f"  Opportunities:    {engine_stats['opportunities_generated']}")
        print(f"  Orders placed:    {executor_stats['orders_placed']}")
        print(f"  Open positions:   {risk_stats['open_positions']}")
        print(f"  Bankroll:         ${risk_stats['bankroll']:.2f}")
        print(f"  Total PnL:        ${risk_stats['total_pnl']:.2f}")
        print(f"  Return:           {risk_stats['total_return_pct']:.1f}%")

        # Only show journal stats if we placed trades THIS session
        if executor_stats['orders_placed'] > 0 and journal_stats["total_trades"] > 0:
            print(f"  Win rate:         {journal_stats['win_rate']:.1%}")
            print(f"  Best trade:       ${journal_stats['best_trade']:.2f}")
            print(f"  Worst trade:      ${journal_stats['worst_trade']:.2f}")
            print(f"  Profit factor:    {journal_stats['profit_factor']:.2f}")

        # Whale exit follow stats
        if self._exits_followed > 0 or self._exits_cancelled > 0:
            print(f"  Exit follows:     {self._exits_followed}")
            print(f"  Exits cancelled:  {self._exits_cancelled} (whale re-entry)")

        # Circuit breaker stats
        if risk_stats.get("circuit_breaker_active"):
            print(f"  Circuit breaker:  ACTIVE (drawdown {risk_stats['current_drawdown_pct']:.1f}%)")
        print(f"  Peak bankroll:    ${risk_stats.get('peak_bankroll', 0):.2f}")
        print(f"  Max drawdown:     {risk_stats.get('current_drawdown_pct', 0):.1f}%")

        # Resolution tracking stats
        if resolution_stats["checks"] > 0:
            print(f"  Resolution checks: {resolution_stats['checks']}")
            print(f"  Trades resolved:   {resolution_stats['resolutions_found']}")

        print("=" * 60)
