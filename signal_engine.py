"""
Consensus signal engine — groups whale signals by market and detects agreement.

Design rationale (from backtest):
  - 60% consensus threshold across 6+ whales produced 57% win rate.
  - 5-minute signal window captures correlated whale activity without stale signals.
  - Tier-weighted scoring: Tier 1 = 3x, Tier 2 = 2x, Tier 3 = 1x.
  - Fast-track: Tier 1 whales with >4x avg bet bypass consensus for speed.

v3 additions:
  - Tier 1/2 solo trades: top whales generate trades WITHOUT consensus at reduced size.
  - Event-level consensus: whales on the same event (different markets) count as consensus.
  - These fixes address the zero-trade problem when whales trade different niches.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from statistics import mean
from typing import Optional

import aiohttp

from config import BotConfig, TIER_1_WALLETS, WHALE_AVG_BET, WHALE_WATCHLIST
from whale_monitor import WhaleSignal

logger = logging.getLogger(__name__)


@dataclass
class TradeOpportunity:
    """A validated trade signal ready for risk check and execution."""

    market_id: str
    condition_id: str
    token_id: str
    direction: str  # "YES" or "NO"
    consensus_pct: float
    n_whales: int
    avg_whale_entry: float
    total_whale_size: float
    whale_aliases: list[str]
    tier_weighted_score: float
    signal_time: float = field(default_factory=time.time)
    is_fast_track: bool = False
    # v3: consensus level and position multiplier
    consensus_level: str = "EXACT_MARKET"  # EXACT_MARKET, EVENT, TIER1_SOLO, TIER2_SOLO, TIER3_SOLO, FAST_TRACK
    position_multiplier: float = 1.0  # Applied on top of normal sizing
    # v5: market metadata for stop-loss decisions
    market_title: str = ""
    hours_to_resolution: float | None = None
    stop_loss_enabled: bool = True  # False for short-duration sports markets

    @property
    def strength_label(self) -> str:
        if self.consensus_pct >= 0.9:
            return "VERY STRONG"
        elif self.consensus_pct >= 0.75:
            return "STRONG"
        elif self.consensus_pct >= 0.6:
            return "MODERATE"
        return "WEAK"


@dataclass
class ExitSignal:
    """Signal that whales are exiting a position — we should follow."""

    market_id: str
    condition_id: str
    token_id: str
    direction: str
    whale_aliases: list[str]
    total_exit_size: float
    signal_time: float = field(default_factory=time.time)


class SignalEngine:
    """
    Processes WhaleSignals into TradeOpportunity or ExitSignal objects.

    Signal hierarchy (checked in order):
      1. Fast-track: Tier 1 + trade >= 4x avg bet -> immediate trade at 50% size
      2. Exact market consensus: 2+ whales on same conditionId -> full size
      3. Event consensus: 2+ whales on same event (different markets) -> 70% size
      4. Tier 1 solo: single Tier 1 whale trade >= $8K -> 40% size
      5. Tier 2 solo: single Tier 2 whale trade >= $10K -> 25% size
      6. Tier 3 solo: single Tier 3 whale trade >= $15K -> 20% size
    """

    def __init__(self, config: BotConfig):
        self.config = config
        self._pending: dict[str, list[WhaleSignal]] = {}
        self._exit_signals: dict[str, list[WhaleSignal]] = {}
        self._opportunities_generated = 0
        self._signals_processed = 0
        self._recent_opportunities: dict[str, float] = {}
        self._dedup_window = 60.0
        # Market-level lock: prevents ANY second trade on a market we already committed to.
        # Set immediately when opportunity is generated (before async execution completes).
        self._market_locks: dict[str, float] = {}  # condition_id -> lock_time
        self._market_lock_duration = 300.0  # 5 min lock

        # ── Signal cooldown: prevents duplicate stacking from chunked whale entries ──
        # Key: (whale_address, condition_id) -> expiry timestamp
        self._signal_cooldowns: dict[tuple[str, str], float] = {}
        self.SIGNAL_COOLDOWN_MINUTES = 60  # Suppress same whale+market for 60 min

        # ── Reference to risk manager (set by bot.py after init) ──
        self._risk_manager = None  # type: ignore

        # Event cache: condition_id -> event_slug (fetched from Gamma API)
        self._event_cache: dict[str, str] = {}
        self._event_cache_misses: dict[str, float] = {}  # condition_id -> last_attempt_time
        self._http_session: aiohttp.ClientSession | None = None

    def set_risk_manager(self, risk_manager) -> None:
        """Set reference to risk manager for open position checks."""
        self._risk_manager = risk_manager

    def _check_entry_price_filter(
        self, entry_price: float, direction: str, alias: str,
        max_price: float | None = None,
    ) -> bool:
        """Reject trades outside the profitable entry price range."""
        effective_max = max_price if max_price is not None else self.config.MAX_ENTRY_PRICE
        if entry_price < self.config.MIN_ENTRY_PRICE:
            logger.info(
                "Filtered: %s %s @ %.3f — below min entry %.2f (extreme longshot)",
                alias, direction, entry_price, self.config.MIN_ENTRY_PRICE,
            )
            return False
        if entry_price > effective_max:
            logger.info(
                "Filtered: %s %s @ %.3f — above max entry %.2f (margin too thin)",
                alias, direction, entry_price, effective_max,
            )
            return False
        return True

    def _check_spread_filter(self, market_title: str, direction: str, alias: str) -> bool:
        """Skip extreme spread bets that are essentially coin flips."""
        if not market_title:
            return True  # Can't filter without title, allow through
        spread_match = re.search(r'Spread:.*?[(\-+](\d+\.?\d*)\)', market_title)
        if spread_match:
            spread_value = float(spread_match.group(1))
            if spread_value > self.config.MAX_SPREAD_POINTS:
                logger.info(
                    "Filtered: %s %s — spread %.1f > max %.1f (extreme spread)",
                    alias, direction, spread_value, self.config.MAX_SPREAD_POINTS,
                )
                return False
        return True

    def is_on_cooldown(self, wallet: str, condition_id: str) -> bool:
        """Check if a whale+market pair is on signal cooldown."""
        key = (wallet, condition_id)
        if key in self._signal_cooldowns:
            if time.time() < self._signal_cooldowns[key]:
                remaining = (self._signal_cooldowns[key] - time.time()) / 60
                logger.debug(
                    "Cooldown active: %s on %s (%.0fm left)",
                    wallet[:10], condition_id[:16], remaining,
                )
                return True
            else:
                del self._signal_cooldowns[key]
        return False

    def set_cooldown(self, wallet: str, condition_id: str, minutes: int = 60) -> None:
        """Set signal cooldown for a whale+market pair."""
        key = (wallet, condition_id)
        self._signal_cooldowns[key] = time.time() + (minutes * 60)
        logger.info(
            "Cooldown set: %s on %s for %dm",
            wallet[:10], condition_id[:16], minutes,
        )

    def has_open_position_on_market(self, condition_id: str) -> bool:
        """Check if we already have any open position on this market,
        OR if we recently generated an opportunity for it (market lock)."""
        # Check market lock first (immediate, pre-execution)
        lock_time = self._market_locks.get(condition_id)
        if lock_time and (time.time() - lock_time) < self._market_lock_duration:
            return True
        if self._risk_manager is None:
            return False
        for pos in self._risk_manager.open_positions:
            if pos.condition_id == condition_id or pos.market_id == condition_id:
                return True
        return False

    def lock_market(self, condition_id: str) -> None:
        """Immediately lock a market when an opportunity is generated."""
        self._market_locks[condition_id] = time.time()

    async def _ensure_session(self) -> None:
        """Lazy-init HTTP session for Gamma API calls."""
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
            )

    async def process_signal(
        self, signal: WhaleSignal
    ) -> Optional[TradeOpportunity | ExitSignal]:
        """
        Process an incoming whale signal through the full hierarchy.

        Returns a TradeOpportunity, ExitSignal, or None.
        """
        self._signals_processed += 1

        if signal.is_exit:
            return self._process_exit(signal)

        # 0a. Entry price filter — reject extreme longshots and near-certainties
        # Tier 1 whales get a wider price range (proven at higher prices)
        max_price = (
            self.config.TIER1_MAX_ENTRY_PRICE
            if signal.tier == 1
            else self.config.MAX_ENTRY_PRICE
        )
        if signal.entry_price > 0 and not self._check_entry_price_filter(
            signal.entry_price, signal.direction, signal.alias, max_price=max_price,
        ):
            return None

        # 0b. Spread magnitude filter — reject extreme spreads (>10.5 points)
        market_title = getattr(signal, "market_title", "") or ""
        if not self._check_spread_filter(market_title, signal.direction, signal.alias):
            return None

        # 1. Check fast-track (Tier 1 + huge relative size)
        if self._should_fast_track(signal):
            return self._fast_track(signal)

        # 2. Feed into consensus pool (checks exact market + event consensus)
        result = await self._check_consensus(signal)
        if result:
            return result

        # Check if this whale is allowed to solo trade (consensus-only whales skip)
        whale_config = WHALE_WATCHLIST.get(signal.wallet, {})
        if not whale_config.get("solo_enabled", True):
            logger.info(
                "Skipped solo from %s (consensus-only whale)", signal.alias,
            )
            return None

        # ── Cooldown check: suppress duplicate signals from same whale on same market ──
        market_key = signal.condition_id or signal.market_id
        if self.is_on_cooldown(signal.wallet, market_key):
            logger.info(
                "Suppressed: duplicate solo from %s on %s (cooldown)",
                signal.alias, market_key[:16],
            )
            return None

        # ── Already have position check: don't open second position on same market ──
        if self.has_open_position_on_market(market_key):
            logger.info(
                "Suppressed: already have position on %s (solo from %s)",
                market_key[:16], signal.alias,
            )
            return None

        # 3. Tier 1 solo trade (no consensus needed)
        if self.config.TIER1_SOLO_ENABLED and signal.tier == 1:
            if signal.size_usd >= self.config.TIER1_SOLO_MIN_USD:
                return self._solo_trade(signal, "TIER1_SOLO", self.config.TIER1_SOLO_POSITION_MULT)

        # 4. Tier 2 solo trade (only from whales with good win rates)
        if self.config.TIER2_SOLO_ENABLED and signal.tier == 2:
            whale_wr = whale_config.get("win_rate", 0.50)
            min_wr = getattr(self.config, "TIER2_SOLO_MIN_WIN_RATE", 0.55)
            if whale_wr < min_wr:
                logger.info(
                    "Skipped T2 solo from %s (win_rate %.0f%% < %.0f%% min)",
                    signal.alias, whale_wr * 100, min_wr * 100,
                )
                return None
            if signal.size_usd >= self.config.TIER2_SOLO_MIN_USD:
                return self._solo_trade(signal, "TIER2_SOLO", self.config.TIER2_SOLO_POSITION_MULT)

        # 5. Tier 3 solo trade (highest bar, smallest size)
        if self.config.TIER3_SOLO_ENABLED and signal.tier == 3:
            if signal.size_usd >= self.config.TIER3_SOLO_MIN_USD:
                return self._solo_trade(signal, "TIER3_SOLO", self.config.TIER3_SOLO_POSITION_MULT)

        return None

    def _should_fast_track(self, signal: WhaleSignal) -> bool:
        """
        Relative fast-track: must be Tier 1 AND trade >= 4x the whale's average bet.
        """
        if signal.tier != 1:
            return False
        avg_bet = WHALE_AVG_BET.get(signal.wallet, 20_000)
        threshold = avg_bet * self.config.FAST_TRACK_MULTIPLIER
        if signal.size_usd >= threshold:
            logger.debug(
                "Fast-track eligible: %s $%.0f >= %.0f (avg $%.0f * %.1fx)",
                signal.alias, signal.size_usd, threshold, avg_bet, self.config.FAST_TRACK_MULTIPLIER,
            )
            return True
        return False

    async def _check_consensus(self, signal: WhaleSignal) -> Optional[TradeOpportunity]:
        """Add signal to pending pool and check both exact-market and event-level consensus."""
        market = signal.condition_id or signal.market_id
        now = time.time()

        # Add to pending signals
        if market not in self._pending:
            self._pending[market] = []
        self._pending[market].append(signal)

        # Prune signals outside the window
        self._pending[market] = [
            s for s in self._pending[market]
            if now - s.timestamp < self.config.SIGNAL_WINDOW_SECONDS
        ]

        # ── Level 1: Exact market consensus ──
        opportunity = self._check_exact_market_consensus(market)
        if opportunity:
            return opportunity

        # ── Level 2: Event-level consensus ──
        if self.config.EVENT_CONSENSUS_ENABLED:
            opportunity = await self._check_event_consensus(signal)
            if opportunity:
                return opportunity

        return None

    def _check_exact_market_consensus(self, market: str) -> Optional[TradeOpportunity]:
        """Check if 2+ whales agree on the exact same market (condition_id)."""
        now = time.time()
        signals = self._pending.get(market, [])
        unique_whales = {s.wallet for s in signals}

        if len(unique_whales) < self.config.MIN_WHALES_SIGNALING:
            return None

        # ── Don't stack consensus on top of existing position ──
        if self.has_open_position_on_market(market):
            logger.info(
                "Consensus CONFIRMS existing position on %s (%d whales agree) — no new trade",
                market[:16], len(unique_whales),
            )
            return None

        # Deduplicate: use latest signal per whale
        latest_per_whale: dict[str, WhaleSignal] = {}
        for s in signals:
            if s.wallet not in latest_per_whale or s.timestamp > latest_per_whale[s.wallet].timestamp:
                latest_per_whale[s.wallet] = s

        # Hedge detection: exclude whales holding both YES and NO
        whale_directions: dict[str, set[str]] = {}
        for s in signals:
            whale_directions.setdefault(s.wallet, set()).add(s.direction)
        hedging_whales = {w for w, dirs in whale_directions.items() if len(dirs) > 1}
        if hedging_whales:
            aliases = [latest_per_whale[w].alias for w in hedging_whales if w in latest_per_whale]
            logger.debug("Excluding %d hedging whale(s): %s", len(hedging_whales), aliases)
            for w in hedging_whales:
                latest_per_whale.pop(w, None)

        deduped = list(latest_per_whale.values())
        if len({s.wallet for s in deduped}) < self.config.MIN_WHALES_SIGNALING:
            return None

        # Direction vote
        yes_votes = [s for s in deduped if s.direction in ("YES", "Y")]
        no_votes = [s for s in deduped if s.direction in ("NO", "N")]
        total = len(deduped)
        if total == 0:
            return None

        if len(yes_votes) >= len(no_votes):
            consensus_dir, majority = "YES", yes_votes
            consensus_pct = len(yes_votes) / total
        else:
            consensus_dir, majority = "NO", no_votes
            consensus_pct = len(no_votes) / total

        if consensus_pct < self.config.CONSENSUS_THRESHOLD:
            return None

        # Dedup window
        if market in self._recent_opportunities:
            if now - self._recent_opportunities[market] < self._dedup_window:
                return None

        opportunity = TradeOpportunity(
            market_id=majority[0].market_id,
            condition_id=majority[0].condition_id,
            token_id=majority[0].token_id,
            direction=consensus_dir,
            consensus_pct=consensus_pct,
            n_whales=len(unique_whales),
            avg_whale_entry=mean(s.entry_price for s in majority if s.entry_price > 0) if any(s.entry_price > 0 for s in majority) else 0.0,
            total_whale_size=sum(s.size_usd for s in majority),
            whale_aliases=[s.alias for s in majority],
            tier_weighted_score=self._calc_tier_score(majority),
            consensus_level="EXACT_MARKET",
            position_multiplier=1.0,
        )

        self._recent_opportunities[market] = now
        self._opportunities_generated += 1
        self._pending[market] = []
        self.lock_market(market)  # Prevent duplicate entries on same market

        logger.info(
            "CONSENSUS [EXACT]: %s %s (%.0f%% from %d whales: %s) | score=%.1f",
            consensus_dir, market[:30], consensus_pct * 100,
            opportunity.n_whales, ", ".join(opportunity.whale_aliases),
            opportunity.tier_weighted_score,
        )
        return opportunity

    async def _check_event_consensus(self, signal: WhaleSignal) -> Optional[TradeOpportunity]:
        """
        Check if another whale has signaled on the same EVENT (different market).
        Uses Gamma API to resolve market -> event mapping.
        """
        now = time.time()
        market = signal.condition_id or signal.market_id

        # Get the event for this signal's market
        event_id = await self._get_event_for_market(market)
        if not event_id:
            return None

        # Search all pending signals for any from a DIFFERENT market in the SAME event
        for other_market, other_signals in self._pending.items():
            if other_market == market:
                continue

            other_event = await self._get_event_for_market(other_market)
            if other_event != event_id:
                continue

            # Same event! Check for unique whales across both markets
            other_whales = {s.wallet for s in other_signals if s.wallet != signal.wallet}
            if not other_whales:
                continue

            # We have 2+ whales on the same event — event consensus!
            # Don't stack on existing position
            if self.has_open_position_on_market(market):
                logger.info(
                    "Event consensus CONFIRMS existing position on %s — no new trade",
                    market[:16],
                )
                return None

            all_signals = self._pending.get(market, []) + other_signals
            combined_aliases = list({s.alias for s in all_signals})
            combined_whales = {s.wallet for s in all_signals}

            # Dedup check
            dedup_key = f"event:{event_id}"
            if dedup_key in self._recent_opportunities:
                if now - self._recent_opportunities[dedup_key] < self._dedup_window:
                    return None

            opportunity = TradeOpportunity(
                market_id=signal.market_id,
                condition_id=signal.condition_id,
                token_id=signal.token_id,
                direction=signal.direction,
                consensus_pct=1.0,  # Both whales agree on the event
                n_whales=len(combined_whales),
                avg_whale_entry=signal.entry_price,
                total_whale_size=sum(s.size_usd for s in all_signals),
                whale_aliases=combined_aliases,
                tier_weighted_score=self._calc_tier_score(all_signals),
                consensus_level="EVENT",
                position_multiplier=self.config.EVENT_CONSENSUS_POSITION_MULT,
            )

            self._recent_opportunities[dedup_key] = now
            self._opportunities_generated += 1
            self.lock_market(market)  # Prevent duplicate entries on same market

            logger.info(
                "CONSENSUS [EVENT]: %s %s (event=%s, %d whales: %s) | 70%% size",
                signal.direction, signal.market_id[:30], event_id[:20],
                len(combined_whales), ", ".join(combined_aliases),
            )
            return opportunity

        return None

    async def _get_event_for_market(self, condition_id: str) -> str | None:
        """
        Resolve a condition_id to its parent event using Gamma API.
        Results are cached to minimize API calls.
        """
        if condition_id in self._event_cache:
            return self._event_cache[condition_id]

        # Don't retry failed lookups within 5 minutes
        last_miss = self._event_cache_misses.get(condition_id, 0)
        if time.time() - last_miss < 300:
            return None

        try:
            await self._ensure_session()
            url = f"{self.config.GAMMA_API}/markets/{condition_id}"
            async with self._http_session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    # Try various fields that might contain event info
                    event_slug = (
                        data.get("eventSlug")
                        or data.get("groupItemTitle")
                        or data.get("event", {}).get("slug", "")
                        if isinstance(data.get("event"), dict)
                        else data.get("event", "")
                    )
                    if event_slug:
                        self._event_cache[condition_id] = event_slug
                        return event_slug
        except Exception as e:
            logger.debug("Gamma API lookup failed for %s: %s", condition_id[:20], e)

        self._event_cache_misses[condition_id] = time.time()
        return None

    def _solo_trade(
        self, signal: WhaleSignal, level: str, multiplier: float,
    ) -> Optional[TradeOpportunity]:
        """Generate a solo trade opportunity for a high-tier whale."""
        market = signal.condition_id or signal.market_id
        now = time.time()

        # Dedup check
        dedup_key = f"{level}:{market}"
        if dedup_key in self._recent_opportunities:
            if now - self._recent_opportunities[dedup_key] < self._dedup_window:
                return None

        opportunity = TradeOpportunity(
            market_id=signal.market_id,
            condition_id=signal.condition_id,
            token_id=signal.token_id,
            direction=signal.direction,
            consensus_pct=1.0,
            n_whales=1,
            avg_whale_entry=signal.entry_price,
            total_whale_size=signal.size_usd,
            whale_aliases=[signal.alias],
            tier_weighted_score=3.0 if signal.tier == 1 else 2.0,
            is_fast_track=False,
            consensus_level=level,
            position_multiplier=multiplier,
        )

        self._recent_opportunities[dedup_key] = now
        self._opportunities_generated += 1
        self.lock_market(market)  # Prevent duplicate entries on same market

        logger.info(
            "%s: %s %s $%.0f by %s (tier %d, %.0f%% size)",
            level, signal.direction, market[:30], signal.size_usd,
            signal.alias, signal.tier, multiplier * 100,
        )
        return opportunity

    def _fast_track(self, signal: WhaleSignal) -> Optional[TradeOpportunity]:
        """Tier 1 fast-track: bypass consensus for unusually large single-whale trades."""
        market = signal.condition_id or signal.market_id
        now = time.time()

        # Cooldown + existing position checks
        if self.is_on_cooldown(signal.wallet, market):
            logger.info("Suppressed: fast-track from %s on %s (cooldown)", signal.alias, market[:16])
            return None
        if self.has_open_position_on_market(market):
            logger.info("Suppressed: fast-track from %s — already have position on %s", signal.alias, market[:16])
            return None

        if market in self._recent_opportunities:
            if now - self._recent_opportunities[market] < self._dedup_window:
                return None

        opportunity = TradeOpportunity(
            market_id=signal.market_id,
            condition_id=signal.condition_id,
            token_id=signal.token_id,
            direction=signal.direction,
            consensus_pct=1.0,
            n_whales=1,
            avg_whale_entry=signal.entry_price,
            total_whale_size=signal.size_usd,
            whale_aliases=[signal.alias],
            tier_weighted_score=3.0,
            is_fast_track=True,
            consensus_level="FAST_TRACK",
            position_multiplier=self.config.FAST_TRACK_POSITION_MULT,
        )

        self._recent_opportunities[market] = now
        self._opportunities_generated += 1
        self.lock_market(market)  # Prevent duplicate entries on same market

        logger.info(
            "FAST-TRACK: %s %s $%.0f by %s (Tier 1, 4x avg bet bypass)",
            signal.direction, market[:30], signal.size_usd, signal.alias,
        )
        return opportunity

    def _process_exit(self, signal: WhaleSignal) -> Optional[ExitSignal]:
        """Track whale exits and emit exit signal when significant."""
        market = signal.condition_id or signal.market_id
        now = time.time()

        if market not in self._exit_signals:
            self._exit_signals[market] = []
        self._exit_signals[market].append(signal)

        self._exit_signals[market] = [
            s for s in self._exit_signals[market]
            if now - s.timestamp < self.config.SIGNAL_WINDOW_SECONDS
        ]

        exits = self._exit_signals[market]
        unique_exiting = {s.wallet for s in exits}

        if len(unique_exiting) >= 2 or (signal.tier == 1 and signal.size_usd >= 25_000):
            exit_signal = ExitSignal(
                market_id=signal.market_id,
                condition_id=signal.condition_id,
                token_id=signal.token_id,
                direction=signal.direction,
                whale_aliases=[s.alias for s in exits],
                total_exit_size=sum(s.size_usd for s in exits),
            )
            self._exit_signals[market] = []

            logger.info(
                "EXIT SIGNAL: %d whales exiting %s (%s) — total $%.0f",
                len(unique_exiting), market[:30],
                ", ".join(exit_signal.whale_aliases), exit_signal.total_exit_size,
            )
            return exit_signal

        return None

    @staticmethod
    def _calc_tier_score(signals: list[WhaleSignal]) -> float:
        """Weight signals by tier: Tier 1 = 3x, Tier 2 = 2x, Tier 3 = 1x."""
        weights = {1: 3.0, 2: 2.0, 3: 1.0}
        if not signals:
            return 0.0
        return sum(weights.get(s.tier, 1.0) for s in signals) / len(signals)

    def cleanup_stale(self) -> None:
        """Remove stale pending signals (called periodically)."""
        now = time.time()
        cutoff = self.config.SIGNAL_WINDOW_SECONDS * 2

        for market in list(self._pending.keys()):
            self._pending[market] = [
                s for s in self._pending[market] if now - s.timestamp < cutoff
            ]
            if not self._pending[market]:
                del self._pending[market]

        for market in list(self._exit_signals.keys()):
            self._exit_signals[market] = [
                s for s in self._exit_signals[market] if now - s.timestamp < cutoff
            ]
            if not self._exit_signals[market]:
                del self._exit_signals[market]

        for key in list(self._recent_opportunities.keys()):
            if now - self._recent_opportunities[key] > self._dedup_window * 5:
                del self._recent_opportunities[key]

        # Clean up expired signal cooldowns
        for key in list(self._signal_cooldowns.keys()):
            if now >= self._signal_cooldowns[key]:
                del self._signal_cooldowns[key]

    async def shutdown(self) -> None:
        """Close HTTP session."""
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()

    def get_stats(self) -> dict:
        return {
            "signals_processed": self._signals_processed,
            "opportunities_generated": self._opportunities_generated,
            "pending_markets": len(self._pending),
            "pending_signals": sum(len(v) for v in self._pending.values()),
            "event_cache_size": len(self._event_cache),
        }
