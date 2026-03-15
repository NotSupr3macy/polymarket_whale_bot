"""
Risk manager — Kelly Criterion position sizing, drawdown circuit breaker,
stop-losses, and exposure limits.

Upgrade 1: Fractional Kelly position sizing.
  f* = (p*b - q) / b  where p=win_rate, q=1-p, b=payout_ratio
  Uses quarter-Kelly (0.25 * f*) for safety + slippage degradation.
  Consensus level boosts allowed: EXACT_MARKET 1.2x, FAST_TRACK 1.0x, solo 0.8x.

Upgrade 2: Drawdown circuit breaker.
  Tracks peak_bankroll. If bankroll drops >8% from peak, halts all trading
  for 24 hours. Protects against cascading losses.

Original backtest context still applies for stop-loss math:
  At 57% win rate with Kelly sizing and 15% stop-loss:
  Binary market: win pays ($1.00 - entry) per share, loss = stop_loss * entry per share.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

from config import BotConfig, WHALE_WATCHLIST
from signal_engine import TradeOpportunity

logger = logging.getLogger(__name__)

# ── Whale Win Rates (from verified stats + leaderboard) ────────────
# Used for Kelly Criterion sizing. Sources: predicts.guru, Polymarket leaderboard,
# Phemex Top 10 Report. Addresses with unknown win rate get DEFAULT_WIN_RATE.

WHALE_WIN_RATES: dict[str, float] = {
    # Tier 1
    "0x9d84ce0306f8551e02efef1680475fc0f1dc1344": 0.633,  # ImJustKen
    "0xe90bec87d9ef430f27f9dcfe72c34b76967d5da2": 0.816,  # gmanas
    "0x019782cab5d844f02bafb71f512758be78579f3c": 0.70,   # majorexploiter (est)
    # Tier 2
    "0x02227b8f5a9636e895607edd3185ed6ee5598ff7": 0.65,   # HorizonSplendidView (est)
    "0x2a2c53bd278c04da9962fcf96490e17f3dfb9bc1": 0.58,   # 0x2a2C...9Bc1 (est, downgraded: churner)
    "0xb90494d9a5d8f71f1930b2aa4b599f95c344c255": 0.64,   # MinorKey4 (est)
    "0x07b8e44b90cc3e91b8d5fe60ea810d2534638e25": 0.61,   # joosangyoo (est)
    "0xdc876e6873772d38716fda7f2452a78d426d7ab6": 0.60,   # 432614799197 (est)
    # Tier 3
    "0xc2e7800b5af46e6093872b177b7a5e7f0563be51": 0.63,   # beachboy4 (est)
    "0x916f7165c2c836aba22edb6453cdbb5f3ea253ba": 0.62,   # WoofMaster (est)
    "0xd218e474776403a330142299f7796e8ba32eb5c9": 0.67,   # Whale_Beta
    "0x39932ca2b7a1b8ab6cbf0b8f7419261b950ccded": 0.60,   # Andromeda1 (est)
}

DEFAULT_WIN_RATE: float = 0.57
KELLY_FRACTION: float = 0.25  # Quarter-Kelly for safety
SLIPPAGE_DEGRADATION: float = 0.05  # 5% further reduction for slippage/timing

# ── Circuit Breaker Parameters ─────────────────────────────────────
MAX_DRAWDOWN_PCT: float = 0.08  # 8% from peak triggers halt
DRAWDOWN_COOLDOWN_HOURS: float = 24.0  # 24h cooldown before trading resumes

# ── Consensus Level Boost Multipliers ──────────────────────────────
CONSENSUS_KELLY_BOOST: dict[str, float] = {
    "EXACT_MARKET": 1.2,
    "EVENT": 1.1,
    "FAST_TRACK": 1.0,
    "TIER1_SOLO": 0.8,
    "TIER2_SOLO": 0.7,
    "TIER3_SOLO": 0.6,
}


# ── Stop-Loss Resolution Threshold ────────────────────────────────
STOP_LOSS_RESOLUTION_THRESHOLD_HOURS: float = 24.0  # Disable stops for markets resolving within 24h


@dataclass
class OpenPosition:
    """Tracks an open position for risk management."""

    trade_id: str
    market_id: str
    condition_id: str
    token_id: str
    direction: str
    entry_price: float
    shares: float
    size_usd: float
    entry_time: float
    stop_price: float  # Calculated at entry: entry_price * (1 - STOP_LOSS_PCT)
    whale_aliases: list[str] = field(default_factory=list)  # Source whale(s)
    stop_loss_enabled: bool = True  # False for short-duration sports markets
    hours_to_resolution: float | None = None  # Hours until market resolves

    @property
    def is_expired(self) -> bool:
        """Positions older than 30 days are flagged for review."""
        return (time.time() - self.entry_time) > 30 * 86400


class RiskManager:
    """
    Enforces position sizing, exposure limits, stop-losses, and circuit breaker.

    Upgrade 1: Kelly Criterion position sizing (replaces flat 3%).
    Upgrade 2: Drawdown circuit breaker (8% from peak = halt).
    """

    def __init__(self, config: BotConfig, initial_bankroll: float):
        self.config = config
        self.bankroll = initial_bankroll
        self.initial_bankroll = initial_bankroll
        self.daily_pnl = 0.0
        self.daily_reset_time = time.time()
        self.open_positions: list[OpenPosition] = []
        self.total_trades = 0
        self.total_pnl = 0.0

        # ── Circuit breaker state ──
        self.peak_bankroll = initial_bankroll
        self.circuit_breaker_active = False
        self.circuit_breaker_triggered_at: float = 0.0

    # ══════════════════════════════════════════════════════════════
    #  UPGRADE 1: KELLY CRITERION POSITION SIZING
    # ══════════════════════════════════════════════════════════════

    def _get_blended_win_rate(self, whale_wallets: list[str]) -> float:
        """
        Get the blended win rate across all whales in a signal.
        For multi-whale signals, average their win rates.
        Falls back to DEFAULT_WIN_RATE for unknown wallets.
        """
        if not whale_wallets:
            return DEFAULT_WIN_RATE

        rates = []
        for wallet in whale_wallets:
            rate = WHALE_WIN_RATES.get(wallet, DEFAULT_WIN_RATE)
            rates.append(rate)

        return sum(rates) / len(rates)

    def _resolve_whale_wallets(self, whale_aliases: list[str]) -> list[str]:
        """Map whale alias names back to wallet addresses."""
        wallets = []
        for alias in whale_aliases:
            for wallet, info in WHALE_WATCHLIST.items():
                if info["alias"] == alias:
                    wallets.append(wallet)
                    break
        return wallets

    def calculate_kelly_size(
        self,
        win_rate: float,
        entry_price: float,
        consensus_level: str,
        stop_loss_enabled: bool = True,
    ) -> float:
        """
        Kelly Criterion for binary prediction markets.

        In a binary market at entry_price p:
          - Win: payout = (1.00 - p) per share  (market resolves to $1)
          - Loss (with stop): loss = stop_loss_pct * p per share
          - Loss (hold to resolution): loss = p per share (full entry price)
          - b (payout ratio) = win_amount / loss_amount

        Kelly fraction: f* = (p_win * b - p_lose) / b
        We use quarter-Kelly (0.25 * f*) minus slippage degradation.
        """
        p = win_rate
        q = 1.0 - p

        # Binary market payoffs
        win_amount = 1.0 - entry_price  # Profit per share on win
        if stop_loss_enabled:
            loss_amount = self.config.STOP_LOSS_PCT * entry_price  # Loss per share on stop
        else:
            loss_amount = entry_price  # Full loss on resolution (no stop)

        if loss_amount <= 0 or win_amount <= 0:
            return 0.0

        b = win_amount / loss_amount  # Payout ratio

        # Kelly formula
        f_star = (p * b - q) / b

        if f_star <= 0:
            # Negative edge — don't trade
            logger.debug(
                "Negative Kelly edge: p=%.3f, b=%.2f, f*=%.4f — skipping",
                p, b, f_star,
            )
            return 0.0

        # Quarter-Kelly with slippage degradation
        fraction = f_star * KELLY_FRACTION * (1.0 - SLIPPAGE_DEGRADATION)

        # Apply consensus-level boost
        boost = CONSENSUS_KELLY_BOOST.get(consensus_level, 0.8)
        fraction *= boost

        # Hard cap at MAX_POSITION_SIZE_PCT
        fraction = min(fraction, self.config.MAX_POSITION_SIZE_PCT)

        return fraction

    def calculate_position_size(self, opportunity: TradeOpportunity) -> float:
        """
        Calculate position size using Kelly Criterion.

        Replaces the flat 3% sizing with:
          fraction = quarter_kelly(blended_win_rate, entry_price) * consensus_boost
          size = bankroll * fraction
          Capped at MAX_POSITION_SIZE_PCT of bankroll.
        """
        # Resolve whale wallets from aliases
        whale_wallets = self._resolve_whale_wallets(opportunity.whale_aliases)

        # Get blended win rate across signal whales
        win_rate = self._get_blended_win_rate(whale_wallets)

        # Get consensus level from the opportunity
        consensus_level = getattr(opportunity, "consensus_level", "EXACT_MARKET")

        # Determine if stop-loss is enabled for this market
        stop_loss_enabled = getattr(opportunity, "stop_loss_enabled", True)

        # Calculate Kelly fraction
        kelly_fraction = self.calculate_kelly_size(
            win_rate=win_rate,
            entry_price=opportunity.avg_whale_entry,
            consensus_level=consensus_level,
            stop_loss_enabled=stop_loss_enabled,
        )

        if kelly_fraction <= 0:
            return 0.0

        # Calculate dollar size
        size = self.bankroll * kelly_fraction

        # Apply position_multiplier from signal engine (solo trades, event consensus)
        if hasattr(opportunity, 'position_multiplier') and opportunity.position_multiplier < 1.0:
            size *= opportunity.position_multiplier
        elif opportunity.is_fast_track:
            size *= self.config.FAST_TRACK_POSITION_MULT

        # Hard cap at MAX_POSITION_SIZE_PCT
        size = min(size, self.bankroll * self.config.MAX_POSITION_SIZE_PCT)

        # Check single-market exposure limit — cap to remaining room
        existing_exposure = self.get_market_exposure(opportunity.condition_id or opportunity.market_id)
        max_market_exposure = self.bankroll * self.config.MAX_SINGLE_MARKET_EXPOSURE
        remaining_room = max_market_exposure - existing_exposure
        if remaining_room <= self.config.MIN_TRADE_SIZE_USD:
            logger.info(
                "No room left on %s ($%.0f of $%.0f used)",
                (opportunity.condition_id or opportunity.market_id)[:16],
                existing_exposure, max_market_exposure,
            )
            return 0.0
        size = min(size, remaining_room)

        # Minimum trade size check
        if size < self.config.MIN_TRADE_SIZE_USD:
            return 0.0

        logger.info(
            "Kelly sizing: $%.2f (kelly_f=%.4f, win_rate=%.3f, entry=%.3f, "
            "consensus=%s, boost=%.1fx, mult=%.2f)",
            size,
            kelly_fraction,
            win_rate,
            opportunity.avg_whale_entry,
            consensus_level,
            CONSENSUS_KELLY_BOOST.get(consensus_level, 0.8),
            getattr(opportunity, 'position_multiplier', 1.0),
        )
        return round(size, 2)

    # ══════════════════════════════════════════════════════════════
    #  UPGRADE 2: DRAWDOWN CIRCUIT BREAKER
    # ══════════════════════════════════════════════════════════════

    def _update_peak_bankroll(self) -> None:
        """Track the high-water mark of bankroll for drawdown calculation."""
        if self.bankroll > self.peak_bankroll:
            self.peak_bankroll = self.bankroll

    def _check_circuit_breaker(self) -> tuple[bool, str]:
        """
        Check if circuit breaker should be active.

        Triggers when bankroll drops >MAX_DRAWDOWN_PCT from peak.
        Stays active for DRAWDOWN_COOLDOWN_HOURS.
        """
        # Check if cooldown has expired
        if self.circuit_breaker_active:
            elapsed = time.time() - self.circuit_breaker_triggered_at
            cooldown_seconds = DRAWDOWN_COOLDOWN_HOURS * 3600
            if elapsed >= cooldown_seconds:
                # Cooldown expired — deactivate
                self.circuit_breaker_active = False
                self.circuit_breaker_triggered_at = 0.0
                # Reset peak to current bankroll so we don't immediately re-trigger
                self.peak_bankroll = self.bankroll
                logger.info(
                    "CIRCUIT BREAKER: Cooldown expired after %.1f hours — trading resumed. "
                    "Peak reset to $%.2f",
                    elapsed / 3600, self.bankroll,
                )
                return False, "OK"
            else:
                remaining = (cooldown_seconds - elapsed) / 3600
                return True, (
                    f"Circuit breaker active — {remaining:.1f}h remaining. "
                    f"Drawdown from peak ${self.peak_bankroll:.2f} to ${self.bankroll:.2f}"
                )

        # Check for new drawdown trigger
        if self.peak_bankroll > 0:
            drawdown = (self.peak_bankroll - self.bankroll) / self.peak_bankroll
            if drawdown >= MAX_DRAWDOWN_PCT:
                self.circuit_breaker_active = True
                self.circuit_breaker_triggered_at = time.time()
                logger.warning(
                    "CIRCUIT BREAKER TRIGGERED: Drawdown %.1f%% (peak=$%.2f, current=$%.2f). "
                    "Trading halted for %d hours.",
                    drawdown * 100, self.peak_bankroll, self.bankroll,
                    int(DRAWDOWN_COOLDOWN_HOURS),
                )
                return True, (
                    f"Circuit breaker TRIGGERED — {drawdown:.1%} drawdown from peak "
                    f"${self.peak_bankroll:.2f}. Halted for {DRAWDOWN_COOLDOWN_HOURS:.0f}h."
                )

        return False, "OK"

    def can_trade(self, opportunity: TradeOpportunity | None = None) -> tuple[bool, str]:
        """
        Check if a new trade is allowed under current risk limits.

        Enforces:
          - Circuit breaker (drawdown protection)
          - Daily loss limit
          - Max open positions (12)
          - Per-whale position limit (max 3 per single whale source)
          - Consensus reservation (last slot reserved for multi-whale signals)
          - Market-level exposure (uses market_id, not token_id)
          - Minimum bankroll
        """
        self._maybe_reset_daily_pnl()

        # ── CIRCUIT BREAKER (Upgrade 2) ──
        breaker_active, breaker_msg = self._check_circuit_breaker()
        if breaker_active:
            return False, breaker_msg

        # Daily loss limit
        daily_limit = self.config.DAILY_LOSS_LIMIT_PCT * self.bankroll
        if self.daily_pnl < -daily_limit:
            return False, f"Daily loss limit hit (${self.daily_pnl:.2f} < -${daily_limit:.2f})"

        # Max open positions
        if len(self.open_positions) >= self.config.MAX_OPEN_POSITIONS:
            return False, f"Max positions reached ({len(self.open_positions)}/{self.config.MAX_OPEN_POSITIONS})"

        # Minimum bankroll
        if self.bankroll < self.config.MIN_TRADE_SIZE_USD * 3:
            return False, f"Bankroll too low (${self.bankroll:.2f})"

        if opportunity is not None:
            # Per-whale position limit: no single whale consumes >MAX_POSITIONS_PER_WHALE slots
            if opportunity.is_fast_track and len(opportunity.whale_aliases) == 1:
                whale_alias = opportunity.whale_aliases[0]
                whale_positions = sum(
                    1 for p in self.open_positions
                    if whale_alias in p.whale_aliases
                )
                if whale_positions >= self.config.MAX_POSITIONS_PER_WHALE:
                    return False, f"Per-whale limit for {whale_alias} ({whale_positions}/{self.config.MAX_POSITIONS_PER_WHALE})"

            # Consensus reservation: reserve last N slot(s) for multi-whale consensus
            slots_remaining = self.config.MAX_OPEN_POSITIONS - len(self.open_positions)
            if slots_remaining <= self.config.CONSENSUS_RESERVED_SLOTS:
                if opportunity.n_whales < 2:
                    return False, "Last slot(s) reserved for consensus trades"

            # Market-level exposure: group by condition_id (YES and NO are same market)
            market_key = opportunity.condition_id or opportunity.market_id
            existing_market_exposure = self.get_market_exposure(market_key)
            max_market = self.bankroll * self.config.MAX_SINGLE_MARKET_EXPOSURE
            remaining = max_market - existing_market_exposure
            if remaining <= self.config.MIN_TRADE_SIZE_USD:
                return False, f"Market exposure limit for {market_key[:20]} (${existing_market_exposure:.0f}/${max_market:.0f})"

        return True, "OK"

    def register_entry(self, position: OpenPosition) -> None:
        """Register a new open position."""
        self.open_positions.append(position)
        self.total_trades += 1
        self._update_peak_bankroll()

        if position.stop_loss_enabled:
            logger.info(
                "Position opened: %s %s $%.2f @ %.4f (stop: %.4f) | %d open",
                position.direction,
                position.market_id[:20],
                position.size_usd,
                position.entry_price,
                position.stop_price,
                len(self.open_positions),
            )
        else:
            hours_str = (
                f"{position.hours_to_resolution:.0f}h"
                if position.hours_to_resolution is not None
                else "sports"
            )
            logger.info(
                "Position opened: %s %s $%.2f @ %.4f (HOLD TO RESOLUTION — %s) | %d open",
                position.direction,
                position.market_id[:20],
                position.size_usd,
                position.entry_price,
                hours_str,
                len(self.open_positions),
            )

    def register_exit(self, trade_id: str, exit_price: float, pnl: float) -> None:
        """Remove position and update PnL tracking."""
        self.open_positions = [p for p in self.open_positions if p.trade_id != trade_id]
        self.daily_pnl += pnl
        self.total_pnl += pnl
        self.bankroll += pnl
        self._update_peak_bankroll()
        logger.info(
            "Position closed: %s PnL=$%.2f | Bankroll=$%.2f | Daily=$%.2f | Peak=$%.2f | %d open",
            trade_id[:12],
            pnl,
            self.bankroll,
            self.daily_pnl,
            self.peak_bankroll,
            len(self.open_positions),
        )

    def check_stop_losses(self, current_prices: dict[str, float]) -> list[OpenPosition]:
        """
        Check all open positions against their stop prices.
        Skips positions in short-duration markets (stop_loss_enabled=False).

        Args:
            current_prices: {token_id: current_price}

        Returns:
            List of positions that should be stopped out.
        """
        stopped = []
        for pos in self.open_positions:
            # Skip stop-loss for short-duration sports markets
            if not pos.stop_loss_enabled:
                continue

            price = current_prices.get(pos.token_id)
            if price is None:
                continue

            if price <= pos.stop_price:
                logger.warning(
                    "STOP-LOSS triggered: %s @ %.4f <= stop %.4f (entry %.4f)",
                    pos.market_id[:20],
                    price,
                    pos.stop_price,
                    pos.entry_price,
                )
                stopped.append(pos)

        return stopped

    def calculate_stop_price(self, entry_price: float) -> float:
        """Calculate stop-loss price: entry * (1 - STOP_LOSS_PCT)."""
        return round(entry_price * (1.0 - self.config.STOP_LOSS_PCT), 4)

    def get_market_exposure(self, market_id: str) -> float:
        """Total dollars exposed to a specific market across ALL positions."""
        return sum(
            pos.size_usd
            for pos in self.open_positions
            if pos.market_id == market_id or pos.condition_id == market_id
        )

    def get_total_exposure(self) -> float:
        """Total USD value of all open positions."""
        return sum(p.size_usd for p in self.open_positions)

    def get_exposure_pct(self) -> float:
        """Total exposure as percentage of bankroll."""
        if self.bankroll <= 0:
            return 1.0
        return self.get_total_exposure() / self.bankroll

    def _maybe_reset_daily_pnl(self) -> None:
        """Reset daily PnL counter at midnight."""
        now = time.time()
        # Reset if more than 24 hours since last reset
        if now - self.daily_reset_time > 86400:
            logger.info("Daily PnL reset: $%.2f -> $0.00", self.daily_pnl)
            self.daily_pnl = 0.0
            self.daily_reset_time = now

    def load_positions(self, positions: list[OpenPosition]) -> None:
        """Restore open positions from journal on restart (idempotent restart)."""
        self.open_positions = positions
        logger.info("Restored %d open positions from journal", len(positions))

    def get_stats(self) -> dict:
        drawdown = 0.0
        if self.peak_bankroll > 0:
            drawdown = (self.peak_bankroll - self.bankroll) / self.peak_bankroll

        return {
            "bankroll": self.bankroll,
            "initial_bankroll": self.initial_bankroll,
            "total_return_pct": ((self.bankroll - self.initial_bankroll) / self.initial_bankroll) * 100 if self.initial_bankroll > 0 else 0,
            "daily_pnl": self.daily_pnl,
            "total_pnl": self.total_pnl,
            "total_trades": self.total_trades,
            "open_positions": len(self.open_positions),
            "total_exposure": self.get_total_exposure(),
            "exposure_pct": self.get_exposure_pct() * 100,
            "peak_bankroll": self.peak_bankroll,
            "current_drawdown_pct": drawdown * 100,
            "circuit_breaker_active": self.circuit_breaker_active,
        }
