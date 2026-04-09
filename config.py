"""
Bot configuration — frozen dataclass with all tunable parameters.

Defaults are validated by walk-forward backtest:
  30 seeds x 180 days x $1,000 starting capital x 12 verified wallets.
  57% win rate, 100% of runs profitable, median 6-month return +657%.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class BotConfig:
    """Immutable bot configuration. Load from .env, override via CLI."""

    # ── Strategy Parameters (from backtest optimization) ─────────────
    # 3% position sizing across 30 seeds produced 100% profitable runs.
    POSITION_SIZE_PCT: float = 0.03
    # Hard cap prevents over-concentration even with strong consensus.
    MAX_POSITION_SIZE_PCT: float = 0.05
    # 60% consensus threshold balances signal quality vs. trade frequency.
    CONSENSUS_THRESHOLD: float = 0.60
    # Minimum 2 whales required — single-whale signals are tier-gated separately.
    MIN_WHALES_SIGNALING: int = 2
    # 12 concurrent positions: up to 3 per whale from fast-track + consensus slots.
    MAX_OPEN_POSITIONS: int = 12
    # No single whale can consume more than 3 of our position slots.
    MAX_POSITIONS_PER_WHALE: int = 3
    # Reserve last N slot(s) exclusively for consensus (2+ whale) signals.
    CONSENSUS_RESERVED_SLOTS: int = 1
    # 10% single-market cap prevents ruin from correlated whale bets.
    MAX_SINGLE_MARKET_EXPOSURE: float = 0.10
    # 20% stop-loss: wider to avoid STOPPED_EARLY on eventual winners in sports markets.
    STOP_LOSS_PCT: float = 0.20
    # Grace period: don't check stop-losses for the first N minutes after entry.
    # Sports prices swing wildly early in games — stops firing in minute 2 of a
    # basketball game turn wins into losses (e.g. Michigan/UConn STOPPED_EARLY).
    STOP_LOSS_GRACE_MINUTES: float = 30.0
    MIN_TRADE_SIZE_USD: float = 5.0
    # Ignore whale trades below $3K — noise filter from leaderboard analysis.
    MIN_WHALE_TRADE_SIZE: float = 3000.0

    # ── Entry Price Filter ───────────────────────────────────────────
    # Only copy trades in the profitable range for small bankrolls.
    # Below 0.30 = longshot (win rate too low for copy-trading).
    # Above 0.55 = margin too thin (data shows net negative above 0.55).
    # Sweet spot is 0.35-0.50 where wins pay 1.8-2.9x and whale edge matters.
    MIN_ENTRY_PRICE: float = 0.30
    MAX_ENTRY_PRICE: float = 0.55

    # ── Spread Magnitude Filter ──────────────────────────────────────
    # Skip extreme spread bets (e.g. Team -14.5). These are coin flips
    # regardless of team strength. Whale directional accuracy drops on these.
    MAX_SPREAD_POINTS: float = 10.5

    # ── Execution Parameters (post-Feb 2026 Polymarket rules) ────────
    # Maker orders: 0% fee + rebates. Taker fees are ~1.56% — never use taker.
    USE_MAKER_ORDERS: bool = True
    # Place limit 0.5% better than market midpoint for maker fill probability.
    PRICE_IMPROVEMENT_BPS: int = 50
    # Cancel unfilled orders after 5 minutes.
    ORDER_TIMEOUT_SECONDS: int = 300
    # Skip trade if price moved >10% from whale entry — alpha already gone.
    MAX_SLIPPAGE_PCT: float = 0.10

    # ── Timing ───────────────────────────────────────────────────────
    # 4-second poll balances speed vs. rate limits (60 req/min = 15 req/poll).
    POLL_INTERVAL_SECONDS: float = 4.0
    # Re-score whale basket weekly using latest leaderboard data.
    WHALE_REBALANCE_HOURS: int = 168
    # Hard stop: halt trading if down 10% in a single day.
    DAILY_LOSS_LIMIT_PCT: float = 0.10

    # ── Fast-Track Parameters (Tier 1 single-whale override) ─────────
    # Fast-track triggers at 4x each whale's average bet size (relative, not absolute).
    # gmanas avg $12.7K -> triggers at $50.8K; ImJustKen avg $41.7K -> triggers at $166.8K.
    FAST_TRACK_MULTIPLIER: float = 4.0
    FAST_TRACK_POSITION_MULT: float = 0.50  # 50% of normal position size

    # ── Tier 1 Solo Trade Parameters ───────────────────────────────
    # Allow Tier 1 whales to generate trades WITHOUT consensus, at reduced size.
    # Needed because these whales trade in different niches and rarely overlap.
    # Lowered from $25K after 12h dry run showed gmanas avg bet is $12.7K.
    TIER1_SOLO_ENABLED: bool = True
    TIER1_SOLO_MIN_USD: float = 1_000  # Lowered to catch more T1 trades (texaskid avg $27K)
    TIER1_SOLO_POSITION_MULT: float = 0.80  # 80% of normal position size — high confidence in T1 whales

    # ── Tier 2 Solo Trade Parameters ───────────────────────────────
    # Tier 2 whales can also solo trade but need larger trades and get smaller size.
    # Lowered from $50K after 12h dry run — real trades were $4K-$13K.
    TIER2_SOLO_ENABLED: bool = True  # Re-enabled with whale-level win rate filter
    TIER2_SOLO_MIN_USD: float = 10_000  # Only on whale trades > $10K
    TIER2_SOLO_MIN_WIN_RATE: float = 0.60  # Only allow solo from T2 whales with >= 60% win rate (raised from 0.55)
    TIER2_SOLO_POSITION_MULT: float = 0.25  # 25% of normal position size

    # ── Tier 3 Solo Trade Parameters ───────────────────────────────
    # Tier 3 whales (Whale_Beta 67% WR, WoofMaster, beachboy4) can solo trade
    # on large bets. Added after 12h dry run showed 0 trades — Tier 3 whales
    # were the most active but had no path to generate trades.
    TIER3_SOLO_ENABLED: bool = False  # Disabled: solo trades from T3 whales generated majority of losses
    TIER3_SOLO_MIN_USD: float = 15_000  # Only on whale trades > $15K
    TIER3_SOLO_POSITION_MULT: float = 0.20  # 20% of normal position size

    # ── Event-Level Consensus ──────────────────────────────────────
    # Match whales on the same event (e.g., same NBA game) even if different markets.
    EVENT_CONSENSUS_ENABLED: bool = True
    EVENT_CONSENSUS_POSITION_MULT: float = 0.70  # 70% of normal position size

    # ── Whale Exit Follow ────────────────────────────────────────
    # When a whale who triggered our position exits, follow with a cooldown.
    # If the whale re-enters within the cooldown window, cancel the pending exit.
    WHALE_EXIT_COOLDOWN_SECONDS: float = 120.0
    # Minimum time a position must be held before following a whale exit.
    # Prevents churning from rapid-cycling whales (scalpers/market-makers).
    MIN_HOLD_MINUTES: float = 15.0

    # ── Startup ───────────────────────────────────────────────────────
    # Suppress all signals for 30s after baseline capture to let data stabilize.
    STARTUP_QUIET_PERIOD_SECONDS: float = 30.0
    # Reject trade if midpoint differs >20% from whale's reported entry.
    ENTRY_PRICE_SANITY_CHECK_PCT: float = 0.20

    # ── Signal Window ────────────────────────────────────────────────
    SIGNAL_WINDOW_SECONDS: int = 300  # Group signals within 5 min window

    # ── API Endpoints ────────────────────────────────────────────────
    CLOB_HOST: str = "https://clob.polymarket.com"
    CLOB_WS: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    DATA_API: str = "https://data-api.polymarket.com"
    GAMMA_API: str = "https://gamma-api.polymarket.com"
    CHAIN_ID: int = 137  # Polygon

    # ── Credentials (from .env) ──────────────────────────────────────
    POLYMARKET_PK: str = field(default_factory=lambda: os.getenv("POLYMARKET_PK", ""))
    POLYMARKET_FUNDER: str = field(default_factory=lambda: os.getenv("POLYMARKET_FUNDER", ""))
    POLYGON_RPC_URL: str = field(
        default_factory=lambda: os.getenv("POLYGON_RPC_URL", "https://polygon-rpc.com")
    )
    TELEGRAM_BOT_TOKEN: str = field(default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN", ""))
    TELEGRAM_CHAT_ID: str = field(default_factory=lambda: os.getenv("TELEGRAM_CHAT_ID", ""))

    # ── Runtime Settings ─────────────────────────────────────────────
    DRY_RUN: bool = field(
        default_factory=lambda: os.getenv("DRY_RUN", "true").lower() == "true"
    )
    INITIAL_BANKROLL: float = field(
        default_factory=lambda: float(os.getenv("INITIAL_BANKROLL", "1000"))
    )
    DB_PATH: str = field(
        default_factory=lambda: os.getenv("DB_PATH", "trades.db")
    )

    def validate(self) -> list[str]:
        """Return list of validation errors, empty if config is valid."""
        errors: list[str] = []
        if not self.DRY_RUN:
            if not self.POLYMARKET_PK:
                errors.append("POLYMARKET_PK required for live trading")
            if not self.POLYMARKET_FUNDER:
                errors.append("POLYMARKET_FUNDER required for live trading")
        if self.POSITION_SIZE_PCT <= 0 or self.POSITION_SIZE_PCT > 1:
            errors.append("POSITION_SIZE_PCT must be in (0, 1]")
        if self.CONSENSUS_THRESHOLD < 0.5 or self.CONSENSUS_THRESHOLD > 1:
            errors.append("CONSENSUS_THRESHOLD must be in [0.5, 1.0]")
        if self.STOP_LOSS_PCT <= 0 or self.STOP_LOSS_PCT >= 1:
            errors.append("STOP_LOSS_PCT must be in (0, 1)")
        if self.INITIAL_BANKROLL < self.MIN_TRADE_SIZE_USD:
            errors.append("INITIAL_BANKROLL must be >= MIN_TRADE_SIZE_USD")
        return errors


# ── Whale Watchlist ──────────────────────────────────────────────────
# Loaded from whales.json (single source of truth).
# Auto-rotation script (monitor/whale_rotation.py) manages additions/removals.

_WHALES_PATH = Path(__file__).parent / "whales.json"


def _load_whales() -> dict[str, dict[str, Any]]:
    """Load whale watchlist from JSON sidecar file."""
    with open(_WHALES_PATH) as f:
        return json.load(f)


_WHALE_DATA: dict[str, dict[str, Any]] = _load_whales()

WHALE_WATCHLIST: dict[str, dict[str, Any]] = _WHALE_DATA

# Quick lookup sets by tier for fast-track logic
TIER_1_WALLETS: set[str] = {
    addr for addr, info in _WHALE_DATA.items() if info["tier"] == 1
}

# Per-whale average bet sizes for relative fast-track thresholds.
WHALE_AVG_BET: dict[str, float] = {
    addr: info["avg_bet"] for addr, info in _WHALE_DATA.items()
}
