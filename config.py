"""
Bot configuration — frozen dataclass with all tunable parameters.

Defaults are validated by walk-forward backtest:
  30 seeds x 180 days x $1,000 starting capital x 12 verified wallets.
  57% win rate, 100% of runs profitable, median 6-month return +657%.
"""

from __future__ import annotations

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
    # 15% stop-loss: EV per $1 = 0.57 * $0.82 - 0.43 * $0.40 = +$0.30.
    STOP_LOSS_PCT: float = 0.15
    MIN_TRADE_SIZE_USD: float = 5.0
    # Ignore whale trades below $3K — noise filter from leaderboard analysis.
    MIN_WHALE_TRADE_SIZE: float = 3000.0

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
    TIER1_SOLO_MIN_USD: float = 15_000  # Only on whale trades > $15K (high conviction only)
    TIER1_SOLO_POSITION_MULT: float = 0.40  # 40% of normal position size

    # ── Tier 2 Solo Trade Parameters ───────────────────────────────
    # Tier 2 whales can also solo trade but need larger trades and get smaller size.
    # Lowered from $50K after 12h dry run — real trades were $4K-$13K.
    TIER2_SOLO_ENABLED: bool = False  # Disabled: solo trades from T2 whales generated majority of losses
    TIER2_SOLO_MIN_USD: float = 10_000  # Only on whale trades > $10K
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
# Real verified addresses from polymarket.com/leaderboard (March 11, 2026),
# cross-referenced with Predicts.guru analytics and Phemex reports.

WHALE_WATCHLIST: dict[str, dict[str, Any]] = {
    # ═══ TIER 1: HIGHEST PRIORITY (massive volume + proven profitability) ═══
    "0x9d84ce0306f8551e02efef1680475fc0f1dc1344": {
        "alias": "ImJustKen",
        "tier": 1,
        "category": "multi",
        "verified_stats": {
            "all_time_pnl": 2_618_357,
            "win_rate": 0.633,
            "total_positions": 10_000,
            "volume_traded": 417_000_000,
            "portfolio_value": 759_000,
            "active_since": "December 2022",
        },
        "source": "Phemex Top 10 Report Jan 2026 + X/@phosphenq verification",
        "notes": "Trades like a sniper. 10K+ positions. Top 20 all-time.",
    },
    "0xe90bec87d9ef430f27f9dcfe72c34b76967d5da2": {
        "alias": "gmanas",
        "tier": 1,
        "category": "sports_multi",
        "verified_stats": {
            "all_time_pnl": 2_530_000,
            "win_rate": 0.816,
            "total_markets": 4_477,
            "volume_traded": 478_000_000,
            "avg_entry_price": 0.57,
            "avg_bet": 12_700,
            "best_trade": 111_000,
            "worst_trade": -42_400,
            "open_positions": 300,
            "portfolio_value": 823_000,
            "buy_sell_ratio": "499:1",
        },
        "source": "Predicts.guru full wallet analytics + Polymarket leaderboard March 2026",
        "notes": "81.6% win rate across 4,477 markets. The discipline benchmark.",
    },
    # ═══ TIER 2: HIGH PRIORITY (strong monthly performers) ═══
    "0x02227b8f5a9636e895607edd3185ed6ee5598ff7": {
        "alias": "HorizonSplendidView",
        "tier": 2,
        "category": "sports",
        "verified_stats": {
            "monthly_pnl": 1_787_559,
            "monthly_volume": 5_781_406,
            "biggest_win": 1_169_502,
            "biggest_win_market": "Man City vs Nottingham Forest",
        },
        "source": "Polymarket leaderboard #2 monthly, March 11, 2026",
    },
    # REMOVED: 0x2a2C...9Bc1 — 0W/3L, HF churner, consensus losses
    # REMOVED: MinorKey4 — 0W/6L, zero wins
    # REMOVED: joosangyoo — 1W/6L, poor performance
    "0xdc876e6873772d38716fda7f2452a78d426d7ab6": {
        "alias": "432614799197",
        "tier": 2,
        "category": "sports",
        "verified_stats": {
            "monthly_pnl": 693_454,
            "monthly_volume": 17_580_704,
        },
        "source": "Polymarket leaderboard #6 monthly, March 11, 2026",
    },
    "0xb45a797faa52b0fd8adc56d30382022b7b12192c": {
        "alias": "bcda",
        "tier": 2,
        "category": "sports",
        "solo_enabled": True,
        "verified_stats": {
            "monthly_pnl": 568_975,
            "monthly_volume": 3_154_673,
            "roi_on_volume": "18%",
        },
        "source": "Polymarket leaderboard #9 monthly, March 2026",
        "notes": "High efficiency — $568K profit on $3.1M volume. Selective bettor.",
    },
    "0x9cb990f1862568a63d8601efeebe0304225c32f2": {
        "alias": "jtwyslljy",
        "tier": 2,
        "category": "sports",
        "solo_enabled": True,
        "verified_stats": {
            "monthly_pnl": 517_807,
            "monthly_volume": 1_699_397,
            "roi_on_volume": "30%",
        },
        "source": "Polymarket leaderboard #10 monthly, March 2026",
        "notes": "Best ROI efficiency in the top 20. Very selective, low volume, high hit rate.",
    },
    "0x93abbc022ce98d6f45d4444b594791cc4b7a9723": {
        "alias": "gatorr",
        "tier": 2,
        "category": "sports",
        "solo_enabled": True,
        "verified_stats": {
            "monthly_pnl": 389_134,
            "monthly_volume": 2_021_224,
            "win_rate": 0.68,
        },
        "source": "Polymarket leaderboard #12 + ScanWhale verified 68% win rate",
        "notes": "68% verified win rate from ScanWhale. Sports specialist. Strong candidate.",
    },
    "0xc65ca4755436f82d8eb461e65781584b8cadea39": {
        "alias": "UAEVALORANTFAN",
        "tier": 2,
        "category": "esports",
        "solo_enabled": True,
        "verified_stats": {
            "monthly_pnl": 275_681,
            "monthly_volume": 737_909,
            "roi_on_volume": "37%",
        },
        "source": "Polymarket leaderboard #16 monthly, March 2026",
        "notes": "Highest ROI in top 20 (37%). Likely esports specialist — gives off-hours coverage.",
    },
    # ═══ TIER 3: SIGNAL DIVERSIFICATION ═══
    "0xc2e7800b5af46e6093872b177b7a5e7f0563be51": {
        "alias": "beachboy4",
        "tier": 3,
        "category": "sports",
        "verified_stats": {
            "monthly_pnl": 402_836,
            "monthly_volume": 822_553,
            "biggest_win_market": "Hawks vs Bucks",
        },
        "source": "Polymarket leaderboard #11 monthly, March 11, 2026",
        "notes": "High ROI relative to volume — selective bettor.",
    },
    "0x916f7165c2c836aba22edb6453cdbb5f3ea253ba": {
        "alias": "WoofMaster",
        "tier": 3,
        "category": "multi",
        "verified_stats": {
            "monthly_pnl": 571_305,
            "monthly_volume": 1_549_559,
        },
        "source": "Polymarket leaderboard #8 monthly, March 11, 2026",
    },
    "0xd218e474776403a330142299f7796e8ba32eb5c9": {
        "alias": "Whale_Beta",
        "tier": 3,
        "category": "multi",
        "verified_stats": {
            "all_time_pnl": 958_059,
            "30d_volume": 1_175_602,
            "win_rate": 0.67,
            "total_positions": 420,
        },
        "source": "Phemex Top 10 Wallets Report, January 2026",
    },
    "0x39932ca2b7a1b8ab6cbf0b8f7419261b950ccded": {
        "alias": "Andromeda1",
        "tier": 3,
        "category": "multi",
        "verified_stats": {
            "monthly_pnl": 219_807,
            "monthly_volume": 818_226,
        },
        "source": "Polymarket leaderboard #20 monthly, March 11, 2026",
    },
}

# Quick lookup sets by tier for fast-track logic
TIER_1_WALLETS: set[str] = {
    addr for addr, info in WHALE_WATCHLIST.items() if info["tier"] == 1
}

# Per-whale average bet sizes for relative fast-track thresholds.
# Sources: volume_traded / total_positions, or estimated from monthly data.
WHALE_AVG_BET: dict[str, float] = {
    # Tier 1
    "0x9d84ce0306f8551e02efef1680475fc0f1dc1344": 41_700,   # ImJustKen: $417M / 10K positions
    "0xe90bec87d9ef430f27f9dcfe72c34b76967d5da2": 12_700,   # gmanas: verified avg from predicts.guru
    # Tier 2
    "0x02227b8f5a9636e895607edd3185ed6ee5598ff7": 15_000,   # HorizonSplendidView
    # REMOVED: 0x2a2C, MinorKey4, joosangyoo (underperforming)
    "0xdc876e6873772d38716fda7f2452a78d426d7ab6": 18_000,   # 432614799197
    "0xb45a797faa52b0fd8adc56d30382022b7b12192c": 10_000,   # bcda
    "0x9cb990f1862568a63d8601efeebe0304225c32f2": 8_000,    # jtwyslljy
    "0x93abbc022ce98d6f45d4444b594791cc4b7a9723": 10_000,   # gatorr
    "0xc65ca4755436f82d8eb461e65781584b8cadea39": 6_000,    # UAEVALORANTFAN
    # Tier 3
    "0xc2e7800b5af46e6093872b177b7a5e7f0563be51": 8_000,    # beachboy4
    "0x916f7165c2c836aba22edb6453cdbb5f3ea253ba": 10_000,   # WoofMaster
    "0xd218e474776403a330142299f7796e8ba32eb5c9": 8_000,    # Whale_Beta
    "0x39932ca2b7a1b8ab6cbf0b8f7419261b950ccded": 6_000,    # Andromeda1
}
