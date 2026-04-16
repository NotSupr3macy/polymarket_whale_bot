"""
TheOnlyHuman VIP tracker — wraps the generic `whale_tracker.WhaleVIPTracker`
with TheOnlyHuman-specific config.

TheOnlyHuman is an elite NBA totals (O/U) specialist — 30d Gamma-resolved:
40W/25L on NBA (+$201K, +29% ROI on $694K stake). $1M+ all-time PnL. We
track NBA only; his soccer/other bets cost -$193K in 30d — not tailable.

Runs in its own tmux session alongside the bot and other whale trackers.

Usage:
    python3 monitor/theonlyhuman_tracker.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from whale_tracker import BetRule, PerformanceFilter, WhaleConfig, run_tracker  # noqa: E402


# TheOnlyHuman — Tier 1 NBA specialist (see whales.json).
# Avg bet ~$12K per market, $1.1M / 30d volume.
THEONLYHUMAN_CONFIG = WhaleConfig(
    wallet="0x6ade597c0e2b43c0bf3542cada8a5e330d73f5b0",
    alias="TheOnlyHuman",
    emoji="🏀",
    # His avg per-market spend is $12K with drip-feed fills. Gate at $500
    # to catch meaningful entries while skipping tail-pick dust fills
    # (he sometimes buys 4¢ longshots at $0.04 = $4 positions).
    min_position_usd=500.0,
    size_increase_alert_usd=5_000.0,
    poll_interval_sec=15,
    dual_write_legacy_texaskid_table=False,
    send_daily_report=False,
    # NBA-only solo alerts. In 30d, soccer cost -$85K (Liverpool) and
    # "other" cost -$108K (Crystal Palace). NBA is the edge.
    solo_alert_sports={"nba"},
    # Performance filter — derived from 7-day size × fav/dog analysis:
    #   Overall +8.4% ROI. Sweet spot is $15K-$50K (both sides positive:
    #   FAV +26% ROI, DOG +66% ROI). $50K+ DOG bucket is −44% ROI.
    # Allow $1K-$50K (either side) and $50K-$150K FAV only.
    performance_filter=PerformanceFilter(
        enabled=True,
        rules=(
            BetRule(min_size_usd=1_000, max_size_usd=50_000),                     # either side
            BetRule(favorite=True, min_size_usd=50_000, max_size_usd=150_000),    # big FAV only
        ),
    ),
    # Subtype filter — block futures (season-long winner) markets. These
    # are mass rebalances after game resolutions, not conviction signals.
    blocked_subtypes={"futures"},
    # Hour filter — afternoon 12-17 PST is his money-making window:
    #   42W/29L, +43.5% ROI on $823K stake (n=71) — 71% of his volume here.
    # Morning 6-12: 1W/4L, −99.5% ROI (n=5). Evening 17-21: −65% (n=14).
    # Restrict alerts to the afternoon sweet spot.
    allowed_hours_pst={12, 13, 14, 15, 16},
)


def main() -> None:
    asyncio.run(run_tracker(THEONLYHUMAN_CONFIG))


if __name__ == "__main__":
    main()
