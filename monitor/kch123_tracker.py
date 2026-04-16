"""
kch123 VIP tracker — wraps the generic `whale_tracker.WhaleVIPTracker`
with kch123-specific config.

kch123 is an elite NHL specialist — 30d Gamma-resolved: 42W/39L on NHL
(+$369K, +21% ROI on $1.72M stake). $12M all-time PnL. We track NHL only;
other sports (he rarely trades) tracked silently for consensus.

Runs in its own tmux session alongside the bot and other whale trackers.

Usage:
    python3 monitor/kch123_tracker.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from whale_tracker import BetRule, PerformanceFilter, WhaleConfig, run_tracker  # noqa: E402


# kch123 — Tier 1 NHL specialist (see whales.json).
# Avg bet ~$19.8K per market on $1.8M / 30d NHL volume.
KCH123_CONFIG = WhaleConfig(
    wallet="0x6a72f61820b26b1fe4d956e17b6dc2a1ea3033ee",
    alias="kch123",
    emoji="🏒",
    # Larger min than bigsix — kch123's big bets are $50K+, his drip-feed
    # trades still start at ~$1K. Gate at $1000 to avoid alert noise on
    # the tiny tail-pick fills visible on his profile.
    min_position_usd=1_000.0,
    size_increase_alert_usd=5_000.0,
    poll_interval_sec=15,
    dual_write_legacy_texaskid_table=False,
    send_daily_report=False,
    # NHL-only solo alerts. His 30d NBA was 0W/1L -$84K — not tailable.
    # All non-NHL tracked in DB for consensus radar only.
    solo_alert_sports={"nhl"},
    # Performance filter — derived from 5-day size × fav/dog analysis:
    #   Overall −18% ROI was driven by one $430K loss. Positive cells:
    #     $1K-$5K FAV: 77.8% WR, +16% ROI
    #     $5K-$15K FAV: +14% ROI
    #     $5K-$15K DOG: +98% ROI
    #     $15K-$50K DOG: positive
    # Hard reject everything >= $50K (one bad ticket wipes the book).
    performance_filter=PerformanceFilter(
        enabled=True,
        rules=(
            BetRule(favorite=True,  min_size_usd=1_000, max_size_usd=15_000),
            BetRule(favorite=False, min_size_usd=5_000, max_size_usd=15_000),
            BetRule(favorite=False, min_size_usd=15_000, max_size_usd=50_000),
        ),
    ),
    # Subtype filter — block futures (Stanley Cup / season winner) markets.
    # Apr 15 evening: 8 futures NEW POSITIONs spammed in 1 minute after
    # Dallas Stars game resolved — all probabilistic rebalances, not
    # conviction signals. Plus segment markets (period winners) for safety.
    blocked_subtypes={"futures", "segment"},
    # Hour filter — afternoon 12-17 PST: 17W/15L but −83.1% ROI on $527K stake
    # (n=32). Morning / evening hours are profitable. Block afternoon.
    blocked_hours_pst={12, 13, 14, 15, 16},
    # Tilt guard — after-loss bucket: 22W/9L (71% WR) but −58% ROI on $562K
    # stake. The $430K loss was an after-loss bet. Mute 4 hours after each loss.
    tilt_mute_hours=4.0,
)


def main() -> None:
    asyncio.run(run_tracker(KCH123_CONFIG))


if __name__ == "__main__":
    main()
