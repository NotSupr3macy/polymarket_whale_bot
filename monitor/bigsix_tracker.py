"""
BigSix VIP tracker — wraps the generic `whale_tracker.WhaleVIPTracker`
with bigsix-specific config.

Runs in its own tmux session alongside the bot and texaskid tracker.

Usage:
    python3 monitor/bigsix_tracker.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# The tmux start script runs this as `python3 monitor/bigsix_tracker.py`
# (a script, not a module), so relative imports don't work. Add the
# monitor/ directory to sys.path and import whale_tracker directly.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from whale_tracker import BetRule, PerformanceFilter, WhaleConfig, run_tracker  # noqa: E402


# bigsix — Tier 1 whale (see whales.json). Avg bet ~$25K.
BIGSIX_CONFIG = WhaleConfig(
    wallet="0xa71093cafc0c099b4ccab24c3cb8018d817923c4",
    alias="bigsix",
    emoji="🐳",
    # Slightly higher min size than texaskid — bigsix doesn't drip-feed as
    # small; gate at $500 to avoid alerting on rollovers from resolved markets.
    min_position_usd=500.0,
    # Standard size-up alert threshold
    size_increase_alert_usd=5_000.0,
    poll_interval_sec=15,
    # Only texaskid dual-writes to the legacy table (ev_engine still reads it)
    dual_write_legacy_texaskid_table=False,
    # Only texaskid emits a daily report for now — Phase 5 will replace this
    # with a consolidated bot-side "whale digest" that covers all whales.
    send_daily_report=False,
    # bigsix demoted to consensus-only for non-NHL sports.
    # NHL: 5W/4L +$136K (HOT) → solo alerts fire
    # All other sports: tracked in DB for consensus radar + cashout engine
    # but no standalone Telegram pings (18.8% overall WR, -$1.1M PnL).
    solo_alert_sports={"nhl"},
    # Performance filter — CROSS-SPORT data said $15K-$50K FAV was −31% ROI,
    # but his NHL-ONLY FAV performance is +7.8% ROI (30W/14L on $2.95M) with
    # the $50K-$150K NHL FAV slice at +7.3% ROI. Since solo_alert_sports
    # already filters to NHL, the cross-sport FAV drain (driven by MLB) can't
    # reach Telegram anyway — the size cap was over-tightened.
    # Kept as a safety rail against runaway $150K+ tickets.
    performance_filter=PerformanceFilter(
        enabled=True,
        rules=(
            BetRule(favorite=False),                            # All underdogs
            BetRule(favorite=True, max_size_usd=150_000),       # FAVs up to $150K
        ),
    ),
    # Subtype filter — his Totals (O/U) book: 17W/19L, −20.7% ROI on $687K
    # stake (n=36). H2H ML, spread, daily-ML all positive.
    # Also block:
    #   - segment markets (CS Map/Period winners) — outside his NHL edge;
    #     his 3 CS Map bets on Apr 15 all lost (FOKUS, MIBR, Imperial).
    #   - futures (Stanley Cup / season-long winners) — probabilistic
    #     rebalances, not conviction signals; spammy after game resolutions.
    blocked_subtypes={"totals", "segment", "futures"},
    # Require multi-trade — his single-trade NEW POSITIONS: 14W/19L at
    # −31.9% ROI (n=33). Multi-trade (added): 36W/25L at +18.9% ROI (n=61).
    # Mute NEW POSITION alerts; SIZE UP on the same cid still fires.
    require_multi_trade=True,
)


def main() -> None:
    asyncio.run(run_tracker(BIGSIX_CONFIG))


if __name__ == "__main__":
    main()
