"""
sportmaster777 VIP tracker — wraps the generic `whale_tracker.WhaleVIPTracker`
with sportmaster777-specific config.

Promoted from shadow pool to paper trader lineup on Apr 18 based on
6-day forward analysis of 655 resolved positions:
  - WR 53.3%, ROI +9.2%, +$57.6K on $627K stake
  - No chalk/arb pattern, no scalp pattern, no survivor bias
  - All 3 sports (NBA, MLB, NHL) non-negative — MLB is strongest (+27% ROI)
  - Clear size-bucket edge: $500-$15K is profitable, $15K+ is tail-risk disaster

Filter design:
  - allow size $500-$15K (both sides), matching his sweet spot
  - block $15K+ (his 1W/3L, -60% ROI bucket with $95K risk exposure)
  - no sport filter — NBA/MLB/NHL all OK
  - no subtype filter — h2h-ml/totals/spread all positive

He's high-frequency (~109 resolved/day over 6d), so expect many
alerts. Size increase threshold set lower ($2K) since his avg bet is
$1-5K — we want to catch adds earlier.

Runs in its own tmux session alongside the bot and other whale trackers.

Usage:
    python3 monitor/sportmaster_tracker.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from whale_tracker import BetRule, PerformanceFilter, WhaleConfig, run_tracker  # noqa: E402


# sportmaster777 — Tier 2 whale (scouted Apr 11, promoted Apr 18).
# Wallet: 0x32ed517a571c01b6e9adecf61ba81ca48ff2f960
# 6-day live tracked: 53.3% WR / +9.2% ROI / +$57K on $627K stake.
SPORTMASTER_CONFIG = WhaleConfig(
    wallet="0x32ed517a571c01b6e9adecf61ba81ca48ff2f960",
    alias="sportmaster777",
    emoji="🎯",
    # Tight gate at $500 — matches bottom of his profitable size range.
    # Sub-$500 bets are break-even (51.2% WR, +0% ROI) — filter handles
    # the strict $500+ min, min_position_usd is just a perf optimization.
    min_position_usd=500.0,
    # Smaller size-up threshold than other whales — his avg bet is $1-5K,
    # so a $2K add is meaningful (vs $5K for e.g. bigsix who bets $50K).
    size_increase_alert_usd=2_000.0,
    poll_interval_sec=15,
    dual_write_legacy_texaskid_table=False,
    send_daily_report=False,
    # No sport filter — his 3-sport book (NBA/MLB/NHL) is all non-negative.
    # MLB is strongest (+27% ROI) but NBA (−1.6% flat) and NHL (+8.8%) worth
    # alerting on too. If MLB edge diverges, revisit after 30 days.
    solo_alert_sports=None,
    # Performance filter — size-based ONLY:
    #   < $500:    +0.0% ROI (break-even, filtered by min_position_usd anyway)
    #   $500-$1.5K: +6.1% ROI (profitable)
    #   $1.5K-$5K:  +37.6% ROI ← sweet spot
    #   $5K-$15K:   +21.7% ROI ← also sweet spot
    #   $15K-$50K:  -60.0% ROI on 1W/3L — hard reject
    performance_filter=PerformanceFilter(
        enabled=True,
        rules=(
            BetRule(min_size_usd=500.0, max_size_usd=15_000.0),
        ),
    ),
    # No subtype filter — all 3 subtypes (h2h-ml/totals/spread) are positive.
    blocked_subtypes=None,
    # No hour filter — his 6-day distribution didn't show clear hour edge.
    allowed_hours_pst=None,
    blocked_hours_pst=None,
    # No tilt guard — after-loss pattern not detected in diagnostic.
    tilt_mute_hours=None,
    # No require_multi_trade — both NEW and SIZE UP are conviction signals for him.
    require_multi_trade=False,
)


def main() -> None:
    asyncio.run(run_tracker(SPORTMASTER_CONFIG))


if __name__ == "__main__":
    main()
