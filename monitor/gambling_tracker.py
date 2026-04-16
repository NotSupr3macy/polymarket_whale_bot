"""
GamblingIsAllYouNeed (GIAYN) VIP tracker — wraps the generic
`whale_tracker.WhaleVIPTracker` with GIAYN-specific config.

Runs in its own tmux session alongside the bot and other whale trackers.

Usage:
    python3 monitor/gambling_tracker.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# The tmux start script runs this as `python3 monitor/gambling_tracker.py`
# (a script, not a module), so relative imports don't work. Add the
# monitor/ directory to sys.path and import whale_tracker directly.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from whale_tracker import WhaleConfig, run_tracker  # noqa: E402


# GamblingIsAllYouNeed (GIAYN) — MLB-focused whale.
GIAYN_CONFIG = WhaleConfig(
    wallet="0x507e52ef684ca2dd91f90a9d26d149dd3288beae",
    alias="GamblingIsAllYouNeed",
    emoji="🎰",
    # Gate at $500 to avoid alerting on rollovers from resolved markets.
    min_position_usd=500.0,
    # Standard size-up alert threshold
    size_increase_alert_usd=5_000.0,
    poll_interval_sec=15,
    # Only texaskid dual-writes to the legacy table (ev_engine still reads it)
    dual_write_legacy_texaskid_table=False,
    # No daily report — Phase 5 will replace this with a consolidated
    # bot-side "whale digest" that covers all whales.
    send_daily_report=False,
    # GIAYN: MLB only for solo alerts.
    # All other sports: tracked in DB for consensus radar + cashout engine
    # but no standalone Telegram pings.
    solo_alert_sports={"mlb"},
)


def main() -> None:
    asyncio.run(run_tracker(GIAYN_CONFIG))


if __name__ == "__main__":
    main()
