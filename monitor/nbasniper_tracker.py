"""
nbasniper VIP tracker — wraps the generic `whale_tracker.WhaleVIPTracker`
with nbasniper-specific config.

nbasniper is a Tier 2 whale with +$714K / 7d on the Polymarket leaderboard
(12W/10L, 55% WR) at time of addition. We don't yet have enough history to
build a per-whale filter, so this wrapper runs in SHADOW MODE:
  - All positions are tracked in `tracked_whale_positions` (DB writes enabled)
  - `solo_alert_sports = set()` — empty set mutes ALL solo Telegram alerts
    from the PARENT bot so we don't spam the main channel
  - The paper trader (monitor/paper_trader.py) reads the DB directly and
    opens paper positions on every nbasniper signal regardless

After ~2 weeks (~20 resolved paper positions) we'll run a counterfactual
backtest on `paper_positions WHERE whale_alias='nbasniper'` and author
a real filter, at which point this file gets upgraded with
`solo_alert_sports`, `blocked_subtypes`, `performance_filter`, etc.

Runs in its own tmux session alongside the bot and other whale trackers.

Usage:
    python3 monitor/nbasniper_tracker.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from whale_tracker import WhaleConfig, run_tracker  # noqa: E402


# nbasniper — Tier 2 whale (see whales.json, promoted to live on Apr 17).
# Wallet: 0x492442eab586f242b53bda933fd5de859c8a3782
# 7d (pre-deploy): 12W/10L, +$714K on leaderboard snapshot.
NBASNIPER_CONFIG = WhaleConfig(
    wallet="0x492442eab586f242b53bda933fd5de859c8a3782",
    alias="nbasniper",
    emoji="🎯",
    # Gate at $1K to avoid tail-pick dust fills. No hard evidence on his avg
    # bet distribution yet — will revisit once we have 2 weeks of tracked data.
    min_position_usd=1_000.0,
    size_increase_alert_usd=5_000.0,
    poll_interval_sec=15,
    dual_write_legacy_texaskid_table=False,
    send_daily_report=False,
    # SHADOW MODE — empty set mutes ALL solo Telegram alerts. Positions still
    # land in `tracked_whale_positions` (muted_reason='shadow' via the sport
    # gate) so the paper trader can copy them. Parent Telegram stays quiet.
    solo_alert_sports=set(),
    # No other filters in v1 — we need history before we can design them.
    performance_filter=None,
    blocked_subtypes=None,
    allowed_hours_pst=None,
    blocked_hours_pst=None,
    require_multi_trade=False,
    tilt_mute_hours=None,
)


def main() -> None:
    asyncio.run(run_tracker(NBASNIPER_CONFIG))


if __name__ == "__main__":
    main()
