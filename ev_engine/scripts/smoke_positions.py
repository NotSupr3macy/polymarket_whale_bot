"""
Smoke test: load open texaskid positions, enrich them, and print a summary.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from ev_engine.position_manager import PositionManager  # noqa: E402


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)


async def main() -> int:
    async with aiohttp.ClientSession() as session:
        pm = PositionManager(session)
        positions = pm.load_open_positions()
        print(f"\nLoaded {len(positions)} open texaskid positions\n")

        await pm.enrich_all(positions)

        for p in positions:
            print("─" * 78)
            print(f"[{p.condition_id[:10]}..] {p.market_title}")
            print(f"  direction: {p.direction}")
            print(f"  size: ${p.current_size_usd:,.0f}  first_seen: {p.first_seen_price}")
            if p.parse:
                print(
                    f"  parsed: sport={p.parse.sport} type={p.parse.bet_type} "
                    f"team1={p.parse.team1_abbr} team2={p.parse.team2_abbr} "
                    f"line={p.parse.line} picked={p.parse.picked_team_abbr}"
                )
            if p.cashout:
                print(
                    f"  cashout: mid={p.cashout.mid_price:.3f} "
                    f"bid={p.cashout.best_bid} ask={p.cashout.best_ask} "
                    f"outcome={p.cashout.outcome}"
                )
            else:
                print("  cashout: (none)")
            if p.mlb_state:
                s = p.mlb_state
                print(
                    f"  MLB live: {s.status} inning={s.inning} "
                    f"{s.home_abbr} {s.home_score}-{s.away_score} {s.away_abbr} "
                    f"outs={s.outs} runners={s.runners_on}"
                )
            if p.nba_state:
                s = p.nba_state
                print(
                    f"  NBA live: {s.status} Q{s.period} "
                    f"{s.home_abbr} {s.home_score}-{s.away_score} {s.away_abbr} "
                    f"elapsed={s.game_time_elapsed_sec}s pace={s.pace_estimate}"
                )
            if p.error:
                print(f"  ERROR: {p.error}")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
