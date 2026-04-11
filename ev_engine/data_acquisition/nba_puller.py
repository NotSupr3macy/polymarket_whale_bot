"""
NBA historical data puller.

Pulls play-by-play data from ESPN's public API for multiple NBA seasons
and processes it inline into state snapshots for model training.

ESPN is used instead of nba_api because:
 - No rate limiting
 - Reliable historical PBP coverage back to ~2002
 - Already proven working in our espn_resolver.py
 - Doesn't require installing heavy dependencies

Season convention:
    An NBA season labelled YYYY runs from Oct YYYY-1 through June YYYY.
    e.g. season=2025 covers Oct 2024 through June 2025 (the 2024-25 season).

Output: data/nba/<season>_states.csv (one file per season)

Each row is one state snapshot (at each scoring event) with:
    game_id, state_idx, period, time_remaining_sec, game_time_elapsed_sec,
    home_score, away_score, score_diff, total_points_so_far, pace_estimate,
    final_home_score, final_away_score, home_win,
    final_point_diff, final_total_points

Checkpointing: data/nba/checkpoint.json tracks completed game_ids.

Usage:
    python -m ev_engine.data_acquisition.nba_puller --seasons 2022 2023 2024 2025
    python -m ev_engine.data_acquisition.nba_puller --seasons 2025 --concurrency 4
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import aiohttp


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | nba_puller | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
#  Config
# ─────────────────────────────────────────────────────────────────────

ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
ESPN_SUMMARY = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary"

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "nba"
CHECKPOINT_PATH = DATA_DIR / "checkpoint.json"

# NBA quarter = 12 min = 720 sec; OT = 5 min = 300 sec
REGULATION_PERIOD_SEC = 720
OT_PERIOD_SEC = 300
REGULATION_GAME_SEC = REGULATION_PERIOD_SEC * 4  # 2880


# ─────────────────────────────────────────────────────────────────────
#  Checkpoint helpers
# ─────────────────────────────────────────────────────────────────────

def load_checkpoint() -> set[str]:
    if not CHECKPOINT_PATH.exists():
        return set()
    try:
        with open(CHECKPOINT_PATH) as f:
            data = json.load(f)
        return set(data.get("completed_game_ids", []))
    except (json.JSONDecodeError, OSError):
        logger.warning("Checkpoint file corrupt, starting fresh")
        return set()


def save_checkpoint(completed: set[str]) -> None:
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_PATH.with_suffix(".json.tmp")
    with open(tmp, "w") as f:
        json.dump({"completed_game_ids": sorted(completed)}, f)
    os.replace(tmp, CHECKPOINT_PATH)


# ─────────────────────────────────────────────────────────────────────
#  Schedule: walk every day of a season and collect game_ids
# ─────────────────────────────────────────────────────────────────────

def season_date_range(season: int) -> tuple[date, date]:
    """
    Return the (start, end) date range covering an NBA season.

    We use Oct 1 (YYYY-1) to Jun 30 (YYYY) to cover preseason through finals.
    """
    return date(season - 1, 10, 1), date(season, 6, 30)


async def fetch_scoreboard(
    session: aiohttp.ClientSession, day: date, retries: int = 3
) -> list[dict]:
    """
    Return list of completed games on a given day.

    Each dict has: game_id, game_date, home_abbr, away_abbr.
    """
    url = ESPN_SCOREBOARD
    params = {"dates": day.strftime("%Y%m%d"), "limit": "100"}
    for attempt in range(retries):
        try:
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status != 200:
                    if attempt == retries - 1:
                        logger.warning("Scoreboard HTTP %d for %s", resp.status, day)
                    await asyncio.sleep(1 + attempt)
                    continue
                data = await resp.json()
                break
        except Exception as e:
            if attempt == retries - 1:
                logger.warning("Scoreboard error for %s: %s", day, e)
            await asyncio.sleep(1 + attempt)
    else:
        return []

    games: list[dict] = []
    for event in data.get("events", []):
        status = event.get("status", {}).get("type", {})
        if not status.get("completed"):
            continue
        comps = event.get("competitions", [])
        if not comps:
            continue
        comp = comps[0]
        competitors = comp.get("competitors", [])
        home = next((c for c in competitors if c.get("homeAway") == "home"), None)
        away = next((c for c in competitors if c.get("homeAway") == "away"), None)
        if not home or not away:
            continue
        games.append(
            {
                "game_id": str(event.get("id") or comp.get("id")),
                "game_date": day.isoformat(),
                "home_abbr": home.get("team", {}).get("abbreviation", ""),
                "away_abbr": away.get("team", {}).get("abbreviation", ""),
            }
        )
    return games


# ─────────────────────────────────────────────────────────────────────
#  Game summary: fetch PBP and extract states
# ─────────────────────────────────────────────────────────────────────

_CLOCK_RE = re.compile(r"^(\d+):(\d+)(?:\.\d+)?$")


def parse_clock_to_seconds(clock_str: str) -> Optional[int]:
    """
    Parse an ESPN clock string ('11:23' or '0:04.5') into seconds remaining
    in the current period.
    """
    if not clock_str:
        return None
    m = _CLOCK_RE.match(str(clock_str).strip())
    if not m:
        return None
    return int(m.group(1)) * 60 + int(m.group(2))


async def fetch_game_summary(
    session: aiohttp.ClientSession, game_id: str, retries: int = 3
) -> Optional[dict]:
    """Fetch ESPN game summary (contains plays array)."""
    url = ESPN_SUMMARY
    params = {"event": game_id}
    for attempt in range(retries):
        try:
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status == 404:
                    return None
                if attempt == retries - 1:
                    logger.warning("Summary HTTP %d for %s", resp.status, game_id)
        except Exception as e:
            if attempt == retries - 1:
                logger.warning("Summary error for %s: %s", game_id, e)
        await asyncio.sleep(2**attempt)
    return None


def extract_states(summary: dict) -> list[dict]:
    """Walk ESPN summary 'plays' and build one state row per play."""
    header = summary.get("header", {})
    comps = header.get("competitions", [])
    if not comps:
        return []
    comp = comps[0]
    competitors = comp.get("competitors", [])
    home = next((c for c in competitors if c.get("homeAway") == "home"), None)
    away = next((c for c in competitors if c.get("homeAway") == "away"), None)
    if not home or not away:
        return []

    try:
        final_home = int(home.get("score") or 0)
        final_away = int(away.get("score") or 0)
    except (TypeError, ValueError):
        return []

    if final_home == 0 and final_away == 0:
        return []
    # Ties do not exist in NBA regulation games
    if final_home == final_away:
        return []

    home_id = home.get("id")
    # away_id = away.get("id")  # unused

    home_win = 1 if final_home > final_away else 0
    final_diff = final_home - final_away
    final_total = final_home + final_away

    plays = summary.get("plays", [])
    if not plays:
        return []

    rows: list[dict] = []
    last_home = 0
    last_away = 0
    for idx, play in enumerate(plays):
        period_obj = play.get("period") or {}
        period = int(period_obj.get("number") or 0)
        if period < 1:
            continue

        clock_str = play.get("clock", {}).get("displayValue", "")
        time_remaining = parse_clock_to_seconds(clock_str)
        if time_remaining is None:
            continue

        # ESPN provides homeScore/awayScore on scoring plays;
        # on non-scoring plays use the running last-known score.
        try:
            if play.get("homeScore") is not None:
                last_home = int(play["homeScore"])
            if play.get("awayScore") is not None:
                last_away = int(play["awayScore"])
        except (TypeError, ValueError):
            pass

        home_score = last_home
        away_score = last_away

        # Compute elapsed game time in seconds
        if period <= 4:
            period_length = REGULATION_PERIOD_SEC
            finished_periods = period - 1
            elapsed = finished_periods * REGULATION_PERIOD_SEC + (period_length - time_remaining)
        else:
            finished_periods_sec = REGULATION_GAME_SEC + (period - 5) * OT_PERIOD_SEC
            elapsed = finished_periods_sec + (OT_PERIOD_SEC - time_remaining)

        if elapsed <= 0:
            continue

        total_points = home_score + away_score
        pace_estimate = (total_points / elapsed) * REGULATION_GAME_SEC if elapsed > 0 else 0

        rows.append(
            {
                "state_idx": idx,
                "period": period,
                "time_remaining_sec": time_remaining,
                "game_time_elapsed_sec": elapsed,
                "home_score": home_score,
                "away_score": away_score,
                "score_diff": home_score - away_score,
                "total_points_so_far": total_points,
                "pace_estimate": round(pace_estimate, 2),
                "final_home_score": final_home,
                "final_away_score": final_away,
                "final_point_diff": final_diff,
                "final_total_points": final_total,
                "home_win": home_win,
            }
        )
    return rows


# ─────────────────────────────────────────────────────────────────────
#  CSV writer
# ─────────────────────────────────────────────────────────────────────

CSV_COLUMNS = [
    "game_id", "state_idx",
    "period", "time_remaining_sec", "game_time_elapsed_sec",
    "home_score", "away_score", "score_diff", "total_points_so_far",
    "pace_estimate",
    "final_home_score", "final_away_score",
    "final_point_diff", "final_total_points", "home_win",
]


class SeasonWriter:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self._files: dict[int, any] = {}
        self._writers: dict[int, any] = {}

    def write_game(self, season: int, game_id: str, rows: list[dict]) -> None:
        if season not in self._writers:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            path = self.data_dir / f"{season}_states.csv"
            exists = path.exists()
            f = open(path, "a", newline="", encoding="utf-8")
            w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            if not exists:
                w.writeheader()
            self._files[season] = f
            self._writers[season] = w
        w = self._writers[season]
        for row in rows:
            row["game_id"] = game_id
            w.writerow(row)
        self._files[season].flush()

    def close(self) -> None:
        for f in self._files.values():
            f.close()


# ─────────────────────────────────────────────────────────────────────
#  Main puller
# ─────────────────────────────────────────────────────────────────────

async def pull_season(
    session: aiohttp.ClientSession,
    season: int,
    writer: SeasonWriter,
    checkpoint: set[str],
    concurrency: int,
    delay: float,
) -> int:
    """Pull all games for one season. Returns new games pulled."""
    start, end = season_date_range(season)
    logger.info("Season %d: scanning dates %s through %s", season, start, end)

    # ── Step 1: walk every day to collect game IDs ────────────────────
    all_games: list[dict] = []
    d = start
    scoreboard_sem = asyncio.Semaphore(concurrency)

    async def scan_day(day: date) -> list[dict]:
        async with scoreboard_sem:
            result = await fetch_scoreboard(session, day)
            await asyncio.sleep(delay)
            return result

    tasks: list = []
    while d <= end:
        tasks.append(scan_day(d))
        d += timedelta(days=1)

    day_results = await asyncio.gather(*tasks)
    for day_games in day_results:
        all_games.extend(day_games)

    pending = [g for g in all_games if g["game_id"] not in checkpoint]
    if not pending:
        logger.info("Season %d: all %d games already in checkpoint", season, len(all_games))
        return 0

    logger.info(
        "Season %d: %d games found, %d to pull (%d already done)",
        season, len(all_games), len(pending), len(all_games) - len(pending),
    )

    # ── Step 2: fetch each game's PBP summary ─────────────────────────
    sem = asyncio.Semaphore(concurrency)
    completed = 0
    failed = 0
    start_time = time.time()

    async def pull_one(g: dict) -> None:
        nonlocal completed, failed
        async with sem:
            game_id = g["game_id"]
            summary = await fetch_game_summary(session, game_id)
            if not summary:
                failed += 1
                return
            rows = extract_states(summary)
            if not rows:
                failed += 1
                return
            writer.write_game(season, game_id, rows)
            checkpoint.add(game_id)
            completed += 1
            if completed % 50 == 0:
                elapsed = time.time() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                remaining = len(pending) - completed
                eta = remaining / rate if rate > 0 else 0
                logger.info(
                    "  Season %d: %d/%d done (%.1f/s, ETA %.0f min, %d failed)",
                    season, completed, len(pending), rate, eta / 60, failed,
                )
                save_checkpoint(checkpoint)
            await asyncio.sleep(delay)

    await asyncio.gather(*[pull_one(g) for g in pending])
    save_checkpoint(checkpoint)
    logger.info(
        "Season %d complete: %d new, %d failed, %.0f sec",
        season, completed, failed, time.time() - start_time,
    )
    return completed


async def main_async(seasons: list[int], concurrency: int, delay: float) -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    checkpoint = load_checkpoint()
    logger.info("Loaded checkpoint: %d games already completed", len(checkpoint))

    writer = SeasonWriter(DATA_DIR)
    total_new = 0
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(limit=concurrency * 2)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            for season in seasons:
                total_new += await pull_season(
                    session, season, writer, checkpoint, concurrency, delay
                )
    finally:
        writer.close()
        save_checkpoint(checkpoint)

    logger.info("DONE. Pulled %d new games across %d seasons.", total_new, len(seasons))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Pull NBA historical game states.")
    parser.add_argument(
        "--seasons", type=int, nargs="+", default=[2022, 2023, 2024, 2025],
        help="Seasons to pull (default: 2022 2023 2024 2025). "
             "Season YYYY covers Oct YYYY-1 through Jun YYYY.",
    )
    parser.add_argument(
        "--concurrency", type=int, default=4,
        help="Max concurrent ESPN requests (default: 4)",
    )
    parser.add_argument(
        "--delay", type=float, default=0.1,
        help="Delay between requests per worker in seconds (default: 0.1)",
    )
    args = parser.parse_args()

    return asyncio.run(main_async(args.seasons, args.concurrency, args.delay))


if __name__ == "__main__":
    sys.exit(main())
