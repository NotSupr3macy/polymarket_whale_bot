"""
MLB historical data puller.

Pulls play-by-play data from statsapi.mlb.com for multiple seasons and
processes it inline into state snapshots suitable for model training.

Output: data/mlb/<season>_states.csv (one file per season)

Each row is one half-inning state snapshot with:
    game_pk, state_idx, inning, top_bottom, outs, score_diff,
    runners_on, total_runs_so_far, home_score, away_score,
    final_home_score, final_away_score, home_win,
    final_run_diff, final_total_runs

Checkpointing: data/mlb/checkpoint.json tracks completed game_pks.
Resume is automatic — just re-run the script.

Usage:
    python -m ev_engine.data_acquisition.mlb_puller --seasons 2022 2023 2024 2025
    python -m ev_engine.data_acquisition.mlb_puller --seasons 2022 --concurrency 3
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional

import aiohttp


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | mlb_puller | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
#  Config
# ─────────────────────────────────────────────────────────────────────

MLB_API_BASE = "https://statsapi.mlb.com/api/v1"
MLB_API_V1_1 = "https://statsapi.mlb.com/api/v1.1"

# Root output directory — resolves to project root `ev_engine/data/mlb/`
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "mlb"
CHECKPOINT_PATH = DATA_DIR / "checkpoint.json"

# Runners-on encoding: key is "1st,2nd,3rd" booleans
RUNNER_CODE: dict[tuple[bool, bool, bool], int] = {
    (False, False, False): 0,  # empty
    (True,  False, False): 1,  # 1st
    (False, True,  False): 2,  # 2nd
    (False, False, True):  3,  # 3rd
    (True,  True,  False): 4,  # 1st + 2nd
    (True,  False, True):  5,  # 1st + 3rd
    (False, True,  True):  6,  # 2nd + 3rd
    (True,  True,  True):  7,  # loaded
}


# ─────────────────────────────────────────────────────────────────────
#  Checkpoint helpers
# ─────────────────────────────────────────────────────────────────────

def load_checkpoint() -> set[int]:
    """Load the set of game_pks already pulled."""
    if not CHECKPOINT_PATH.exists():
        return set()
    try:
        with open(CHECKPOINT_PATH) as f:
            data = json.load(f)
        return set(data.get("completed_game_pks", []))
    except (json.JSONDecodeError, OSError):
        logger.warning("Checkpoint file corrupt, starting fresh")
        return set()


def save_checkpoint(completed: set[int]) -> None:
    """Atomically write the checkpoint file."""
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_PATH.with_suffix(".json.tmp")
    with open(tmp, "w") as f:
        json.dump({"completed_game_pks": sorted(completed)}, f)
    os.replace(tmp, CHECKPOINT_PATH)


# ─────────────────────────────────────────────────────────────────────
#  Schedule: find all game_pks for a season
# ─────────────────────────────────────────────────────────────────────

async def fetch_season_schedule(
    session: aiohttp.ClientSession, season: int
) -> list[dict]:
    """
    Fetch all regular season + postseason games for a given MLB season.

    Returns a list of dicts with keys: game_pk, game_date, status, home, away.
    """
    # Game types: R=regular, F=wild card, D=division, L=LCS, W=world series
    url = f"{MLB_API_BASE}/schedule"
    params = {
        "sportId": "1",
        "season": str(season),
        "gameType": "R,F,D,L,W",
        "hydrate": "team",
    }
    async with session.get(
        url, params=params, timeout=aiohttp.ClientTimeout(total=30)
    ) as resp:
        if resp.status != 200:
            logger.error("Schedule API %d for season %s", resp.status, season)
            return []
        data = await resp.json()

    games: list[dict] = []
    for day in data.get("dates", []):
        for g in day.get("games", []):
            status = g.get("status", {}).get("detailedState", "")
            # Only keep games that actually finished
            if status not in ("Final", "Completed Early", "Game Over"):
                continue
            games.append(
                {
                    "game_pk": g["gamePk"],
                    "game_date": g.get("officialDate") or g.get("gameDate", "")[:10],
                    "status": status,
                    "home": g.get("teams", {}).get("home", {}).get("team", {}).get("abbreviation", ""),
                    "away": g.get("teams", {}).get("away", {}).get("team", {}).get("abbreviation", ""),
                }
            )
    logger.info("Season %d: %d completed games", season, len(games))
    return games


# ─────────────────────────────────────────────────────────────────────
#  Game feed: extract state snapshots
# ─────────────────────────────────────────────────────────────────────

async def fetch_game_feed(
    session: aiohttp.ClientSession, game_pk: int, retries: int = 3
) -> Optional[dict]:
    """Fetch raw game feed JSON with exponential backoff on failure."""
    url = f"{MLB_API_V1_1}/game/{game_pk}/feed/live"
    for attempt in range(retries):
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status == 404:
                    logger.warning("Game %d not found", game_pk)
                    return None
                logger.warning("Game %d HTTP %d (attempt %d)", game_pk, resp.status, attempt + 1)
        except Exception as e:
            logger.warning("Game %d fetch error: %s (attempt %d)", game_pk, e, attempt + 1)
        await asyncio.sleep(2**attempt)
    return None


def extract_states(feed: dict) -> list[dict]:
    """
    Walk a game feed and emit one state row per play (before each pitch is
    resolved — i.e. at the start of each batter, or after each out change).

    We take snapshots at the END of each play: that's when inning/outs/runners
    are well-defined and the remaining game is a clean forward prediction.
    """
    live = feed.get("liveData", {})
    plays_data = live.get("plays", {})
    all_plays = plays_data.get("allPlays", [])
    if not all_plays:
        return []

    # Final score — needed for target variables
    linescore = live.get("linescore", {})
    teams_final = linescore.get("teams", {})
    final_home = int(teams_final.get("home", {}).get("runs") or 0)
    final_away = int(teams_final.get("away", {}).get("runs") or 0)

    # Skip games that ended weirdly (tied, suspended, etc.)
    if final_home == 0 and final_away == 0:
        return []
    if final_home == final_away:
        # Ties exist in spring training but shouldn't in regular + post season
        return []

    home_win = 1 if final_home > final_away else 0
    final_run_diff = final_home - final_away  # home perspective
    final_total = final_home + final_away

    rows: list[dict] = []
    for idx, play in enumerate(all_plays):
        about = play.get("about", {})
        result = play.get("result", {})
        count = play.get("count", {})
        matchup = play.get("matchup", {})

        inning = int(about.get("inning") or 0)
        if inning < 1 or inning > 20:
            continue
        is_top = bool(about.get("isTopInning"))

        # Score BEFORE this play's result — safer for causal modeling:
        # treat state as "game state with all prior plays resolved"
        home_score = int(result.get("homeScore") or 0)
        away_score = int(result.get("awayScore") or 0)

        outs = int(count.get("outs") or 0)
        if outs > 3:
            outs = 3

        runners = matchup.get("postOnFirst"), matchup.get("postOnSecond"), matchup.get("postOnThird")
        # Each is either None or a dict describing the runner
        r_code = RUNNER_CODE[(bool(runners[0]), bool(runners[1]), bool(runners[2]))]

        # Batting team: if top of inning, away bats; if bottom, home bats
        batting_is_home = not is_top
        # Score diff from batting team's perspective
        if batting_is_home:
            score_diff = home_score - away_score
        else:
            score_diff = away_score - home_score

        rows.append(
            {
                "state_idx": idx,
                "inning": inning,
                "top_bottom": 0 if is_top else 1,
                "outs": outs,
                "runners_on": r_code,
                "home_score": home_score,
                "away_score": away_score,
                "score_diff": score_diff,
                "total_runs_so_far": home_score + away_score,
                "final_home_score": final_home,
                "final_away_score": final_away,
                "final_run_diff": final_run_diff,
                "final_total_runs": final_total,
                "home_win": home_win,
            }
        )
    return rows


# ─────────────────────────────────────────────────────────────────────
#  CSV writer
# ─────────────────────────────────────────────────────────────────────

CSV_COLUMNS = [
    "game_pk", "state_idx",
    "inning", "top_bottom", "outs", "runners_on",
    "home_score", "away_score", "score_diff", "total_runs_so_far",
    "final_home_score", "final_away_score",
    "final_run_diff", "final_total_runs", "home_win",
]


class SeasonWriter:
    """Appends state rows to per-season CSV files."""

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self._files: dict[int, any] = {}
        self._writers: dict[int, any] = {}

    def write_game(self, season: int, game_pk: int, rows: list[dict]) -> None:
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
            row["game_pk"] = game_pk
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
    checkpoint: set[int],
    concurrency: int,
    delay: float,
) -> int:
    """Pull all games for one season. Returns number of new games pulled."""
    games = await fetch_season_schedule(session, season)
    pending = [g for g in games if g["game_pk"] not in checkpoint]
    if not pending:
        logger.info("Season %d: all %d games already in checkpoint, skipping", season, len(games))
        return 0

    logger.info(
        "Season %d: %d games to pull (%d already done)",
        season, len(pending), len(games) - len(pending),
    )

    sem = asyncio.Semaphore(concurrency)
    completed = 0
    failed = 0
    start_time = time.time()

    async def pull_one(g: dict) -> None:
        nonlocal completed, failed
        async with sem:
            game_pk = g["game_pk"]
            feed = await fetch_game_feed(session, game_pk)
            if not feed:
                failed += 1
                return
            rows = extract_states(feed)
            if not rows:
                failed += 1
                return
            writer.write_game(season, game_pk, rows)
            checkpoint.add(game_pk)
            completed += 1
            if completed % 50 == 0:
                elapsed = time.time() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                remaining = len(pending) - completed
                eta_sec = remaining / rate if rate > 0 else 0
                logger.info(
                    "  Season %d: %d/%d done (%.1f/s, ETA %.0f min, %d failed)",
                    season, completed, len(pending), rate, eta_sec / 60, failed,
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
    parser = argparse.ArgumentParser(description="Pull MLB historical game states.")
    parser.add_argument(
        "--seasons", type=int, nargs="+", default=[2022, 2023, 2024, 2025],
        help="Seasons to pull (default: 2022 2023 2024 2025)",
    )
    parser.add_argument(
        "--concurrency", type=int, default=4,
        help="Max concurrent game feed requests (default: 4)",
    )
    parser.add_argument(
        "--delay", type=float, default=0.15,
        help="Delay between requests per worker in seconds (default: 0.15)",
    )
    args = parser.parse_args()

    return asyncio.run(main_async(args.seasons, args.concurrency, args.delay))


if __name__ == "__main__":
    sys.exit(main())
