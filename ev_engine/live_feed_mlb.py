"""
Live MLB game state feed.

Queries statsapi.mlb.com for in-progress games and returns normalized
game state dicts compatible with the trained models.

Usage:
    feed = MLBLiveFeed(session)
    games_today = await feed.find_games(date_obj, home_abbr, away_abbr)
    state = await feed.get_live_state(game_pk)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Optional

import aiohttp

from .data_acquisition.mlb_puller import RUNNER_CODE


logger = logging.getLogger(__name__)

MLB_API_BASE = "https://statsapi.mlb.com/api/v1"
MLB_API_V1_1 = "https://statsapi.mlb.com/api/v1.1"


@dataclass
class MLBGameState:
    """Normalized live MLB game state for model inference."""
    game_pk: int
    status: str              # "PRE_GAME", "LIVE", "FINAL", "UNKNOWN"
    inning: int              # 1+
    top_bottom: int          # 0=top, 1=bottom
    outs: int                # 0-3
    runners_on: int          # 0-7
    home_score: int
    away_score: int
    score_diff: int          # batting team perspective
    total_runs_so_far: int
    final_home_score: int    # only valid when status=FINAL
    final_away_score: int    # only valid when status=FINAL
    home_abbr: str
    away_abbr: str


class MLBLiveFeed:
    """Async MLB live data feed. Reuses a shared aiohttp session."""

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self._session = session

    async def find_games(
        self, target_date: date, home_abbr: Optional[str] = None,
        away_abbr: Optional[str] = None,
    ) -> list[dict]:
        """
        Return all MLB games on a date, optionally filtered to a specific matchup.

        Returns list of dicts: {game_pk, home_abbr, away_abbr, status}.
        """
        url = f"{MLB_API_BASE}/schedule"
        params = {
            "sportId": "1",
            "date": target_date.isoformat(),
            "hydrate": "team",
        }
        try:
            async with self._session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    logger.warning("Schedule HTTP %d", resp.status)
                    return []
                data = await resp.json()
        except Exception as e:
            logger.warning("Schedule fetch error: %s", e)
            return []

        games: list[dict] = []
        for day in data.get("dates", []):
            for g in day.get("games", []):
                home = g.get("teams", {}).get("home", {}).get("team", {}).get("abbreviation", "")
                away = g.get("teams", {}).get("away", {}).get("team", {}).get("abbreviation", "")
                if home_abbr and home.upper() != home_abbr.upper():
                    if not away_abbr or away.upper() != home_abbr.upper():
                        continue
                if away_abbr and away.upper() != away_abbr.upper():
                    if not home_abbr or home.upper() != away_abbr.upper():
                        continue
                games.append({
                    "game_pk": g["gamePk"],
                    "home_abbr": home,
                    "away_abbr": away,
                    "status": g.get("status", {}).get("abstractGameState", "Unknown"),
                })
        return games

    async def get_live_state(self, game_pk: int) -> Optional[MLBGameState]:
        """Fetch the current game state for a given game_pk."""
        url = f"{MLB_API_V1_1}/game/{game_pk}/feed/live"
        try:
            async with self._session.get(
                url, timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    logger.warning("Game %d HTTP %d", game_pk, resp.status)
                    return None
                feed = await resp.json()
        except Exception as e:
            logger.warning("Game %d fetch error: %s", game_pk, e)
            return None

        gd = feed.get("gameData", {})
        live = feed.get("liveData", {})
        ls = live.get("linescore", {})
        teams = gd.get("teams", {})
        home_abbr = teams.get("home", {}).get("abbreviation", "")
        away_abbr = teams.get("away", {}).get("abbreviation", "")

        abstract = gd.get("status", {}).get("abstractGameState", "")
        detailed = gd.get("status", {}).get("detailedState", "")

        if abstract == "Preview":
            status = "PRE_GAME"
        elif abstract == "Live":
            status = "LIVE"
        elif abstract == "Final" or detailed in ("Final", "Completed Early", "Game Over"):
            status = "FINAL"
        else:
            status = "UNKNOWN"

        teams_ls = ls.get("teams", {})
        home_score = int(teams_ls.get("home", {}).get("runs") or 0)
        away_score = int(teams_ls.get("away", {}).get("runs") or 0)

        # Current state
        inning = int(ls.get("currentInning") or 0)
        is_top = (ls.get("inningHalf", "").lower() == "top") or (ls.get("isTopInning") is True)
        outs = int(ls.get("outs") or 0)
        if outs > 3:
            outs = 3

        offense = ls.get("offense", {}) or {}
        on1st = bool(offense.get("first"))
        on2nd = bool(offense.get("second"))
        on3rd = bool(offense.get("third"))
        r_code = RUNNER_CODE[(on1st, on2nd, on3rd)]

        batting_is_home = not is_top
        score_diff = (home_score - away_score) if batting_is_home else (away_score - home_score)

        return MLBGameState(
            game_pk=game_pk,
            status=status,
            inning=inning or 1,
            top_bottom=0 if is_top else 1,
            outs=outs,
            runners_on=r_code,
            home_score=home_score,
            away_score=away_score,
            score_diff=score_diff,
            total_runs_so_far=home_score + away_score,
            final_home_score=home_score if status == "FINAL" else 0,
            final_away_score=away_score if status == "FINAL" else 0,
            home_abbr=home_abbr,
            away_abbr=away_abbr,
        )
