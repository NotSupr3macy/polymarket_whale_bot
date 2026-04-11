"""
Live NBA game state feed via ESPN's public API.

Same architecture and output shape as live_feed_mlb.py but for NBA.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Optional

import aiohttp

from .data_acquisition.nba_puller import (
    REGULATION_GAME_SEC,
    REGULATION_PERIOD_SEC,
    OT_PERIOD_SEC,
    parse_clock_to_seconds,
)
from .team_mappings import match_nba_team


logger = logging.getLogger(__name__)

ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
ESPN_SUMMARY = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary"


@dataclass
class NBAGameState:
    """Normalized live NBA game state for model inference."""
    game_id: str
    status: str                   # "PRE_GAME", "LIVE", "FINAL", "UNKNOWN"
    period: int                   # 1-4 regulation, 5+ OT
    time_remaining_sec: int       # seconds remaining in current period
    game_time_elapsed_sec: int    # seconds elapsed since tipoff
    home_score: int
    away_score: int
    score_diff: int               # home - away
    total_points_so_far: int
    pace_estimate: float          # projected final total
    final_home_score: int
    final_away_score: int
    home_abbr: str
    away_abbr: str


class NBALiveFeed:
    def __init__(self, session: aiohttp.ClientSession) -> None:
        self._session = session

    async def find_games(
        self, target_date: date, home_abbr: Optional[str] = None,
        away_abbr: Optional[str] = None,
    ) -> list[dict]:
        """
        Return all NBA games on a date, optionally filtered to a specific matchup.
        """
        url = ESPN_SCOREBOARD
        params = {"dates": target_date.strftime("%Y%m%d"), "limit": "100"}
        try:
            async with self._session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    logger.warning("Scoreboard HTTP %d", resp.status)
                    return []
                data = await resp.json()
        except Exception as e:
            logger.warning("Scoreboard fetch error: %s", e)
            return []

        games: list[dict] = []
        for event in data.get("events", []):
            comps = event.get("competitions", [])
            if not comps:
                continue
            comp = comps[0]
            competitors = comp.get("competitors", [])
            home = next((c for c in competitors if c.get("homeAway") == "home"), None)
            away = next((c for c in competitors if c.get("homeAway") == "away"), None)
            if not home or not away:
                continue
            h_abbr_raw = home.get("team", {}).get("abbreviation", "")
            a_abbr_raw = away.get("team", {}).get("abbreviation", "")
            # Canonicalize ESPN abbrs (e.g. "SA" -> "SAS") to our internal ones
            h_abbr = match_nba_team(h_abbr_raw) or h_abbr_raw.upper()
            a_abbr = match_nba_team(a_abbr_raw) or a_abbr_raw.upper()

            want_home = home_abbr.upper() if home_abbr else None
            want_away = away_abbr.upper() if away_abbr else None
            if want_home and h_abbr != want_home:
                if not want_away or a_abbr != want_home:
                    continue
            if want_away and a_abbr != want_away:
                if not want_home or h_abbr != want_away:
                    continue

            status_type = event.get("status", {}).get("type", {})
            abstract = "UNKNOWN"
            if status_type.get("state") == "pre":
                abstract = "PRE_GAME"
            elif status_type.get("state") == "in":
                abstract = "LIVE"
            elif status_type.get("completed"):
                abstract = "FINAL"

            games.append({
                "game_id": str(event.get("id") or comp.get("id")),
                "home_abbr": h_abbr,
                "away_abbr": a_abbr,
                "status": abstract,
            })
        return games

    async def get_live_state(self, game_id: str) -> Optional[NBAGameState]:
        """Fetch the current state for a given NBA game_id."""
        url = ESPN_SUMMARY
        params = {"event": game_id}
        try:
            async with self._session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    logger.warning("Summary HTTP %d for %s", resp.status, game_id)
                    return None
                summary = await resp.json()
        except Exception as e:
            logger.warning("Summary fetch error for %s: %s", game_id, e)
            return None

        header = summary.get("header", {})
        comps = header.get("competitions", [])
        if not comps:
            return None
        comp = comps[0]
        competitors = comp.get("competitors", [])
        home = next((c for c in competitors if c.get("homeAway") == "home"), None)
        away = next((c for c in competitors if c.get("homeAway") == "away"), None)
        if not home or not away:
            return None

        home_abbr = home.get("team", {}).get("abbreviation", "")
        away_abbr = away.get("team", {}).get("abbreviation", "")
        try:
            home_score = int(home.get("score") or 0)
            away_score = int(away.get("score") or 0)
        except (TypeError, ValueError):
            home_score = 0
            away_score = 0

        # Status
        st = header.get("competitions", [{}])[0].get("status", {})
        state = (st.get("type", {}) or {}).get("state", "")
        completed = (st.get("type", {}) or {}).get("completed", False)
        if completed or state == "post":
            status = "FINAL"
        elif state == "in":
            status = "LIVE"
        elif state == "pre":
            status = "PRE_GAME"
        else:
            status = "UNKNOWN"

        # Current period + clock
        period = int((st.get("period") or 0))
        clock_str = st.get("displayClock", "")
        time_remaining = parse_clock_to_seconds(clock_str) or 0

        # Compute elapsed
        if status == "FINAL":
            # Estimate total game length: regulation + any OT periods
            ot_periods = max(0, period - 4)
            elapsed = REGULATION_GAME_SEC + ot_periods * OT_PERIOD_SEC
        elif period == 0:
            elapsed = 0
        elif period <= 4:
            elapsed = (period - 1) * REGULATION_PERIOD_SEC + (REGULATION_PERIOD_SEC - time_remaining)
        else:
            elapsed = REGULATION_GAME_SEC + (period - 5) * OT_PERIOD_SEC + (OT_PERIOD_SEC - time_remaining)

        total_points = home_score + away_score
        if elapsed > 0:
            pace_estimate = (total_points / elapsed) * REGULATION_GAME_SEC
        else:
            pace_estimate = 0.0

        return NBAGameState(
            game_id=game_id,
            status=status,
            period=period or 1,
            time_remaining_sec=time_remaining,
            game_time_elapsed_sec=max(0, elapsed),
            home_score=home_score,
            away_score=away_score,
            score_diff=home_score - away_score,
            total_points_so_far=total_points,
            pace_estimate=round(pace_estimate, 2),
            final_home_score=home_score if status == "FINAL" else 0,
            final_away_score=away_score if status == "FINAL" else 0,
            home_abbr=home_abbr,
            away_abbr=away_abbr,
        )
