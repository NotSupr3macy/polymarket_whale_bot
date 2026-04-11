"""
Texaskid position manager for the EV engine.

Loads open positions from the trades.db `texaskid_positions` table, enriches
each with:
  - Polymarket cashout quote (CLOB midpoint + bid/ask)
  - Matched live game state (MLB or NBA)
  - Parsed bet type / line / team

This module is the bridge between "what does Texaskid currently hold?" and
"what does our model think is the fair probability of that bet resolving?".

Downstream (decision_engine.py) will compare our estimated probability vs
the cashout price to decide whether to alert.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import aiohttp

from .live_feed_mlb import MLBGameState, MLBLiveFeed
from .live_feed_nba import NBAGameState, NBALiveFeed
from .polymarket_client import CashoutQuote, PolymarketClient
from .team_mappings import (
    detect_sport,
    match_mlb_team,
    match_nba_team,
)


logger = logging.getLogger(__name__)

DEFAULT_DB = Path(__file__).resolve().parent.parent / "trades.db"


@dataclass
class BetParse:
    """Parsed bet type from a market title + direction."""
    sport: Optional[str]          # "MLB" | "NBA" | None (other sports ignored)
    bet_type: Optional[str]       # "moneyline" | "spread" | "over_under"
    team1_abbr: Optional[str]     # home/away team from title (ordering not yet resolved)
    team2_abbr: Optional[str]
    line: Optional[float]         # raw spread/total line from the title
    direction: str                # the user's bet side (as stored)
    picked_team_abbr: Optional[str]  # for moneyline/spread: which team user picked


@dataclass
class TexaskidPosition:
    """A single open position enriched with live state + cashout."""
    condition_id: str
    direction: str
    market_title: str
    current_size_usd: float
    first_seen_price: float
    current_price_db: float       # what the DB last recorded (may be stale)
    last_updated: Optional[str]

    # Enrichment (filled in async)
    parse: Optional[BetParse] = None
    cashout: Optional[CashoutQuote] = None
    mlb_state: Optional[MLBGameState] = None
    nba_state: Optional[NBAGameState] = None
    error: Optional[str] = None

    @property
    def sport(self) -> Optional[str]:
        return self.parse.sport if self.parse else None

    @property
    def has_live_state(self) -> bool:
        return self.mlb_state is not None or self.nba_state is not None

    @property
    def live_status(self) -> Optional[str]:
        if self.mlb_state:
            return self.mlb_state.status
        if self.nba_state:
            return self.nba_state.status
        return None


class PositionManager:
    """Loads and enriches texaskid positions."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        db_path: Path = DEFAULT_DB,
    ) -> None:
        self._session = session
        self._db_path = db_path
        self.poly = PolymarketClient(session)
        self.mlb_feed = MLBLiveFeed(session)
        self.nba_feed = NBALiveFeed(session)

    # ─────────────────────────────────────────────────────────
    #  DB load
    # ─────────────────────────────────────────────────────────

    def load_open_positions(self) -> list[TexaskidPosition]:
        """Return all open texaskid positions from the DB."""
        if not self._db_path.exists():
            logger.error("DB not found: %s", self._db_path)
            return []

        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                """
                SELECT condition_id, direction, market_title, current_size_usd,
                       first_seen_price, current_price, last_updated
                FROM texaskid_positions
                WHERE status = 'open'
                ORDER BY current_size_usd DESC
                """
            ).fetchall()
        finally:
            conn.close()

        positions = []
        for r in rows:
            positions.append(TexaskidPosition(
                condition_id=r["condition_id"],
                direction=r["direction"] or "",
                market_title=r["market_title"] or "",
                current_size_usd=float(r["current_size_usd"] or 0),
                first_seen_price=float(r["first_seen_price"] or 0),
                current_price_db=float(r["current_price"] or 0),
                last_updated=r["last_updated"],
            ))
        return positions

    # ─────────────────────────────────────────────────────────
    #  Title parsing
    # ─────────────────────────────────────────────────────────

    def parse_bet(self, title: str, direction: str) -> BetParse:
        """
        Parse a texaskid market_title + direction into a structured bet.

        Examples:
          "Mavericks vs. Spurs: O/U 237.5", "Under"
          "Spread: Philadelphia Phillies (-1.5)", "Arizona Diamondbacks"
          "New York Yankees vs. Tampa Bay Rays", "Tampa Bay Rays"
        """
        t = (title or "").strip()
        d = (direction or "").strip()
        sport = detect_sport(t)

        bet_type: Optional[str] = None
        line: Optional[float] = None
        team1_abbr: Optional[str] = None
        team2_abbr: Optional[str] = None
        picked: Optional[str] = None

        lower = t.lower()
        matcher = match_mlb_team if sport == "MLB" else (match_nba_team if sport == "NBA" else None)

        # ── Over / Under ──
        if "o/u" in lower or "over/under" in lower or "total" in lower:
            bet_type = "over_under"
            m = re.search(r"o/u\s*(\d+\.?\d*)", lower)
            if not m:
                m = re.search(r"(\d+\.?\d*)\s*$", t)
            if m:
                try:
                    line = float(m.group(1))
                except ValueError:
                    pass
            # Teams before the colon
            before = t.split(":")[0] if ":" in t else t
            vs = re.search(r"(.+?)\s+vs\.?\s+(.+)", before, re.IGNORECASE)
            if vs and matcher:
                team1_abbr = matcher(vs.group(1).strip())
                team2_abbr = matcher(vs.group(2).strip())

        # ── Spread ──
        elif "spread" in lower or re.search(r"\([-+]?\d+\.?\d*\)", t):
            bet_type = "spread"
            m = re.search(r"\(([-+]?\d+\.?\d*)\)", t)
            if m:
                try:
                    line = float(m.group(1))
                except ValueError:
                    pass
            if matcher:
                # The title names one team (the favorite); the other inferred from game match
                favorite = matcher(t.replace("Spread:", "").strip())
                team1_abbr = favorite
                picked = matcher(d) if d else None

        # ── Moneyline ──
        elif " vs" in lower or " @ " in lower or " at " in lower:
            bet_type = "moneyline"
            vs = re.search(r"(.+?)\s+(?:vs\.?|@|at)\s+(.+)", t, re.IGNORECASE)
            if vs and matcher:
                team1_abbr = matcher(vs.group(1).strip())
                team2_abbr = matcher(vs.group(2).strip())
            if matcher and d:
                picked = matcher(d)

        if bet_type in ("moneyline", "spread") and picked is None and matcher and d:
            picked = matcher(d)

        return BetParse(
            sport=sport,
            bet_type=bet_type,
            team1_abbr=team1_abbr,
            team2_abbr=team2_abbr,
            line=line,
            direction=d,
            picked_team_abbr=picked,
        )

    # ─────────────────────────────────────────────────────────
    #  Enrichment: cashout + live game state
    # ─────────────────────────────────────────────────────────

    async def enrich(self, pos: TexaskidPosition) -> TexaskidPosition:
        """Fill in parse/cashout/live_state for one position."""
        pos.parse = self.parse_bet(pos.market_title, pos.direction)

        # 1) Cashout quote — works for any sport
        try:
            pos.cashout = await self.poly.get_cashout_quote(pos.condition_id, pos.direction)
        except Exception as e:
            pos.error = f"cashout_error: {e}"

        # 2) Live game state — MLB/NBA only
        if pos.parse.sport == "MLB" and (pos.parse.team1_abbr or pos.parse.team2_abbr):
            state = await self._resolve_mlb_game(pos.parse)
            if state:
                pos.mlb_state = state
        elif pos.parse.sport == "NBA" and (pos.parse.team1_abbr or pos.parse.team2_abbr):
            state = await self._resolve_nba_game(pos.parse)
            if state:
                pos.nba_state = state

        return pos

    async def enrich_all(self, positions: list[TexaskidPosition]) -> list[TexaskidPosition]:
        """Enrich every position sequentially (low QPS, avoids rate limits)."""
        for p in positions:
            await self.enrich(p)
        return positions

    # ─────────────────────────────────────────────────────────
    #  Game matching
    # ─────────────────────────────────────────────────────────

    async def _resolve_mlb_game(self, parse: BetParse) -> Optional[MLBGameState]:
        candidates: list[dict] = []
        # Search today and yesterday (for late-night games that rolled over)
        for day in _candidate_dates():
            games = await self.mlb_feed.find_games(
                day, home_abbr=parse.team1_abbr, away_abbr=parse.team2_abbr,
            )
            if not games and parse.team2_abbr:
                games = await self.mlb_feed.find_games(
                    day, home_abbr=parse.team2_abbr, away_abbr=parse.team1_abbr,
                )
            candidates.extend(games)
            if candidates:
                break

        if not candidates:
            return None

        # Prefer LIVE, then PRE, then latest
        candidates.sort(key=lambda g: _status_order(g.get("status", "")))
        for g in candidates:
            state = await self.mlb_feed.get_live_state(int(g["game_pk"]))
            if state and state.status != "UNKNOWN":
                return state
        return None

    async def _resolve_nba_game(self, parse: BetParse) -> Optional[NBAGameState]:
        candidates: list[dict] = []
        for day in _candidate_dates():
            games = await self.nba_feed.find_games(
                day, home_abbr=parse.team1_abbr, away_abbr=parse.team2_abbr,
            )
            if not games and parse.team2_abbr:
                games = await self.nba_feed.find_games(
                    day, home_abbr=parse.team2_abbr, away_abbr=parse.team1_abbr,
                )
            candidates.extend(games)
            if candidates:
                break

        if not candidates:
            return None

        candidates.sort(key=lambda g: _status_order(g.get("status", "")))
        for g in candidates:
            state = await self.nba_feed.get_live_state(str(g["game_id"]))
            if state and state.status != "UNKNOWN":
                return state
        return None


# ─────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────

_PST = timezone(timedelta(hours=-7))


def _candidate_dates() -> list[date]:
    """Today + yesterday in PST (covers late-night rollover)."""
    now = datetime.now(_PST).date()
    return [now, now - timedelta(days=1)]


def _status_order(status: str) -> int:
    """Sort key: LIVE first, then PRE_GAME, then others, then FINAL."""
    s = status.upper()
    if s in ("LIVE", "IN"):
        return 0
    if s in ("PRE_GAME", "PREVIEW", "SCHEDULED"):
        return 1
    if "FINAL" in s:
        return 3
    return 2
