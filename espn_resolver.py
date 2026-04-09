"""
ESPN-based fast resolution for sports markets.

Checks ESPN's free scoreboard API for final scores and resolves trades
immediately instead of waiting hours/days for Polymarket's official resolution.

Supports: NBA, NHL, MLB, NCAAB, NCAAF, NFL + European soccer
  (Premier League, La Liga, Bundesliga, Serie A, Ligue 1, Champions League)
Bet types: Moneyline, Spread, Over/Under, "Will X win?" markets
"""

import re
import time
import logging
import aiohttp
from dataclasses import dataclass

logger = logging.getLogger("espn_resolver")

# ESPN API endpoints (free, no auth required)
ESPN_ENDPOINTS = {
    # US Sports
    "nba": "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
    "nhl": "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard",
    "mlb": "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard",
    "ncaab": "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard",
    "ncaaf": "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard",
    "nfl": "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
    # European Soccer
    "epl": "https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard",
    "la_liga": "https://site.api.espn.com/apis/site/v2/sports/soccer/esp.1/scoreboard",
    "bundesliga": "https://site.api.espn.com/apis/site/v2/sports/soccer/ger.1/scoreboard",
    "serie_a": "https://site.api.espn.com/apis/site/v2/sports/soccer/ita.1/scoreboard",
    "ligue_1": "https://site.api.espn.com/apis/site/v2/sports/soccer/fra.1/scoreboard",
    "ucl": "https://site.api.espn.com/apis/site/v2/sports/soccer/uefa.champions/scoreboard",
    "uel": "https://site.api.espn.com/apis/site/v2/sports/soccer/uefa.europa/scoreboard",
    "mls": "https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1/scoreboard",
}

# Team name aliases: maps Polymarket names to ESPN names
# Only needed when they differ significantly
TEAM_ALIASES = {
    "blazers": "trail blazers",
    "niners": "49ers",
    "sixers": "76ers",
    "wolves": "timberwolves",
    # European soccer common abbreviations
    "psg": "paris saint-germain",
    "paris saint-germain fc": "paris saint-germain",
    "fc barcelona": "barcelona",
    "club atlético de madrid": "atletico madrid",
    "atlético madrid": "atletico madrid",
    "atletico de madrid": "atletico madrid",
    "bayern münchen": "bayern munich",
    "fc bayern münchen": "bayern munich",
    "borussia dortmund": "dortmund",
    "real madrid cf": "real madrid",
    "arsenal fc": "arsenal",
    "manchester united fc": "manchester united",
    "manchester city fc": "manchester city",
    "chelsea fc": "chelsea",
    "liverpool fc": "liverpool",
    "tottenham hotspur fc": "tottenham hotspur",
    "ac milan": "milan",
    "inter milan": "internazionale",
    "juventus fc": "juventus",
    "as roma": "roma",
    "ssc napoli": "napoli",
}


@dataclass
class GameResult:
    """Result from ESPN for a completed game."""
    home_team: str
    away_team: str
    home_score: int
    away_score: int
    winner: str  # Full team name (displayName)
    sport: str


@dataclass
class BetResolution:
    """Resolution of a specific bet type based on game result."""
    won: bool
    direction: str  # The winning direction/team
    margin: int  # Score difference
    total_points: int  # Combined score


def parse_market_title(title: str) -> dict:
    """
    Parse a Polymarket market title to extract bet type and parameters.

    Returns dict with keys: bet_type, teams, spread_value, ou_value, sport_hint
    bet_type: "moneyline" | "spread" | "over_under" | "unknown"
    """
    result = {
        "bet_type": "unknown",
        "teams": [],
        "spread_value": None,
        "ou_value": None,
        "team1": "",
        "team2": "",
    }

    # Spread: "Spread: Team (-6.5)" or "Spread: Team (+3.5)"
    spread_match = re.match(
        r"Spread:\s+(.+?)\s+\(([+-]?\d+\.?\d*)\)",
        title,
        re.IGNORECASE,
    )
    if spread_match:
        result["bet_type"] = "spread"
        result["team1"] = spread_match.group(1).strip()
        result["spread_value"] = float(spread_match.group(2))
        return result

    # Over/Under: "Team1 vs. Team2: O/U 232.5" or "Team1 vs Team2: O/U 6.5"
    ou_match = re.match(
        r"(.+?)\s+vs\.?\s+(.+?):\s+O/U\s+(\d+\.?\d*)",
        title,
        re.IGNORECASE,
    )
    if ou_match:
        result["bet_type"] = "over_under"
        result["team1"] = ou_match.group(1).strip()
        result["team2"] = ou_match.group(2).strip()
        result["ou_value"] = float(ou_match.group(3))
        return result

    # Moneyline: "Team1 vs. Team2" or "Team1 vs Team2"
    ml_match = re.match(
        r"(.+?)\s+vs\.?\s+(.+?)$",
        title,
        re.IGNORECASE,
    )
    if ml_match:
        result["bet_type"] = "moneyline"
        result["team1"] = ml_match.group(1).strip()
        result["team2"] = ml_match.group(2).strip()
        return result

    # "Will X win?" or "Will X win on YYYY-MM-DD?" — soccer/special moneyline
    will_win_match = re.match(
        r"Will\s+(.+?)\s+win(?:\s+on\s+\d{4}-\d{2}-\d{2})?\s*\??$",
        title,
        re.IGNORECASE,
    )
    if will_win_match:
        result["bet_type"] = "will_win"
        result["team1"] = will_win_match.group(1).strip()
        return result

    return result


def _normalize_team_name(name: str) -> str:
    """Normalize a team name for fuzzy matching."""
    name = name.lower().strip()
    # Remove common prefixes/suffixes
    for prefix in ["the ", "spread: "]:
        if name.startswith(prefix):
            name = name[len(prefix):]
    # Apply aliases
    for alias, canonical in TEAM_ALIASES.items():
        if alias in name:
            name = name.replace(alias, canonical)
    return name


def _teams_match(polymarket_name: str, espn_name: str) -> bool:
    """Check if a Polymarket team name matches an ESPN team name."""
    pm = _normalize_team_name(polymarket_name)
    espn = _normalize_team_name(espn_name)

    # Exact match
    if pm == espn:
        return True

    # One contains the other (handles "Hawks" matching "Atlanta Hawks")
    if pm in espn or espn in pm:
        return True

    # Word-level match: all words in the shorter name appear in the longer
    pm_words = set(pm.split())
    espn_words = set(espn.split())
    shorter = pm_words if len(pm_words) <= len(espn_words) else espn_words
    longer = espn_words if len(pm_words) <= len(espn_words) else pm_words
    if shorter and shorter.issubset(longer):
        return True

    return False


def _detect_sport(title: str) -> list[str]:
    """Guess which sport(s) a market might be from the title."""
    title_lower = title.lower()

    # ── European Soccer Detection ─────────────────────────────────
    soccer_keywords = [
        # Teams
        "barcelona", "fc barcelona", "real madrid", "atletico", "atlético",
        "bayern", "münchen", "dortmund", "borussia", "psg", "paris saint-germain",
        "arsenal", "chelsea", "liverpool", "manchester", "tottenham", "everton",
        "west ham", "newcastle", "aston villa", "crystal palace", "wolves",
        "juventus", "milan", "inter", "napoli", "roma", "lazio", "fiorentina",
        "lyon", "marseille", "monaco", "lille",
        # League markers
        "fc ", " fc", " cf", "cf ",
        "champions league", "europa league", "premier league",
        "la liga", "bundesliga", "serie a", "ligue 1",
    ]
    # "Will X win?" format is almost always soccer on Polymarket
    is_will_win = title_lower.startswith("will ") and "win" in title_lower

    if any(k in title_lower for k in soccer_keywords) or is_will_win:
        # Try to narrow down which league
        soccer_leagues = []
        epl_teams = ["arsenal", "chelsea", "liverpool", "manchester", "tottenham",
                     "everton", "west ham", "newcastle", "aston villa", "crystal palace",
                     "brighton", "fulham", "brentford", "bournemouth", "nottingham",
                     "wolverhampton", "leicester", "ipswich", "southampton"]
        la_liga_teams = ["barcelona", "real madrid", "atletico", "atlético", "sevilla",
                         "villarreal", "real sociedad", "betis", "athletic bilbao",
                         "celta", "valencia", "mallorca", "girona", "getafe", "osasuna"]
        bundesliga_teams = ["bayern", "münchen", "dortmund", "borussia", "leverkusen",
                            "leipzig", "wolfsburg", "freiburg", "hoffenheim", "stuttgart",
                            "frankfurt", "mainz", "augsburg", "werder", "union berlin"]
        serie_a_teams = ["juventus", "milan", "inter", "napoli", "roma", "lazio",
                         "fiorentina", "atalanta", "torino", "bologna", "monza",
                         "sassuolo", "verona", "udinese", "lecce", "empoli", "genoa"]
        ligue_1_teams = ["psg", "paris saint-germain", "marseille", "lyon", "monaco",
                         "lille", "nice", "rennes", "lens", "strasbourg", "toulouse",
                         "nantes", "montpellier", "reims", "brest", "clermont"]

        if any(t in title_lower for t in epl_teams):
            soccer_leagues.append("epl")
        if any(t in title_lower for t in la_liga_teams):
            soccer_leagues.append("la_liga")
        if any(t in title_lower for t in bundesliga_teams):
            soccer_leagues.append("bundesliga")
        if any(t in title_lower for t in serie_a_teams):
            soccer_leagues.append("serie_a")
        if any(t in title_lower for t in ligue_1_teams):
            soccer_leagues.append("ligue_1")

        # If no specific league detected, check all + UCL
        if not soccer_leagues:
            soccer_leagues = ["ucl", "uel", "epl", "la_liga", "bundesliga", "serie_a", "ligue_1", "mls"]

        return soccer_leagues

    # ── US Sports Detection ──────────────────────────────────────
    nhl_keywords = [
        "sharks", "ducks", "flames", "oilers", "canucks", "avalanche",
        "predators", "blackhawks", "red wings", "maple leafs", "bruins",
        "penguins", "flyers", "rangers", "islanders", "devils", "capitals",
        "hurricanes", "panthers", "lightning", "blue jackets", "kraken",
        "jets", "wild", "stars", "blues", "golden knights", "sabres",
        "senators", "canadiens",
    ]
    mlb_keywords = [
        "yankees", "mets", "dodgers", "angels", "cubs", "white sox",
        "red sox", "cardinals", "braves", "diamondbacks", "brewers",
        "reds", "pirates", "phillies", "nationals", "marlins", "rays",
        "orioles", "blue jays", "guardians", "twins", "royals", "tigers",
        "astros", "mariners", "padres", "giants", "rockies", "athletics",
    ]
    ncaab_keywords = [
        "huskies", "wolverines", "badgers", "fighting illini", "wildcats",
        "bulldogs", "bears", "longhorns", "seminoles", "crimson tide",
        "tigers", "gators",
    ]

    sports = []

    if any(k in title_lower for k in nhl_keywords):
        sports.append("nhl")
    if any(k in title_lower for k in mlb_keywords):
        sports.append("mlb")
    if any(k in title_lower for k in ncaab_keywords):
        sports.append("ncaab")

    # NBA is default for basketball-sounding markets not caught by NCAAB
    if not sports:
        sports.append("nba")

    # O/U with high totals = basketball, low = hockey/baseball
    ou_match = re.search(r"O/U\s+(\d+\.?\d*)", title)
    if ou_match:
        total = float(ou_match.group(1))
        if total > 100:
            if "nba" not in sports:
                sports.insert(0, "nba")
            if "ncaab" not in sports:
                sports.append("ncaab")
        elif total < 15:
            if "nhl" not in sports:
                sports.insert(0, "nhl")
            if "mlb" not in sports:
                sports.append("mlb")

    return sports


def resolve_bet(parsed: dict, game: GameResult) -> BetResolution | None:
    """
    Given a parsed market and a game result, determine if the bet won.

    Returns BetResolution or None if can't determine.
    """
    total = game.home_score + game.away_score
    margin = game.home_score - game.away_score  # Positive = home won

    if parsed["bet_type"] == "moneyline":
        return BetResolution(
            won=True,  # Will be set by caller based on direction
            direction=game.winner,
            margin=abs(margin),
            total_points=total,
        )

    elif parsed["bet_type"] == "over_under":
        ou_val = parsed["ou_value"]
        if total == ou_val:
            return None  # Push — can't resolve
        winning_direction = "OVER" if total > ou_val else "UNDER"
        return BetResolution(
            won=True,
            direction=winning_direction,
            margin=abs(total - int(ou_val)),
            total_points=total,
        )

    elif parsed["bet_type"] == "will_win":
        # "Will X win?" — team1 must be the winner (draw = LOSS for YES bettors)
        team = parsed["team1"]
        team_won = _teams_match(team, game.winner)
        return BetResolution(
            won=team_won,
            direction=game.winner,
            margin=abs(margin),
            total_points=total,
        )

    elif parsed["bet_type"] == "spread":
        spread_val = parsed["spread_value"]
        # Find which team is the spread team
        spread_team = parsed["team1"]

        # Determine if spread team is home or away
        if _teams_match(spread_team, game.home_team):
            adjusted_margin = game.home_score + spread_val - game.away_score
        elif _teams_match(spread_team, game.away_team):
            adjusted_margin = game.away_score + spread_val - game.home_score
        else:
            logger.warning("Could not match spread team '%s' to game teams", spread_team)
            return None

        if adjusted_margin == 0:
            return None  # Push
        winning_direction = spread_team if adjusted_margin > 0 else "opponent"
        # For Polymarket, the other side's name matters
        if adjusted_margin <= 0:
            # The spread team lost against the spread
            if _teams_match(spread_team, game.home_team):
                winning_direction = game.away_team
            else:
                winning_direction = game.home_team

        return BetResolution(
            won=adjusted_margin > 0,
            direction=winning_direction,
            margin=abs(adjusted_margin),
            total_points=total,
        )

    return None


class ESPNResolver:
    """
    Periodically checks ESPN for game results and resolves trades faster
    than Polymarket's official resolution.
    """

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None
        self._cache: dict[str, dict] = {}  # sport -> scoreboard data
        self._cache_time: dict[str, float] = {}
        self._cache_ttl = 60.0  # Refresh every 60s
        self._resolved_trades: set[str] = set()  # trade_ids already resolved

    async def _ensure_session(self) -> None:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
            )

    async def _fetch_scoreboard(self, sport: str) -> dict | None:
        """Fetch scoreboard for a sport, with caching."""
        now = time.time()
        if sport in self._cache and (now - self._cache_time.get(sport, 0)) < self._cache_ttl:
            return self._cache[sport]

        await self._ensure_session()
        url = ESPN_ENDPOINTS.get(sport)
        if not url:
            return None

        try:
            async with self._session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self._cache[sport] = data
                    self._cache_time[sport] = now
                    return data
                else:
                    logger.warning("ESPN API returned %d for %s", resp.status, sport)
                    return None
        except Exception as e:
            logger.warning("ESPN API error for %s: %s", sport, e)
            return None

    def _find_game(self, data: dict, team1: str, team2: str) -> GameResult | None:
        """Find a completed game matching the given teams."""
        for event in data.get("events", []):
            competition = event.get("competitions", [{}])[0]
            status = competition.get("status", {}).get("type", {})

            if not status.get("completed", False):
                continue

            competitors = competition.get("competitors", [])
            if len(competitors) < 2:
                continue

            home = competitors[0]
            away = competitors[1]
            home_name = home.get("team", {}).get("displayName", "")
            away_name = away.get("team", {}).get("displayName", "")
            home_short = home.get("team", {}).get("shortDisplayName", "")
            away_short = away.get("team", {}).get("shortDisplayName", "")

            # Check if teams match (try both orderings)
            match = False
            if team1 and team2:
                match = (
                    (_teams_match(team1, home_name) or _teams_match(team1, home_short))
                    and (_teams_match(team2, away_name) or _teams_match(team2, away_short))
                ) or (
                    (_teams_match(team1, away_name) or _teams_match(team1, away_short))
                    and (_teams_match(team2, home_name) or _teams_match(team2, home_short))
                )
            elif team1:
                # Spread/single team — just find the game with this team
                match = (
                    _teams_match(team1, home_name) or _teams_match(team1, home_short)
                    or _teams_match(team1, away_name) or _teams_match(team1, away_short)
                )

            if not match:
                continue

            try:
                home_score = int(home.get("score", "0"))
                away_score = int(away.get("score", "0"))
            except (ValueError, TypeError):
                continue

            winner = home_name if home.get("winner") else away_name

            return GameResult(
                home_team=home_name,
                away_team=away_name,
                home_score=home_score,
                away_score=away_score,
                winner=winner,
                sport=event.get("shortName", ""),
            )

        return None

    async def check_resolution(
        self, trade_id: str, market_title: str, direction: str,
    ) -> dict | None:
        """
        Check if a trade can be resolved via ESPN.

        Returns dict with keys: won, winning_direction, total_points, margin
        Or None if game not found or not yet final.
        """
        if trade_id in self._resolved_trades:
            return None

        parsed = parse_market_title(market_title)
        if parsed["bet_type"] == "unknown":
            return None

        sports = _detect_sport(market_title)

        for sport in sports:
            data = await self._fetch_scoreboard(sport)
            if not data:
                continue

            game = self._find_game(data, parsed["team1"], parsed.get("team2", ""))
            if not game:
                continue

            resolution = resolve_bet(parsed, game)
            if not resolution:
                continue

            # Determine if OUR direction won
            our_direction = direction.upper()
            if parsed["bet_type"] == "over_under":
                won = resolution.direction.upper() == our_direction
            elif parsed["bet_type"] == "moneyline":
                won = _teams_match(direction, resolution.direction)
            elif parsed["bet_type"] == "spread":
                won = _teams_match(direction, resolution.direction)
            elif parsed["bet_type"] == "will_win":
                # YES = team wins, NO = team doesn't win (loss or draw)
                if our_direction == "YES":
                    won = resolution.won
                elif our_direction == "NO":
                    won = not resolution.won
                else:
                    # Direction might be team name — match against winner
                    won = _teams_match(direction, resolution.direction)
            else:
                continue

            self._resolved_trades.add(trade_id)

            logger.info(
                "ESPN RESOLVED: %s | %s -> %s (%s) | Score: %d-%d | %s",
                market_title[:50],
                direction,
                "WIN" if won else "LOSS",
                resolution.direction,
                game.home_score,
                game.away_score,
                game.sport,
            )

            return {
                "won": won,
                "winning_direction": resolution.direction,
                "total_points": resolution.total_points,
                "margin": resolution.margin,
                "home_team": game.home_team,
                "away_team": game.away_team,
                "home_score": game.home_score,
                "away_score": game.away_score,
            }

        return None

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
