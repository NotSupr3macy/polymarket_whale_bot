"""
Team name mappings for MLB and NBA.

Handles Polymarket slug parsing, MLB Stats API team IDs, ESPN team abbreviations,
and fuzzy matching from market titles to team identities.

Polymarket slug examples:
    mlb-phi-ari-2026-04-10          (Phillies @ Diamondbacks)
    nba-lal-bos-2026-04-10          (Lakers @ Celtics)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Optional


# ─────────────────────────────────────────────────────────────────────
#  MLB Teams (30)
# ─────────────────────────────────────────────────────────────────────
# Format: abbr -> (full_name, mlb_api_id, espn_abbr, alt_names)

MLB_TEAMS: dict[str, dict] = {
    # AL East
    "NYY": {"name": "New York Yankees",       "mlb_id": 147, "espn": "nyy", "alts": ["yankees", "new york yankees", "ny yankees"]},
    "BOS": {"name": "Boston Red Sox",         "mlb_id": 111, "espn": "bos", "alts": ["red sox", "boston red sox", "bosox"]},
    "TB":  {"name": "Tampa Bay Rays",         "mlb_id": 139, "espn": "tb",  "alts": ["rays", "tampa bay rays", "tampa bay", "tampa"]},
    "TOR": {"name": "Toronto Blue Jays",      "mlb_id": 141, "espn": "tor", "alts": ["blue jays", "toronto blue jays", "bluejays", "jays"]},
    "BAL": {"name": "Baltimore Orioles",      "mlb_id": 110, "espn": "bal", "alts": ["orioles", "baltimore orioles", "os"]},
    # AL Central
    "CLE": {"name": "Cleveland Guardians",    "mlb_id": 114, "espn": "cle", "alts": ["guardians", "cleveland guardians", "cleveland"]},
    "MIN": {"name": "Minnesota Twins",        "mlb_id": 142, "espn": "min", "alts": ["twins", "minnesota twins", "minnesota"]},
    "KC":  {"name": "Kansas City Royals",     "mlb_id": 118, "espn": "kc",  "alts": ["royals", "kansas city royals", "kansas city"]},
    "DET": {"name": "Detroit Tigers",         "mlb_id": 116, "espn": "det", "alts": ["tigers", "detroit tigers", "detroit"]},
    "CWS": {"name": "Chicago White Sox",      "mlb_id": 145, "espn": "cws", "alts": ["white sox", "chicago white sox", "whitesox", "chi sox", "cws", "chw"]},
    # AL West
    "HOU": {"name": "Houston Astros",         "mlb_id": 117, "espn": "hou", "alts": ["astros", "houston astros", "houston"]},
    "TEX": {"name": "Texas Rangers",          "mlb_id": 140, "espn": "tex", "alts": ["rangers", "texas rangers"]},
    "SEA": {"name": "Seattle Mariners",       "mlb_id": 136, "espn": "sea", "alts": ["mariners", "seattle mariners", "seattle"]},
    "LAA": {"name": "Los Angeles Angels",     "mlb_id": 108, "espn": "laa", "alts": ["angels", "la angels", "los angeles angels", "anaheim angels"]},
    "OAK": {"name": "Athletics",              "mlb_id": 133, "espn": "oak", "alts": ["athletics", "oakland athletics", "oakland", "a's", "as", "ath"]},
    # NL East
    "PHI": {"name": "Philadelphia Phillies",  "mlb_id": 143, "espn": "phi", "alts": ["phillies", "philadelphia phillies", "philadelphia", "philly"]},
    "ATL": {"name": "Atlanta Braves",         "mlb_id": 144, "espn": "atl", "alts": ["braves", "atlanta braves", "atlanta"]},
    "NYM": {"name": "New York Mets",          "mlb_id": 121, "espn": "nym", "alts": ["mets", "new york mets", "ny mets"]},
    "WSH": {"name": "Washington Nationals",   "mlb_id": 120, "espn": "wsh", "alts": ["nationals", "washington nationals", "washington", "nats", "was"]},
    "MIA": {"name": "Miami Marlins",          "mlb_id": 146, "espn": "mia", "alts": ["marlins", "miami marlins", "miami", "florida marlins"]},
    # NL Central
    "MIL": {"name": "Milwaukee Brewers",      "mlb_id": 158, "espn": "mil", "alts": ["brewers", "milwaukee brewers", "milwaukee"]},
    "CHC": {"name": "Chicago Cubs",           "mlb_id": 112, "espn": "chc", "alts": ["cubs", "chicago cubs", "chi cubs", "chc"]},
    "STL": {"name": "St. Louis Cardinals",    "mlb_id": 138, "espn": "stl", "alts": ["cardinals", "st louis cardinals", "st. louis cardinals", "st louis", "cards"]},
    "CIN": {"name": "Cincinnati Reds",        "mlb_id": 113, "espn": "cin", "alts": ["reds", "cincinnati reds", "cincinnati"]},
    "PIT": {"name": "Pittsburgh Pirates",     "mlb_id": 134, "espn": "pit", "alts": ["pirates", "pittsburgh pirates", "pittsburgh", "bucs"]},
    # NL West
    "LAD": {"name": "Los Angeles Dodgers",    "mlb_id": 119, "espn": "lad", "alts": ["dodgers", "los angeles dodgers", "la dodgers"]},
    "SD":  {"name": "San Diego Padres",       "mlb_id": 135, "espn": "sd",  "alts": ["padres", "san diego padres", "san diego", "sdp"]},
    "ARI": {"name": "Arizona Diamondbacks",   "mlb_id": 109, "espn": "ari", "alts": ["diamondbacks", "arizona diamondbacks", "arizona", "dbacks", "d-backs"]},
    "SF":  {"name": "San Francisco Giants",   "mlb_id": 137, "espn": "sf",  "alts": ["giants", "san francisco giants", "san francisco", "sfg"]},
    "COL": {"name": "Colorado Rockies",       "mlb_id": 115, "espn": "col", "alts": ["rockies", "colorado rockies", "colorado"]},
}


# ─────────────────────────────────────────────────────────────────────
#  NBA Teams (30)
# ─────────────────────────────────────────────────────────────────────
# Format: abbr -> (full_name, espn_abbr, alt_names)

NBA_TEAMS: dict[str, dict] = {
    # Eastern Conference — Atlantic
    "BOS": {"name": "Boston Celtics",          "espn": "bos", "alts": ["celtics", "boston celtics", "boston"]},
    "NYK": {"name": "New York Knicks",         "espn": "ny",  "alts": ["knicks", "new york knicks", "ny knicks", "nyk"]},
    "PHI": {"name": "Philadelphia 76ers",      "espn": "phi", "alts": ["76ers", "philadelphia 76ers", "sixers", "phila 76ers"]},
    "BKN": {"name": "Brooklyn Nets",           "espn": "bkn", "alts": ["nets", "brooklyn nets", "brooklyn"]},
    "TOR": {"name": "Toronto Raptors",         "espn": "tor", "alts": ["raptors", "toronto raptors", "toronto"]},
    # Eastern Conference — Central
    "MIL": {"name": "Milwaukee Bucks",         "espn": "mil", "alts": ["bucks", "milwaukee bucks", "milwaukee"]},
    "CLE": {"name": "Cleveland Cavaliers",     "espn": "cle", "alts": ["cavaliers", "cleveland cavaliers", "cavs", "cleveland"]},
    "IND": {"name": "Indiana Pacers",          "espn": "ind", "alts": ["pacers", "indiana pacers", "indiana"]},
    "CHI": {"name": "Chicago Bulls",           "espn": "chi", "alts": ["bulls", "chicago bulls", "chicago"]},
    "DET": {"name": "Detroit Pistons",         "espn": "det", "alts": ["pistons", "detroit pistons", "detroit"]},
    # Eastern Conference — Southeast
    "MIA": {"name": "Miami Heat",              "espn": "mia", "alts": ["heat", "miami heat", "miami"]},
    "ORL": {"name": "Orlando Magic",           "espn": "orl", "alts": ["magic", "orlando magic", "orlando"]},
    "ATL": {"name": "Atlanta Hawks",           "espn": "atl", "alts": ["hawks", "atlanta hawks", "atlanta"]},
    "WAS": {"name": "Washington Wizards",      "espn": "wsh", "alts": ["wizards", "washington wizards", "washington", "wsh"]},
    "CHA": {"name": "Charlotte Hornets",       "espn": "cha", "alts": ["hornets", "charlotte hornets", "charlotte"]},
    # Western Conference — Northwest
    "DEN": {"name": "Denver Nuggets",          "espn": "den", "alts": ["nuggets", "denver nuggets", "denver"]},
    "OKC": {"name": "Oklahoma City Thunder",   "espn": "okc", "alts": ["thunder", "oklahoma city thunder", "oklahoma city", "okc thunder"]},
    "MIN": {"name": "Minnesota Timberwolves",  "espn": "min", "alts": ["timberwolves", "minnesota timberwolves", "minnesota", "wolves", "twolves"]},
    "POR": {"name": "Portland Trail Blazers",  "espn": "por", "alts": ["trail blazers", "portland trail blazers", "blazers", "portland"]},
    "UTA": {"name": "Utah Jazz",               "espn": "utah", "alts": ["jazz", "utah jazz", "utah"]},
    # Western Conference — Pacific
    "LAL": {"name": "Los Angeles Lakers",      "espn": "lal", "alts": ["lakers", "los angeles lakers", "la lakers"]},
    "LAC": {"name": "LA Clippers",             "espn": "lac", "alts": ["clippers", "la clippers", "los angeles clippers"]},
    "PHX": {"name": "Phoenix Suns",            "espn": "phx", "alts": ["suns", "phoenix suns", "phoenix"]},
    "GSW": {"name": "Golden State Warriors",   "espn": "gs",  "alts": ["warriors", "golden state warriors", "golden state", "gsw", "dubs"]},
    "SAC": {"name": "Sacramento Kings",        "espn": "sac", "alts": ["kings", "sacramento kings", "sacramento"]},
    # Western Conference — Southwest
    "DAL": {"name": "Dallas Mavericks",        "espn": "dal", "alts": ["mavericks", "dallas mavericks", "dallas", "mavs"]},
    "HOU": {"name": "Houston Rockets",         "espn": "hou", "alts": ["rockets", "houston rockets", "houston"]},
    "MEM": {"name": "Memphis Grizzlies",       "espn": "mem", "alts": ["grizzlies", "memphis grizzlies", "memphis", "griz"]},
    "NOP": {"name": "New Orleans Pelicans",    "espn": "no",  "alts": ["pelicans", "new orleans pelicans", "new orleans", "pels"]},
    "SAS": {"name": "San Antonio Spurs",       "espn": "sa",  "alts": ["spurs", "san antonio spurs", "san antonio"]},
}


# ─────────────────────────────────────────────────────────────────────
#  Reverse lookup indexes (built once on import)
# ─────────────────────────────────────────────────────────────────────

def _build_name_index(teams: dict[str, dict]) -> dict[str, str]:
    """Map lowercased name / alias / abbr / espn-abbr -> canonical abbr."""
    idx: dict[str, str] = {}
    for abbr, info in teams.items():
        idx[abbr.lower()] = abbr
        idx[info["name"].lower()] = abbr
        espn = info.get("espn")
        if espn:
            idx[espn.lower()] = abbr
        for alt in info["alts"]:
            idx[alt.lower()] = abbr
    return idx


_MLB_NAME_INDEX = _build_name_index(MLB_TEAMS)
_NBA_NAME_INDEX = _build_name_index(NBA_TEAMS)


# ─────────────────────────────────────────────────────────────────────
#  Dataclasses
# ─────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ParsedSlug:
    """Result of parsing a Polymarket market slug."""
    sport: str                  # "MLB" or "NBA"
    team1_abbr: str             # First team in the slug (often away team)
    team2_abbr: str             # Second team in the slug (often home team)
    team1_name: str
    team2_name: str
    game_date: Optional[date]   # Game date if present in slug
    raw_slug: str


# ─────────────────────────────────────────────────────────────────────
#  Public lookup functions
# ─────────────────────────────────────────────────────────────────────

def match_mlb_team(text: str) -> Optional[str]:
    """Fuzzy-match any text to an MLB team abbreviation. Returns None if no match."""
    if not text:
        return None
    t = text.strip().lower()
    if t in _MLB_NAME_INDEX:
        return _MLB_NAME_INDEX[t]
    # Try substring match — longest alias wins to avoid e.g. "NY" matching both NYY and NYM
    best: tuple[str, int] = ("", 0)
    for alias, abbr in _MLB_NAME_INDEX.items():
        if len(alias) >= 4 and alias in t and len(alias) > best[1]:
            best = (abbr, len(alias))
    return best[0] or None


def match_nba_team(text: str) -> Optional[str]:
    """Fuzzy-match any text to an NBA team abbreviation. Returns None if no match."""
    if not text:
        return None
    t = text.strip().lower()
    if t in _NBA_NAME_INDEX:
        return _NBA_NAME_INDEX[t]
    best: tuple[str, int] = ("", 0)
    for alias, abbr in _NBA_NAME_INDEX.items():
        if len(alias) >= 4 and alias in t and len(alias) > best[1]:
            best = (abbr, len(alias))
    return best[0] or None


def get_mlb_team_id(abbr: str) -> Optional[int]:
    """Get the MLB Stats API numeric team ID for an abbreviation."""
    info = MLB_TEAMS.get(abbr.upper())
    return info["mlb_id"] if info else None


def get_mlb_team_name(abbr: str) -> Optional[str]:
    info = MLB_TEAMS.get(abbr.upper())
    return info["name"] if info else None


def get_nba_team_name(abbr: str) -> Optional[str]:
    info = NBA_TEAMS.get(abbr.upper())
    return info["name"] if info else None


def detect_sport(text: str) -> Optional[str]:
    """Guess sport from a market title or slug. Returns 'MLB', 'NBA', or None."""
    if not text:
        return None
    t = text.lower()
    # Explicit league keywords
    if "mlb" in t or any(w in t for w in ["inning", "innings", "strikeout", "home run"]):
        return "MLB"
    if "nba" in t or any(w in t for w in ["quarter", "points in the game"]):
        return "NBA"
    # Team-name detection (longer aliases win)
    mlb_hits = sum(1 for alias in _MLB_NAME_INDEX if len(alias) >= 5 and alias in t)
    nba_hits = sum(1 for alias in _NBA_NAME_INDEX if len(alias) >= 5 and alias in t)
    if mlb_hits > nba_hits:
        return "MLB"
    if nba_hits > mlb_hits:
        return "NBA"
    return None


# ─────────────────────────────────────────────────────────────────────
#  Polymarket slug parsing
# ─────────────────────────────────────────────────────────────────────

# Matches patterns like:
#   mlb-phi-ari-2026-04-10
#   nba-lal-bos-2026-04-10
#   mlb-phi-ari
_SLUG_RE = re.compile(
    r"^(mlb|nba)-([a-z]{2,4})-([a-z]{2,4})(?:-(\d{4}-\d{2}-\d{2}))?",
    re.IGNORECASE,
)


def parse_polymarket_slug(slug: str) -> Optional[ParsedSlug]:
    """
    Parse a Polymarket market slug into sport + two teams + date.

    Returns None if the slug doesn't match the expected pattern or teams
    can't be matched. Handles both team orderings.
    """
    if not slug:
        return None
    m = _SLUG_RE.match(slug.strip().lower())
    if not m:
        return None

    sport = m.group(1).upper()
    raw_t1 = m.group(2)
    raw_t2 = m.group(3)
    date_str = m.group(4)

    if sport == "MLB":
        t1 = match_mlb_team(raw_t1)
        t2 = match_mlb_team(raw_t2)
    else:
        t1 = match_nba_team(raw_t1)
        t2 = match_nba_team(raw_t2)

    if not t1 or not t2:
        return None

    game_date = None
    if date_str:
        try:
            game_date = date.fromisoformat(date_str)
        except ValueError:
            pass

    t1_name = get_mlb_team_name(t1) if sport == "MLB" else get_nba_team_name(t1)
    t2_name = get_mlb_team_name(t2) if sport == "MLB" else get_nba_team_name(t2)

    return ParsedSlug(
        sport=sport,
        team1_abbr=t1,
        team2_abbr=t2,
        team1_name=t1_name or t1,
        team2_name=t2_name or t2,
        game_date=game_date,
        raw_slug=slug,
    )


def parse_market_title(title: str) -> Optional[dict]:
    """
    Extract teams and market type from a natural-language market title.

    Examples:
        "Athletics vs. New York Yankees"           -> moneyline OAK vs NYY
        "Spread: Phillies (-1.5)"                  -> spread PHI -1.5
        "Phillies vs. Diamondbacks: O/U 8.5"       -> over_under PHI/ARI 8.5
        "Will Lakers win on 2026-04-10?"           -> moneyline LAL (YES=win)

    Returns dict with: {sport, bet_type, team1, team2, line, date} or None.
    Any field may be None if not present in the title.
    """
    if not title:
        return None
    t = title.strip()
    result: dict = {
        "sport": detect_sport(t),
        "bet_type": None,
        "team1": None,
        "team2": None,
        "line": None,
    }

    # Detect bet type
    lower = t.lower()
    if "o/u" in lower or "over/under" in lower or "total" in lower:
        result["bet_type"] = "over_under"
        # Extract line number
        m = re.search(r"(\d+\.?\d*)\s*(?:runs?|points?)?$", t)
        if m:
            result["line"] = float(m.group(1))
        else:
            m = re.search(r"o/u\s*(\d+\.?\d*)", lower)
            if m:
                result["line"] = float(m.group(1))
    elif "spread" in lower or re.search(r"\([-+]\d+\.?\d*\)", t):
        result["bet_type"] = "spread"
        m = re.search(r"\(([-+]?\d+\.?\d*)\)", t)
        if m:
            result["line"] = float(m.group(1))
    elif " vs" in lower or " @ " in lower or "will" in lower:
        result["bet_type"] = "moneyline"

    # Extract teams — look for both MLB and NBA hits
    if result["sport"] == "MLB":
        matcher = match_mlb_team
    elif result["sport"] == "NBA":
        matcher = match_nba_team
    else:
        matcher = None

    if matcher:
        # Try "X vs Y" or "X @ Y" patterns first
        vs_match = re.search(r"([A-Za-z .'-]+?)\s+(?:vs\.?|@|at)\s+([A-Za-z .'-]+?)(?:\s*[:,]|$)", t, re.IGNORECASE)
        if vs_match:
            result["team1"] = matcher(vs_match.group(1).strip())
            result["team2"] = matcher(vs_match.group(2).strip())
        else:
            # Fallback: scan the whole string for any team
            result["team1"] = matcher(t)

    return result


# ─────────────────────────────────────────────────────────────────────
#  Self-test
# ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        # MLB
        ("mlb-phi-ari-2026-04-10", "MLB", "PHI", "ARI"),
        ("MLB-LAD-SF", "MLB", "LAD", "SF"),
        # NBA
        ("nba-lal-bos-2026-04-10", "NBA", "LAL", "BOS"),
        ("nba-gsw-sac", "NBA", "GSW", "SAC"),
    ]
    print("Slug parsing tests:")
    for slug, exp_sport, exp_t1, exp_t2 in tests:
        parsed = parse_polymarket_slug(slug)
        ok = parsed and parsed.sport == exp_sport and parsed.team1_abbr == exp_t1 and parsed.team2_abbr == exp_t2
        status = "OK" if ok else "FAIL"
        print(f"  [{status}] {slug} -> {parsed}")

    title_tests = [
        "Athletics vs. New York Yankees",
        "Spread: Philadelphia Phillies (-1.5)",
        "Los Angeles Dodgers vs. San Diego Padres: O/U 8.5",
        "Boston Celtics vs. Lakers",
        "Will Golden State Warriors win on 2026-04-10?",
    ]
    print("\nTitle parsing tests:")
    for title in title_tests:
        parsed = parse_market_title(title)
        print(f"  {title:55s} -> {parsed}")

    print(f"\n{len(MLB_TEAMS)} MLB teams, {len(NBA_TEAMS)} NBA teams loaded.")
