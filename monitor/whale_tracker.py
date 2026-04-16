"""
Generic VIP whale tracker — real-time alerts when a whale enters a NEW market.

This is the generalized version of `texaskid_tracker.py`. One process runs
one `WhaleVIPTracker` instance bound to a single whale wallet; spin up
multiple tmux sessions (one per VIP whale) to track bigsix, texaskid,
ImJustKen etc. in parallel.

Behavior is identical to texaskid_tracker's v2 (post-Holland-fix):
  - Loads baseline from `tracked_whale_positions` table (per-wallet)
  - First poll alerts on any live position NOT already in the DB baseline
    (so restarts never silently swallow missed entries again)
  - Detects NEW markets, SIZE INCREASES (≥ $5K), and RESOLUTIONS
  - Every alert is enriched via `utils.market_context` (Polymarket URL,
    current orderbook, tailable-at-entry liquidity)

For backward compatibility, the texaskid tracker ALSO writes to the legacy
`texaskid_positions` table until Phase 3 of the cashout generalization
lands and `ev_engine/position_manager.py` is switched over to read from
`tracked_whale_positions`.

Usage (from another module):
    from monitor.whale_tracker import WhaleVIPTracker, WhaleConfig

    cfg = WhaleConfig(
        wallet="0xa71093cafc0c099b4ccab24c3cb8018d817923c4",
        alias="bigsix",
        emoji="🐳",
    )
    await WhaleVIPTracker(cfg).start()
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import sys
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple

import aiohttp

# Allow importing from the project root (utils/market_context.py)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    from utils.market_context import build_market_context  # noqa: E402
except Exception:  # pragma: no cover — never block the tracker on import
    build_market_context = None  # type: ignore


logger = logging.getLogger(__name__)


# ── Module-level constants ─────────────────────────────────────────────
DATA_API = "https://data-api.polymarket.com"
POLL_INTERVAL = 15              # seconds — fast polling for VIP whales
MIN_POSITION_USD = 50           # low threshold to catch drip-feed entries
POSITION_LIMIT = 100
SIZE_INCREASE_ALERT_USD = 5_000  # alert only on size bumps this large or more


# ── Sport detection + confidence flagger ────────────────────────────────
#
# Each whale gets a sport→confidence map derived from historical analysis.
# The flagger appends a colored tag to NEW POSITION / ADDING SIZE alerts
# so the user instantly knows whether to tail or ignore.

# Confidence tiers
CONF_HOT = "🟢 HOT"          # Historically profitable sport for this whale
CONF_WARM = "🟡 WARM"        # Near-breakeven or small sample
CONF_COLD = "🔴 COLD"        # Historically unprofitable — tail with caution
CONF_TOXIC = "☠️ TOXIC"       # Catastrophic record — consider fading

# Per-whale sport confidence maps (updated from 10-day deep analysis 2026-04-12).
# Keys are lowercase sport tags returned by detect_sport().
# Missing keys default to WARM (insufficient data).
WHALE_SPORT_CONFIDENCE: dict[str, dict[str, str]] = {
    "bigsix": {
        "nhl":    CONF_HOT,     # 5W/4L, +$136K — real edge
        "ufc":    CONF_HOT,     # 3W/2L, +$27K  — hedge strat works
        "soccer": CONF_WARM,    # 2W/4L, -$14K  — Bayern carry, small sample
        "nba":    CONF_COLD,    # 1W/5L, -$146K — spreads getting destroyed
        "mlb":    CONF_TOXIC,   # 1W/11L, -$330K — 6.7% win rate
    },
    "texaskid": {
        "nhl":    CONF_HOT,     # 2W/1L, solid picks
        "ufc":    CONF_HOT,     # 1W/1L, but good edge on Holland
        "nba":    CONF_WARM,    # all open, looking strong but unresolved
        "mlb":    CONF_WARM,    # 2W/1L, mixed — one massive loss
        "soccer": CONF_WARM,    # no data yet
    },
    "kch123": {
        "nhl":    CONF_HOT,     # 42W/39L = 52% WR, +$369K, +21% ROI on $1.72M 30d stake
        "nba":    CONF_TOXIC,   # 0W/1L, -$84K — filtered out, solo NHL-only
    },
    "theonlyhuman": {
        "nba":    CONF_HOT,     # 40W/25L = 62% WR, +$201K, +29% ROI on $694K 30d stake
        "mlb":    CONF_WARM,    # 5W/0L +$21K — too small to call HOT
        "soccer": CONF_TOXIC,   # -$85K Liverpool — filtered out, solo NBA-only
    },
}


# NHL team names — "kings" and "rangers" removed (ambiguous with NBA/MLB)
_NHL_TEAMS = {
    "avalanche", "blackhawks", "blues", "bruins", "canadiens", "canucks",
    "capitals", "coyotes", "ducks", "flames", "flyers", "golden knights",
    "hurricanes", "islanders", "jets", "kraken", "lightning",
    "maple leafs", "oilers", "panthers", "penguins", "predators",
    "red wings", "sabres", "senators", "sharks", "stars", "wild",
    "utah",  # Utah Hockey Club
}

# NBA team names — "kings" removed (ambiguous with NHL LA Kings)
_NBA_TEAMS = {
    "76ers", "bucks", "bulls", "cavaliers", "celtics", "clippers", "grizzlies",
    "hawks", "heat", "hornets", "jazz", "knicks", "lakers", "magic",
    "mavericks", "nets", "nuggets", "pacers", "pelicans", "pistons", "raptors",
    "rockets", "sixers", "spurs", "suns", "thunder", "timberwolves",
    "trail blazers", "warriors", "wizards",
}

# MLB team names — "rangers" removed (ambiguous with NHL NY Rangers)
_MLB_TEAMS = {
    "angels", "astros", "athletics", "blue jays", "braves", "brewers",
    "cardinals", "cubs", "diamondbacks", "dodgers", "giants", "guardians",
    "mariners", "marlins", "mets", "nationals", "orioles", "padres",
    "phillies", "pirates", "rays", "red sox", "reds", "rockies",
    "royals", "tigers", "twins", "white sox", "yankees",
}


_PST_TZ = timezone(timedelta(hours=-7))


def classify_subtype(title: str) -> str:
    """Classify a Polymarket question into one of the broad bet-subtype
    categories used by performance filters.

    Returns one of:
      draw     — soccer 3-way draw market
      totals   — Over/Under, O/U market
      spread   — point-spread / puckline market
      segment  — game-segment (period/map/quarter/inning/set/game-N)
      daily-ml — 'Will X win on YYYY-MM-DD'
      h2h-ml   — head-to-head moneyline (A vs. B)
      futures  — season/tournament winner
      other    — anything else
    """
    t = title.lower()
    if "end in a draw" in t:
        return "draw"
    if "o/u" in t or "over/under" in t:
        return "totals"
    if "spread" in t:
        return "spread"
    if re.search(r"\b(period|map|quarter|inning|set|game \d)\b", t):
        return "segment"
    if ("winner" in t and ("quarterfinal" in t or "semifinal" in t or "final" in t)):
        return "futures"
    if "win on 20" in t:
        return "daily-ml"
    if " vs " in t or " vs. " in t:
        return "h2h-ml"
    if "win the" in t and ("cup" in t or "champion" in t or "division" in t or "conference" in t):
        return "futures"
    return "other"


def detect_sport(title: str, slug: str = "") -> str:
    """Return a lowercase sport tag from a market title / slug.

    Returns one of: 'nhl', 'nba', 'mlb', 'ufc', 'soccer', 'unknown'.
    """
    t = title.lower()
    s = slug.lower()

    # Early-exit on explicit esports / non-ball-sport markers so the
    # team-name substring fallback below can't falsely tag e.g. "Wildcard"
    # (CS team) as NHL Minnesota Wild.
    if any(k in t for k in (
        "counter-strike", "cs:go", "cs2", "csgo",
        "valorant", "league of legends", "dota", "esports",
    )):
        return "unknown"

    # Slug-based detection (most reliable — disambiguates kings/rangers)
    if "nhl-" in s:
        return "nhl"
    if "nba-" in s:
        return "nba"
    if "mlb-" in s:
        return "mlb"
    if "ufc-" in s or "ufc " in t:
        return "ufc"
    # Soccer leagues in slugs
    if any(lg in s for lg in ("bun-", "ser-", "ered-", "lig-", "epl-", "ligue-")):
        return "soccer"

    # Title-based fallback: check unambiguous team names with WORD BOUNDARIES.
    # Plain substring matching falsely catches "wildcard" → "wild" (NHL),
    # "hundreds" → "reds" (MLB), etc. The \b regex ensures whole-word match.
    def _has_team(team_set: set[str], text: str) -> bool:
        for team in team_set:
            # Escape team phrase (handles multi-word like "red wings", "blue jays")
            if re.search(r"\b" + re.escape(team) + r"\b", text):
                return True
        return False

    if _has_team(_NHL_TEAMS, t):
        return "nhl"
    if _has_team(_NBA_TEAMS, t):
        return "nba"
    if _has_team(_MLB_TEAMS, t):
        return "mlb"

    # Ambiguous teams: disambiguate with city names
    if re.search(r"\bkings\b", t):
        if "sacramento" in t:
            return "nba"
        if "los angeles" in t or "la kings" in t:
            return "nhl"
        # Default: NBA Kings are far more common on Polymarket
        return "nba"
    if re.search(r"\brangers\b", t):
        if "texas" in t:
            return "mlb"
        if "new york" in t or "ny rangers" in t:
            return "nhl"
        # Default: Texas Rangers MLB more common on Polymarket
        return "mlb"

    # Soccer keywords in title (word-bounded)
    if re.search(r"\bfc\b", t) or re.search(r"\bac\b", t) or re.search(r"\bas\b", t) or "win on 20" in t:
        return "soccer"

    return "unknown"


def get_sport_confidence(alias: str, sport: str) -> str:
    """Return the confidence tag for a whale+sport combination."""
    whale_map = WHALE_SPORT_CONFIDENCE.get(alias.lower(), {})
    return whale_map.get(sport, CONF_WARM)


def format_sport_flag(alias: str, title: str, slug: str = "") -> str:
    """Return a one-line HTML string like '🏒 NHL — 🟢 HOT' for alerts.

    Returns empty string if sport is unknown (no flag shown).
    """
    sport = detect_sport(title, slug)
    if sport == "unknown":
        return ""

    conf = get_sport_confidence(alias, sport)

    sport_emoji = {
        "nhl": "🏒", "nba": "🏀", "mlb": "⚾",
        "ufc": "🥊", "soccer": "⚽",
    }.get(sport, "🎯")

    sport_label = sport.upper()

    return f"{sport_emoji} {sport_label} — {conf}"

DB_PATH = os.getenv(
    "DB_PATH",
    str(Path(__file__).resolve().parent.parent / "trades.db"),
)

# Telegram (read from env; same as bot.py / texaskid_tracker.py).
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# US Pacific Time (display-only).
_PST = timezone(timedelta(hours=-7))


# ── Config + DB schema ─────────────────────────────────────────────────


@dataclass(frozen=True)
class BetRule:
    """A single allow-rule for the performance filter.

    An alert passes the filter if its (entry_price, size_usd) tuple matches
    ANY rule in the filter's rules list.

    - favorite=True  matches entry_price >= fav_threshold
    - favorite=False matches entry_price <  fav_threshold
    - favorite=None  matches either side
    - size range is INCLUSIVE-EXCLUSIVE: [min_size_usd, max_size_usd)
    """

    favorite: Optional[bool] = None
    min_size_usd: float = 0.0
    max_size_usd: float = float("inf")


@dataclass(frozen=True)
class PerformanceFilter:
    """OR-semantics gate over a list of BetRule.

    enabled=False allows everything (no-op). enabled=True with rules=()
    suppresses everything (nothing matches).
    """

    enabled: bool = False
    rules: Tuple[BetRule, ...] = ()
    fav_threshold: float = 0.50

    def allows(self, entry_price: float, size_usd: float) -> bool:
        if not self.enabled:
            return True
        is_fav = entry_price >= self.fav_threshold
        for r in self.rules:
            if r.favorite is not None and r.favorite != is_fav:
                continue
            if size_usd < r.min_size_usd:
                continue
            if size_usd >= r.max_size_usd:
                continue
            return True
        return False


@dataclass
class WhaleConfig:
    """Per-whale runtime configuration for a VIP tracker instance."""

    wallet: str
    alias: str
    # Visual icon used in every Telegram alert emitted by this tracker
    emoji: str = "🐋"
    # Minimum size (in USD) for a new position to fire an alert
    min_position_usd: float = MIN_POSITION_USD
    # Size-increase threshold — "this whale bumped the bet" alert trigger
    size_increase_alert_usd: float = SIZE_INCREASE_ALERT_USD
    # Poll cadence — VIP whales poll at 15s; can override per-whale
    poll_interval_sec: int = POLL_INTERVAL
    # Whether this tracker should also dual-write to the legacy
    # `texaskid_positions` table. Set True ONLY for texaskid itself.
    dual_write_legacy_texaskid_table: bool = False
    # Whether this tracker sends its own daily W/L report. Set to False
    # for secondary whales so only one daily report arrives per day.
    send_daily_report: bool = True
    # Hour (PST) at which the daily report is emitted.
    daily_report_hour_pst: int = 8
    # Sport filter for solo Telegram alerts. If set, ONLY markets in these
    # sports fire NEW POSITION / ADDING SIZE / WIN / LOSS alerts. Other
    # sports are still tracked in DB (for consensus radar + cashout engine)
    # but the user doesn't get a standalone Telegram ping for them.
    # None = no filter (all sports alert). Example: {"nhl", "ufc"}
    solo_alert_sports: Optional[set[str]] = None
    # Per-whale performance filter: gates Telegram alerts by (entry_price,
    # size_usd). Derived from backtested win-rate × bet-size × fav/dog
    # buckets. None = no filter (all qualifying trades alert as before).
    # DB writes are always unaffected — only the Telegram send is gated.
    performance_filter: Optional[PerformanceFilter] = None
    # Bet-subtype blocklist. Any market whose title maps (via
    # classify_subtype) to a subtype in this set is suppressed.
    # Example: {"totals"} for bigsix (his totals ROI is −21%).
    blocked_subtypes: Optional[set[str]] = None
    # Hour-of-day filter (0-23 in PST). If set, ONLY these hours alert.
    # Example: {12,13,14,15,16} for TheOnlyHuman (afternoon sweet spot).
    allowed_hours_pst: Optional[set[int]] = None
    # Hour-of-day blocklist (0-23 in PST). If set, these hours never alert.
    # Example: {12,13,14,15,16} for kch123 (afternoon drain).
    blocked_hours_pst: Optional[set[int]] = None
    # If True, suppress NEW POSITION alerts (but still baseline the position).
    # SIZE UP alerts on the same cid later will still fire. Use for whales
    # whose add-ons outperform their initials (e.g. bigsix: adds +19% ROI
    # vs initials −32%).
    require_multi_trade: bool = False
    # Tilt guard — if a loss resolution has fired within this many hours,
    # suppress all new alerts. Example: 4.0 for kch123 (his after-loss
    # bucket was −58% ROI, concentrated in one huge re-entry loss).
    tilt_mute_hours: Optional[float] = None


# Generic table schema. Composite PK on (wallet, condition_id) so multiple
# whales can safely share a single table.
TRACKED_SCHEMA = """
CREATE TABLE IF NOT EXISTS tracked_whale_positions (
    wallet TEXT NOT NULL,
    alias TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    direction TEXT NOT NULL,
    market_title TEXT NOT NULL,
    first_seen_price REAL,
    first_seen_size_usd REAL,
    current_size_usd REAL,
    current_price REAL,
    status TEXT NOT NULL DEFAULT 'open',
    outcome TEXT,
    pnl REAL,
    first_seen_at TEXT NOT NULL,
    last_updated TEXT,
    alert_sent INTEGER DEFAULT 0,
    resolved_at TEXT,
    PRIMARY KEY (wallet, condition_id)
);

CREATE INDEX IF NOT EXISTS idx_tracked_whale_positions_wallet_status
    ON tracked_whale_positions(wallet, status);
CREATE INDEX IF NOT EXISTS idx_tracked_whale_positions_status
    ON tracked_whale_positions(status);
"""


def init_db(db_path: str = DB_PATH) -> None:
    """Create `tracked_whale_positions` if missing. Idempotent."""
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(TRACKED_SCHEMA)
        conn.commit()
    finally:
        conn.close()


# ── Telegram sender ────────────────────────────────────────────────────


async def send_telegram(message: str) -> bool:
    """Send an HTML-formatted Telegram alert. Shared across all trackers."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram not configured — alert not sent")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    chat_ids = [cid.strip() for cid in TELEGRAM_CHAT_ID.split(",") if cid.strip()]
    any_success = False

    for cid in chat_ids:
        payload = {
            "chat_id": cid,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, json=payload,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status == 200:
                        any_success = True
                    else:
                        body = await resp.text()
                        logger.warning(
                            "Telegram error %d: %s", resp.status, body[:200],
                        )
        except Exception as e:
            logger.debug("Telegram send failed: %s", e)

    return any_success


# ── Main tracker class ─────────────────────────────────────────────────


class WhaleVIPTracker:
    """Generic VIP whale tracker — one instance per whale wallet."""

    def __init__(self, cfg: WhaleConfig, db_path: str = DB_PATH) -> None:
        self.cfg = cfg
        self.db_path = db_path
        self.session: aiohttp.ClientSession | None = None
        self.running = False
        # baseline: {condition_id: {title, direction, size_usd, price, ...}}
        self.baseline: dict[str, dict] = {}
        self.first_poll = True
        # True only when the DB had zero recorded positions for this whale.
        # Controls whether first-poll alerts fire (see Holland fix rationale
        # in texaskid_tracker v2).
        self.fresh_install: bool = False
        # Daily report tracking — only send one per PST day per tracker.
        self.last_daily_report_date: str | None = None
        # Size-alert cooldown: {condition_id: (last_alert_ts, size_at_alert)}
        # Suppress duplicate ADDING SIZE alerts for 10 minutes per market,
        # UNLESS the cumulative delta since last alert exceeds $20K (genuine
        # large size-up should break through the cooldown).
        self._size_alert_cooldown: dict[str, tuple[float, float]] = {}
        # Tilt guard state: unix timestamp of the most recent LOSS resolution
        # seen by this tracker in the current session. Used with
        # cfg.tilt_mute_hours to suppress alerts while the whale is
        # presumed to be tilting.
        self._last_loss_ts: float = 0.0
        # BULLETPROOF BACKSTOP: in-memory set of cids we've already processed
        # a resolution for in this process. Belt-and-suspenders alongside the
        # self.baseline[cid]["status"] = "closed" write — if the baseline dict
        # update is somehow bypassed (e.g. the cid isn't in self.baseline at
        # the moment of resolution, or another code path resets status), this
        # set still catches the dup. Cleared only on explicit re-open.
        self._resolved_cids_this_session: set[str] = set()

    def _should_alert(self, title: str) -> bool:
        """Return True if this market's sport passes the solo_alert_sports filter."""
        if self.cfg.solo_alert_sports is None:
            return True  # no filter — alert on everything
        sport = detect_sport(title)
        return sport in self.cfg.solo_alert_sports

    def _should_alert_by_performance(
        self, entry_price: float, size_usd: float, reason_ctx: str
    ) -> bool:
        """Return True if this bet's (entry_price, size_usd) passes the
        performance filter. Logs a SUPPRESSED line on reject for traceability."""
        pf = self.cfg.performance_filter
        if pf is None:
            return True
        if pf.allows(entry_price, size_usd):
            return True
        logger.info(
            "SUPPRESSED alert [%s] (performance filter): %s | entry=$%.3f size=$%.0f",
            self.cfg.alias, reason_ctx[:40], entry_price, size_usd,
        )
        return False

    def _should_alert_by_subtype(self, title: str) -> bool:
        """Block alerts on bet subtypes known to lose money for this whale."""
        blocked = self.cfg.blocked_subtypes
        if not blocked:
            return True
        subtype = classify_subtype(title)
        if subtype in blocked:
            logger.info(
                "SUPPRESSED alert [%s] (subtype=%s blocked): %s",
                self.cfg.alias, subtype, title[:40],
            )
            return False
        return True

    def _should_alert_by_hour(self, title: str) -> bool:
        """Allowed-hours + blocked-hours filter (PST hour 0-23)."""
        allowed = self.cfg.allowed_hours_pst
        blocked = self.cfg.blocked_hours_pst
        if allowed is None and not blocked:
            return True
        hour = datetime.now(_PST_TZ).hour
        if allowed is not None and hour not in allowed:
            logger.info(
                "SUPPRESSED alert [%s] (hour=%d not in allowed_hours_pst): %s",
                self.cfg.alias, hour, title[:40],
            )
            return False
        if blocked and hour in blocked:
            logger.info(
                "SUPPRESSED alert [%s] (hour=%d in blocked_hours_pst): %s",
                self.cfg.alias, hour, title[:40],
            )
            return False
        return True

    def _should_alert_by_tilt(self, title: str) -> bool:
        """Tilt guard — suppress while within tilt_mute_hours of a recent loss."""
        mute_h = self.cfg.tilt_mute_hours
        if not mute_h or self._last_loss_ts <= 0:
            return True
        import time as _time
        since = _time.time() - self._last_loss_ts
        if since < mute_h * 3600:
            hrs_left = (mute_h * 3600 - since) / 3600
            logger.info(
                "SUPPRESSED alert [%s] (tilt guard, %.1fh left of %.1fh mute): %s",
                self.cfg.alias, hrs_left, mute_h, title[:40],
            )
            return False
        return True

    def _should_alert_initial(self, title: str) -> bool:
        """If require_multi_trade=True, suppress NEW POSITION alerts (but
        still baseline the position so SIZE UP on the same cid will fire)."""
        if not self.cfg.require_multi_trade:
            return True
        logger.info(
            "SUPPRESSED alert [%s] (require_multi_trade — NEW POSITION muted, will alert on SIZE UP): %s",
            self.cfg.alias, title[:40],
        )
        return False

    # ── Lifecycle ──

    async def start(self) -> None:
        init_db(self.db_path)
        self.session = aiohttp.ClientSession()
        self.running = True

        # Load known positions from DB so restarts don't re-alert every cycle.
        self._load_known_positions()

        logger.info(
            "%s VIP tracker started — polling every %ds (wallet=%s)",
            self.cfg.alias, self.cfg.poll_interval_sec, self.cfg.wallet[:10],
        )

        while self.running:
            try:
                await self._poll_cycle()
                if self.cfg.send_daily_report:
                    await self._maybe_send_daily_report()
            except Exception as e:
                logger.error("%s poll cycle error: %s", self.cfg.alias, e)

            await asyncio.sleep(self.cfg.poll_interval_sec)

    async def stop(self) -> None:
        self.running = False
        if self.session:
            await self.session.close()
        logger.info("%s tracker stopped", self.cfg.alias)

    # ── DB helpers ──

    def _load_known_positions(self) -> None:
        """Load prior-run state for THIS wallet from both old and new tables.

        Strategy:
          1. Always read from the new `tracked_whale_positions` table.
          2. If this tracker is texaskid AND the new table has zero rows
             for the wallet, fall back to the legacy `texaskid_positions`
             table so the Holland-fix's "alert on anything we missed"
             behavior still works through the schema migration.
        """
        self.baseline.clear()
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            # 1) New generic table
            rows = conn.execute(
                """
                SELECT condition_id, direction, market_title,
                       current_size_usd, current_price, status
                FROM tracked_whale_positions
                WHERE wallet = ?
                """,
                (self.cfg.wallet.lower(),),
            ).fetchall()

            # 2) Legacy table fallback for texaskid only
            if not rows and self.cfg.dual_write_legacy_texaskid_table:
                try:
                    rows = conn.execute(
                        """
                        SELECT condition_id, direction, market_title,
                               current_size_usd, current_price, status
                        FROM texaskid_positions
                        """
                    ).fetchall()
                except sqlite3.OperationalError:
                    rows = []  # legacy table missing — safe to ignore
            conn.close()

            for row in rows:
                self.baseline[row["condition_id"]] = {
                    "title": row["market_title"],
                    "direction": row["direction"],
                    "size_usd": row["current_size_usd"] or 0,
                    "price": row["current_price"] or 0.5,
                    "status": row["status"],
                }

            self.fresh_install = len(self.baseline) == 0
            logger.info(
                "%s: loaded %d known positions (fresh_install=%s)",
                self.cfg.alias, len(self.baseline), self.fresh_install,
            )
        except Exception as e:
            logger.error(
                "%s: failed to load known positions: %s", self.cfg.alias, e,
            )
            # Treat DB failure as fresh install to avoid alert spam on recovery
            self.fresh_install = True

    def _insert_new_position(
        self, cid: str, pos: dict, now_iso: str,
    ) -> None:
        """Insert a new-position row into both tables (dual-write for texaskid)."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute(
                """
                INSERT OR IGNORE INTO tracked_whale_positions
                    (wallet, alias, condition_id, direction, market_title,
                     first_seen_price, first_seen_size_usd, current_size_usd,
                     current_price, status, first_seen_at, last_updated,
                     alert_sent)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, 1)
                """,
                (
                    self.cfg.wallet.lower(), self.cfg.alias, cid,
                    pos["direction"], pos["title"], pos["price"],
                    pos["size_usd"], pos["size_usd"], pos["price"],
                    now_iso, now_iso,
                ),
            )
            if self.cfg.dual_write_legacy_texaskid_table:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO texaskid_positions
                        (condition_id, direction, market_title,
                         first_seen_price, first_seen_size_usd,
                         current_size_usd, current_price, status,
                         first_seen_at, last_updated, alert_sent)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, 1)
                    """,
                    (
                        cid, pos["direction"], pos["title"], pos["price"],
                        pos["size_usd"], pos["size_usd"], pos["price"],
                        now_iso, now_iso,
                    ),
                )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(
                "%s: DB insert failed for %s: %s",
                self.cfg.alias, cid[:16], e,
            )

    def _update_position_size(
        self, cid: str, new_size: float, price: float, now_iso: str,
    ) -> None:
        """Update current_size_usd / current_price after a size-up alert."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute(
                """
                UPDATE tracked_whale_positions
                SET current_size_usd = ?, current_price = ?, last_updated = ?
                WHERE wallet = ? AND condition_id = ?
                """,
                (new_size, price, now_iso, self.cfg.wallet.lower(), cid),
            )
            if self.cfg.dual_write_legacy_texaskid_table:
                conn.execute(
                    """
                    UPDATE texaskid_positions
                    SET current_size_usd = ?, current_price = ?, last_updated = ?
                    WHERE condition_id = ?
                    """,
                    (new_size, price, now_iso, cid),
                )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.debug("%s: size update failed: %s", self.cfg.alias, e)

    def _mark_resolved(
        self, cid: str, outcome: str, now_iso: str,
    ) -> None:
        """Mark a position as resolved in both tables."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute(
                """
                UPDATE tracked_whale_positions
                SET status = 'closed', outcome = ?, resolved_at = ?
                WHERE wallet = ? AND condition_id = ?
                """,
                (outcome, now_iso, self.cfg.wallet.lower(), cid),
            )
            if self.cfg.dual_write_legacy_texaskid_table:
                conn.execute(
                    """
                    UPDATE texaskid_positions
                    SET status = 'closed', outcome = ?, resolved_at = ?
                    WHERE condition_id = ?
                    """,
                    (outcome, now_iso, cid),
                )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.debug("%s: resolve update failed: %s", self.cfg.alias, e)

    # ── Activity + CLOB helpers (for hybrid W/L in daily report) ──

    async def _fetch_activity(self, lookback_days: int = 7) -> list[dict]:
        """Fetch TRADE entries from activity API within lookback window.

        Catches trades for positions that were redeemed (winners that
        disappear from the positions API).
        """
        assert self.session is not None
        from datetime import timezone as _tz
        url = f"{DATA_API}/v1/activity"
        all_trades: list[dict] = []
        offset = 0
        cutoff_ts = int((datetime.now(_tz.utc) - timedelta(days=lookback_days)).timestamp())

        while True:
            params = {"user": self.cfg.wallet, "limit": "100", "offset": str(offset)}
            try:
                async with self.session.get(
                    url, params=params, timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status != 200:
                        break
                    batch = await resp.json()
                    if not batch:
                        break
                    for entry in batch:
                        if entry.get("timestamp", 0) >= cutoff_ts and entry.get("type") == "TRADE":
                            all_trades.append(entry)
                    oldest_ts = min(e.get("timestamp", 0) for e in batch)
                    if oldest_ts < cutoff_ts or len(batch) < 100:
                        break
                    offset += 100
                    await asyncio.sleep(0.3)
            except Exception:
                break

        return all_trades

    async def _check_token_price(self, token_id: str) -> float | None:
        """Get last trade price for a token from CLOB API."""
        assert self.session is not None
        url = "https://clob.polymarket.com/last-trade-price"
        try:
            async with self.session.get(
                url, params={"token_id": token_id},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    return float(data.get("price", 0.5))
        except Exception:
            pass
        return None

    # ── Position fetching ──

    async def _fetch_positions(self) -> list[dict]:
        """Fetch all current positions for the tracked wallet."""
        assert self.session is not None
        positions: list[dict] = []
        offset = 0
        while True:
            params = {
                "user": self.cfg.wallet,
                "sizeThreshold": "0.1",
                "limit": str(POSITION_LIMIT),
                "offset": str(offset),
            }
            try:
                async with self.session.get(
                    f"{DATA_API}/v1/positions", params=params,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        break
                    batch = await resp.json()
                    if not batch:
                        break
                    positions.extend(batch)
                    if len(batch) < POSITION_LIMIT:
                        break
                    offset += POSITION_LIMIT
                    await asyncio.sleep(0.2)
            except Exception:
                break
        return positions

    # ── Enrichment helper ──

    async def _enrich_lines(
        self, *, condition_id: str, direction: str, token_id: str,
        entry_price: float, size_usd: float,
    ) -> list[str]:
        """Return HTML-formatted enrichment lines (URL + price + liquidity)."""
        if build_market_context is None or self.session is None:
            return []
        try:
            ctx = await build_market_context(
                self.session,
                condition_id=condition_id,
                direction=direction,
                token_id=token_id,
                entry_price=entry_price,
                size_usd=size_usd,
                whale_avg_bet=0.0,
            )
        except Exception as e:
            logger.debug(
                "%s: enrichment failed for %s: %s",
                self.cfg.alias, condition_id[:12], e,
            )
            return []

        lines: list[str] = []
        price_line = ctx.render_price_line(entry_price)
        if price_line:
            lines.append(price_line)
        liq_line = ctx.render_liquidity_line()
        if liq_line:
            lines.append(liq_line)
        tail_line = ctx.render_tailable_line()
        if tail_line:
            lines.append(tail_line)
        url_line = ctx.render_url_line()
        if url_line:
            lines.append(url_line)
        return lines

    # ── Main poll cycle ──

    async def _poll_cycle(self) -> None:
        """Main poll: detect new markets, size changes, and resolutions."""
        positions = await self._fetch_positions()

        # ── Group raw positions by conditionId ─────────────────────
        # A wallet can hold BOTH sides of a market (e.g. a hedge or a
        # partial flip). If we key `current` by cid alone, the second
        # iteration overwrites the first — which historically made the
        # tracker mislabel losses as wins whenever the wallet held a
        # tiny hedge on the winning side (e.g. kch123's $428K Canadiens
        # loss got labeled WIN because he held a $1.4K Flyers hedge).
        by_cid: dict[str, list[dict]] = {}
        for pos in positions:
            cid = pos.get("conditionId", "")
            if not cid:
                continue
            initial_value = float(pos.get("initialValue") or 0)
            if initial_value < self.cfg.min_position_usd:
                continue
            entry = {
                "title": pos.get("title", ""),
                "direction": pos.get("outcome", "YES"),
                "size_usd": initial_value,
                "price": float(
                    pos.get("avgPrice")
                    or (pos.get("curPrice") if pos.get("curPrice") is not None else 0.5)
                ),
                "cur_price": float(pos.get("curPrice")) if pos.get("curPrice") is not None else 0.5,
                "redeemable": pos.get("redeemable", False),
                "size_shares": float(pos.get("size") or 0),
                "asset": pos.get("asset", "") or pos.get("tokenId", ""),
            }
            by_cid.setdefault(cid, []).append(entry)

        # For each cid, pick the side that matches the baseline's tracked
        # direction (the one we previously alerted on); otherwise take the
        # largest by initialValue. This prevents a hedge on the opposite
        # side from hijacking the resolution signal.
        current: dict[str, dict] = {}
        for cid, entries in by_cid.items():
            if len(entries) == 1:
                current[cid] = entries[0]
                continue
            tracked_dir = (self.baseline.get(cid) or {}).get("direction")
            picked = None
            if tracked_dir:
                for e in entries:
                    if e["direction"].strip().lower() == tracked_dir.strip().lower():
                        picked = e
                        break
            if picked is None:
                picked = max(entries, key=lambda e: e["size_usd"])
            current[cid] = picked
            other_sides = [
                f"{e['direction']}(${e['size_usd']:,.0f} @ ${e['cur_price']:.3f})"
                for e in entries if e is not picked
            ]
            logger.info(
                "DUAL-SIDE [%s] cid=%s picked=%s $%.0f @ $%.3f; ignored=[%s]",
                self.cfg.alias, cid[:16], picked["direction"],
                picked["size_usd"], picked["cur_price"],
                ", ".join(other_sides),
            )

        now = datetime.now(timezone.utc).isoformat()
        now_pst = datetime.now(_PST).strftime("%b %d, %I:%M %p PST")

        # ── Detect NEW markets ─────────────────────────────────────
        for cid, pos in current.items():
            # Re-open path: if baseline marks this cid closed but the whale
            # still holds a healthy (non-rail) position in it, treat as NEW
            # so the promote resumes. Covers false-closed rows from flaky
            # Gamma resolutions or backfill mislabels.
            #
            # Guarded by a 30-minute resolution cooldown: once we've marked a
            # position RESOLVED, don't re-open it for at least 1800 seconds
            # even if price flickers back inside the rail range. Without this
            # guard, a single curPrice ping (e.g. Gamma briefly flips from
            # 0.985 → 0.98) triggers a re-open → resolve → re-open loop, which
            # spams Telegram (we saw 5 copies of the same Marlins alert in
            # 10 min on texaskid, and 8 Stanley Cup futures in 1 min on kch123).
            import time as _time_mod
            _now_ts = _time_mod.time()
            _resolved_at = 0.0
            if cid in self.baseline:
                _resolved_at = float(self.baseline[cid].get("resolved_at_ts") or 0)
            _in_cooldown = (_resolved_at > 0) and (_now_ts - _resolved_at < 1800)
            reopening = (
                cid in self.baseline
                and self.baseline[cid].get("status") == "closed"
                and not pos.get("redeemable")
                and 0.02 < pos.get("cur_price", 0.5) < 0.98
                and not _in_cooldown
            )
            if cid in self.baseline and self.baseline[cid].get("status") == "closed" and _in_cooldown:
                # Still in resolution cooldown — skip entirely to avoid spam
                logger.debug(
                    "[%s] re-open blocked by cooldown (%.0fs left): %s",
                    self.cfg.alias, 1800 - (_now_ts - _resolved_at), cid[:16],
                )
                continue
            if cid in self.baseline and not reopening:
                continue

            # Suppress only on true fresh install — otherwise fire even on
            # first poll so restart-misses get alerted.
            if self.first_poll and self.fresh_install:
                continue

            # Skip already-resolved markets (cur price at the rails)
            if (
                pos["redeemable"]
                or pos["cur_price"] >= 0.98
                or pos["cur_price"] <= 0.02
            ):
                continue

            if reopening:
                logger.info(
                    "RE-OPEN [%s]: %s | %s @ $%.3f | $%.0f | %s (was status=closed)",
                    self.cfg.alias, pos["direction"], pos["title"][:50],
                    pos["price"], pos["size_usd"], cid[:16],
                )
                # Clear the closed flag so the promote block refreshes baseline.
                self.baseline[cid]["status"] = None
                # Clear the bulletproof resolved-set too so the re-opened
                # position can be freshly tracked again.
                self._resolved_cids_this_session.discard(cid)
                # Reset DB so cashout engine sees it alive again.
                try:
                    conn = sqlite3.connect(self.db_path)
                    conn.execute(
                        """
                        UPDATE tracked_whale_positions
                        SET status='open', outcome=NULL, resolved_at=NULL,
                            current_size_usd=?, current_price=?, last_updated=?
                        WHERE wallet=? AND condition_id=?
                        """,
                        (pos["size_usd"], pos["price"], now,
                         self.cfg.wallet.lower(), cid),
                    )
                    conn.commit()
                    conn.close()
                except Exception as e:
                    logger.error("%s: re-open DB update failed: %s", self.cfg.alias, e)

            logger.info(
                "NEW MARKET [%s]: %s | %s @ $%.3f | $%.0f | %s",
                self.cfg.alias, pos["direction"], pos["title"][:50],
                pos["price"], pos["size_usd"], cid[:16],
            )

            self._insert_new_position(cid, pos, now)

            # Sport filter: still track in DB but skip Telegram for filtered sports
            if not self._should_alert(pos["title"]):
                logger.info(
                    "SUPPRESSED alert [%s]: %s (sport not in solo_alert_sports)",
                    self.cfg.alias, pos["title"][:40],
                )
                continue

            # Performance filter: gate on (entry_price, size_usd) bucket
            if not self._should_alert_by_performance(
                pos["price"], pos["size_usd"], pos["title"]
            ):
                continue

            # Subtype / hour-of-day / tilt guards
            if not self._should_alert_by_subtype(pos["title"]):
                continue
            if not self._should_alert_by_hour(pos["title"]):
                continue
            if not self._should_alert_by_tilt(pos["title"]):
                continue

            # Require-multi-trade: suppress NEW POSITION alerts; SIZE UP
            # on the same cid later will still pass (baseline is populated
            # by _insert_new_position above regardless of alert).
            if not self._should_alert_initial(pos["title"]):
                continue

            ctx_lines = await self._enrich_lines(
                condition_id=cid,
                direction=pos["direction"],
                token_id=pos.get("asset", ""),
                entry_price=pos["price"],
                size_usd=pos["size_usd"],
            )
            header = f"{self.cfg.emoji} <b>{self.cfg.alias.upper()} NEW POSITION</b>"
            sport_flag = format_sport_flag(
                self.cfg.alias, pos["title"],
                # We don't have the slug in 'pos' but title detection works
            )
            msg_lines = [
                header,
                "",
                f"<b>Market:</b> {pos['title']}",
                f"<b>Side:</b> {pos['direction']}",
                f"<b>Entry Price:</b> ${pos['price']:.3f}",
                f"<b>Size:</b> ${pos['size_usd']:,.0f}",
            ]
            if sport_flag:
                msg_lines.append("")
                msg_lines.append(sport_flag)
            if ctx_lines:
                msg_lines.append("")
                msg_lines.extend(ctx_lines)
            msg_lines += [
                "",
                f"<i>Time: {now_pst}</i>",
                "",
                f"💡 Consider placing a limit order at ~${pos['price']:.2f}",
            ]
            await send_telegram("\n".join(msg_lines))

        # ── Detect significant SIZE INCREASES ──────────────────────
        import time as _time
        now_ts = _time.time()
        for cid, pos in current.items():
            if cid not in self.baseline:
                continue
            if self.first_poll:
                continue  # Never alert on first poll — baseline may be stale
            prev = self.baseline[cid]
            # Skip positions flagged closed — re-open path below handles this.
            # Without this guard, a falsely-closed row (e.g. from a flaky
            # Gamma resolution or a backfill mislabel) keeps firing SIZE UP
            # every poll because promote-to-baseline is gated on status.
            if prev.get("status") == "closed":
                continue
            prev_size = prev.get("size_usd", 0)
            new_size = pos["size_usd"]
            delta = new_size - prev_size
            if delta < self.cfg.size_increase_alert_usd:
                continue

            logger.info(
                "SIZE UP [%s]: %s | $%.0f -> $%.0f (+$%.0f) | %s",
                self.cfg.alias, pos["title"][:40],
                prev_size, new_size, delta, pos["direction"],
            )

            # Always update DB, but only send Telegram if sport passes filter
            self._update_position_size(cid, new_size, pos["price"], now)

            if not self._should_alert(pos["title"]):
                continue

            # Performance filter on the new total size (not delta)
            if not self._should_alert_by_performance(
                pos["price"], new_size, pos["title"]
            ):
                continue

            # Subtype / hour-of-day / tilt guards (same as NEW path).
            # Note: require_multi_trade is NOT checked here — SIZE UP is
            # exactly what we want to let through for those whales.
            if not self._should_alert_by_subtype(pos["title"]):
                continue
            if not self._should_alert_by_hour(pos["title"]):
                continue
            if not self._should_alert_by_tilt(pos["title"]):
                continue

            # Cooldown: suppress Telegram for 10 min per market UNLESS
            # the cumulative delta since last alert exceeds $20K (genuine
            # large size-up breaks through the cooldown).
            last_alert_ts, size_at_alert = self._size_alert_cooldown.get(cid, (0, 0))
            delta_since_alert = new_size - size_at_alert if size_at_alert else delta
            cooldown_active = (now_ts - last_alert_ts) < 600
            breakthrough = delta_since_alert >= 20_000

            if cooldown_active and not breakthrough:
                logger.info(
                    "SIZE UP [%s] (cooldown, skipping Telegram): %s | delta_since_alert=$%.0f",
                    self.cfg.alias, pos["title"][:40], delta_since_alert,
                )
                continue
            self._size_alert_cooldown[cid] = (now_ts, new_size)

            ctx_lines = await self._enrich_lines(
                condition_id=cid,
                direction=pos["direction"],
                token_id=pos.get("asset", ""),
                entry_price=pos["price"],
                size_usd=delta,
            )
            if breakthrough:
                header = f"{self.cfg.emoji}📈📈 <b>{self.cfg.alias.upper()} MAJOR SIZE-UP</b>"
                show_delta = delta_since_alert
            else:
                header = f"{self.cfg.emoji}📈 <b>{self.cfg.alias.upper()} ADDING SIZE</b>"
                show_delta = delta
            sport_flag = format_sport_flag(
                self.cfg.alias, pos["title"],
            )
            msg_lines = [
                header,
                "",
                f"<b>Market:</b> {pos['title']}",
                f"<b>Side:</b> {pos['direction']}",
                f"<b>Size:</b> ${prev_size:,.0f} → ${new_size:,.0f} "
                f"(+${show_delta:,.0f})",
                f"<b>Price:</b> ${pos['price']:.3f}",
            ]
            if sport_flag:
                msg_lines.append("")
                msg_lines.append(sport_flag)
            if ctx_lines:
                msg_lines.append("")
                msg_lines.extend(ctx_lines)
            msg_lines += ["", f"<i>Time: {now_pst}</i>"]
            await send_telegram("\n".join(msg_lines))

        # ── Detect RESOLUTIONS ──────────────────────────────────────
        for cid, prev in list(self.baseline.items()):
            # Skip if baseline marks closed OR this process has already
            # processed a resolution for this cid. The in-memory set is a
            # belt-and-suspenders backstop — if baseline[cid]["status"] write
            # is somehow bypassed, this set still catches repeat resolutions.
            if prev.get("status") == "closed" or cid in self._resolved_cids_this_session:
                continue

            cur = current.get(cid)
            resolved = False
            outcome = ""

            if cur is None:
                resolved = True
                outcome = "RESOLVED"
            elif cur["redeemable"]:
                resolved = True
                outcome = "WIN" if cur["cur_price"] >= 0.5 else "LOSS"
            elif cur["cur_price"] >= 0.98:
                resolved = True
                outcome = "WIN"
            elif cur["cur_price"] <= 0.02:
                resolved = True
                outcome = "LOSS"

            if not resolved:
                continue
            if self.first_poll:
                # Never send resolution alerts on first poll — baseline may
                # contain positions that resolved while the tracker was down.
                # Mark closed in both DB AND baseline so the promote section
                # doesn't overwrite it, causing re-detection on the next poll.
                logger.info(
                    "RESOLVED [%s] (first poll, skipping alert): %s | %s",
                    self.cfg.alias, outcome, prev.get("title", "?")[:40],
                )
                self._mark_resolved(cid, outcome, now)
                self._resolved_cids_this_session.add(cid)  # backstop
                if cid in self.baseline:
                    import time as _time_mod
                    self.baseline[cid]["status"] = "closed"
                    self.baseline[cid]["resolved_at_ts"] = _time_mod.time()
                continue

            title = prev.get("title", "?")
            entry_price = prev.get("price", 0.5)
            size_usd = prev.get("size_usd", 0)
            emoji = (
                "✅" if outcome == "WIN"
                else "❌" if outcome == "LOSS"
                else "📋"
            )

            logger.info(
                "RESOLVED [%s]: %s | %s | %s",
                self.cfg.alias, outcome, title[:40], cid[:16],
            )

            # Always update DB, only send Telegram if sport passes filter
            self._mark_resolved(cid, outcome, now)

            # CRITICAL: mark baseline closed + stamp resolution ts BEFORE any
            # gate check. Otherwise, if a downstream gate (sport / perf /
            # subtype / hour) returns False, we `continue` without marking
            # closed → every subsequent poll re-detects the same resolution
            # → infinite log spam. Discovered when TheOnlyHuman's NBA-only
            # sport filter rejected an MLB Royals-Tigers resolution and the
            # same RESOLVED line appeared 5× in 1 minute.
            self._resolved_cids_this_session.add(cid)  # bulletproof backstop
            if cid in self.baseline:
                import time as _time_mod
                self.baseline[cid]["status"] = "closed"
                self.baseline[cid]["resolved_at_ts"] = _time_mod.time()

            # Tilt state update: record loss timestamps regardless of whether
            # we send the alert. Tilt guard uses this to mute future alerts.
            if outcome == "LOSS":
                import time as _time
                self._last_loss_ts = _time.time()
                logger.info(
                    "[%s] LOSS recorded at ts=%.0f (tilt guard source)",
                    self.cfg.alias, self._last_loss_ts,
                )

            if not self._should_alert(title):
                continue

            # Performance filter: don't announce resolution for a bet we
            # suppressed at entry. Use the baseline fields captured at NEW.
            if not self._should_alert_by_performance(
                entry_price, size_usd, title
            ):
                continue

            # Subtype / hour guards — don't announce resolution for bets we
            # would have suppressed at entry. (Tilt guard is intentionally
            # skipped here: a resolution IS a tilt-source event.)
            if not self._should_alert_by_subtype(title):
                continue
            if not self._should_alert_by_hour(title):
                continue

            header = (
                f"{self.cfg.emoji}{emoji} <b>{self.cfg.alias.upper()} {outcome}</b>"
            )
            sport_flag = format_sport_flag(self.cfg.alias, title)
            sport_line = f"\n{sport_flag}" if sport_flag else ""
            msg = (
                f"{header}\n"
                f"\n"
                f"<b>Market:</b> {title}\n"
                f"<b>Side:</b> {prev.get('direction', '?')}\n"
                f"<b>Entry:</b> ${entry_price:.3f} | Size: ${size_usd:,.0f}"
                f"{sport_line}\n"
                f"\n"
                f"<i>Time: {now_pst}</i>"
            )
            await send_telegram(msg)
            # Note: baseline status=closed + resolved_at_ts already set above
            # BEFORE the gate checks (see CRITICAL comment) to prevent the
            # re-log loop when a gate rejects a resolution.

        # ── Promote current → baseline (skip closed entries) ──────
        #
        # Important: on a TRUE fresh install (no prior DB state), mark any
        # position already at the resolution rails as `status="closed"`
        # before promoting it. Without this, the next poll's RESOLUTION
        # branch fires on every already-won position in the whale's book
        # (caused a ~20-alert spam wave the first time bigsix spun up).
        #
        # Phase 3 bridge: on a fresh install we ALSO seed the live
        # `tracked_whale_positions` rows for any still-open positions,
        # so the ev_engine cashout path can see the whale's current
        # book immediately instead of waiting for a NEW MARKET / SIZE UP
        # event to land naturally. The seeded rows are flagged
        # `alert_sent=1` (via _insert_new_position) so we never re-alert
        # on them after the bridge write.
        seeded_live = 0
        for cid, pos in current.items():
            if (
                cid not in self.baseline
                or self.baseline[cid].get("status") != "closed"
            ):
                promoted = dict(pos)
                at_rails = (
                    pos.get("redeemable")
                    or pos.get("cur_price", 0.5) >= 0.98
                    or pos.get("cur_price", 0.5) <= 0.02
                )
                if self.first_poll and at_rails:
                    promoted["status"] = "closed"
                elif self.first_poll and self.fresh_install and not at_rails:
                    # Bridge-seed: silently persist this open position so
                    # the cashout engine can pick it up.
                    self._insert_new_position(cid, pos, now)
                    seeded_live += 1
                self.baseline[cid] = promoted

        if self.first_poll:
            logger.info(
                "%s: baseline updated, %d positions tracked (seeded %d live rows to DB)",
                self.cfg.alias, len(current), seeded_live,
            )
            self.first_poll = False

    # ── Daily report ──

    async def _maybe_send_daily_report(self) -> None:
        """Send a once-per-day W/L summary at `daily_report_hour_pst` local.

        Uses a hybrid approach:
          1. Positions API curPrice rails (>= 0.98 = WIN, <= 0.02 = LOSS)
          2. Activity API + CLOB last-trade-price to recover redeemed winners
             that vanished from the positions API.
        """
        now_pst = datetime.now(_PST)
        today_str = now_pst.strftime("%Y-%m-%d")

        if now_pst.hour < self.cfg.daily_report_hour_pst:
            return
        if self.last_daily_report_date == today_str:
            return

        try:
            positions = await self._fetch_positions()
        except Exception as e:
            logger.error("%s: daily report fetch failed: %s", self.cfg.alias, e)
            return

        wins = 0
        losses = 0
        open_positions = 0
        total_size_open = 0.0
        biggest_win: tuple[float, str] = (0.0, "")
        biggest_loss: tuple[float, str] = (0.0, "")
        seen_cids: set[str] = set()  # track conditionId:outcome combos counted

        # ── Step 1: Positions API (curPrice rails) ──
        for p in positions:
            init = float(p.get("initialValue") or 0)
            if init < self.cfg.min_position_usd:
                continue

            cur = float(p.get("curPrice") or 0)
            redeem = p.get("redeemable", False)
            title = p.get("title") or ""

            if redeem or cur >= 0.98 or cur <= 0.02:
                cid = p.get("conditionId", "")
                outcome = p.get("outcome", "")
                seen_cids.add(f"{cid}:{outcome}")

                if cur >= 0.98:
                    wins += 1
                    if init > biggest_win[0]:
                        biggest_win = (init, title)
                elif cur <= 0.02:
                    losses += 1
                    if init > biggest_loss[0]:
                        biggest_loss = (init, title)
            else:
                open_positions += 1
                total_size_open += init

        pos_wins, pos_losses = wins, losses

        # ── Step 2: Activity API supplement (catch redeemed winners) ──
        try:
            trades = await self._fetch_activity(lookback_days=7)
        except Exception as e:
            logger.debug("%s: activity fetch for daily report failed: %s", self.cfg.alias, e)
            trades = []

        if trades:
            # Group by conditionId, skip already-counted
            activity_positions: dict[str, dict] = {}
            for t in trades:
                cid = t.get("conditionId", "")
                outcome = t.get("outcome", "")
                pos_key = f"{cid}:{outcome}"
                if pos_key in seen_cids or not cid:
                    continue
                if cid not in activity_positions:
                    activity_positions[cid] = {
                        "title": t.get("title", ""),
                        "outcome": outcome,
                        "asset": t.get("asset", ""),
                        "buy_usd": 0.0,
                        "sell_usd": 0.0,
                        "pos_key": pos_key,
                    }
                usd = float(t.get("usdcSize", 0))
                if t.get("side") == "BUY":
                    activity_positions[cid]["buy_usd"] += usd
                else:
                    activity_positions[cid]["sell_usd"] += usd

            for cid, apos in activity_positions.items():
                if apos["buy_usd"] == 0:
                    continue
                net_cost = apos["buy_usd"] - apos["sell_usd"]
                if net_cost <= 0:
                    continue

                token_price = await self._check_token_price(apos["asset"])
                await asyncio.sleep(0.05)

                if token_price is None:
                    continue
                if token_price >= 0.90:
                    wins += 1
                    seen_cids.add(apos["pos_key"])
                    if net_cost > biggest_win[0]:
                        biggest_win = (net_cost, apos["title"])
                elif token_price <= 0.10:
                    losses += 1
                    seen_cids.add(apos["pos_key"])
                    if net_cost > biggest_loss[0]:
                        biggest_loss = (net_cost, apos["title"])

        extra_w = wins - pos_wins
        extra_l = losses - pos_losses
        if extra_w or extra_l:
            logger.info(
                "%s: daily report recovered +%dW/+%dL from activity API",
                self.cfg.alias, extra_w, extra_l,
            )

        resolved = wins + losses
        wr = (wins / resolved) if resolved else 0.0

        lines = [
            f"{self.cfg.emoji}📊 <b>{self.cfg.alias.upper()} DAILY REPORT</b>",
            "",
            f"<b>Record:</b> {wins}W / {losses}L ({wr:.0%} WR)",
            f"<b>Open positions:</b> {open_positions}",
            f"<b>Open exposure:</b> ${total_size_open:,.0f}",
        ]
        if biggest_win[0] > 0:
            lines.append(
                f"<b>Biggest win:</b> ${biggest_win[0]:,.0f} — {biggest_win[1][:40]}"
            )
        if biggest_loss[0] > 0:
            lines.append(
                f"<b>Biggest loss:</b> ${biggest_loss[0]:,.0f} — {biggest_loss[1][:40]}"
            )
        lines.append("")
        lines.append(
            f"<i>Snapshot: {now_pst.strftime('%b %d, %I:%M %p PST')}</i>"
        )

        await send_telegram("\n".join(lines))
        logger.info(
            "%s: daily report sent — %dW/%dL (%.0f%%), %d open",
            self.cfg.alias, wins, losses, wr * 100, open_positions,
        )
        self.last_daily_report_date = today_str


# ── Convenience runner ────────────────────────────────────────────────


async def run_tracker(cfg: WhaleConfig) -> None:
    """Standard tracker runner: handle signals + graceful shutdown."""
    import signal

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
    )

    # Load .env directly since tmux sessions don't always inherit it
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())
    # Refresh module-level telegram credentials after .env load
    global TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "") or TELEGRAM_BOT_TOKEN
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "") or TELEGRAM_CHAT_ID

    tracker = WhaleVIPTracker(cfg)

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                sig, lambda: asyncio.create_task(tracker.stop()),
            )
        except NotImplementedError:
            pass  # Windows

    try:
        await tracker.start()
    except KeyboardInterrupt:
        pass
    finally:
        await tracker.stop()
