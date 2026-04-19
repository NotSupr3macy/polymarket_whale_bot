"""
Paper Trading Bot — $100 bankroll multi-whale copy trader.

Reads filtered whale signals from `tracked_whale_positions` and
`texaskid_positions`, opens paper positions at the whale's entry price,
sizes them proportionally to a $100 bankroll with conviction multipliers,
and closes them when the underlying whale position resolves.

Sends 3 types of notifications to a SEPARATE Telegram bot (new token via
PAPER_BOT_TOKEN env var — distinct from the parent whale-tracker bot):
  1. NEW PAPER TRADE  — opens
  2. PAPER WIN / LOSS — resolutions
  3. 6H WHALE UPDATE  — per-whale W/L + PnL + bankroll

Design:
  - Read-only consumer — never modifies whale tracker tables
  - Polls every 30 seconds
  - Resolutions processed BEFORE new-signal scan each tick so freed bankroll
    is immediately deployable
  - State persisted in `paper_state` + `paper_positions` tables
  - Survives restart — open positions, bankroll, and 6h timer all recover
    from DB on boot
  - Dry-run mode via `DRY_RUN=1` env var (short-circuits Telegram sends)

Known v1 limitations:
  - Does NOT scale paper size when whale does SIZE UP (original size kept)
  - Consensus detection has a 30-min window per spec
  - `nbasniper` is a paper-only shadow whale — his tracker writes
    muted_reason='sport' (empty solo_alert_sports), so his candidate query
    specifically bypasses the mute check
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import aiohttp

# Allow importing from project root if run as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-14s | %(message)s",
)
logger = logging.getLogger("paper_trader")

# ── Config ─────────────────────────────────────────────────────────────
DB_PATH = os.getenv(
    "DB_PATH",
    str(Path(__file__).resolve().parent.parent / "trades.db"),
)

# Load .env manually (tmux sessions don't always inherit env)
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())

PAPER_BOT_TOKEN = os.getenv("PAPER_BOT_TOKEN", "")
PAPER_BOT_CHAT_ID = os.getenv("PAPER_BOT_CHAT_ID", "")
# Paper-trader-specific dry-run flag. Deliberately NOT `DRY_RUN` because the
# live trading bot (bot.py, config.py) already uses that env var to toggle
# real-money execution — and the server has it set to "true" permanently.
# Our paper trader is already a simulator; we want Telegram alerts to fire
# by default. Only opt into no-Telegram mode via PAPER_TRADER_SILENT=1 (for
# local tests).
DRY_RUN = os.getenv("PAPER_TRADER_SILENT", "").lower() in ("1", "true", "yes")

STARTING_BANKROLL = 100.0
POLL_INTERVAL_SEC = 30
UPDATE_INTERVAL_HOURS = 6
BANKROLL_DEPLOYMENT_CAP_FRAC = 0.60  # max 60% of bankroll deployed
# Apr 18 bump 0.50 → 0.60: with bigger sizes on winning whales
# (sportmaster $6, GIAYN $5), we'd otherwise clip concurrent fires at
# 8–9 open positions. Headroom needed so conviction-multiplier
# consensus trades ($9–$15) can actually land.
HARD_SIZE_CAP_USD = 15.0  # from 3-way consensus
MIN_POSITION_USD = 3.0  # below this, we won't open

# Per-whale base allocation (fraction of bankroll) — confirmed by user
#
# Apr 19 update (after 24h live review):
#   texaskid    1W/6L  (14% WR, −$25 over 24h)  → REMOVED; now SHADOW only
#                      (see SHADOW_WHALES below — positions log-only, no open)
#   sportmaster 13W/5L (72% WR, +$26)           — kept at $6 (top performer)
#   Others as set in Apr 18 rebalance.
BASE_ALLOC = {
    "TheOnlyHuman": 0.08,          # $8 — unchanged, limited live data
    "kch123": 0.05,                # $5 — unchanged, quiet whale
    "nbasniper": 0.04,             # $4 — shadow-to-live, no filter yet
    "GamblingIsAllYouNeed": 0.05,  # $5 — after 4W/1L live then 7W/5L over 24h
    "sportmaster777": 0.06,        # $6 — crushing it, 72% WR across 18 bets
    "bigsix": 0.03,                # $3 — unchanged
}

# Whales muted from opening paper positions, but their candidate rows are
# still logged at INFO level once per (alias, cid) so we keep collecting
# performance data to decide re-inclusion. They are NOT in BASE_ALLOC.
#
# texaskid: 1W/6L over first 24h (14% WR, −$25 realized). Re-evaluate when
# he shows 5 wins across any rolling 7-day window.
SHADOW_WHALES = {"texaskid"}

# Whales exempt from the post-loss tilt-guard multiplier (×0.5).
# Rationale: tilt guard was designed for kch123 after his $430K
# after-loss re-entry disaster. But high-frequency whales like
# GamblingIsAllYouNeed take frequent small losses as normal variance
# (62.5% WR on 339 resolved = ~127 losses, most not emotional tilt).
# Halving their base size ($4 → $2) then collides with MIN_POSITION_USD=$3
# and mutes them entirely for 4 hours after every loss — effectively
# killing their signal for the rest of the day.
#
# Apr 19: added sportmaster777 after 24h data. He's 72% WR on 18 bets and
# tilt-guard was systematically cutting his $6 base to $3 (because he'd taken
# a single loss in the prior 4h). That's penalizing his PROVEN edge — the
# opposite of what we want.
TILT_GUARD_EXCLUDE = {"GamblingIsAllYouNeed", "sportmaster777"}

# Per-whale concurrent-position cap. Defaults to unlimited. Applied in
# can_open() to prevent one high-frequency whale from monopolizing the
# global deploy-cap slot pool.
#
# GIAYN peaked at 13 simultaneous open positions on Apr 18, consuming $52
# of the $58.47 deploy-cap budget — kch123's Flyers entry got skipped 20+
# times over 10 min waiting for any slot. Cap GIAYN at 8 open to leave
# runway for other whales.
MAX_CONCURRENT_BY_WHALE = {"GamblingIsAllYouNeed": 8}

# Skip any entry where the market's game_start_time is within this many
# minutes away, OR the game has already started. Catches:
#   - last-15-min pre-game fires (often hedges or line-chase)
#   - in-game whale entries at extreme prices (e.g. sportmaster Rockets/Lakers
#     Over 207.5 at $0.073 — mid-game scramble, lost $3)
# Set to 0 to disable.
MIN_MINUTES_TO_GAME_START = 15

# Which table each whale writes to (texaskid = legacy separate table)
WHALE_TABLE = {
    "TheOnlyHuman": "tracked_whale_positions",
    "kch123": "tracked_whale_positions",
    "bigsix": "tracked_whale_positions",
    "nbasniper": "tracked_whale_positions",
    "GamblingIsAllYouNeed": "tracked_whale_positions",
    "sportmaster777": "tracked_whale_positions",
    "texaskid": "texaskid_positions",
}

# US Pacific Time (display-only)
_PST = timezone(timedelta(hours=-7))


# ── Schema ─────────────────────────────────────────────────────────────
_SCHEMA = """
CREATE TABLE IF NOT EXISTS paper_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    whale_alias TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    direction TEXT NOT NULL,
    market_title TEXT NOT NULL,
    entry_price REAL NOT NULL,
    paper_size_usd REAL NOT NULL,
    bankroll_at_open REAL NOT NULL,
    conviction_mult REAL NOT NULL DEFAULT 1.0,
    opened_at TEXT NOT NULL,
    resolved_at TEXT,
    outcome TEXT NOT NULL DEFAULT 'OPEN',
    resolution_price REAL,
    paper_pnl REAL,
    source_table TEXT NOT NULL,
    UNIQUE(whale_alias, condition_id, opened_at)
);

CREATE INDEX IF NOT EXISTS idx_paper_positions_outcome
    ON paper_positions(outcome);

CREATE INDEX IF NOT EXISTS idx_paper_positions_whale_opened
    ON paper_positions(whale_alias, opened_at);

CREATE TABLE IF NOT EXISTS paper_state (
    id INTEGER PRIMARY KEY CHECK (id=1),
    bankroll_usd REAL NOT NULL,
    started_at TEXT NOT NULL,
    next_update_ts TEXT NOT NULL,
    last_update_ts TEXT
);
"""


def init_db(db_path: str = DB_PATH) -> None:
    """Create paper_positions + paper_state tables if missing. Idempotent."""
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(_SCHEMA)
        # Seed state row on fresh install
        cur = conn.execute("SELECT COUNT(*) FROM paper_state")
        if cur.fetchone()[0] == 0:
            now_iso = datetime.now(timezone.utc).isoformat()
            next_update = (
                datetime.now(timezone.utc)
                + timedelta(hours=UPDATE_INTERVAL_HOURS)
            ).isoformat()
            conn.execute(
                "INSERT INTO paper_state (id, bankroll_usd, started_at, next_update_ts)"
                " VALUES (1, ?, ?, ?)",
                (STARTING_BANKROLL, now_iso, next_update),
            )
            logger.info(
                "paper_state seeded: bankroll=$%.2f, started_at=%s",
                STARTING_BANKROLL, now_iso,
            )
        conn.commit()
    finally:
        conn.close()


# ── State helpers ──────────────────────────────────────────────────────
def load_state(conn: sqlite3.Connection) -> dict:
    row = conn.execute(
        "SELECT bankroll_usd, started_at, next_update_ts, last_update_ts"
        " FROM paper_state WHERE id=1"
    ).fetchone()
    if not row:
        raise RuntimeError("paper_state missing — init_db() not run?")
    return {
        "bankroll_usd": float(row[0]),
        "started_at": row[1],
        "next_update_ts": row[2],
        "last_update_ts": row[3],
    }


def set_bankroll(conn: sqlite3.Connection, bankroll: float) -> None:
    conn.execute(
        "UPDATE paper_state SET bankroll_usd=? WHERE id=1", (bankroll,)
    )


def set_next_update(conn: sqlite3.Connection, next_ts: str) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE paper_state SET next_update_ts=?, last_update_ts=? WHERE id=1",
        (next_ts, now_iso),
    )


# ── Telegram ───────────────────────────────────────────────────────────
async def send_telegram(message: str) -> bool:
    """Send via the PAPER bot token (separate from parent whale-tracker bot)."""
    if DRY_RUN:
        logger.info("[DRY_RUN] Telegram skipped: %s", message[:80])
        return True
    if not PAPER_BOT_TOKEN or not PAPER_BOT_CHAT_ID:
        logger.warning("Paper Telegram not configured — alert not sent")
        return False

    url = f"https://api.telegram.org/bot{PAPER_BOT_TOKEN}/sendMessage"
    chat_ids = [c.strip() for c in PAPER_BOT_CHAT_ID.split(",") if c.strip()]
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
                    url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        any_success = True
                    else:
                        body = await resp.text()
                        logger.warning(
                            "Paper Telegram error %d: %s",
                            resp.status, body[:200],
                        )
        except Exception as e:
            logger.debug("Paper Telegram send failed: %s", e)
    return any_success


# ── Conviction ─────────────────────────────────────────────────────────
def compute_conviction_mult(
    conn: sqlite3.Connection, alias: str, cid: str, direction: str,
) -> tuple[float, str]:
    """Return (multiplier, description_for_log)."""
    mult = 1.0
    desc_parts = []
    # Consensus: count OTHER whales with OPEN paper position on same (cid, direction)
    n_peers = conn.execute(
        """SELECT COUNT(DISTINCT whale_alias) FROM paper_positions
           WHERE condition_id=? AND direction=?
             AND outcome='OPEN' AND whale_alias != ?
             AND opened_at > datetime('now','-30 minutes')""",
        (cid, direction, alias),
    ).fetchone()[0]
    total_whales = n_peers + 1
    if total_whales == 2:
        mult *= 1.5
        desc_parts.append("consensus-2x=1.5")
    elif total_whales >= 3:
        mult *= 2.0
        desc_parts.append(f"consensus-{total_whales}x=2.0")

    # After-loss: same whale had a LOSS in last 4 hours → halve the size
    # (unless this whale is exempt — see TILT_GUARD_EXCLUDE rationale above).
    if alias not in TILT_GUARD_EXCLUDE:
        recent_loss = conn.execute(
            """SELECT 1 FROM paper_positions
               WHERE whale_alias=? AND outcome='LOSS'
                 AND resolved_at > datetime('now','-4 hours') LIMIT 1""",
            (alias,),
        ).fetchone()
        if recent_loss:
            mult *= 0.5
            desc_parts.append("tilt-guard=0.5")

    return mult, ",".join(desc_parts) if desc_parts else "none"


# ── Bankroll checks ────────────────────────────────────────────────────
def currently_deployed(conn: sqlite3.Connection) -> float:
    row = conn.execute(
        "SELECT COALESCE(SUM(paper_size_usd),0) FROM paper_positions"
        " WHERE outcome='OPEN'"
    ).fetchone()
    return float(row[0])


def can_open(
    conn: sqlite3.Connection, size: float, state: dict,
    alias: Optional[str] = None,
) -> tuple[bool, str]:
    """Return (ok, reason_if_not).

    If `alias` is supplied, also enforces MAX_CONCURRENT_BY_WHALE.
    """
    if size < MIN_POSITION_USD:
        return False, f"size ${size:.2f} below min ${MIN_POSITION_USD}"
    if state["bankroll_usd"] < size:
        return False, f"bankroll ${state['bankroll_usd']:.2f} < size ${size:.2f}"
    deployed = currently_deployed(conn)
    # Cap deployed to BANKROLL_DEPLOYMENT_CAP_FRAC of current equity
    # (bankroll + already-deployed)
    equity = state["bankroll_usd"] + deployed
    cap = equity * BANKROLL_DEPLOYMENT_CAP_FRAC
    if deployed + size > cap:
        return False, (
            f"deploy cap ${cap:.2f} would be exceeded "
            f"(current ${deployed:.2f} + ${size:.2f})"
        )
    if alias is not None:
        per_whale_cap = MAX_CONCURRENT_BY_WHALE.get(alias)
        if per_whale_cap is not None:
            n_open_for_whale = conn.execute(
                "SELECT COUNT(*) FROM paper_positions"
                " WHERE whale_alias=? AND outcome='OPEN'",
                (alias,),
            ).fetchone()[0]
            if n_open_for_whale >= per_whale_cap:
                return False, (
                    f"per-whale cap {per_whale_cap} reached "
                    f"for {alias} ({n_open_for_whale} open)"
                )
    return True, ""


# ── Shadow whales — log-only candidates ────────────────────────────────
# Tracks (alias, cid) we've already logged this session so we don't spam
# INFO lines every 30s. Resets on process restart (re-logs once per restart
# for any still-open shadow signal, which is acceptable).
_shadow_logged_keys: set[tuple[str, str]] = set()


def log_shadow_candidate(sig: dict) -> None:
    """Log one INFO line per unique (alias, cid) shadow-whale candidate so
    we can later assess whether that whale deserves re-promotion."""
    key = (sig["alias"], sig["cid"])
    if key in _shadow_logged_keys:
        return
    _shadow_logged_keys.add(key)
    logger.info(
        "SHADOW [%s] WOULD_OPEN: %s side=%s entry=$%.3f whale_size=$%.0f",
        sig["alias"], sig["title"][:60], sig["direction"],
        sig["entry_price"], sig["whale_size_usd"],
    )


# ── Game-start time lookup (cached via game_start_radar) ───────────────
try:
    from monitor.game_start_radar import fetch_game_start, parse_iso_utc
except ImportError:
    # Graceful fallback if import ordering breaks — disable the filter.
    fetch_game_start = None  # type: ignore
    parse_iso_utc = None  # type: ignore


async def too_close_to_game_start(cid: str) -> tuple[bool, str]:
    """Return (should_skip, reason).

    Blocks when game_start_time is within MIN_MINUTES_TO_GAME_START minutes
    from now, OR when the game has already started. If gameStartTime is
    unknown (non-sports market, API down) we allow the open through.
    """
    if MIN_MINUTES_TO_GAME_START <= 0 or fetch_game_start is None:
        return False, ""
    try:
        start_iso, _slug = await asyncio.to_thread(fetch_game_start, cid)
    except Exception as e:
        logger.debug("game_start lookup err for %s: %s", cid[:16], e)
        return False, ""
    if not start_iso or parse_iso_utc is None:
        return False, ""
    start_dt = parse_iso_utc(start_iso)
    if start_dt is None:
        return False, ""
    mins_until = (start_dt - datetime.now(timezone.utc)).total_seconds() / 60.0
    if mins_until < MIN_MINUTES_TO_GAME_START:
        return True, (
            f"game starts in {mins_until:+.1f} min "
            f"(< {MIN_MINUTES_TO_GAME_START} min cutoff)"
        )
    return False, ""


# ── Candidate query ────────────────────────────────────────────────────
def query_candidates(conn: sqlite3.Connection, started_at: str) -> list[dict]:
    """Find all whale positions that should trigger paper opens.

    A candidate must:
      - Come from a tracked whale in BASE_ALLOC
      - Be open (status='open')
      - Have been first-seen AFTER the paper trader started (avoid
        retroactively opening a bunch of stale positions at first launch)
      - Be unmuted (muted_reason IS NULL) — EXCEPT for nbasniper whose
        tracker emits muted_reason='sport' in shadow mode; we want to
        copy those too
      - NOT already have an OPEN paper position with the same cid
    """
    rows = []
    # Cross-whale duplicate filter: block a candidate if ANY other paper
    # position is already OPEN on the same (cid, direction) and was opened
    # MORE than 30 minutes ago. Inside the 30-min window we allow the open
    # through so the consensus-multiplier (2-whale=1.5x, 3-whale=2.0x) can
    # stack on the existing position. Outside it, a second fire is just
    # duplicate exposure on the same thesis and we skip.
    #
    # NOTE: julianday() is used instead of string comparison because
    # opened_at is stored as Python isoformat (`2026-04-18T08:45:00+00:00`)
    # while SQLite's datetime('now','-30 minutes') returns a space-separated
    # format — the two don't compare lexicographically due to the T vs space
    # character mismatch at position 11. julianday() parses both.
    DUPE_FILTER_SQL = """
        AND NOT EXISTS (
            SELECT 1 FROM paper_positions pp_dupe
            WHERE pp_dupe.condition_id = {table}.condition_id
              AND pp_dupe.direction = {table}.direction
              AND pp_dupe.outcome = 'OPEN'
              AND julianday(pp_dupe.opened_at)
                  <= julianday('now', '-30 minutes')
        )
    """
    # ── tracked_whale_positions (bigsix, kch123, TheOnlyHuman, nbasniper) ──
    # Use alias match (case insensitive via LOWER) to keep SQL simple.
    # nbasniper bypass: include his rows even when muted_reason='sport'.
    tracked_aliases = [a for a, t in WHALE_TABLE.items() if t == "tracked_whale_positions"]
    placeholders = ",".join("?" for _ in tracked_aliases)
    cur = conn.execute(
        f"""
        SELECT alias, condition_id, direction, market_title,
               first_seen_price, first_seen_size_usd, first_seen_at,
               muted_reason
        FROM tracked_whale_positions
        WHERE alias IN ({placeholders})
          AND status = 'open'
          AND first_seen_at > ?
          AND (
              muted_reason IS NULL
              OR (alias = 'nbasniper' AND muted_reason = 'sport')
          )
          AND NOT EXISTS (
              SELECT 1 FROM paper_positions pp
              WHERE pp.whale_alias = tracked_whale_positions.alias
                AND pp.condition_id = tracked_whale_positions.condition_id
                AND pp.outcome = 'OPEN'
          )
          {DUPE_FILTER_SQL.format(table='tracked_whale_positions')}
        ORDER BY first_seen_at ASC
        """,
        (*tracked_aliases, started_at),
    )
    for r in cur.fetchall():
        rows.append({
            "alias": r[0], "cid": r[1], "direction": r[2],
            "title": r[3], "entry_price": r[4] or 0.5,
            "whale_size_usd": r[5] or 0, "first_seen_at": r[6],
            "muted_reason": r[7], "source_table": "tracked_whale_positions",
        })

    # ── texaskid_positions (legacy table, always alias='texaskid') ──
    cur = conn.execute(
        f"""
        SELECT 'texaskid', condition_id, direction, market_title,
               first_seen_price, first_seen_size_usd, first_seen_at,
               muted_reason
        FROM texaskid_positions
        WHERE status = 'open'
          AND first_seen_at > ?
          AND muted_reason IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM paper_positions pp
              WHERE pp.whale_alias = 'texaskid'
                AND pp.condition_id = texaskid_positions.condition_id
                AND pp.outcome = 'OPEN'
          )
          {DUPE_FILTER_SQL.format(table='texaskid_positions')}
        ORDER BY first_seen_at ASC
        """,
        (started_at,),
    )
    for r in cur.fetchall():
        rows.append({
            "alias": r[0], "cid": r[1], "direction": r[2],
            "title": r[3], "entry_price": r[4] or 0.5,
            "whale_size_usd": r[5] or 0, "first_seen_at": r[6],
            "muted_reason": r[7], "source_table": "texaskid_positions",
        })
    return rows


def query_source_status(
    conn: sqlite3.Connection, source_table: str, alias: str, cid: str,
) -> Optional[dict]:
    """Look up the whale-tracker row for this paper position's cid to see
    if it has resolved yet. Returns None if the row was deleted (unlikely)."""
    if source_table == "tracked_whale_positions":
        row = conn.execute(
            "SELECT status, outcome, current_price, resolved_at"
            " FROM tracked_whale_positions"
            " WHERE alias=? AND condition_id=?",
            (alias, cid),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT status, outcome, current_price, resolved_at"
            " FROM texaskid_positions WHERE condition_id=?",
            (cid,),
        ).fetchone()
    if not row:
        return None
    return {
        "status": row[0], "outcome": row[1],
        "current_price": row[2], "resolved_at": row[3],
    }


async def resolve_ambiguous_via_gamma(
    cid: str, direction: str,
) -> Optional[tuple[str, float]]:
    """Last-resort lookup for paper positions whose source whale flipped
    to status='closed' with outcome='RESOLVED' (i.e. the whale's position
    DISAPPEARED from Polymarket /positions before our tracker caught its
    price hitting the rails — common for high-frequency whales who redeem
    or sell out quickly).

    Queries Polymarket Gamma directly for the market's final outcomePrices
    to determine whether the paper position actually won or lost.

    Returns:
        ('WIN', 1.0)   — paper direction is the winning side
        ('LOSS', 0.0)  — paper direction is the losing side
        None           — couldn't determine (Gamma unavailable, market still
                         live, or direction not found in outcomes array).
                         Caller should fall back to $0 break-even behavior.
    """
    for params in [
        {"condition_ids": cid, "closed": "true"},
        {"condition_ids": cid},
    ]:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://gamma-api.polymarket.com/markets",
                    params=params,
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    if r.status != 200:
                        continue
                    data = await r.json()
        except Exception as e:
            logger.debug("Gamma lookup err for %s: %s", cid[:16], e)
            continue

        if not data:
            continue
        m = data[0]
        try:
            op = m.get("outcomePrices", "[]")
            op = json.loads(op) if isinstance(op, str) else op
            oc = m.get("outcomes", "[]")
            oc = json.loads(oc) if isinstance(oc, str) else oc
        except Exception:
            continue

        # Match direction → outcome index (case-insensitive strip compare)
        idx = next(
            (i for i, x in enumerate(oc)
             if str(x).strip().lower() == direction.strip().lower()),
            None,
        )
        if idx is None or idx >= len(op):
            continue
        try:
            wp = float(op[idx])
        except (TypeError, ValueError):
            continue

        if wp >= 0.99:
            return ("WIN", 1.0)
        if wp <= 0.01:
            return ("LOSS", 0.0)
        # Market not actually at rails — genuine ambiguity, keep as break-even.
        return None

    return None


# ── Open / close ──────────────────────────────────────────────────────
async def open_paper_position(
    conn: sqlite3.Connection, sig: dict, size: float, mult: float,
    state: dict,
) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT OR IGNORE INTO paper_positions
           (whale_alias, condition_id, direction, market_title, entry_price,
            paper_size_usd, bankroll_at_open, conviction_mult,
            opened_at, outcome, source_table)
           VALUES (?,?,?,?,?,?,?,?,?,'OPEN',?)""",
        (
            sig["alias"], sig["cid"], sig["direction"], sig["title"],
            sig["entry_price"], size, state["bankroll_usd"], mult, now_iso,
            sig["source_table"],
        ),
    )
    if conn.total_changes == 0:
        logger.debug(
            "INSERT OR IGNORE skipped (race): %s %s",
            sig["alias"], sig["cid"][:16],
        )
        return
    new_bankroll = state["bankroll_usd"] - size
    set_bankroll(conn, new_bankroll)
    state["bankroll_usd"] = new_bankroll
    conn.commit()

    base_pct = int(BASE_ALLOC[sig["alias"]] * 100)
    mult_str = f"{mult:.1f}x" if mult != 1.0 else "1.0x"
    open_stats = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(paper_size_usd), 0)"
        " FROM paper_positions WHERE outcome='OPEN'"
    ).fetchone()
    n_open = int(open_stats[0])
    deployed = float(open_stats[1])
    msg = (
        f"🧪 <b>NEW PAPER TRADE — {sig['alias']}</b>\n\n"
        f"<b>Market:</b> {sig['title']}\n"
        f"<b>Side:</b> {sig['direction']}\n"
        f"<b>Entry:</b> ${sig['entry_price']:.3f}\n"
        f"<b>Whale stake:</b> ${sig['whale_size_usd']:,.0f}\n"
        f"<b>Paper size:</b> ${size:.2f}  ({base_pct}% × {mult_str} conviction)\n"
        f"\n"
        f"<b>Bankroll:</b> ${new_bankroll:.2f} | <b>In positions:</b> ${deployed:.2f} ({n_open} open)"
    )
    await send_telegram(msg)
    logger.info(
        "OPEN [%s] %s %s @ $%.3f  size=$%.2f  mult=%.2f  bankroll=$%.2f",
        sig["alias"], sig["direction"], sig["title"][:40],
        sig["entry_price"], size, mult, new_bankroll,
    )


async def close_paper_position(
    conn: sqlite3.Connection, pp: dict, src: dict, state: dict,
) -> None:
    """Close a paper position given the whale's resolution outcome."""
    # Determine payout:
    # Binary market — on WIN you get 1.0 per share; on LOSS you get 0.
    # Paper P&L at resolution:
    #   WIN:  size * (1/entry - 1) = size/entry - size
    #   LOSS: -size
    tracker_outcome = src["outcome"]
    entry = pp["entry_price"]
    size = pp["paper_size_usd"]

    # CRITICAL: always verify the outcome via Gamma API using THIS PAPER
    # POSITION'S direction — don't trust the whale_tracker's outcome field.
    #
    # Why: whale_tracker only tracks ONE side per (wallet, cid). If a whale
    # holds BOTH sides of a market (e.g. original bet on Timberwolves, then
    # bought Nuggets when Timberwolves was losing), the tracker may pick the
    # winning side's price at resolution → wrongly mark outcome='WIN' even
    # though our paper position was on the losing side.
    #
    # Confirmed bug on Apr 18: sportmaster777 Timberwolves ML @ $0.324 was
    # marked WIN +$6.25 by paper_trader trusting tracker, but Gamma showed
    # Timberwolves=$0.000 (losing side). paper was actually LOSS −$3.
    #
    # Gamma's outcomePrices are the source of truth — they reflect the
    # market's actual resolution, not the whale's holdings.
    outcome = tracker_outcome
    gamma_result = await resolve_ambiguous_via_gamma(
        pp["condition_id"], pp["direction"],
    )
    if gamma_result is not None:
        gamma_outcome, _ = gamma_result
        if gamma_outcome != tracker_outcome and tracker_outcome in ("WIN", "LOSS"):
            logger.warning(
                "[%s] tracker outcome %s DISAGREES with Gamma %s for %s "
                "side=%s — using Gamma (likely dual-side position bug)",
                pp["whale_alias"], tracker_outcome, gamma_outcome,
                pp["market_title"][:40], pp["direction"],
            )
        elif tracker_outcome == "RESOLVED":
            logger.info(
                "[%s] ambiguous RESOLVED upgraded via Gamma to %s: %s (cid=%s)",
                pp["whale_alias"], gamma_outcome,
                pp["market_title"][:40], pp["condition_id"][:16],
            )
        outcome = gamma_outcome
    else:
        if tracker_outcome == "RESOLVED":
            logger.info(
                "[%s] RESOLVED remains ambiguous after Gamma lookup: %s",
                pp["whale_alias"], pp["market_title"][:40],
            )
        else:
            logger.debug(
                "[%s] Gamma lookup failed for verification, trusting tracker outcome %s",
                pp["whale_alias"], tracker_outcome,
            )

    if outcome == "WIN":
        pnl = size * (1.0 / entry - 1.0) if entry > 0 else 0.0
        resolution_price = 1.0
        return_to_bankroll = size + pnl
        emoji = "✅"
    elif outcome == "LOSS":
        pnl = -size
        resolution_price = 0.0
        return_to_bankroll = 0.0
        emoji = "❌"
    else:
        # RESOLVED / ambiguous — treat as break-even (return stake only)
        pnl = 0.0
        resolution_price = float(src.get("current_price") or 0.5)
        return_to_bankroll = size
        emoji = "📋"

    now_iso = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """UPDATE paper_positions
           SET resolved_at=?, outcome=?, resolution_price=?, paper_pnl=?
           WHERE id=?""",
        (now_iso, outcome, resolution_price, pnl, pp["id"]),
    )
    new_bankroll = state["bankroll_usd"] + return_to_bankroll
    set_bankroll(conn, new_bankroll)
    state["bankroll_usd"] = new_bankroll
    conn.commit()

    # Post-close snapshot of remaining open positions for the alert footer
    open_stats = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(paper_size_usd), 0)"
        " FROM paper_positions WHERE outcome='OPEN'"
    ).fetchone()
    n_open = int(open_stats[0])
    deployed = float(open_stats[1])

    msg = (
        f"🧪{emoji} <b>PAPER {outcome} — {pp['whale_alias']}</b>\n\n"
        f"<b>Market:</b> {pp['market_title']}\n"
        f"<b>Side:</b> {pp['direction']} | <b>Entry:</b> ${entry:.3f} → ${resolution_price:.3f}\n"
        f"<b>Paper P&amp;L:</b> ${pnl:+.2f}\n"
        f"\n"
        f"<b>Bankroll:</b> ${new_bankroll:.2f} | <b>In positions:</b> ${deployed:.2f} ({n_open} open)"
    )
    await send_telegram(msg)
    logger.info(
        "CLOSE [%s] %s %s @ %.3f -> %.3f  pnl=$%.2f  bankroll=$%.2f",
        pp["whale_alias"], outcome, pp["market_title"][:40],
        entry, resolution_price, pnl, new_bankroll,
    )


# ── 6-hour update ──────────────────────────────────────────────────────
async def send_6h_update(conn: sqlite3.Connection, state: dict) -> None:
    bankroll = state["bankroll_usd"]
    pct_delta = (bankroll - STARTING_BANKROLL) / STARTING_BANKROLL * 100.0

    # All-time aggregates across resolved positions
    agg = conn.execute(
        """SELECT
              COALESCE(SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END), 0),
              COALESCE(SUM(CASE WHEN outcome='LOSS' THEN 1 ELSE 0 END), 0),
              COALESCE(SUM(paper_pnl), 0)
           FROM paper_positions
           WHERE outcome IN ('WIN','LOSS')"""
    ).fetchone()
    total_w, total_l, total_pnl = int(agg[0]), int(agg[1]), float(agg[2])
    total_n = total_w + total_l
    total_wr = (total_w / total_n * 100) if total_n else 0.0

    # Currently open
    n_open_row = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(paper_size_usd),0)"
        " FROM paper_positions WHERE outcome='OPEN'"
    ).fetchone()
    n_open = int(n_open_row[0])
    deployed = float(n_open_row[1])

    # Per-whale stats
    per_whale = conn.execute(
        """SELECT whale_alias,
                  SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) AS w,
                  SUM(CASE WHEN outcome='LOSS' THEN 1 ELSE 0 END) AS l,
                  COALESCE(SUM(paper_pnl),0) AS pnl
           FROM paper_positions
           WHERE outcome IN ('WIN','LOSS')
           GROUP BY whale_alias"""
    ).fetchall()
    whale_stats = {
        row[0]: {"w": int(row[1] or 0), "l": int(row[2] or 0), "pnl": float(row[3] or 0)}
        for row in per_whale
    }

    # Last 6h delta
    recent = conn.execute(
        """SELECT
              COALESCE(SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END), 0),
              COALESCE(SUM(CASE WHEN outcome='LOSS' THEN 1 ELSE 0 END), 0),
              COALESCE(SUM(paper_pnl), 0)
           FROM paper_positions
           WHERE outcome IN ('WIN','LOSS')
             AND resolved_at > datetime('now', '-6 hours')"""
    ).fetchone()
    r_w, r_l, r_pnl = int(recent[0]), int(recent[1]), float(recent[2])

    # Format
    now_pst = datetime.now(_PST).strftime("%a %b %d, %I:%M %p PST")
    lines = [
        f"📊 <b>PAPER BOT UPDATE — {now_pst}</b>",
        "",
        f"<b>Bankroll:</b> ${bankroll:.2f} ({pct_delta:+.1f}%) | "
        f"<b>In positions:</b> ${deployed:.2f} ({n_open} open)",
        f"<b>Total:</b> {total_w}W/{total_l}L  ({total_wr:.1f}% WR)",
        "",
        "<b>By whale:</b>",
    ]
    for alias in BASE_ALLOC:
        s = whale_stats.get(alias, {"w": 0, "l": 0, "pnl": 0.0})
        n = s["w"] + s["l"]
        wr = (s["w"] / n * 100) if n else 0.0
        lines.append(
            f"  {alias:<14} {s['w']:>2}W/{s['l']:>2}L  {wr:>5.1f}%  ${s['pnl']:+.2f}"
        )
    lines.append("")
    lines.append(f"<b>Recent (last 6h):</b> {r_w}W/{r_l}L  ${r_pnl:+.2f}")

    if bankroll < MIN_POSITION_USD:
        lines.append("")
        lines.append("⚠️ <b>BANKROLL BELOW MIN POSITION SIZE — paused new entries until wins refill</b>")

    await send_telegram("\n".join(lines))
    logger.info("6h update sent: bankroll=$%.2f, total=%dW/%dL, pnl=$%.2f",
                bankroll, total_w, total_l, total_pnl)


# ── Main loop ──────────────────────────────────────────────────────────
async def run() -> None:
    init_db()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    state = load_state(conn)
    logger.info(
        "Paper trader started. bankroll=$%.2f, started_at=%s, DRY_RUN=%s",
        state["bankroll_usd"], state["started_at"], DRY_RUN,
    )

    while True:
        tick_start = datetime.now(timezone.utc)
        try:
            state = load_state(conn)

            # ── 1. RESOLUTIONS ─────────────────────────────────────────
            open_rows = conn.execute(
                """SELECT id, whale_alias, condition_id, direction,
                          market_title, entry_price, paper_size_usd,
                          source_table
                   FROM paper_positions WHERE outcome='OPEN'"""
            ).fetchall()
            for row in open_rows:
                pp = dict(row)
                src = query_source_status(
                    conn, pp["source_table"], pp["whale_alias"], pp["condition_id"],
                )
                if src and src["status"] == "closed" and src["outcome"] in ("WIN", "LOSS", "RESOLVED"):
                    await close_paper_position(conn, pp, src, state)

            # ── 2. NEW SIGNALS ─────────────────────────────────────────
            candidates = query_candidates(conn, state["started_at"])
            for sig in candidates:
                # Shadow whales: log the would-be entry, skip the open.
                if sig["alias"] in SHADOW_WHALES:
                    log_shadow_candidate(sig)
                    continue

                base_frac = BASE_ALLOC.get(sig["alias"])
                if base_frac is None:
                    continue

                # Pre-sizing filter: skip if too close to game start /
                # already in-game. Uses cached Gamma lookup (6h TTL) so
                # repeat-checks on same cid are free.
                skip_time, time_reason = await too_close_to_game_start(sig["cid"])
                if skip_time:
                    logger.info(
                        "SKIP open [%s] %s: %s",
                        sig["alias"], sig["title"][:40], time_reason,
                    )
                    continue

                base_size = STARTING_BANKROLL * base_frac
                mult, mult_desc = compute_conviction_mult(
                    conn, sig["alias"], sig["cid"], sig["direction"],
                )
                size = min(base_size * mult, HARD_SIZE_CAP_USD)
                ok, reason = can_open(conn, size, state, alias=sig["alias"])
                if not ok:
                    logger.info(
                        "SKIP open [%s] %s: %s (wanted $%.2f, mult %s)",
                        sig["alias"], sig["title"][:40], reason, size, mult_desc,
                    )
                    continue
                await open_paper_position(conn, sig, size, mult, state)

            # ── 3. 6-HOUR UPDATE ───────────────────────────────────────
            now_iso = datetime.now(timezone.utc).isoformat()
            if now_iso >= state["next_update_ts"]:
                await send_6h_update(conn, state)
                next_ts = (
                    datetime.now(timezone.utc)
                    + timedelta(hours=UPDATE_INTERVAL_HOURS)
                ).isoformat()
                set_next_update(conn, next_ts)
                conn.commit()
                state["next_update_ts"] = next_ts

        except Exception as e:
            logger.exception("tick error: %s", e)

        elapsed = (datetime.now(timezone.utc) - tick_start).total_seconds()
        await asyncio.sleep(max(0, POLL_INTERVAL_SEC - elapsed))


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
