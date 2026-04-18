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
BANKROLL_DEPLOYMENT_CAP_FRAC = 0.50  # max 50% of bankroll deployed
HARD_SIZE_CAP_USD = 15.0  # from 3-way consensus
MIN_POSITION_USD = 3.0  # below this, we won't open

# Per-whale base allocation (fraction of bankroll) — confirmed by user
#
# Apr 18 update: added GamblingIsAllYouNeed at $4 base. Earlier excluded
# per counterfactual which showed his MLB-only filter over-restricted him
# (-88% ROI on 9 bets, tiny sample). Live tracking shows he's highly
# active on MLB (16 passing-filter positions in 6h on Apr 18) with
# 62.5% WR on 339 resolved shadow trades — break-even ROI but positive
# signal worth copying.
BASE_ALLOC = {
    "TheOnlyHuman": 0.08,          # $8 on $100
    "texaskid": 0.06,              # $6
    "kch123": 0.05,                # $5
    "nbasniper": 0.04,             # $4 — shadow-to-live, no filter yet
    "GamblingIsAllYouNeed": 0.04,  # $4 — MLB-only, high activity
    "bigsix": 0.03,                # $3
}

# Which table each whale writes to (texaskid = legacy separate table)
WHALE_TABLE = {
    "TheOnlyHuman": "tracked_whale_positions",
    "kch123": "tracked_whale_positions",
    "bigsix": "tracked_whale_positions",
    "nbasniper": "tracked_whale_positions",
    "GamblingIsAllYouNeed": "tracked_whale_positions",
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
) -> tuple[bool, str]:
    """Return (ok, reason_if_not)."""
    if size < MIN_POSITION_USD:
        return False, f"size ${size:.2f} below min ${MIN_POSITION_USD}"
    if state["bankroll_usd"] < size:
        return False, f"bankroll ${state['bankroll_usd']:.2f} < size ${size:.2f}"
    deployed = currently_deployed(conn)
    # Cap deployed to 50% of current equity (bankroll + already-deployed)
    equity = state["bankroll_usd"] + deployed
    cap = equity * BANKROLL_DEPLOYMENT_CAP_FRAC
    if deployed + size > cap:
        return False, (
            f"deploy cap ${cap:.2f} would be exceeded "
            f"(current ${deployed:.2f} + ${size:.2f})"
        )
    return True, ""


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
        """
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
    n_open = conn.execute(
        "SELECT COUNT(*) FROM paper_positions WHERE outcome='OPEN'"
    ).fetchone()[0]
    msg = (
        f"🧪 <b>NEW PAPER TRADE — {sig['alias']}</b>\n\n"
        f"<b>Market:</b> {sig['title']}\n"
        f"<b>Side:</b> {sig['direction']}\n"
        f"<b>Entry:</b> ${sig['entry_price']:.3f}\n"
        f"<b>Whale stake:</b> ${sig['whale_size_usd']:,.0f}\n"
        f"<b>Paper size:</b> ${size:.2f}  ({base_pct}% × {mult_str} conviction)\n"
        f"\n"
        f"<b>Bankroll:</b> ${new_bankroll:.2f} / ${STARTING_BANKROLL:.2f}\n"
        f"<b>Open positions:</b> {n_open}"
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
    outcome = src["outcome"]
    entry = pp["entry_price"]
    size = pp["paper_size_usd"]
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

    msg = (
        f"🧪{emoji} <b>PAPER {outcome} — {pp['whale_alias']}</b>\n\n"
        f"<b>Market:</b> {pp['market_title']}\n"
        f"<b>Side:</b> {pp['direction']} | <b>Entry:</b> ${entry:.3f} → ${resolution_price:.3f}\n"
        f"<b>Paper P&amp;L:</b> ${pnl:+.2f}\n"
        f"<b>Bankroll:</b> ${new_bankroll:.2f} / ${STARTING_BANKROLL:.2f}"
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
        f"<b>Bankroll:</b> ${bankroll:.2f} / ${STARTING_BANKROLL:.2f}  ({pct_delta:+.1f}%)",
        f"<b>Total:</b> {total_w}W/{total_l}L  ({total_wr:.1f}% WR)",
        f"<b>Open:</b> {n_open} positions (${deployed:.2f} deployed)",
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
                base_frac = BASE_ALLOC.get(sig["alias"])
                if base_frac is None:
                    continue
                base_size = STARTING_BANKROLL * base_frac
                mult, mult_desc = compute_conviction_mult(
                    conn, sig["alias"], sig["cid"], sig["direction"],
                )
                size = min(base_size * mult, HARD_SIZE_CAP_USD)
                ok, reason = can_open(conn, size, state)
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
