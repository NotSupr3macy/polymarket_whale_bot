"""Live Polymarket trader — real-money mirror of paper_trader for the
sportmaster-only 3-day trial.

Mirrors paper_trader's signal loop but places signed CLOB orders
instead of INSERTing into paper_positions. Everything else (filter
stack, Gamma-authoritative resolution) is reused via import.

Safety gates, in order of strictness (any failure halts new entries):
  1. LIVE_TRADER_ENABLED env var must equal "1"
  2. EMERGENCY_HALT sentinel must NOT exist
  3. Daily realized loss < LIVE_MAX_DAILY_LOSS_USD
  4. Bankroll reconciles on-chain within tolerance (every 10 min)
  5. Each order must pass liquidity check
  6. Each fill is checked for slippage (logged, not halt)

Trial config (read from .env):
  LIVE_TRADER_ENABLED=0            # flip to 1 to enable
  LIVE_MAX_DAILY_LOSS_USD=5        # circuit breaker at 50% of $10
  LIVE_BANKROLL_USD=10             # starting bankroll (informational)
  LIVE_PER_TRADE_USD=1             # fixed $1 per trade
  LIVE_WHALE=sportmaster777        # only this whale fires
"""
from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Load .env before importing anything that reads env vars
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Reuse paper_trader's filter logic — same strategy, different execution
from monitor.paper_trader import (
    WHALE_FILTERS, WHALE_MAX_ENTRY, MIN_ENTRY_PRICE,
    WHALE_TABLE, classify_subtype, resolve_ambiguous_via_gamma,
    send_telegram, _PST,
)
from monitor import trade_safety
from monitor import clob_client_wrapper as clob

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-14s | %(message)s",
)
logger = logging.getLogger("live_trader")

# ── Config ─────────────────────────────────────────────────────────────
DB_PATH = os.getenv(
    "DB_PATH",
    str(Path(__file__).resolve().parent.parent / "trades.db"),
)

POLL_INTERVAL_SEC = int(os.getenv("LIVE_POLL_INTERVAL_SEC", "30"))
RECONCILE_EVERY_TICKS = int(os.getenv("LIVE_RECONCILE_EVERY_TICKS", "20"))  # every ~10 min
TARGET_WHALE = os.getenv("LIVE_WHALE", "sportmaster777")
PER_TRADE_USD = float(os.getenv("LIVE_PER_TRADE_USD", "1.0"))
STARTING_BANKROLL = float(os.getenv("LIVE_BANKROLL_USD", "10.0"))


# ── DB schema ──────────────────────────────────────────────────────────
_SCHEMA = """
CREATE TABLE IF NOT EXISTS live_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    whale_alias TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    direction TEXT NOT NULL,
    market_title TEXT NOT NULL,
    token_id TEXT,

    -- Sizing
    entry_price REAL NOT NULL,            -- whale's entry (intended)
    size_usd REAL NOT NULL,               -- intended USD spend
    size_shares REAL NOT NULL,            -- intended share count
    actual_price REAL,                    -- filled price
    actual_size REAL,                     -- filled shares
    actual_cost_usd REAL,                 -- actual USDC spent

    -- Order lifecycle
    order_id TEXT,
    tx_hash TEXT,
    status TEXT NOT NULL,                 -- PLACING, PLACED, PARTIAL, FILLED,
                                          -- FAILED, CANCELLED, OPEN, CLOSED
    fail_reason TEXT,
    clob_resp_json TEXT,

    -- Timestamps
    opened_at TEXT NOT NULL,
    filled_at TEXT,
    resolved_at TEXT,

    -- Resolution
    outcome TEXT,                         -- WIN, LOSS, RESOLVED, OPEN
    resolution_price REAL,
    pnl_usd REAL,
    redeem_tx_hash TEXT,

    UNIQUE(whale_alias, condition_id, opened_at)
);

CREATE INDEX IF NOT EXISTS idx_live_positions_status
    ON live_positions(status);

CREATE INDEX IF NOT EXISTS idx_live_positions_outcome
    ON live_positions(outcome);

CREATE TABLE IF NOT EXISTS live_state (
    id INTEGER PRIMARY KEY CHECK (id=1),
    bankroll_usd REAL NOT NULL,
    started_at TEXT NOT NULL,
    last_reconcile_at TEXT,
    last_reconcile_drift_usd REAL,
    emergency_halt INTEGER DEFAULT 0,
    tick_count INTEGER DEFAULT 0
);
"""


def init_db(db_path: str = DB_PATH) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(_SCHEMA)
        cur = conn.execute("SELECT COUNT(*) FROM live_state")
        if cur.fetchone()[0] == 0:
            now_iso = datetime.now(timezone.utc).isoformat()
            conn.execute(
                """INSERT INTO live_state (id, bankroll_usd, started_at)
                   VALUES (1, ?, ?)""",
                (STARTING_BANKROLL, now_iso),
            )
            logger.info(
                "live_state seeded: bankroll=$%.2f started_at=%s",
                STARTING_BANKROLL, now_iso,
            )
        conn.commit()
    finally:
        conn.close()


# ── State helpers ──────────────────────────────────────────────────────
def load_state(conn: sqlite3.Connection) -> dict:
    row = conn.execute(
        """SELECT bankroll_usd, started_at, last_reconcile_at,
                  last_reconcile_drift_usd, emergency_halt, tick_count
           FROM live_state WHERE id=1"""
    ).fetchone()
    if not row:
        raise RuntimeError("live_state missing — init_db() not run?")
    return {
        "bankroll_usd": float(row[0]),
        "started_at": row[1],
        "last_reconcile_at": row[2],
        "last_reconcile_drift_usd": row[3],
        "emergency_halt": int(row[4] or 0),
        "tick_count": int(row[5] or 0),
    }


def set_bankroll(conn: sqlite3.Connection, value: float) -> None:
    conn.execute(
        "UPDATE live_state SET bankroll_usd=? WHERE id=1", (value,)
    )


def increment_tick(conn: sqlite3.Connection) -> None:
    conn.execute(
        "UPDATE live_state SET tick_count=tick_count+1 WHERE id=1"
    )


# ── Candidate query (sportmaster-only subset) ──────────────────────────
def query_sportmaster_candidates(
    conn: sqlite3.Connection, started_at: str,
) -> list[dict]:
    """Filtered subset of paper_trader.query_candidates — only TARGET_WHALE."""
    rows = conn.execute(
        f"""
        SELECT alias, condition_id, direction, market_title,
               first_seen_price, first_seen_size_usd, first_seen_at,
               muted_reason
        FROM tracked_whale_positions
        WHERE alias = ?
          AND status = 'open'
          AND first_seen_at > ?
          AND muted_reason IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM live_positions lp
              WHERE lp.whale_alias = tracked_whale_positions.alias
                AND lp.condition_id = tracked_whale_positions.condition_id
                AND lp.outcome IN ('OPEN')
          )
        ORDER BY first_seen_at ASC
        """,
        (TARGET_WHALE, started_at),
    ).fetchall()
    out = []
    for r in rows:
        out.append({
            "alias": r[0], "cid": r[1], "direction": r[2],
            "title": r[3], "entry_price": r[4] or 0.5,
            "whale_size_usd": r[5] or 0,
            "first_seen_at": r[6], "muted_reason": r[7],
        })
    return out


# ── Open a live position ───────────────────────────────────────────────
async def open_live_position(
    conn: sqlite3.Connection, sig: dict, size_usd: float, state: dict,
) -> None:
    """Place a real CLOB order. Records every step of the lifecycle
    so we have an audit trail even if the daemon crashes mid-flow."""
    now_iso = datetime.now(timezone.utc).isoformat()

    # Resolve token_id for the direction
    tokens = await clob.get_market_tokens(sig["cid"])
    if not tokens or sig["direction"] not in tokens:
        logger.warning(
            "SKIP [%s] %s: could not resolve token_id for direction '%s' "
            "(available: %s)",
            sig["alias"], sig["title"][:40],
            sig["direction"], list(tokens.keys()),
        )
        return
    token_id = tokens[sig["direction"]]

    entry_price = float(sig["entry_price"])
    size_shares = size_usd / entry_price if entry_price > 0 else 0
    if size_shares < 1.0:
        # Polymarket min order size is typically ~1 share
        logger.info(
            "SKIP [%s] %s: size %.2f shares < 1 (entry=$%.3f, usd=$%.2f)",
            sig["alias"], sig["title"][:40], size_shares, entry_price, size_usd,
        )
        return

    # Liquidity check BEFORE placing
    ok, reason = await trade_safety.check_liquidity(
        token_id, entry_price, size_usd,
    )
    if not ok:
        logger.info("SKIP [%s] liquidity: %s", sig["alias"], reason)
        return

    # Insert DB row BEFORE placing order (audit trail)
    cursor = conn.execute(
        """INSERT INTO live_positions
           (whale_alias, condition_id, direction, market_title, token_id,
            entry_price, size_usd, size_shares,
            status, outcome, opened_at)
           VALUES (?,?,?,?,?,?,?,?, 'PLACING', 'OPEN', ?)""",
        (
            sig["alias"], sig["cid"], sig["direction"], sig["title"],
            token_id, entry_price, size_usd, size_shares, now_iso,
        ),
    )
    pos_id = cursor.lastrowid
    conn.commit()

    try:
        resp = await clob.place_limit_order(
            token_id=token_id,
            price=entry_price,
            size_shares=size_shares,
            side="BUY",
            order_type="GTC",
        )
    except Exception as e:
        conn.execute(
            """UPDATE live_positions
               SET status='FAILED', fail_reason=?
               WHERE id=?""",
            (str(e)[:500], pos_id),
        )
        conn.commit()
        logger.exception("order placement failed for %s: %s", sig["alias"], e)
        return

    import json
    order_id = resp.get("orderID") or resp.get("order_id") or ""
    success = bool(resp.get("success"))

    conn.execute(
        """UPDATE live_positions
           SET order_id=?,
               status=?,
               clob_resp_json=?,
               fail_reason=?
           WHERE id=?""",
        (
            order_id,
            "PLACED" if success else "FAILED",
            json.dumps(resp, default=str)[:5000],
            resp.get("errorMsg", "") if not success else None,
            pos_id,
        ),
    )
    conn.commit()

    if not success:
        logger.warning(
            "order REJECTED by CLOB: %s: %s",
            sig["alias"], resp.get("errorMsg"),
        )
        return

    logger.info(
        "OPEN [%s] %s %s @ $%.3f size=$%.2f (%.2f sh) order_id=%s",
        sig["alias"], sig["direction"], sig["title"][:40],
        entry_price, size_usd, size_shares, order_id[:16],
    )

    # Best-effort: see if it filled immediately
    await asyncio.sleep(3)  # let CLOB propagate
    await _check_and_record_fill(conn, pos_id)


async def _check_and_record_fill(
    conn: sqlite3.Connection, pos_id: int,
) -> None:
    """Poll CLOB for an order's fill state and update DB.
    Updates actual_price / actual_size / actual_cost_usd once filled.
    Safe to call repeatedly."""
    row = conn.execute(
        """SELECT order_id, entry_price, size_shares, status
           FROM live_positions WHERE id=?""",
        (pos_id,),
    ).fetchone()
    if not row or not row[0]:
        return
    order_id, expected_price, expected_size, status = row
    if status in ("FILLED", "FAILED", "CANCELLED"):
        return

    try:
        order = await clob.get_order_status(order_id)
    except Exception as e:
        logger.debug("get_order_status failed for %s: %s", order_id[:16], e)
        return

    # order dict shape varies by SDK; handle both common variants
    matched = float(order.get("size_matched") or order.get("filled") or 0)
    total = float(order.get("original_size") or order.get("size") or expected_size)

    if matched >= total * 0.99:
        actual_price = float(
            order.get("price") or order.get("avg_price") or expected_price
        )
        actual_cost = actual_price * matched

        conn.execute(
            """UPDATE live_positions
               SET status='FILLED', actual_price=?, actual_size=?,
                   actual_cost_usd=?, filled_at=?
               WHERE id=?""",
            (
                actual_price, matched, actual_cost,
                datetime.now(timezone.utc).isoformat(), pos_id,
            ),
        )
        # Deduct actual spend from bankroll
        state_row = conn.execute(
            "SELECT bankroll_usd FROM live_state WHERE id=1"
        ).fetchone()
        cur_bankroll = float(state_row[0]) if state_row else 0
        set_bankroll(conn, cur_bankroll - actual_cost)
        conn.commit()

        slip_ok, slip_reason = trade_safety.check_slippage(
            float(expected_price), actual_price,
        )
        logger.info(
            "FILLED id=%d price=$%.3f size=%.2f cost=$%.2f bankroll=$%.2f%s",
            pos_id, actual_price, matched, actual_cost,
            cur_bankroll - actual_cost,
            "" if slip_ok else f"  [slippage WARN: {slip_reason}]",
        )
    elif matched > 0:
        logger.info(
            "PARTIAL id=%d matched %.2f/%.2f", pos_id, matched, total,
        )
        conn.execute(
            "UPDATE live_positions SET status='PARTIAL' WHERE id=?",
            (pos_id,),
        )
        conn.commit()


# ── Close a live position (resolution) ─────────────────────────────────
async def try_resolve_position(
    conn: sqlite3.Connection, pos: dict,
) -> None:
    """Check if the market has resolved. If so, mark outcome and compute PnL.

    NOTE: redemption (converting winning shares to USDC on-chain) is
    NOT yet implemented in this trial version. For now we record the
    resolution in DB; manual redemption happens via Polymarket UI.
    Future: call redeem_winning_position() once implemented.
    """
    gamma_result = await resolve_ambiguous_via_gamma(
        pos["condition_id"], pos["direction"],
    )
    if gamma_result is None or gamma_result[0] not in ("WIN", "LOSS"):
        return  # market still LIVE or Gamma unreachable

    outcome = gamma_result[0]
    actual_cost = float(pos.get("actual_cost_usd") or 0)
    actual_size = float(pos.get("actual_size") or 0)

    if outcome == "WIN":
        pnl = actual_size * 1.0 - actual_cost  # shares * $1 payout - cost
        resolution_price = 1.0
    else:  # LOSS
        pnl = -actual_cost
        resolution_price = 0.0

    now_iso = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """UPDATE live_positions
           SET outcome=?, status='CLOSED',
               resolution_price=?, pnl_usd=?, resolved_at=?
           WHERE id=?""",
        (outcome, resolution_price, pnl, now_iso, pos["id"]),
    )
    conn.commit()

    # Telegram alert
    emoji = "✅" if outcome == "WIN" else "❌"
    msg = (
        f"💰{emoji} <b>LIVE {outcome} — {pos['whale_alias']}</b>\n\n"
        f"<b>Market:</b> {pos['market_title']}\n"
        f"<b>Side:</b> {pos['direction']} | "
        f"<b>Entry:</b> ${float(pos['entry_price']):.3f}\n"
        f"<b>Realized P&amp;L:</b> ${pnl:+.2f}\n\n"
        f"<i>Note: redemption not auto. Check Polymarket UI for USDC.</i>"
    )
    await send_telegram(msg)

    logger.info(
        "RESOLVED id=%d outcome=%s pnl=$%+.2f",
        pos["id"], outcome, pnl,
    )


# ── Main loop ──────────────────────────────────────────────────────────
async def run() -> None:
    # Boot safety gates
    if os.getenv("LIVE_TRADER_ENABLED", "0") != "1":
        logger.error(
            "LIVE_TRADER_ENABLED != 1 — refusing to start. "
            "Flip env var and restart when ready."
        )
        return

    if trade_safety.check_emergency_sentinel():
        logger.error(
            "EMERGENCY_HALT sentinel present — refusing to start. "
            "Delete logs/EMERGENCY_HALT after reviewing the halt reason."
        )
        return

    init_db()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    state = load_state(conn)

    if state["emergency_halt"]:
        logger.error("live_state.emergency_halt=1 — refusing to start.")
        return

    logger.info(
        "Live trader started. whale=%s per_trade=$%.2f bankroll=$%.2f "
        "max_daily_loss=$%.2f",
        TARGET_WHALE, PER_TRADE_USD, state["bankroll_usd"],
        trade_safety.MAX_DAILY_LOSS_USD,
    )

    while True:
        tick_start = datetime.now(timezone.utc)
        try:
            state = load_state(conn)
            increment_tick(conn)
            conn.commit()

            # ── Safety gates ─────────────────────────────────────────
            loss_ok, loss_reason = trade_safety.check_daily_loss_limit(conn)
            if not loss_ok:
                logger.warning("DAILY LOSS LIMIT: %s — resolve-only mode",
                               loss_reason)
                await _resolution_sweep(conn)
                await asyncio.sleep(POLL_INTERVAL_SEC)
                continue

            # Periodic reconciliation
            if state["tick_count"] % RECONCILE_EVERY_TICKS == 0:
                recon_ok, recon_reason = await trade_safety.check_bankroll_reconciliation(conn)
                if not recon_ok:
                    trade_safety.emergency_stop(
                        f"Bankroll reconciliation failed: {recon_reason}",
                        db_path=DB_PATH,
                    )
                    return  # emergency_stop sys.exits

            # ── Poll + fill-check open orders ────────────────────────
            for row in conn.execute(
                """SELECT id FROM live_positions
                   WHERE status IN ('PLACED','PARTIAL')"""
            ).fetchall():
                await _check_and_record_fill(conn, row["id"])

            # ── Resolution sweep ─────────────────────────────────────
            await _resolution_sweep(conn)

            # ── New signals ──────────────────────────────────────────
            candidates = query_sportmaster_candidates(
                conn, state["started_at"],
            )
            for sig in candidates:
                # Apply same filter stack as paper
                wf = WHALE_FILTERS.get(sig["alias"])
                if wf and not wf(sig):
                    logger.info(
                        "SKIP [%s] whale-filter: %s @ $%.3f",
                        sig["alias"], sig["title"][:40], sig["entry_price"],
                    )
                    continue
                max_entry = WHALE_MAX_ENTRY.get(sig["alias"])
                if max_entry is not None and sig["entry_price"] > max_entry:
                    logger.info(
                        "SKIP [%s] entry $%.3f > max_entry $%.3f: %s",
                        sig["alias"], sig["entry_price"], max_entry,
                        sig["title"][:40],
                    )
                    continue
                if sig["entry_price"] < MIN_ENTRY_PRICE:
                    logger.info(
                        "SKIP [%s] entry $%.3f < min_entry $%.3f: %s",
                        sig["alias"], sig["entry_price"], MIN_ENTRY_PRICE,
                        sig["title"][:40],
                    )
                    continue

                # Bankroll check
                if state["bankroll_usd"] < PER_TRADE_USD:
                    logger.info(
                        "SKIP bankroll $%.2f < per-trade $%.2f",
                        state["bankroll_usd"], PER_TRADE_USD,
                    )
                    break

                await open_live_position(conn, sig, PER_TRADE_USD, state)

        except Exception as e:
            logger.exception("tick error: %s", e)

        elapsed = (datetime.now(timezone.utc) - tick_start).total_seconds()
        await asyncio.sleep(max(0, POLL_INTERVAL_SEC - elapsed))


async def _resolution_sweep(conn: sqlite3.Connection) -> None:
    """Check all filled-but-not-closed positions for resolution."""
    rows = conn.execute(
        """SELECT id, whale_alias, condition_id, direction, market_title,
                  entry_price, actual_size, actual_cost_usd
           FROM live_positions
           WHERE status='FILLED' AND outcome='OPEN'"""
    ).fetchall()
    for row in rows:
        pos = dict(row)
        await try_resolve_position(conn, pos)


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
