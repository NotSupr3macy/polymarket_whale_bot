"""Circuit breakers + reconciliation for live_trader.

All functions return (ok: bool, reason: str). emergency_stop() raises
SystemExit to halt the daemon outright — only used for unrecoverable
states (corrupt DB, reconciliation drift beyond tolerance).

The daemon calls these between every tick. Any fail halts new entries
but still allows resolution/redemption of existing positions.
"""
from __future__ import annotations

import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("trade_safety")

# ── Config (from env) ──────────────────────────────────────────────────
def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


MAX_DAILY_LOSS_USD = _env_float("LIVE_MAX_DAILY_LOSS_USD", 5.0)
RECONCILE_TOLERANCE_USD = _env_float("LIVE_RECONCILE_TOLERANCE_USD", 1.0)

# Slippage we'll accept relative to whale's entry. For tiny $1 orders
# in markets where the whale just cleared the book, fills will often be
# 5–20% above his price. Default loosened from 0.02 to 0.20 so the bot
# can actually trade. EV is still positive at sportmaster's 60–70% WR
# on dogs even with 15–20% slippage.
MAX_SLIPPAGE_FRAC = _env_float("LIVE_MAX_SLIPPAGE_FRAC", 0.20)

# Liquidity cushion: how many times our order size we want sitting in
# the book within MAX_SLIPPAGE_FRAC of target. For $1 orders this can
# safely be 1x — we just need ≥$1 of book depth to cover our purchase.
MIN_LIQUIDITY_CUSHION = _env_float("LIVE_MIN_LIQUIDITY_CUSHION", 1.0)


# ── Daily loss circuit breaker ─────────────────────────────────────────
def check_daily_loss_limit(conn: sqlite3.Connection) -> tuple[bool, str]:
    """Halt new entries if today's realized PnL has dropped below
    -MAX_DAILY_LOSS_USD. Already-open positions can still resolve.

    "Today" = UTC midnight to now.
    """
    today_start = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    ).isoformat()
    row = conn.execute(
        """SELECT COALESCE(SUM(pnl_usd), 0) AS realized
           FROM live_positions
           WHERE resolved_at > ?""",
        (today_start,),
    ).fetchone()
    realized = float(row[0] if row else 0)
    if realized <= -MAX_DAILY_LOSS_USD:
        return False, (
            f"daily loss ${realized:+.2f} <= -${MAX_DAILY_LOSS_USD:.2f} cap"
        )
    return True, ""


# ── Bankroll reconciliation ────────────────────────────────────────────
async def check_bankroll_reconciliation(
    conn: sqlite3.Connection,
) -> tuple[bool, str]:
    """Compare DB's tracked state vs on-chain USDC + open-order value.

    Bankroll_DB + deployed_DB should equal USDC_on_chain + open_orders_USD
    within RECONCILE_TOLERANCE_USD.

    Drift is normal and expected:
      - Gas fees (tiny, ~$0.01/tx)
      - Fill price differing from book snapshot
      - Unresolved partial fills

    This is the last line of defense against a DB/chain desync bug.
    """
    # Lazy import to avoid loading py-clob-client during test collection
    from monitor.clob_client_wrapper import (
        get_usdc_balance, get_polymarket_balance, get_open_orders_value,
    )

    row = conn.execute(
        "SELECT bankroll_usd FROM live_state WHERE id=1"
    ).fetchone()
    if not row:
        return False, "live_state missing"
    db_bankroll = float(row[0])

    # IMPORTANT: only count UNFILLED orders as "deployed" because once
    # filled the USDC has already been deducted from bankroll AND
    # converted to outcome tokens. FILLED positions show up as ERC1155
    # token holdings, not as "deployed USDC."
    #
    # The right comparison:
    #   db_bankroll                        = what we think USDC should be
    #   on_chain_usdc + open_orders_value  = actual USDC available
    # These should match (within fee dust) when DB is in sync.
    deployed_row = conn.execute(
        """SELECT COALESCE(SUM(size_usd), 0) AS dep
           FROM live_positions
           WHERE status IN ('PLACED', 'PARTIAL')"""
    ).fetchone()
    db_pending_orders = float(deployed_row[0] if deployed_row else 0)
    db_expected_usdc = db_bankroll  # pending orders haven't deducted yet

    # On-chain side: USDC available + USDC locked in unfilled orders
    on_chain_usdc = await get_polymarket_balance()
    open_orders_usd = await get_open_orders_value()
    chain_usdc_total = on_chain_usdc + open_orders_usd

    drift = abs(db_expected_usdc - chain_usdc_total)
    logger.info(
        "reconcile: DB bankroll=$%.2f (pending orders=$%.2f) | "
        "chain=$%.2f (usdc=$%.2f + open_orders=$%.2f) | drift=$%.2f",
        db_bankroll, db_pending_orders,
        chain_usdc_total, on_chain_usdc, open_orders_usd, drift,
    )

    # Persist drift for observability
    conn.execute(
        """UPDATE live_state
           SET last_reconcile_at=?, last_reconcile_drift_usd=?
           WHERE id=1""",
        (datetime.now(timezone.utc).isoformat(), drift),
    )
    conn.commit()

    if drift > RECONCILE_TOLERANCE_USD:
        return False, (
            f"reconciliation drift ${drift:.2f} > tolerance "
            f"${RECONCILE_TOLERANCE_USD:.2f}"
        )
    return True, ""


# ── Per-order safety ───────────────────────────────────────────────────
async def check_liquidity(
    token_id: str,
    target_price: float,
    order_size_usd: float,
) -> tuple[bool, str]:
    """Refuse to place the order if the book doesn't have enough
    depth at/near the target price.

    We want at least MIN_LIQUIDITY_CUSHION × order_size in asks
    priced <= target_price × (1 + MAX_SLIPPAGE_FRAC). Prevents buying
    into thin books where we'd blow through multiple price levels.
    """
    from monitor.clob_client_wrapper import get_order_book
    book = await get_order_book(token_id)
    asks = book.get("asks", [])
    if not asks:
        return False, "empty ask book"

    max_acceptable_price = target_price * (1 + MAX_SLIPPAGE_FRAC)
    available_usd = 0.0
    for lvl in asks:
        if lvl["price"] > max_acceptable_price:
            break
        available_usd += lvl["price"] * lvl["size"]

    required = order_size_usd * MIN_LIQUIDITY_CUSHION
    if available_usd < required:
        return False, (
            f"book depth ${available_usd:.2f} < "
            f"{MIN_LIQUIDITY_CUSHION:.1f}x cushion ${required:.2f} "
            f"at target $<={max_acceptable_price:.3f}"
        )
    return True, ""


def check_slippage(
    expected_price: float, actual_price: float,
) -> tuple[bool, str]:
    """Post-fill check: did we fill within acceptable slippage?
    Returns (ok, reason). ok=False is a WARNING, not a halt —
    the order already filled; we log and accept.
    """
    if expected_price <= 0:
        return True, ""
    slip = abs(actual_price - expected_price) / expected_price
    if slip > MAX_SLIPPAGE_FRAC:
        return False, (
            f"slippage {slip*100:.1f}% exceeds cap "
            f"{MAX_SLIPPAGE_FRAC*100:.1f}% "
            f"(expected=${expected_price:.3f} actual=${actual_price:.3f})"
        )
    return True, ""


# ── Hard stop ──────────────────────────────────────────────────────────
class EmergencyStopException(SystemExit):
    """Raised on unrecoverable errors. Halts daemon via SystemExit."""


def emergency_stop(reason: str, db_path: str = None) -> None:
    """Log critical, write sentinel file, and exit process.

    The sentinel file (`logs/EMERGENCY_HALT`) is checked by
    `start_live_trader.sh` — once present, the daemon refuses to
    restart until it's manually deleted by the operator.

    This is the "never silently recover" door. Reserved for:
      - Reconciliation drift beyond tolerance
      - Corrupt DB state
      - Private key / auth failures
      - Any state where blindly continuing risks funds
    """
    logger.critical("EMERGENCY STOP: %s", reason)
    try:
        # Mark in DB if possible
        if db_path:
            conn = sqlite3.connect(db_path)
            conn.execute(
                "UPDATE live_state SET emergency_halt=1 WHERE id=1"
            )
            conn.commit()
            conn.close()
    except Exception:
        pass

    # Sentinel file
    sentinel_dir = Path(
        os.getenv("LIVE_LOG_DIR", "/home/botuser/whale-bot/logs")
    )
    sentinel_dir.mkdir(parents=True, exist_ok=True)
    sentinel = sentinel_dir / "EMERGENCY_HALT"
    try:
        sentinel.write_text(
            f"{datetime.now(timezone.utc).isoformat()}  {reason}\n",
            encoding="utf-8",
        )
    except Exception:
        pass

    # Telegram alert (best-effort, don't block on failure)
    try:
        import asyncio
        from monitor.paper_trader import send_telegram
        asyncio.create_task(
            send_telegram(f"🚨 <b>LIVE TRADER HALTED</b>\n\n{reason}")
        )
    except Exception:
        pass

    sys.exit(1)


def check_emergency_sentinel(log_dir: str = None) -> bool:
    """Returns True if EMERGENCY_HALT sentinel file exists.
    start_live_trader.sh calls this before spawning the daemon."""
    sd = Path(log_dir or os.getenv(
        "LIVE_LOG_DIR", "/home/botuser/whale-bot/logs"
    ))
    return (sd / "EMERGENCY_HALT").exists()
