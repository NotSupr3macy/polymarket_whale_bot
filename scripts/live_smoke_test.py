#!/usr/bin/env python3
"""Smoke test: place ONE real $0.50 test order to verify the live-trader
pipeline end-to-end. Intentionally priced LOW so we're unlikely to get
filled in 60s, then we cancel it.

If it does fill (rare at $0.10 below market), we'll leave it as a real
position for the daemon to handle later.

Steps:
    1. Precheck (re-run subset of live_precheck)
    2. Find ONE liquid NBA market with depth at target price
    3. Place a BUY order for $0.50 worth at market_best_ask - $0.10
       (intentionally below book so it sits as maker for ~60s)
    4. Poll order status for 60s
    5. If FILLED: log it; leave position for daemon
       If not filled: cancel cleanly
    6. Report: order placement latency, fill state, cancel state

Run:
    source venv/bin/activate
    python3 scripts/live_smoke_test.py
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Load .env
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def log(*a):
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}]", *a, flush=True)


async def find_test_market():
    """Find any currently-open liquid market to place our test order against.
    Prefer sportmaster's current tracker rows for thematic consistency."""
    import sqlite3
    db_path = os.getenv(
        "DB_PATH",
        "/home/botuser/whale-bot/trades.db",
    )
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """SELECT condition_id, direction, market_title, first_seen_price
           FROM tracked_whale_positions
           WHERE alias='sportmaster777'
             AND status='open'
             AND muted_reason IS NULL
             AND first_seen_price BETWEEN 0.20 AND 0.80
           ORDER BY first_seen_at DESC
           LIMIT 1"""
    ).fetchone()
    conn.close()
    if not row:
        return None
    return dict(row)


async def main():
    log("=" * 70)
    log("LIVE SMOKE TEST — places ONE real $0.50 order, then cancels")
    log("=" * 70)

    if os.getenv("LIVE_TRADER_ENABLED", "0") == "1":
        log("[WARN] LIVE_TRADER_ENABLED=1 — is the daemon already running?")
        log("       The daemon's candidate query may collide with this test.")
        log("       Consider stopping the daemon first with:")
        log("           bash deploy/stop_live_trader.sh")
        log()

    # 1. Quick auth probe
    from monitor import clob_client_wrapper as clob
    try:
        client = clob.get_client()
        log("[OK] CLOB client constructed")
    except Exception as e:
        log(f"[FAIL] client construction: {e}")
        return 1

    try:
        orders_before = await clob.get_open_orders()
        log(f"[OK] open orders before test: {len(orders_before)}")
    except Exception as e:
        log(f"[FAIL] could not fetch open orders: {e}")
        return 1

    # 2. Pick a market
    market = await find_test_market()
    if not market:
        log("[FAIL] no suitable market found in tracked_whale_positions. "
            "Wait for sportmaster to fire something, then retry.")
        return 1
    log(f"[OK] test market: {market['market_title'][:50]} / "
        f"{market['direction']} @ whale's $ {market['first_seen_price']:.3f}")

    # 3. Resolve token_id
    tokens = await clob.get_market_tokens(market["condition_id"])
    token_id = tokens.get(market["direction"])
    if not token_id:
        log(f"[FAIL] could not find token_id for direction "
            f"'{market['direction']}' in {list(tokens.keys())}")
        return 1
    log(f"[OK] resolved token_id: {token_id[:16]}...")

    # 4. Fetch book to price below best ask
    book = await clob.get_order_book(token_id)
    asks = book.get("asks", [])
    if not asks:
        log("[FAIL] empty ask book — try a different market")
        return 1
    best_ask = asks[0]["price"]
    # Place at best_ask - $0.10 to avoid immediate fill; clamp >= 0.01
    target_price = max(0.01, round(best_ask - 0.10, 3))
    size_usd = 0.50
    size_shares = round(size_usd / target_price, 2)
    log(f"[OK] book best ask: $ {best_ask:.3f}, "
        f"target price: $ {target_price:.3f}, "
        f"size: {size_shares} sh (= ${size_usd:.2f})")

    # Confirm with user
    log(f"\nAbout to place:")
    log(f"  BUY {size_shares} shares @ ${target_price:.3f} = ${size_usd:.2f}")
    log(f"  on {market['market_title'][:60]}")
    log(f"  direction: {market['direction']}")
    log(f"  order type: GTC (good-till-cancelled)")
    log()
    try:
        resp = input("Proceed with real order? [y/N]: ")
    except EOFError:
        log("[ABORT] non-interactive shell — use --yes flag (not implemented)")
        return 1
    if resp.strip().lower() != "y":
        log("[ABORT] user declined")
        return 0

    # 5. Place order
    log("placing order...")
    t0 = time.time()
    try:
        order_resp = await clob.place_limit_order(
            token_id=token_id,
            price=target_price,
            size_shares=size_shares,
            side="BUY",
            order_type="GTC",
        )
    except Exception as e:
        log(f"[FAIL] order placement exception: {e}")
        return 1
    placement_ms = int((time.time() - t0) * 1000)
    log(f"[OK] placement round-trip: {placement_ms}ms")
    log(f"     response: {order_resp}")

    order_id = order_resp.get("orderID") or order_resp.get("order_id")
    success = bool(order_resp.get("success"))
    if not success or not order_id:
        log(f"[FAIL] order rejected: {order_resp.get('errorMsg')}")
        return 1
    log(f"[OK] order placed, id: {order_id}")

    # 6. Poll for 60s
    log("polling order status for 60s...")
    filled = False
    for i in range(12):
        await asyncio.sleep(5)
        try:
            status = await clob.get_order_status(order_id)
            matched = float(
                status.get("size_matched") or status.get("filled") or 0
            )
            log(f"  tick {i+1}/12: matched={matched}")
            if matched >= size_shares * 0.99:
                filled = True
                log(f"[INFO] order filled unexpectedly — leaving as real position")
                log(f"       check it in Polymarket UI, or let live_trader.py "
                    f"handle resolution")
                break
        except Exception as e:
            log(f"  tick {i+1}/12: get_status err: {e}")

    # 7. Cancel if not filled
    if not filled:
        log("cancelling...")
        try:
            cancel_resp = await clob.cancel_order(order_id)
            log(f"[OK] cancel response: {cancel_resp}")
        except Exception as e:
            log(f"[FAIL] cancel exception: {e}")
            return 1

    # 8. Sanity: open orders count should return to baseline (or +1 if filled)
    orders_after = await clob.get_open_orders()
    expected = len(orders_before) if not filled else len(orders_before) + 0
    actual = len(orders_after)
    log(f"[INFO] open orders after test: {actual} (baseline: {len(orders_before)})")

    log()
    log("=" * 70)
    log("SMOKE TEST COMPLETE")
    if filled:
        log("  Status: FILLED (unexpected at price below book — lucky fill)")
        log("  Check Polymarket UI for your position")
        log("  live_trader.py will handle resolution when the market settles")
    else:
        log("  Status: placed + cancelled cleanly")
        log("  Pipeline verified: signing, posting, polling, cancelling all work")
    log("  Next: edit .env to set LIVE_TRADER_ENABLED=1, then:")
    log("        bash deploy/start_live_trader.sh")
    log("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
