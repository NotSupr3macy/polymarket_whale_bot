#!/usr/bin/env python3
"""Force a single $1 BUY on a sportmaster open position at current
best-ask + buffer. Fills immediately. Bypasses all filters and safety
gates — for manual verification only.

Run:
    source venv/bin/activate
    python3 scripts/force_buy.py [keyword]

Examples:
    python3 scripts/force_buy.py Nationals
    python3 scripts/force_buy.py Athletics
    python3 scripts/force_buy.py    # picks most recent sportmaster open
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

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


async def main():
    keyword = sys.argv[1] if len(sys.argv) > 1 else None

    conn = sqlite3.connect("/home/botuser/whale-bot/trades.db")
    if keyword:
        rows = conn.execute(
            """SELECT condition_id, direction, market_title, first_seen_price
               FROM tracked_whale_positions
               WHERE alias='sportmaster777' AND status='open'
                 AND market_title LIKE ?
               ORDER BY first_seen_at DESC LIMIT 1""",
            (f"%{keyword}%",),
        ).fetchone()
    else:
        rows = conn.execute(
            """SELECT condition_id, direction, market_title, first_seen_price
               FROM tracked_whale_positions
               WHERE alias='sportmaster777' AND status='open'
               ORDER BY first_seen_at DESC LIMIT 1"""
        ).fetchone()
    conn.close()

    if not rows:
        log(f"[FAIL] no open sportmaster position{'matching '+keyword if keyword else ''}")
        return 1

    cid, direction, title, whale_price = rows
    log(f"Target: {title}")
    log(f"Side: {direction}, sportmaster's entry: ${whale_price:.3f}")

    from monitor import clob_client_wrapper as clob

    tokens = await clob.get_market_tokens(cid)
    token_id = tokens.get(direction)
    if not token_id:
        log(f"[FAIL] could not resolve token_id for {direction}")
        return 1
    log(f"token_id: {token_id[:16]}...")

    book = await clob.get_order_book(token_id)
    asks = book.get("asks", [])
    if not asks:
        log("[FAIL] empty ask book")
        return 1

    best_ask = asks[0]["price"]
    # Buy at best_ask + $0.01 as a marketable limit (will fill at best_ask)
    target_price = round(best_ask + 0.01, 3)
    size_usd = 1.00
    size_shares = round(size_usd / target_price, 2)
    if size_shares < 1.0:
        size_shares = 1.0

    log(f"Best ask: ${best_ask:.3f}")
    log(f"Placing: BUY {size_shares} sh @ ${target_price:.3f} (= ${size_shares * target_price:.2f})")
    log(f"Slippage from sportmaster: {(target_price - whale_price) / whale_price * 100:+.1f}%")

    try:
        ans = input("\nProceed with REAL order? [y/N]: ")
    except EOFError:
        ans = "n"
    if ans.strip().lower() != "y":
        log("Aborted")
        return 0

    log("placing...")
    t0 = time.time()
    try:
        resp = await clob.place_limit_order(
            token_id=token_id,
            price=target_price,
            size_shares=size_shares,
            side="BUY",
            order_type="GTC",
        )
    except Exception as e:
        log(f"[FAIL] place_order: {e}")
        return 1

    elapsed = (time.time() - t0) * 1000
    log(f"placement: {elapsed:.0f}ms")
    log(f"response: {resp}")

    if not resp.get("success"):
        log(f"[FAIL] CLOB rejected: {resp.get('errorMsg')}")
        return 1

    order_id = resp.get("orderID") or resp.get("order_id")
    log(f"[OK] order placed: {order_id}")

    # Poll briefly for fill
    log("polling for fill...")
    for i in range(10):
        await asyncio.sleep(2)
        try:
            status = await clob.get_order_status(order_id)
            matched = float(status.get("size_matched") or status.get("filled") or 0)
            log(f"  poll {i+1}/10: matched={matched}")
            if matched >= size_shares * 0.99:
                log(f"[OK] FILLED at ~${target_price:.3f}")
                log(f"\n>>> Check polymarket.com — position should appear")
                log(f">>> Or check polygonscan.com/address/{os.getenv('POLYMARKET_FUNDER_ADDRESS')}")
                return 0
        except Exception as e:
            log(f"  poll err: {e}")

    log("[INFO] Order placed but didn't fully fill in 20s — will sit until cancelled or filled")
    log(f"       Order ID: {order_id}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
