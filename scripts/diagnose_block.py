#!/usr/bin/env python3
"""Determine what level of Polymarket block we're hitting.

Tests in order of granularity:
  1. Public unauthenticated reads — if blocked, it's IP-level
  2. Authenticated reads (with our API key) — if blocked, account-level
  3. Authenticated writes (place_order) — if only this fails, write-level only
  4. Geo-detection probe via Polymarket's own region check

Run:
    source venv/bin/activate
    python3 scripts/diagnose_block.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import aiohttp

_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

CLOB = "https://clob.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"
DATA = "https://data-api.polymarket.com"


def hr():
    print("-" * 70)


async def check_ip_info():
    print("\n## 1. What does the world see as our IP / location?")
    async with aiohttp.ClientSession() as s:
        async with s.get("https://ipinfo.io/json", timeout=10) as r:
            info = await r.json()
    print(f"   IP:       {info.get('ip')}")
    print(f"   Country:  {info.get('country')}")
    print(f"   City:     {info.get('city')}")
    print(f"   Org:      {info.get('org')}")
    print(f"   ASN:      {info.get('org', '').split()[0] if info.get('org') else 'unknown'}")
    return info


async def check_unauth_reads():
    print("\n## 2. Public/unauthenticated CLOB reads (anyone can do these)")
    tests = [
        ("CLOB GET /markets?limit=1", f"{CLOB}/markets?limit=1"),
        ("CLOB GET /sampling-markets", f"{CLOB}/sampling-markets?limit=1"),
        ("Gamma GET /markets?limit=1", f"{GAMMA}/markets?limit=1"),
        ("Data API GET /trades", f"{DATA}/trades?limit=1"),
    ]
    async with aiohttp.ClientSession() as s:
        for label, url in tests:
            try:
                async with s.get(url, timeout=10) as r:
                    status = r.status
                    body = await r.text()
                    body_preview = body[:120].replace("\n", " ")
            except Exception as e:
                status = "ERR"
                body_preview = str(e)
            marker = "✅" if status == 200 else "❌"
            print(f"   {marker} {label:38s} HTTP {status}")
            if status != 200:
                print(f"      body: {body_preview}")


async def check_auth_reads():
    print("\n## 3. Authenticated CLOB reads (need our API key)")
    try:
        from monitor.clob_client_wrapper import get_client
        client = get_client()
    except Exception as e:
        print(f"   ❌ client setup failed: {e}")
        return

    try:
        orders = client.get_orders()
        print(f"   ✅ get_orders():       OK (you have {len(orders or [])} open orders)")
    except Exception as e:
        print(f"   ❌ get_orders():       FAIL: {e}")

    try:
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        sig = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "0"))
        bal = client.get_balance_allowance(
            BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL, signature_type=sig,
            )
        )
        if isinstance(bal, dict):
            usd = int(bal.get("balance", 0)) / 1_000_000
        else:
            usd = int(getattr(bal, "balance", 0)) / 1_000_000
        print(f"   ✅ get_balance:        ${usd:.2f}")
    except Exception as e:
        print(f"   ❌ get_balance:        FAIL: {e}")


async def check_auth_write():
    """Attempt a write (place_order) and report exactly what comes back."""
    print("\n## 4. Authenticated CLOB write (place_order — likely the blocked one)")
    try:
        from monitor import clob_client_wrapper as clob
        from py_clob_client.exceptions import PolyApiException
    except Exception as e:
        print(f"   ❌ wrapper import failed: {e}")
        return

    # Use sportmaster's most recent open tracker row (lowest-effort to find a market)
    import sqlite3
    db_path = os.getenv("DB_PATH", "/home/botuser/whale-bot/trades.db")
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        """SELECT condition_id, direction, market_title, first_seen_price
           FROM tracked_whale_positions
           WHERE alias='sportmaster777'
             AND status='open'
             AND first_seen_price BETWEEN 0.20 AND 0.80
           ORDER BY first_seen_at DESC LIMIT 1"""
    ).fetchone()
    conn.close()
    if not row:
        print("   ⚠️  no test market found in tracker — skipping write test")
        return
    cid, direction, title, whale_price = row

    tokens = await clob.get_market_tokens(cid)
    token_id = tokens.get(direction)
    if not token_id:
        print(f"   ⚠️  could not resolve token_id for {direction} — skipping")
        return

    book = await clob.get_order_book(token_id)
    asks = book.get("asks", [])
    if not asks:
        print("   ⚠️  empty book — skipping")
        return
    best_ask = asks[0]["price"]

    # Place at $0.10 BELOW best ask, $0.30 max — well below market so won't fill
    target_price = max(0.01, round(best_ask - 0.10, 3))
    size_shares = round(0.30 / target_price, 2)
    if size_shares < 1:
        size_shares = 1.0

    print(f"   Trying: BUY {size_shares} sh @ ${target_price:.3f} on '{title[:40]}'")

    try:
        resp = await clob.place_limit_order(
            token_id=token_id,
            price=target_price,
            size_shares=size_shares,
            side="BUY",
            order_type="GTC",
        )
        print(f"   ✅ ORDER ACCEPTED — write works!")
        print(f"      response: {resp}")
        # Cancel immediately
        order_id = resp.get("orderID") or resp.get("order_id")
        if order_id:
            await clob.cancel_order(order_id)
            print(f"   ✅ cancelled successfully")
    except PolyApiException as e:
        msg = str(e)
        print(f"   ❌ ORDER REJECTED: {msg}")
        if "geoblock" in msg.lower() or "region" in msg.lower():
            print(f"      → IP-LEVEL or REGION-LEVEL block")
    except Exception as e:
        print(f"   ❌ unexpected error: {type(e).__name__}: {e}")


async def main():
    print("=" * 70)
    print("POLYMARKET BLOCK DIAGNOSTIC")
    print("=" * 70)

    info = await check_ip_info()

    await check_unauth_reads()
    await check_auth_reads()
    await check_auth_write()

    print()
    hr()
    print("INTERPRETATION GUIDE")
    hr()
    print("""
  All reads OK + write FAIL with 'region' error:
    → COUNTRY-LEVEL block on writes only.
    → Move to a Polymarket-allowed country IP fixes it.

  All reads OK + write FAIL with non-region error:
    → API/account/parameter issue, not a geoblock.

  Some reads FAIL with 403:
    → IP-LEVEL block on this datacenter.
    → ANY datacenter IP may have same problem.
    → Need residential IP (Mullvad VPN or residential proxy).

  Reads OK + write OK:
    → No block; live_smoke_test.py should also work.
    → Re-run live_smoke_test.py.

  All FAIL:
    → Probably IP-level + datacenter detection.
    → Try a different country AND/OR a residential IP.
""")


if __name__ == "__main__":
    asyncio.run(main())
