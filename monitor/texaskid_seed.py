"""
One-time seed script: backfill texaskid_positions table with all current positions.

Run this after the tracker has been restarted to populate the DB with
positions the tracker missed (because it was started mid-stream).

Usage:
    python3 monitor/texaskid_seed.py
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

TEXASKID_WALLET = "0xc8075693f48668a264b9fa313b47f52712fcc12b"
DATA_API = "https://data-api.polymarket.com"
MIN_POSITION_USD = 50
DB_PATH = os.getenv(
    "DB_PATH", str(Path(__file__).resolve().parent.parent / "trades.db")
)


async def fetch_positions() -> list[dict]:
    positions: list[dict] = []
    offset = 0
    async with aiohttp.ClientSession() as s:
        while True:
            params = {
                "user": TEXASKID_WALLET,
                "sizeThreshold": "0.1",
                "limit": "100",
                "offset": str(offset),
            }
            async with s.get(
                f"{DATA_API}/v1/positions",
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    break
                batch = await resp.json()
                if not batch:
                    break
                positions.extend(batch)
                if len(batch) < 100:
                    break
                offset += 100
                await asyncio.sleep(0.2)
    return positions


def classify(pos: dict) -> tuple[str, str]:
    """Return (status, outcome) based on position state."""
    cur = float(pos.get("curPrice") or 0)
    redeem = pos.get("redeemable", False)

    if redeem or cur >= 0.98 or cur <= 0.02:
        if cur >= 0.98:
            return "closed", "WIN"
        elif cur <= 0.02:
            return "closed", "LOSS"
        else:
            return "closed", "RESOLVED"
    else:
        return "open", ""


async def main() -> int:
    print(f"Fetching texaskid positions from API...")
    positions = await fetch_positions()
    print(f"Got {len(positions)} positions\n")

    conn = sqlite3.connect(DB_PATH)

    # Ensure table exists
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS texaskid_positions (
            condition_id TEXT PRIMARY KEY,
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
            resolved_at TEXT
        )
        """
    )

    now = datetime.now(timezone.utc).isoformat()
    inserted = 0
    updated = 0
    skipped = 0

    for p in positions:
        cid = p.get("conditionId") or ""
        init = float(p.get("initialValue") or 0)
        if not cid or init < MIN_POSITION_USD:
            skipped += 1
            continue

        title = p.get("title") or ""
        side = p.get("outcome") or "YES"
        avg = float(p.get("avgPrice") or p.get("curPrice") or 0.5)
        cur = float(p.get("curPrice") or 0.5)
        status, outcome = classify(p)

        existing = conn.execute(
            "SELECT 1 FROM texaskid_positions WHERE condition_id=?", (cid,)
        ).fetchone()

        if existing:
            conn.execute(
                """UPDATE texaskid_positions
                   SET current_size_usd=?, current_price=?, status=?, outcome=?,
                       last_updated=?, alert_sent=1
                   WHERE condition_id=?""",
                (init, cur, status, outcome, now, cid),
            )
            updated += 1
        else:
            conn.execute(
                """INSERT INTO texaskid_positions
                   (condition_id, direction, market_title, first_seen_price,
                    first_seen_size_usd, current_size_usd, current_price,
                    status, outcome, first_seen_at, last_updated, alert_sent,
                    resolved_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)""",
                (
                    cid, side, title, avg, init, init, cur,
                    status, outcome, now, now,
                    now if status == "closed" else None,
                ),
            )
            inserted += 1

    conn.commit()

    # Summary
    wins = conn.execute(
        "SELECT COUNT(*) FROM texaskid_positions WHERE outcome='WIN'"
    ).fetchone()[0]
    losses = conn.execute(
        "SELECT COUNT(*) FROM texaskid_positions WHERE outcome='LOSS'"
    ).fetchone()[0]
    open_ = conn.execute(
        "SELECT COUNT(*) FROM texaskid_positions WHERE status='open'"
    ).fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM texaskid_positions").fetchone()[0]
    conn.close()

    print(f"Seed complete:")
    print(f"  Inserted: {inserted}")
    print(f"  Updated:  {updated}")
    print(f"  Skipped:  {skipped} (below ${MIN_POSITION_USD} threshold)")
    print()
    print(f"Tracker DB state:")
    print(f"  Total:    {total}")
    print(f"  Wins:     {wins}")
    print(f"  Losses:   {losses}")
    print(f"  Open:     {open_}")
    if wins + losses > 0:
        wr = wins / (wins + losses)
        print(f"  Win Rate: {wr:.1%} ({wins}W/{losses}L)")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
