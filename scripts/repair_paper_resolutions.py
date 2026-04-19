#!/usr/bin/env python3
"""Retroactively fix paper_positions rows whose outcome disagrees with
Gamma's final outcomePrices, and adjust the paper bankroll accordingly.

Categories of repair:
  A. MISMATCH: paper says WIN but Gamma says LOSS (or vice versa)
     → correct outcome + pnl + resolution_price; adjust bankroll
  B. AMBIGUOUS-NOW-KNOWN: paper says RESOLVED $0 but Gamma now has final prices
     → upgrade to WIN/LOSS; adjust bankroll

Default is DRY RUN. Pass --apply to commit.

Usage:
    python3 scripts/repair_paper_resolutions.py           # dry run
    python3 scripts/repair_paper_resolutions.py --apply   # commit fixes
"""
import argparse
import asyncio
import aiohttp
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = os.getenv("DB_PATH", str(Path(__file__).resolve().parent.parent / "trades.db"))
H = {"User-Agent": "Mozilla/5.0"}


async def fetch_market(session, cid):
    for params in [{"condition_ids": cid, "closed": "true"},
                   {"condition_ids": cid}]:
        try:
            async with session.get(
                "https://gamma-api.polymarket.com/markets",
                params=params, headers=H,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                if r.status != 200:
                    continue
                d = await r.json()
                if d:
                    return d[0]
        except Exception:
            continue
    return None


def parse_outcomes(m):
    try:
        op = m.get("outcomePrices", "[]")
        op = json.loads(op) if isinstance(op, str) else op
        oc = m.get("outcomes", "[]")
        oc = json.loads(oc) if isinstance(oc, str) else oc
        return [(name, float(p)) for name, p in zip(oc, op)]
    except Exception:
        return []


def determine_outcome(outcome_pairs, direction):
    # Apr 19: threshold loosened 0.99/0.01 → 0.95/0.05 to match
    # resolve_ambiguous_via_gamma in paper_trader.py. Catches markets
    # that have settled on CLOB but haven't hit UMA-finalized $1.00/$0.00
    # yet (settlement lag window).
    for name, price in outcome_pairs:
        if str(name).strip().lower() == str(direction).strip().lower():
            if price >= 0.95:
                return "WIN", 1.0
            if price <= 0.05:
                return "LOSS", 0.0
            return "LIVE", price
    return "NOT_FOUND", 0.0


def compute_correct_pnl(outcome, size, entry):
    """Return (pnl, resolution_price, bankroll_delta) for the correct outcome.
    bankroll_delta: what the bankroll SHOULD have received on close."""
    if outcome == "WIN":
        pnl = size * (1.0 / entry - 1.0) if entry > 0 else 0.0
        return pnl, 1.0, size + pnl
    if outcome == "LOSS":
        return -size, 0.0, 0.0
    # RESOLVED (fallback)
    return 0.0, 0.5, size


def compute_old_pnl(outcome, size, entry, stored_pnl, stored_price):
    """What was actually applied to bankroll at original close time."""
    if outcome == "WIN":
        return stored_pnl, stored_price, size + (stored_pnl or 0)
    if outcome == "LOSS":
        return -size, 0.0, 0.0
    # RESOLVED: stake refund only
    return 0.0, stored_price or 0.5, size


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="Actually apply fixes (default: dry run)")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT id, whale_alias, condition_id, direction, market_title,
                  entry_price, paper_size_usd, opened_at, resolved_at,
                  outcome, resolution_price, paper_pnl
           FROM paper_positions
           WHERE outcome IN ('WIN','LOSS','RESOLVED')
           ORDER BY resolved_at"""
    ).fetchall()
    print(f"Scanning {len(rows)} resolved paper positions against Gamma...\n")

    fixes = []
    async with aiohttp.ClientSession() as session:
        for r in rows:
            pp = dict(r)
            m = await fetch_market(session, pp["condition_id"])
            if not m:
                continue
            pairs = parse_outcomes(m)
            if not pairs:
                continue
            gamma_outcome, gamma_price = determine_outcome(pairs, pp["direction"])
            if gamma_outcome in ("LIVE", "NOT_FOUND"):
                continue  # skip — not definitively resolved

            current_outcome = pp["outcome"]
            if gamma_outcome == current_outcome:
                continue  # already correct

            # Need to fix
            size = pp["paper_size_usd"]
            entry = pp["entry_price"]
            old_pnl, old_resprice, old_bankroll_delta = compute_old_pnl(
                current_outcome, size, entry,
                pp["paper_pnl"], pp["resolution_price"],
            )
            new_pnl, new_resprice, new_bankroll_delta = compute_correct_pnl(
                gamma_outcome, size, entry,
            )
            bankroll_adjustment = new_bankroll_delta - old_bankroll_delta

            fixes.append({
                "id": pp["id"],
                "alias": pp["whale_alias"],
                "title": pp["market_title"],
                "direction": pp["direction"],
                "entry": entry,
                "size": size,
                "old_outcome": current_outcome,
                "new_outcome": gamma_outcome,
                "old_pnl": old_pnl,
                "new_pnl": new_pnl,
                "new_resprice": new_resprice,
                "bankroll_adjustment": bankroll_adjustment,
            })

    if not fixes:
        print("[OK] No repairs needed — all resolutions match Gamma.")
        conn.close()
        return

    # Report
    print(f"{'='*100}")
    print(f"FIXES NEEDED: {len(fixes)}")
    print(f"{'='*100}\n")
    total_adj = 0.0
    for f in fixes:
        print(f"  id={f['id']}  {f['alias']}")
        print(f"    Market: {f['title'][:70]}")
        print(f"    Side: {f['direction']} @ ${f['entry']:.3f}  size=${f['size']:.2f}")
        print(f"    CURRENT: {f['old_outcome']} pnl=${f['old_pnl']:+.2f}")
        print(f"    CORRECT: {f['new_outcome']} pnl=${f['new_pnl']:+.2f}")
        print(f"    Bankroll adjustment: ${f['bankroll_adjustment']:+.2f}")
        print()
        total_adj += f["bankroll_adjustment"]

    # Current bankroll
    row = conn.execute("SELECT bankroll_usd FROM paper_state WHERE id=1").fetchone()
    current_bankroll = float(row[0]) if row else 0.0
    corrected_bankroll = current_bankroll + total_adj

    print(f"{'='*100}")
    print(f"SUMMARY")
    print(f"{'='*100}")
    print(f"  Current bankroll:      ${current_bankroll:.2f}")
    print(f"  Total adjustments:     ${total_adj:+.2f}")
    print(f"  Corrected bankroll:    ${corrected_bankroll:.2f}")
    print()

    if not args.apply:
        print("[DRY RUN] Use --apply to commit these fixes.")
        conn.close()
        return

    # Apply
    now_iso = datetime.now(timezone.utc).isoformat()
    for f in fixes:
        conn.execute(
            """UPDATE paper_positions
               SET outcome=?, paper_pnl=?, resolution_price=?
               WHERE id=?""",
            (f["new_outcome"], f["new_pnl"], f["new_resprice"], f["id"]),
        )
    conn.execute(
        "UPDATE paper_state SET bankroll_usd=? WHERE id=1",
        (corrected_bankroll,),
    )
    conn.commit()
    conn.close()
    print(f"[APPLIED] {len(fixes)} rows fixed, bankroll adjusted to ${corrected_bankroll:.2f}")


if __name__ == "__main__":
    asyncio.run(main())
