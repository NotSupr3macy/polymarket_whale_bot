#!/usr/bin/env python3
"""Cross-check every resolved paper_positions row against Polymarket's
actual final outcomePrices. Flags any where the paper trader's recorded
outcome differs from Gamma's ground truth.

Run on the server: python3 scripts/verify_paper_resolutions.py
"""
import asyncio, aiohttp, json, os, sqlite3, sys
from pathlib import Path
from datetime import datetime, timezone

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
                data = await r.json()
                if data:
                    return data[0]
        except Exception as e:
            print(f"  API err for {cid[:16]}: {e}", file=sys.stderr)
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
    """Return ('WIN'|'LOSS'|'LIVE'|'NOT_FOUND', final_price)."""
    for name, price in outcome_pairs:
        if str(name).strip().lower() == str(direction).strip().lower():
            if price >= 0.99:
                return "WIN", price
            if price <= 0.01:
                return "LOSS", price
            return "LIVE", price
    return "NOT_FOUND", 0.0


async def main():
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
    conn.close()
    print(f"Verifying {len(rows)} resolved paper positions\n")

    mismatches = []
    confirms = []
    unknowns = []

    async with aiohttp.ClientSession() as session:
        for r in rows:
            pp = dict(r)
            m = await fetch_market(session, pp["condition_id"])
            if not m:
                unknowns.append((pp, "gamma_not_found", []))
                continue
            outcomes = parse_outcomes(m)
            if not outcomes:
                unknowns.append((pp, "no_outcome_prices", []))
                continue
            gamma_outcome, gamma_price = determine_outcome(outcomes, pp["direction"])

            paper_outcome = pp["outcome"]
            # Normalize for comparison
            match = False
            if gamma_outcome == "LIVE":
                note = f"gamma says STILL LIVE at ${gamma_price:.3f}"
                unknowns.append((pp, note, outcomes))
                continue
            if gamma_outcome == "NOT_FOUND":
                note = f"direction '{pp['direction']}' not in Gamma outcomes {[o[0] for o in outcomes]}"
                unknowns.append((pp, note, outcomes))
                continue

            if paper_outcome == "RESOLVED":
                # Paper trader left this ambiguous; Gamma knows the answer
                note = f"paper said 📋 ambiguous, Gamma says actually {gamma_outcome}"
                unknowns.append((pp, note, outcomes))
                continue

            if paper_outcome == gamma_outcome:
                confirms.append(pp)
            else:
                # paper=WIN but gamma=LOSS, or vice versa
                mismatches.append((pp, gamma_outcome, gamma_price, outcomes))

    # ── Report ──
    print(f"=" * 90)
    print(f"RESULTS: {len(confirms)} confirmed, {len(mismatches)} MISMATCHES, {len(unknowns)} unknown")
    print(f"=" * 90)

    if mismatches:
        print(f"\n🚨 MISMATCHES (paper_trader wrong, Gamma right):\n")
        for pp, gamma_outcome, gamma_price, outcomes in mismatches:
            print(f"  [{pp['resolved_at'][:19]}] {pp['whale_alias']}")
            print(f"    Market: {pp['market_title'][:70]}")
            print(f"    Side: {pp['direction']} @ ${pp['entry_price']:.3f}")
            print(f"    Paper said: {pp['outcome']} (P&L ${pp['paper_pnl']:+.2f})")
            print(f"    Gamma says: {gamma_outcome} at ${gamma_price:.3f}")
            print(f"    All outcomes: {[(n, f'{p:.3f}') for n,p in outcomes]}")
            print(f"    paper_positions.id = {pp['id']}")
            print()

    if unknowns:
        print(f"\n⚠️  UNKNOWN / AMBIGUOUS:\n")
        for pp, note, outcomes in unknowns:
            print(f"  [{pp['resolved_at'][:19] if pp['resolved_at'] else 'open'}] {pp['whale_alias']}")
            print(f"    Market: {pp['market_title'][:70]}")
            print(f"    Side: {pp['direction']} @ ${pp['entry_price']:.3f}")
            print(f"    Paper outcome: {pp['outcome']} (P&L ${pp['paper_pnl'] if pp['paper_pnl'] else 0:+.2f})")
            print(f"    Reason: {note}")
            if outcomes:
                print(f"    Outcomes: {[(n, f'{p:.3f}') for n,p in outcomes]}")
            print()

    if confirms:
        print(f"\n✅ CONFIRMED CORRECT ({len(confirms)}):")
        for pp in confirms:
            emoji = "✓W" if pp["outcome"] == "WIN" else "✓L"
            print(f"  {emoji}  {pp['whale_alias']:22s} {pp['market_title'][:50]:50s}  ${pp['paper_pnl']:+.2f}")

    # Summary
    if mismatches:
        print(f"\n🚨 CRITICAL: {len(mismatches)} paper positions have WRONG outcomes.")
        total_wrong_pnl = sum(pp["paper_pnl"] or 0 for pp, *_ in mismatches)
        print(f"   Erroneous paper P&L: ${total_wrong_pnl:+.2f}")
    else:
        print(f"\n✅ All confirmable resolutions match Gamma's truth.")


if __name__ == "__main__":
    asyncio.run(main())
