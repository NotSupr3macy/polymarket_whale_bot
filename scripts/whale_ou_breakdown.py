#!/usr/bin/env python3
"""Analyze a whale's NBA totals (O/U) history on Polymarket.
Splits by Over vs Under to detect directional asymmetry.

Example output:
    NBA O/U:   31W / 23L   57.4% WR   +$145K PnL
    ├── Over:  12W / 15L   44.4% WR   -$22K   <-- leak
    └── Under: 19W /  8L   70.4% WR  +$167K   <-- print

Usage:
    python3 scripts/whale_ou_breakdown.py ALIAS WALLET
    python3 scripts/whale_ou_breakdown.py sportmaster777 0x32ed517a571c01b6e9adecf61ba81ca48ff2f960
    python3 scripts/whale_ou_breakdown.py nbasniper 0x492442eab586f242b53bda933fd5de859c8a3782
"""
import asyncio
import aiohttp
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone

H = {"User-Agent": "Mozilla/5.0"}


async def fetch_trades(s, wallet):
    out, off = [], 0
    while True:
        try:
            async with s.get(
                "https://data-api.polymarket.com/trades",
                params={"user": wallet, "takerOnly": "false",
                        "limit": 500, "offset": off},
                timeout=30,
            ) as r:
                d = await r.json()
        except Exception:
            break
        if not isinstance(d, list) or not d:
            break
        out.extend(d)
        if len(d) < 500:
            break
        off += 500
        if off > 30000:
            break
    return out


async def fetch_market(s, sem, cid):
    """Try multiple Gamma query strategies. Some of nbasniper's older
    markets are missing under certain param combos — be thorough."""
    async with sem:
        attempts = [
            {"condition_ids": cid, "closed": "true"},
            {"condition_ids": cid, "closed": "false"},
            {"condition_ids": cid},
            {"condition_ids": cid, "active": "false"},
            {"condition_ids": cid, "archived": "true"},
        ]
        for params in attempts:
            try:
                async with s.get(
                    "https://gamma-api.polymarket.com/markets",
                    params=params, headers=H, timeout=20,
                ) as r:
                    if r.status == 200:
                        d = await r.json()
                        if d:
                            return d[0]
            except Exception:
                continue
        return None


def is_totals_market(title: str) -> bool:
    t = (title or "").lower()
    return "o/u" in t or "over/under" in t


def sport_from_slug(slug: str) -> str:
    """Polymarket slug prefix convention: 'nba-lakers-rockets-...'"""
    if not slug:
        return "unknown"
    return slug.split("-")[0].lower()


def ou_direction(direction: str) -> str:
    """Normalize direction to 'Over' / 'Under' or '?'"""
    d = (direction or "").strip().lower()
    if d == "over":
        return "Over"
    if d == "under":
        return "Under"
    return direction  # fall-through (shouldn't happen for totals)


def log(*a):
    print(*a, flush=True)


async def analyze(alias, wallet):
    log(f"\n{'=' * 80}")
    log(f"NBA TOTALS (O/U) ANALYSIS: {alias}  ({wallet})")
    log(f"{'=' * 80}")

    async with aiohttp.ClientSession() as s:
        trades = await fetch_trades(s, wallet)
        log(f"Fetched {len(trades)} trades")
        if not trades:
            log("NO TRADES")
            return

        ts_old = min(int(t.get("timestamp", 0)) for t in trades)
        ts_new = max(int(t.get("timestamp", 0)) for t in trades)
        span_days = (ts_new - ts_old) / 86400
        log(f"Span: {datetime.fromtimestamp(ts_old, tz=timezone.utc):%Y-%m-%d}"
            f" -> {datetime.fromtimestamp(ts_new, tz=timezone.utc):%Y-%m-%d}"
            f"  ({span_days:.1f} days)")

        # Pull unique cids
        cids = {t.get("conditionId", "") for t in trades if t.get("conditionId")}
        log(f"Fetching Gamma data for {len(cids)} unique markets...")
        sem = asyncio.Semaphore(20)
        markets = await asyncio.gather(*[fetch_market(s, sem, c) for c in cids])
        md_map = dict(zip(cids, markets))
        hits = sum(1 for m in markets if m is not None)
        log(f"Gamma hit rate: {hits}/{len(cids)} "
            f"({hits / len(cids) * 100 if cids else 0:.1f}%)")

    # Reconstruct positions per (cid, outcome)
    pos = defaultdict(lambda: {
        "buy_sh": 0.0, "buy_usd": 0.0, "sell_sh": 0.0, "sell_usd": 0.0,
        "title": "",
    })
    for t in trades:
        cid = t.get("conditionId", "")
        if not cid:
            continue
        outcome = t.get("outcome", "")
        key = (cid, outcome)
        p = pos[key]
        p["title"] = t.get("title", "")
        sz = float(t.get("size", 0))
        pr = float(t.get("price", 0))
        if t.get("side", "").upper() == "BUY":
            p["buy_sh"] += sz
            p["buy_usd"] += sz * pr
        else:
            p["sell_sh"] += sz
            p["sell_usd"] += sz * pr

    # Totals-specific diagnostic: how many totals positions did we find,
    # and of those, how many had Gamma data available?
    totals_positions = [(cid, outcome, p) for (cid, outcome), p in pos.items()
                        if is_totals_market(p["title"])]
    totals_cids_set = {cid for cid, _, _ in totals_positions}
    totals_with_gamma = sum(1 for c in totals_cids_set if md_map.get(c) is not None)
    log(f"Totals positions: {len(totals_positions)} "
        f"across {len(totals_cids_set)} markets")
    log(f"Totals markets with Gamma data: {totals_with_gamma}/"
        f"{len(totals_cids_set)} "
        f"({totals_with_gamma / len(totals_cids_set) * 100 if totals_cids_set else 0:.1f}%)")

    # For each position: determine sport, direction, outcome status
    # Track why rows get dropped for diagnostic
    drop_reasons = defaultdict(int)
    rows = []
    for (cid, outcome), p in pos.items():
        if not is_totals_market(p["title"]):
            drop_reasons["not_totals"] += 1
            continue
        m = md_map.get(cid)
        if not m:
            drop_reasons["gamma_not_found"] += 1
            continue
        sport = sport_from_slug(m.get("slug", ""))
        direction = ou_direction(outcome)
        try:
            op = m.get("outcomePrices", "[]")
            op = json.loads(op) if isinstance(op, str) else op
            oc = m.get("outcomes", "[]")
            oc = json.loads(oc) if isinstance(oc, str) else oc
        except Exception:
            drop_reasons["bad_outcomes_json"] += 1
            continue

        idx = next(
            (i for i, x in enumerate(oc)
             if str(x).strip().lower() == outcome.strip().lower()),
            None,
        )
        if idx is None or idx >= len(op):
            drop_reasons["direction_not_in_outcomes"] += 1
            continue
        try:
            wp = float(op[idx])
        except Exception:
            drop_reasons["price_not_numeric"] += 1
            continue

        net_sh = p["buy_sh"] - p["sell_sh"]
        net_usd = p["buy_usd"] - p["sell_usd"]
        # held-to-resolution only (skip scalps with net_sh ~= 0)
        if net_sh < 0.01:
            drop_reasons["scalped_flat"] += 1
            continue
        entry = net_usd / net_sh if net_sh > 0.01 else 0
        if wp >= 0.95:
            status, won = "WIN", True
            pnl = net_sh - net_usd
        elif wp <= 0.05:
            status, won = "LOSS", False
            pnl = -net_usd
        else:
            drop_reasons["still_live"] += 1
            continue  # still live or ambiguous

        rows.append({
            "sport": sport, "direction": direction,
            "entry": entry, "stake": net_usd, "pnl": pnl, "won": won,
            "title": p["title"],
        })

    # Surface diagnostic so we understand why rows may be missing
    log(f"\nDrop-reason histogram (positions not included in resolved rows):")
    for reason, count in sorted(drop_reasons.items(), key=lambda kv: -kv[1]):
        log(f"  {reason:30s} {count:>5}")
    log(f"  {'-> resolved rows kept':30s} {len(rows):>5}")

    if not rows:
        log("\nNo resolved totals positions found.")
        return

    # ─── Aggregate breakdowns ───────────────────────────────────────
    def _stats(subset):
        if not subset:
            return None
        w = sum(1 for r in subset if r["won"])
        stake = sum(r["stake"] for r in subset)
        pnl = sum(r["pnl"] for r in subset)
        wr = w / len(subset) * 100
        roi = pnl / stake * 100 if stake else 0
        return {"n": len(subset), "w": w, "l": len(subset) - w, "wr": wr,
                "stake": stake, "pnl": pnl, "roi": roi}

    # All totals (every sport)
    log(f"\n=== ALL TOTALS (all sports) ===")
    all_totals = _stats(rows)
    log(f"  {all_totals['w']:>3}W / {all_totals['l']:>3}L   "
        f"{all_totals['wr']:>5.1f}% WR   "
        f"${all_totals['pnl']:>+11,.0f}   "
        f"stake ${all_totals['stake']:>9,.0f}   "
        f"ROI {all_totals['roi']:>+6.1f}%")

    # All totals split Over vs Under
    log(f"\n  Split by direction:")
    for direction in ("Over", "Under"):
        sub = [r for r in rows if r["direction"] == direction]
        s = _stats(sub)
        if not s:
            log(f"    {direction:6s}  (no data)")
            continue
        log(f"    {direction:6s}  {s['w']:>3}W / {s['l']:>3}L   "
            f"{s['wr']:>5.1f}% WR   "
            f"${s['pnl']:>+11,.0f}   "
            f"stake ${s['stake']:>9,.0f}   "
            f"ROI {s['roi']:>+6.1f}%")

    # ─── NBA-specific ───────────────────────────────────────────────
    nba_rows = [r for r in rows if r["sport"] == "nba"]
    log(f"\n=== NBA TOTALS ONLY ===")
    s_nba = _stats(nba_rows)
    if not s_nba:
        log("  No NBA totals found.")
    else:
        log(f"  {s_nba['w']:>3}W / {s_nba['l']:>3}L   "
            f"{s_nba['wr']:>5.1f}% WR   "
            f"${s_nba['pnl']:>+11,.0f}   "
            f"stake ${s_nba['stake']:>9,.0f}   "
            f"ROI {s_nba['roi']:>+6.1f}%")

        log(f"\n  Split by direction:")
        for direction in ("Over", "Under"):
            sub = [r for r in nba_rows if r["direction"] == direction]
            s = _stats(sub)
            if not s:
                log(f"    {direction:6s}  (no data)")
                continue
            marker = "  <- PRINT" if s['roi'] > 10 else ("  <- LEAK" if s['roi'] < -10 else "")
            log(f"    {direction:6s}  {s['w']:>3}W / {s['l']:>3}L   "
                f"{s['wr']:>5.1f}% WR   "
                f"${s['pnl']:>+11,.0f}   "
                f"stake ${s['stake']:>9,.0f}   "
                f"ROI {s['roi']:>+6.1f}%{marker}")

    # ─── All sports, totals, direction breakdown ────────────────────
    log(f"\n=== ALL SPORTS × DIRECTION ===")
    by_sport_dir = defaultdict(list)
    for r in rows:
        by_sport_dir[(r["sport"], r["direction"])].append(r)
    for (sport, direction), sub in sorted(by_sport_dir.items(),
                                           key=lambda kv: -len(kv[1])):
        s = _stats(sub)
        marker = "  <- PRINT" if s['roi'] > 10 else ("  <- LEAK" if s['roi'] < -10 else "")
        log(f"  {sport:10s} {direction:6s}  {s['w']:>3}W / {s['l']:>3}L   "
            f"{s['wr']:>5.1f}% WR   "
            f"${s['pnl']:>+11,.0f}   "
            f"stake ${s['stake']:>9,.0f}   "
            f"ROI {s['roi']:>+6.1f}%{marker}")

    # ─── Verdict ────────────────────────────────────────────────────
    log(f"\n{'-' * 80}")
    if s_nba:
        nba_over = _stats([r for r in nba_rows if r["direction"] == "Over"])
        nba_under = _stats([r for r in nba_rows if r["direction"] == "Under"])
        if nba_over and nba_under:
            diff = nba_under["roi"] - nba_over["roi"]
            log(f"NBA Under/Over ROI delta: {diff:+.1f} percentage points")
            if abs(diff) > 15 and (nba_over["n"] >= 10 or nba_under["n"] >= 10):
                strong = "UNDERS" if diff > 0 else "OVERS"
                weak = "OVERS" if diff > 0 else "UNDERS"
                log(f"  [ASYMMETRIC EDGE] favor NBA {strong} over {weak}")
            else:
                log(f"  [SYMMETRIC-ISH] no strong directional preference")
    log(f"{'-' * 80}")


async def main():
    if len(sys.argv) != 3:
        print("Usage: whale_ou_breakdown.py ALIAS WALLET", file=sys.stderr)
        sys.exit(2)
    alias, wallet = sys.argv[1], sys.argv[2]
    await analyze(alias, wallet)


if __name__ == "__main__":
    asyncio.run(main())
