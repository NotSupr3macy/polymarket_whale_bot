#!/usr/bin/env python3
"""Fingerprint a single wallet to classify it as:
  [PROMOTE]  — real directional whale, worth copying
  [SHADOW]   — ambiguous, add to paper-trader SHADOW_WHALES for 1-2 weeks
  [REJECT]   — scalp / arb / bridge / too-much-chalk, skip

Adds MERGE activity detection on top of promotion_review.py logic:
  When a whale buys both YES and NO on the same market then calls the
  /activity "MERGE" operation, that's arbitrage — they're locking in the
  spread, not making a directional bet. BUY/SELL-only scans miss this
  because shares netted to zero looks indistinguishable from scalping.

Usage:
    python3 scripts/fingerprint_whale.py ALIAS WALLET
    python3 scripts/fingerprint_whale.py mystery 0x32b484581fc5606dE9C1e43AF4636b6Be9BC8B21
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


async def fetch_activity(s, wallet):
    """Fetch /activity which reports MERGE, REDEEM, SPLIT ops not in /trades."""
    out, off = [], 0
    while True:
        try:
            async with s.get(
                "https://data-api.polymarket.com/activity",
                params={"user": wallet, "limit": 500, "offset": off},
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
    async with sem:
        for params in [{"condition_ids": cid, "closed": "true"},
                       {"condition_ids": cid}]:
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


def classify_subtype(title):
    t = title.lower()
    if "end in a draw" in t: return "draw"
    if "o/u" in t or "over/under" in t: return "totals"
    if "spread" in t: return "spread"
    if re.search(r"\b(period|map|quarter|inning|set|game \d)\b", t):
        return "segment"
    if "winner" in t and ("quarterfinal" in t or "semifinal" in t or "final" in t):
        return "futures"
    if "win on 20" in t: return "daily-ml"
    if " vs " in t or " vs. " in t: return "h2h-ml"
    if "win the" in t and ("cup" in t or "champion" in t or "division" in t or "conference" in t):
        return "futures"
    return "other"


def log(*a):
    print(*a, flush=True)


async def fingerprint(alias, wallet):
    log(f"\n{'#' * 80}")
    log(f"# FINGERPRINT: {alias}  ({wallet})")
    log(f"{'#' * 80}")
    async with aiohttp.ClientSession() as s:
        trades = await fetch_trades(s, wallet)
        activity = await fetch_activity(s, wallet)
        log(f"Fetched {len(trades)} trades, {len(activity)} activity ops")
        if not trades:
            log("NO TRADES — cannot fingerprint")
            return

        # ── MERGE / SPLIT counting (arb detector) ────────────────────
        merge_cids = defaultdict(int)
        split_cids = defaultdict(int)
        redeem_cids = defaultdict(int)
        for a in activity:
            op = (a.get("type") or a.get("operation") or "").upper()
            cid = a.get("conditionId") or a.get("condition_id") or ""
            if not cid: continue
            if "MERGE" in op: merge_cids[cid] += 1
            elif "SPLIT" in op: split_cids[cid] += 1
            elif "REDEEM" in op: redeem_cids[cid] += 1
        n_merges = sum(merge_cids.values())
        n_splits = sum(split_cids.values())
        n_redeems = sum(redeem_cids.values())
        log(f"\nActivity ops: {n_merges} MERGE, {n_splits} SPLIT, {n_redeems} REDEEM")

        ts_old = min(int(t.get("timestamp", 0)) for t in trades)
        ts_new = max(int(t.get("timestamp", 0)) for t in trades)
        span_days = (ts_new - ts_old) / 86400
        log(f"Trade span: {datetime.fromtimestamp(ts_old, tz=timezone.utc):%Y-%m-%d}"
            f" -> {datetime.fromtimestamp(ts_new, tz=timezone.utc):%Y-%m-%d}"
            f"  ({span_days:.1f} days)")

        cids = {t.get("conditionId", "") for t in trades if t.get("conditionId")}
        log(f"Unique markets touched: {len(cids)}")
        sem = asyncio.Semaphore(20)
        mkts = await asyncio.gather(*[fetch_market(s, sem, c) for c in cids])
        md_map = dict(zip(cids, mkts))

    # ── Reconstruct positions per (cid, outcome) ─────────────────────
    pos = defaultdict(lambda: {
        "buy_sh": 0, "buy_usd": 0, "sell_sh": 0, "sell_usd": 0,
        "title": "", "n_trades": 0, "first_ts": None, "last_ts": 0,
    })
    per_cid_outcomes = defaultdict(set)
    for t in trades:
        cid = t.get("conditionId", "")
        if not cid: continue
        outcome = t.get("outcome", "")
        key = (cid, outcome)
        p = pos[key]
        p["title"] = t.get("title", "")
        per_cid_outcomes[cid].add(outcome)
        sz = float(t.get("size", 0))
        pr = float(t.get("price", 0))
        ts = int(t.get("timestamp", 0))
        p["n_trades"] += 1
        if t.get("side", "").upper() == "BUY":
            p["buy_sh"] += sz; p["buy_usd"] += sz * pr
        else:
            p["sell_sh"] += sz; p["sell_usd"] += sz * pr
        if p["first_ts"] is None or ts < p["first_ts"]:
            p["first_ts"] = ts
        p["last_ts"] = max(p["last_ts"], ts)

    # ── Both-sides detection (arb fingerprint) ───────────────────────
    n_dual = sum(1 for cid, outs in per_cid_outcomes.items() if len(outs) >= 2)
    dual_pct = n_dual / len(cids) * 100 if cids else 0
    log(f"Markets with BOTH outcomes traded: {n_dual}/{len(cids)} ({dual_pct:.1f}%)")

    # ── Classify each held position ──────────────────────────────────
    rows = []
    for (cid, outcome), p in pos.items():
        net_sh = p["buy_sh"] - p["sell_sh"]
        net_usd = p["buy_usd"] - p["sell_usd"]
        is_scalp = abs(net_sh) < 0.01 and p["buy_sh"] > 0
        was_merged = cid in merge_cids
        if net_sh <= 0 and not is_scalp and not was_merged:
            continue
        entry = net_usd / net_sh if net_sh > 0.01 else (
            p["buy_usd"] / p["buy_sh"] if p["buy_sh"] > 0.01 else 0
        )
        m = md_map.get(cid)
        if not m: continue
        try:
            op = m.get("outcomePrices", "[]")
            op = json.loads(op) if isinstance(op, str) else op
            oc = m.get("outcomes", "[]")
            oc = json.loads(oc) if isinstance(oc, str) else oc
        except Exception:
            continue
        idx = next(
            (i for i, x in enumerate(oc)
             if str(x).strip().lower() == outcome.strip().lower()),
            None,
        )
        if idx is None or idx >= len(op):
            continue
        try:
            wp = float(op[idx])
        except Exception:
            continue

        if was_merged:
            status = "MERGED"; won = None
            pnl = p["sell_usd"] - p["buy_usd"]
        elif is_scalp:
            status = "SCALPED"; won = None
            pnl = p["sell_usd"] - p["buy_usd"]
        elif wp >= 0.99:
            status = "WIN"; won = True
            pnl = net_sh - net_usd
        elif wp <= 0.01:
            status = "LOSS"; won = False
            pnl = -net_usd
        else:
            status = "LIVE"; won = None; pnl = None

        rows.append({
            "cid": cid, "title": p["title"], "outcome": outcome,
            "entry": entry, "stake": max(net_usd, p["buy_usd"]),
            "status": status, "won": won, "pnl": pnl,
            "subtype": classify_subtype(p["title"]),
        })

    resolved = [r for r in rows if r["status"] in ("WIN", "LOSS")]
    scalped = [r for r in rows if r["status"] == "SCALPED"]
    merged = [r for r in rows if r["status"] == "MERGED"]
    live = [r for r in rows if r["status"] == "LIVE"]
    total_exits = len(resolved) + len(scalped) + len(merged)

    log(f"\nResolved: {len(resolved)}  Scalped: {len(scalped)}  "
        f"Merged: {len(merged)}  Live: {len(live)}")

    if not resolved:
        log("0 held-to-resolution positions — cannot evaluate directional edge")
        return

    # ── Directional stats ────────────────────────────────────────────
    w = sum(1 for r in resolved if r["won"])
    stake = sum(r["stake"] for r in resolved)
    pnl_total = sum(r["pnl"] for r in resolved)
    wr = w / len(resolved) * 100
    roi = pnl_total / stake * 100 if stake else 0
    scalp_ratio = len(scalped) / total_exits * 100 if total_exits else 0
    merge_ratio = len(merged) / total_exits * 100 if total_exits else 0

    log(f"\n=== DIRECTIONAL PERFORMANCE (held-to-resolution only) ===")
    log(f"  WR:    {wr:.1f}%")
    log(f"  ROI:   {roi:+.1f}%")
    log(f"  PnL:   ${pnl_total:+,.0f}")
    log(f"  Stake: ${stake:,.0f}")

    # ── Arb fingerprints ─────────────────────────────────────────────
    log(f"\n=== ARB / SCALP FINGERPRINTS ===")
    log(f"  Scalp ratio:  {scalp_ratio:.1f}%  ({len(scalped)}/{total_exits} exits)")
    log(f"  Merge ratio:  {merge_ratio:.1f}%  ({len(merged)}/{total_exits} exits)")
    log(f"  Dual-side %:  {dual_pct:.1f}%     ({n_dual}/{len(cids)} markets had both outcomes bought)")

    # ── Entry price distribution ─────────────────────────────────────
    log(f"\n=== ENTRY PRICE DISTRIBUTION ===")
    buckets = [
        (0, 0.10, "longshot <$0.10"),
        (0.10, 0.25, "deep dog"),
        (0.25, 0.50, "dog"),
        (0.50, 0.75, "fav"),
        (0.75, 0.90, "heavy fav"),
        (0.90, 1.01, "CHALK >=$0.90"),
    ]
    for lo, hi, name in buckets:
        sub = [r for r in resolved if lo <= r["entry"] < hi]
        if not sub: continue
        sw = sum(1 for r in sub if r["won"])
        sst = sum(r["stake"] for r in sub)
        spn = sum(r["pnl"] for r in sub)
        sro = spn / sst * 100 if sst else 0
        log(f"  {name:18s} {sw:>3}W/{len(sub) - sw:>3}L  WR {sw / len(sub) * 100:>5.1f}%  "
            f"stake ${sst:>9,.0f}  PnL ${spn:>+9,.0f}  ROI {sro:>+6.1f}%")

    chalk = [r for r in resolved if r["entry"] >= 0.90]
    chalk_pct = len(chalk) / len(resolved) * 100

    # ── By subtype ───────────────────────────────────────────────────
    log(f"\n=== BY MARKET SUBTYPE ===")
    by_sub = defaultdict(lambda: {"w": 0, "l": 0, "stake": 0, "pnl": 0})
    for r in resolved:
        b = by_sub[r["subtype"]]
        if r["won"]: b["w"] += 1
        else:       b["l"] += 1
        b["stake"] += r["stake"]; b["pnl"] += r["pnl"]
    for sub, b in sorted(by_sub.items(), key=lambda kv: -kv[1]["stake"]):
        n = b["w"] + b["l"]
        if n == 0: continue
        log(f"  {sub:12s} {b['w']:>3}W/{b['l']:>3}L  WR {b['w'] / n * 100:>5.1f}%  "
            f"stake ${b['stake']:>9,.0f}  PnL ${b['pnl']:>+9,.0f}")

    # ── Verdict ──────────────────────────────────────────────────────
    log(f"\n{'=' * 60}")
    log(f"VERDICT")
    log(f"{'=' * 60}")
    reasons = []
    if merge_ratio > 20:
        reasons.append(f"MERGE-heavy ({merge_ratio:.0f}% of exits) = arb bot")
    if scalp_ratio > 40:
        reasons.append(f"scalp-heavy ({scalp_ratio:.0f}% of exits) = survivor bias")
    if dual_pct > 35:
        reasons.append(f"dual-side trading ({dual_pct:.0f}% of markets) = hedge/arb")
    if chalk_pct > 40:
        reasons.append(f"chalk-heavy ({chalk_pct:.0f}% of bets at >=$0.90)")

    if reasons:
        log(f"  [REJECT] — {'; '.join(reasons)}")
    elif wr >= 60 and roi >= 15 and len(resolved) >= 20:
        log(f"  [PROMOTE] — WR {wr:.1f}% / ROI {roi:+.1f}% on {len(resolved)} bets, clean fingerprint")
    elif wr >= 55 and roi >= 5 and len(resolved) >= 20:
        log(f"  [SHADOW] — WR {wr:.1f}% / ROI {roi:+.1f}% cautious positive, collect 1-2wk more")
    elif len(resolved) < 20:
        log(f"  [SHADOW] — only {len(resolved)} resolved bets, too small to promote; shadow-log first")
    elif wr < 50 or roi < 0:
        log(f"  [REJECT] — WR {wr:.1f}% / ROI {roi:+.1f}% is net-losing directional")
    else:
        log(f"  [SHADOW] — WR {wr:.1f}% / ROI {roi:+.1f}% marginal, collect shadow data")


async def main():
    if len(sys.argv) != 3:
        print("Usage: fingerprint_whale.py ALIAS WALLET", file=sys.stderr)
        sys.exit(2)
    alias, wallet = sys.argv[1], sys.argv[2]
    await fingerprint(alias, wallet)


if __name__ == "__main__":
    asyncio.run(main())
