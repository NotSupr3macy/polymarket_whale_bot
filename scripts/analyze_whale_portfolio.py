#!/usr/bin/env python3
"""Portfolio optimization analysis for the paper trader.

Answers:
  1. What's the optimal subset of whales to tail?
  2. What's the effective trades-per-day rate for each subset?
  3. What trade-count cap (if any) would have improved results?

Uses all resolved paper_positions as the empirical sample. Pools
stats per whale, then enumerates every non-empty subset (2^N-1 combos
for N whales) and ranks by:
  - Total PnL
  - Sharpe-like ratio (PnL / stdev of daily returns)
  - Max drawdown
  - Trades per day (throughput)

Usage:
    python3 scripts/analyze_whale_portfolio.py
    python3 scripts/analyze_whale_portfolio.py --top 15
"""
import argparse
import os
import sqlite3
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path

DB_PATH = os.getenv(
    "DB_PATH",
    str(Path(__file__).resolve().parent.parent / "trades.db"),
)


def load_resolved(conn):
    rows = conn.execute(
        """SELECT whale_alias, paper_pnl, paper_size_usd, outcome,
                  opened_at, resolved_at
           FROM paper_positions
           WHERE outcome IN ('WIN','LOSS')
           ORDER BY resolved_at"""
    ).fetchall()
    out = []
    for r in rows:
        out.append({
            "whale": r[0],
            "pnl": float(r[1] or 0),
            "size": float(r[2] or 0),
            "outcome": r[3],
            "opened": r[4],
            "resolved": r[5],
        })
    return out


def per_whale_stats(positions):
    by_whale = defaultdict(list)
    for p in positions:
        by_whale[p["whale"]].append(p)
    out = {}
    for whale, ps in by_whale.items():
        w = sum(1 for p in ps if p["outcome"] == "WIN")
        l = sum(1 for p in ps if p["outcome"] == "LOSS")
        pnl = sum(p["pnl"] for p in ps)
        stake = sum(p["size"] for p in ps)
        out[whale] = {
            "trades": len(ps),
            "wins": w,
            "losses": l,
            "wr": w / len(ps) * 100 if ps else 0,
            "pnl": pnl,
            "stake": stake,
            "roi": pnl / stake * 100 if stake else 0,
            "positions": ps,
        }
    return out


def subset_metrics(positions, subset):
    """Simulate a portfolio that only trades the given subset of whales."""
    filtered = [p for p in positions if p["whale"] in subset]
    if not filtered:
        return None

    # Time-ordered cumulative PnL → max drawdown
    filtered.sort(key=lambda p: p["resolved"])
    running = 0.0
    peak = 0.0
    max_dd = 0.0
    daily = defaultdict(float)
    for p in filtered:
        running += p["pnl"]
        peak = max(peak, running)
        dd = peak - running
        max_dd = max(max_dd, dd)
        day = p["resolved"][:10]  # YYYY-MM-DD
        daily[day] += p["pnl"]

    # Sample std dev of daily P&L (or 0 if ≤1 day)
    daily_pnls = list(daily.values())
    try:
        stdev = statistics.stdev(daily_pnls) if len(daily_pnls) > 1 else 0.0
    except statistics.StatisticsError:
        stdev = 0.0

    total_pnl = running
    total_w = sum(1 for p in filtered if p["outcome"] == "WIN")
    total_l = sum(1 for p in filtered if p["outcome"] == "LOSS")
    total_stake = sum(p["size"] for p in filtered)

    # Trades per day
    n_days = len(daily) if daily else 1
    tpd = len(filtered) / n_days

    # Sharpe-like: mean daily pnl / stdev (higher = better risk-adjusted)
    mean_daily = total_pnl / n_days if n_days else 0
    sharpe = mean_daily / stdev if stdev > 0 else float('inf') if mean_daily > 0 else 0

    return {
        "subset": subset,
        "trades": len(filtered),
        "wins": total_w,
        "losses": total_l,
        "wr": total_w / len(filtered) * 100 if filtered else 0,
        "pnl": total_pnl,
        "stake": total_stake,
        "roi": total_pnl / total_stake * 100 if total_stake else 0,
        "max_dd": max_dd,
        "n_days": n_days,
        "trades_per_day": tpd,
        "daily_stdev": stdev,
        "sharpe": sharpe,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--top", type=int, default=10,
                    help="Top N subsets to show per ranking metric")
    args = ap.parse_args()

    conn = sqlite3.connect(DB_PATH)
    positions = load_resolved(conn)
    if not positions:
        print("No resolved positions in paper_positions — nothing to analyze.")
        return

    print(f"Sample: {len(positions)} resolved paper positions\n")

    whales = per_whale_stats(positions)

    # ── Per-whale summary ────────────────────────────────────────────
    print("=" * 95)
    print(f"{'WHALE':<22} {'TRADES':>7} {'W/L':>8} {'WR':>7} "
          f"{'PNL':>10} {'STAKE':>10} {'ROI':>8}")
    print("-" * 95)
    for whale, s in sorted(whales.items(), key=lambda kv: -kv[1]["pnl"]):
        print(f"{whale:<22} {s['trades']:>7} "
              f"{s['wins']:>3}/{s['losses']:<3}  {s['wr']:>6.1f}%  "
              f"${s['pnl']:>+8.2f}  ${s['stake']:>8.2f}  {s['roi']:>+6.1f}%")
    print("-" * 95)

    total_pnl = sum(s["pnl"] for s in whales.values())
    total_trades = sum(s["trades"] for s in whales.values())
    total_stake = sum(s["stake"] for s in whales.values())
    print(f"{'TOTAL':<22} {total_trades:>7} "
          f"{sum(s['wins'] for s in whales.values()):>3}/"
          f"{sum(s['losses'] for s in whales.values()):<3}  "
          f"          ${total_pnl:>+8.2f}  ${total_stake:>8.2f}  "
          f"{total_pnl/total_stake*100 if total_stake else 0:>+6.1f}%")

    # ── All subsets: enumerate and rank ──────────────────────────────
    whale_names = sorted(whales.keys())
    all_subsets = []
    for k in range(1, len(whale_names) + 1):
        for combo in combinations(whale_names, k):
            m = subset_metrics(positions, set(combo))
            if m:
                all_subsets.append(m)
    print(f"\nEvaluated {len(all_subsets)} non-empty whale subsets "
          f"(up to {len(whale_names)} whales)\n")

    # ── Top by PnL ───────────────────────────────────────────────────
    def short(subset):
        return ",".join(sorted(w[:4] for w in subset))

    print("=" * 100)
    print(f"TOP {args.top} SUBSETS BY TOTAL PnL")
    print("=" * 100)
    print(f"{'SIZE':>4}  {'SUBSET':<40} {'TRADES':>7} {'WR':>6} "
          f"{'PNL':>10} {'ROI':>7} {'MAXDD':>8} {'T/DAY':>6}")
    for m in sorted(all_subsets, key=lambda x: -x["pnl"])[:args.top]:
        print(f"{len(m['subset']):>4}  {short(m['subset']):<40} "
              f"{m['trades']:>7} {m['wr']:>5.1f}% ${m['pnl']:>+8.2f} "
              f"{m['roi']:>+6.1f}% ${m['max_dd']:>6.2f} "
              f"{m['trades_per_day']:>5.1f}")

    # ── Top by ROI (filtered to ≥5 trades to avoid noise) ────────────
    min_trades = 5
    meaningful = [m for m in all_subsets if m["trades"] >= min_trades]
    print(f"\nTOP {args.top} SUBSETS BY ROI (≥{min_trades} trades)")
    print("=" * 100)
    for m in sorted(meaningful, key=lambda x: -x["roi"])[:args.top]:
        print(f"{len(m['subset']):>4}  {short(m['subset']):<40} "
              f"{m['trades']:>7} {m['wr']:>5.1f}% ${m['pnl']:>+8.2f} "
              f"{m['roi']:>+6.1f}% ${m['max_dd']:>6.2f} "
              f"{m['trades_per_day']:>5.1f}")

    # ── Best subset per size class ───────────────────────────────────
    print(f"\nBEST SUBSET AT EACH SIZE (by PnL)")
    print("=" * 100)
    print(f"{'SIZE':>4}  {'SUBSET':<40} {'TRADES':>7} {'WR':>6} "
          f"{'PNL':>10} {'ROI':>7} {'MAXDD':>8} {'T/DAY':>6}")
    for k in range(1, len(whale_names) + 1):
        subsets_k = [m for m in all_subsets if len(m["subset"]) == k]
        if not subsets_k:
            continue
        best = max(subsets_k, key=lambda m: m["pnl"])
        print(f"{k:>4}  {short(best['subset']):<40} "
              f"{best['trades']:>7} {best['wr']:>5.1f}% "
              f"${best['pnl']:>+8.2f} {best['roi']:>+6.1f}% "
              f"${best['max_dd']:>6.2f} {best['trades_per_day']:>5.1f}")

    # ── Trades per day: correlation with PnL? ────────────────────────
    print(f"\nTRADES/DAY vs PROFIT (are high-volume subsets more profitable?)")
    print("=" * 80)
    buckets = [
        (0, 2, "low <2/day"),
        (2, 5, "med 2-5/day"),
        (5, 10, "high 5-10/day"),
        (10, 9999, "very high 10+/day"),
    ]
    for lo, hi, name in buckets:
        sub = [m for m in all_subsets if lo <= m["trades_per_day"] < hi
               and m["trades"] >= min_trades]
        if not sub: continue
        avg_pnl = sum(m["pnl"] for m in sub) / len(sub)
        avg_roi = sum(m["roi"] for m in sub) / len(sub)
        profitable = sum(1 for m in sub if m["pnl"] > 0)
        print(f"  {name:18s}  {len(sub):>4} subsets, {profitable} profitable, "
              f"avg PnL ${avg_pnl:+.2f}, avg ROI {avg_roi:+.1f}%")

    # ── Size class: is more whales always better? ────────────────────
    print(f"\nPORTFOLIO SIZE vs PROFIT")
    print("=" * 80)
    print(f"{'SIZE':>4}  {'SUBSETS':>7}  {'% PROFITABLE':>12}  "
          f"{'AVG PnL':>10}  {'BEST PnL':>10}  {'AVG DRAWDOWN':>14}")
    for k in range(1, len(whale_names) + 1):
        subs = [m for m in all_subsets if len(m["subset"]) == k]
        if not subs: continue
        profitable = sum(1 for m in subs if m["pnl"] > 0)
        avg_pnl = sum(m["pnl"] for m in subs) / len(subs)
        best_pnl = max(m["pnl"] for m in subs)
        avg_dd = sum(m["max_dd"] for m in subs) / len(subs)
        print(f"{k:>4}  {len(subs):>7}  "
              f"{profitable / len(subs) * 100:>11.1f}%  "
              f"${avg_pnl:>+8.2f}  ${best_pnl:>+8.2f}  ${avg_dd:>+11.2f}")

    print()
    conn.close()


if __name__ == "__main__":
    main()
