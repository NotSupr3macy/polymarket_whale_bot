#!/usr/bin/env python3
"""Prune the shadow candidate pool based on forward-tracked performance.

Reads `monitor/shadow_candidates.json` + `shadow_trades` table. Flips
the `status` on underperforming whales from "shadowing" to "rejected"
so `whale_shadow.py` stops tracking them on its next load.

Rejection criteria (ANY of these triggers):
  1. resolved >= 15 AND roi <= -15%     — hard loser with enough signal
  2. resolved >= 100 AND roi < 0        — deep sample, negative edge
  3. resolved >= 30 AND wr < 40%        — poor WR over meaningful sample

Default is DRY RUN (prints what would change without writing). Pass
`--apply` to actually rewrite shadow_candidates.json (with backup).

Usage:
    python3 scripts/prune_shadow_pool.py               # dry run
    python3 scripts/prune_shadow_pool.py --apply       # write changes
    python3 scripts/prune_shadow_pool.py --db PATH     # override DB path
"""
from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_JSON = (
    Path(__file__).resolve().parent.parent / "monitor" / "shadow_candidates.json"
)
DEFAULT_DB = Path(__file__).resolve().parent.parent / "trades.db"

# Rejection thresholds — tuned to match today's data:
#   Dechamfraud (18 resolved, -29.8% ROI) → rule 1
#   bossoskil1  (11 resolved, -88.7% ROI) → borderline, let it survive this round
#     ↑ note: 11 < 15, so it would NOT be rejected under rule 1. Intentional —
#       11 resolved is too thin. Rule 1 picks up Dechamfraud, Cannae, ic4cream,
#       swisstony. For bossoskil1 we need rule 1 with a lower threshold, OR
#       wait for more data. Defaulting conservative for safety.
#
#   Cannae       (233 resolved, -17.3% ROI) → rule 1 AND rule 2
#   swisstony    (546 resolved, -14.9% ROI) → rule 1 AND rule 2
#   ic4cream     (88 resolved, -18.5% ROI)  → rule 1 only
#   PeterDeboerCancerPatient (18 resolved, -6.7% ROI) → only slightly negative; keep
#
# NOTE: With shadow tracker dead since Apr 13, rule thresholds are based on
# 3-day forward data — small sample. Revisit after shadow tracker revived
# and we have 7+ days of data.
MIN_RESOLVED_FOR_ROI_REJECT = 15
ROI_REJECT_THRESHOLD = -15.0  # %

MIN_RESOLVED_FOR_DEEP_SAMPLE_REJECT = 100
# Tightened from 0% to -5%: a large-sample whale at -0.7% ROI is break-even
# with variance noise, not necessarily a bleeder. Our paper trader's filter
# may well turn such a whale profitable. Only reject when the deep-sample
# ROI is clearly negative enough that no filter will save them.
DEEP_SAMPLE_ROI_THRESHOLD = -5.0  # %

MIN_RESOLVED_FOR_WR_REJECT = 30
WR_REJECT_THRESHOLD = 40.0  # %

# Protected whales — never auto-reject. These are in the paper trader's
# active lineup (monitor/paper_trader.py :: BASE_ALLOC) or otherwise
# manually designated as core. Removing one should require editing this
# list deliberately, not a statistical auto-prune.
PROTECTED_ALIASES = {
    "texaskid",
    "kch123",
    "TheOnlyHuman",
    "GamblingIsAllYouNeed",
    "nbasniper",
    "bigsix",
}


def compute_performance(conn: sqlite3.Connection) -> dict[str, dict]:
    """Return per-wallet shadow performance stats.

    Keyed by wallet (so we can join to shadow_candidates.json).
    Returns {} if the shadow_trades table doesn't exist yet
    (e.g. on a machine where the shadow tracker has never run).
    """
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='shadow_trades'"
    ).fetchone()
    if not exists:
        print("WARN: shadow_trades table does not exist in this DB — "
              "can't evaluate performance. Run this on the server.",
              file=sys.stderr)
        return {}
    rows = conn.execute(
        """
        SELECT
          wallet,
          alias,
          SUM(CASE WHEN status='closed' THEN 1 ELSE 0 END) AS resolved,
          SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) AS w,
          SUM(CASE WHEN outcome='LOSS' THEN 1 ELSE 0 END) AS l,
          SUM(COALESCE(pnl, 0)) AS total_pnl,
          SUM(entry_size_usd) AS total_stake,
          MAX(entry_time) AS last_trade
        FROM shadow_trades
        GROUP BY wallet, alias
        """
    ).fetchall()
    out = {}
    for row in rows:
        wallet, alias, resolved, w, l, pnl, stake, last = row
        resolved = int(resolved or 0)
        w = int(w or 0); l = int(l or 0)
        pnl = float(pnl or 0); stake = float(stake or 0)
        decided = w + l
        wr = (w / decided * 100) if decided else 0.0
        roi = (pnl / stake * 100) if stake else 0.0
        out[wallet] = {
            "alias": alias, "resolved": resolved, "w": w, "l": l,
            "wr": wr, "pnl": pnl, "stake": stake, "roi": roi,
            "last_trade": last,
        }
    return out


def decide_action(alias: str, perf: dict) -> tuple[str, str]:
    """Return (action, reason). action in {'keep', 'reject'}."""
    # Protected-whale hard override: never auto-reject paper lineup members.
    if alias in PROTECTED_ALIASES:
        return "keep", "protected (in paper trader lineup)"

    resolved = perf["resolved"]
    roi = perf["roi"]
    wr = perf["wr"]

    # Rule 1: meaningful sample + clear ROI loss
    if resolved >= MIN_RESOLVED_FOR_ROI_REJECT and roi <= ROI_REJECT_THRESHOLD:
        return "reject", f"rule-1: resolved={resolved} ROI={roi:.1f}% <= {ROI_REJECT_THRESHOLD}%"

    # Rule 2: deep sample + clearly negative ROI (-5% threshold)
    if resolved >= MIN_RESOLVED_FOR_DEEP_SAMPLE_REJECT and roi < DEEP_SAMPLE_ROI_THRESHOLD:
        return "reject", f"rule-2: resolved={resolved} ROI={roi:.1f}% < {DEEP_SAMPLE_ROI_THRESHOLD}% (deep sample)"

    # Rule 3: meaningful sample + very poor WR
    if resolved >= MIN_RESOLVED_FOR_WR_REJECT and wr < WR_REJECT_THRESHOLD:
        return "reject", f"rule-3: resolved={resolved} WR={wr:.1f}% < {WR_REJECT_THRESHOLD}%"

    return "keep", ""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="Actually write changes (default: dry run)")
    parser.add_argument("--json", default=str(DEFAULT_JSON),
                        help=f"Path to shadow_candidates.json (default: {DEFAULT_JSON})")
    parser.add_argument("--db", default=str(DEFAULT_DB),
                        help=f"Path to trades.db (default: {DEFAULT_DB})")
    args = parser.parse_args()

    json_path = Path(args.json)
    db_path = Path(args.db)

    if not json_path.exists():
        print(f"ERROR: {json_path} not found", file=sys.stderr)
        return 1
    if not db_path.exists():
        print(f"ERROR: {db_path} not found", file=sys.stderr)
        return 1

    with open(json_path) as f:
        candidates = json.load(f)

    conn = sqlite3.connect(db_path)
    try:
        perf_by_wallet = compute_performance(conn)
    finally:
        conn.close()

    print(f"Loaded {len(candidates)} candidates from {json_path.name}")
    print(f"Found shadow trading data for {len(perf_by_wallet)} wallets")
    print()

    # Evaluate each candidate
    actions = []
    for wallet, cand in candidates.items():
        alias = cand.get("alias", wallet[:10])
        current_status = cand.get("status")
        perf = perf_by_wallet.get(wallet)

        if current_status != "shadowing":
            # Already promoted or already rejected — skip
            actions.append((wallet, alias, current_status, "skip-not-shadowing", ""))
            continue

        if not perf or perf["resolved"] == 0:
            actions.append((wallet, alias, "shadowing", "keep-no-data", ""))
            continue

        action, reason = decide_action(alias, perf)
        if action == "reject":
            actions.append((wallet, alias, "->rejected", reason, perf))
        else:
            actions.append((wallet, alias, "keep-shadowing", "", perf))

    # Report
    print(f"{'ALIAS':<28} {'STATUS':<20} {'RES':>5} {'WR':>7} {'ROI':>8} {'PNL':>11}  REASON")
    print("-" * 120)
    for wallet, alias, status, reason, perf in actions:
        if isinstance(perf, dict):
            print(f"{alias:<28} {status:<20} {perf['resolved']:>5} "
                  f"{perf['wr']:>6.1f}% {perf['roi']:>7.1f}% {perf['pnl']:>+10,.0f}  {reason}")
        else:
            print(f"{alias:<28} {status:<20} {'--':>5} {'--':>7} {'--':>8} {'--':>11}  {reason}")

    to_reject = [(w, a, p) for w, a, s, r, p in actions if s == "->rejected"]
    print(f"\nWould reject: {len(to_reject)} whales")
    total_pnl_cut = sum(p["pnl"] for _, _, p in to_reject if isinstance(p, dict))
    total_stake_cut = sum(p["stake"] for _, _, p in to_reject if isinstance(p, dict))
    if to_reject:
        print(f"Total realized PnL saved: ${total_pnl_cut:+,.0f} "
              f"(had been ${total_stake_cut:,.0f} in simulated stake)")

    if not args.apply:
        print("\n[DRY RUN] Use --apply to write changes.")
        return 0

    # Write changes
    backup = json_path.with_suffix(
        f".json.bak.{datetime.now(timezone.utc):%Y%m%d%H%M%S}"
    )
    shutil.copy2(json_path, backup)
    print(f"\nBacked up original to {backup}")

    for wallet, alias, perf in to_reject:
        candidates[wallet]["status"] = "rejected"
        candidates[wallet]["rejected_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if isinstance(perf, dict):
            candidates[wallet]["rejection_reason"] = (
                f"auto-prune: resolved={perf['resolved']} "
                f"WR={perf['wr']:.1f}% ROI={perf['roi']:.1f}% "
                f"PnL=${perf['pnl']:+,.0f}"
            )

    tmp = json_path.with_suffix(".json.tmp")
    with open(tmp, "w") as f:
        json.dump(candidates, f, indent=2, sort_keys=False)
    tmp.replace(json_path)
    print(f"Wrote {len(to_reject)} rejections to {json_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
