#!/usr/bin/env python3
"""Surgical reset: wipe paper_positions opened before a cutoff date,
keep today's activity, and recompute bankroll from $100 starting point.

Useful after strategy overhauls when we want a clean slate but DON'T want
to lose today's in-flight positions.

Logic:
  - DELETE paper_positions WHERE opened_at < cutoff
  - Recompute bankroll as:
        $100.00
      − sum(paper_size_usd for OPEN positions)          # still deployed
      − sum(paper_size_usd for LOSS positions)           # stake consumed
      + sum(paper_pnl for WIN positions)                 # pnl only; stake returned
      + 0   for RESOLVED positions                       # stake returned, $0 pnl
  - paper_state.started_at = cutoff (so pre-cutoff tracker rows don't
    retro-trigger on the next tick)
  - Reset 6h update timer

Default: dry-run. Pass --apply to commit.

Usage:
    python3 scripts/reset_to_today.py                            # dry-run, cutoff=today UTC 00:00
    python3 scripts/reset_to_today.py --apply
    python3 scripts/reset_to_today.py --cutoff 2026-04-20 --apply
"""
import argparse
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

DB_PATH = os.getenv(
    "DB_PATH",
    str(Path(__file__).resolve().parent.parent / "trades.db"),
)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Actually commit the reset (default: dry run)")
    ap.add_argument("--cutoff",
                    default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    help="YYYY-MM-DD: delete positions opened before this. "
                         "Defaults to today (UTC).")
    ap.add_argument("--starting-bankroll", type=float, default=100.0,
                    help="Starting bankroll to anchor math (default $100)")
    args = ap.parse_args()

    cutoff_iso = f"{args.cutoff}T00:00:00+00:00"
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Rows we'll DELETE (opened before cutoff)
    doomed = conn.execute(
        """SELECT id, whale_alias, market_title, direction, outcome,
                  paper_size_usd, paper_pnl, opened_at
           FROM paper_positions
           WHERE opened_at < ?
           ORDER BY opened_at""",
        (cutoff_iso,),
    ).fetchall()

    # Rows we'll KEEP (opened on/after cutoff)
    kept = conn.execute(
        """SELECT id, whale_alias, market_title, direction, outcome,
                  paper_size_usd, paper_pnl, opened_at
           FROM paper_positions
           WHERE opened_at >= ?
           ORDER BY opened_at""",
        (cutoff_iso,),
    ).fetchall()

    # Compute new bankroll from kept positions
    new_bankroll = args.starting_bankroll
    for r in kept:
        size = float(r["paper_size_usd"] or 0)
        pnl = float(r["paper_pnl"] or 0)
        if r["outcome"] == "OPEN":
            new_bankroll -= size
        elif r["outcome"] == "WIN":
            new_bankroll += pnl
        elif r["outcome"] == "LOSS":
            new_bankroll -= size
        # RESOLVED: net zero

    cur_state = conn.execute(
        "SELECT bankroll_usd, started_at FROM paper_state WHERE id=1"
    ).fetchone()

    # ── Report ─────────────────────────────────────────────────────
    print(f"{'='*70}")
    print(f"RESET TO TODAY")
    print(f"{'='*70}")
    print(f"Cutoff:           {cutoff_iso}")
    print(f"Starting bankroll: ${args.starting_bankroll:.2f}")
    print()
    print(f"Current state:")
    print(f"  Bankroll:      ${float(cur_state['bankroll_usd']):.2f}")
    print(f"  Started at:    {cur_state['started_at']}")
    print()
    print(f"Positions to DELETE (opened before cutoff): {len(doomed)}")
    w_del = sum(1 for r in doomed if r["outcome"] == "WIN")
    l_del = sum(1 for r in doomed if r["outcome"] == "LOSS")
    r_del = sum(1 for r in doomed if r["outcome"] == "RESOLVED")
    o_del = sum(1 for r in doomed if r["outcome"] == "OPEN")
    print(f"  By outcome: {w_del} WIN, {l_del} LOSS, "
          f"{r_del} RESOLVED, {o_del} OPEN")
    print()
    print(f"Positions to KEEP (opened on/after cutoff): {len(kept)}")
    if kept:
        print(f"{'':4}{'whale':22s} {'outcome':8s} {'side':20s} "
              f"{'entry':>6s} {'size':>6s} {'pnl':>7s}  market")
        for r in kept:
            print(f"    {r['whale_alias']:22s} {r['outcome']:8s} "
                  f"{r['direction'][:20]:20s} "
                  f"${float(r['paper_size_usd'] or 0):.2f}  "
                  f"${float(r['paper_pnl'] or 0):+.2f}  "
                  f"{(r['market_title'] or '')[:40]}")
    print()
    print(f"Computed new bankroll: ${new_bankroll:.2f}")
    print()

    if not args.apply:
        print(f"[DRY RUN] Use --apply to commit. Will delete {len(doomed)} "
              f"rows and set bankroll=${new_bankroll:.2f}, "
              f"started_at={cutoff_iso}")
        conn.close()
        return

    # ── Commit ─────────────────────────────────────────────────────
    conn.execute("DELETE FROM paper_positions WHERE opened_at < ?",
                 (cutoff_iso,))
    now = datetime.now(timezone.utc)
    next_upd = (now + timedelta(hours=6)).isoformat()
    conn.execute(
        """UPDATE paper_state
           SET bankroll_usd=?,
               started_at=?,
               next_update_ts=?,
               last_update_ts=NULL
           WHERE id=1""",
        (new_bankroll, cutoff_iso, next_upd),
    )
    conn.commit()
    conn.close()
    print(f"[APPLIED] Deleted {len(doomed)} pre-cutoff rows. "
          f"Bankroll set to ${new_bankroll:.2f}. "
          f"started_at={cutoff_iso}. {len(kept)} kept positions preserved.")


if __name__ == "__main__":
    main()
