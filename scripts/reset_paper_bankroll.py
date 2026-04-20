#!/usr/bin/env python3
"""Reset the paper trader back to a fresh $100 bankroll while preserving
all historical resolved positions (for analysis).

What it does:
  - Closes any OPEN positions to RESOLVED (break-even, refunds stake)
  - Sets paper_state.bankroll_usd = $100.00
  - Sets paper_state.started_at = NOW so pre-existing whale-tracker rows
    are not retro-opened as paper positions on the next tick
  - Keeps all historical paper_positions rows intact (for
    scripts/analyze_whale_portfolio.py and verify_paper_resolutions.py)
  - Resets paper_state.next_update_ts to +6h from now

Default is DRY RUN. Pass --apply to commit.

Usage:
    python3 scripts/reset_paper_bankroll.py
    python3 scripts/reset_paper_bankroll.py --apply
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
    args = ap.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Current state
    row = conn.execute(
        "SELECT bankroll_usd, started_at, next_update_ts FROM paper_state WHERE id=1"
    ).fetchone()
    if not row:
        print("ERROR: paper_state row missing. Run paper_trader.init_db() first.")
        return
    cur_bankroll = float(row["bankroll_usd"])
    cur_started = row["started_at"]
    cur_next_upd = row["next_update_ts"]

    # Open positions
    open_rows = conn.execute(
        """SELECT id, whale_alias, market_title, direction,
                  entry_price, paper_size_usd
           FROM paper_positions
           WHERE outcome='OPEN'"""
    ).fetchall()
    open_stake = sum(float(r["paper_size_usd"]) for r in open_rows)

    # Historical count (for safekeeping confirmation)
    hist = conn.execute(
        """SELECT outcome, COUNT(*) AS n
           FROM paper_positions
           WHERE outcome IN ('WIN','LOSS','RESOLVED')
           GROUP BY outcome"""
    ).fetchall()
    hist_summary = ", ".join(f"{r['n']} {r['outcome']}" for r in hist)

    now = datetime.now(timezone.utc)
    new_started = now.isoformat()
    new_next_upd = (now + timedelta(hours=6)).isoformat()

    print(f"{'='*70}")
    print(f"PAPER TRADER RESET")
    print(f"{'='*70}")
    print(f"Current state:")
    print(f"  Bankroll:      ${cur_bankroll:.2f}")
    print(f"  Started at:    {cur_started}")
    print(f"  Next update:   {cur_next_upd}")
    print(f"  Open positions: {len(open_rows)} (${open_stake:.2f} deployed)")
    if open_rows:
        print(f"  Will force-close to RESOLVED break-even:")
        for r in open_rows:
            print(f"    id={r['id']:3d}  {r['whale_alias']:20s} "
                  f"{r['direction']:20s} @ ${r['entry_price']:.3f}  "
                  f"size=${r['paper_size_usd']:.2f}")
    print(f"  Historical resolved positions preserved: {hist_summary}")
    print()
    print(f"After reset:")
    print(f"  Bankroll:      $100.00")
    print(f"  Started at:    {new_started}")
    print(f"  Next update:   {new_next_upd}")
    print(f"  Open positions: 0")
    print()

    if not args.apply:
        print("[DRY RUN] Use --apply to commit the reset.")
        conn.close()
        return

    # Close open positions as RESOLVED break-even. Do NOT delete; keeps the
    # history. resolution_price = current whale-tracker price if available,
    # else the entry price (doesn't matter for break-even PnL=0).
    now_iso = now.isoformat()
    for r in open_rows:
        conn.execute(
            """UPDATE paper_positions
               SET outcome='RESOLVED', resolved_at=?,
                   paper_pnl=0.0, resolution_price=entry_price
               WHERE id=?""",
            (now_iso, r["id"]),
        )

    conn.execute(
        """UPDATE paper_state
           SET bankroll_usd=100.0,
               started_at=?,
               next_update_ts=?,
               last_update_ts=NULL
           WHERE id=1""",
        (new_started, new_next_upd),
    )
    conn.commit()
    conn.close()
    print(f"[APPLIED] Bankroll reset to $100.00. "
          f"{len(open_rows)} open positions closed to RESOLVED break-even.")
    print(f"Historical paper_positions preserved for analytics.")


if __name__ == "__main__":
    main()
