"""
Collects bot health data for the monitoring agent.

Reads from the database and log files — never touches the running bot.
Output: monitor/reports/report_YYYY-MM-DD.json
"""

from __future__ import annotations

import glob
import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _aware(dt_str: str) -> datetime:
    """Parse ISO datetime string and ensure it's timezone-aware (UTC)."""
    dt = datetime.fromisoformat(dt_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def collect() -> str:
    """Gather all bot data into a structured JSON report. Returns report path."""

    report: dict = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "period": "last_24h",
    }

    # ── Trade Journal Data ─────────────────────────────────────────
    db_path = os.getenv("DB_PATH", "trades.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # All closed trades (overall stats)
    all_closed = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM trades_dry WHERE status=? ORDER BY entry_time",
            ("closed",),
        ).fetchall()
    ]

    # Recent closed (last 24h)
    yesterday = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    recent_closed = [t for t in all_closed if (t.get("exit_time") or "") > yesterday]

    # Open positions
    open_positions = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM trades_dry WHERE status=? ORDER BY entry_time",
            ("open",),
        ).fetchall()
    ]

    # Stale positions (open > 48 hours)
    cutoff_48h = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
    stale_positions = [p for p in open_positions if p["entry_time"] < cutoff_48h]

    conn.close()

    # ── Per-Whale Stats ────────────────────────────────────────────
    whale_stats: dict = {}
    for t in all_closed:
        whales = json.loads(t.get("whale_signals", "[]"))
        for w in whales:
            if w not in whale_stats:
                whale_stats[w] = {"trades": 0, "wins": 0, "pnl": 0.0, "losses": []}
            whale_stats[w]["trades"] += 1
            pnl = t.get("pnl") or 0
            if t.get("outcome") == "WIN" and pnl > 0:
                whale_stats[w]["wins"] += 1
            whale_stats[w]["pnl"] += pnl
            if pnl < 0:
                whale_stats[w]["losses"].append(
                    {
                        "market": t.get("market_title", ""),
                        "pnl": pnl,
                        "reason": t.get("failure_reason", ""),
                    }
                )

    # ── Resolution Health ──────────────────────────────────────────
    resolution_stats = {
        "total_resolved": len(
            [t for t in all_closed if t.get("exit_reason") == "resolution"]
        ),
        "total_stopped": len(
            [t for t in all_closed if t.get("exit_reason") == "stop_loss"]
        ),
        "stopped_at_near_zero": len(
            [
                t
                for t in all_closed
                if t.get("exit_reason") == "stop_loss"
                and t.get("exit_price") is not None
                and t["exit_price"] < 0.01
            ]
        ),
        "stale_positions": [
            {
                "id": p["id"],
                "direction": p["direction"],
                "market_title": p.get("market_title", ""),
                "market_id": p["market_id"],
                "condition_id": p.get("condition_id", ""),
                "entry_time": p["entry_time"],
                "hours_open": round(
                    (
                        datetime.now(timezone.utc)
                        - _aware(p["entry_time"])
                    ).total_seconds()
                    / 3600,
                    1,
                ),
                "stop_loss_enabled": p.get("stop_price") is not None and p.get("stop_price", 0) > 0,
                "stop_price": p.get("stop_price"),
            }
            for p in stale_positions
        ],
    }

    # ── Log Health ─────────────────────────────────────────────────
    log_files = sorted(glob.glob("logs/bot*.log"))
    log_health: dict = {"poll_errors": 0, "dns_errors": 0, "total_lines": 0}
    if log_files:
        latest_log = log_files[-1]
        try:
            with open(latest_log, "r", errors="ignore") as f:
                for line in f:
                    log_health["total_lines"] += 1
                    if "Poll error" in line:
                        log_health["poll_errors"] += 1
                    if "DNS" in line or "getaddrinfo" in line:
                        log_health["dns_errors"] += 1
        except OSError:
            pass

    # ── Assemble Report ────────────────────────────────────────────
    total_trades = len(all_closed)
    wins = len(
        [t for t in all_closed if t.get("outcome") == "WIN" and (t.get("pnl") or 0) > 0]
    )

    report["summary"] = {
        "total_closed_trades": total_trades,
        "wins": wins,
        "losses": total_trades - wins,
        "win_rate": round(wins / total_trades, 3) if total_trades > 0 else 0,
        "total_pnl": round(sum(t.get("pnl") or 0 for t in all_closed), 2),
        "open_positions": len(open_positions),
        "stale_positions": len(stale_positions),
    }

    report["recent_24h"] = {
        "trades_closed": len(recent_closed),
        "pnl": round(sum(t.get("pnl") or 0 for t in recent_closed), 2),
    }

    report["whale_stats"] = whale_stats
    report["resolution_health"] = resolution_stats
    report["log_health"] = log_health

    # ── Known Issues (already fixed — skip re-alerting) ─────────────
    known_issues_path = "monitor/known_issues.json"
    if os.path.exists(known_issues_path):
        with open(known_issues_path) as f:
            report["known_issues"] = json.load(f).get("resolved_issues", [])
    else:
        report["known_issues"] = []

    report["open_positions"] = [
        {
            "id": p["id"],
            "direction": p["direction"],
            "market_title": p.get("market_title", ""),
            "entry_time": p["entry_time"],
            "entry_price": p["entry_price"],
            "stop_loss_enabled": p.get("stop_price") is not None and p.get("stop_price", 0) > 0,
                "stop_price": p.get("stop_price"),
            "hours_open": round(
                (
                    datetime.now(timezone.utc)
                    - _aware(p["entry_time"])
                ).total_seconds()
                / 3600,
                1,
            ),
        }
        for p in open_positions
    ]

    # Save report
    os.makedirs("monitor/reports", exist_ok=True)
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    report_path = f"monitor/reports/report_{date_str}.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    # Also save as latest for easy access
    with open("monitor/reports/latest.json", "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"Report saved: {report_path}")
    return report_path


if __name__ == "__main__":
    collect()
