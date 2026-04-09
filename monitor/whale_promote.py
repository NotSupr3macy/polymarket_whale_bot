"""
Phase 3: Promote/Demote — graduate shadow whales to live, remove underperformers.

Runs weekly (Sundays) via run_agent.sh. Evaluates:
  - Shadow candidates with 8+ trades and 14+ days → promote to Tier 3
  - Live whales with 8+ trades and <40% WR → demote/remove

Exit codes:
  0 = no changes needed
  1 = error
  2 = changes applied (bot restart needed)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

# ── Promote/Demote Parameters ────────────────────────────────────────
MIN_SHADOW_TRADES = 8           # Minimum shadow trades before promotion
MIN_SHADOW_DAYS = 14            # Minimum days in shadow before promotion
MIN_SHADOW_WIN_RATE = 0.55      # Shadow WR threshold for promotion
MIN_LIVE_TRADES = 8             # Minimum live trades before demotion eval
MIN_LIVE_WIN_RATE = 0.45        # Live WR below this → demote
MAX_LIVE_WHALES = 18            # Cap on total live whales
MAX_PROMOTIONS_PER_WEEK = 3     # Don't flood the watchlist

SHADOW_PATH = Path(__file__).resolve().parent / "shadow_candidates.json"
WHALES_PATH = Path(__file__).resolve().parent.parent / "whales.json"
DB_PATH = os.getenv("DB_PATH", str(Path(__file__).resolve().parent.parent / "trades.db"))
PROMOTE_LOG = Path(__file__).resolve().parent / "promote_log.jsonl"


def load_shadow_candidates() -> dict:
    """Load shadow candidates JSON."""
    if not SHADOW_PATH.exists():
        return {}
    with open(SHADOW_PATH) as f:
        return json.load(f)


def save_shadow_candidates(data: dict) -> None:
    """Atomically save shadow candidates JSON."""
    tmp = SHADOW_PATH.with_suffix(".json.tmp")
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
    os.replace(tmp, SHADOW_PATH)


def load_whales() -> dict:
    """Load whales.json."""
    with open(WHALES_PATH) as f:
        return json.load(f)


def save_whales(data: dict) -> None:
    """Atomically write whales.json with backup."""
    invalid_keys = [k for k in data if not k or len(k) != 42 or not k.startswith("0x")]
    for k in invalid_keys:
        logger.warning("Removing invalid wallet key: %r", k)
        del data[k]

    if WHALES_PATH.exists():
        shutil.copy2(WHALES_PATH, WHALES_PATH.with_suffix(".json.bak"))

    tmp = WHALES_PATH.with_suffix(".json.tmp")
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
    os.replace(tmp, WHALES_PATH)
    logger.info("Saved whales.json (%d whales)", len(data))


def get_shadow_performance() -> dict[str, dict]:
    """
    Get per-wallet shadow trade performance from shadow_trades table.
    Returns: {wallet: {alias, trades, wins, losses, pnl, win_rate}}
    """
    stats: dict[str, dict] = {}
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT wallet, alias, outcome, pnl FROM shadow_trades "
            "WHERE status='closed' AND outcome IS NOT NULL"
        ).fetchall()
        conn.close()
    except Exception as e:
        logger.error("Shadow trades DB query failed: %s", e)
        return {}

    for row in rows:
        wallet = row["wallet"]
        if wallet not in stats:
            stats[wallet] = {
                "alias": row["alias"],
                "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0,
            }
        stats[wallet]["trades"] += 1
        pnl = row["pnl"] or 0
        stats[wallet]["pnl"] += pnl
        if row["outcome"] == "WIN" and pnl > 0:
            stats[wallet]["wins"] += 1
        else:
            stats[wallet]["losses"] += 1

    for s in stats.values():
        s["win_rate"] = round(s["wins"] / s["trades"], 3) if s["trades"] > 0 else 0
        s["pnl"] = round(s["pnl"], 2)

    return stats


def get_live_performance(whales: dict) -> dict[str, dict]:
    """
    Get per-whale live performance from trades_dry table.
    Returns: {wallet: {alias, trades, wins, losses, pnl, win_rate}}
    """
    alias_to_wallet = {info["alias"]: addr for addr, info in whales.items()}
    stats: dict[str, dict] = {}

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT whale_signals, outcome, pnl FROM trades_dry "
            "WHERE status='closed' AND outcome IS NOT NULL"
        ).fetchall()
        conn.close()
    except Exception as e:
        logger.error("Trades DB query failed: %s", e)
        return {}

    alias_stats: dict[str, dict] = {}
    for row in rows:
        try:
            aliases = json.loads(row["whale_signals"] or "[]")
        except json.JSONDecodeError:
            continue
        for alias in aliases:
            if alias not in alias_stats:
                alias_stats[alias] = {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0}
            alias_stats[alias]["trades"] += 1
            pnl = row["pnl"] or 0
            alias_stats[alias]["pnl"] += pnl
            if row["outcome"] == "WIN" and pnl > 0:
                alias_stats[alias]["wins"] += 1
            else:
                alias_stats[alias]["losses"] += 1

    for alias, s in alias_stats.items():
        wallet = alias_to_wallet.get(alias)
        if wallet:
            s["alias"] = alias
            s["win_rate"] = round(s["wins"] / s["trades"], 3) if s["trades"] > 0 else 0
            s["pnl"] = round(s["pnl"], 2)
            stats[wallet] = s

    return stats


def evaluate_promotions(shadow: dict, shadow_perf: dict) -> list[dict]:
    """Find shadow candidates ready for promotion."""
    now = datetime.now(timezone.utc)
    promotions = []

    for wallet, info in shadow.items():
        if info.get("status") != "shadowing":
            continue

        # Check minimum shadow period
        shadow_start = info.get("shadow_start")
        if shadow_start:
            start_dt = datetime.fromisoformat(shadow_start).replace(tzinfo=timezone.utc)
            days_shadowed = (now - start_dt).days
            if days_shadowed < MIN_SHADOW_DAYS:
                logger.debug("%s: only %d days shadowed (need %d)",
                           info["alias"], days_shadowed, MIN_SHADOW_DAYS)
                continue
        else:
            continue

        # Check shadow trade performance
        perf = shadow_perf.get(wallet)
        if not perf:
            logger.debug("%s: no shadow trades yet", info["alias"])
            continue

        if perf["trades"] < MIN_SHADOW_TRADES:
            logger.debug("%s: only %d shadow trades (need %d)",
                       info["alias"], perf["trades"], MIN_SHADOW_TRADES)
            continue

        if perf["win_rate"] < MIN_SHADOW_WIN_RATE:
            logger.info("SHADOW REJECT: %s — %dW/%dL (%.0f%% WR) below %.0f%% threshold",
                       info["alias"], perf["wins"], perf["losses"],
                       perf["win_rate"] * 100, MIN_SHADOW_WIN_RATE * 100)
            continue

        promotions.append({
            "wallet": wallet,
            "alias": info["alias"],
            "trades": perf["trades"],
            "wins": perf["wins"],
            "losses": perf["losses"],
            "win_rate": perf["win_rate"],
            "pnl": perf["pnl"],
            "days_shadowed": days_shadowed,
            "monthly_pnl": info.get("monthly_pnl", 0),
            "monthly_volume": info.get("monthly_volume", 0),
            "est_avg_bet": info.get("est_avg_bet", 10000),
            "real_win_rate": info.get("real_win_rate", 0.57),
            "score": info.get("score", 0),
            "category": info.get("category", "sports"),
        })

    # Sort by shadow win rate, then score
    promotions.sort(key=lambda p: (p["win_rate"], p["score"]), reverse=True)
    return promotions[:MAX_PROMOTIONS_PER_WEEK]


def evaluate_demotions(whales: dict, live_perf: dict) -> list[dict]:
    """Find live whales that should be demoted."""
    demotions = []

    for wallet, info in whales.items():
        if info.get("protected", False):
            continue
        if info.get("tier") == 1:
            continue

        perf = live_perf.get(wallet)
        if not perf:
            continue
        if perf["trades"] < MIN_LIVE_TRADES:
            continue
        if perf["win_rate"] < MIN_LIVE_WIN_RATE:
            demotions.append({
                "wallet": wallet,
                "alias": info["alias"],
                "tier": info.get("tier", 3),
                "trades": perf["trades"],
                "wins": perf["wins"],
                "losses": perf["losses"],
                "win_rate": perf["win_rate"],
                "pnl": perf["pnl"],
            })
            logger.info("DEMOTION: %s — %dW/%dL (%.0f%% WR, PnL $%.2f)",
                       info["alias"], perf["wins"], perf["losses"],
                       perf["win_rate"] * 100, perf["pnl"])

    return demotions


def evaluate_rejections(shadow: dict, shadow_perf: dict) -> list[dict]:
    """Find shadow candidates that should be rejected (failed shadow period)."""
    now = datetime.now(timezone.utc)
    rejections = []

    for wallet, info in shadow.items():
        if info.get("status") != "shadowing":
            continue

        shadow_start = info.get("shadow_start")
        if not shadow_start:
            continue

        start_dt = datetime.fromisoformat(shadow_start).replace(tzinfo=timezone.utc)
        days_shadowed = (now - start_dt).days

        # Only evaluate after minimum period
        if days_shadowed < MIN_SHADOW_DAYS:
            continue

        perf = shadow_perf.get(wallet)
        if not perf or perf["trades"] < MIN_SHADOW_TRADES:
            # If 30+ days and still not enough trades, reject as inactive
            if days_shadowed >= 30:
                rejections.append({
                    "wallet": wallet,
                    "alias": info["alias"],
                    "reason": "inactive",
                    "days": days_shadowed,
                    "trades": perf["trades"] if perf else 0,
                })
            continue

        if perf["win_rate"] < MIN_SHADOW_WIN_RATE:
            rejections.append({
                "wallet": wallet,
                "alias": info["alias"],
                "reason": "low_wr",
                "win_rate": perf["win_rate"],
                "trades": perf["trades"],
                "wins": perf["wins"],
                "losses": perf["losses"],
                "days": days_shadowed,
            })

    return rejections


def build_whale_entry(promo: dict) -> dict:
    """Build a whales.json entry for a promoted whale."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return {
        "alias": promo["alias"],
        "tier": 3,
        "protected": False,
        "category": promo.get("category", "sports"),
        "solo_enabled": False,
        "win_rate": promo["real_win_rate"],
        "avg_bet": promo.get("est_avg_bet", 10000),
        "verified_stats": {
            "monthly_pnl": promo.get("monthly_pnl", 0),
            "monthly_volume": promo.get("monthly_volume", 0),
            "shadow_record": f"{promo['wins']}W/{promo['losses']}L ({promo['win_rate']:.0%})",
            "shadow_days": promo.get("days_shadowed", 0),
        },
        "source": f"Auto-discovery pipeline (shadow verified), {now}",
        "notes": f"Shadow: {promo['wins']}W/{promo['losses']}L over {promo.get('days_shadowed', 0)} days. Score={promo.get('score', 0)}",
        "added_at": now,
        "added_by": "auto_discovery",
    }


def send_telegram_sync(message: str) -> None:
    """Send a Telegram notification."""
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return
    try:
        subprocess.run(
            [
                "curl", "-s", "-X", "POST",
                f"https://api.telegram.org/bot{token}/sendMessage",
                "--data-urlencode", f"chat_id={chat_id}",
                "--data-urlencode", f"text={message}",
            ],
            capture_output=True, timeout=10,
        )
    except Exception as e:
        logger.debug("Telegram send failed: %s", e)


def log_event(event: dict) -> None:
    """Append event to promote_log.jsonl."""
    event["timestamp"] = datetime.now(timezone.utc).isoformat()
    with open(PROMOTE_LOG, "a") as f:
        f.write(json.dumps(event) + "\n")


def run_promote() -> int:
    """
    Main promote/demote logic.
    Returns: 0=no changes, 1=error, 2=changes applied (restart needed)
    """
    # Load data
    try:
        whales = load_whales()
        shadow = load_shadow_candidates()
    except Exception as e:
        logger.error("Failed to load data: %s", e)
        return 1

    shadow_perf = get_shadow_performance()
    live_perf = get_live_performance(whales)

    changes = False
    promotions_applied = []
    demotions_applied = []
    rejections_applied = []

    # ── Evaluate demotions first (make room) ──────────────────────────
    demotions = evaluate_demotions(whales, live_perf)
    for d in demotions:
        del whales[d["wallet"]]
        demotions_applied.append(d)
        changes = True
        logger.info("REMOVED: %s (%dW/%dL, %.0f%% WR)",
                   d["alias"], d["wins"], d["losses"], d["win_rate"] * 100)

    # ── Evaluate promotions ───────────────────────────────────────────
    promotions = evaluate_promotions(shadow, shadow_perf)
    for p in promotions:
        if len(whales) >= MAX_LIVE_WHALES:
            logger.warning("Live whale cap (%d) reached, skipping promotion of %s",
                         MAX_LIVE_WHALES, p["alias"])
            break

        whales[p["wallet"]] = build_whale_entry(p)
        shadow[p["wallet"]]["status"] = "promoted"
        shadow[p["wallet"]]["promoted_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        promotions_applied.append(p)
        changes = True
        logger.info("PROMOTED: %s → Tier 3 (%dW/%dL, %.0f%% shadow WR)",
                   p["alias"], p["wins"], p["losses"], p["win_rate"] * 100)

    # ── Evaluate rejections ───────────────────────────────────────────
    rejections = evaluate_rejections(shadow, shadow_perf)
    for r in rejections:
        shadow[r["wallet"]]["status"] = "rejected"
        shadow[r["wallet"]]["rejected_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        shadow[r["wallet"]]["rejection_reason"] = r["reason"]
        rejections_applied.append(r)
        logger.info("REJECTED: %s (reason=%s)", r["alias"], r["reason"])

    # ── Save changes ──────────────────────────────────────────────────
    if changes:
        save_whales(whales)

    if rejections_applied or promotions_applied:
        save_shadow_candidates(shadow)

    # ── Log event ─────────────────────────────────────────────────────
    if promotions_applied or demotions_applied or rejections_applied:
        log_event({
            "promoted": [{"alias": p["alias"], "wallet": p["wallet"]} for p in promotions_applied],
            "demoted": [{"alias": d["alias"], "wallet": d["wallet"]} for d in demotions_applied],
            "rejected": [{"alias": r["alias"], "wallet": r["wallet"]} for r in rejections_applied],
            "whale_count": len(whales),
        })

    # ── Send Telegram alerts ──────────────────────────────────────────

    # Individual promotion alerts
    for p in promotions_applied:
        msg = (
            f"✅ WHALE PROMOTED\n\n"
            f"{p['alias']} graduated to live Tier 3!\n\n"
            f"Shadow record: {p['wins']}W/{p['losses']}L ({p['win_rate']:.0%} WR, {p['trades']} trades)\n"
            f"Shadow period: {p.get('days_shadowed', '?')} days\n"
            f"Monthly PnL: ${p.get('monthly_pnl', 0):,.0f} | Volume: ${p.get('monthly_volume', 0):,.0f}\n\n"
            f"Now tracking {len(whales)} whales"
        )
        send_telegram_sync(msg)

    # Individual demotion alerts
    for d in demotions_applied:
        msg = (
            f"❌ WHALE DEMOTED\n\n"
            f"{d['alias']} removed from watchlist\n\n"
            f"Live record: {d['wins']}W/{d['losses']}L ({d['win_rate']:.0%} WR, {d['trades']} trades)\n"
            f"PnL: ${d['pnl']:,.2f}\n\n"
            f"Watchlist: {len(whales)} whales remaining"
        )
        send_telegram_sync(msg)

    # ── Weekly shadow update ──────────────────────────────────────────
    active_shadow = {w: info for w, info in shadow.items() if info.get("status") == "shadowing"}
    if active_shadow:
        lines = ["👻 SHADOW UPDATE\n"]
        lines.append(f"{len(active_shadow)} candidates being tracked:\n")

        for wallet, info in sorted(active_shadow.items(), key=lambda x: x[1].get("score", 0), reverse=True):
            alias = info["alias"]
            perf = shadow_perf.get(wallet)
            if perf:
                wr_emoji = "⬆️" if perf["win_rate"] >= 0.60 else ("🔄" if perf["win_rate"] >= 0.50 else "⬇️")
                if perf["trades"] >= MIN_SHADOW_TRADES and perf["win_rate"] >= MIN_SHADOW_WIN_RATE:
                    wr_emoji = "✅ Ready"
                lines.append(
                    f"  {alias}: {perf['trades']} trades, "
                    f"{perf['wins']}W/{perf['losses']}L ({perf['win_rate']:.0%} WR) {wr_emoji}"
                )
            else:
                lines.append(f"  {alias}: 0 trades (monitoring...)")

        total_shadow_trades = sum(p["trades"] for p in shadow_perf.values())
        avg_wr = (
            sum(p["win_rate"] for p in shadow_perf.values()) / len(shadow_perf)
            if shadow_perf else 0
        )
        lines.append(f"\nAvg shadow WR: {avg_wr:.0%} | Total trades: {total_shadow_trades}")

        send_telegram_sync("\n".join(lines))

    # ── Weekly discovery report ───────────────────────────────────────
    total_promoted = sum(1 for v in shadow.values() if v.get("status") == "promoted")
    total_rejected = sum(1 for v in shadow.values() if v.get("status") == "rejected")
    total_active = len(active_shadow)

    report_lines = [
        "📊 DISCOVERY REPORT\n",
        f"Pipeline health (week of {datetime.now(timezone.utc).strftime('%b %d')}):\n",
        f"Promoted this week: {len(promotions_applied)}",
        f"Demoted this week: {len(demotions_applied)}",
        f"Rejected this week: {len(rejections_applied)}",
        f"\nLive watchlist: {len(whales)} whales",
        f"Shadow pool: {total_active} candidates",
        f"All-time promoted: {total_promoted} | rejected: {total_rejected}",
    ]
    send_telegram_sync("\n".join(report_lines))

    if not changes and not rejections_applied:
        logger.info("No promotions or demotions needed this week")
        return 0

    return 2 if changes else 0


def main() -> int:
    """Entry point."""
    return run_promote()


if __name__ == "__main__":
    sys.exit(main())
