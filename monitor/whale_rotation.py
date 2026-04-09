"""
Automatic whale rotation — removes underperformers, adds from leaderboard.

Runs as part of the monitoring agent pipeline (run_agent.sh).
Deterministic logic — no LLM involved.

Rules:
  - Remove whales with win rate < MIN_WIN_RATE after MIN_TRADES trades
  - Never remove protected whales (Tier 1)
  - Replace with top monthly earners from Polymarket leaderboard
  - New whales always enter at Tier 3, solo_enabled=False
  - Maintain TARGET_WHALE_COUNT whales at all times

Exit codes:
  0 = no rotation needed
  1 = error
  2 = rotation applied (bot restart needed)
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import asyncio

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

# ── Rotation Parameters ──────────────────────────────────────────────
TARGET_WHALE_COUNT = 16
MIN_TRADES = 8          # Minimum closed trades before evaluating
MIN_WIN_RATE = 0.45     # Below this → auto-remove
MIN_LEADERBOARD_PNL = 100_000  # Monthly PnL floor for new candidates ($100K)
DEFAULT_WIN_RATE = 0.57
DEFAULT_AVG_BET = 10_000

WHALES_PATH = Path(__file__).resolve().parent.parent / "whales.json"
DB_PATH = os.getenv("DB_PATH", str(Path(__file__).resolve().parent.parent / "trades.db"))
DATA_API = "https://data-api.polymarket.com"
ROTATION_LOG = Path(__file__).resolve().parent / "rotation_log.jsonl"


def load_whales() -> dict:
    """Load current whales.json."""
    with open(WHALES_PATH) as f:
        return json.load(f)


def save_whales(data: dict) -> None:
    """Atomically write whales.json (write .tmp, rename)."""
    # Safety net: remove any empty or invalid wallet keys
    invalid_keys = [k for k in data if not k or len(k) != 42 or not k.startswith("0x")]
    for k in invalid_keys:
        logger.warning("Removing invalid wallet key from whales.json: %r", k)
        del data[k]

    # Backup current version
    if WHALES_PATH.exists():
        shutil.copy2(WHALES_PATH, WHALES_PATH.with_suffix(".json.bak"))

    tmp = WHALES_PATH.with_suffix(".json.tmp")
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
    os.replace(tmp, WHALES_PATH)
    logger.info("Saved whales.json (%d whales)", len(data))


def build_alias_to_wallet(whales: dict) -> dict[str, str]:
    """Build reverse map: alias → wallet address."""
    return {info["alias"]: addr for addr, info in whales.items()}


def get_whale_performance(whales: dict) -> dict[str, dict]:
    """
    Query trades_dry for per-whale win/loss stats.

    The DB stores aliases in whale_signals JSON column (e.g. '["ImJustKen","gmanas"]').
    We aggregate per alias, then map back to wallet addresses.

    Returns: {wallet_address: {alias, trades, wins, losses, pnl, win_rate}}
    """
    alias_to_wallet = build_alias_to_wallet(whales)
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
        logger.error("DB query failed: %s", e)
        return {}

    # Aggregate per alias
    alias_stats: dict[str, dict] = {}
    for row in rows:
        signals_raw = row["whale_signals"] or "[]"
        try:
            aliases = json.loads(signals_raw)
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

    # Map alias stats to wallet addresses
    for alias, s in alias_stats.items():
        wallet = alias_to_wallet.get(alias)
        if wallet:
            s["alias"] = alias
            s["win_rate"] = round(s["wins"] / s["trades"], 3) if s["trades"] > 0 else 0
            stats[wallet] = s

    return stats


def identify_underperformers(whales: dict, performance: dict) -> list[dict]:
    """
    Find whales with MIN_TRADES+ trades AND win rate < MIN_WIN_RATE.
    Never removes protected whales.
    """
    removals = []
    for wallet, info in whales.items():
        if info.get("protected", False):
            continue
        if info.get("tier") == 1:
            continue

        perf = performance.get(wallet)
        if not perf:
            continue
        if perf["trades"] < MIN_TRADES:
            continue
        if perf["win_rate"] < MIN_WIN_RATE:
            removals.append({
                "wallet": wallet,
                "alias": info["alias"],
                "trades": perf["trades"],
                "wins": perf["wins"],
                "losses": perf["losses"],
                "win_rate": perf["win_rate"],
                "pnl": round(perf["pnl"], 2),
            })
            logger.info(
                "UNDERPERFORMER: %s — %dW/%dL (%.0f%% WR, %d trades, PnL $%.2f)",
                info["alias"], perf["wins"], perf["losses"],
                perf["win_rate"] * 100, perf["trades"], perf["pnl"],
            )

    return removals


async def fetch_leaderboard_candidates(exclude_wallets: set[str]) -> list[dict]:
    """
    Fetch top monthly earners from Polymarket leaderboard API.
    Filter for candidates not already in the watchlist.
    """
    url = f"{DATA_API}/v1/leaderboard"
    params = {"period": "month", "limit": "50"}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    logger.error("Leaderboard API returned %d", resp.status)
                    return []
                data = await resp.json()
    except Exception as e:
        logger.error("Leaderboard API failed: %s", e)
        return []

    candidates = []
    for entry in data:
        addr = entry.get("address", "").lower().strip()
        pnl = entry.get("pnl", 0)
        volume = entry.get("volume", 0)

        # Skip invalid addresses (must be 0x + 40 hex chars)
        if not addr or len(addr) != 42 or not addr.startswith("0x"):
            logger.warning("Skipping invalid address from leaderboard: %r", addr)
            continue

        # Skip if already in watchlist
        if addr in exclude_wallets:
            continue

        # Minimum PnL filter
        if pnl < MIN_LEADERBOARD_PNL:
            continue

        # Estimate avg bet from volume (rough: volume / 100 trades assumed)
        num_trades = entry.get("num_trades") or entry.get("numTrades") or 100
        est_avg_bet = min(max(int(volume / max(num_trades, 1)), 5000), 30000)

        candidates.append({
            "wallet": addr,
            "alias": entry.get("username") or entry.get("name") or addr[:10],
            "monthly_pnl": pnl,
            "monthly_volume": volume,
            "est_avg_bet": est_avg_bet,
        })

    # Sort by PnL descending
    candidates.sort(key=lambda c: c["monthly_pnl"], reverse=True)
    logger.info("Found %d leaderboard candidates (after filtering)", len(candidates))
    return candidates


def build_new_whale_entry(candidate: dict) -> dict:
    """Build a whales.json entry for a new whale (Tier 3 probation)."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return {
        "alias": candidate["alias"],
        "tier": 3,
        "protected": False,
        "category": "multi",
        "solo_enabled": False,
        "win_rate": DEFAULT_WIN_RATE,
        "avg_bet": candidate.get("est_avg_bet", DEFAULT_AVG_BET),
        "verified_stats": {
            "monthly_pnl": candidate["monthly_pnl"],
            "monthly_volume": candidate["monthly_volume"],
        },
        "source": f"Auto-rotation from leaderboard, {now}",
        "added_at": now,
        "added_by": "auto_rotation",
    }


def send_telegram_sync(message: str) -> None:
    """Send a Telegram notification (sync, via curl subprocess)."""
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


def log_rotation(removals: list[dict], additions: list[dict], whale_count_before: int, whale_count_after: int) -> None:
    """Append rotation event to JSONL log."""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "removed": removals,
        "added": [{"wallet": a["wallet"], "alias": a["alias"], "monthly_pnl": a["monthly_pnl"]} for a in additions],
        "whale_count_before": whale_count_before,
        "whale_count_after": whale_count_after,
    }
    with open(ROTATION_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")


async def run_rotation() -> int:
    """
    Main rotation logic. Returns exit code:
      0 = no rotation needed
      1 = error
      2 = rotation applied (restart needed)
    """
    # Load current whales
    try:
        whales = load_whales()
    except Exception as e:
        logger.error("Failed to load whales.json: %s", e)
        return 1

    current_count = len(whales)
    logger.info("Current watchlist: %d whales", current_count)

    # Get per-whale performance from DB
    performance = get_whale_performance(whales)
    if not performance:
        logger.info("No performance data yet — skipping rotation")
        return 0

    # Identify underperformers
    removals = identify_underperformers(whales, performance)

    # Calculate how many we need to add
    # (removals + deficit from being under target)
    deficit = max(0, TARGET_WHALE_COUNT - current_count)
    needed = len(removals) + deficit

    if needed == 0:
        logger.info("No rotation needed — all whales performing above threshold")
        return 0

    # Fetch replacement candidates from leaderboard
    exclude = set(whales.keys())
    candidates = await fetch_leaderboard_candidates(exclude)

    if len(candidates) < needed:
        if len(removals) > 0:
            logger.warning(
                "Not enough leaderboard candidates (%d found, %d needed) — "
                "aborting rotation to avoid reducing watchlist below target",
                len(candidates), needed,
            )
            return 0
        # If we're just topping up, add what we can
        needed = len(candidates)

    if needed == 0:
        return 0

    # Select top candidates
    additions = candidates[:needed]

    # Apply removals
    for r in removals:
        del whales[r["wallet"]]
        logger.info("REMOVED: %s (%s)", r["alias"], r["wallet"][:10])

    # Apply additions
    for a in additions:
        whales[a["wallet"]] = build_new_whale_entry(a)
        logger.info("ADDED: %s (%s) — Monthly PnL $%,.0f", a["alias"], a["wallet"][:10], a["monthly_pnl"])

    # Save
    save_whales(whales)

    # Log rotation
    log_rotation(removals, additions, current_count, len(whales))

    # Build Telegram message
    lines = ["🔄 WHALE ROTATION\n"]

    if removals:
        lines.append("Removed (underperforming):")
        for r in removals:
            lines.append(
                f"  ❌ {r['alias']}: {r['wins']}W/{r['losses']}L "
                f"({r['win_rate']:.0%} WR, {r['trades']} trades, PnL ${r['pnl']:,.2f})"
            )
        lines.append("")

    if additions:
        lines.append("Added (from leaderboard, Tier 3 probation):")
        for a in additions:
            lines.append(
                f"  ✅ {a['alias']}: Monthly PnL ${a['monthly_pnl']:,.0f}, "
                f"Vol ${a['monthly_volume']:,.0f}"
            )
        lines.append("")

    lines.append(f"Watchlist: {len(whales)} whales")
    lines.append("Bot restarting to load new config...")

    send_telegram_sync("\n".join(lines))

    return 2  # Signal restart needed


def main() -> int:
    """Entry point for CLI / run_agent.sh."""
    return asyncio.run(run_rotation())


if __name__ == "__main__":
    sys.exit(main())
