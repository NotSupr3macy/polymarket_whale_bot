"""
Phase 1: Whale Scout — discover high-performing traders from the Polymarket leaderboard.

Runs daily via run_agent.sh. Fetches leaderboard, calculates real win rates
from resolved positions, filters for sports-focused traders, and saves
candidates to shadow_candidates.json for Phase 2 (shadow tracking).

Exit codes:
  0 = no new candidates found
  1 = error
  2 = new candidates added to shadow pool
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

# ── Scout Parameters ──────────────────────────────────────────────────
DATA_API = "https://data-api.polymarket.com"
LEADERBOARD_LIMIT = 100
MIN_REAL_WIN_RATE = 0.58          # Minimum verified win rate from positions
MIN_RESOLVED_POSITIONS = 10       # Need enough data to trust the WR
MIN_MONTHLY_PNL = 50_000          # $50K+ monthly PnL
MIN_SPORTS_PCT = 0.40             # At least 40% sports positions
MAX_SHADOW_POOL = 20              # Cap shadow pool size
API_DELAY = 0.5                   # Seconds between API calls (rate limiting)

SHADOW_PATH = Path(__file__).resolve().parent / "shadow_candidates.json"
WHALES_PATH = Path(__file__).resolve().parent.parent / "whales.json"

# Sports detection keywords
SPORTS_KEYWORDS = re.compile(
    r"\b(vs\.?|spread|o/u|over/under|moneyline|nba|nfl|nhl|mlb|ncaa|"
    r"ufc|mma|epl|premier league|la liga|serie a|bundesliga|ligue 1|"
    r"uefa|champions league|europa|mls|nascar|f1|formula|pga|atp|wta|"
    r"cavaliers|lakers|celtics|warriors|knicks|nets|heat|bulls|"
    r"yankees|dodgers|cubs|red sox|braves|astros|"
    r"chiefs|eagles|49ers|cowboys|bills|ravens|"
    r"oilers|maple leafs|rangers|bruins|panthers|"
    r"madrid|barcelona|arsenal|liverpool|manchester|chelsea|bayern|psg|"
    r"juventus|inter|milan|dortmund|"
    r"thunder|nuggets|76ers|bucks|clippers|spurs|rockets|"
    r"padres|mets|phillies|orioles|tigers|twins|"
    r"packers|dolphins|bengals|lions|steelers|"
    r"flames|senators|lightning|penguins|canadiens|"
    r"win on 2026|win on 2025)\b",
    re.IGNORECASE,
)


def load_shadow_candidates() -> dict:
    """Load existing shadow candidates or return empty dict."""
    if SHADOW_PATH.exists():
        try:
            with open(SHADOW_PATH) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            logger.warning("Failed to load shadow_candidates.json, starting fresh")
    return {}


def save_shadow_candidates(data: dict) -> None:
    """Atomically write shadow_candidates.json."""
    tmp = SHADOW_PATH.with_suffix(".json.tmp")
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
    os.replace(tmp, SHADOW_PATH)
    logger.info("Saved shadow_candidates.json (%d entries)", len(data))


def load_whales() -> dict:
    """Load current whales.json."""
    with open(WHALES_PATH) as f:
        return json.load(f)


def is_sports_market(title: str) -> bool:
    """Check if a market title looks like a sports market."""
    return bool(SPORTS_KEYWORDS.search(title or ""))


def calculate_score(win_rate: float, volume: float, sports_pct: float) -> float:
    """
    Score a candidate: weighted combination of accuracy, scale, and sports focus.

    - Win rate (40%): most important — we need accurate traders
    - Volume (30%): ensures they trade enough for us to copy
    - Sports pct (30%): we focus on sports markets
    """
    wr_score = win_rate * 100  # 0-100 scale
    vol_score = min(math.log10(max(volume, 1)) * 10, 100)  # log scale, cap 100
    sports_score = sports_pct * 100  # 0-100 scale
    return round(wr_score * 0.40 + vol_score * 0.30 + sports_score * 0.30, 1)


async def fetch_leaderboard(session: aiohttp.ClientSession, period: str) -> list[dict]:
    """Fetch leaderboard for a given period."""
    url = f"{DATA_API}/v1/leaderboard"
    params = {"period": period, "limit": str(LEADERBOARD_LIMIT)}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                logger.error("Leaderboard API (%s) returned %d", period, resp.status)
                return []
            return await resp.json()
    except Exception as e:
        logger.error("Leaderboard API (%s) failed: %s", period, e)
        return []


async def fetch_positions(session: aiohttp.ClientSession, wallet: str) -> list[dict]:
    """Fetch all positions for a wallet (with pagination)."""
    positions = []
    offset = 0
    limit = 100
    while True:
        url = f"{DATA_API}/v1/positions"
        params = {"user": wallet, "sizeThreshold": "0.1", "limit": str(limit), "offset": str(offset)}
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    break
                batch = await resp.json()
                if not batch:
                    break
                positions.extend(batch)
                if len(batch) < limit:
                    break
                offset += limit
                await asyncio.sleep(0.2)
        except Exception as e:
            logger.debug("Positions API failed for %s: %s", wallet[:10], e)
            break
    return positions


def analyze_positions(positions: list[dict]) -> dict:
    """
    Analyze a wallet's positions to calculate real win rate and sports focus.

    Returns: {win_rate, wins, losses, resolved, sports_pct, total_positions, avg_bet}
    """
    wins = 0
    losses = 0
    sports_count = 0
    total_value = 0.0

    for pos in positions:
        title = pos.get("title", "")
        cur_price = pos.get("curPrice", 0.5)
        redeemable = pos.get("redeemable", False)
        initial_value = pos.get("initialValue", 0)
        total_value += initial_value

        # Count sports positions
        if is_sports_market(title):
            sports_count += 1

        # Count resolved positions
        if redeemable:
            if cur_price >= 0.95:
                wins += 1
            elif cur_price <= 0.05:
                losses += 1
        elif cur_price >= 0.98:
            # Nearly resolved — likely a win
            wins += 1
        elif cur_price <= 0.02:
            # Nearly resolved — likely a loss
            losses += 1

    resolved = wins + losses
    total = len(positions)

    return {
        "wins": wins,
        "losses": losses,
        "resolved": resolved,
        "win_rate": round(wins / resolved, 3) if resolved > 0 else 0,
        "sports_pct": round(sports_count / total, 3) if total > 0 else 0,
        "total_positions": total,
        "avg_bet": round(total_value / total) if total > 0 else 0,
    }


def send_telegram_sync(message: str) -> None:
    """Send a Telegram notification (sync, via curl subprocess)."""
    import subprocess
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


async def run_scout() -> int:
    """
    Main scout logic.

    Returns:
        0 = no new candidates
        1 = error
        2 = new candidates found
    """
    try:
        whales = load_whales()
    except Exception as e:
        logger.error("Failed to load whales.json: %s", e)
        return 1

    existing_wallets = set(whales.keys())
    shadow = load_shadow_candidates()
    active_shadow_wallets = {
        w for w, info in shadow.items()
        if info.get("status") in ("scouted", "shadowing")
    }

    # ── Fetch leaderboard (monthly + weekly) ──────────────────────────
    async with aiohttp.ClientSession() as session:
        month_data, week_data = await asyncio.gather(
            fetch_leaderboard(session, "month"),
            fetch_leaderboard(session, "week"),
        )

    # Deduplicate: keep higher PnL entry per wallet
    combined: dict[str, dict] = {}
    for entry in month_data:
        addr = (entry.get("proxyWallet") or entry.get("address", "")).lower().strip()
        if addr and len(addr) == 42:
            combined[addr] = {**entry, "_period": "month"}

    for entry in week_data:
        addr = (entry.get("proxyWallet") or entry.get("address", "")).lower().strip()
        if addr and len(addr) == 42:
            existing = combined.get(addr)
            if not existing or (entry.get("pnl", 0) > existing.get("pnl", 0)):
                combined[addr] = {**entry, "_period": "week"}

    logger.info("Leaderboard: %d monthly + %d weekly = %d unique wallets",
                len(month_data), len(week_data), len(combined))

    # ── Filter candidates ─────────────────────────────────────────────
    # Skip wallets already in watchlist or active shadow pool
    skip_wallets = existing_wallets | active_shadow_wallets
    candidates_to_analyze = []

    for addr, entry in combined.items():
        if addr in skip_wallets:
            continue
        pnl = entry.get("pnl", 0)
        if pnl < MIN_MONTHLY_PNL:
            continue
        candidates_to_analyze.append((addr, entry))

    logger.info("Candidates to analyze: %d (after filtering existing + low PnL)", len(candidates_to_analyze))

    if not candidates_to_analyze:
        logger.info("No new candidates to analyze")
        return 0

    # ── Analyze each candidate's positions ────────────────────────────
    new_candidates = []

    async with aiohttp.ClientSession() as session:
        for addr, entry in candidates_to_analyze[:30]:  # Cap at 30 to avoid rate limits
            await asyncio.sleep(API_DELAY)
            positions = await fetch_positions(session, addr)

            if not positions:
                logger.debug("No positions for %s, skipping", addr[:10])
                continue

            analysis = analyze_positions(positions)

            # Apply filters
            if analysis["resolved"] < MIN_RESOLVED_POSITIONS:
                logger.debug("  %s: only %d resolved positions, need %d",
                           addr[:10], analysis["resolved"], MIN_RESOLVED_POSITIONS)
                continue

            if analysis["win_rate"] < MIN_REAL_WIN_RATE:
                logger.debug("  %s: WR %.1f%% < %.1f%% minimum",
                           addr[:10], analysis["win_rate"] * 100, MIN_REAL_WIN_RATE * 100)
                continue

            if analysis["sports_pct"] < MIN_SPORTS_PCT:
                logger.debug("  %s: sports %.1f%% < %.1f%% minimum",
                           addr[:10], analysis["sports_pct"] * 100, MIN_SPORTS_PCT * 100)
                continue

            username = entry.get("userName") or entry.get("username") or entry.get("name") or addr[:10]
            volume = entry.get("vol") or entry.get("volume", 0)
            score = calculate_score(analysis["win_rate"], volume, analysis["sports_pct"])

            candidate = {
                "alias": username,
                "wallet": addr,
                "status": "scouted",
                "scouted_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "shadow_start": None,
                "leaderboard_rank": int(entry.get("rank", 0)),
                "leaderboard_period": entry.get("_period", "month"),
                "monthly_pnl": round(entry.get("pnl", 0), 2),
                "monthly_volume": round(volume, 2),
                "real_win_rate": analysis["win_rate"],
                "resolved_positions": analysis["resolved"],
                "wins": analysis["wins"],
                "losses": analysis["losses"],
                "sports_pct": analysis["sports_pct"],
                "total_positions": analysis["total_positions"],
                "est_avg_bet": analysis["avg_bet"],
                "score": score,
                "category": "sports" if analysis["sports_pct"] >= 0.5 else "multi",
            }

            new_candidates.append(candidate)
            logger.info("  CANDIDATE: %s | WR=%.1f%% (%dW/%dL, %d resolved) | "
                       "Sports=%.0f%% | PnL=$%,.0f | Score=%.1f",
                       username, analysis["win_rate"] * 100,
                       analysis["wins"], analysis["losses"], analysis["resolved"],
                       analysis["sports_pct"] * 100, entry.get("pnl", 0), score)

    if not new_candidates:
        logger.info("No candidates passed all filters")
        return 0

    # ── Sort by score and add to shadow pool ──────────────────────────
    new_candidates.sort(key=lambda c: c["score"], reverse=True)

    added_count = 0
    for candidate in new_candidates:
        addr = candidate["wallet"]
        if addr not in shadow:
            # Mark as scouted — Phase 2 will transition to "shadowing"
            candidate["status"] = "shadowing"
            candidate["shadow_start"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            shadow[addr] = candidate
            added_count += 1
            logger.info("ADDED TO SHADOW: %s (score=%.1f)", candidate["alias"], candidate["score"])

    # Cap shadow pool (keep highest scores among active)
    active = {w: info for w, info in shadow.items() if info.get("status") in ("scouted", "shadowing")}
    if len(active) > MAX_SHADOW_POOL:
        sorted_active = sorted(active.items(), key=lambda x: x[1].get("score", 0), reverse=True)
        for wallet, info in sorted_active[MAX_SHADOW_POOL:]:
            shadow[wallet]["status"] = "dropped"
            logger.info("DROPPED FROM SHADOW (pool full): %s", info["alias"])

    save_shadow_candidates(shadow)

    # ── Telegram notification ─────────────────────────────────────────
    active_count = sum(1 for v in shadow.values() if v.get("status") in ("scouted", "shadowing"))

    lines = [f"🔍 WHALE SCOUT\n"]
    lines.append(f"Scanned: {len(combined)} leaderboard wallets")
    lines.append(f"New candidates: {added_count}\n")

    for c in new_candidates[:5]:  # Show top 5
        lines.append(
            f"  {c['alias']}: {c['real_win_rate']:.0%} WR "
            f"({c['wins']}W/{c['losses']}L, {c['resolved_positions']} resolved) | "
            f"${c['monthly_pnl']:,.0f} PnL | Score {c['score']}"
        )

    if len(new_candidates) > 5:
        lines.append(f"  ... and {len(new_candidates) - 5} more")

    lines.append(f"\nShadow pool: {active_count} candidates")
    send_telegram_sync("\n".join(lines))

    logger.info("Scout complete: %d new candidates, %d total in shadow pool", added_count, active_count)
    return 2 if added_count > 0 else 0


def main() -> int:
    """Entry point."""
    return asyncio.run(run_scout())


if __name__ == "__main__":
    sys.exit(main())
