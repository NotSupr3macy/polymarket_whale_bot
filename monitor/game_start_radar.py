"""
Game-start radar — fires two alerts per tracked game:
  1. T-10 minutes before kickoff / puck-drop / tip-off
  2. The moment the game starts

Pulls `gameStartTime` from Polymarket's Gamma API for every condition_id
that has an open whale position, then schedules alerts around those times.

Alerts aggregate all whales on the same (condition_id, direction) cluster
so you see exactly who bet what on the game that's about to start.

Dedupe is persisted in SQLite (`game_alerts_sent` table) so restarts never
double-fire. A game that's already past T-10 on startup simply gets its
T-10 flag seeded silently; the START alert still fires normally.

Usage:
    python3 monitor/game_start_radar.py
    python3 monitor/game_start_radar.py --dry-run
    python3 monitor/game_start_radar.py --once

Deployed via deploy/start_game_radar.sh into its own tmux session.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sqlite3
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger("game_start_radar")

# ── Paths ──────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
DB_PATH = Path(os.getenv("DB_PATH", str(ROOT / "trades.db")))
WHALES_JSON = ROOT / "whales.json"
ENV_PATH = ROOT / ".env"
sys.path.insert(0, str(ROOT))

# ── Tuning ─────────────────────────────────────────────────────────────
CHECK_INTERVAL_SEC = 30               # poll cadence — tight so we catch ±window
MIN_POSITION_USD = 500                # ignore dust
T10_WINDOW = (9 * 60, 11 * 60)        # fire if 9-11 min from kickoff
START_WINDOW = (-60, 120)             # fire if -1 min to +2 min from kickoff
MARKET_CACHE_TTL_SEC = 6 * 3600       # re-fetch gameStartTime every 6h
GAMMA_URL = "https://gamma-api.polymarket.com/markets"

TEXASKID_WALLET = "0xc8075693f48668a264b9fa313b47f52712fcc12b"

# Lazy import for sport formatting
try:
    from monitor.whale_tracker import (  # type: ignore
        format_sport_flag, detect_sport,
    )
except Exception as e:
    logger.warning("whale_tracker import failed (%s) — using fallback", e)
    def detect_sport(title: str, slug: str = "") -> str:  # type: ignore
        t = (title or "").lower()
        if any(k in t for k in ("nhl", "leafs", "oilers")): return "nhl"
        if any(k in t for k in ("nba", "o/u", "pistons")): return "nba"
        if "mlb" in t: return "mlb"
        return "unknown"

    def format_sport_flag(alias, title, slug=""):  # type: ignore
        return ""


# ───────────────────────────────────────────────────────────────────────
#  DB
# ───────────────────────────────────────────────────────────────────────
_ALERTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS game_alerts_sent (
    condition_id TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    game_start_time TEXT,
    PRIMARY KEY (condition_id, alert_type)
);

CREATE INDEX IF NOT EXISTS idx_game_alerts_sent_type
    ON game_alerts_sent(alert_type, sent_at);
"""


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(_ALERTS_SCHEMA)
    conn.commit()


def was_sent(conn: sqlite3.Connection, cid: str, alert_type: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM game_alerts_sent WHERE condition_id = ? AND alert_type = ?",
        (cid, alert_type),
    )
    return cur.fetchone() is not None


def record_sent(
    conn: sqlite3.Connection, cid: str, alert_type: str, game_start: str,
) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO game_alerts_sent
           (condition_id, alert_type, sent_at, game_start_time)
           VALUES (?, ?, ?, ?)""",
        (cid, alert_type, datetime.now(timezone.utc).isoformat(), game_start),
    )
    conn.commit()


# ───────────────────────────────────────────────────────────────────────
#  Env + whale meta
# ───────────────────────────────────────────────────────────────────────
def _load_env() -> None:
    if not ENV_PATH.exists():
        return
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())


# ───────────────────────────────────────────────────────────────────────
#  Fetch open positions (same pattern as market_consensus_radar)
# ───────────────────────────────────────────────────────────────────────
def fetch_open_positions(conn: sqlite3.Connection) -> list[dict]:
    rows: list[dict] = []
    try:
        cur = conn.execute(
            """
            SELECT wallet, alias, condition_id, market_title, direction,
                   current_size_usd, first_seen_price
            FROM tracked_whale_positions
            WHERE status = 'open' AND current_size_usd >= ?
              AND (muted_reason IS NULL OR muted_reason = '')
            """,
            (MIN_POSITION_USD,),
        )
        for r in cur.fetchall():
            rows.append({
                "wallet": (r["wallet"] or "").lower(),
                "alias": r["alias"] or "unknown",
                "condition_id": r["condition_id"],
                "market_title": r["market_title"] or "",
                "direction": r["direction"] or "",
                "size_usd": float(r["current_size_usd"] or 0),
                "entry_price": float(r["first_seen_price"] or 0),
            })
    except sqlite3.OperationalError as e:
        logger.info("tracked_whale_positions unavailable: %s", e)

    try:
        cur = conn.execute(
            """
            SELECT condition_id, market_title, direction, current_size_usd
            FROM texaskid_positions
            WHERE status = 'open' AND current_size_usd >= ?
              AND (muted_reason IS NULL OR muted_reason = '')
            """,
            (MIN_POSITION_USD,),
        )
        for r in cur.fetchall():
            rows.append({
                "wallet": TEXASKID_WALLET,
                "alias": "texaskid",
                "condition_id": r["condition_id"],
                "market_title": r["market_title"] or "",
                "direction": r["direction"] or "",
                "size_usd": float(r["current_size_usd"] or 0),
                "entry_price": 0.0,
            })
    except sqlite3.OperationalError:
        pass

    return rows


# ───────────────────────────────────────────────────────────────────────
#  Gamma API — resolve gameStartTime for each condition_id (cached)
# ───────────────────────────────────────────────────────────────────────
_market_cache: dict[str, tuple[float, Optional[str], str]] = {}
# condition_id -> (fetched_ts, game_start_iso_or_none, slug)


def fetch_game_start(condition_id: str) -> tuple[Optional[str], str]:
    """Return (game_start_iso_utc, market_slug). None if not a game."""
    now = datetime.now(timezone.utc).timestamp()
    cached = _market_cache.get(condition_id)
    if cached and (now - cached[0]) < MARKET_CACHE_TTL_SEC:
        return cached[1], cached[2]

    start: Optional[str] = None
    slug = ""
    for params in (
        {"condition_ids": condition_id},
        {"condition_ids": condition_id, "closed": "true"},
    ):
        qs = urllib.parse.urlencode(params)
        req = urllib.request.Request(
            f"{GAMMA_URL}?{qs}",
            headers={"User-Agent": "Mozilla/5.0 (whale-bot/1.0)"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                if 200 <= resp.status < 300:
                    data = json.loads(resp.read().decode())
                    if isinstance(data, list) and data:
                        raw = data[0]
                        gst = raw.get("gameStartTime")
                        slug = raw.get("slug", "") or slug
                        if gst:
                            start = gst
                            break
                        # fall back to endDate only if it looks like a short game (same-day resolution)
                        end = raw.get("endDate")
                        if end and not start:
                            start = end  # conservative — better than nothing
        except Exception as e:
            logger.debug("gamma fetch failed for %s: %s", condition_id[:12], e)

    _market_cache[condition_id] = (now, start, slug)
    return start, slug


def parse_iso_utc(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


# ───────────────────────────────────────────────────────────────────────
#  Telegram
# ───────────────────────────────────────────────────────────────────────
def send_telegram(message: str) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        logger.warning("Telegram not configured")
        return False
    any_ok = False
    for cid in [c.strip() for c in chat_id.split(",") if c.strip()]:
        data = urllib.parse.urlencode({
            "chat_id": cid, "text": message,
            "parse_mode": "HTML", "disable_web_page_preview": "true",
        }).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage", data=data,
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                if 200 <= resp.status < 300:
                    any_ok = True
                else:
                    logger.warning("Telegram non-2xx: %s", resp.status)
        except Exception as e:
            logger.warning("Telegram send failed: %s", e)
    return any_ok


# ───────────────────────────────────────────────────────────────────────
#  Alert formatting
# ───────────────────────────────────────────────────────────────────────
def format_time_until(delta_sec: float) -> str:
    mins = int(round(delta_sec / 60))
    if delta_sec < 0:
        return f"{abs(mins)} min ago"
    return f"{mins} min"


def format_game_alert(
    alert_type: str,  # 't10' or 'start'
    condition_id: str,
    positions: list[dict],
    game_start_utc: datetime,
) -> str:
    """Build a combined alert for all whales on this game."""
    title = positions[0]["market_title"] or "?"
    slug = positions[0].get("slug", "")
    sport = detect_sport(title, slug)
    sport_emoji = {
        "nhl": "🏒", "nba": "🏀", "mlb": "⚾",
        "ufc": "🥊", "soccer": "⚽",
    }.get(sport, "🎯")

    # Group positions by side, then list each whale in that side
    by_side: dict[str, list[dict]] = {}
    for p in positions:
        by_side.setdefault(p["direction"] or "?", []).append(p)

    # Game time in PST for display
    pst = timezone(timedelta(hours=-7))
    start_pst = game_start_utc.astimezone(pst)
    start_local = start_pst.strftime("%b %d, %I:%M %p PST")

    now = datetime.now(timezone.utc)
    delta_sec = (game_start_utc - now).total_seconds()
    time_until = format_time_until(delta_sec)

    # Header
    if alert_type == "t10":
        header = f"⏰ <b>GAME IN ~10 MINUTES</b> {sport_emoji}"
        time_line = f"<b>Starts in:</b> {time_until} (at {start_local})"
    else:
        header = f"🚨 <b>GAME STARTING NOW</b> {sport_emoji}"
        time_line = f"<b>Tip-off / puck-drop:</b> {start_local}"

    lines: list[str] = [
        header,
        "",
        f"<b>Market:</b> {title}",
        time_line,
        "",
        "<b>Whales on this trade:</b>",
    ]

    total = 0.0
    for side, plist in by_side.items():
        plist.sort(key=lambda x: -x["size_usd"])
        for p in plist:
            alias = p["alias"]
            size = p["size_usd"]
            price = p.get("entry_price", 0)
            total += size
            price_str = f" @ ${price:.3f}" if price > 0 else ""
            lines.append(
                f"  • <b>{alias}</b> on <b>{side}</b>: ${size:,.0f}{price_str}",
            )

    lines += [
        "",
        f"<b>Combined size:</b> ${total:,.0f}",
    ]

    if len(by_side) > 1:
        lines.append("⚠️ <i>Whales are SPLIT across sides on this market</i>")

    pm_url = f"https://polymarket.com/market/{condition_id}"
    lines += [
        "",
        f"<a href=\"{pm_url}\">Open on Polymarket</a>",
    ]
    return "\n".join(lines)


# ───────────────────────────────────────────────────────────────────────
#  Radar
# ───────────────────────────────────────────────────────────────────────
class GameStartRadar:
    def __init__(self, dry_run: bool = False) -> None:
        self.dry_run = dry_run
        self.running = True

    def run_once(self) -> None:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        init_schema(conn)
        try:
            positions = fetch_open_positions(conn)
            if not positions:
                logger.debug("no open positions")
                return

            # Group by condition_id
            by_cid: dict[str, list[dict]] = {}
            for p in positions:
                by_cid.setdefault(p["condition_id"], []).append(p)

            now = datetime.now(timezone.utc)
            logger.debug("scanning %d unique markets", len(by_cid))

            for cid, plist in by_cid.items():
                start_iso, slug = fetch_game_start(cid)
                if not start_iso:
                    continue
                start_utc = parse_iso_utc(start_iso)
                if not start_utc:
                    continue

                # Attach slug for sport detection
                for p in plist:
                    p["slug"] = slug

                delta = (start_utc - now).total_seconds()

                # T-10 window
                if T10_WINDOW[0] <= delta <= T10_WINDOW[1]:
                    if not was_sent(conn, cid, "t10"):
                        msg = format_game_alert("t10", cid, plist, start_utc)
                        self._send(msg, f"T-10 {plist[0]['market_title'][:40]}")
                        record_sent(conn, cid, "t10", start_iso)

                # START window
                if START_WINDOW[0] <= delta <= START_WINDOW[1]:
                    if not was_sent(conn, cid, "start"):
                        msg = format_game_alert("start", cid, plist, start_utc)
                        self._send(msg, f"START {plist[0]['market_title'][:40]}")
                        record_sent(conn, cid, "start", start_iso)

                # If we started past both windows (startup, stale game),
                # record silently so we never fire retroactively.
                if delta < START_WINDOW[0]:
                    if not was_sent(conn, cid, "t10"):
                        record_sent(conn, cid, "t10", start_iso)
                    if not was_sent(conn, cid, "start"):
                        record_sent(conn, cid, "start", start_iso)

        finally:
            conn.close()

    def _send(self, msg: str, label: str) -> None:
        if self.dry_run:
            print(f"=== {label} ===")
            print(msg)
            print("─" * 60)
            return
        ok = send_telegram(msg)
        if ok:
            logger.warning("SENT alert: %s", label)
        else:
            logger.error("FAILED alert: %s", label)

    async def run_forever(self) -> None:
        logger.info(
            "game-start radar started: poll=%ds t10=%s start=%s dry_run=%s",
            CHECK_INTERVAL_SEC, T10_WINDOW, START_WINDOW, self.dry_run,
        )
        while self.running:
            try:
                self.run_once()
            except Exception as e:
                logger.exception("run_once failed: %s", e)
            await asyncio.sleep(CHECK_INTERVAL_SEC)
        logger.info("stopped")

    def stop(self) -> None:
        self.running = False


async def _async_main(dry_run: bool, once: bool) -> int:
    radar = GameStartRadar(dry_run=dry_run)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, radar.stop)
        except NotImplementedError:
            pass
    if once:
        radar.run_once()
    else:
        await radar.run_forever()
    return 0


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )
    parser = argparse.ArgumentParser(description="Whale game-start radar")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    _load_env()
    return asyncio.run(_async_main(args.dry_run, args.once))


if __name__ == "__main__":
    sys.exit(main())
