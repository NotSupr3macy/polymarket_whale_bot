"""
TexasKid VIP Tracker — real-time alerts when texaskid enters a NEW market.

Polls his wallet every 15 seconds, compares positions to baseline.
When a new condition_id appears, sends a Telegram alert immediately.

Runs as its own tmux session alongside bot.py and whale_shadow.py.

Usage:
    python3 monitor/texaskid_tracker.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sqlite3
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-16s | %(message)s",
)
logger = logging.getLogger("texaskid_tracker")

# ── Configuration ─────────────────────────────────────────────────────
DATA_API = "https://data-api.polymarket.com"
TEXASKID_WALLET = "0xc8075693f48668a264b9fa313b47f52712fcc12b"
TEXASKID_ALIAS = "texaskid"

POLL_INTERVAL = 15          # seconds — fast polling for VIP whale
MIN_POSITION_USD = 50       # low threshold to catch his drip-feed entries early
POSITION_LIMIT = 100

DB_PATH = os.getenv("DB_PATH", str(Path(__file__).resolve().parent.parent / "trades.db"))

# ── Load .env file directly (tmux sessions don't always inherit env) ──
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _key, _, _val = _line.partition("=")
                os.environ.setdefault(_key.strip(), _val.strip())

# Telegram — read from env (same as bot.py)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# US Pacific Time
_PST = timezone(timedelta(hours=-7))

# ── Database Setup ────────────────────────────────────────────────────
TRACKER_SCHEMA = """
CREATE TABLE IF NOT EXISTS texaskid_positions (
    condition_id TEXT PRIMARY KEY,
    direction TEXT NOT NULL,
    market_title TEXT NOT NULL,
    first_seen_price REAL,
    first_seen_size_usd REAL,
    current_size_usd REAL,
    current_price REAL,
    status TEXT NOT NULL DEFAULT 'open',
    outcome TEXT,
    pnl REAL,
    first_seen_at TEXT NOT NULL,
    last_updated TEXT,
    alert_sent INTEGER DEFAULT 0,
    resolved_at TEXT
);
"""


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(TRACKER_SCHEMA)
    conn.commit()
    conn.close()
    logger.info("TexasKid tracker DB initialized: %s", DB_PATH)


# ── Telegram ──────────────────────────────────────────────────────────
async def send_telegram(message: str) -> bool:
    """Send a Telegram alert."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram not configured — alert not sent")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    # Support comma-separated chat IDs
    chat_ids = [cid.strip() for cid in TELEGRAM_CHAT_ID.split(",") if cid.strip()]
    any_success = False

    for cid in chat_ids:
        payload = {
            "chat_id": cid,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        any_success = True
                    else:
                        body = await resp.text()
                        logger.warning("Telegram error %d: %s", resp.status, body[:200])
        except Exception as e:
            logger.debug("Telegram send failed: %s", e)

    return any_success


# ── Tracker ───────────────────────────────────────────────────────────
class TexasKidTracker:

    def __init__(self) -> None:
        self.session: aiohttp.ClientSession | None = None
        self.running = False
        # baseline: {condition_id: {title, direction, size_usd, price, ...}}
        self.baseline: dict[str, dict] = {}
        self.first_poll = True

    async def start(self) -> None:
        init_db()
        self.session = aiohttp.ClientSession()
        self.running = True

        # Load known positions from DB so we don't re-alert on restart
        self._load_known_positions()

        logger.info("TexasKid VIP tracker started — polling every %ds", POLL_INTERVAL)

        while self.running:
            try:
                await self._poll_cycle()
            except Exception as e:
                logger.error("Poll cycle error: %s", e)

            await asyncio.sleep(POLL_INTERVAL)

    async def stop(self) -> None:
        self.running = False
        if self.session:
            await self.session.close()
        logger.info("TexasKid tracker stopped")

    def _load_known_positions(self) -> None:
        """Load already-tracked positions from DB to avoid duplicate alerts."""
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT condition_id, direction, market_title, current_size_usd, current_price, status "
                "FROM texaskid_positions"
            ).fetchall()
            conn.close()

            for row in rows:
                self.baseline[row["condition_id"]] = {
                    "title": row["market_title"],
                    "direction": row["direction"],
                    "size_usd": row["current_size_usd"] or 0,
                    "price": row["current_price"] or 0.5,
                    "status": row["status"],
                }

            logger.info("Loaded %d known texaskid positions from DB", len(self.baseline))
        except Exception as e:
            logger.error("Failed to load known positions: %s", e)

    async def _fetch_positions(self) -> list[dict]:
        """Fetch all current positions."""
        positions = []
        offset = 0
        while True:
            params = {
                "user": TEXASKID_WALLET,
                "sizeThreshold": "0.1",
                "limit": str(POSITION_LIMIT),
                "offset": str(offset),
            }
            try:
                async with self.session.get(
                    f"{DATA_API}/v1/positions", params=params,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    if resp.status != 200:
                        break
                    batch = await resp.json()
                    if not batch:
                        break
                    positions.extend(batch)
                    if len(batch) < POSITION_LIMIT:
                        break
                    offset += POSITION_LIMIT
                    await asyncio.sleep(0.2)
            except Exception:
                break
        return positions

    async def _poll_cycle(self) -> None:
        """Main poll: detect new markets, size changes, and resolutions."""
        positions = await self._fetch_positions()

        current: dict[str, dict] = {}
        for pos in positions:
            cid = pos.get("conditionId", "")
            if not cid:
                continue
            initial_value = float(pos.get("initialValue") or 0)
            if initial_value < MIN_POSITION_USD:
                continue

            current[cid] = {
                "title": pos.get("title", ""),
                "direction": pos.get("outcome", "YES"),
                "size_usd": initial_value,
                "price": float(pos.get("avgPrice") or pos.get("curPrice") or 0.5),
                "cur_price": float(pos.get("curPrice") or 0.5),
                "redeemable": pos.get("redeemable", False),
                "size_shares": float(pos.get("size") or 0),
            }

        now = datetime.now(timezone.utc).isoformat()
        now_pst = datetime.now(_PST).strftime("%b %d, %I:%M %p PST")

        # ── Detect NEW markets ─────────────────────────────────────────
        for cid, pos in current.items():
            if cid not in self.baseline:
                if self.first_poll:
                    # First poll — don't alert, just baseline
                    continue

                # Skip already-resolved
                if pos["redeemable"] or pos["cur_price"] >= 0.98 or pos["cur_price"] <= 0.02:
                    continue

                logger.info(
                    "NEW MARKET: %s | %s @ $%.3f | $%.0f | %s",
                    pos["direction"], pos["title"][:50], pos["price"], pos["size_usd"], cid[:16],
                )

                # Record to DB
                try:
                    conn = sqlite3.connect(DB_PATH)
                    conn.execute(
                        """INSERT OR IGNORE INTO texaskid_positions
                           (condition_id, direction, market_title, first_seen_price,
                            first_seen_size_usd, current_size_usd, current_price,
                            status, first_seen_at, last_updated, alert_sent)
                           VALUES (?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, 1)""",
                        (cid, pos["direction"], pos["title"], pos["price"],
                         pos["size_usd"], pos["size_usd"], pos["price"],
                         now, now),
                    )
                    conn.commit()
                    conn.close()
                except Exception as e:
                    logger.error("DB insert failed: %s", e)

                # Send Telegram alert
                msg = (
                    f"🤠 <b>TEXASKID NEW POSITION</b>\n"
                    f"\n"
                    f"<b>Market:</b> {pos['title']}\n"
                    f"<b>Side:</b> {pos['direction']}\n"
                    f"<b>Entry Price:</b> ${pos['price']:.3f}\n"
                    f"<b>Size:</b> ${pos['size_usd']:,.0f}\n"
                    f"\n"
                    f"<i>Time: {now_pst}</i>\n"
                    f"\n"
                    f"💡 He typically drip-feeds limit orders at this price.\n"
                    f"Consider placing your own limit order at ~${pos['price']:.2f}"
                )
                await send_telegram(msg)

        # ── Detect significant SIZE INCREASES on existing positions ─────
        for cid, pos in current.items():
            if cid in self.baseline and not self.first_poll:
                prev = self.baseline[cid]
                prev_size = prev.get("size_usd", 0)
                new_size = pos["size_usd"]

                # Alert if size increased by more than $5K (he's adding to position)
                if new_size - prev_size >= 5000:
                    logger.info(
                        "SIZE UP: %s | $%.0f -> $%.0f (+$%.0f) | %s",
                        pos["title"][:40], prev_size, new_size, new_size - prev_size,
                        pos["direction"],
                    )

                    msg = (
                        f"🤠📈 <b>TEXASKID ADDING SIZE</b>\n"
                        f"\n"
                        f"<b>Market:</b> {pos['title']}\n"
                        f"<b>Side:</b> {pos['direction']}\n"
                        f"<b>Size:</b> ${prev_size:,.0f} → ${new_size:,.0f} (+${new_size - prev_size:,.0f})\n"
                        f"<b>Price:</b> ${pos['price']:.3f}\n"
                        f"\n"
                        f"<i>Time: {now_pst}</i>"
                    )
                    await send_telegram(msg)

                    # Update DB
                    try:
                        conn = sqlite3.connect(DB_PATH)
                        conn.execute(
                            "UPDATE texaskid_positions SET current_size_usd=?, current_price=?, last_updated=? WHERE condition_id=?",
                            (new_size, pos["price"], now, cid),
                        )
                        conn.commit()
                        conn.close()
                    except Exception:
                        pass

        # ── Detect RESOLUTIONS ─────────────────────────────────────────
        for cid, prev in list(self.baseline.items()):
            if prev.get("status") == "closed":
                continue

            cur = current.get(cid)
            resolved = False
            outcome = ""

            if cur is None:
                # Position disappeared — likely resolved
                resolved = True
                outcome = "RESOLVED"
            elif cur["redeemable"]:
                resolved = True
                outcome = "WIN" if cur["cur_price"] >= 0.5 else "LOSS"
            elif cur["cur_price"] >= 0.98:
                resolved = True
                outcome = "WIN"
            elif cur["cur_price"] <= 0.02:
                resolved = True
                outcome = "LOSS"

            if resolved and not self.first_poll:
                title = prev.get("title", "?")
                entry_price = prev.get("price", 0.5)
                size_usd = prev.get("size_usd", 0)

                emoji = "✅" if outcome == "WIN" else "❌" if outcome == "LOSS" else "📋"

                logger.info("RESOLVED: %s | %s | %s", outcome, title[:40], cid[:16])

                msg = (
                    f"🤠{emoji} <b>TEXASKID {outcome}</b>\n"
                    f"\n"
                    f"<b>Market:</b> {title}\n"
                    f"<b>Side:</b> {prev.get('direction', '?')}\n"
                    f"<b>Entry:</b> ${entry_price:.3f} | Size: ${size_usd:,.0f}\n"
                    f"\n"
                    f"<i>Time: {now_pst}</i>"
                )
                await send_telegram(msg)

                # Update DB
                try:
                    conn = sqlite3.connect(DB_PATH)
                    conn.execute(
                        "UPDATE texaskid_positions SET status='closed', outcome=?, resolved_at=? WHERE condition_id=?",
                        (outcome, now, cid),
                    )
                    conn.commit()
                    conn.close()
                except Exception:
                    pass

                # Mark closed in baseline so we don't re-alert
                if cid in self.baseline:
                    self.baseline[cid]["status"] = "closed"

        # Update baseline
        for cid, pos in current.items():
            if cid not in self.baseline or self.baseline[cid].get("status") != "closed":
                self.baseline[cid] = pos

        if self.first_poll:
            logger.info("Baseline captured: %d positions tracked", len(current))
            self.first_poll = False


async def main_async() -> None:
    tracker = TexasKidTracker()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(tracker.stop()))
        except NotImplementedError:
            pass  # Windows

    try:
        await tracker.start()
    except KeyboardInterrupt:
        pass
    finally:
        await tracker.stop()


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
