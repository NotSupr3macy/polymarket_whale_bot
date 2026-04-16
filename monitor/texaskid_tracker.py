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
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple

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


# ── Performance filter (per-whale bet-size × fav/dog gate) ───────────────
@dataclass(frozen=True)
class BetRule:
    """A single allow-rule.

    - favorite=True  matches entry_price >= fav_threshold
    - favorite=False matches entry_price <  fav_threshold
    - favorite=None  matches either side
    - size range is INCLUSIVE-EXCLUSIVE: [min_size_usd, max_size_usd)
    """

    favorite: Optional[bool] = None
    min_size_usd: float = 0.0
    max_size_usd: float = float("inf")


@dataclass(frozen=True)
class PerformanceFilter:
    """OR-semantics over a list of BetRule. enabled=False allows everything."""

    enabled: bool = False
    rules: Tuple[BetRule, ...] = ()
    fav_threshold: float = 0.50

    def allows(self, entry_price: float, size_usd: float) -> bool:
        if not self.enabled:
            return True
        is_fav = entry_price >= self.fav_threshold
        for r in self.rules:
            if r.favorite is not None and r.favorite != is_fav:
                continue
            if size_usd < r.min_size_usd:
                continue
            if size_usd >= r.max_size_usd:
                continue
            return True
        return False


# texaskid: −10% ROI overall was driven by $50K-$150K favorites (−29% on $1.52M).
# Allow any underdog, and favorites only under $50K.
PERFORMANCE_FILTER = PerformanceFilter(
    enabled=True,
    rules=(
        BetRule(favorite=True, min_size_usd=0.0, max_size_usd=50_000),
        BetRule(favorite=False),  # All underdogs allowed
    ),
)


def _should_alert_by_performance(entry_price: float, size_usd: float, reason_ctx: str) -> bool:
    """Gate Telegram alerts by performance filter. DB writes are unaffected."""
    if PERFORMANCE_FILTER.allows(entry_price, size_usd):
        return True
    logger.info(
        "SUPPRESSED alert [texaskid] (performance filter): %s | entry=$%.3f size=$%.0f",
        reason_ctx[:40], entry_price, size_usd,
    )
    return False


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
        # Daily report tracking — only send one per UTC day
        self.last_daily_report_date: str | None = None
        # Size-alert cooldown: {condition_id: (last_alert_ts, size_at_alert)}
        # Suppress duplicate ADDING SIZE alerts for 10 minutes per market,
        # UNLESS the cumulative delta since last alert exceeds $20K (genuine
        # large size-up should break through the cooldown).
        self._size_alert_cooldown: dict[str, tuple[float, float]] = {}
        # Bulletproof backstop: in-memory set of cids we've already processed
        # a resolution for in this process. Prevents re-RESOLVED loops even
        # if the baseline[cid]["status"] = "closed" write is somehow bypassed.
        self._resolved_cids_this_session: set[str] = set()

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
                # Phase 5: per-whale daily reports are replaced by the
                # consolidated `monitor/whale_digest.py` cron job which
                # covers every whale in a single message. Leaving the
                # method on the class but gating it off here.
                # await self._maybe_send_daily_report()
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
                "price": float(
                    pos.get("avgPrice")
                    or (pos.get("curPrice") if pos.get("curPrice") is not None else 0.5)
                ),
                "cur_price": float(pos.get("curPrice")) if pos.get("curPrice") is not None else 0.5,
                "redeemable": pos.get("redeemable", False),
                "size_shares": float(pos.get("size") or 0),
            }

        now = datetime.now(timezone.utc).isoformat()
        now_pst = datetime.now(_PST).strftime("%b %d, %I:%M %p PST")

        # ── Detect NEW markets ─────────────────────────────────────────
        for cid, pos in current.items():
            # Re-open path: baseline marks this cid closed but the whale still
            # holds a healthy (non-rail) position. Fires when a RESOLVED event
            # was triggered by a transient Gamma/API blip while the whale was
            # actually still in the position. Without this, NEW MARKET and
            # baseline-promote both skip while SIZE UP re-fires every poll.
            #
            # Guarded by a 30-min resolution cooldown — once we mark a position
            # RESOLVED we don't re-open for at least 1800 s to prevent
            # curPrice-flicker loops that spam duplicate alerts.
            import time as _time_mod
            _now_ts = _time_mod.time()
            _resolved_at = float(
                self.baseline.get(cid, {}).get("resolved_at_ts") or 0
            )
            _in_cooldown = (_resolved_at > 0) and (_now_ts - _resolved_at < 1800)
            if (
                cid in self.baseline
                and self.baseline[cid].get("status") == "closed"
                and _in_cooldown
            ):
                logger.debug(
                    "re-open blocked by cooldown (%.0fs left): %s",
                    1800 - (_now_ts - _resolved_at), cid[:16],
                )
                continue
            reopening = (
                cid in self.baseline
                and self.baseline[cid].get("status") == "closed"
                and not pos.get("redeemable")
                and 0.02 < pos.get("cur_price", 0.5) < 0.98
                and not _in_cooldown
            )
            if cid not in self.baseline or reopening:
                if self.first_poll:
                    # First poll — don't alert, just baseline
                    continue

                # Skip already-resolved
                if pos["redeemable"] or pos["cur_price"] >= 0.98 or pos["cur_price"] <= 0.02:
                    continue

                if reopening:
                    logger.info(
                        "RE-OPEN: %s | %s @ $%.3f | $%.0f | %s (was status=closed)",
                        pos["direction"], pos["title"][:50], pos["price"],
                        pos["size_usd"], cid[:16],
                    )
                    # Clear closed flag so baseline promote refreshes size_usd.
                    self.baseline[cid]["status"] = None
                    # Clear backstop so re-opened position can be re-tracked.
                    self._resolved_cids_this_session.discard(cid)
                    try:
                        conn = sqlite3.connect(DB_PATH)
                        conn.execute(
                            """UPDATE texaskid_positions
                               SET status='open', outcome=NULL, resolved_at=NULL,
                                   current_size_usd=?, current_price=?, last_updated=?
                               WHERE condition_id=?""",
                            (pos["size_usd"], pos["price"], now, cid),
                        )
                        conn.commit()
                        conn.close()
                    except Exception as e:
                        logger.error("Re-open DB update failed: %s", e)

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

                # Performance filter: gate Telegram (DB insert above is unaffected)
                if _should_alert_by_performance(pos["price"], pos["size_usd"], pos["title"]):
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
        import time as _time
        now_ts = _time.time()
        for cid, pos in current.items():
            if cid in self.baseline and not self.first_poll:  # Skip first poll — baseline may be stale
                prev = self.baseline[cid]
                # Skip positions flagged closed — re-open path above handles this.
                # Without this guard, a falsely-closed row (flaky Gamma / transient
                # /positions blip) keeps firing SIZE UP every poll because
                # promote-to-baseline is gated on status.
                if prev.get("status") == "closed":
                    continue
                prev_size = prev.get("size_usd", 0)
                new_size = pos["size_usd"]

                # Alert if size increased by more than $5K (he's adding to position)
                if new_size - prev_size >= 5000:
                    logger.info(
                        "SIZE UP: %s | $%.0f -> $%.0f (+$%.0f) | %s",
                        pos["title"][:40], prev_size, new_size, new_size - prev_size,
                        pos["direction"],
                    )

                    # Cooldown: suppress Telegram for 10 min per market UNLESS
                    # the cumulative delta since last alert exceeds $20K (genuine
                    # large size-up breaks through the cooldown).
                    last_alert_ts, size_at_alert = self._size_alert_cooldown.get(cid, (0, 0))
                    delta_since_alert = new_size - size_at_alert if size_at_alert else new_size - prev_size
                    cooldown_active = (now_ts - last_alert_ts) < 600
                    breakthrough = delta_since_alert >= 20_000

                    if not cooldown_active or breakthrough:
                        # Performance filter: gate on new total size (not delta)
                        if _should_alert_by_performance(pos["price"], new_size, pos["title"]):
                            self._size_alert_cooldown[cid] = (now_ts, new_size)
                            label = "🤠📈📈 <b>TEXASKID MAJOR SIZE-UP</b>" if breakthrough else "🤠📈 <b>TEXASKID ADDING SIZE</b>"
                            show_delta = delta_since_alert if breakthrough else (new_size - prev_size)
                            msg = (
                                f"{label}\n"
                                f"\n"
                                f"<b>Market:</b> {pos['title']}\n"
                                f"<b>Side:</b> {pos['direction']}\n"
                                f"<b>Size:</b> ${prev_size:,.0f} → ${new_size:,.0f} (+${show_delta:,.0f})\n"
                                f"<b>Price:</b> ${pos['price']:.3f}\n"
                                f"\n"
                                f"<i>Time: {now_pst}</i>"
                            )
                            await send_telegram(msg)
                    else:
                        logger.info(
                            "SIZE UP (cooldown, skipping Telegram): %s | delta_since_alert=$%.0f",
                            pos["title"][:40], delta_since_alert,
                        )

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
            # Belt-and-suspenders: skip if baseline marks closed OR the
            # in-memory backstop set has already processed this cid.
            if prev.get("status") == "closed" or cid in self._resolved_cids_this_session:
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

            if not resolved:
                continue

            title = prev.get("title", "?")

            if self.first_poll:
                # Never send resolution alerts on first poll — baseline may
                # contain positions that resolved while the tracker was down.
                logger.info(
                    "RESOLVED (first poll, skipping alert): %s | %s",
                    outcome, title[:40],
                )
            else:
                entry_price = prev.get("price", 0.5)
                size_usd = prev.get("size_usd", 0)
                emoji = "✅" if outcome == "WIN" else "❌" if outcome == "LOSS" else "📋"
                logger.info("RESOLVED: %s | %s | %s", outcome, title[:40], cid[:16])
                # Performance filter: don't announce a resolution for a bet we
                # suppressed at entry. Uses the same predicate on baseline fields.
                if _should_alert_by_performance(entry_price, size_usd, title):
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

            # Always update DB + baseline + backstop set regardless of first_poll
            self._resolved_cids_this_session.add(cid)
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
            if cid in self.baseline:
                import time as _time_mod
                self.baseline[cid]["status"] = "closed"
                self.baseline[cid]["resolved_at_ts"] = _time_mod.time()

        # Update baseline
        for cid, pos in current.items():
            if cid not in self.baseline or self.baseline[cid].get("status") != "closed":
                self.baseline[cid] = pos

        if self.first_poll:
            # Seed DB with current positions so restarts have accurate baselines.
            # Without this, the DB stays empty and every restart sees a stale
            # baseline causing SIZE UP alert spam.
            now_iso = datetime.now(timezone.utc).isoformat()
            try:
                conn = sqlite3.connect(DB_PATH)
                for cid, pos in current.items():
                    if pos.get("redeemable") or pos.get("cur_price", 0.5) >= 0.98 or pos.get("cur_price", 0.5) <= 0.02:
                        continue  # skip resolved
                    conn.execute(
                        """INSERT OR REPLACE INTO texaskid_positions
                           (condition_id, direction, market_title, first_seen_price,
                            first_seen_size_usd, current_size_usd, current_price,
                            status, first_seen_at, last_updated, alert_sent)
                           VALUES (?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, 1)""",
                        (cid, pos["direction"], pos["title"], pos["price"],
                         pos["size_usd"], pos["size_usd"], pos["price"],
                         now_iso, now_iso),
                    )
                conn.commit()
                conn.close()
            except Exception as e:
                logger.error("DB seed on first poll failed: %s", e)
            logger.info("Baseline captured: %d positions tracked", len(current))
            self.first_poll = False

    async def _maybe_send_daily_report(self) -> None:
        """Send a daily performance summary at 8am PST each day."""
        now_pst = datetime.now(_PST)
        today_str = now_pst.strftime("%Y-%m-%d")

        # Only send once per day, and only after 8am PST
        if now_pst.hour < 8:
            return
        if self.last_daily_report_date == today_str:
            return

        try:
            positions = await self._fetch_positions()
        except Exception as e:
            logger.error("Daily report fetch failed: %s", e)
            return

        # Classify all positions by outcome using live price + redeemable
        wins = 0
        losses = 0
        open_positions = 0
        total_size_open = 0.0
        biggest_win = (0.0, "")
        biggest_loss = (0.0, "")

        for p in positions:
            init = float(p.get("initialValue") or 0)
            if init < MIN_POSITION_USD:
                continue

            cur = float(p.get("curPrice") or 0)
            redeem = p.get("redeemable", False)
            title = p.get("title") or ""

            if redeem or cur >= 0.98 or cur <= 0.02:
                if cur >= 0.98:
                    wins += 1
                    if init > biggest_win[0]:
                        biggest_win = (init, title)
                elif cur <= 0.02:
                    losses += 1
                    if init > biggest_loss[0]:
                        biggest_loss = (init, title)
            else:
                open_positions += 1
                total_size_open += init

        resolved = wins + losses
        wr = (wins / resolved) if resolved else 0.0

        # Build Telegram message
        lines = [
            "🤠📊 <b>TEXASKID DAILY REPORT</b>",
            "",
            f"<b>Record:</b> {wins}W / {losses}L ({wr:.0%} WR)",
            f"<b>Open positions:</b> {open_positions}",
            f"<b>Open exposure:</b> ${total_size_open:,.0f}",
        ]
        if biggest_win[0] > 0:
            lines.append(f"<b>Biggest win:</b> ${biggest_win[0]:,.0f} — {biggest_win[1][:40]}")
        if biggest_loss[0] > 0:
            lines.append(f"<b>Biggest loss:</b> ${biggest_loss[0]:,.0f} — {biggest_loss[1][:40]}")
        lines.append("")
        lines.append(f"<i>Snapshot: {now_pst.strftime('%b %d, %I:%M %p PST')}</i>")

        await send_telegram("\n".join(lines))
        logger.info(
            "Daily report sent: %dW/%dL (%.0f%%), %d open",
            wins, losses, wr * 100, open_positions,
        )
        self.last_daily_report_date = today_str


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
