"""
Phase 2: Shadow Monitor — paper-trade shadow candidates without real money.

Runs as a long-lived process in its own tmux session. Polls shadow candidate
wallets every 30 seconds, records new positions to shadow_trades table,
and resolves them when the market settles.

Usage:
    python3 monitor/whale_shadow.py

Designed to run alongside bot.py without interfering. Shares trades.db
but writes only to the shadow_trades table.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sqlite3
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-16s | %(message)s",
)
logger = logging.getLogger("shadow_monitor")

# ── Configuration ─────────────────────────────────────────────────────
DATA_API = "https://data-api.polymarket.com"
POLL_INTERVAL = 30          # seconds between full poll cycles
POSITION_LIMIT = 100        # max positions to fetch per wallet
MIN_POSITION_USD = 100      # ignore tiny positions (< $100)
RESOLUTION_THRESHOLD = 0.98 # curPrice above this = WIN, below 0.02 = LOSS

SHADOW_PATH = Path(__file__).resolve().parent / "shadow_candidates.json"
DB_PATH = os.getenv("DB_PATH", str(Path(__file__).resolve().parent.parent / "trades.db"))

# ── Database Setup ────────────────────────────────────────────────────
SHADOW_SCHEMA = """
CREATE TABLE IF NOT EXISTS shadow_trades (
    id TEXT PRIMARY KEY,
    wallet TEXT NOT NULL,
    alias TEXT NOT NULL,
    market_id TEXT NOT NULL,
    condition_id TEXT,
    token_id TEXT,
    direction TEXT NOT NULL,
    entry_price REAL NOT NULL,
    entry_size REAL NOT NULL,
    entry_size_usd REAL NOT NULL,
    exit_price REAL,
    pnl REAL,
    outcome TEXT,
    market_title TEXT,
    status TEXT NOT NULL DEFAULT 'open',
    entry_time TEXT NOT NULL,
    exit_time TEXT,
    resolved_at TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_shadow_wallet ON shadow_trades(wallet);
CREATE INDEX IF NOT EXISTS idx_shadow_status ON shadow_trades(status);
CREATE INDEX IF NOT EXISTS idx_shadow_alias ON shadow_trades(alias);
CREATE INDEX IF NOT EXISTS idx_shadow_condition ON shadow_trades(condition_id);
"""


def init_db() -> None:
    """Create shadow_trades table if it doesn't exist."""
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SHADOW_SCHEMA)
    conn.commit()
    conn.close()
    logger.info("Shadow trades DB initialized: %s", DB_PATH)


def load_shadow_candidates() -> dict[str, dict]:
    """Load active shadow candidates from JSON."""
    if not SHADOW_PATH.exists():
        return {}
    try:
        with open(SHADOW_PATH) as f:
            data = json.load(f)
        return {
            wallet: info
            for wallet, info in data.items()
            if info.get("status") == "shadowing"
        }
    except (json.JSONDecodeError, OSError) as e:
        logger.error("Failed to load shadow_candidates.json: %s", e)
        return {}


class ShadowMonitor:
    """Monitors shadow candidate wallets and paper-trades their positions."""

    def __init__(self) -> None:
        self.session: aiohttp.ClientSession | None = None
        self.running = False
        # baseline: {wallet: {condition_id: {size, avgPrice, direction, title, ...}}}
        self.baselines: dict[str, dict[str, dict]] = {}
        # track open shadow trades: {condition_id_wallet: trade_id}
        self.open_trades: dict[str, str] = {}

    async def start(self) -> None:
        """Start the shadow monitor loop."""
        init_db()
        self.session = aiohttp.ClientSession()
        self.running = True

        # Load existing open shadow trades from DB
        self._load_open_trades()

        logger.info("Shadow monitor started")

        while self.running:
            try:
                candidates = load_shadow_candidates()
                if candidates:
                    await self._poll_cycle(candidates)
                else:
                    logger.debug("No shadow candidates to monitor")
            except Exception as e:
                logger.error("Poll cycle error: %s", e)

            await asyncio.sleep(POLL_INTERVAL)

    async def stop(self) -> None:
        """Gracefully stop the monitor."""
        self.running = False
        if self.session:
            await self.session.close()
        logger.info("Shadow monitor stopped")

    def _load_open_trades(self) -> None:
        """Load open shadow trades from DB to resume tracking."""
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT id, wallet, condition_id FROM shadow_trades WHERE status='open'"
            ).fetchall()
            conn.close()

            for row in rows:
                key = f"{row['condition_id']}_{row['wallet']}"
                self.open_trades[key] = row["id"]

            logger.info("Loaded %d open shadow trades from DB", len(self.open_trades))
        except Exception as e:
            logger.error("Failed to load open trades: %s", e)

    async def _poll_cycle(self, candidates: dict[str, dict]) -> None:
        """Poll all shadow candidates for position changes."""
        for wallet, info in candidates.items():
            alias = info.get("alias", wallet[:10])
            try:
                positions = await self._fetch_positions(wallet)
                await self._process_positions(wallet, alias, positions)
                await asyncio.sleep(0.3)  # Rate limiting between wallets
            except Exception as e:
                logger.debug("Error polling %s: %s", alias, e)

    async def _fetch_positions(self, wallet: str) -> list[dict]:
        """Fetch current positions for a wallet."""
        positions = []
        offset = 0
        while True:
            url = f"{DATA_API}/v1/positions"
            params = {
                "user": wallet,
                "sizeThreshold": "0.1",
                "limit": str(POSITION_LIMIT),
                "offset": str(offset),
            }
            try:
                async with self.session.get(
                    url, params=params, timeout=aiohttp.ClientTimeout(total=15)
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

    async def _process_positions(
        self, wallet: str, alias: str, positions: list[dict]
    ) -> None:
        """Compare positions to baseline, detect entries and resolutions."""
        current: dict[str, dict] = {}
        for pos in positions:
            cid = pos.get("conditionId", "")
            if not cid:
                continue
            initial_value = pos.get("initialValue", 0)
            if initial_value < MIN_POSITION_USD:
                continue
            current[cid] = pos

        prev_baseline = self.baselines.get(wallet, {})

        # ── Detect NEW positions (not in baseline) ────────────────────
        for cid, pos in current.items():
            if cid not in prev_baseline:
                key = f"{cid}_{wallet}"
                if key not in self.open_trades:
                    await self._record_entry(wallet, alias, pos)

        # ── Detect RESOLVED positions (in baseline, gone or price settled) ─
        for cid, prev_pos in prev_baseline.items():
            key = f"{cid}_{wallet}"
            if key not in self.open_trades:
                continue

            cur_pos = current.get(cid)

            if cur_pos is None:
                # Position disappeared — check if it was redeemable
                await self._resolve_trade(key, prev_pos, resolved=True)
            elif cur_pos.get("redeemable"):
                # Still visible but redeemable — resolved
                await self._resolve_trade(key, cur_pos, resolved=True)
            else:
                # Check if price has effectively settled
                cur_price = cur_pos.get("curPrice", 0.5)
                if cur_price >= RESOLUTION_THRESHOLD or cur_price <= (1 - RESOLUTION_THRESHOLD):
                    await self._resolve_trade(key, cur_pos, resolved=False)

        # Update baseline
        self.baselines[wallet] = current

    async def _record_entry(self, wallet: str, alias: str, pos: dict) -> None:
        """Record a new shadow trade entry."""
        cid = pos.get("conditionId", "")
        trade_id = str(uuid.uuid4())[:8] + "-shadow"
        key = f"{cid}_{wallet}"

        title = pos.get("title", "")
        direction = pos.get("outcome", "YES")
        avg_price = pos.get("avgPrice", 0.5)
        size = pos.get("size", 0)
        initial_value = pos.get("initialValue", 0)
        now = datetime.now(timezone.utc).isoformat()

        try:
            conn = sqlite3.connect(DB_PATH)
            conn.execute(
                """INSERT OR IGNORE INTO shadow_trades
                   (id, wallet, alias, market_id, condition_id, token_id,
                    direction, entry_price, entry_size, entry_size_usd,
                    market_title, status, entry_time)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)""",
                (
                    trade_id, wallet, alias, cid, cid,
                    pos.get("asset", ""),
                    direction, avg_price, size, initial_value,
                    title, now,
                ),
            )
            conn.commit()
            conn.close()

            self.open_trades[key] = trade_id
            logger.info(
                "SHADOW ENTRY: %s | %s %s @ $%.3f | $%,.0f | %s",
                alias, direction, title[:40], avg_price, initial_value, trade_id,
            )
        except Exception as e:
            logger.error("Failed to record shadow entry: %s", e)

    async def _resolve_trade(
        self, key: str, pos: dict, resolved: bool
    ) -> None:
        """Resolve a shadow trade based on final price."""
        trade_id = self.open_trades.get(key)
        if not trade_id:
            return

        cur_price = pos.get("curPrice", 0.5)
        redeemable = pos.get("redeemable", False)

        # Determine outcome
        if redeemable or cur_price >= RESOLUTION_THRESHOLD:
            if cur_price >= 0.5:
                outcome = "WIN"
                exit_price = 1.0
            else:
                outcome = "LOSS"
                exit_price = 0.0
        elif cur_price <= (1 - RESOLUTION_THRESHOLD):
            outcome = "LOSS"
            exit_price = 0.0
        else:
            # Not clearly resolved yet
            return

        now = datetime.now(timezone.utc).isoformat()

        try:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT entry_price, entry_size, entry_size_usd FROM shadow_trades WHERE id=?",
                (trade_id,)
            ).fetchone()

            if row:
                entry_price = row["entry_price"]
                entry_size = row["entry_size"]
                entry_size_usd = row["entry_size_usd"]

                if outcome == "WIN":
                    pnl = entry_size * (1.0 - entry_price)
                else:
                    pnl = -entry_size_usd

                conn.execute(
                    """UPDATE shadow_trades
                       SET exit_price=?, pnl=?, outcome=?, status='closed',
                           exit_time=?, resolved_at=?
                       WHERE id=?""",
                    (exit_price, round(pnl, 2), outcome, now, now, trade_id),
                )
                conn.commit()

                title = pos.get("title", "?")
                alias = key.split("_")[0][:10]  # rough
                logger.info(
                    "SHADOW RESOLVED: %s | %s | PnL=$%.2f | %s",
                    trade_id, outcome, pnl, title[:40],
                )

            conn.close()
            del self.open_trades[key]

        except Exception as e:
            logger.error("Failed to resolve shadow trade %s: %s", trade_id, e)


async def main_async() -> None:
    """Main entry point with signal handling."""
    monitor = ShadowMonitor()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(monitor.stop()))
        except NotImplementedError:
            pass  # Windows

    try:
        await monitor.start()
    except KeyboardInterrupt:
        pass
    finally:
        await monitor.stop()


def main() -> None:
    """Entry point."""
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
