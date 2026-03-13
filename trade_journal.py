"""
Trade journal — SQLite-backed trade logging and performance analytics.

Persists every trade for:
  - Idempotent restarts (reload open positions on startup)
  - Performance tracking (win rate, PnL, exposure over time)
  - Resolution tracking (market outcome, failure classification)
  - CSV export for external analysis

v3: Separate tables for dry-run vs live.
v4: Added resolution tracking columns (market_title, market_category,
    resolution, payout, outcome, consensus_level, failure_reason,
    entry_delay_seconds, resolved_at). Auto-migrates old schema.
"""

from __future__ import annotations

import csv
import json
import logging
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    condition_id TEXT,
    token_id TEXT NOT NULL,
    direction TEXT NOT NULL,
    entry_price REAL NOT NULL,
    exit_price REAL,
    position_size REAL NOT NULL,
    shares REAL NOT NULL,
    pnl REAL,
    whale_signals TEXT,
    consensus_pct REAL,
    n_whales INTEGER,
    tier_score REAL,
    is_fast_track INTEGER DEFAULT 0,
    entry_time TEXT NOT NULL,
    exit_time TEXT,
    status TEXT NOT NULL DEFAULT 'open',
    exit_reason TEXT,
    order_id TEXT,
    stop_price REAL,
    created_at TEXT DEFAULT (datetime('now')),
    -- v4: Resolution tracking columns
    market_title TEXT,
    market_category TEXT,
    resolution TEXT,
    payout REAL,
    outcome TEXT,
    consensus_level TEXT,
    failure_reason TEXT,
    entry_delay_seconds REAL,
    resolved_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
CREATE INDEX IF NOT EXISTS idx_trades_market ON trades(market_id);
CREATE INDEX IF NOT EXISTS idx_trades_entry_time ON trades(entry_time);
"""

# Columns added in v4 that may not exist in old databases
V4_MIGRATION_COLUMNS = [
    ("market_title", "TEXT"),
    ("market_category", "TEXT"),
    ("resolution", "TEXT"),
    ("payout", "REAL"),
    ("outcome", "TEXT"),
    ("consensus_level", "TEXT"),
    ("failure_reason", "TEXT"),
    ("entry_delay_seconds", "REAL"),
    ("resolved_at", "TEXT"),
]


class TradeJournal:
    """SQLite-backed trade journal with performance analytics.

    v3: Supports separate tables for dry-run vs live trading to prevent
    dry-run phantom positions from polluting live trade data.
    v4: Resolution tracking, failure classification, consensus level logging.
    """

    def __init__(self, db_path: str = "trades.db", dry_run: bool = False):
        self.db_path = db_path
        self.dry_run = dry_run
        self.table_name = "trades_dry" if dry_run else "trades"
        self._conn: sqlite3.Connection | None = None

    def initialize(self) -> None:
        """Create database and tables if they don't exist."""
        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row
        # Always create both tables so --stats works regardless of mode
        self._conn.executescript(SCHEMA)
        # Create dry-run table with same schema
        dry_schema = SCHEMA.replace("trades", "trades_dry")
        self._conn.executescript(dry_schema)
        self._conn.commit()

        # Auto-migrate old databases that lack v4 columns
        self._migrate_v4()

        logger.info("TradeJournal initialized: %s (table=%s)", self.db_path, self.table_name)

    def _migrate_v4(self) -> None:
        """Add v4 resolution tracking columns to existing tables if missing."""
        assert self._conn is not None

        for table in ("trades", "trades_dry"):
            # Get existing columns
            cursor = self._conn.execute(f"PRAGMA table_info({table})")
            existing_cols = {row[1] for row in cursor.fetchall()}

            for col_name, col_type in V4_MIGRATION_COLUMNS:
                if col_name not in existing_cols:
                    try:
                        self._conn.execute(
                            f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}"
                        )
                        logger.info("Migrated %s: added column %s %s", table, col_name, col_type)
                    except sqlite3.OperationalError:
                        pass  # Column already exists (race condition)

        self._conn.commit()

    def clean_open_positions(self) -> int:
        """Close all open positions from previous sessions (for --clean flag).
        Returns the number of positions cleaned."""
        assert self._conn is not None
        cursor = self._conn.execute(
            f"UPDATE {self.table_name} SET status = 'cleaned', exit_reason = 'manual_clean', "
            f"pnl = 0, exit_time = ? WHERE status = 'open'",
            (time.strftime("%Y-%m-%d %H:%M:%S"),),
        )
        self._conn.commit()
        count = cursor.rowcount
        if count > 0:
            logger.info("Cleaned %d stale open positions from %s", count, self.table_name)
        return count

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            logger.info("TradeJournal closed")

    def log_entry(
        self,
        market_id: str,
        condition_id: str,
        token_id: str,
        direction: str,
        entry_price: float,
        position_size: float,
        shares: float,
        whale_signals: list[str],
        consensus_pct: float,
        n_whales: int,
        tier_score: float,
        is_fast_track: bool,
        order_id: str,
        stop_price: float,
        consensus_level: str = "EXACT_MARKET",
        market_title: str = "",
        entry_delay_seconds: float = 0.0,
    ) -> str:
        """Log a new trade entry. Returns the trade ID."""
        assert self._conn is not None
        trade_id = str(uuid.uuid4())[:12]

        self._conn.execute(
            f"""
            INSERT INTO {self.table_name} (
                id, market_id, condition_id, token_id, direction,
                entry_price, position_size, shares, whale_signals,
                consensus_pct, n_whales, tier_score, is_fast_track,
                entry_time, status, order_id, stop_price,
                consensus_level, market_title, entry_delay_seconds
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, ?)
            """,
            (
                trade_id,
                market_id,
                condition_id,
                token_id,
                direction,
                entry_price,
                position_size,
                shares,
                json.dumps(whale_signals),
                consensus_pct,
                n_whales,
                tier_score,
                1 if is_fast_track else 0,
                time.strftime("%Y-%m-%d %H:%M:%S"),
                order_id,
                stop_price,
                consensus_level,
                market_title,
                entry_delay_seconds,
            ),
        )
        self._conn.commit()
        logger.debug("Journal entry: %s %s $%.2f (consensus=%s)", trade_id, direction, position_size, consensus_level)
        return trade_id

    def log_exit(
        self,
        trade_id: str,
        exit_price: float,
        pnl: float,
        exit_reason: str,
    ) -> None:
        """Log a trade exit (close, stop-loss, take-profit, whale exit)."""
        assert self._conn is not None

        self._conn.execute(
            f"""
            UPDATE {self.table_name} SET
                exit_price = ?,
                pnl = ?,
                exit_time = ?,
                status = 'closed',
                exit_reason = ?
            WHERE id = ?
            """,
            (
                exit_price,
                pnl,
                time.strftime("%Y-%m-%d %H:%M:%S"),
                exit_reason,
                trade_id,
            ),
        )
        self._conn.commit()
        logger.debug("Journal exit: %s pnl=$%.2f (%s)", trade_id, pnl, exit_reason)

    def resolve_position(
        self,
        trade_id: str,
        resolution: str,
        payout: float,
        outcome: str,
        failure_reason: str = "",
    ) -> None:
        """
        Record the market resolution for a closed trade.

        Args:
            trade_id: The trade to resolve
            resolution: Market resolution (e.g., "YES", "NO", "VOID")
            payout: Actual payout per share ($1.00 for win, $0 for loss)
            outcome: "WIN", "LOSS", or "VOID"
            failure_reason: Category from classify_failure() if a loss
        """
        assert self._conn is not None

        self._conn.execute(
            f"""
            UPDATE {self.table_name} SET
                resolution = ?,
                payout = ?,
                outcome = ?,
                failure_reason = ?,
                resolved_at = ?
            WHERE id = ?
            """,
            (
                resolution,
                payout,
                outcome,
                failure_reason,
                time.strftime("%Y-%m-%d %H:%M:%S"),
                trade_id,
            ),
        )
        self._conn.commit()
        logger.info(
            "Trade resolved: %s -> %s (payout=$%.2f, outcome=%s, reason=%s)",
            trade_id, resolution, payout, outcome, failure_reason,
        )

    def update_trade(self, trade_id: str, **kwargs) -> None:
        """Generic update for any trade field(s). Use for market_title, category, etc."""
        assert self._conn is not None

        if not kwargs:
            return

        set_clauses = ", ".join(f"{k} = ?" for k in kwargs)
        values = list(kwargs.values()) + [trade_id]

        self._conn.execute(
            f"UPDATE {self.table_name} SET {set_clauses} WHERE id = ?",
            values,
        )
        self._conn.commit()

    def get_open_positions(self) -> list[dict]:
        """Get all currently open positions (for restart recovery)."""
        assert self._conn is not None
        cursor = self._conn.execute(
            f"SELECT * FROM {self.table_name} WHERE status = 'open' ORDER BY entry_time"
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_closed_trades(self, limit: int = 100) -> list[dict]:
        """Get recent closed trades."""
        assert self._conn is not None
        cursor = self._conn.execute(
            f"SELECT * FROM {self.table_name} WHERE status = 'closed' ORDER BY exit_time DESC LIMIT ?",
            (limit,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_all_closed_trades(self) -> list[dict]:
        """Get ALL closed trades (for analytics)."""
        assert self._conn is not None
        cursor = self._conn.execute(
            f"SELECT * FROM {self.table_name} WHERE status = 'closed' ORDER BY exit_time"
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_unresolved_trades(self) -> list[dict]:
        """Get closed trades that haven't been resolved yet (no resolution field)."""
        assert self._conn is not None
        cursor = self._conn.execute(
            f"""
            SELECT * FROM {self.table_name}
            WHERE status = 'closed' AND (resolution IS NULL OR resolution = '')
            ORDER BY exit_time
            """
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_whale_outcomes(self) -> dict[str, dict]:
        """
        Get per-whale performance data for analytics and score updates.

        Returns: {alias: {wins: int, losses: int, total_pnl: float, trades: int}}
        """
        assert self._conn is not None
        cursor = self._conn.execute(
            f"SELECT * FROM {self.table_name} WHERE status = 'closed' AND outcome IS NOT NULL"
        )
        trades = [dict(row) for row in cursor.fetchall()]

        whale_stats: dict[str, dict] = {}
        for t in trades:
            aliases_raw = t.get("whale_signals", "[]")
            try:
                aliases = json.loads(aliases_raw)
            except (json.JSONDecodeError, TypeError):
                aliases = []

            outcome = t.get("outcome", "")
            pnl = t.get("pnl", 0.0) or 0.0

            for alias in aliases:
                if alias not in whale_stats:
                    whale_stats[alias] = {"wins": 0, "losses": 0, "total_pnl": 0.0, "trades": 0}

                whale_stats[alias]["trades"] += 1
                whale_stats[alias]["total_pnl"] += pnl
                if outcome == "WIN":
                    whale_stats[alias]["wins"] += 1
                elif outcome == "LOSS":
                    whale_stats[alias]["losses"] += 1

        return whale_stats

    def get_performance_stats(self) -> dict:
        """Calculate overall performance statistics."""
        assert self._conn is not None
        cursor = self._conn.execute(
            f"SELECT * FROM {self.table_name} WHERE status = 'closed'"
        )
        trades = [dict(row) for row in cursor.fetchall()]

        if not trades:
            return {
                "total_trades": 0,
                "win_rate": 0.0,
                "total_pnl": 0.0,
                "avg_pnl": 0.0,
                "best_trade": 0.0,
                "worst_trade": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "profit_factor": 0.0,
                "open_positions": len(self.get_open_positions()),
            }

        pnls = [t["pnl"] or 0.0 for t in trades]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]
        total_pnl = sum(pnls)
        gross_profit = sum(wins) if wins else 0.0
        gross_loss = abs(sum(losses)) if losses else 0.0

        return {
            "total_trades": len(trades),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": len(wins) / len(trades) if trades else 0.0,
            "total_pnl": total_pnl,
            "avg_pnl": total_pnl / len(trades),
            "best_trade": max(pnls) if pnls else 0.0,
            "worst_trade": min(pnls) if pnls else 0.0,
            "avg_win": gross_profit / len(wins) if wins else 0.0,
            "avg_loss": -gross_loss / len(losses) if losses else 0.0,
            "profit_factor": gross_profit / gross_loss if gross_loss > 0 else float("inf"),
            "open_positions": len(self.get_open_positions()),
            "by_exit_reason": self._count_by_exit_reason(trades),
            "by_direction": self._count_by_direction(trades),
        }

    def _count_by_exit_reason(self, trades: list[dict]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for t in trades:
            reason = t.get("exit_reason", "unknown")
            counts[reason] = counts.get(reason, 0) + 1
        return counts

    def _count_by_direction(self, trades: list[dict]) -> dict[str, dict]:
        result: dict[str, dict] = {}
        for t in trades:
            d = t.get("direction", "?")
            if d not in result:
                result[d] = {"count": 0, "pnl": 0.0}
            result[d]["count"] += 1
            result[d]["pnl"] += t.get("pnl", 0.0) or 0.0
        return result

    def export_csv(self, filepath: str = "trades_export.csv") -> str:
        """Export all trades to CSV."""
        assert self._conn is not None

        cursor = self._conn.execute(f"SELECT * FROM {self.table_name} ORDER BY entry_time")
        trades = cursor.fetchall()

        if not trades:
            logger.warning("No trades to export")
            return filepath

        columns = [description[0] for description in cursor.description]

        with open(filepath, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            for trade in trades:
                writer.writerow(trade)

        logger.info("Exported %d trades to %s", len(trades), filepath)
        return filepath

    def get_daily_summary(self) -> dict:
        """Get today's trading summary."""
        assert self._conn is not None
        today = time.strftime("%Y-%m-%d")

        cursor = self._conn.execute(
            f"""
            SELECT
                COUNT(*) as trades,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                SUM(pnl) as total_pnl,
                MAX(pnl) as best,
                MIN(pnl) as worst
            FROM {self.table_name}
            WHERE status = 'closed' AND exit_time LIKE ?
            """,
            (f"{today}%",),
        )
        row = cursor.fetchone()
        return dict(row) if row else {}
