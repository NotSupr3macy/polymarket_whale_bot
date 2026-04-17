"""
Whale consensus radar — alert when 4+ whales enter the same category in 30 min.

Runs as its own long-lived process (like a tracker) in a dedicated tmux
session. Polls `tracked_whale_positions` and `texaskid_positions` every
CHECK_INTERVAL_SEC, groups new entries by sport / market category, and
fires a Telegram alert the instant a category crosses CONSENSUS_THRESHOLD
distinct whales within CONSENSUS_WINDOW_MIN.

Dedupe strategy: each alert is keyed on (category, frozenset(aliases)).
Once a specific cluster of whales has been alerted, it won't re-fire until
the cluster's composition changes (either a new whale joins, raising the
severity, OR the 30-min window slides past one of the original members).

Usage:
    python3 monitor/whale_consensus_radar.py
    python3 monitor/whale_consensus_radar.py --dry-run

Typically launched via `deploy/start_consensus.sh` into a `consensus-radar`
tmux session alongside the other whale trackers.
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

logger = logging.getLogger("consensus_radar")

# ── Paths ──────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
DB_PATH = Path(os.getenv("DB_PATH", str(ROOT / "trades.db")))
WHALES_JSON = ROOT / "whales.json"
ENV_PATH = ROOT / ".env"

# ── Tuning knobs ───────────────────────────────────────────────────────
CONSENSUS_WINDOW_MIN = 30       # rolling look-back in minutes
CONSENSUS_THRESHOLD = 4         # min distinct whales to fire an alert
CHECK_INTERVAL_SEC = 60         # poll cadence
ALERT_COOLDOWN_SEC = 3600       # re-fire the SAME cluster at most hourly
MIN_POSITION_USD = 500          # ignore tiny entries from the radar

# Ensure the radar always exposes itself via sys.path so absolute imports
# work when invoked as a script from tmux.
sys.path.insert(0, str(ROOT))


# ───────────────────────────────────────────────────────────────────────
#  Sport / category classification
# ───────────────────────────────────────────────────────────────────────
# Lazy import so the radar still starts even if the import fails.
try:
    from ev_engine.team_mappings import detect_sport as _detect_sport  # type: ignore
except Exception:
    _detect_sport = None  # type: ignore


_CATEGORY_KEYWORDS: list[tuple[str, str]] = [
    ("UFC", "UFC"),
    ("MLB", "MLB"),
    ("NBA", "NBA"),
    ("NHL", "NHL"),
    ("SOCCER", "SOCCER"),
    ("TENNIS", "TENNIS"),
    ("election", "POLITICS"),
    ("president", "POLITICS"),
    ("senate", "POLITICS"),
    ("congress", "POLITICS"),
    ("bitcoin", "CRYPTO"),
    ("ethereum", "CRYPTO"),
    ("btc ", "CRYPTO"),
    ("eth ", "CRYPTO"),
    ("cs2", "ESPORTS"),
    ("counter-strike", "ESPORTS"),
    ("valorant", "ESPORTS"),
    ("league of legends", "ESPORTS"),
    ("dota", "ESPORTS"),
]


def classify_market(title: str) -> str:
    """Return a coarse category for a market title. Used for clustering."""
    if not title:
        return "OTHER"
    # Try the ev_engine's sport detector first — it's more rigorous.
    if _detect_sport is not None:
        try:
            s = _detect_sport(title)
            if s:
                return s.upper()
        except Exception:
            pass
    lower = title.lower()
    for kw, cat in _CATEGORY_KEYWORDS:
        if kw.lower() in lower:
            return cat
    return "OTHER"


# ───────────────────────────────────────────────────────────────────────
#  DB query: recent entries across every whale
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


def _load_whale_alias_map() -> dict[str, str]:
    """wallet (lowercased) → alias, loaded once from whales.json."""
    try:
        with open(WHALES_JSON) as f:
            data = json.load(f)
        return {w.lower(): meta.get("alias", w[:10]) for w, meta in data.items()}
    except Exception as e:
        logger.warning("Failed to load whales.json: %s", e)
        return {}


def fetch_recent_entries(
    conn: sqlite3.Connection, window_min: int,
) -> list[dict]:
    """Return every whale position with first_seen_at > now-window.

    Draws from both `tracked_whale_positions` (all whales) and the legacy
    `texaskid_positions` (texaskid only). Legacy rows are tagged
    wallet=TEXASKID_WALLET, alias='texaskid' at read time.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=window_min)).isoformat()
    rows: list[dict] = []

    # 1) Generic table
    try:
        cur = conn.execute(
            """
            SELECT wallet, alias, condition_id, market_title,
                   direction, current_size_usd, first_seen_at
            FROM tracked_whale_positions
            WHERE first_seen_at >= ?
              AND status = 'open'
              AND current_size_usd >= ?
              AND (muted_reason IS NULL OR muted_reason = '')
            """,
            (cutoff, MIN_POSITION_USD),
        )
        for r in cur.fetchall():
            rows.append({
                "wallet": (r["wallet"] or "").lower(),
                "alias": r["alias"] or "unknown",
                "condition_id": r["condition_id"],
                "market_title": r["market_title"] or "",
                "direction": r["direction"] or "",
                "size_usd": float(r["current_size_usd"] or 0),
                "first_seen_at": r["first_seen_at"],
            })
    except sqlite3.OperationalError as e:
        logger.info("tracked_whale_positions unavailable: %s", e)

    # 2) Legacy texaskid_positions (same-table dedupe on condition_id at
    #    the cluster level).
    try:
        TEXASKID_WALLET = "0xc8075693f48668a264b9fa313b47f52712fcc12b"
        cur = conn.execute(
            """
            SELECT condition_id, market_title, direction,
                   current_size_usd, first_seen_at
            FROM texaskid_positions
            WHERE first_seen_at >= ?
              AND status = 'open'
              AND current_size_usd >= ?
              AND (muted_reason IS NULL OR muted_reason = '')
            """,
            (cutoff, MIN_POSITION_USD),
        )
        for r in cur.fetchall():
            rows.append({
                "wallet": TEXASKID_WALLET,
                "alias": "texaskid",
                "condition_id": r["condition_id"],
                "market_title": r["market_title"] or "",
                "direction": r["direction"] or "",
                "size_usd": float(r["current_size_usd"] or 0),
                "first_seen_at": r["first_seen_at"],
            })
    except sqlite3.OperationalError:
        pass

    return rows


# ───────────────────────────────────────────────────────────────────────
#  Cluster detection
# ───────────────────────────────────────────────────────────────────────
def build_clusters(entries: list[dict]) -> dict[str, list[dict]]:
    """Group entries by category; return {category: [entries]}."""
    out: dict[str, list[dict]] = {}
    for e in entries:
        cat = classify_market(e["market_title"])
        out.setdefault(cat, []).append(e)
    return out


def distinct_whales(cluster: list[dict]) -> set[str]:
    return {e["alias"] for e in cluster}


# ───────────────────────────────────────────────────────────────────────
#  Telegram sender (stdlib)
# ───────────────────────────────────────────────────────────────────────
def send_telegram(message: str) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        logger.warning("Telegram not configured — alert dropped")
        return False

    any_success = False
    for cid in [c.strip() for c in chat_id.split(",") if c.strip()]:
        data = urllib.parse.urlencode({
            "chat_id": cid,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        }).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=data,
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                if 200 <= resp.status < 300:
                    any_success = True
                else:
                    logger.warning("Telegram non-2xx: %s", resp.status)
        except Exception as e:
            logger.warning("Telegram send failed for %s: %s", cid, e)
    return any_success


def format_alert(category: str, cluster: list[dict]) -> str:
    """Build the HTML alert body for a consensus cluster."""
    whales = sorted(distinct_whales(cluster))
    n_whales = len(whales)

    now_pst = datetime.now(timezone(timedelta(hours=-7))).strftime("%b %d, %I:%M %p PST")

    # Sort entries by size desc for display — the biggest bet draws the eye.
    by_size = sorted(cluster, key=lambda e: -e["size_usd"])

    lines: list[str] = [
        f"🎯 <b>WHALE CONSENSUS — {category}</b>",
        "",
        f"<b>{n_whales} whales</b> entered <b>{category}</b> markets in the last "
        f"{CONSENSUS_WINDOW_MIN} minutes:",
        "",
    ]
    for e in by_size[:10]:  # cap to 10 rows to keep the alert compact
        alias = e["alias"]
        title = (e["market_title"] or "?")[:48]
        size = e["size_usd"]
        side = e["direction"]
        lines.append(f"  • <b>{alias}</b>: {title} ({side}) — ${size:,.0f}")
    if len(by_size) > 10:
        lines.append(f"  • …and {len(by_size) - 10} more")
    lines.append("")
    lines.append(f"<i>Snapshot: {now_pst}</i>")
    return "\n".join(lines)


# ───────────────────────────────────────────────────────────────────────
#  Radar runtime
# ───────────────────────────────────────────────────────────────────────
class ConsensusRadar:
    """Long-lived polling loop that scans for multi-whale category clusters."""

    def __init__(self, dry_run: bool = False) -> None:
        self.dry_run = dry_run
        self.running = True
        # Dedupe: {cluster_key: (last_sent_ts, whale_set)}
        self._fired: dict[str, tuple[float, frozenset[str]]] = {}

    def _cluster_key(self, category: str, whales: frozenset[str]) -> str:
        return f"{category}:{'|'.join(sorted(whales))}"

    def _should_fire(
        self, category: str, whales: frozenset[str],
    ) -> bool:
        """Return True unless the same cluster fired recently."""
        now = datetime.now(timezone.utc).timestamp()
        # Check if the new cluster is a strict superset of a fired one —
        # that means a new whale joined and we should escalate the alert.
        key = self._cluster_key(category, whales)
        prev = self._fired.get(key)
        if prev and (now - prev[0]) < ALERT_COOLDOWN_SEC:
            return False
        # Also check for near-duplicate clusters within the cooldown window
        for prev_key, (ts, prev_whales) in self._fired.items():
            if not prev_key.startswith(f"{category}:"):
                continue
            if (now - ts) >= ALERT_COOLDOWN_SEC:
                continue
            # If the new cluster is identical OR a subset of what already
            # fired, suppress. If it's a strict superset (new whale joined),
            # allow the alert to re-fire so the user sees the escalation.
            if whales.issubset(prev_whales):
                return False
        return True

    def _record_fire(self, category: str, whales: frozenset[str]) -> None:
        now = datetime.now(timezone.utc).timestamp()
        self._fired[self._cluster_key(category, whales)] = (now, whales)
        # Garbage-collect entries older than 3x the cooldown
        cutoff = now - 3 * ALERT_COOLDOWN_SEC
        self._fired = {
            k: v for k, v in self._fired.items() if v[0] >= cutoff
        }

    def run_once(self) -> None:
        """Single scan: fetch recent entries, classify, alert on clusters."""
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            entries = fetch_recent_entries(conn, CONSENSUS_WINDOW_MIN)
        finally:
            conn.close()

        if not entries:
            logger.debug("no recent entries")
            return

        clusters = build_clusters(entries)
        for category, cluster in clusters.items():
            whales = distinct_whales(cluster)
            n = len(whales)
            if n < CONSENSUS_THRESHOLD:
                logger.debug(
                    "cluster %s below threshold (%d whales: %s)",
                    category, n, sorted(whales),
                )
                continue

            frozen = frozenset(whales)
            if not self._should_fire(category, frozen):
                logger.info(
                    "suppressing duplicate cluster %s (%d whales)", category, n,
                )
                continue

            logger.warning(
                "CONSENSUS FIRED: %s with %d whales: %s",
                category, n, sorted(whales),
            )
            msg = format_alert(category, cluster)
            if self.dry_run:
                print(msg)
                print("─" * 60)
            else:
                sent = send_telegram(msg)
                if not sent:
                    logger.error("alert send failed for %s", category)
                    continue
            self._record_fire(category, frozen)

    async def run_forever(self) -> None:
        logger.info(
            "consensus radar started: window=%dmin threshold=%d poll=%ds dry_run=%s",
            CONSENSUS_WINDOW_MIN, CONSENSUS_THRESHOLD,
            CHECK_INTERVAL_SEC, self.dry_run,
        )
        while self.running:
            try:
                self.run_once()
            except Exception as e:
                logger.exception("run_once failed: %s", e)
            await asyncio.sleep(CHECK_INTERVAL_SEC)
        logger.info("consensus radar stopped")

    def stop(self) -> None:
        logger.info("stop requested")
        self.running = False


async def _async_main(dry_run: bool, once: bool) -> int:
    radar = ConsensusRadar(dry_run=dry_run)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, radar.stop)
        except NotImplementedError:
            pass  # Windows

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
    parser = argparse.ArgumentParser(description="Whale consensus radar")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print alerts to stdout instead of sending Telegram",
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Run a single scan then exit (useful for smoke tests)",
    )
    args = parser.parse_args()

    _load_env()
    return asyncio.run(_async_main(args.dry_run, args.once))


if __name__ == "__main__":
    sys.exit(main())
