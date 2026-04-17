"""
Same-market whale consensus radar — alert when 2+ whales open positions on
the **same market and same side** (not just the same sport).

Complements `whale_consensus_radar.py` (which fires on 4+ whales entering
the same *category* in 30 min). This one is tighter and more actionable:
it tells you "kch123 AND bigsix are both on Oilers YES right now — tail it."

Logic:
  1. Poll every CHECK_INTERVAL_SEC.
  2. Query open positions from `tracked_whale_positions` + legacy
     `texaskid_positions`, where current_size_usd >= MIN_POSITION_USD.
  3. Group rows by (condition_id, direction) — identical market & side.
  4. Fire an alert when a group has >= 2 distinct whales.
  5. Dedupe on (condition_id, direction, frozenset(aliases)) — re-fire
     only when a new whale JOINS the cluster (escalation).

Startup seeding:
  On first scan, we record existing consensus silently so the radar
  doesn't dump weeks of historical overlaps the moment it boots. Any
  new or expanded consensus that appears AFTER startup fires normally.

Usage:
    python3 monitor/market_consensus_radar.py              # live
    python3 monitor/market_consensus_radar.py --dry-run    # print only
    python3 monitor/market_consensus_radar.py --once       # single scan

Deployed via deploy/start_market_consensus.sh into its own tmux session.
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

logger = logging.getLogger("market_consensus_radar")

# ── Paths ──────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
DB_PATH = Path(os.getenv("DB_PATH", str(ROOT / "trades.db")))
WHALES_JSON = ROOT / "whales.json"
ENV_PATH = ROOT / ".env"
sys.path.insert(0, str(ROOT))

# ── Tuning ─────────────────────────────────────────────────────────────
CHECK_INTERVAL_SEC = 60            # poll cadence
MIN_POSITION_USD = 500             # ignore dust
ALERT_COOLDOWN_SEC = 7 * 24 * 3600 # re-fire same identical cluster at most once/week
MAX_POSITION_AGE_HOURS = 7 * 24    # ignore positions > 7d old (stale holds)

TEXASKID_WALLET = "0xc8075693f48668a264b9fa313b47f52712fcc12b"

# Lazy import of the sport-flag helper so we reuse the tracker's logic.
try:
    from monitor.whale_tracker import (  # type: ignore
        format_sport_flag, detect_sport, get_sport_confidence,
    )
except Exception as e:
    logger.warning("format_sport_flag import failed (%s) — using fallback", e)
    def detect_sport(title: str, slug: str = "") -> str:  # type: ignore
        t = (title or "").lower()
        if any(k in t for k in ("nhl", "maple leafs", "oilers", "islanders")):
            return "nhl"
        if any(k in t for k in ("nba", "o/u", "pistons", "pacers", "heat")):
            return "nba"
        if "mlb" in t:
            return "mlb"
        return "unknown"

    def get_sport_confidence(alias: str, sport: str) -> str:  # type: ignore
        return "🟡 WARM"

    def format_sport_flag(alias: str, title: str, slug: str = "") -> str:  # type: ignore
        sp = detect_sport(title, slug)
        if sp == "unknown":
            return ""
        emo = {"nhl": "🏒", "nba": "🏀", "mlb": "⚾"}.get(sp, "🎯")
        return f"{emo} {sp.upper()} — {get_sport_confidence(alias, sp)}"


# ───────────────────────────────────────────────────────────────────────
#  Env / whale roster
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


def _load_whale_meta() -> dict[str, dict]:
    """wallet.lower() → {alias, tier, category, solo_enabled, ...}"""
    try:
        with open(WHALES_JSON) as f:
            data = json.load(f)
        return {w.lower(): meta for w, meta in data.items()}
    except Exception as e:
        logger.warning("whales.json load failed: %s", e)
        return {}


# ───────────────────────────────────────────────────────────────────────
#  DB query
# ───────────────────────────────────────────────────────────────────────
def fetch_open_positions(
    conn: sqlite3.Connection, max_age_hours: int,
) -> list[dict]:
    """Return currently-open whale positions across both tables."""
    cutoff = (
        datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    ).isoformat()
    rows: list[dict] = []

    # Generic tracker table
    try:
        cur = conn.execute(
            """
            SELECT wallet, alias, condition_id, market_title,
                   direction, current_size_usd, first_seen_price,
                   current_price, first_seen_at
            FROM tracked_whale_positions
            WHERE status = 'open'
              AND current_size_usd >= ?
              AND first_seen_at >= ?
              AND (muted_reason IS NULL OR muted_reason = '')
            """,
            (MIN_POSITION_USD, cutoff),
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
                "current_price": float(r["current_price"] or 0),
                "first_seen_at": r["first_seen_at"],
            })
    except sqlite3.OperationalError as e:
        logger.info("tracked_whale_positions unavailable: %s", e)

    # Legacy texaskid table (no price columns)
    try:
        cur = conn.execute(
            """
            SELECT condition_id, market_title, direction,
                   current_size_usd, first_seen_at
            FROM texaskid_positions
            WHERE status = 'open'
              AND current_size_usd >= ?
              AND first_seen_at >= ?
              AND (muted_reason IS NULL OR muted_reason = '')
            """,
            (MIN_POSITION_USD, cutoff),
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
                "current_price": 0.0,
                "first_seen_at": r["first_seen_at"],
            })
    except sqlite3.OperationalError:
        pass

    return rows


# ───────────────────────────────────────────────────────────────────────
#  Clustering
# ───────────────────────────────────────────────────────────────────────
def build_same_market_clusters(
    entries: list[dict],
) -> dict[tuple[str, str], list[dict]]:
    """Group by (condition_id, direction). Drop singletons. Dedupe so each
    whale appears once per cluster (largest position wins if a whale has
    duplicate rows across tables, e.g. texaskid double-write)."""
    grouped: dict[tuple[str, str], dict[str, dict]] = {}
    for e in entries:
        key = (e["condition_id"], e["direction"])
        whale_map = grouped.setdefault(key, {})
        existing = whale_map.get(e["alias"])
        if existing is None or e["size_usd"] > existing["size_usd"]:
            whale_map[e["alias"]] = e

    out: dict[tuple[str, str], list[dict]] = {}
    for key, whale_map in grouped.items():
        if len(whale_map) < 2:
            continue
        out[key] = list(whale_map.values())
    return out


# ───────────────────────────────────────────────────────────────────────
#  Telegram
# ───────────────────────────────────────────────────────────────────────
def send_telegram(message: str) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        logger.warning("Telegram not configured — alert dropped")
        return False
    any_ok = False
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
                    any_ok = True
                else:
                    logger.warning("Telegram non-2xx: %s", resp.status)
        except Exception as e:
            logger.warning("Telegram send failed for %s: %s", cid, e)
    return any_ok


def format_alert(
    condition_id: str, direction: str, cluster: list[dict],
) -> str:
    # Sort by size desc — biggest bet first
    whales_sorted = sorted(cluster, key=lambda e: -e["size_usd"])
    n_whales = len(whales_sorted)
    title = whales_sorted[0]["market_title"] or "?"
    combined = sum(e["size_usd"] for e in whales_sorted)

    # Best (lowest) non-zero entry price — this is the price to target
    entry_prices = [e["entry_price"] for e in whales_sorted if e["entry_price"] > 0]
    best_entry = min(entry_prices) if entry_prices else None

    # Sport flag from the first whale (all have same market title anyway)
    lead_alias = whales_sorted[0]["alias"]
    sport = detect_sport(title)
    sport_flag = format_sport_flag(lead_alias, title)

    now_pst = datetime.now(timezone(timedelta(hours=-7))).strftime(
        "%b %d, %I:%M %p PST",
    )

    # Polymarket URL — we only have condition_id, so link to the event page
    # via the generic `/event/` redirect (Polymarket resolves by slug but
    # /market/<cid> also works for the market page).
    pm_url = f"https://polymarket.com/market/{condition_id}"

    lines: list[str] = [
        f"🎯🎯 <b>SAME-MARKET CONSENSUS — {n_whales} WHALES</b>",
        "",
        f"<b>Market:</b> {title}",
        f"<b>Side:</b> {direction}",
    ]
    if sport_flag:
        lines.append(sport_flag)
    lines += ["", "<b>Whales on this trade:</b>"]

    for e in whales_sorted:
        alias = e["alias"]
        size = e["size_usd"]
        entry = e["entry_price"]
        conf = get_sport_confidence(alias, sport) if sport != "unknown" else ""
        price_str = f" @ ${entry:.3f}" if entry > 0 else ""
        conf_str = f" {conf}" if conf else ""
        lines.append(f"  • <b>{alias}</b>: ${size:,.0f}{price_str}{conf_str}")

    lines += [
        "",
        f"<b>Combined size:</b> ${combined:,.0f}",
    ]
    if best_entry is not None:
        lines.append(f"<b>Best recorded entry:</b> ${best_entry:.3f}")
        lines.append(f"<i>Target limit order: ~${best_entry:.2f}</i>")

    lines += [
        "",
        f"<a href=\"{pm_url}\">Open on Polymarket</a>",
        "",
        f"<i>Snapshot: {now_pst}</i>",
    ]
    return "\n".join(lines)


# ───────────────────────────────────────────────────────────────────────
#  Radar runtime
# ───────────────────────────────────────────────────────────────────────
class MarketConsensusRadar:
    """Long-lived polling loop for same-market whale consensus."""

    def __init__(self, dry_run: bool = False, seed_silently: bool = True) -> None:
        self.dry_run = dry_run
        self.running = True
        self.first_scan = seed_silently
        # Dedupe: {cluster_key: (last_sent_ts, frozen_whales)}
        self._fired: dict[str, tuple[float, frozenset[str]]] = {}

    @staticmethod
    def _cluster_key(
        condition_id: str, direction: str, whales: frozenset[str],
    ) -> str:
        return f"{condition_id}|{direction}|{'+'.join(sorted(whales))}"

    def _should_fire(
        self, condition_id: str, direction: str, whales: frozenset[str],
    ) -> bool:
        """Suppress identical or subset clusters within cooldown. A strict
        superset (new whale joined) always fires as an escalation."""
        now = datetime.now(timezone.utc).timestamp()
        key = self._cluster_key(condition_id, direction, whales)
        prev = self._fired.get(key)
        if prev and (now - prev[0]) < ALERT_COOLDOWN_SEC:
            return False
        prefix = f"{condition_id}|{direction}|"
        for pk, (ts, pw) in self._fired.items():
            if not pk.startswith(prefix):
                continue
            if (now - ts) >= ALERT_COOLDOWN_SEC:
                continue
            # Same cluster OR a subset of what already fired → suppress.
            if whales.issubset(pw):
                return False
        return True

    def _record_fire(
        self, condition_id: str, direction: str, whales: frozenset[str],
    ) -> None:
        now = datetime.now(timezone.utc).timestamp()
        self._fired[self._cluster_key(condition_id, direction, whales)] = (
            now, whales,
        )
        # GC entries older than 2× cooldown
        cutoff = now - 2 * ALERT_COOLDOWN_SEC
        self._fired = {k: v for k, v in self._fired.items() if v[0] >= cutoff}

    def run_once(self) -> None:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            entries = fetch_open_positions(conn, MAX_POSITION_AGE_HOURS)
        finally:
            conn.close()

        clusters = build_same_market_clusters(entries)

        if self.first_scan:
            # Seed the fired-set silently so we don't dump weeks of
            # historical overlaps on first boot.
            seeded = 0
            for (cid, direction), cluster in clusters.items():
                whales = frozenset(e["alias"] for e in cluster)
                self._record_fire(cid, direction, whales)
                seeded += 1
            logger.info(
                "startup seed: %d existing consensus clusters recorded "
                "silently (will fire on NEW or expanded clusters only)",
                seeded,
            )
            self.first_scan = False
            return

        if not clusters:
            logger.debug("no same-market consensus right now")
            return

        for (cid, direction), cluster in clusters.items():
            whales = frozenset(e["alias"] for e in cluster)
            if not self._should_fire(cid, direction, whales):
                logger.debug(
                    "suppressing duplicate consensus %s|%s (%s)",
                    cid[:12], direction, sorted(whales),
                )
                continue

            title = cluster[0]["market_title"][:48]
            logger.warning(
                "CONSENSUS FIRED: %s [%s] — %d whales: %s",
                title, direction, len(whales), sorted(whales),
            )
            msg = format_alert(cid, direction, cluster)
            if self.dry_run:
                print(msg)
                print("─" * 60)
            else:
                sent = send_telegram(msg)
                if not sent:
                    logger.error(
                        "alert send failed for %s|%s", cid[:12], direction,
                    )
                    continue
            self._record_fire(cid, direction, whales)

    async def run_forever(self) -> None:
        logger.info(
            "market consensus radar started: min_size=$%d max_age=%dh "
            "poll=%ds cooldown=%dh dry_run=%s",
            MIN_POSITION_USD, MAX_POSITION_AGE_HOURS,
            CHECK_INTERVAL_SEC, ALERT_COOLDOWN_SEC // 3600, self.dry_run,
        )
        while self.running:
            try:
                self.run_once()
            except Exception as e:
                logger.exception("run_once failed: %s", e)
            await asyncio.sleep(CHECK_INTERVAL_SEC)
        logger.info("market consensus radar stopped")

    def stop(self) -> None:
        logger.info("stop requested")
        self.running = False


async def _async_main(dry_run: bool, once: bool, no_seed: bool) -> int:
    radar = MarketConsensusRadar(
        dry_run=dry_run, seed_silently=not no_seed,
    )
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
    parser = argparse.ArgumentParser(description="Same-market whale consensus radar")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print alerts to stdout instead of sending")
    parser.add_argument("--once", action="store_true",
                        help="Run a single scan then exit")
    parser.add_argument("--no-seed", action="store_true",
                        help="Skip silent seeding — fire on all current "
                             "consensus clusters (use for testing only)")
    args = parser.parse_args()
    _load_env()
    return asyncio.run(_async_main(args.dry_run, args.once, args.no_seed))


if __name__ == "__main__":
    sys.exit(main())
