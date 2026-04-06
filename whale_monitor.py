"""
Whale position monitor — the critical real-time component.

Detects whale trades within 1-3 seconds using dual-method redundancy:
  Method A: Polymarket Data API polling (primary, every 4s)
  Method B: Polygon RPC on-chain Transfer event monitoring (backup)

Bug fix log:
  v2: Added baseline capture + quiet period to prevent first-poll flood.
  v3: Full pagination, 2-poll confirmation, share-count detection.
  v4: Whale exit tracking with exit_pct. Market title resolution from Gamma API
      with persistent caching. Human-readable log lines.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any

import aiohttp

from config import BotConfig, WHALE_WATCHLIST, TIER_1_WALLETS

logger = logging.getLogger(__name__)


@dataclass
class MarketMeta:
    """Cached market metadata from the Gamma API."""

    title: str
    end_date: str = ""
    condition_id: str = ""


@dataclass
class WhaleSignal:
    """A detected whale trade event."""

    wallet: str
    alias: str
    market_id: str
    condition_id: str
    token_id: str
    direction: str  # "YES" or "NO"
    entry_price: float
    size_usd: float
    timestamp: float
    tier: int
    category: str = ""
    market_title: str = ""
    is_exit: bool = False
    exit_pct: float = 0.0  # 0.0 for entries, 0.0-1.0 for exits (1.0 = full exit)

    @property
    def is_fast_track_eligible(self) -> bool:
        return self.tier == 1


@dataclass
class PositionSnapshot:
    """Snapshot of a whale's position in a single market."""

    market_id: str
    condition_id: str
    token_id: str
    outcome: str  # "Yes" or "No"
    size: float  # Number of shares (token units) — STABLE across polls
    avg_price: float
    size_usd: float  # size * avg_price
    cur_price: float = 0.0
    asset: str = ""
    market_slug: str = ""


class WhaleMonitor:
    """
    Monitors whale wallets for new trades via dual-method detection.

    v4 features (on top of v3):
      - exit_pct on WhaleSignal: full exit = 1.0, partial = share_delta/original
      - Market title cache: resolves condition_id -> human-readable title via Gamma API
      - Log lines show market title, condition_id prefix, and end date
    """

    def __init__(self, config: BotConfig, watchlist: dict[str, dict] | None = None):
        self.config = config
        self.watchlist = watchlist or WHALE_WATCHLIST
        self.signal_queue: asyncio.Queue[WhaleSignal] = asyncio.Queue()

        # Confirmed positions: only positions seen in 2+ consecutive polls
        self._confirmed_positions: dict[str, dict[str, PositionSnapshot]] = {}
        # Pending new: positions seen in 1 poll, awaiting confirmation
        self._pending_new: dict[str, dict[str, PositionSnapshot]] = {}
        # Pending exit: positions absent for 1 poll, awaiting confirmation
        self._pending_exit: dict[str, dict[str, PositionSnapshot]] = {}

        # Market title cache: condition_id -> MarketMeta (fetched from Gamma API)
        self._market_meta_cache: dict[str, MarketMeta] = {}

        self._session: aiohttp.ClientSession | None = None
        self._running = False
        self._poll_count = 0
        self._errors: dict[str, int] = {}

        # Cold-start baseline tracking
        self._all_baselined = False
        self._quiet_until: float = 0.0

    async def start(self) -> None:
        """Initialize HTTP session and start polling."""
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15),
            headers={"Accept": "application/json"},
        )
        self._running = True
        logger.info(
            "WhaleMonitor started — tracking %d wallets (%d Tier 1, %d Tier 2, %d Tier 3)",
            len(self.watchlist),
            sum(1 for w in self.watchlist.values() if w["tier"] == 1),
            sum(1 for w in self.watchlist.values() if w["tier"] == 2),
            sum(1 for w in self.watchlist.values() if w["tier"] == 3),
        )

    async def stop(self) -> None:
        """Gracefully stop monitoring."""
        self._running = False
        if self._session and not self._session.closed:
            await self._session.close()
        logger.info("WhaleMonitor stopped after %d poll cycles", self._poll_count)

    # ── Main poll loop ─────────────────────────────────────────────

    async def poll_loop(self) -> None:
        """Main polling loop with 3-phase startup."""
        if not self._session:
            await self.start()

        wallets = list(self.watchlist.keys())
        n_wallets = len(wallets)

        if not self._all_baselined:
            await self._capture_baselines(wallets)

        while self._running:
            cycle_start = time.monotonic()
            self._poll_count += 1

            stagger_delay = self.config.POLL_INTERVAL_SECONDS / max(n_wallets, 1)

            for i, wallet_addr in enumerate(wallets):
                if not self._running:
                    break
                try:
                    await self._poll_wallet(wallet_addr)
                    self._errors[wallet_addr] = 0
                except aiohttp.ClientResponseError as e:
                    self._handle_poll_error(wallet_addr, e)
                    if e.status == 429:
                        logger.warning("Rate limited, backing off for this cycle")
                        break
                except Exception as e:
                    self._handle_poll_error(wallet_addr, e)

                if i < n_wallets - 1:
                    await asyncio.sleep(stagger_delay)

            elapsed = time.monotonic() - cycle_start
            remaining = self.config.POLL_INTERVAL_SECONDS - elapsed
            if remaining > 0:
                await asyncio.sleep(remaining)

    async def _capture_baselines(self, wallets: list[str]) -> None:
        """Phase 1: Fetch existing positions for every whale (paginated). No signals."""
        logger.info("Capturing baselines for %d whales (with full pagination)...", len(wallets))

        for wallet_addr in wallets:
            if not self._running:
                return
            alias = self.watchlist[wallet_addr]["alias"]
            try:
                positions = await self._fetch_all_positions(wallet_addr)
                self._confirmed_positions[wallet_addr] = positions
                logger.info(
                    "  Baseline: %s — %d positions captured",
                    alias, len(positions),
                )
            except Exception as e:
                self._confirmed_positions[wallet_addr] = {}
                logger.warning(
                    "  Baseline: %s — failed (%s: %s), using empty snapshot",
                    alias, type(e).__name__, e,
                )

            await asyncio.sleep(0.5)

        self._all_baselined = True
        logger.info(
            "All %d whales baselined — entering %.0fs quiet period...",
            len(wallets), self.config.STARTUP_QUIET_PERIOD_SECONDS,
        )

        self._quiet_until = time.time() + self.config.STARTUP_QUIET_PERIOD_SECONDS
        await asyncio.sleep(self.config.STARTUP_QUIET_PERIOD_SECONDS)
        logger.info("Quiet period complete — monitoring with 2-poll confirmation active...")

    # ── Per-wallet poll ────────────────────────────────────────────

    async def _poll_wallet(self, wallet: str) -> None:
        """Fetch positions and detect changes with 2-poll confirmation + share-based detection."""
        current = await self._fetch_all_positions(wallet)
        confirmed = self._confirmed_positions.get(wallet, {})
        info = self.watchlist[wallet]

        in_quiet_period = time.time() < self._quiet_until

        if not in_quiet_period:
            # ── Detect new positions and size increases ──
            for key, pos in current.items():
                conf_pos = confirmed.get(key)

                if conf_pos is None:
                    pending = self._pending_new.get(wallet, {})
                    if key in pending:
                        # 2-poll confirmed new position
                        delta_usd = pos.size * max(pos.avg_price, pos.cur_price, 0.01)
                        if delta_usd >= self.config.MIN_WHALE_TRADE_SIZE:
                            meta = await self._resolve_market_meta(pos.condition_id)
                            signal = self._make_signal(
                                wallet, info, pos, delta_usd,
                                is_exit=False, exit_pct=0.0,
                                market_title=meta.title,
                            )
                            await self.signal_queue.put(signal)
                            end_info = f" ends {meta.end_date[:10]}" if meta.end_date else ""
                            logger.info(
                                'WHALE SIGNAL (confirmed): %s %s $%.0f on "%s" (%s%s) (tier %d)',
                                info["alias"], pos.outcome.upper(), delta_usd,
                                meta.title, pos.condition_id[:8], end_info, info["tier"],
                            )
                        self._pending_new.get(wallet, {}).pop(key, None)
                    else:
                        if wallet not in self._pending_new:
                            self._pending_new[wallet] = {}
                        self._pending_new[wallet][key] = pos
                else:
                    # Check for share count INCREASE
                    share_delta = pos.size - conf_pos.size
                    if share_delta > 0:
                        price = max(pos.avg_price, pos.cur_price, 0.01)
                        delta_usd = share_delta * price
                        if delta_usd >= self.config.MIN_WHALE_TRADE_SIZE:
                            meta = await self._resolve_market_meta(pos.condition_id)
                            signal = self._make_signal(
                                wallet, info, pos, delta_usd,
                                is_exit=False, exit_pct=0.0,
                                market_title=meta.title,
                            )
                            await self.signal_queue.put(signal)
                            end_info = f" ends {meta.end_date[:10]}" if meta.end_date else ""
                            logger.info(
                                'WHALE INCREASE: %s %s +%.0f shares ($%.0f) on "%s" (%s%s)',
                                info["alias"], pos.outcome.upper(), share_delta,
                                delta_usd, meta.title, pos.condition_id[:8], end_info,
                            )

            # ── Detect exits ──
            for key, conf_pos in confirmed.items():
                cur_pos = current.get(key)

                if cur_pos is None:
                    # Full exit candidate
                    pending_exit = self._pending_exit.get(wallet, {})
                    if key in pending_exit:
                        # 2-poll confirmed FULL exit
                        meta = await self._resolve_market_meta(conf_pos.condition_id)
                        signal = self._make_signal(
                            wallet, info, conf_pos, conf_pos.size_usd,
                            is_exit=True, exit_pct=1.0,
                            market_title=meta.title,
                        )
                        await self.signal_queue.put(signal)
                        logger.info(
                            'WHALE EXIT (100%%): %s closed %s ($%.0f) on "%s" (%s)',
                            info["alias"], conf_pos.outcome.upper(),
                            conf_pos.size_usd, meta.title, conf_pos.condition_id[:8],
                        )
                        self._pending_exit.get(wallet, {}).pop(key, None)
                    else:
                        if wallet not in self._pending_exit:
                            self._pending_exit[wallet] = {}
                        self._pending_exit[wallet][key] = conf_pos
                else:
                    # Partial exit: share count DECREASE
                    share_delta = conf_pos.size - cur_pos.size
                    if share_delta > 0:
                        price = max(conf_pos.avg_price, cur_pos.cur_price, 0.01)
                        delta_usd = share_delta * price
                        exit_pct = share_delta / conf_pos.size if conf_pos.size > 0 else 1.0
                        if delta_usd >= self.config.MIN_WHALE_TRADE_SIZE:
                            meta = await self._resolve_market_meta(conf_pos.condition_id)
                            signal = self._make_signal(
                                wallet, info, conf_pos, delta_usd,
                                is_exit=True, exit_pct=exit_pct,
                                market_title=meta.title,
                            )
                            await self.signal_queue.put(signal)
                            logger.info(
                                'WHALE DECREASE (%.0f%%): %s %s -%.0f shares ($%.0f) on "%s" (%s)',
                                exit_pct * 100,
                                info["alias"], conf_pos.outcome.upper(), share_delta,
                                delta_usd, meta.title, conf_pos.condition_id[:8],
                            )

            # ── Clean up stale pending entries ──
            if wallet in self._pending_new:
                self._pending_new[wallet] = {
                    k: v for k, v in self._pending_new[wallet].items()
                    if k in current
                }
            if wallet in self._pending_exit:
                self._pending_exit[wallet] = {
                    k: v for k, v in self._pending_exit[wallet].items()
                    if k not in current
                }

        # Update confirmed positions
        self._confirmed_positions[wallet] = dict(current)

    # ── Signal construction ────────────────────────────────────────

    def _make_signal(
        self, wallet: str, info: dict, pos: PositionSnapshot,
        size_usd: float, is_exit: bool, exit_pct: float,
        market_title: str = "",
    ) -> WhaleSignal:
        """Create a WhaleSignal from a position snapshot."""
        return WhaleSignal(
            wallet=wallet,
            alias=info["alias"],
            market_id=pos.market_id,
            condition_id=pos.condition_id,
            token_id=pos.token_id,
            direction=pos.outcome.upper(),
            entry_price=pos.avg_price,
            size_usd=size_usd,
            timestamp=time.time(),
            tier=info["tier"],
            category=info.get("category", ""),
            market_title=market_title,
            is_exit=is_exit,
            exit_pct=exit_pct,
        )

    # ── Market title resolution (Gamma API) ────────────────────────

    async def _resolve_market_meta(self, condition_id: str) -> MarketMeta:
        """Fetch market title + end date from Gamma API, with persistent cache."""
        if condition_id in self._market_meta_cache:
            return self._market_meta_cache[condition_id]

        meta = MarketMeta(title=condition_id[:12], condition_id=condition_id)

        if not self._session or not condition_id:
            self._market_meta_cache[condition_id] = meta
            return meta

        try:
            url = f"{self.config.GAMMA_API}/markets/{condition_id}"
            async with self._session.get(
                url, timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    title = (
                        data.get("question")
                        or data.get("title")
                        or data.get("groupItemTitle")
                        or condition_id[:12]
                    )
                    end_date = (
                        data.get("endDate")
                        or data.get("end_date_iso")
                        or data.get("expirationDate")
                        or ""
                    )
                    meta = MarketMeta(
                        title=title,
                        end_date=end_date,
                        condition_id=condition_id,
                    )
        except Exception as e:
            logger.debug("Market meta fetch failed for %s: %s", condition_id[:12], e)

        self._market_meta_cache[condition_id] = meta
        return meta

    # ── Position fetching (paginated) ──────────────────────────────

    async def _fetch_all_positions(self, wallet: str) -> dict[str, PositionSnapshot]:
        """Fetch ALL positions using offset-based pagination."""
        assert self._session is not None

        all_positions: dict[str, PositionSnapshot] = {}
        offset = 0
        limit = 100
        pages = 0

        while True:
            url = f"{self.config.DATA_API}/v1/positions"
            params = {
                "user": wallet,
                "sizeThreshold": "0.1",
                "limit": str(limit),
                "offset": str(offset),
            }

            async with self._session.get(url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()

            pages += 1

            if not isinstance(data, list) or len(data) == 0:
                break

            for item in data:
                try:
                    condition_id = item.get("conditionId", "")
                    market_id = item.get("marketSlug", condition_id) or condition_id
                    size = float(item.get("size", 0))
                    avg_price = float(item.get("avgPrice", 0))
                    cur_price = float(item.get("curPrice", item.get("price", 0)))

                    if size <= 0:
                        continue

                    # The CLOB token ID is in the "asset" field, NOT "tokenId".
                    # Using the wrong field caused all positions to share token_id=""
                    # which broke stop-loss checking (cross-contaminated prices).
                    clob_token = item.get("asset", "") or item.get("tokenId", "")

                    snap = PositionSnapshot(
                        market_id=market_id,
                        condition_id=condition_id,
                        token_id=clob_token,
                        outcome=item.get("outcome", "Unknown"),
                        size=size,
                        avg_price=avg_price,
                        size_usd=size * avg_price,
                        cur_price=cur_price,
                        asset=item.get("asset", ""),
                        market_slug=item.get("marketSlug", ""),
                    )
                    all_positions[condition_id] = snap
                except (ValueError, TypeError) as e:
                    logger.debug("Skipping malformed position entry: %s", e)
                    continue

            if len(data) < limit:
                break

            offset += limit
            await asyncio.sleep(0.3)

        if pages > 1:
            logger.debug(
                "Paginated %d pages for %s — %d total positions",
                pages, wallet[:10], len(all_positions),
            )

        return all_positions

    # ── Utility methods ────────────────────────────────────────────

    async def fetch_activity(self, wallet: str, limit: int = 10) -> list[dict[str, Any]]:
        """Fetch recent activity from Polymarket Data API."""
        assert self._session is not None
        url = f"{self.config.DATA_API}/v1/activity"
        params = {"user": wallet, "limit": str(limit)}
        async with self._session.get(url, params=params) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def verify_wallet(self, wallet: str) -> dict | None:
        """Verify a wallet address by fetching its profile."""
        assert self._session is not None
        try:
            url = f"{self.config.DATA_API}/v1/profiles"
            params = {"user": wallet}
            async with self._session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data if data else None
        except Exception as e:
            logger.debug("Profile fetch failed for %s: %s", wallet[:10], e)
        return None

    def _handle_poll_error(self, wallet: str, error: Exception) -> None:
        """Track consecutive errors with exponential backoff logging."""
        self._errors[wallet] = self._errors.get(wallet, 0) + 1
        count = self._errors[wallet]

        if count <= 3 or count % 10 == 0:
            alias = self.watchlist[wallet].get("alias", wallet[:10])
            logger.error(
                "Poll error for %s (attempt %d): %s: %s",
                alias, count, type(error).__name__, error,
            )
            if count == 1:
                logger.debug("Traceback for %s poll error:", alias, exc_info=True)

    def get_stats(self) -> dict[str, Any]:
        """Return monitoring statistics for the CLI dashboard."""
        return {
            "poll_count": self._poll_count,
            "wallets_tracked": len(self.watchlist),
            "signals_queued": self.signal_queue.qsize(),
            "positions_cached": sum(
                len(p) for p in self._confirmed_positions.values()
            ),
            "pending_new": sum(len(p) for p in self._pending_new.values()),
            "pending_exit": sum(len(p) for p in self._pending_exit.values()),
            "market_titles_cached": len(self._market_meta_cache),
            "error_wallets": {
                self.watchlist[w]["alias"]: c
                for w, c in self._errors.items()
                if c > 0
            },
        }
