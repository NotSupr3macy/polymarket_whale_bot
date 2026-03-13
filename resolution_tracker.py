"""
Resolution tracker — checks market outcomes and classifies failures.

Upgrade 3: Polls the CLOB API every 5 minutes for resolved markets.
  When a market resolves, records outcome, payout, and resolution in the journal.

Upgrade 4: Failure classification on every loss.
  Categories:
    BAD_WHALE        — whale's own trade also lost (whale was wrong)
    LATE_ENTRY       — our entry was >5% worse than whale's entry
    STOPPED_EARLY    — we stopped out, but market eventually went our way
    WHALE_EXITED_RIGHT — whale exited profitably, we didn't follow fast enough
    WHALE_EXITED_WRONG — whale exited at a loss too
    EXTERNAL_SHOCK   — sudden market move (>20% in <1h)
    WRONG_DIRECTION  — market resolved opposite to our direction
    LOW_CONSENSUS    — trade was solo/low-consensus (n_whales < 2)

Runs as a background task in bot.py, checking every 5 minutes.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Optional

import aiohttp

from config import BotConfig
from trade_journal import TradeJournal

logger = logging.getLogger(__name__)

# ── Resolution check interval ─────────────────────────────────────
RESOLUTION_CHECK_INTERVAL_SECONDS = 300  # 5 minutes

# ── Failure classification categories ──────────────────────────────
FAILURE_CATEGORIES = {
    "BAD_WHALE": "Whale's own trade also lost — the whale was wrong",
    "LATE_ENTRY": "Our entry was >5% worse than whale's entry price",
    "STOPPED_EARLY": "We stopped out but market eventually went our way",
    "WHALE_EXITED_RIGHT": "Whale exited profitably, we didn't follow fast enough",
    "WHALE_EXITED_WRONG": "Whale also exited at a loss",
    "EXTERNAL_SHOCK": "Sudden market move (>20% in <1h)",
    "WRONG_DIRECTION": "Market resolved opposite to our direction",
    "LOW_CONSENSUS": "Trade was solo/low-consensus (n_whales < 2)",
}


class ResolutionTracker:
    """
    Background task that checks market resolutions and classifies failures.

    Lifecycle:
      1. Every 5 minutes, fetch unresolved closed trades from journal
      2. For each, query CLOB API for market resolution status
      3. If resolved: record outcome (WIN/LOSS/VOID) and payout
      4. If loss: classify failure reason
    """

    def __init__(self, config: BotConfig, journal: TradeJournal):
        self.config = config
        self.journal = journal
        self._session: aiohttp.ClientSession | None = None
        self._resolution_cache: dict[str, dict] = {}  # condition_id -> resolution data
        self._first_check = True  # Print one full raw response on first check
        self._stats = {
            "checks": 0,
            "resolutions_found": 0,
            "api_errors": 0,
        }

    async def start(self) -> None:
        """Create HTTP session."""
        if not self._session:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15),
            )

    async def stop(self) -> None:
        """Close HTTP session."""
        if self._session:
            await self._session.close()
            self._session = None

    async def check_resolutions(self) -> int:
        """
        Check all unresolved closed trades for market resolution.

        Returns: number of trades resolved in this check.
        """
        self._stats["checks"] += 1
        check_num = self._stats["checks"]

        unresolved = self.journal.get_unresolved_trades()
        logger.info(
            "Resolution check #%d: %d unresolved closed trades to check",
            check_num,
            len(unresolved),
        )

        if not unresolved:
            return 0

        resolved_count = 0

        for trade in unresolved:
            trade_id = trade.get("id", "?")
            condition_id = trade.get("condition_id", "")
            token_id = trade.get("token_id", "")
            market_id = trade.get("market_id", "")
            direction = trade.get("direction", "?")

            # ── Legacy position check ──
            if not condition_id:
                logger.warning(
                    "  [%s] SKIP: empty condition_id (legacy position) | "
                    "market_id=%s token_id=%s dir=%s",
                    trade_id, market_id[:20], token_id[:20] if token_id else "(empty)", direction,
                )
                continue

            if not token_id:
                logger.info(
                    "  [%s] Legacy position (empty token_id), using condition_id for lookup | "
                    "cond=%s...  dir=%s",
                    trade_id, condition_id[:20], direction,
                )

            # ── Check cache first ──
            if condition_id in self._resolution_cache:
                resolution_data = self._resolution_cache[condition_id]
                logger.debug("  [%s] Using cached resolution data for %s...", trade_id, condition_id[:16])
            else:
                resolution_data = await self._fetch_market_resolution(condition_id)
                if resolution_data:
                    self._resolution_cache[condition_id] = resolution_data

            if not resolution_data:
                logger.info(
                    "  [%s] No resolution data returned | cond=%s...  dir=%s",
                    trade_id, condition_id[:20], direction,
                )
                continue

            # ── Check if market has actually resolved ──
            tokens = resolution_data.get("tokens", [])
            has_winner = any(t.get("winner") is True for t in tokens)

            if not has_winner:
                logger.info(
                    "  [%s] Market not yet resolved (no winner) | %s | dir=%s",
                    trade_id,
                    resolution_data.get("question", condition_id[:20]),
                    direction,
                )
                continue

            # ── Market IS resolved — determine outcome ──
            resolved_count += 1
            self._stats["resolutions_found"] += 1

            entry_price = trade.get("entry_price", 0.5)
            pnl = trade.get("pnl", 0.0) or 0.0

            # Find the winning outcome name
            winning_outcome = ""
            for t in tokens:
                if t.get("winner") is True:
                    winning_outcome = t.get("outcome", "")
                    break

            # Build a resolution string from the winner
            market_resolution = winning_outcome if winning_outcome else "RESOLVED"

            # Determine our outcome
            outcome = self._determine_outcome(direction, tokens)

            # Calculate actual payout
            if outcome == "WIN":
                payout = 1.0 - entry_price  # Profit per share
            elif outcome == "LOSS":
                payout = 0.0
            else:
                payout = entry_price  # VOID: refund

            # Classify failure reason
            failure_reason = ""
            exit_reason = trade.get("exit_reason", "")
            if outcome == "LOSS":
                failure_reason = self._classify_failure(trade, resolution_data)
            elif outcome == "WIN" and pnl < 0 and exit_reason == "stop_loss":
                # Market resolved in our favor but we got stopped out early
                failure_reason = "STOPPED_EARLY"
            elif outcome == "WIN" and pnl < 0 and "whale_exit" in exit_reason:
                failure_reason = "WHALE_EXITED_RIGHT"

            # Get market title from CLOB question field
            market_title = resolution_data.get("question", "")

            if market_title:
                self.journal.update_trade(
                    trade["id"],
                    market_title=market_title,
                    market_category=resolution_data.get("category", ""),
                )

            # Record resolution
            self.journal.resolve_position(
                trade_id=trade["id"],
                resolution=market_resolution,
                payout=payout,
                outcome=outcome,
                failure_reason=failure_reason,
            )

            logger.info(
                "  RESOLVED: [%s] %s -> %s (winner=%s) | PnL=$%.2f | %s%s",
                trade_id,
                direction,
                outcome,
                winning_outcome,
                pnl,
                market_title[:40] if market_title else condition_id[:20],
                f" | Failure: {failure_reason}" if failure_reason else "",
            )

        logger.info(
            "Resolution check #%d complete: %d/%d resolved | "
            "Total resolved: %d | API errors: %d",
            check_num,
            resolved_count,
            len(unresolved),
            self._stats["resolutions_found"],
            self._stats["api_errors"],
        )
        return resolved_count

    async def _fetch_market_resolution(self, condition_id: str) -> Optional[dict]:
        """
        Fetch market resolution status from the CLOB API.

        Uses: GET {CLOB_HOST}/markets/{condition_id}
        Returns token-level winner/price data.
        """
        if not self._session:
            await self.start()

        assert self._session is not None

        url = f"{self.config.CLOB_HOST}/markets/{condition_id}"
        logger.info("    Fetching: %s", url)

        try:
            async with self._session.get(url) as resp:
                status = resp.status
                logger.info("    HTTP %d from CLOB API for %s...", status, condition_id[:20])

                if status != 200:
                    body = await resp.text()
                    logger.warning(
                        "    CLOB API error %d for %s: %s",
                        status, condition_id[:20], body[:200],
                    )
                    self._stats["api_errors"] += 1
                    return None

                data = await resp.json()

                if not data:
                    logger.warning("    Empty response from CLOB API for %s...", condition_id[:20])
                    return None

                # Log key fields
                tokens = data.get("tokens", [])
                question = data.get("question", "")
                closed = data.get("closed", False)
                active = data.get("active", None)

                token_summary = []
                for t in tokens:
                    token_summary.append(
                        f"{t.get('outcome', '?')}(price={t.get('price', '?')}, "
                        f"winner={t.get('winner', '?')})"
                    )

                logger.info(
                    "    Market: %s | closed=%s active=%s | tokens: %s",
                    question[:50] if question else "(no question)",
                    closed,
                    active,
                    ", ".join(token_summary) if token_summary else "(none)",
                )

                # Print one full raw response for debugging
                if self._first_check:
                    self._first_check = False
                    logger.info(
                        "    [DEBUG] Full raw CLOB response:\n%s",
                        json.dumps(data, indent=2, default=str)[:2000],
                    )

                return {
                    "question": question,
                    "category": "",  # CLOB API doesn't have category
                    "tokens": tokens,
                    "closed": closed,
                    "active": active,
                    "condition_id": condition_id,
                    "end_date": data.get("end_date_iso", ""),
                    "market_slug": data.get("market_slug", ""),
                }

        except Exception as e:
            self._stats["api_errors"] += 1
            logger.warning(
                "    CLOB API request failed for %s...: %s",
                condition_id[:20], e,
            )
            return None

    def _determine_outcome(self, our_direction: str, tokens: list[dict]) -> str:
        """
        Determine if our trade won or lost based on token-level resolution data.

        The CLOB API returns tokens with:
          - outcome: "Nuggets", "Spurs", "Yes", "No", "Over", "Under", etc.
          - winner: True/False
          - price: 1 or 0

        Our direction field contains the outcome name (e.g., "NUGGETS", "YES", "OVER").
        Match case-insensitively.
        """
        if not tokens:
            logger.warning("No tokens in resolution data — treating as VOID")
            return "VOID"

        our_dir_lower = our_direction.strip().lower()

        # Find the token matching our direction
        for token in tokens:
            token_outcome = token.get("outcome", "").strip().lower()
            if token_outcome == our_dir_lower:
                if token.get("winner") is True:
                    return "WIN"
                else:
                    return "LOSS"

        # If no exact match, log it and try a fuzzy match
        logger.warning(
            "Direction '%s' didn't match any token outcomes: %s",
            our_direction,
            [t.get("outcome", "") for t in tokens],
        )

        # Fuzzy: check if our direction is contained in or contains an outcome
        for token in tokens:
            token_outcome = token.get("outcome", "").strip().lower()
            if our_dir_lower in token_outcome or token_outcome in our_dir_lower:
                logger.info(
                    "Fuzzy matched '%s' -> '%s' (winner=%s)",
                    our_direction, token.get("outcome"), token.get("winner"),
                )
                if token.get("winner") is True:
                    return "WIN"
                else:
                    return "LOSS"

        logger.warning("Could not match direction '%s' to any outcome — VOID", our_direction)
        return "VOID"

    # ══════════════════════════════════════════════════════════════
    #  UPGRADE 4: FAILURE CLASSIFICATION
    # ══════════════════════════════════════════════════════════════

    def _classify_failure(self, trade: dict, resolution_data: dict) -> str:
        """
        Classify why a losing trade lost. Checks conditions in priority order.

        Priority:
          1. LOW_CONSENSUS — trade was solo/low consensus (root cause likely)
          2. LATE_ENTRY — we entered too late (>5% worse)
          3. STOPPED_EARLY — stopped out but market eventually went right
          4. WRONG_DIRECTION — market resolved against us (clean loss)
          5. BAD_WHALE — default when whale was just wrong
        """
        exit_reason = trade.get("exit_reason", "")
        entry_price = trade.get("entry_price", 0.5)
        exit_price = trade.get("exit_price", 0.0) or 0.0
        n_whales = trade.get("n_whales", 0) or 0
        consensus_level = trade.get("consensus_level", "")

        # 1. LOW_CONSENSUS — solo trades or low-consensus signals
        if n_whales < 2 or consensus_level in ("TIER1_SOLO", "TIER2_SOLO", "TIER3_SOLO"):
            return "LOW_CONSENSUS"

        # 2. LATE_ENTRY — our entry was significantly worse
        direction = trade.get("direction", "YES")
        if direction.upper() in ("YES",) and entry_price > 0.70:
            return "LATE_ENTRY"
        elif direction.upper() in ("NO",) and entry_price < 0.30:
            return "LATE_ENTRY"

        # 3. STOPPED_EARLY — we hit stop-loss but market eventually resolved our way
        if exit_reason == "stop_loss":
            tokens = resolution_data.get("tokens", [])
            outcome = self._determine_outcome(direction, tokens)
            if outcome == "WIN":
                return "STOPPED_EARLY"

        # 4. WHALE_EXITED — check if exit was a whale follow
        if "whale_exit" in exit_reason:
            if exit_price > entry_price:
                return "WHALE_EXITED_RIGHT"
            else:
                return "WHALE_EXITED_WRONG"

        # 5. WRONG_DIRECTION — market resolved against us
        return "WRONG_DIRECTION"

    def get_stats(self) -> dict:
        return dict(self._stats)
