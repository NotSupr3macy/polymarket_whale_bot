"""
Polymarket client for the EV engine.

Responsibilities:
  - Resolve a condition_id to its CLOB token_ids + outcome labels via the
    Polymarket Gamma API (with in-process caching).
  - Fetch current CLOB midpoint / book for a token_id (read-only, no auth).
  - Given a texaskid position (condition_id + direction string), return the
    token_id the user holds and the current cashout (mid) price.

This is a thin, async, read-only client shared across the EV engine.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import aiohttp


logger = logging.getLogger(__name__)

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_HOST = "https://clob.polymarket.com"


@dataclass
class MarketInfo:
    """Resolved market metadata from the Gamma API."""
    condition_id: str
    question: str
    slug: str
    outcomes: list[str]           # e.g. ["Yes", "No"] or ["Phillies", "Diamondbacks"]
    token_ids: list[str]          # aligned with outcomes
    end_date: Optional[str]
    game_start_time: Optional[str]
    closed: bool


@dataclass
class CashoutQuote:
    """A current sell quote for a position."""
    token_id: str
    outcome: str
    mid_price: float              # current midpoint (best proxy for cashout)
    best_bid: Optional[float]     # what you'd actually get selling into the book
    best_ask: Optional[float]
    spread: Optional[float]


class PolymarketClient:
    """Async Polymarket read-only client (Gamma + CLOB)."""

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self._session = session
        self._market_cache: dict[str, MarketInfo] = {}

    # ─────────────────────────────────────────────────────────
    #  Gamma — market metadata
    # ─────────────────────────────────────────────────────────

    async def get_market_info(self, condition_id: str) -> Optional[MarketInfo]:
        """Resolve condition_id -> outcomes/token_ids via Gamma API (cached)."""
        if condition_id in self._market_cache:
            return self._market_cache[condition_id]

        url = f"{GAMMA_API}/markets"
        params = {"condition_ids": condition_id}
        try:
            async with self._session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    logger.warning("Gamma HTTP %d for %s", resp.status, condition_id)
                    return None
                data = await resp.json()
        except Exception as e:
            logger.warning("Gamma fetch error for %s: %s", condition_id, e)
            return None

        # Gamma returns a list
        items = data if isinstance(data, list) else data.get("data", [])
        if not items:
            return None
        raw = items[0]

        # Outcomes and token IDs come back as JSON-string lists sometimes
        outcomes = _parse_list_field(raw.get("outcomes"))
        token_ids = _parse_list_field(raw.get("clobTokenIds"))

        if not outcomes or not token_ids or len(outcomes) != len(token_ids):
            logger.warning(
                "Gamma missing outcomes/tokens for %s (outcomes=%s, tokens=%s)",
                condition_id, outcomes, token_ids,
            )
            return None

        info = MarketInfo(
            condition_id=condition_id,
            question=raw.get("question", ""),
            slug=raw.get("slug", ""),
            outcomes=outcomes,
            token_ids=token_ids,
            end_date=raw.get("endDate"),
            game_start_time=raw.get("gameStartTime"),
            closed=bool(raw.get("closed", False)),
        )
        self._market_cache[condition_id] = info
        return info

    # ─────────────────────────────────────────────────────────
    #  CLOB — midpoint / book
    # ─────────────────────────────────────────────────────────

    async def get_midpoint(self, token_id: str) -> Optional[float]:
        """Fetch current midpoint for a token (read-only, no auth)."""
        url = f"{CLOB_HOST}/midpoint"
        params = {"token_id": token_id}
        try:
            async with self._session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                mid = data.get("mid")
                return float(mid) if mid is not None else None
        except Exception as e:
            logger.debug("CLOB midpoint fetch failed for %s: %s", token_id[:12], e)
            return None

    async def get_book(self, token_id: str) -> Optional[dict]:
        """Fetch full orderbook for a token."""
        url = f"{CLOB_HOST}/book"
        params = {"token_id": token_id}
        try:
            async with self._session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    return None
                return await resp.json()
        except Exception as e:
            logger.debug("CLOB book fetch failed for %s: %s", token_id[:12], e)
            return None

    async def get_best_prices(self, token_id: str) -> tuple[Optional[float], Optional[float]]:
        """Return (best_bid, best_ask) from the orderbook."""
        book = await self.get_book(token_id)
        if not book:
            return None, None
        bids = book.get("bids") or []
        asks = book.get("asks") or []
        best_bid = float(bids[-1]["price"]) if bids else None
        best_ask = float(asks[-1]["price"]) if asks else None
        # Polymarket sometimes returns them ascending, sometimes descending — take extremes
        if bids:
            best_bid = max(float(b["price"]) for b in bids)
        if asks:
            best_ask = min(float(a["price"]) for a in asks)
        return best_bid, best_ask

    # ─────────────────────────────────────────────────────────
    #  Position-level helpers
    # ─────────────────────────────────────────────────────────

    def resolve_outcome_token(
        self, info: MarketInfo, direction: str,
    ) -> Optional[tuple[str, str]]:
        """
        Match a position's `direction` string to one of the market outcomes,
        returning (outcome_label, token_id).

        `direction` comes from texaskid_positions and might be:
          - "Yes" / "No"
          - "Over" / "Under"
          - "Arizona Diamondbacks"
          - "Heat"
        """
        if not info.outcomes:
            return None
        d = (direction or "").strip().lower()
        if not d:
            return None

        # 1) Exact case-insensitive match
        for outcome, token in zip(info.outcomes, info.token_ids):
            if outcome.strip().lower() == d:
                return outcome, token

        # 2) Substring match either direction (e.g. "Heat" vs "Miami Heat")
        for outcome, token in zip(info.outcomes, info.token_ids):
            o = outcome.strip().lower()
            if d in o or o in d:
                return outcome, token

        # 3) Over/Under fallback: outcomes may be ["Yes","No"] with Over=Yes
        if d in ("over", "yes") and "yes" in [o.lower() for o in info.outcomes]:
            for outcome, token in zip(info.outcomes, info.token_ids):
                if outcome.lower() == "yes":
                    return outcome, token
        if d in ("under", "no") and "no" in [o.lower() for o in info.outcomes]:
            for outcome, token in zip(info.outcomes, info.token_ids):
                if outcome.lower() == "no":
                    return outcome, token

        return None

    async def get_cashout_quote(
        self, condition_id: str, direction: str,
    ) -> Optional[CashoutQuote]:
        """End-to-end: condition_id + direction -> current CashoutQuote."""
        info = await self.get_market_info(condition_id)
        if info is None or info.closed:
            return None

        resolved = self.resolve_outcome_token(info, direction)
        if not resolved:
            logger.warning(
                "Could not match direction %r to outcomes %s (cond=%s)",
                direction, info.outcomes, condition_id[:12],
            )
            return None
        outcome, token_id = resolved

        mid = await self.get_midpoint(token_id)
        if mid is None:
            return None

        bid, ask = await self.get_best_prices(token_id)
        spread = (ask - bid) if (bid is not None and ask is not None) else None

        return CashoutQuote(
            token_id=token_id,
            outcome=outcome,
            mid_price=mid,
            best_bid=bid,
            best_ask=ask,
            spread=spread,
        )


# ─────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────

def _parse_list_field(value) -> list[str]:
    """Gamma sometimes returns list fields as JSON-encoded strings."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str):
        import json
        try:
            parsed = json.loads(value)
            return [str(v) for v in parsed] if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []
