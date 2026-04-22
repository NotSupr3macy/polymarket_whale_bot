"""Thin wrapper around py-clob-client for Polymarket order placement.

Isolates:
  - Credential loading from env
  - Client singleton construction
  - Order signing + posting
  - Market metadata fetches (token_ids, order book)
  - USDC / collateral balance checks

All other live_trader modules import from here instead of touching
py_clob_client directly, so the SDK is swappable at one file.

Environment variables required:
    POLYMARKET_PRIVATE_KEY     0x... (64 hex chars)
    POLYMARKET_API_KEY         uuid-ish
    POLYMARKET_API_SECRET      base64-ish
    POLYMARKET_API_PASSPHRASE  passphrase
    POLYMARKET_WALLET_ADDRESS  0x... (public address derived from priv key)

This module does NOT read .env — loading is handled by live_trader.py
at daemon startup.
"""
from __future__ import annotations

import json
import logging
import os
from decimal import Decimal
from typing import Optional

import aiohttp

logger = logging.getLogger("clob_wrapper")

# ── Polygon chain constants ────────────────────────────────────────────
POLYGON_CHAIN_ID = 137
CLOB_HOST = "https://clob.polymarket.com"
GAMMA_HOST = "https://gamma-api.polymarket.com"
DATA_HOST = "https://data-api.polymarket.com"
POLYGON_RPC = "https://polygon-rpc.com"  # public RPC; fine for balance reads

# USDC.e on Polygon (Polymarket's collateral token)
USDC_E_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

# ── Lazy client singleton ──────────────────────────────────────────────
_client = None


def _import_clob():
    """Import py_clob_client lazily so tests can run without it installed."""
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import (
            ApiCreds, OrderArgs, OrderType,
        )
        from py_clob_client.constants import POLYGON
        return {
            "ClobClient": ClobClient,
            "ApiCreds": ApiCreds,
            "OrderArgs": OrderArgs,
            "OrderType": OrderType,
            "POLYGON": POLYGON,
        }
    except ImportError as e:
        raise RuntimeError(
            "py-clob-client not installed. Run:\n"
            "    source venv/bin/activate\n"
            "    pip install py-clob-client"
        ) from e


def _require_env(key: str) -> str:
    v = os.getenv(key, "").strip()
    if not v:
        raise RuntimeError(f"Missing required env var: {key}")
    return v


def get_client():
    """Return a singleton ClobClient. Lazy-constructed on first call."""
    global _client
    if _client is not None:
        return _client

    pkg = _import_clob()
    pkey = _require_env("POLYMARKET_PRIVATE_KEY")
    if not pkey.startswith("0x") or len(pkey) != 66:
        raise RuntimeError(
            "POLYMARKET_PRIVATE_KEY malformed — must be 0x-prefixed 64 hex"
        )

    creds = pkg["ApiCreds"](
        api_key=_require_env("POLYMARKET_API_KEY"),
        api_secret=_require_env("POLYMARKET_API_SECRET"),
        api_passphrase=_require_env("POLYMARKET_API_PASSPHRASE"),
    )

    # signature_type controls how orders and balance queries are signed.
    # 0 = EOA direct (no proxy) — newest Polymarket signup flow (relay-based)
    # 1 = POLY_PROXY — older proxy-wallet flow
    # 2 = POLY_GNOSIS_SAFE — for Gnosis Safe multisig setups
    # Controlled by env so we can switch without code change.
    sig_type = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "0"))

    # For sig_type 1 and 2, the FUNDER is a separate address from the EOA
    # (signer). The Safe/Proxy wallet holds the funds and is the order's
    # maker. Without explicit funder, py-clob-client uses the EOA as
    # funder which gives "invalid signature" rejections on the CLOB.
    funder = os.getenv("POLYMARKET_FUNDER_ADDRESS", "").strip() or None

    client_kwargs = {
        "host": CLOB_HOST,
        "key": pkey,
        "chain_id": POLYGON_CHAIN_ID,
        "creds": creds,
        "signature_type": sig_type,
    }
    if funder and sig_type in (1, 2):
        client_kwargs["funder"] = funder

    _client = pkg["ClobClient"](**client_kwargs)
    logger.info(
        "CLOB client constructed chain=%d signature_type=%d funder=%s",
        POLYGON_CHAIN_ID, sig_type, funder or "(none, using EOA)",
    )
    return _client


def derive_wallet_address() -> str:
    """Derive the public wallet address from POLYMARKET_PRIVATE_KEY.
    Used by precheck to verify env consistency."""
    from eth_account import Account  # pulled in by py-clob-client
    pkey = _require_env("POLYMARKET_PRIVATE_KEY")
    acct = Account.from_key(pkey)
    return acct.address


# ── Order placement ────────────────────────────────────────────────────
async def place_limit_order(
    token_id: str,
    price: float,
    size_shares: float,
    side: str = "BUY",
    order_type: str = "GTC",
) -> dict:
    """Sign and submit a limit order to the CLOB.

    Args:
        token_id: The ERC1155 token id for YES/NO outcome in the market.
        price: Limit price in [0.01, 0.99]. Quantized to 0.001 granularity.
        size_shares: Number of shares (not USD). USD = price × shares.
        side: "BUY" or "SELL". "BUY" for opens, "SELL" for exits.
        order_type: "GTC" (good-till-cancelled), "FOK" (fill-or-kill),
                    "GTD" (good-till-date), or "FAK" (fill-and-kill).

    Returns:
        Dict with keys: success, errorMsg, orderID, transactionsHashes,
                        status, takingAmount, makingAmount.
        On rejection: success=False, errorMsg explains why.
        On acceptance: success=True, orderID is the CLOB's reference.
    """
    pkg = _import_clob()
    client = get_client()

    args = pkg["OrderArgs"](
        token_id=token_id,
        price=round(price, 3),
        size=round(size_shares, 2),
        side=side.upper(),
    )
    signed = client.create_order(args)

    # Map string to enum
    ot = {
        "GTC": pkg["OrderType"].GTC,
        "FOK": pkg["OrderType"].FOK,
        "GTD": pkg["OrderType"].GTD,
        "FAK": pkg["OrderType"].FAK,
    }[order_type.upper()]

    resp = client.post_order(signed, ot)
    logger.info(
        "order posted: token=%s price=$%.3f size=%.2f side=%s type=%s "
        "resp=%s",
        token_id[:16], price, size_shares, side, order_type, resp,
    )
    return resp


async def cancel_order(order_id: str) -> dict:
    """Cancel an open order by id."""
    client = get_client()
    resp = client.cancel(order_id)
    logger.info("order cancelled: id=%s resp=%s", order_id[:16], resp)
    return resp


async def get_order_status(order_id: str) -> dict:
    """Fetch current state of an order (PLACED, PARTIAL, FILLED, CANCELLED)."""
    client = get_client()
    return client.get_order(order_id)


# ── Market data ────────────────────────────────────────────────────────
async def get_market_tokens(condition_id: str) -> dict[str, str]:
    """Map outcome name (e.g. 'Over', 'Lakers') to its ERC1155 token_id.

    Returns dict like {"Over": "12345...", "Under": "98765..."}.
    Empty dict on failure.
    """
    params = {"condition_ids": condition_id}
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f"{GAMMA_HOST}/markets",
            params=params,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status != 200:
                return {}
            data = await r.json()
    if not data:
        return {}
    m = data[0]
    try:
        outcomes = m.get("outcomes", "[]")
        outcomes = json.loads(outcomes) if isinstance(outcomes, str) else outcomes
        token_ids = m.get("clobTokenIds", "[]")
        token_ids = json.loads(token_ids) if isinstance(token_ids, str) else token_ids
    except Exception as e:
        logger.warning("parse market failed for %s: %s", condition_id[:16], e)
        return {}
    if len(outcomes) != len(token_ids):
        logger.warning("outcomes/tokens length mismatch for %s", condition_id[:16])
        return {}
    return dict(zip(outcomes, token_ids))


async def get_order_book(token_id: str) -> dict:
    """Fetch the current order book for a token. Returns:
        {"asks": [{"price": 0.52, "size": 1234}, ...],
         "bids": [{"price": 0.51, "size": 5678}, ...]}
    Asks sorted ascending by price, bids sorted descending.
    """
    url = f"{CLOB_HOST}/book"
    params = {"token_id": token_id}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                url, params=params,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status != 200:
                    logger.debug("book fetch HTTP %d for %s",
                                 r.status, token_id[:16])
                    return {"asks": [], "bids": []}
                data = await r.json()
    except Exception as e:
        logger.debug("book fetch err for %s: %s", token_id[:16], e)
        return {"asks": [], "bids": []}

    asks = [{"price": float(lvl["price"]), "size": float(lvl["size"])}
            for lvl in data.get("asks", [])]
    bids = [{"price": float(lvl["price"]), "size": float(lvl["size"])}
            for lvl in data.get("bids", [])]
    asks.sort(key=lambda x: x["price"])
    bids.sort(key=lambda x: x["price"], reverse=True)
    return {"asks": asks, "bids": bids}


# ── Balance + positions ────────────────────────────────────────────────
async def get_usdc_balance() -> float:
    """Query on-chain USDC.e balance of our wallet on Polygon.
    Returns balance in USD (human units). Uses public RPC.
    """
    address = _require_env("POLYMARKET_WALLET_ADDRESS")
    # balanceOf(address) selector = 0x70a08231, padded arg is the address
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{
            "to": USDC_E_ADDRESS,
            "data": "0x70a08231000000000000000000000000" + address[2:].lower(),
        }, "latest"],
        "id": 1,
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                POLYGON_RPC, json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                data = await r.json()
    except Exception as e:
        logger.warning("USDC balance fetch failed: %s", e)
        return -1.0
    raw = int(data.get("result", "0x0"), 16)
    # USDC has 6 decimals
    return raw / 1_000_000.0


async def get_polymarket_balance() -> float:
    """Query Polymarket's collateral balance for our account.

    This is what's actually usable for trading (vs USDC sitting in the
    wallet but not yet deposited). Polymarket uses a proxy wallet
    system; deposits show up via the CLOB /balance-allowance endpoint.
    """
    client = get_client()
    try:
        # Use the typed params object — older SDKs crash on raw dicts
        # because they try to read .signature_type attr.
        #
        # CRITICAL: signature_type must match the wallet type used at
        # signup. MetaMask users on Polymarket get a POLY_PROXY (type 1).
        # If we leave this at the default (0 = EOA), the CLOB returns
        # the balance of the EOA (which is $0 after deposit) instead
        # of the proxy (where the $10 actually lives).
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        sig_type = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "0"))
        params = BalanceAllowanceParams(
            asset_type=AssetType.COLLATERAL,
            signature_type=sig_type,
        )
        bal = client.get_balance_allowance(params)
        if isinstance(bal, dict):
            # Response has 'balance' and 'allowance' keys. USDC = 6 decimals.
            raw = int(bal.get("balance", 0))
            return raw / 1_000_000.0
        raw = int(getattr(bal, "balance", 0))
        return raw / 1_000_000.0
    except Exception as e:
        logger.warning(
            "Polymarket balance fetch failed (%s), "
            "falling back to USDC on-chain", e,
        )
        return await get_usdc_balance()


async def get_open_orders() -> list[dict]:
    """All currently-open orders for our account."""
    client = get_client()
    try:
        return client.get_orders() or []
    except Exception as e:
        logger.warning("open orders fetch failed: %s", e)
        return []


async def get_open_orders_value() -> float:
    """Sum of USDC locked in currently open orders.
    Useful for reconciliation: bankroll = wallet USDC + open-order value."""
    orders = await get_open_orders()
    total = 0.0
    for o in orders:
        try:
            price = float(o.get("price", 0))
            size = float(o.get("original_size", o.get("size", 0)))
            filled = float(o.get("size_matched", 0))
            remaining = max(0, size - filled)
            total += price * remaining
        except Exception:
            continue
    return total


# ── Redemption ─────────────────────────────────────────────────────────
async def redeem_winning_position(condition_id: str) -> dict:
    """Call redeemPositions on the ConditionalTokens contract to convert
    winning outcome tokens into USDC.

    Must be called after the market resolves and prices go to rails.
    Safe to call for losing positions too (returns 0 USDC, costs a
    small amount of gas).

    Returns dict with tx_hash, USDC received, status.
    """
    # Actual implementation involves constructing a raw transaction to the
    # ConditionalTokens contract. py-clob-client may have a helper; if not,
    # we'd use web3.py directly.
    #
    # For trial v1: rely on Polymarket's UI-side auto-redeem if possible,
    # OR implement minimal web3.py call here.
    #
    # Deferred implementation note: the first version will likely use
    # py-clob-client's redemption helper if available, else a direct
    # contract call via web3.py.
    raise NotImplementedError(
        "redeem_winning_position: implement after smoke test confirms order "
        "flow works. Polymarket auto-redeems on many markets via resolver; "
        "manual call is only needed for edge cases."
    )
