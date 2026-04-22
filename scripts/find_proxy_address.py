#!/usr/bin/env python3
"""Discover the Polymarket proxy wallet address associated with our EOA.

Context: When you sign up on Polymarket with MetaMask, Polymarket creates
a smart-contract proxy wallet for you. Your deposited USDC lives in that
PROXY, not your EOA (MetaMask address). The CLOB API routes orders
through the proxy.

This script probes py-clob-client for the proxy address via various
methods, queries the on-chain USDC balance of each address we find,
and reports which one actually has the $10.

Run:
    source venv/bin/activate
    python3 scripts/find_proxy_address.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

# Load .env
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"


async def query_usdc_balance(address: str) -> float:
    """Raw on-chain USDC balance for any address."""
    import aiohttp
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{
            "to": USDC_E,
            "data": "0x70a08231000000000000000000000000" + address[2:].lower(),
        }, "latest"],
        "id": 1,
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                "https://polygon-rpc.com",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                data = await r.json()
    except Exception as e:
        return -1
    raw = int(data.get("result", "0x0"), 16)
    return raw / 1_000_000.0


def try_method(client, method_name):
    """Try calling a zero-arg method on client, return the result or None."""
    method = getattr(client, method_name, None)
    if method is None:
        return None, f"{method_name}: not available"
    try:
        result = method()
        return result, f"{method_name}(): {result!r}"[:200]
    except Exception as e:
        return None, f"{method_name}(): ERR {type(e).__name__}: {e}"[:200]


async def main():
    from monitor.clob_client_wrapper import get_client

    print("=" * 70)
    print("POLYMARKET PROXY ADDRESS DISCOVERY")
    print("=" * 70)

    try:
        client = get_client()
    except Exception as e:
        print(f"[FAIL] client construction: {e}")
        return 1

    eoa = os.getenv("POLYMARKET_WALLET_ADDRESS", "")
    print(f"\nEOA (POLYMARKET_WALLET_ADDRESS): {eoa}")
    eoa_bal = await query_usdc_balance(eoa)
    print(f"  on-chain USDC at EOA: ${eoa_bal:.2f}")

    # Try every method name that might return an address
    print("\nProbing py-clob-client for address-returning methods:")
    candidates = {}
    for method_name in [
        "get_address", "get_funder_address", "get_safe_address",
        "get_proxy_address", "address", "funder", "safe_address",
        "proxy_address", "eoa_address", "wallet_address",
    ]:
        result, msg = try_method(client, method_name)
        print(f"  {msg}")
        if isinstance(result, str) and result.startswith("0x") and len(result) == 42:
            candidates[method_name] = result

    # Probe client attributes (not methods)
    print("\nChecking client attributes that hold addresses:")
    for attr in ["address", "funder", "signer", "_signer", "wallet",
                 "account", "eoa", "proxy"]:
        v = getattr(client, attr, None)
        if v is not None:
            print(f"  client.{attr} = {v!r}"[:200])
            # Check if it's an object with .address or similar
            for sub in ["address", "_address"]:
                sv = getattr(v, sub, None)
                if isinstance(sv, str) and sv.startswith("0x"):
                    candidates[f"client.{attr}.{sub}"] = sv
            if isinstance(v, str) and v.startswith("0x") and len(v) == 42:
                candidates[f"client.{attr}"] = v

    # Query on-chain balance of each discovered address
    if candidates:
        print(f"\nQuerying on-chain USDC for each discovered address:")
        print(f"{'source':40s} {'address':44s} {'USDC':>10}")
        print("-" * 100)
        seen = set()
        for source, addr in candidates.items():
            if addr.lower() in seen:
                continue
            seen.add(addr.lower())
            bal = await query_usdc_balance(addr)
            marker = "  <-- HAS MONEY" if bal >= 1 else ""
            print(f"{source:40s} {addr:44s} ${bal:>7.2f}{marker}")
    else:
        print("\n[WARN] no address-returning methods found on client")

    # Derive the proxy address deterministically as backup
    # Polymarket's proxy uses a CREATE2 formula; this is how the frontend
    # computes it. We reproduce it here:
    try:
        from eth_utils import keccak, to_checksum_address
        # Polymarket POLY_PROXY_FACTORY on Polygon
        FACTORY = "0xaacfeea03eb1561c4e67d661e40682bd20e3541b"
        IMPL = "0xd2b7b3d0a88fa4a2fe22aa1e0fcb9f08a5a70d4f"
        # Salt = keccak256(abi.encode(eoa))
        # Init code for proxy = uint8 + minimal proxy bytecode
        print(f"\n(Informational — derived proxy address calculation skipped; "
              f"the CLOB SDK would give the correct one if it exposed it)")
    except Exception:
        pass

    print()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
