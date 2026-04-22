#!/usr/bin/env python3
"""For POLY_GNOSIS_SAFE (signature_type=2) accounts, find the Safe
address that corresponds to our EOA.

The Safe address is what holds funds and is the order's "funder".
Without it, place_order signatures fail with "invalid signature".

Probes multiple sources:
  1. Polymarket data API endpoints that may expose the Safe
  2. CREATE2 derivation from EOA via Polymarket's Safe factory
  3. On-chain log scan for SafeProxyCreation events

Run:
    source venv/bin/activate
    python3 scripts/find_safe_address.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import aiohttp

_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())


async def probe_polymarket_apis(eoa: str):
    """Try endpoints that might return the Safe address for an EOA."""
    print("\n## 1. Probing Polymarket data API for account info")

    endpoints = [
        f"https://data-api.polymarket.com/positions?user={eoa}&limit=1",
        f"https://data-api.polymarket.com/value?user={eoa}",
        f"https://data-api.polymarket.com/profile?user={eoa}",
        f"https://data-api.polymarket.com/user/{eoa}",
        f"https://gamma-api.polymarket.com/profile?address={eoa}",
        f"https://clob.polymarket.com/account",  # auth required
    ]

    found_addresses = set()
    async with aiohttp.ClientSession() as s:
        for url in endpoints:
            try:
                async with s.get(url, timeout=10,
                                 headers={"User-Agent": "Mozilla/5.0"}) as r:
                    body = await r.text()
                    status = r.status
            except Exception as e:
                print(f"  {url[:60]:60s} ERR: {e}"); continue

            print(f"  HTTP {status}: {url[:50]}")
            # Scan body for any 0x... addresses we haven't seen
            import re
            addrs = set(re.findall(r"0x[a-fA-F0-9]{40}", body))
            new = addrs - found_addresses - {eoa.lower(), eoa}
            for a in new:
                if a.lower() != eoa.lower():
                    found_addresses.add(a)
                    print(f"    found address: {a}")
            if status == 200 and len(body) < 800:
                print(f"    body: {body[:500]}")

    return found_addresses


async def query_usdc_at_address(addr: str) -> float:
    payload = {
        "jsonrpc": "2.0", "method": "eth_call",
        "params": [{
            "to": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
            "data": "0x70a08231000000000000000000000000" + addr[2:].lower(),
        }, "latest"], "id": 1,
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post("https://polygon-rpc.com",
                              json=payload, timeout=10) as r:
                d = await r.json()
        raw = int(d.get("result", "0x0"), 16)
        return raw / 1_000_000
    except Exception:
        return -1


async def find_via_safe_factory_logs(eoa: str):
    """Polymarket deploys Gnosis Safe proxies via a known factory contract.
    Scan the factory's logs for SafeProxyCreation events that reference
    our EOA as an owner.
    """
    print("\n## 2. Querying Polygon for Safe Proxy creation events")

    # Polymarket's Safe Proxy Factory on Polygon
    # Known address as of 2024: 0xaacFeEa03eb1561C4e67d661e40682Bd20E3541b
    # If unknown, this section just skips.
    SAFE_FACTORY = "0xaacFeEa03eb1561C4e67d661e40682Bd20E3541b"

    # SafeProxyCreation event signature
    # event ProxyCreation(SafeProxy proxy, address singleton)
    PROXY_CREATION_TOPIC = (
        "0x4f51faf6c4561ff95f067657e43439f0f856d97c04d9ec9070a6199ad418e235"
    )

    # We need to scan recent blocks. Start from a few months ago.
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getLogs",
        "params": [{
            "fromBlock": "earliest",
            "toBlock": "latest",
            "address": SAFE_FACTORY,
            "topics": [PROXY_CREATION_TOPIC],
        }],
        "id": 1,
    }

    # This will return MANY events. We need to filter by EOA somehow.
    # Without the EOA in topics, this isn't direct — Safe creation uses
    # CREATE2 with a salt derived from owners + threshold.
    # Skip this expensive scan for now.
    print("  (skipped — would require scanning all factory events)")


async def derive_safe_via_create2(eoa: str):
    """Derive the Polymarket Safe address using CREATE2.

    For Polymarket's Safe factory:
      proxy_address = keccak256(0xff || factory || salt || keccak256(init_code))[12:]

    Where:
      salt = keccak256(abi.encodePacked(eoa, 0))  # nonce 0
      init_code = SafeProxy bytecode + abi.encode(safe_singleton)

    This requires knowing the exact factory + singleton + bytecode. Only
    accurate if Polymarket uses the standard Gnosis Safe Proxy factory.
    """
    print("\n## 3. CREATE2 derivation (deterministic Safe address)")
    print("  (requires exact factory bytecode — Polymarket-specific, "
          "not implemented in this probe)")


async def check_clob_signer_internals():
    """Inspect what the CLOB SDK is using internally as funder/maker."""
    print("\n## 4. py-clob-client internal state")
    try:
        from monitor.clob_client_wrapper import get_client
        client = get_client()
    except Exception as e:
        print(f"  ERR: {e}"); return None

    # Dump everything that looks like an address
    import re
    for attr in dir(client):
        if attr.startswith("_"): continue
        try:
            val = getattr(client, attr)
        except Exception:
            continue
        if isinstance(val, str) and val.startswith("0x") and len(val) == 42:
            print(f"  client.{attr} = {val}")
        elif callable(val):
            try:
                result = val()
                if isinstance(result, str) and result.startswith("0x") and len(result) == 42:
                    print(f"  client.{attr}() = {result}")
            except Exception:
                pass

    # Specifically check for funder
    funder = getattr(client, "funder_address", None) or \
             getattr(client, "funder", None) or \
             os.getenv("POLYMARKET_FUNDER_ADDRESS")
    print(f"  current funder: {funder!r}")
    return funder


async def main():
    eoa = os.getenv("POLYMARKET_WALLET_ADDRESS")
    if not eoa:
        print("[FAIL] POLYMARKET_WALLET_ADDRESS not set")
        return 1

    print("=" * 70)
    print(f"FINDING SAFE ADDRESS FOR EOA: {eoa}")
    print("=" * 70)

    found = await probe_polymarket_apis(eoa)
    await find_via_safe_factory_logs(eoa)
    await derive_safe_via_create2(eoa)
    funder = await check_clob_signer_internals()

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    if found:
        print(f"\nAddresses surfaced in API responses (other than EOA):")
        for addr in found:
            bal = await query_usdc_at_address(addr)
            marker = "  <-- HAS USDC ($10?)" if bal >= 5 else ""
            print(f"  {addr}  USDC=${bal:.2f}{marker}")
    else:
        print("\nNo Safe address found via probing.")

    print(f"""
Manual fallback to find your Safe address:

  1. Open polymarket.com (VPN to Vienna), log in
  2. Click your avatar (top-right) → Profile or Wallet
  3. Look for a "Funder" or "Address" or "Safe" — that's the address
     that holds your $10
  4. Add it to .env:

     POLYMARKET_FUNDER_ADDRESS=0x...

  5. Re-run scripts/diagnose_block.py
""")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
