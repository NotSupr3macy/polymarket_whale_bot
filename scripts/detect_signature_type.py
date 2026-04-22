#!/usr/bin/env python3
"""Probe all 3 Polymarket signature types to see which one matches
this account. Derives api_creds for each, then tests balance_allowance.
The signature_type that returns a non-zero balance is the correct one.

Run:
    source venv/bin/activate
    python3 scripts/detect_signature_type.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())


def main():
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import (
            BalanceAllowanceParams, AssetType,
        )
    except ImportError:
        print("[FAIL] py-clob-client not installed")
        return 1

    pkey = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
    if not pkey.startswith("0x") or len(pkey) != 66:
        print("[FAIL] POLYMARKET_PRIVATE_KEY malformed")
        return 1

    HOST = "https://clob.polymarket.com"
    POLYGON = 137

    print("=" * 70)
    print("SIGNATURE TYPE DETECTOR")
    print("=" * 70)
    print()
    print("Testing each signature_type by:")
    print("  1. Constructing a client with that type")
    print("  2. Deriving api_creds at that type")
    print("  3. Querying get_balance_allowance")
    print("  The type that returns non-zero balance is YOUR account type.")
    print()

    results = []
    for sig_type in [0, 1, 2]:
        type_name = {0: "EOA", 1: "POLY_PROXY", 2: "POLY_GNOSIS_SAFE"}[sig_type]
        print(f"\n--- signature_type={sig_type} ({type_name}) ---")
        try:
            # Construct minimal client (no creds yet) to derive creds
            probe_client = ClobClient(
                host=HOST, key=pkey, chain_id=POLYGON,
                signature_type=sig_type,
            )
            creds = probe_client.create_or_derive_api_creds()
            api_key = getattr(creds, "api_key", None)
            api_secret = getattr(creds, "api_secret", None)
            api_passphrase = getattr(creds, "api_passphrase", None)
            print(f"  derived api_key: {api_key[:16] if api_key else None}...")

            # Rebuild with creds
            from py_clob_client.clob_types import ApiCreds
            full_client = ClobClient(
                host=HOST, key=pkey, chain_id=POLYGON,
                creds=ApiCreds(
                    api_key=api_key,
                    api_secret=api_secret,
                    api_passphrase=api_passphrase,
                ),
                signature_type=sig_type,
            )
            params = BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL,
                signature_type=sig_type,
            )
            bal = full_client.get_balance_allowance(params)
            if isinstance(bal, dict):
                balance = int(bal.get("balance", 0)) / 1_000_000
            else:
                balance = int(getattr(bal, "balance", 0)) / 1_000_000
            marker = "  <-- YOUR ACCOUNT TYPE" if balance >= 1 else ""
            print(f"  balance: ${balance:.2f}{marker}")
            results.append((sig_type, type_name, balance,
                            api_key, api_secret, api_passphrase))
        except Exception as e:
            print(f"  ERR: {type(e).__name__}: {str(e)[:150]}")
            results.append((sig_type, type_name, 0, None, None, None))

    print()
    print("=" * 70)
    winner = [r for r in results if r[2] >= 1]
    if winner:
        sig_type, name, bal, key, secret, passphrase = winner[0]
        print(f"DETECTED: signature_type={sig_type} ({name}) — balance ${bal:.2f}")
        print()
        print("Update .env with these lines (replace existing POLYMARKET_API_* if different):")
        print()
        print(f"POLYMARKET_SIGNATURE_TYPE={sig_type}")
        print(f"POLYMARKET_API_KEY={key}")
        print(f"POLYMARKET_API_SECRET={secret}")
        print(f"POLYMARKET_API_PASSPHRASE={passphrase}")
    else:
        print("NO SIGNATURE TYPE SHOWED A NON-ZERO BALANCE")
        print("  Possible reasons:")
        print("  - $10 deposit hasn't confirmed yet (wait 5 min)")
        print("  - Deposit went to a different wallet than this private key")
        print("  - Polymarket account was created with a different method")
        print()
        print("Check Polymarket UI balance (polymarket.com, VPN on) — does it")
        print("still show $10? If yes, we need to investigate the deposit path.")
    print("=" * 70)
    return 0 if winner else 1


if __name__ == "__main__":
    sys.exit(main())
