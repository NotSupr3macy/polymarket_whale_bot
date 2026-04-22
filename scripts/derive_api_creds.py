#!/usr/bin/env python3
"""Derive Polymarket CLOB API credentials (key, secret, passphrase) from
your EOA private key. Polymarket's newer UI only shows the api_key
identifier — the secret and passphrase are computed deterministically
from an EIP-712 signature over a specific message using your private key.

This script:
  1. Loads POLYMARKET_PRIVATE_KEY from .env
  2. Constructs a minimal ClobClient (just private key, no creds yet)
  3. Calls create_or_derive_api_creds() which signs the proper message
     and queries Polymarket to derive the full credential triple
  4. Prints the three values in ready-to-paste .env format

Safe to run multiple times — returns the SAME credentials each time
(they're deterministic from your private key + the signed message).

Run:
    source venv/bin/activate
    python3 scripts/derive_api_creds.py
"""
from __future__ import annotations

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


def main():
    pkey = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
    if not pkey:
        print("[FAIL] POLYMARKET_PRIVATE_KEY not set in .env", file=sys.stderr)
        print("       Add your MetaMask private key (with 0x prefix) to .env first.")
        return 1
    if not pkey.startswith("0x") or len(pkey) != 66:
        print(f"[FAIL] POLYMARKET_PRIVATE_KEY malformed (got len={len(pkey)}, "
              f"want 66 with 0x prefix)", file=sys.stderr)
        return 1

    try:
        from py_clob_client.client import ClobClient
    except ImportError:
        print("[FAIL] py-clob-client not installed. Run:", file=sys.stderr)
        print("           pip install py-clob-client", file=sys.stderr)
        return 1

    POLYGON = 137
    HOST = "https://clob.polymarket.com"

    print(f"Constructing ClobClient (chain_id={POLYGON})...")
    try:
        # Minimal client — no API creds yet
        client = ClobClient(host=HOST, key=pkey, chain_id=POLYGON)
    except Exception as e:
        print(f"[FAIL] client construction: {e}", file=sys.stderr)
        return 1

    print("Deriving API credentials (signs EIP-712 and queries Polymarket)...")
    try:
        # This either creates a new API key set if none exists, or derives
        # the existing one deterministically. Same result either way.
        creds = client.create_or_derive_api_creds()
    except AttributeError:
        # Older SDK version — fall back to separate methods
        try:
            creds = client.derive_api_key()
        except Exception:
            try:
                creds = client.create_api_key()
            except Exception as e2:
                print(f"[FAIL] API key derive/create failed: {e2}", file=sys.stderr)
                return 1
    except Exception as e:
        print(f"[FAIL] API key derive failed: {e}", file=sys.stderr)
        print("       Possible causes:", file=sys.stderr)
        print("         - Polymarket account not set up (visit polymarket.com first)", file=sys.stderr)
        print("         - Private key is for wrong wallet", file=sys.stderr)
        print("         - Rate-limit on API key operations (wait 60s)", file=sys.stderr)
        return 1

    # Extract values from the creds object (handle various shapes)
    api_key = getattr(creds, "api_key", None) or creds.get("apiKey") if hasattr(creds, "get") else None
    api_secret = getattr(creds, "api_secret", None) or (creds.get("secret") if hasattr(creds, "get") else None)
    api_passphrase = getattr(creds, "api_passphrase", None) or (creds.get("passphrase") if hasattr(creds, "get") else None)

    if not (api_key and api_secret and api_passphrase):
        # Try direct attribute access with different names
        api_key = getattr(creds, "apiKey", api_key)
        api_secret = getattr(creds, "secret", api_secret)
        api_passphrase = getattr(creds, "passphrase", api_passphrase)

    if not (api_key and api_secret and api_passphrase):
        print(f"[FAIL] could not extract all 3 values from creds object", file=sys.stderr)
        print(f"       creds type: {type(creds).__name__}", file=sys.stderr)
        print(f"       creds repr: {creds!r}"[:300], file=sys.stderr)
        print(f"       attrs: {dir(creds)}", file=sys.stderr)
        return 1

    print()
    print("=" * 70)
    print("API CREDENTIALS DERIVED — add these lines to .env")
    print("=" * 70)
    print()
    print(f"POLYMARKET_API_KEY={api_key}")
    print(f"POLYMARKET_API_SECRET={api_secret}")
    print(f"POLYMARKET_API_PASSPHRASE={api_passphrase}")
    print()
    print("=" * 70)
    print()
    print("Now:")
    print("  1. Copy those 3 lines into /home/botuser/whale-bot/.env")
    print("  2. chmod 600 /home/botuser/whale-bot/.env")
    print("  3. Run: python3 scripts/live_precheck.py")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
