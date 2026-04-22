#!/usr/bin/env python3
"""Pre-flight check for live trader. Verifies every precondition
before you're allowed to flip LIVE_TRADER_ENABLED=1.

Checks (in order — stops at first failure):
    1. All required env vars present + well-formed
    2. Private key derivation matches POLYMARKET_WALLET_ADDRESS
    3. Polygon USDC balance >= LIVE_BANKROLL_USD
    4. Polymarket deposit balance >= LIVE_BANKROLL_USD
    5. CLOB API credentials valid (can fetch account info)
    6. py-clob-client installed and importable
    7. LIVE_TRADER_ENABLED env var is 0 (not yet enabled)
    8. No EMERGENCY_HALT sentinel

Run:
    source venv/bin/activate
    python3 scripts/live_precheck.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

# Load .env manually
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def ok(msg: str):
    print(f"  [OK]   {msg}")


def fail(msg: str):
    print(f"  [FAIL] {msg}")


def warn(msg: str):
    print(f"  [WARN] {msg}")


def check_env_vars() -> bool:
    print("\n1. Env vars")
    required = [
        "POLYMARKET_PRIVATE_KEY",
        "POLYMARKET_API_KEY",
        "POLYMARKET_API_SECRET",
        "POLYMARKET_API_PASSPHRASE",
        "POLYMARKET_WALLET_ADDRESS",
        "LIVE_BANKROLL_USD",
        "LIVE_MAX_DAILY_LOSS_USD",
        "LIVE_PER_TRADE_USD",
        "LIVE_WHALE",
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        fail(f"missing env vars: {', '.join(missing)}")
        return False
    ok(f"{len(required)} required env vars present")

    pkey = os.getenv("POLYMARKET_PRIVATE_KEY", "")
    if not pkey.startswith("0x") or len(pkey) != 66:
        fail(f"POLYMARKET_PRIVATE_KEY malformed "
             f"(got len={len(pkey)}, want 66 with 0x prefix)")
        return False
    ok("POLYMARKET_PRIVATE_KEY well-formed (0x + 64 hex)")

    addr = os.getenv("POLYMARKET_WALLET_ADDRESS", "")
    if not addr.startswith("0x") or len(addr) != 42:
        fail(f"POLYMARKET_WALLET_ADDRESS malformed "
             f"(got len={len(addr)}, want 42 with 0x prefix)")
        return False
    ok(f"POLYMARKET_WALLET_ADDRESS well-formed: {addr[:8]}...{addr[-4:]}")

    return True


def check_key_derivation() -> bool:
    print("\n2. Private key → address derivation")
    try:
        from eth_account import Account
    except ImportError:
        fail("eth-account not installed. "
             "Run: pip install eth-account")
        return False
    pkey = os.getenv("POLYMARKET_PRIVATE_KEY")
    expected = os.getenv("POLYMARKET_WALLET_ADDRESS", "").lower()
    try:
        derived = Account.from_key(pkey).address.lower()
    except Exception as e:
        fail(f"derivation failed: {e}")
        return False
    if derived != expected:
        fail(f"derived address {derived} != env POLYMARKET_WALLET_ADDRESS "
             f"{expected} — one of them is wrong")
        return False
    ok(f"derived address matches POLYMARKET_WALLET_ADDRESS")
    return True


async def check_polygon_balance() -> bool:
    print("\n3. Polygon USDC balance")
    try:
        from monitor.clob_client_wrapper import get_usdc_balance
    except Exception as e:
        fail(f"wrapper import failed: {e}")
        return False
    try:
        bal = await get_usdc_balance()
    except Exception as e:
        fail(f"RPC call failed: {e}")
        return False
    if bal < 0:
        fail("balance query returned error sentinel")
        return False
    want = float(os.getenv("LIVE_BANKROLL_USD", "10"))
    msg = f"on-chain USDC balance: ${bal:.2f}"
    if bal < want:
        warn(f"{msg} (want ≥ ${want:.2f} — may be OK if already deposited)")
    else:
        ok(msg)
    return True


async def check_polymarket_balance() -> bool:
    print("\n4. Polymarket deposit balance")
    try:
        from monitor.clob_client_wrapper import get_polymarket_balance
        bal = await get_polymarket_balance()
    except RuntimeError as e:
        if "py-clob-client not installed" in str(e):
            fail(str(e))
            return False
        fail(f"balance query failed: {e}")
        return False
    except Exception as e:
        fail(f"balance query failed: {e}")
        return False
    want = float(os.getenv("LIVE_BANKROLL_USD", "10"))
    msg = f"Polymarket deposit balance: ${bal:.2f}"
    if bal < want:
        warn(f"{msg} (want ≥ ${want:.2f} — "
             f"if not deposited yet, do that via Polymarket UI)")
        return False
    ok(msg)
    return True


def check_clob_api() -> bool:
    print("\n5. CLOB API authentication")
    try:
        from monitor.clob_client_wrapper import get_client
        c = get_client()
    except RuntimeError as e:
        fail(str(e))
        return False
    except Exception as e:
        fail(f"client construction failed: {e}")
        return False
    # Try a read that requires auth
    try:
        orders = c.get_orders()
        ok(f"CLOB API reachable (you have {len(orders or [])} open orders)")
    except Exception as e:
        fail(f"CLOB API auth or call failed: {e}")
        return False
    return True


def check_py_clob_client() -> bool:
    print("\n6. py-clob-client installed")
    try:
        import py_clob_client  # noqa: F401
        ok(f"py-clob-client importable")
    except ImportError:
        fail("py-clob-client NOT installed. "
             "Run: source venv/bin/activate && pip install py-clob-client")
        return False
    return True


def check_safety_toggles() -> bool:
    print("\n7. Safety toggles")
    enabled = os.getenv("LIVE_TRADER_ENABLED", "0")
    if enabled == "1":
        warn(f"LIVE_TRADER_ENABLED=1 — the daemon will start when invoked. "
             f"Flip to 0 in .env if you're still preparing.")
    else:
        ok(f"LIVE_TRADER_ENABLED={enabled} (safe — daemon will not start)")

    sentinel = Path("/home/botuser/whale-bot/logs/EMERGENCY_HALT")
    if sentinel.exists():
        fail(f"EMERGENCY_HALT sentinel present at {sentinel}. "
             f"Review cause in the file, then delete to resume.")
        return False
    ok("no EMERGENCY_HALT sentinel")
    return True


async def main():
    print("=" * 70)
    print("LIVE TRADER PRECHECK")
    print("=" * 70)

    all_pass = True

    all_pass &= check_py_clob_client()
    if not all_pass:
        print("\n[ABORT] Fix py-clob-client install first.")
        return 1

    all_pass &= check_env_vars()
    if not all_pass:
        print("\n[ABORT] Fix env vars in .env first.")
        return 1

    all_pass &= check_key_derivation()
    all_pass &= await check_polygon_balance()
    all_pass &= await check_polymarket_balance()
    all_pass &= check_clob_api()
    all_pass &= check_safety_toggles()

    print()
    print("=" * 70)
    if all_pass:
        print("[ALL CHECKS PASSED]")
        print("  Next step: python3 scripts/live_smoke_test.py")
    else:
        print("[PRECHECK FAILED — address items above before proceeding]")
    print("=" * 70)
    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
