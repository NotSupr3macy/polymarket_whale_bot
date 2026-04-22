# Live Trial Setup Guide

**Goal:** Get a funded Polymarket account + API credentials onto the
server so `live_trader.py` can auto-trade $10 following sportmaster777
for 3 days.

**Time estimate:** 60–90 minutes of focused work across ~24 hours
(waiting on on-chain confirmations is the slowest part).

---

## Step 0 — Threat model (read before starting)

You're doing this from a US residence. The steps below involve VPN and
wallet-level circumvention. Assume:

- **The $10 could be frozen** if Polymarket's anti-circumvention
  detects US origin. You might lose the funds AND the account.
- **Use a fresh wallet** that has never held significant funds. If
  anything gets compromised, the blast radius is $10.
- **Never share the wallet private key in chat** — even with me.

---

## Step 1 — VPN

Install a reputable paid VPN. Avoid free ones (they sell data).

- **NordVPN**, **ExpressVPN**, **Mullvad**, or **ProtonVPN** all work.
- Connect to a **non-US** region. Canada, Mexico, UK, or EU is fine.
- Verify your IP with https://whatismyipaddress.com/ — should show
  a non-US location.
- **Keep the VPN on for every Polymarket action** (signup, deposit, etc).

---

## Step 2 — Create a fresh Polygon wallet

**Do NOT reuse any existing wallet.** Use MetaMask or Rabby:

1. Install MetaMask browser extension (https://metamask.io)
2. Create a new wallet — write the seed phrase on paper, store offline
3. Rename the default account to something like `polymarket-trial`
4. Add the **Polygon** network if not already there:
   - Click the network dropdown → Add Network → Polygon
   - Or use https://chainlist.org/?search=polygon → Add to MetaMask
5. **Back up the private key separately** (MetaMask → Account Details
   → Export Private Key). You'll need this later for the server.
   - Store in a password manager, NOT a text file

---

## Step 3 — Sign up on Polymarket

1. With VPN on, go to https://polymarket.com
2. Click **Log In** → select **MetaMask** (or Magic / email option)
3. Approve the signature request in MetaMask
4. Complete any profile setup they request (no KYC for small accounts
   typically — they may ask for email verification)

You now have a Polymarket account tied to your new wallet.

---

## Step 4 — Fund the wallet with $10 USDC on Polygon

You need **USDC.e** (the bridged USDC Polymarket uses) on the
**Polygon** network. Not Ethereum mainnet, not regular USDC.

**Cheapest path (recommended):**

1. Buy $12 of USDC on Coinbase/Kraken/Gemini (slight buffer for gas)
2. Withdraw to your MetaMask Polygon address
3. Select **Polygon network** for the withdrawal (saves ~$20 vs ETH)
4. Wait for deposit confirmation (~5–10 min)

**Alternative (if your exchange doesn't support Polygon withdrawals):**

1. Buy USDC on Ethereum, send to wallet
2. Bridge to Polygon via:
   - Polymarket's built-in bridge (in their deposit UI)
   - Or https://wallet.polygon.technology/
3. Note: this costs ~$10–20 in gas, not recommended for $10 trial

Verify your wallet shows ~$10 USDC.e on Polygon via
https://polygonscan.com/address/YOUR_WALLET_ADDRESS

---

## Step 5 — Deposit to Polymarket

1. On polymarket.com (with VPN still on), click **Deposit**
2. Select USDC
3. Approve the `approve()` call in MetaMask (one-time, ~$0.01 gas)
4. Approve the `deposit()` call (~$0.01 gas)
5. Wait for the Polymarket UI to show your $10 balance

At this point you should see `$10.00` usable on Polymarket.

---

## Step 6 — Generate CLOB API credentials

1. Go to https://polymarket.com/settings/api
2. Click **Create API Key**
3. Copy these three values **immediately** (only shown once):
   - `api_key`
   - `api_secret`
   - `api_passphrase`

Store all three in a password manager.

---

## Step 7 — Add credentials to server `.env`

SSH into the server:

```bash
ssh root@138.197.104.70
cd /home/botuser/whale-bot
nano .env
```

Add these lines at the bottom (replace with your actual values):

```bash
# === Live trader credentials (sportmaster trial) ===
POLYMARKET_PRIVATE_KEY=0xYOUR_PRIVATE_KEY_HERE
POLYMARKET_API_KEY=YOUR_API_KEY
POLYMARKET_API_SECRET=YOUR_API_SECRET
POLYMARKET_API_PASSPHRASE=YOUR_API_PASSPHRASE
POLYMARKET_WALLET_ADDRESS=0xYOUR_PUBLIC_ADDRESS

# Safety toggles (trial settings — do not change until trial ends)
LIVE_TRADER_ENABLED=0            # Flip to 1 only when ready to go live
LIVE_MAX_DAILY_LOSS_USD=5        # Circuit breaker at 50% of $10
LIVE_BANKROLL_USD=10             # Initial bankroll
LIVE_PER_TRADE_USD=1             # Fixed $1 per trade
LIVE_WHALE=sportmaster777        # Only this whale fires
```

Set strict permissions:

```bash
chmod 600 .env
chown botuser:botuser .env
```

Verify no extra spaces, no quotes around values (bash `.env` loader
is picky):

```bash
grep POLYMARKET .env
# Should show 5 lines, each with KEY=value format
```

---

## Step 8 — Confirm readiness

Run this diagnostic (will be created alongside `live_trader.py`):

```bash
cd /home/botuser/whale-bot
source venv/bin/activate
python3 scripts/live_precheck.py
```

Expected output (green means ready):

```
[OK] POLYMARKET_PRIVATE_KEY loaded (64 hex chars)
[OK] POLYMARKET_API_* credentials loaded
[OK] Wallet address matches private key derivation
[OK] USDC balance on Polygon: $10.00
[OK] Polymarket deposit balance: $10.00
[OK] CLOB API authentication successful
[OK] LIVE_TRADER_ENABLED=0 (safe — no trading yet)
```

If any line shows `[FAIL]`, do not proceed. Message me with the error.

---

## Step 9 — Smoke test (one $0.50 order)

When `live_precheck.py` is green:

```bash
python3 scripts/live_smoke_test.py
```

This places ONE real order for $0.50, waits for fill, then
immediately cancels or accepts based on the market. Expected output:

```
[SMOKE] Placing $0.50 test order on sportmaster's lowest-price position
[SMOKE] Order ID: 0x...
[SMOKE] Filled at $0.XXX, slippage YY bps
[SMOKE] Position appears in live_positions table
[SMOKE] Position verified on-chain
[OK] Smoke test passed — pipeline functional
```

If the smoke test is green, you're cleared for the 3-day live trial.

---

## Step 10 — Go live

```bash
# Flip the safety toggle
sed -i 's/LIVE_TRADER_ENABLED=0/LIVE_TRADER_ENABLED=1/' .env

# Start the daemon
bash deploy/start_live_trader.sh

# Watch the first few ticks
tail -f logs/live_trader.log
```

**You're now live.** Every sportmaster signal will trigger a real
$1 order.

---

## Daily monitoring during the trial

```bash
# Check bankroll + open positions
sqlite3 trades.db "SELECT * FROM live_state WHERE id=1"
sqlite3 -header -column trades.db "
  SELECT substr(opened_at,12,5) AS t, status, outcome,
         ROUND(entry_price,3) AS entry, ROUND(actual_price,3) AS fill,
         ROUND(pnl_usd,2) AS pnl, substr(market_title,1,40) AS mkt
  FROM live_positions
  WHERE opened_at > datetime('now','-1 day')
  ORDER BY opened_at DESC"

# Any errors?
grep -iE "error|fail|emergency" logs/live_trader.log | tail

# Reconciliation drift (should be <$1)
tail -5 logs/live_reconcile.log
```

---

## Emergency stop

If anything looks wrong:

```bash
bash deploy/stop_live_trader.sh

# Or if daemon is hung:
tmux kill-session -t live-trader
```

Open orders stay open on Polymarket — you can cancel them manually via
the website if needed. Deposited USDC is never at risk from a code bug
(only from market outcomes).

---

## End of trial (after 3 days)

1. Stop the daemon
2. Wait for any open positions to resolve + redeem
3. Run `scripts/live_trial_report.py` (will be built) — generates
   summary of slippage, fill rate, realized PnL, lessons learned
4. Decide: scale to $50, or iterate on code, or stop

---

**Last updated:** 2026-04-22 — drafted at start of Phase 1c live trial
