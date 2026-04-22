# VPS Migration: US → Frankfurt (Germany)

**Why:** Polymarket's CLOB API geoblocks US IPs. Moving the bot to a
European data center unblocks live trading. Paper trader + whale
tracker also work fine from the new region.

**Time estimate:** 30–45 min of focused work.
**Cost:** Same ($6/mo DigitalOcean droplet). Keep old as backup for a
week before destroying.

**Chosen region:** Frankfurt (FRA1) — closest to Vienna where you
created your Polymarket account.

---

## Phase 1: Spin up new droplet (10 min)

### 1.1 DigitalOcean control panel

1. Go to https://cloud.digitalocean.com → Droplets → **Create Droplet**
2. **Region:** Frankfurt (FRA1)
3. **OS:** Ubuntu 24.04 LTS (match existing server)
4. **Plan:** Basic → Regular Intel → $6/mo (1 GB / 1 CPU) — same as current
5. **Authentication:** SSH key (add your existing public key, or create new)
6. **Hostname:** `whale-bot-fra1`
7. Click **Create Droplet**. Wait ~60 sec for provisioning.
8. Note the new IP (e.g., `167.71.x.x`). Call this `$NEW_IP` below.

### 1.2 Initial SSH

```bash
# Test SSH works
ssh root@$NEW_IP

# First login: you'll get a host-key fingerprint warning. Type 'yes'.
# Then you should be in a fresh Ubuntu shell.

# Update the system (takes ~2 min)
apt update && apt upgrade -y

# Install the basics we need
apt install -y python3 python3-pip python3-venv python3-dev \
               git tmux sqlite3 curl build-essential

# Verify versions (should match what we had)
python3 --version    # 3.12.x expected
```

---

## Phase 2: Package state on OLD server (5 min)

SSH into the OLD server (`ssh root@138.197.104.70`) and bundle everything:

```bash
cd /home/botuser/whale-bot
source venv/bin/activate

# Stop everything to get a clean snapshot
bash deploy/stop_paper_trader.sh 2>/dev/null || true
bash deploy/stop_live_trader.sh 2>/dev/null || true
tmux kill-server 2>/dev/null || true

# Verify nothing's running
ps -eo pid,cmd | grep -E "paper_trader|live_trader|whale_tracker" | grep -v grep
# Should be empty

# Create migration bundle
mkdir -p /tmp/migration
cp .env /tmp/migration/env-backup              # secrets
cp trades.db /tmp/migration/                   # main DB
cp -r logs /tmp/migration/logs-backup 2>/dev/null || true
crontab -l > /tmp/migration/crontab.txt 2>/dev/null || echo "# no crontab" > /tmp/migration/crontab.txt

# Check contents
ls -lh /tmp/migration/
du -sh /tmp/migration/

# Package it
cd /tmp
tar czf migration.tar.gz migration/
ls -lh /tmp/migration.tar.gz
# Should be a few MB
```

---

## Phase 3: Transfer to NEW server (5 min)

From the OLD server, `scp` to the new:

```bash
# Replace $NEW_IP with your new droplet's IP
scp /tmp/migration.tar.gz root@$NEW_IP:/tmp/
```

(You may get a host-key prompt on first SSH-from-old-to-new — type `yes`.)

On the NEW server:

```bash
cd /tmp
tar xzf migration.tar.gz
ls migration/
```

---

## Phase 4: Set up NEW server (15 min)

### 4.1 Create botuser + directory structure

```bash
# Mirror the old server's structure
useradd -m -s /bin/bash botuser
mkdir -p /home/botuser/whale-bot
cd /home/botuser/whale-bot

# Clone the repo
git clone https://github.com/NotSupr3macy/polymarket_whale_bot.git .

# Restore state
cp /tmp/migration/env-backup .env
cp /tmp/migration/trades.db .
cp -r /tmp/migration/logs-backup logs 2>/dev/null || mkdir -p logs

# Set permissions
chown -R botuser:botuser /home/botuser/whale-bot
chmod 600 /home/botuser/whale-bot/.env
```

### 4.2 Python venv

```bash
cd /home/botuser/whale-bot
python3 -m venv venv
source venv/bin/activate

# Install deps (adjust if requirements.txt exists)
pip install --upgrade pip
pip install aiohttp requests python-dotenv \
            py-clob-client eth-account

# Check the modules that already existed
pip list | grep -E "aiohttp|clob|eth"
```

If there's a `requirements.txt` in the repo, use it:
```bash
pip install -r requirements.txt
```

### 4.3 Verify Python imports work

```bash
# Quick sanity check
python3 -c "
import sqlite3, asyncio, aiohttp
print('stdlib OK')
import py_clob_client
print('py_clob_client:', py_clob_client.__version__ if hasattr(py_clob_client, '__version__') else 'OK')
"
```

### 4.4 Run precheck from the new IP

```bash
python3 scripts/live_precheck.py
```

**All checks should pass** now. Specifically:
- Polymarket balance should still show $10 (same account)
- CLOB API auth should succeed (now that IP isn't geoblocked)
- No EMERGENCY_HALT sentinel (fresh server)

### 4.5 Run smoke test

```bash
python3 scripts/live_smoke_test.py
```

**This should succeed now** (the geoblock is gone). You'll see the
actual round-trip latency, order placement, and cancel all work.

---

## Phase 5: Restore background jobs (5 min)

### 5.1 Restore crontab

```bash
cat /tmp/migration/crontab.txt
# If there are entries (repair_paper_resolutions cron), apply them:
crontab /tmp/migration/crontab.txt

# Verify
crontab -l
```

### 5.2 Restart whale tracker (paper trading side — optional)

If you want to keep the paper bot running alongside the live trial:

```bash
cd /home/botuser/whale-bot
bash deploy/start_paper_trader.sh
# and any whale-specific trackers that were running before:
bash deploy/start_sportmaster.sh 2>/dev/null
bash deploy/start_kch123.sh 2>/dev/null
bash deploy/start_bigsix.sh 2>/dev/null
bash deploy/start_gambling.sh 2>/dev/null
bash deploy/start_nbasniper.sh 2>/dev/null
bash deploy/start_theonlyhuman.sh 2>/dev/null

# Verify all running
tmux ls
```

### 5.3 Enable + start live trader

```bash
# Flip the safety toggle
sed -i 's/LIVE_TRADER_ENABLED=0/LIVE_TRADER_ENABLED=1/' .env

# Start the daemon
bash deploy/start_live_trader.sh

# Watch first fires
tail -f logs/live_trader.log
# Ctrl-C to detach (daemon keeps running)
```

---

## Phase 6: Verify everything (5 min)

```bash
# Processes running
tmux ls

# Logs updating
tail -5 logs/paper_trader.log
tail -5 logs/live_trader.log 2>/dev/null

# DB reflects recent activity
sqlite3 trades.db "SELECT COUNT(*), MAX(opened_at) FROM paper_positions WHERE opened_at > datetime('now','-1 hour')"
sqlite3 trades.db "SELECT bankroll_usd FROM paper_state WHERE id=1"
sqlite3 trades.db "SELECT bankroll_usd FROM live_state WHERE id=1"

# Telegram working (should receive 6h updates same as before)
```

---

## Phase 7: Cleanup (after 24 hours)

If everything works for a full day:

### 7.1 Destroy old droplet

1. DigitalOcean control panel → Droplets → old server → Destroy
2. Save ~$6/mo

### 7.2 Update GitHub deploy keys (if any)
If the old server had write access to the GitHub repo (unlikely for this setup), revoke its SSH key.

### 7.3 Delete migration bundle

```bash
# On the NEW server
rm -rf /tmp/migration /tmp/migration.tar.gz
```

---

## Gotchas to watch for

### Time zone differences
Old server was likely US Eastern or UTC. New server in Frankfurt is UTC+1 by default. The bot uses UTC internally so this doesn't break anything, but log timestamps may look different.

```bash
# Set server to UTC to match log expectations
timedatectl set-timezone UTC
timedatectl
```

### SSH key fingerprint warnings
Each time you SSH between servers you'll see a first-time fingerprint warning. Type `yes` to accept.

### Droplet firewall
DigitalOcean droplets are open by default. If you had a UFW firewall configured on the old one, reapply:

```bash
# Check old server
ssh root@138.197.104.70 'ufw status verbose'
# If active, apply same rules to new server
```

### Verify non-US IP
Before running live_smoke_test:

```bash
curl -s https://ipinfo.io/json | python3 -m json.tool | grep -E "country|city|region"
```

Should show country `DE` (Germany). If it shows `US`, something's wrong.

---

## Rollback (if migration fails)

If the new server doesn't work after 2 hours of debugging:

1. Old server still has all state — you didn't delete it
2. Restart whale trackers + paper trader on old server:
   ```bash
   ssh root@138.197.104.70
   cd /home/botuser/whale-bot
   bash deploy/start_paper_trader.sh
   bash deploy/start_sportmaster.sh  # etc.
   ```
3. Live trader stays off (US geoblock makes it impossible from there anyway)
4. Destroy new droplet to stop billing

---

**Last updated:** 2026-04-22 — Phase 1c migration for live trial
