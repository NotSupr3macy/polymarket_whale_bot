#!/usr/bin/env bash
# Package all server state into /tmp/migration.tar.gz for VPS migration.
#
# Captures:
#   - .env (secrets)
#   - trades.db (main DB with all positions/state)
#   - logs/ directory (paper_trader, live_trader, whale tracker logs)
#   - crontab (if any)
#
# Does NOT capture:
#   - venv/ (rebuild on new server via pip install)
#   - repo code (git clone on new server)
#   - tmux sessions (restart on new server)
#
# Run this on the OLD server before migrating.
set -euo pipefail

BOT_DIR="${BOT_DIR:-/home/botuser/whale-bot}"
MIG_DIR="/tmp/migration"
OUT_TGZ="/tmp/migration.tar.gz"

echo "=========================================================="
echo "Migration state packager"
echo "=========================================================="

# Step 1: confirm nothing's actively writing
running=$(ps -eo pid,cmd | grep -E "paper_trader\.py|live_trader\.py|whale_tracker\.py" | grep -v grep | wc -l)
if [[ $running -gt 0 ]]; then
    echo
    echo "[WARN] The following processes are still running:"
    ps -eo pid,cmd | grep -E "paper_trader\.py|live_trader\.py|whale_tracker\.py" | grep -v grep
    echo
    read -p "Kill them to get clean snapshot? [y/N] " -r ans
    if [[ $ans == "y" || $ans == "Y" ]]; then
        tmux kill-server 2>/dev/null || true
        sleep 2
        echo "[OK] processes stopped"
    else
        echo "[ABORT] re-run after stopping daemons (tmux kill-server)"
        exit 1
    fi
fi

# Step 2: create migration directory
rm -rf "$MIG_DIR"
mkdir -p "$MIG_DIR"
cd "$BOT_DIR"

# Step 3: copy .env (the critical secrets)
if [[ ! -f ".env" ]]; then
    echo "[FAIL] $BOT_DIR/.env not found — nothing to migrate"
    exit 1
fi
cp .env "$MIG_DIR/env-backup"
env_lines=$(wc -l < "$MIG_DIR/env-backup")
echo "[OK] .env captured ($env_lines lines)"

# Step 4: copy DB
if [[ ! -f "trades.db" ]]; then
    echo "[WARN] trades.db not found — skipping"
else
    cp trades.db "$MIG_DIR/trades.db"
    db_size=$(du -h trades.db | cut -f1)
    echo "[OK] trades.db captured ($db_size)"
fi

# Step 5: logs (optional but helpful for post-migration analysis)
if [[ -d "logs" ]]; then
    mkdir -p "$MIG_DIR/logs-backup"
    # Compress logs aggressively — they can be large
    for log in logs/*.log; do
        [[ -f "$log" ]] || continue
        gzip -c "$log" > "$MIG_DIR/logs-backup/$(basename "$log").gz"
    done
    log_size=$(du -sh "$MIG_DIR/logs-backup" | cut -f1)
    echo "[OK] logs captured (compressed: $log_size)"
fi

# Step 6: crontab
if crontab -l > "$MIG_DIR/crontab.txt" 2>/dev/null; then
    cron_lines=$(wc -l < "$MIG_DIR/crontab.txt")
    echo "[OK] crontab captured ($cron_lines entries)"
else
    echo "# no crontab" > "$MIG_DIR/crontab.txt"
    echo "[INFO] no crontab found (that's fine if you never set one up)"
fi

# Step 7: create manifest + checksum
{
    echo "MIGRATION BUNDLE"
    echo "Created: $(date -u)"
    echo "Source: $(hostname) / $BOT_DIR"
    echo "IP: $(curl -s https://ipinfo.io/ip 2>/dev/null || echo unknown)"
    echo ""
    echo "Contents:"
    find "$MIG_DIR" -type f -exec ls -lh {} \;
} > "$MIG_DIR/MANIFEST.txt"
cat "$MIG_DIR/MANIFEST.txt"

# Step 8: package
cd /tmp
tar czf "$OUT_TGZ" migration/
chmod 600 "$OUT_TGZ"

echo
echo "=========================================================="
echo "[DONE] Bundle: $OUT_TGZ"
echo "       Size: $(du -h $OUT_TGZ | cut -f1)"
echo "=========================================================="
echo
echo "Next step — transfer to new server:"
echo "  scp $OUT_TGZ root@<NEW_IP>:/tmp/"
echo
echo "On new server:"
echo "  cd /tmp && tar xzf migration.tar.gz"
echo "  # Then follow docs/VPS_MIGRATION.md Phase 4"
