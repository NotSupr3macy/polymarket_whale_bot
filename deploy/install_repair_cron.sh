#!/usr/bin/env bash
# Install a cron job that runs repair_paper_resolutions.py --apply
# every 30 minutes. Upgrades RESOLVED break-even rows to WIN/LOSS
# when Gamma has caught up to ground truth.
#
# Idempotent: removes any existing repair cron first, then installs.
# Run: bash deploy/install_repair_cron.sh
set -euo pipefail

BOT_DIR="${BOT_DIR:-/home/botuser/whale-bot}"
PY="$BOT_DIR/venv/bin/python3"
SCRIPT="$BOT_DIR/scripts/repair_paper_resolutions.py"
LOG="$BOT_DIR/logs/repair_cron.log"

if [[ ! -x "$PY" ]]; then
    echo "ERROR: python3 not found at $PY" >&2
    exit 1
fi
if [[ ! -f "$SCRIPT" ]]; then
    echo "ERROR: repair script not found at $SCRIPT" >&2
    exit 1
fi

# Build cron line: every 30 min, apply repairs, log stdout+stderr
CRON_LINE="*/30 * * * * cd $BOT_DIR && $PY $SCRIPT --apply >> $LOG 2>&1"

# Replace any existing repair cron line, keep others intact
(crontab -l 2>/dev/null | grep -v 'repair_paper_resolutions.py' || true; \
 echo "$CRON_LINE") | crontab -

echo "[OK] Installed repair cron:"
crontab -l | grep repair_paper_resolutions
echo
echo "Log will be written to: $LOG"
echo "To remove: bash deploy/uninstall_repair_cron.sh"
