#!/usr/bin/env bash
# Remove the paper-trader repair cron job. Paper positions that are
# booked RESOLVED from now on will stay RESOLVED until someone runs
# repair_paper_resolutions.py --apply manually.
set -euo pipefail

if ! crontab -l 2>/dev/null | grep -q 'repair_paper_resolutions.py'; then
    echo "No repair cron found — nothing to remove."
    exit 0
fi

crontab -l 2>/dev/null | grep -v 'repair_paper_resolutions.py' | crontab -
echo "[OK] Removed repair cron."
