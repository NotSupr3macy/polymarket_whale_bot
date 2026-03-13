#!/usr/bin/env bash
# Health check — auto-restart bot if crashed.
# Add to cron: */5 * * * * /path/to/deploy/health_check.sh --live >> logs/health.log 2>&1

set -euo pipefail
cd "$(dirname "$0")/.."

SESSION="whale-bot"
LOG="logs/health.log"

if tmux has-session -t "$SESSION" 2>/dev/null; then
    exit 0  # Still running, all good
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') | Bot not running — restarting..." >> "$LOG"
./deploy/start.sh "$@"
echo "$(date '+%Y-%m-%d %H:%M:%S') | Bot restarted" >> "$LOG"
