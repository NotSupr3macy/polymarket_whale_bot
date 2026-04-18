#!/usr/bin/env bash
# Start the nbasniper VIP tracker in its own tmux session.
# SHADOW MODE: all Telegram alerts from the parent bot are muted (empty
# solo_alert_sports); DB writes are still active so the paper trader copies.
set -euo pipefail
cd "$(dirname "$0")/.."

SESSION="nbasniper-tracker"

if tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "Session '$SESSION' already running. Stop it first: ./deploy/stop_nbasniper.sh"
    exit 1
fi

if [ -f .env ]; then
    set -a; source .env; set +a
fi

tmux new-session -d -s "$SESSION" \
    "cd $(pwd) && source venv/bin/activate && python3 monitor/nbasniper_tracker.py 2>&1 | tee -a logs/nbasniper.log"

echo "nbasniper tracker started in tmux session '$SESSION'"
echo "  Attach:  tmux attach -t $SESSION"
echo "  Logs:    tail -f logs/nbasniper.log"
echo "  Stop:    ./deploy/stop_nbasniper.sh"
