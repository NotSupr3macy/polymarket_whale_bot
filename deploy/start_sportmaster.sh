#!/usr/bin/env bash
# Start the sportmaster777 VIP tracker in its own tmux session.
# Promoted from shadow pool to paper trader lineup on Apr 18 after 655
# resolved shadow trades at 53.3% WR / +9.2% ROI. Runs with size filter
# $500-$15K (his profitable range, excludes the $15K+ tail-risk bucket).
set -euo pipefail
cd "$(dirname "$0")/.."

SESSION="sportmaster-tracker"

if tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "Session '$SESSION' already running. Stop it first: ./deploy/stop_sportmaster.sh"
    exit 1
fi

if [ -f .env ]; then
    set -a; source .env; set +a
fi

tmux new-session -d -s "$SESSION" \
    "cd $(pwd) && source venv/bin/activate && python3 monitor/sportmaster_tracker.py 2>&1 | tee -a logs/sportmaster.log"

echo "sportmaster777 tracker started in tmux session '$SESSION'"
echo "  Attach:  tmux attach -t $SESSION"
echo "  Logs:    tail -f logs/sportmaster.log"
echo "  Stop:    ./deploy/stop_sportmaster.sh"
