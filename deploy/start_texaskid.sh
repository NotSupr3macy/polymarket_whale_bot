#!/usr/bin/env bash
# Start the TexasKid VIP tracker in its own tmux session
set -euo pipefail
cd "$(dirname "$0")/.."

SESSION="texaskid-tracker"

if tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "Session '$SESSION' already running. Stop it first: ./deploy/stop_texaskid.sh"
    exit 1
fi

# Source env vars (same .env as bot)
if [ -f .env ]; then
    set -a; source .env; set +a
fi

tmux new-session -d -s "$SESSION" \
    "cd $(pwd) && source venv/bin/activate && python3 monitor/texaskid_tracker.py 2>&1 | tee -a logs/texaskid.log"

echo "TexasKid tracker started in tmux session '$SESSION'"
echo "  Attach:  tmux attach -t $SESSION"
echo "  Logs:    tail -f logs/texaskid.log"
echo "  Stop:    ./deploy/stop_texaskid.sh"
