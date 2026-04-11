#!/usr/bin/env bash
# Start the EV cashout engine in its own tmux session
set -euo pipefail
cd "$(dirname "$0")/.."

SESSION="ev-engine"

if tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "Session '$SESSION' already running. Stop it first: ./deploy/stop_ev_engine.sh"
    exit 1
fi

if [ -f .env ]; then
    set -a; source .env; set +a
fi

mkdir -p logs

tmux new-session -d -s "$SESSION" \
    "cd $(pwd) && source venv/bin/activate && python3 -m ev_engine.main --interval 60 2>&1 | tee -a logs/ev_engine.log"

echo "EV engine started in tmux session '$SESSION'"
echo "  Attach:  tmux attach -t $SESSION"
echo "  Logs:    tail -f logs/ev_engine.log"
echo "  Stop:    ./deploy/stop_ev_engine.sh"
