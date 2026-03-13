#!/usr/bin/env bash
# Start the whale bot in a tmux session.
# Usage: ./deploy/start.sh [--live] [--bankroll 500]

set -euo pipefail
cd "$(dirname "$0")/.."

SESSION="whale-bot"

if tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "Bot already running in tmux session '$SESSION'"
    echo "  Attach: tmux attach -t $SESSION"
    echo "  Stop:   ./deploy/stop.sh"
    exit 1
fi

# Pass all CLI args through (e.g. --live --bankroll 500)
tmux new-session -d -s "$SESSION" \
    "python3 cli.py $* 2>&1 | tee -a logs/bot.log"

echo "Bot started in tmux session '$SESSION'"
echo "  Attach:  tmux attach -t $SESSION"
echo "  Logs:    tail -f logs/bot.log"
echo "  Stop:    ./deploy/stop.sh"
