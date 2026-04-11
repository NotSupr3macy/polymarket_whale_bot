#!/usr/bin/env bash
# Stop the EV cashout engine
set -euo pipefail

SESSION="ev-engine"

if tmux has-session -t "$SESSION" 2>/dev/null; then
    tmux kill-session -t "$SESSION"
    echo "EV engine stopped."
else
    echo "No '$SESSION' tmux session running."
fi
