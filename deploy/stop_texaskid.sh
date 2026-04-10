#!/usr/bin/env bash
# Stop the TexasKid VIP tracker
set -euo pipefail

SESSION="texaskid-tracker"

if ! tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "No '$SESSION' session running."
    exit 0
fi

# Send Ctrl+C for graceful shutdown
tmux send-keys -t "$SESSION" C-c
sleep 2

# Kill session if still alive
if tmux has-session -t "$SESSION" 2>/dev/null; then
    tmux kill-session -t "$SESSION"
fi

echo "TexasKid tracker stopped."
