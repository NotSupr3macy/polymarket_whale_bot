#!/usr/bin/env bash
# Gracefully stop the whale bot (sends SIGINT for clean shutdown).

set -euo pipefail

SESSION="whale-bot"

if ! tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "No bot session running."
    exit 0
fi

echo "Sending SIGINT to bot (graceful shutdown)..."
tmux send-keys -t "$SESSION" C-c

# Wait up to 15 seconds for graceful exit
for i in $(seq 1 15); do
    if ! tmux has-session -t "$SESSION" 2>/dev/null; then
        echo "Bot stopped cleanly."
        exit 0
    fi
    sleep 1
done

# Force kill if still running
echo "Force-killing tmux session..."
tmux kill-session -t "$SESSION"
echo "Bot killed."
