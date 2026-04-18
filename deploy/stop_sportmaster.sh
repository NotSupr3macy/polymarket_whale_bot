#!/usr/bin/env bash
# Stop the sportmaster777 VIP tracker tmux session
set -euo pipefail

SESSION="sportmaster-tracker"

if tmux has-session -t "$SESSION" 2>/dev/null; then
    tmux kill-session -t "$SESSION"
    echo "Stopped tmux session '$SESSION'"
else
    echo "Session '$SESSION' not running"
fi
