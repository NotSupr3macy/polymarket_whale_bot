#!/usr/bin/env bash
# Launch the NBA historical data puller in a detached tmux session.
# Usage: ./ev_engine/scripts/run_nba_pull.sh [seasons...]
#   e.g. ./ev_engine/scripts/run_nba_pull.sh 2022 2023 2024 2025

set -e
cd "$(dirname "$0")/../.."

SEASONS="${*:-2022 2023 2024 2025}"
SESSION="ev-nba-pull"

if tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "tmux session '$SESSION' already running. Attach with: tmux attach -t $SESSION"
    exit 1
fi

mkdir -p logs
tmux new-session -d -s "$SESSION" \
    "source venv/bin/activate && python -m ev_engine.data_acquisition.nba_puller --seasons $SEASONS 2>&1 | tee -a logs/ev_nba_pull.log"

echo "Started NBA puller in tmux session '$SESSION'"
echo "  Attach: tmux attach -t $SESSION"
echo "  Logs:   tail -f logs/ev_nba_pull.log"
