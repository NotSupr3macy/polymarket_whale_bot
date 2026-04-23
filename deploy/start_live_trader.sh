#!/usr/bin/env bash
# Start the live trader daemon in a tmux session.
# REFUSES to start if EMERGENCY_HALT sentinel exists OR if
# LIVE_TRADER_ENABLED != 1 in .env.
set -euo pipefail

BOT_DIR="${BOT_DIR:-/home/botuser/whale-bot}"
SESSION="${LIVE_SESSION:-live-trader}"
LOG_DIR="$BOT_DIR/logs"
LOG_FILE="$LOG_DIR/live_trader.log"
SENTINEL="$LOG_DIR/EMERGENCY_HALT"
ENV_FILE="$BOT_DIR/.env"

mkdir -p "$LOG_DIR"

# Safety gate 1: sentinel
if [[ -f "$SENTINEL" ]]; then
    echo "ERROR: EMERGENCY_HALT sentinel exists at $SENTINEL" >&2
    echo "Contents:" >&2
    cat "$SENTINEL" >&2
    echo >&2
    echo "Review the halt reason. Once resolved, delete the sentinel:" >&2
    echo "    rm $SENTINEL" >&2
    exit 1
fi

# Safety gate 2: LIVE_TRADER_ENABLED
if [[ ! -f "$ENV_FILE" ]]; then
    echo "ERROR: $ENV_FILE not found" >&2
    exit 1
fi
if ! grep -q '^LIVE_TRADER_ENABLED=1' "$ENV_FILE"; then
    echo "ERROR: LIVE_TRADER_ENABLED != 1 in $ENV_FILE" >&2
    echo "To enable live trading, edit .env and set LIVE_TRADER_ENABLED=1" >&2
    exit 1
fi

# Don't double-start
if tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "ERROR: tmux session '$SESSION' already running" >&2
    echo "Stop it first with: bash deploy/stop_live_trader.sh" >&2
    exit 1
fi

# Start in new tmux session.
# Use python3 -u (unbuffered) AND write directly to log file (skip tee)
# because tee block-buffers when stdout isn't a terminal. Adding `tee`
# was masking active daemon output, making it appear dead.
tmux new-session -d -s "$SESSION" -c "$BOT_DIR" \
    "cd $BOT_DIR && source venv/bin/activate && PYTHONUNBUFFERED=1 exec python3 -u monitor/live_trader.py >> $LOG_FILE 2>&1"

sleep 1
if ! tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "ERROR: tmux session failed to start" >&2
    exit 1
fi

echo "Live trader started in tmux session '$SESSION'"
echo "  Attach:  tmux attach -t $SESSION"
echo "  Logs:    tail -f $LOG_FILE"
echo "  Stop:    bash deploy/stop_live_trader.sh"
echo
echo "WATCHING first 20 log lines (Ctrl-C to detach, daemon keeps running):"
sleep 2
tail -n 20 "$LOG_FILE" 2>/dev/null || true
