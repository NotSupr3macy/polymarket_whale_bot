#!/usr/bin/env bash
# Start the paper trader daemon in its own tmux session.
#
# Reads filtered whale signals from the whale-tracker DB tables and
# copies them into a $100 paper bankroll, with per-whale sizing and
# conviction multipliers. Sends alerts to a SEPARATE Telegram bot via
# the PAPER_BOT_TOKEN / PAPER_BOT_CHAT_ID env vars.
set -euo pipefail
cd "$(dirname "$0")/.."

SESSION="paper-trader"

if tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "Session '$SESSION' already running. Stop it first: ./deploy/stop_paper_trader.sh"
    exit 1
fi

if [ -f .env ]; then
    set -a; source .env; set +a
fi

# Fail fast if the separate paper-bot creds are missing.
if [ -z "${PAPER_BOT_TOKEN:-}" ] || [ -z "${PAPER_BOT_CHAT_ID:-}" ]; then
    echo "ERROR: PAPER_BOT_TOKEN and PAPER_BOT_CHAT_ID must be set in .env"
    echo "  (register a new bot via @BotFather, add both to .env)"
    exit 1
fi

tmux new-session -d -s "$SESSION" \
    "cd $(pwd) && source venv/bin/activate && python3 monitor/paper_trader.py 2>&1 | tee -a logs/paper_trader.log"

echo "Paper trader started in tmux session '$SESSION'"
echo "  Attach:  tmux attach -t $SESSION"
echo "  Logs:    tail -f logs/paper_trader.log"
echo "  Stop:    ./deploy/stop_paper_trader.sh"
