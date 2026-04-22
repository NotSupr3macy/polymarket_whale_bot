#!/usr/bin/env bash
# Stop the live trader daemon. Does NOT cancel open CLOB orders —
# those remain active and can still fill. If you want to cancel all
# open orders too, use scripts/live_emergency_stop.py (if built) or
# cancel manually via Polymarket UI.
set -euo pipefail

SESSION="${LIVE_SESSION:-live-trader}"

if ! tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "No tmux session '$SESSION' running"
    exit 0
fi

tmux kill-session -t "$SESSION"
echo "Stopped tmux session '$SESSION'"
echo
echo "NOTE: open CLOB orders remain active. To see them:"
echo "    cd /home/botuser/whale-bot && source venv/bin/activate"
echo "    python3 -c 'import asyncio; from monitor.clob_client_wrapper import get_open_orders; print(asyncio.run(get_open_orders()))'"
echo
echo "To cancel all open orders manually, use Polymarket UI."
