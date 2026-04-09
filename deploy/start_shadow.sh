#!/bin/bash
# Start the shadow monitor in a tmux session
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SCRIPT_DIR"

# Check if already running
if tmux has-session -t whale-shadow 2>/dev/null; then
    echo "Shadow monitor already running in tmux session 'whale-shadow'"
    echo "  Attach:  tmux attach -t whale-shadow"
    exit 0
fi

# Activate venv
ACTIVATE=""
if [ -f "venv/bin/activate" ]; then
    ACTIVATE="source venv/bin/activate && "
fi

# Load env
ENV_LOAD=""
if [ -f ".env" ]; then
    ENV_LOAD="export \$(grep -v '^#' .env | grep -v '^\$' | xargs) && "
fi

# Start in tmux
tmux new-session -d -s whale-shadow \
    "cd $SCRIPT_DIR && ${ACTIVATE}${ENV_LOAD}python3 monitor/whale_shadow.py 2>&1 | tee -a logs/shadow.log"

echo "Shadow monitor started in tmux session 'whale-shadow'"
echo "  Attach:  tmux attach -t whale-shadow"
echo "  Logs:    tail -f logs/shadow.log"
echo "  Stop:    ./deploy/stop_shadow.sh"
