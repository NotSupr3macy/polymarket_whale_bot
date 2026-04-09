#!/bin/bash
# Stop the shadow monitor gracefully
if tmux has-session -t whale-shadow 2>/dev/null; then
    # Send SIGINT for graceful shutdown
    tmux send-keys -t whale-shadow C-c
    sleep 2
    # Kill session if still alive
    if tmux has-session -t whale-shadow 2>/dev/null; then
        tmux kill-session -t whale-shadow
    fi
    echo "Shadow monitor stopped."
else
    echo "Shadow monitor not running."
fi
