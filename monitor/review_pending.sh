#!/bin/bash
# ============================================================================
# Review pending Tier 2 changes proposed by the monitoring agent.
#
# Usage: bash monitor/review_pending.sh
# ============================================================================

PENDING_DIR="monitor/pending"

if [ ! -d "$PENDING_DIR" ] || [ -z "$(ls -A "$PENDING_DIR" 2>/dev/null)" ]; then
    echo "✅ No pending proposals."
    exit 0
fi

echo "========================================"
echo "  PENDING TIER 2 PROPOSALS"
echo "========================================"
echo ""

# Show summary if it exists
if [ -f "$PENDING_DIR/summary.txt" ]; then
    echo "── Summary ──────────────────────────"
    cat "$PENDING_DIR/summary.txt"
    echo ""
    echo "─────────────────────────────────────"
    echo ""
fi

echo "Files:"
ls -la "$PENDING_DIR/"
echo ""
echo "Options:"
echo "  1. Review a file:   cat monitor/pending/<filename>"
echo "  2. Apply a change:  cp monitor/pending/<file> <destination>"
echo "                      bash deploy/stop.sh && bash deploy/start.sh"
echo "  3. Reject all:      rm monitor/pending/*"
echo ""
echo "After reviewing, restart the bot if you applied changes:"
echo "  bash deploy/stop.sh && bash deploy/start.sh"
