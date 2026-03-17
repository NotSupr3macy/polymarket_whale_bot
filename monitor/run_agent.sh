#!/bin/bash
# ============================================================================
# Daily Monitoring Agent — runs via cron
# Collects data -> builds prompt -> runs Claude Code -> applies Tier 1 fixes
#
# Cron setup (run twice daily — after overnight + afternoon resolutions):
#   0 6 * * * cd /home/botuser/whale-bot && bash monitor/run_agent.sh
#   0 18 * * * cd /home/botuser/whale-bot && bash monitor/run_agent.sh
# ============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SCRIPT_DIR"

# Activate venv
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
fi

# Load env vars (skip comments and blank lines)
if [ -f ".env" ]; then
    export $(grep -v '^#' .env | grep -v '^$' | xargs)
fi

TIMESTAMP=$(date +%Y%m%d_%H%M)
LOG="monitor/logs/agent_${TIMESTAMP}.log"
OUTPUT_FILE="monitor/reports/agent_output_${TIMESTAMP}.json"
mkdir -p monitor/logs monitor/pending monitor/reports

echo "$(date): Starting monitoring agent" >> "$LOG"

# ── Step 1: Collect data ──────────────────────────────────────────
echo "$(date): Collecting data..." >> "$LOG"
python3 monitor/collect_data.py 2>> "$LOG"
echo "$(date): Report generated" >> "$LOG"

# ── Step 2: Build prompt ─────────────────────────────────────────
PROMPT=$(python3 monitor/agent_prompt.py monitor/reports/latest.json 2>> "$LOG")

# ── Step 3: Run Claude Code with Tier 1 permissions ──────────────
# Grant Bash + Write access so the agent can do Tier 1 auto-fixes:
#   - sqlite3 queries/updates on trades.db
#   - mkdir, file writes to monitor/pending/
#   - log file cleanup
# The tiered prompt constrains WHAT it does; these flags let it DO things.
echo "$(date): Running Claude Code analysis..." >> "$LOG"
echo "$PROMPT" | claude -p \
    --output-format json \
    --allowedTools "Bash(sqlite3:*)" "Bash(mkdir:*)" "Bash(rm:monitor/*)" "Bash(ls:*)" "Bash(find:monitor/*)" "Bash(cat:*)" \
    "Write(monitor/pending/*)" "Write(monitor/reports/*)" \
    > "$OUTPUT_FILE" 2>> "$LOG" \
    || echo '{"type":"result","result":"claude command failed"}' > "$OUTPUT_FILE"

echo "$(date): Claude Code output saved to $OUTPUT_FILE" >> "$LOG"

# ── Step 4: Extract Telegram summary and send ────────────────────
# Uses standalone parser to avoid bash string escaping issues
TELEGRAM_MSG=$(python3 monitor/parse_output.py "$OUTPUT_FILE" --telegram 2>> "$LOG")

if [ -n "$TELEGRAM_BOT_TOKEN" ] && [ -n "$TELEGRAM_CHAT_ID" ]; then
    # Send without parse_mode to avoid HTML/markdown breaking on special chars
    curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
        --data-urlencode "chat_id=${TELEGRAM_CHAT_ID}" \
        --data-urlencode "text=🤖 DAILY AGENT REPORT
${TELEGRAM_MSG}" \
        > /dev/null 2>> "$LOG"
    echo "$(date): Telegram alert sent" >> "$LOG"
fi

# ── Step 5: Check if Tier 1 actions require a bot restart ────────
NEEDS_RESTART=$(python3 monitor/parse_output.py "$OUTPUT_FILE" --needs-restart 2>> "$LOG")

if [ "$NEEDS_RESTART" = "yes" ]; then
    echo "$(date): Tier 1 changes require bot restart" >> "$LOG"
    bash deploy/stop.sh >> "$LOG" 2>&1
    sleep 3
    bash deploy/start.sh >> "$LOG" 2>&1
    echo "$(date): Bot restarted" >> "$LOG"
fi

# ── Step 6: Check for pending Tier 2 proposals ───────────────────
PENDING_COUNT=$(ls -1 monitor/pending/*.py monitor/pending/*.patch monitor/pending/*.md 2>/dev/null | wc -l || echo 0)
if [ "$PENDING_COUNT" -gt "0" ]; then
    echo "$(date): $PENDING_COUNT Tier 2 proposals pending review in monitor/pending/" >> "$LOG"
fi

# ── Step 7: Clean old reports (keep last 30 days) ────────────────
find monitor/reports -name "report_*.json" -mtime +30 -delete 2>/dev/null || true
find monitor/reports -name "agent_output_*.json" -mtime +30 -delete 2>/dev/null || true
find monitor/logs -name "agent_*.log" -mtime +30 -delete 2>/dev/null || true

echo "$(date): Agent cycle complete" >> "$LOG"
