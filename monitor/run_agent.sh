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

LOG="monitor/logs/agent_$(date +%Y%m%d_%H%M).log"
mkdir -p monitor/logs monitor/pending monitor/reports

echo "$(date): Starting monitoring agent" >> "$LOG"

# ── Step 1: Collect data ──────────────────────────────────────────
echo "$(date): Collecting data..." >> "$LOG"
REPORT_PATH=$(python3 monitor/collect_data.py 2>> "$LOG")
echo "$(date): Report generated" >> "$LOG"

# ── Step 2: Build prompt ─────────────────────────────────────────
PROMPT=$(python3 monitor/agent_prompt.py monitor/reports/latest.json 2>> "$LOG")

# ── Step 3: Run Claude Code ──────────────────────────────────────
echo "$(date): Running Claude Code analysis..." >> "$LOG"
CLAUDE_OUTPUT=$(echo "$PROMPT" | claude -p --output-format json 2>> "$LOG" || echo '{"error": "claude command failed"}')

# Save full output for audit trail
echo "$CLAUDE_OUTPUT" > "monitor/reports/agent_output_$(date +%Y%m%d_%H%M).json"
echo "$(date): Claude Code output saved" >> "$LOG"

# ── Step 4: Extract Telegram summary and send ────────────────────
# claude --output-format json wraps output in {"type":"result","result":"..."}
# The inner result text contains a ```json ... ``` block with the actual report.
TELEGRAM_MSG=$(echo "$CLAUDE_OUTPUT" | python3 -c "
import sys, json, re
try:
    wrapper = json.load(sys.stdin)

    # Extract inner result text from Claude's JSON envelope
    inner_text = wrapper.get('result', '') if isinstance(wrapper, dict) else ''

    # Try to find a JSON block in the inner text (```json ... ```)
    match = re.search(r'\`\`\`json\s*\n(.*?)\n\`\`\`', inner_text, re.DOTALL)
    if match:
        data = json.loads(match.group(1))
    else:
        # Maybe the result IS the JSON directly
        data = json.loads(inner_text) if inner_text else wrapper

    msg = data.get('telegram_summary', 'Agent ran but no summary generated.')

    # Add critical alerts
    alerts = data.get('alerts', [])
    critical = [a for a in alerts if a.get('severity') == 'critical']
    if critical:
        msg += '\n\n🚨 CRITICAL ALERTS:'
        for a in critical:
            msg += f\"\n- {a['message']}\"

    # Add pending proposals
    proposals = data.get('tier2_proposals', [])
    if proposals:
        msg += f'\n\n📋 {len(proposals)} change(s) pending your review'
        for p in proposals:
            msg += f\"\n- {p['proposal']}\"

    # Add tier 1 actions taken
    actions = data.get('tier1_actions', [])
    if actions:
        msg += f'\n\n🔧 {len(actions)} auto-fix(es) applied'
        for a in actions:
            msg += f\"\n- {a['action']}: {a['status']}\"

    print(msg)
except Exception as e:
    print(f'Agent report parse error: {e}')
" 2>> "$LOG")

# Send to Telegram
if [ -n "$TELEGRAM_BOT_TOKEN" ] && [ -n "$TELEGRAM_CHAT_ID" ]; then
    curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
        -d chat_id="$TELEGRAM_CHAT_ID" \
        -d text="🤖 DAILY AGENT REPORT
${TELEGRAM_MSG}" \
        > /dev/null 2>> "$LOG"
    echo "$(date): Telegram alert sent" >> "$LOG"
fi

# ── Step 5: Check if Tier 1 actions require a bot restart ────────
NEEDS_RESTART=$(echo "$CLAUDE_OUTPUT" | python3 -c "
import sys, json, re
try:
    wrapper = json.load(sys.stdin)
    inner_text = wrapper.get('result', '') if isinstance(wrapper, dict) else ''
    match = re.search(r'\`\`\`json\s*\n(.*?)\n\`\`\`', inner_text, re.DOTALL)
    if match:
        data = json.loads(match.group(1))
    else:
        data = json.loads(inner_text) if inner_text else wrapper
    actions = data.get('tier1_actions', [])
    for a in actions:
        if 'resolve' in a.get('action', '').lower() and a.get('status') == 'done':
            print('yes')
            sys.exit(0)
    print('no')
except Exception:
    print('no')
" 2>> "$LOG")

if [ "$NEEDS_RESTART" = "yes" ]; then
    echo "$(date): Tier 1 changes require bot restart" >> "$LOG"
    bash deploy/stop.sh >> "$LOG" 2>&1
    sleep 3
    bash deploy/start.sh >> "$LOG" 2>&1
    echo "$(date): Bot restarted" >> "$LOG"
fi

# ── Step 6: Check for pending Tier 2 proposals ───────────────────
PENDING_COUNT=$(ls -1 monitor/pending/*.py monitor/pending/*.patch 2>/dev/null | wc -l || echo 0)
if [ "$PENDING_COUNT" -gt "0" ]; then
    echo "$(date): $PENDING_COUNT Tier 2 proposals pending review in monitor/pending/" >> "$LOG"
fi

# ── Step 7: Clean old reports (keep last 30 days) ────────────────
find monitor/reports -name "report_*.json" -mtime +30 -delete 2>/dev/null || true
find monitor/reports -name "agent_output_*.json" -mtime +30 -delete 2>/dev/null || true
find monitor/logs -name "agent_*.log" -mtime +30 -delete 2>/dev/null || true

echo "$(date): Agent cycle complete" >> "$LOG"
