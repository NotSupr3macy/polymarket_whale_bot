"""
Parse Claude Code agent output and extract the Telegram summary.

Claude CLI --output-format json wraps responses in:
  {"type":"result", "result":"...text with ```json {...} ``` block..."}

This script extracts the inner JSON and returns the telegram_summary field.

Usage:
  python3 monitor/parse_output.py <agent_output_file> [--telegram | --needs-restart]
"""

import json
import re
import sys


def extract_report(filepath: str) -> dict:
    """Extract the structured report JSON from Claude's output wrapper."""
    with open(filepath) as f:
        wrapper = json.load(f)

    # Get the inner result text
    inner_text = wrapper.get("result", "") if isinstance(wrapper, dict) else ""

    if not inner_text:
        return {}

    # Try to find a ```json ... ``` block in the text
    # Be forgiving: allow optional whitespace around backticks
    match = re.search(r"```json\s*\n(.*?)\n\s*```", inner_text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass

    # Try without the json language tag
    match = re.search(r"```\s*\n(\{.*?\})\n\s*```", inner_text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass

    # Try to find raw JSON object in the text (no code fence)
    match = re.search(r'(\{"tier1_actions".*\})', inner_text, re.DOTALL)
    if match:
        # Find the matching closing brace
        text = match.group(1)
        depth = 0
        for i, ch in enumerate(text):
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(text[: i + 1])
                    except json.JSONDecodeError:
                        break

    # Fallback: maybe the result IS JSON directly
    try:
        data = json.loads(inner_text)
        if isinstance(data, dict):
            return data
    except json.JSONDecodeError:
        pass

    return {}


def build_telegram_message(data: dict) -> str:
    """Build a Telegram-safe message from the report data."""
    if not data:
        return "Agent ran but output could not be parsed. Check monitor/reports/"

    msg = data.get("telegram_summary", "")
    if not msg:
        return "Agent ran but no summary was generated. Check monitor/reports/"

    # Telegram has a 4096 char limit — truncate if needed
    # Strip markdown bold (*text*) since we send without parse_mode
    msg = msg.replace("*", "")

    if len(msg) > 3800:
        msg = msg[:3800] + "\n\n... (truncated)"

    return msg


def check_needs_restart(data: dict) -> bool:
    """Check if any Tier 1 action requires a bot restart."""
    actions = data.get("tier1_actions", [])
    for a in actions:
        if "resolve" in a.get("action", "").lower() and a.get("status") == "done":
            return True
    return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 monitor/parse_output.py <agent_output.json> [--telegram | --needs-restart]")
        sys.exit(1)

    filepath = sys.argv[1]
    mode = sys.argv[2] if len(sys.argv) > 2 else "--telegram"

    try:
        data = extract_report(filepath)

        if mode == "--telegram":
            print(build_telegram_message(data))
        elif mode == "--needs-restart":
            print("yes" if check_needs_restart(data) else "no")
        else:
            # Dump the extracted report
            print(json.dumps(data, indent=2))
    except Exception as e:
        if mode == "--telegram":
            print(f"Agent parse error: {e}")
        elif mode == "--needs-restart":
            print("no")
        else:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
