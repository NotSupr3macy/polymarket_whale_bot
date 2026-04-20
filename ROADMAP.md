# Whale Bot — Future Work / Revisit Queue

Things worth doing **later**, once current-phase priorities are done.
Don't touch these until the trigger condition fires — premature addition
muddies the analysis of whatever we're currently working on.

---

## Paper trader profitability validation

**Trigger:** 5+ consecutive days of positive paper P&L under the
Apr 20 fingerprint-tuned filter config.

When hit, move on to:

### 1. Claude Plugin Marketplace for Polymarket
https://github.com/harish-garg/Claude-Plugin-Marketplace-for-Polymarket

4 Claude Code plugins (slash commands + AI assistants) with baked-in
Polymarket API knowledge:

- **polymarket-clob** — trading/orders, order book depth
- **polymarket-data** — positions/portfolios/leaderboards
- **polymarket-gamma** — market metadata/search
- **polymarket-websocket** — real-time streaming (SUB-SECOND whale detection)

Install with:
```bash
# In Claude Code:
# /plugin install polymarket-websocket
# /plugin install polymarket-clob
```

### 1a. WebSocket whale detection
**Priority:** HIGH once paper is profitable.
**Current state:** We poll `tracked_whale_positions` every 30s. A whale
signal can be up to 30s stale before paper_trader sees it — other copy
bots race us.

**With websockets:** Sub-100ms from whale's on-chain confirm to our tick.
Meaningful edge when multiple copy-traders compete for the same entry
price.

### 1b. Order-book depth check
**Priority:** MEDIUM.
**Current state:** Paper trader opens at whale's entry price even when
the live order book has $0 at that level (visible in live bot alerts as
"⚠️ Only $0k at whale's entry — thin"). This inflates paper results vs
what a retail bettor could actually execute.

**With polymarket-clob:** Before opening, check the book — if retail
can't actually fill at whale's price ±0.005, skip. Aligns paper with
reality.

### 1c. Live trading path
**Priority:** only after 2+ weeks of paper >+10% ROI.
Move from `paper_positions` to real CLOB order placement. Requires
signing key + real capital. polymarket-clob handles the order
lifecycle.

---

## Other future items

### Consensus meta-whale
**Trigger:** Portfolio analysis shows sportmaster + GIAYN + kch123
hitting same (cid, side) on 10+ markets with 70%+ combined WR.

Automatically size up to $10-$15 when 2+ whales agree (we have
consensus mult=1.5x/2.0x now, but it triggers within 30 min; could
extend for same-day consensus).

### Texaskid / TheOnlyHuman re-evaluation
**Trigger:** 7 days of shadow-log data (`grep "SHADOW \[texaskid\]" logs/paper_trader.log`).

Analyze shadow-log outcomes against what we actually filtered. If
texaskid is 10+W on dogs over 7 days, promote him back at $3 with
dog-only filter.

### Log rotation
Paper trader log has accumulated ~50K lines of historical SKIP noise
from iterating config. Add `logrotate` or `find -size +50M -delete`
cron. Low priority — disk space is not currently an issue.

### Whale correlation analysis
Build a script that computes pairwise whale outcome correlation. If
two whales always win/lose together, they're effectively one signal
(double-stake risk). If they're uncorrelated, diversification is real.

---

**Last updated:** 2026-04-20 — post fingerprint-rewire commit `e8e89a8`
