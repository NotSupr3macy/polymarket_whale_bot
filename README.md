# polymarket_whale_bot

An autonomous copy-trading bot that monitors top-performing wallets on Polymarket and replicates their positions with risk-adjusted sizing. Deployed to a DigitalOcean VPS and run persistently via tmux.

## What it does
Tracks a curated watchlist of high-conviction traders, detects new positions via the Polymarket API, and opens corresponding trades sized using the Kelly Criterion. Includes a Bayesian scoring system that updates each whale's weight based on realized PnL over time, so capital allocation shifts toward traders who actually win.

## Stack
Python · Polymarket CLOB API · DigitalOcean VPS · tmux for persistence · Telegram Bot API for live alerts

## Hard problems solved
- **Signal deduplication**: early versions stacked duplicate signals when a whale modified an existing position. Rewrote the position-tracking layer to diff against held state rather than firing on every event.
- **Stop-loss ordering**: stop-losses were occasionally placed before the entry filled on slow markets. Refactored the order pipeline to await fill confirmation before submitting protective orders.
- **Hold-to-resolution logic for sports markets**: sports outcomes resolve binary, so mid-market stop-losses destroyed expected value. Added a market-type classifier that routes sports positions to a hold-to-resolution branch.
- **Pagination + field mismatches**: Polymarket's API paginates inconsistently across endpoints; built a unified pagination wrapper to handle it.

## Status
Live, currently running on VPS. Whale list, position sizing parameters, and API keys are stripped from this public version.
