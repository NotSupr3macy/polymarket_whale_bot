"""
CLI entry point for the Polymarket whale copy-trading bot.

Usage:
  python cli.py                     # Dry run (default)
  python cli.py --live              # Live trading
  python cli.py --live --bankroll 500
  python cli.py --stats             # Show performance stats
  python cli.py --export            # Export trades to CSV
  python cli.py --refresh-whales    # Re-fetch leaderboard
  python cli.py --dashboard         # Show live monitoring dashboard
  python cli.py --analytics         # Full analytics + whale score update
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import sys
import time
from dataclasses import replace
from pathlib import Path

from config import BotConfig, WHALE_WATCHLIST


def setup_logging(verbose: bool = False) -> None:
    """Configure structured logging with timestamps and module names."""
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s"
    logging.basicConfig(
        level=level,
        format=fmt,
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("whale_bot.log", mode="a"),
        ],
    )
    # Quiet noisy libraries
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("web3").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Polymarket Whale Copy-Trading Bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cli.py                     Dry run with $1000 default bankroll
  python cli.py --live --bankroll 500   Live trading with $500
  python cli.py --stats             Show performance statistics
  python cli.py --export            Export trade history to CSV
  python cli.py --analytics         Full performance analytics report
        """,
    )

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--live",
        action="store_true",
        help="Enable live trading (default is dry run)",
    )
    mode.add_argument(
        "--stats",
        action="store_true",
        help="Show performance statistics and exit",
    )
    mode.add_argument(
        "--export",
        action="store_true",
        help="Export trade history to CSV and exit",
    )
    mode.add_argument(
        "--refresh-whales",
        action="store_true",
        help="Fetch current leaderboard and update whale watchlist",
    )
    mode.add_argument(
        "--dashboard",
        action="store_true",
        help="Show whale monitoring dashboard",
    )
    mode.add_argument(
        "--analytics",
        action="store_true",
        help="Full performance analytics with whale scoring and failure analysis",
    )
    mode.add_argument(
        "--status",
        action="store_true",
        help="Quick health check: open positions, today's PnL, last trade",
    )

    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clear stale open positions from previous sessions before starting",
    )
    parser.add_argument(
        "--bankroll",
        type=float,
        default=None,
        help="Starting bankroll in USD (default: from .env or $1000)",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=None,
        help="Path to SQLite database (default: trades.db)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug logging",
    )

    return parser


def show_stats(config: BotConfig, dry_run: bool = True) -> None:
    """Display performance statistics from the trade journal."""
    from trade_journal import TradeJournal

    journal = TradeJournal(config.DB_PATH, dry_run=dry_run)
    journal.initialize()

    stats = journal.get_performance_stats()
    daily = journal.get_daily_summary()

    try:
        from rich.console import Console
        from rich.table import Table
        from rich.panel import Panel

        console = Console()

        # Overall stats
        table = Table(title="Performance Statistics", show_header=False)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("Total trades", str(stats["total_trades"]))
        if stats["total_trades"] > 0:
            table.add_row("Wins / Losses", f"{stats.get('wins', 0)} / {stats.get('losses', 0)}")
            table.add_row("Win rate", f"{stats['win_rate']:.1%}")
            table.add_row("Total PnL", f"${stats['total_pnl']:.2f}")
            table.add_row("Avg PnL/trade", f"${stats['avg_pnl']:.2f}")
            table.add_row("Best trade", f"${stats['best_trade']:.2f}")
            table.add_row("Worst trade", f"${stats['worst_trade']:.2f}")
            table.add_row("Avg win", f"${stats.get('avg_win', 0):.2f}")
            table.add_row("Avg loss", f"${stats.get('avg_loss', 0):.2f}")
            pf = stats.get('profit_factor', 0)
            table.add_row("Profit factor", f"{pf:.2f}" if pf != float("inf") else "INF")
        table.add_row("Open positions", str(stats["open_positions"]))

        console.print(table)

        # Daily summary
        if daily and daily.get("trades"):
            console.print(f"\n[bold]Today:[/bold] {daily['trades']} trades, "
                         f"PnL=${daily.get('total_pnl', 0):.2f}")

        # Exit reasons breakdown
        if stats.get("by_exit_reason"):
            console.print("\n[bold]Exit reasons:[/bold]")
            for reason, count in stats["by_exit_reason"].items():
                console.print(f"  {reason}: {count}")

    except ImportError:
        # Fallback without rich
        print("\n=== Performance Statistics ===")
        print(f"Total trades:    {stats['total_trades']}")
        if stats["total_trades"] > 0:
            print(f"Win rate:        {stats['win_rate']:.1%}")
            print(f"Total PnL:       ${stats['total_pnl']:.2f}")
            print(f"Best trade:      ${stats['best_trade']:.2f}")
            print(f"Worst trade:     ${stats['worst_trade']:.2f}")
        print(f"Open positions:  {stats['open_positions']}")

    journal.close()


def export_trades(config: BotConfig) -> None:
    """Export trade history to CSV."""
    from trade_journal import TradeJournal

    journal = TradeJournal(config.DB_PATH)
    journal.initialize()
    filepath = journal.export_csv()
    print(f"Trades exported to: {filepath}")
    journal.close()


def show_dashboard(config: BotConfig) -> None:
    """Show whale watchlist dashboard."""
    try:
        from rich.console import Console
        from rich.table import Table

        console = Console()

        table = Table(title="Whale Watchlist")
        table.add_column("Tier", style="bold")
        table.add_column("Alias", style="cyan")
        table.add_column("Category")
        table.add_column("Address")
        table.add_column("Key Stats", style="green")

        for addr, info in sorted(WHALE_WATCHLIST.items(), key=lambda x: x[1]["tier"]):
            stats = info.get("verified_stats", {})
            pnl = stats.get("all_time_pnl") or stats.get("monthly_pnl") or stats.get("monthly_pnl_march_2026", 0)
            wr = stats.get("win_rate", "")
            stat_str = f"PnL: ${pnl:,.0f}" if pnl else ""
            if wr:
                stat_str += f" | WR: {wr:.0%}"

            tier_color = {1: "red", 2: "yellow", 3: "white"}.get(info["tier"], "white")
            table.add_row(
                f"[{tier_color}]{info['tier']}[/{tier_color}]",
                info["alias"],
                info.get("category", ""),
                f"{addr[:8]}...{addr[-6:]}",
                stat_str,
            )

        console.print(table)
        console.print(f"\nTotal: {len(WHALE_WATCHLIST)} wallets")

    except ImportError:
        print("\n=== Whale Watchlist ===")
        for addr, info in sorted(WHALE_WATCHLIST.items(), key=lambda x: x[1]["tier"]):
            print(f"  T{info['tier']} | {info['alias']:20s} | {addr[:10]}...")


async def refresh_whales(config: BotConfig) -> None:
    """Fetch current leaderboard data and display for manual review."""
    import aiohttp

    print("Fetching current Polymarket leaderboard...")

    try:
        async with aiohttp.ClientSession() as session:
            # Fetch top earners from the leaderboard API
            url = f"{config.DATA_API}/v1/leaderboard"
            params = {"period": "month", "limit": "25"}
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    print(f"\nTop {len(data)} monthly earners:")
                    for i, entry in enumerate(data[:25], 1):
                        addr = entry.get("address", "?")
                        pnl = entry.get("pnl", 0)
                        volume = entry.get("volume", 0)
                        print(f"  {i:2d}. {addr[:10]}... | PnL: ${pnl:,.0f} | Vol: ${volume:,.0f}")
                else:
                    print(f"Leaderboard API returned {resp.status}")
                    print("Manual update: visit polymarket.com/leaderboard and update config.py")

    except Exception as e:
        print(f"Failed to fetch leaderboard: {e}")
        print("Manual update: visit polymarket.com/leaderboard and update config.py")


# ══════════════════════════════════════════════════════════════════
#  UPGRADE 5: PERFORMANCE ANALYTICS + WHALE SCORE UPDATES
# ══════════════════════════════════════════════════════════════════

BAYESIAN_PRIOR_WEIGHT = 50  # Trades worth of prior belief before blending

def show_analytics(config: BotConfig) -> None:
    """
    Comprehensive performance analytics with:
      1. Sharpe ratio and max drawdown
      2. Per-whale performance breakdown
      3. Per-consensus-level performance
      4. Failure reason breakdown
      5. Automatic whale win rate blending (Bayesian update)
    """
    from trade_journal import TradeJournal
    from risk_manager import WHALE_WIN_RATES, DEFAULT_WIN_RATE

    # Run for both dry-run and live
    for mode_label, dry_run in [("DRY RUN", True), ("LIVE", False)]:
        journal = TradeJournal(config.DB_PATH, dry_run=dry_run)
        journal.initialize()

        trades = journal.get_all_closed_trades()
        if not trades:
            print(f"\n=== {mode_label} Analytics: No closed trades ===")
            journal.close()
            continue

        print(f"\n{'='*70}")
        print(f"  {mode_label} ANALYTICS — {len(trades)} closed trades")
        print(f"{'='*70}")

        # ── 1. Overall metrics ──
        pnls = [t.get("pnl", 0) or 0.0 for t in trades]
        wins = sum(1 for p in pnls if p > 0)
        losses = sum(1 for p in pnls if p < 0)
        total_pnl = sum(pnls)
        avg_pnl = total_pnl / len(pnls) if pnls else 0

        # Sharpe ratio (annualized, assuming daily PnL)
        sharpe = _calculate_sharpe(pnls)

        # Max drawdown from PnL curve
        max_dd, max_dd_pct = _calculate_max_drawdown(pnls, config.INITIAL_BANKROLL)

        print(f"\n  Total PnL:        ${total_pnl:,.2f}")
        print(f"  Win rate:         {wins}/{len(pnls)} ({wins/len(pnls):.1%})")
        print(f"  Avg PnL/trade:    ${avg_pnl:.2f}")
        print(f"  Sharpe ratio:     {sharpe:.2f}")
        print(f"  Max drawdown:     ${max_dd:.2f} ({max_dd_pct:.1%})")

        try:
            from rich.console import Console
            from rich.table import Table
            console = Console()
            _show_analytics_rich(console, trades, config)
        except ImportError:
            _show_analytics_plain(trades, config)

        # ── 5. Whale score updates (Bayesian blending) ──
        whale_outcomes = journal.get_whale_outcomes()
        if whale_outcomes:
            print(f"\n  --- Whale Score Updates (Bayesian, prior weight={BAYESIAN_PRIOR_WEIGHT}) ---")
            _update_whale_scores(whale_outcomes)

        journal.close()


def _calculate_sharpe(pnls: list[float], risk_free_rate: float = 0.0) -> float:
    """
    Calculate Sharpe ratio from a list of PnLs.
    Annualized assuming ~365 trading days (crypto markets).
    """
    if len(pnls) < 2:
        return 0.0

    avg = sum(pnls) / len(pnls)
    variance = sum((p - avg) ** 2 for p in pnls) / (len(pnls) - 1)
    std = math.sqrt(variance) if variance > 0 else 0.001

    daily_sharpe = (avg - risk_free_rate) / std
    annualized = daily_sharpe * math.sqrt(365)
    return annualized


def _calculate_max_drawdown(pnls: list[float], initial_bankroll: float) -> tuple[float, float]:
    """
    Calculate max drawdown from PnL series.
    Returns (max_drawdown_dollars, max_drawdown_pct).
    """
    if not pnls:
        return 0.0, 0.0

    equity = initial_bankroll
    peak = equity
    max_dd = 0.0

    for pnl in pnls:
        equity += pnl
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    max_dd_pct = max_dd / peak if peak > 0 else 0.0
    return max_dd, max_dd_pct


def _show_analytics_rich(console, trades: list[dict], config: BotConfig) -> None:
    """Rich-formatted analytics tables."""
    from rich.table import Table

    # ── 2. Per-whale performance ──
    whale_stats = _build_whale_stats(trades)
    if whale_stats:
        table = Table(title="Per-Whale Performance")
        table.add_column("Whale", style="cyan")
        table.add_column("Trades", justify="right")
        table.add_column("Wins", justify="right", style="green")
        table.add_column("Losses", justify="right", style="red")
        table.add_column("Win Rate", justify="right")
        table.add_column("PnL", justify="right")
        table.add_column("Avg PnL", justify="right")

        for alias, s in sorted(whale_stats.items(), key=lambda x: x[1]["pnl"], reverse=True):
            wr = s["wins"] / s["trades"] if s["trades"] > 0 else 0
            pnl_color = "green" if s["pnl"] >= 0 else "red"
            table.add_row(
                alias,
                str(s["trades"]),
                str(s["wins"]),
                str(s["losses"]),
                f"{wr:.0%}",
                f"[{pnl_color}]${s['pnl']:.2f}[/{pnl_color}]",
                f"${s['pnl']/s['trades']:.2f}" if s["trades"] > 0 else "$0.00",
            )

        console.print(table)

    # ── 3. Per-consensus-level performance ──
    consensus_stats = _build_consensus_stats(trades)
    if consensus_stats:
        table = Table(title="Per-Consensus-Level Performance")
        table.add_column("Level", style="cyan")
        table.add_column("Trades", justify="right")
        table.add_column("Win Rate", justify="right")
        table.add_column("PnL", justify="right")
        table.add_column("Avg PnL", justify="right")

        for level, s in sorted(consensus_stats.items(), key=lambda x: x[1]["pnl"], reverse=True):
            wr = s["wins"] / s["trades"] if s["trades"] > 0 else 0
            pnl_color = "green" if s["pnl"] >= 0 else "red"
            table.add_row(
                level or "unknown",
                str(s["trades"]),
                f"{wr:.0%}",
                f"[{pnl_color}]${s['pnl']:.2f}[/{pnl_color}]",
                f"${s['pnl']/s['trades']:.2f}" if s["trades"] > 0 else "$0.00",
            )

        console.print(table)

    # ── 4. Failure reason breakdown ──
    failure_stats = _build_failure_stats(trades)
    if failure_stats:
        table = Table(title="Failure Classification")
        table.add_column("Reason", style="cyan")
        table.add_column("Count", justify="right")
        table.add_column("Total Loss", justify="right", style="red")
        table.add_column("Avg Loss", justify="right")

        for reason, s in sorted(failure_stats.items(), key=lambda x: x[1]["total_loss"]):
            table.add_row(
                reason,
                str(s["count"]),
                f"${s['total_loss']:.2f}",
                f"${s['total_loss']/s['count']:.2f}" if s["count"] > 0 else "$0.00",
            )

        console.print(table)


def _show_analytics_plain(trades: list[dict], config: BotConfig) -> None:
    """Plain text analytics (no rich library)."""
    # Per-whale
    whale_stats = _build_whale_stats(trades)
    if whale_stats:
        print("\n  --- Per-Whale Performance ---")
        for alias, s in sorted(whale_stats.items(), key=lambda x: x[1]["pnl"], reverse=True):
            wr = s["wins"] / s["trades"] if s["trades"] > 0 else 0
            print(f"    {alias:20s} | {s['trades']:3d} trades | WR {wr:.0%} | PnL ${s['pnl']:.2f}")

    # Per-consensus
    consensus_stats = _build_consensus_stats(trades)
    if consensus_stats:
        print("\n  --- Per-Consensus-Level ---")
        for level, s in sorted(consensus_stats.items(), key=lambda x: x[1]["pnl"], reverse=True):
            wr = s["wins"] / s["trades"] if s["trades"] > 0 else 0
            print(f"    {(level or 'unknown'):20s} | {s['trades']:3d} trades | WR {wr:.0%} | PnL ${s['pnl']:.2f}")

    # Failures
    failure_stats = _build_failure_stats(trades)
    if failure_stats:
        print("\n  --- Failure Reasons ---")
        for reason, s in sorted(failure_stats.items(), key=lambda x: x[1]["total_loss"]):
            print(f"    {reason:25s} | {s['count']:3d} | Loss: ${s['total_loss']:.2f}")


def _build_whale_stats(trades: list[dict]) -> dict[str, dict]:
    """Aggregate performance by whale alias."""
    stats: dict[str, dict] = {}
    for t in trades:
        aliases_raw = t.get("whale_signals", "[]")
        try:
            aliases = json.loads(aliases_raw)
        except (json.JSONDecodeError, TypeError):
            aliases = []

        pnl = t.get("pnl", 0.0) or 0.0
        is_win = pnl > 0

        for alias in aliases:
            if alias not in stats:
                stats[alias] = {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0}
            stats[alias]["trades"] += 1
            stats[alias]["pnl"] += pnl
            if is_win:
                stats[alias]["wins"] += 1
            elif pnl < 0:
                stats[alias]["losses"] += 1

    return stats


def _build_consensus_stats(trades: list[dict]) -> dict[str, dict]:
    """Aggregate performance by consensus level."""
    stats: dict[str, dict] = {}
    for t in trades:
        level = t.get("consensus_level", "") or "unknown"
        pnl = t.get("pnl", 0.0) or 0.0

        if level not in stats:
            stats[level] = {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0}
        stats[level]["trades"] += 1
        stats[level]["pnl"] += pnl
        if pnl > 0:
            stats[level]["wins"] += 1
        elif pnl < 0:
            stats[level]["losses"] += 1

    return stats


def _build_failure_stats(trades: list[dict]) -> dict[str, dict]:
    """Aggregate failure reasons for losing trades."""
    stats: dict[str, dict] = {}
    for t in trades:
        reason = t.get("failure_reason", "")
        if not reason:
            continue

        if reason not in stats:
            stats[reason] = {"count": 0, "total_loss": 0.0}
        stats[reason]["count"] += 1
        stats[reason]["total_loss"] += t.get("pnl", 0.0) or 0.0

    return stats


def _update_whale_scores(whale_outcomes: dict[str, dict]) -> None:
    """
    Bayesian blending of observed whale win rates with prior (WHALE_WIN_RATES).

    Formula:
      blended_rate = (prior_rate * PRIOR_WEIGHT + observed_wins) / (PRIOR_WEIGHT + observed_trades)

    This "shrinks" the observed rate toward the prior — useful when we have
    few observations from our own trading. With PRIOR_WEIGHT=50, we need ~50
    trades before the observed rate dominates.

    Prints suggested updates; does NOT modify risk_manager.py automatically.
    """
    from risk_manager import WHALE_WIN_RATES, DEFAULT_WIN_RATE

    for alias, data in sorted(whale_outcomes.items(), key=lambda x: x[1]["trades"], reverse=True):
        trades = data["trades"]
        wins = data["wins"]

        if trades == 0:
            continue

        # Find the prior rate for this whale
        prior_rate = DEFAULT_WIN_RATE
        for wallet, info in WHALE_WATCHLIST.items():
            if info["alias"] == alias:
                prior_rate = WHALE_WIN_RATES.get(wallet, DEFAULT_WIN_RATE)
                break

        observed_rate = wins / trades if trades > 0 else 0.0

        # Bayesian blending
        blended_rate = (prior_rate * BAYESIAN_PRIOR_WEIGHT + wins) / (BAYESIAN_PRIOR_WEIGHT + trades)

        # Direction indicator
        if blended_rate > prior_rate + 0.02:
            direction = "UP"
        elif blended_rate < prior_rate - 0.02:
            direction = "DOWN"
        else:
            direction = "="

        print(
            f"    {alias:20s} | {trades:3d} trades | "
            f"Observed: {observed_rate:.0%} | Prior: {prior_rate:.0%} | "
            f"Blended: {blended_rate:.0%} {direction}"
        )


def show_status(config: BotConfig) -> None:
    """Quick health check: open positions, today's PnL, last trade time."""
    from trade_journal import TradeJournal
    import os

    for mode_label, dry_run in [("DRY RUN", True), ("LIVE", False)]:
        journal = TradeJournal(config.DB_PATH, dry_run=dry_run)
        journal.initialize()

        open_positions = journal.get_open_positions()
        daily = journal.get_daily_summary()
        closed = journal.get_closed_trades(limit=1)

        last_trade_time = closed[0].get("exit_time", "?") if closed else "none"

        # Check if bot process is running (tmux session exists)
        import subprocess
        try:
            bot_running = subprocess.run(
                ["tmux", "has-session", "-t", "whale-bot"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            ).returncode == 0
        except FileNotFoundError:
            bot_running = False  # tmux not installed (e.g. Windows)

        print(f"\n=== {mode_label} STATUS ===")
        print(f"  Bot process:     {'RUNNING' if bot_running else 'STOPPED'}")
        print(f"  Open positions:  {len(open_positions)}")
        if open_positions:
            total_exposure = sum(p.get("position_size", 0) for p in open_positions)
            print(f"  Total exposure:  ${total_exposure:,.2f}")
            for p in open_positions:
                direction = p.get("direction", "?")
                size = p.get("position_size", 0)
                entry = p.get("entry_price", 0)
                title = p.get("market_title", "") or p.get("market_id", "?")[:30]
                print(f"    {direction:4s} ${size:,.2f} @ {entry:.3f} | {title[:40]}")

        today_trades = daily.get("trades", 0) if daily else 0
        today_pnl = daily.get("total_pnl", 0) or 0 if daily else 0
        print(f"  Today:           {today_trades} trades, PnL ${today_pnl:,.2f}")
        print(f"  Last trade:      {last_trade_time}")

        journal.close()


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    setup_logging(args.verbose)

    # Build config with CLI overrides
    overrides = {}
    if args.live:
        overrides["DRY_RUN"] = False
    if args.bankroll is not None:
        overrides["INITIAL_BANKROLL"] = args.bankroll
    if args.db is not None:
        overrides["DB_PATH"] = args.db

    config = BotConfig() if not overrides else replace(BotConfig(), **overrides)

    # Validate config
    errors = config.validate()
    if errors:
        for err in errors:
            print(f"Config error: {err}")
        sys.exit(1)

    # Handle command modes
    if args.stats:
        print("=== DRY RUN Stats ===")
        show_stats(config, dry_run=True)
        print("\n=== LIVE Stats ===")
        show_stats(config, dry_run=False)
        return

    if args.export:
        export_trades(config)
        return

    if args.dashboard:
        show_dashboard(config)
        return

    if args.refresh_whales:
        asyncio.run(refresh_whales(config))
        return

    if args.analytics:
        show_analytics(config)
        return

    if args.status:
        show_status(config)
        return

    # Clean stale positions if requested
    if args.clean:
        from trade_journal import TradeJournal
        # Clean both live and dry-run tables
        for dry in (False, True):
            journal = TradeJournal(config.DB_PATH, dry_run=dry)
            journal.initialize()
            count = journal.clean_open_positions()
            label = "dry-run" if dry else "live"
            if count > 0:
                print(f"Cleaned {count} stale open positions from {label} journal")
            journal.close()

    # Run the bot
    from bot import WhaleBot

    bot = WhaleBot(config)

    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\nInterrupted by user")


if __name__ == "__main__":
    main()
