"""
EV cashout engine main loop.

Continuously:
  1. Load open Texaskid positions from trades.db
  2. Enrich each with cashout quote + live game state
  3. Run the decision engine
  4. For any "cashout" decision, fire a Telegram alert (with dedupe)

Usage:
    python -m ev_engine.main                 # run forever at 60s interval
    python -m ev_engine.main --once          # single pass then exit
    python -m ev_engine.main --interval 30   # custom interval
    python -m ev_engine.main --dry-run       # never actually send Telegram alerts

The loop reads env vars from the same .env as the whale bot (shares
TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, etc.).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

from .alerter import Alerter
from .decision_engine import DecisionEngine
from .position_manager import PositionManager


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | ev | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# Load .env from repo root
ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")


class EVEngine:
    def __init__(self, dry_run: bool = False) -> None:
        self.dry_run = dry_run
        self.decision = DecisionEngine()
        self.alerter = Alerter()
        self._stop = asyncio.Event()

    async def run_once(self) -> None:
        async with aiohttp.ClientSession() as session:
            pm = PositionManager(session)
            positions = pm.load_open_positions()
            if not positions:
                logger.info("No open positions.")
                return

            logger.info("Evaluating %d open positions...", len(positions))
            await pm.enrich_all(positions)

            for pos in positions:
                decision = self.decision.evaluate(pos)
                tag = f"[{pos.condition_id[:8]}] {pos.market_title[:48]}"
                if decision.action == "cashout":
                    logger.warning(
                        "CASHOUT %s dir=%s %s",
                        tag, pos.direction, decision.reason,
                    )
                    if not self.dry_run:
                        await self.alerter.maybe_alert(pos, decision)
                elif decision.action == "hold":
                    logger.info("HOLD    %s %s", tag, decision.reason)
                else:
                    logger.info("SKIP    %s %s", tag, decision.reason)

    async def run_forever(self, interval_sec: int) -> None:
        logger.info(
            "EV engine starting: interval=%ds dry_run=%s",
            interval_sec, self.dry_run,
        )
        while not self._stop.is_set():
            try:
                await self.run_once()
            except Exception as e:
                logger.exception("run_once failed: %s", e)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval_sec)
            except asyncio.TimeoutError:
                pass
        logger.info("EV engine stopped.")

    def request_stop(self) -> None:
        logger.info("Stop requested.")
        self._stop.set()


async def _async_main(args: argparse.Namespace) -> int:
    engine = EVEngine(dry_run=args.dry_run)

    # Graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, engine.request_stop)
        except NotImplementedError:
            # Windows
            pass

    if args.once:
        await engine.run_once()
    else:
        await engine.run_forever(args.interval)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="EV cashout engine")
    parser.add_argument("--once", action="store_true", help="Run one pass and exit")
    parser.add_argument("--interval", type=int, default=60, help="Loop interval seconds")
    parser.add_argument("--dry-run", action="store_true",
                        help="Evaluate positions but don't send Telegram alerts")
    args = parser.parse_args()
    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    sys.exit(main())
