"""
Telegram alerter for the EV engine.

Only sends messages when the decision engine says "cashout" — per spec,
there are no hold/open/position-update notifications. The module also
dedupes so we don't spam the same cashout alert every polling cycle.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Optional

from utils.telegram_alerts import send_alert

from .decision_engine import Decision
from .position_manager import TexaskidPosition


logger = logging.getLogger(__name__)


# Dedupe window: don't re-send the same cashout alert more than once per this many sec
ALERT_DEDUPE_SEC = 15 * 60


@dataclass
class AlertRecord:
    """Last-sent timestamp keyed by (condition_id, direction)."""
    sent_at: float
    cashout_price: float
    p_hold: float


class Alerter:
    """Cashout-only Telegram notifier with dedupe."""

    def __init__(self) -> None:
        self._bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        self._chat_id = (
            os.environ.get("TELEGRAM_CHAT_IDS")
            or os.environ.get("TELEGRAM_CHAT_ID")
            or ""
        )
        if not self._bot_token or not self._chat_id:
            logger.warning(
                "Telegram not configured (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID missing)"
            )
        self._sent: dict[tuple[str, str], AlertRecord] = {}

    def _should_send(self, pos: TexaskidPosition, decision: Decision) -> bool:
        key = (pos.condition_id, pos.direction)
        now = time.time()
        prev = self._sent.get(key)
        if prev and (now - prev.sent_at) < ALERT_DEDUPE_SEC:
            # Only re-alert within the window if the edge got meaningfully worse
            if decision.p_hold is not None and decision.cashout_price is not None:
                edge_now = decision.cashout_price - decision.p_hold
                edge_prev = prev.cashout_price - prev.p_hold
                if edge_now - edge_prev < 0.05:
                    return False
        return True

    def _record(self, pos: TexaskidPosition, decision: Decision) -> None:
        self._sent[(pos.condition_id, pos.direction)] = AlertRecord(
            sent_at=time.time(),
            cashout_price=decision.cashout_price or 0,
            p_hold=decision.p_hold or 0,
        )

    async def maybe_alert(self, pos: TexaskidPosition, decision: Decision) -> bool:
        """Send a Telegram alert if this is a new cashout recommendation."""
        if decision.action != "cashout":
            return False
        if not self._should_send(pos, decision):
            return False
        if not self._bot_token or not self._chat_id:
            return False

        message = self._format(pos, decision)
        try:
            ok = await send_alert(self._bot_token, self._chat_id, message)
        except Exception as e:
            logger.warning("Telegram send failed: %s", e)
            return False

        if ok:
            self._record(pos, decision)
            logger.info(
                "Cashout alert sent: %s %s edge=%+.3f",
                pos.market_title[:40], pos.direction, decision.edge or 0,
            )
        return ok

    @staticmethod
    def _format(pos: TexaskidPosition, decision: Decision) -> str:
        """Build the HTML-formatted cashout alert.

        Always displays numbers relative to a fixed $10 position — Texaskid's
        real size is ignored on the alert so the user sees a stable, easy-to-
        read reference scale.
        """
        p = decision.p_hold or 0.0
        c = decision.cashout_price or 0.0
        edge = decision.edge or 0.0

        display_size = 10.0
        display_loss = display_size * edge if edge > 0 else 0.0
        display_lock_in = c * display_size

        # Game state line
        live_line = ""
        if pos.mlb_state:
            s = pos.mlb_state
            half = "top" if s.top_bottom == 0 else "bot"
            live_line = (
                f"⚾ {s.away_abbr} {s.away_score} @ {s.home_abbr} {s.home_score} "
                f"({half} {s.inning}, {s.outs} out, runners={s.runners_on})"
            )
        elif pos.nba_state:
            s = pos.nba_state
            mins = s.time_remaining_sec // 60
            secs = s.time_remaining_sec % 60
            live_line = (
                f"🏀 {s.away_abbr} {s.away_score} @ {s.home_abbr} {s.home_score} "
                f"(Q{s.period}, {mins}:{secs:02d})"
            )

        return (
            f"🚨 <b>CASH OUT RECOMMENDED</b>\n"
            f"<b>{_escape(pos.market_title)}</b>\n"
            f"Side: <b>{_escape(pos.direction)}</b>\n"
            f"{live_line}\n"
            f"\n"
            f"Model p(win) : <b>{p:.3f}</b>\n"
            f"Cashout price: <b>{c:.3f}</b>\n"
            f"Edge         : <b>{edge:+.3f}</b>\n"
            f"Position size: <b>${display_size:,.2f}</b>\n"
            f"Expected loss: <b>${display_loss:,.2f}</b> if held to resolution\n"
            f"\n"
            f"<i>Sell now to lock in ${display_lock_in:,.2f}</i>"
        )


def _escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
