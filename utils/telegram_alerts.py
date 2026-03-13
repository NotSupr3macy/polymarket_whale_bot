"""
Telegram notification utility for trade alerts.

Sends formatted messages to a Telegram chat/group via the Bot API.
Requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env.
"""

from __future__ import annotations

import logging

import aiohttp

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


async def send_alert(token: str, chat_id: str, message: str) -> bool:
    """
    Send a Telegram message.

    Args:
        token: Bot API token from @BotFather
        chat_id: Target chat/group ID
        message: Message text (supports basic HTML)

    Returns:
        True if sent successfully
    """
    if not token or not chat_id:
        return False

    url = TELEGRAM_API.format(token=token)
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return True
                else:
                    body = await resp.text()
                    logger.warning("Telegram API error %d: %s", resp.status, body[:200])
                    return False
    except Exception as e:
        logger.debug("Telegram send failed: %s", e)
        return False


async def send_trade_alert(
    token: str,
    chat_id: str,
    direction: str,
    market: str,
    size: float,
    price: float,
    whales: list[str],
    consensus: float,
) -> bool:
    """Send a formatted trade alert."""
    msg = (
        f"<b>TRADE SIGNAL</b>\n"
        f"Direction: {direction}\n"
        f"Market: {market[:50]}\n"
        f"Size: ${size:,.2f} @ ${price:.4f}\n"
        f"Whales: {', '.join(whales)}\n"
        f"Consensus: {consensus:.0%}"
    )
    return await send_alert(token, chat_id, msg)


async def send_stop_loss_alert(
    token: str,
    chat_id: str,
    market: str,
    pnl: float,
    entry: float,
    exit_price: float,
) -> bool:
    """Send a stop-loss triggered alert."""
    msg = (
        f"<b>STOP-LOSS TRIGGERED</b>\n"
        f"Market: {market[:50]}\n"
        f"PnL: ${pnl:,.2f}\n"
        f"Entry: ${entry:.4f} -> Exit: ${exit_price:.4f}"
    )
    return await send_alert(token, chat_id, msg)
