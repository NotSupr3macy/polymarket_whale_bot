"""
Telegram notification utility for trade alerts.

Sends formatted messages to one or more Telegram chats via the Bot API.
Supports both single and comma-separated chat IDs:
  TELEGRAM_CHAT_ID=123456789          (single user or group)
  TELEGRAM_CHAT_IDS=123456789,-100987  (multiple targets)
"""

from __future__ import annotations

import logging

import aiohttp

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


def resolve_chat_ids(chat_id: str = "", chat_ids: str = "") -> list[str]:
    """Build deduplicated list from TELEGRAM_CHAT_ID and TELEGRAM_CHAT_IDS."""
    raw = f"{chat_ids},{chat_id}" if chat_ids else chat_id
    seen: set[str] = set()
    result: list[str] = []
    for cid in raw.split(","):
        cid = cid.strip()
        if cid and cid not in seen:
            seen.add(cid)
            result.append(cid)
    return result


async def send_alert(token: str, chat_id: str, message: str) -> bool:
    """
    Send a Telegram message to one or more chats.

    Args:
        token: Bot API token from @BotFather
        chat_id: Single chat ID or comma-separated list of chat IDs
        message: Message text (supports basic HTML)

    Returns:
        True if sent successfully to at least one chat
    """
    if not token or not chat_id:
        return False

    chat_ids = resolve_chat_ids(chat_id)
    any_success = False

    for cid in chat_ids:
        url = TELEGRAM_API.format(token=token)
        payload = {
            "chat_id": cid,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        any_success = True
                    else:
                        body = await resp.text()
                        logger.warning("Telegram API error %d for chat %s: %s", resp.status, cid, body[:200])
        except Exception as e:
            logger.debug("Telegram send failed for chat %s: %s", cid, e)

    return any_success


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
