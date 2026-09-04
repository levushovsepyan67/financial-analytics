"""Утилиты для алертов и уведомлений."""

import logging
from typing import Optional

import requests

from config.settings import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)


def send_telegram_message(
    message: str,
    bot_token: Optional[str] = None,
    chat_id: Optional[str] = None,
) -> bool:
    """Отправляет сообщение в Telegram. Возвращает True при успехе."""
    token = bot_token or TELEGRAM_BOT_TOKEN
    cid = chat_id or TELEGRAM_CHAT_ID

    if not token or not cid:
        logger.warning("Telegram не настроен (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": cid,
        "text": message,
        "parse_mode": "HTML",
    }

    try:
        response = requests.post(url, json=payload, timeout=15)
        response.raise_for_status()
        logger.info("Telegram сообщение отправлено")
        return True
    except Exception as exc:
        logger.error("Ошибка отправки Telegram: %s", exc)
        return False
