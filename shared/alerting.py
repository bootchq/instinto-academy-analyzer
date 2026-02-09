"""Централизованная система алертов для Railway сервисов."""

import os
import traceback
import requests
from datetime import datetime, timezone

from . import time_utils

ADMIN_ID = 57186925
# Для алертов используем отдельный токен (@analiz_raboty_manager_bot)
# Если ALERT_BOT_TOKEN не задан — фоллбэк на TELEGRAM_BOT_TOKEN
ALERT_BOT_TOKEN = os.environ.get("ALERT_BOT_TOKEN") or os.environ.get("TELEGRAM_BOT_TOKEN")


def send_telegram(chat_id: int, text: str, parse_mode: str = "HTML"):
    """Отправить сообщение в Telegram через бот алертов."""
    if not ALERT_BOT_TOKEN:
        print(f"⚠️ ALERT_BOT_TOKEN не установлен, пропускаю алерт")
        return False

    url = f"https://api.telegram.org/bot{ALERT_BOT_TOKEN}/sendMessage"
    try:
        response = requests.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode
        }, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"⚠️ Не удалось отправить алерт: {e}")
        return False


def alert_error(service_name: str, error: Exception, context: str = ""):
    """Отправить алерт об ошибке."""
    tb = traceback.format_exc()
    timestamp = time_utils.utc_now().strftime("%Y-%m-%d %H:%M UTC")

    text = f"🔴 <b>ОШИБКА: {service_name}</b>\n\n"
    text += f"⏰ {timestamp}\n"
    if context:
        text += f"📍 {context}\n"
    text += f"❌ {type(error).__name__}: {str(error)}\n\n"
    text += f"<pre>{tb[:800]}</pre>"  # Telegram лимит 4096, оставляем место

    send_telegram(ADMIN_ID, text)
    print(text)  # Дублируем в stdout для Railway logs


def alert_success(service_name: str, message: str, stats: dict = None):
    """Отправить алерт об успешном выполнении."""
    timestamp = time_utils.utc_now().strftime("%Y-%m-%d %H:%M UTC")

    text = f"🟢 <b>{service_name}</b>\n\n"
    text += f"⏰ {timestamp}\n"
    text += f"✅ {message}\n"

    if stats:
        text += "\n📊 <b>Статистика:</b>\n"
        for key, value in stats.items():
            text += f"  • {key}: {value}\n"

    send_telegram(ADMIN_ID, text)
    print(text)


def alert_warning(service_name: str, message: str):
    """Отправить предупреждение (опционально, для будущего)."""
    timestamp = time_utils.utc_now().strftime("%Y-%m-%d %H:%M UTC")

    text = f"🟡 <b>ПРЕДУПРЕЖДЕНИЕ: {service_name}</b>\n\n"
    text += f"⏰ {timestamp}\n"
    text += f"⚠️ {message}\n"

    send_telegram(ADMIN_ID, text)
    print(text)
