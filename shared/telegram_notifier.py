"""Общий модуль для отправки Telegram уведомлений."""
from __future__ import annotations

from typing import List

import requests


# Названия навыков для кнопок модулей
SKILL_NAMES = {
    "greeting_score": "Приветствие",
    "needs_score": "Выявление потребностей",
    "presentation_score": "Презентация",
    "objection_score": "Работа с возражениями",
    "closing_score": "Закрытие сделки",
    "cross_sell_score": "Допродажа",
}


class TelegramNotifier:
    """Отправляет уведомления в Telegram."""

    def __init__(self, bot_token: str):
        self.bot_token = bot_token
        self.base_url = f"https://api.telegram.org/bot{bot_token}"

    def send(self, chat_id: str, message: str, parse_mode: str = "HTML", reply_markup: dict = None) -> bool:
        """Отправить сообщение конкретному пользователю."""
        if not self.bot_token or not chat_id:
            print(f"Telegram не настроен (нет токена или chat_id={chat_id})")
            return False
        try:
            payload = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": parse_mode
            }
            if reply_markup:
                payload["reply_markup"] = reply_markup

            resp = requests.post(
                f"{self.base_url}/sendMessage",
                json=payload,
                timeout=10
            )
            if resp.status_code != 200:
                print(f"Telegram API error для {chat_id}: {resp.text}")
            return resp.status_code == 200
        except Exception as e:
            print(f"Ошибка отправки в Telegram ({chat_id}): {e}")
            return False

    def send_with_module_buttons(self, chat_id: str, message: str, skill_keys: List[str]) -> bool:
        """Отправить сообщение с inline-кнопками модулей."""
        # Маппинг skill_key -> module_id
        skill_to_module = {
            "greeting_score": "greeting",
            "needs_score": "needs_discovery",
            "presentation_score": "presentation",
            "objection_score": "objection_handling",
            "closing_score": "closing",
            "cross_sell_score": "cross_sell",
        }

        buttons = []
        for skill_key in skill_keys:
            module_id = skill_to_module.get(skill_key)
            if module_id:
                skill_name = SKILL_NAMES.get(skill_key, skill_key)
                buttons.append([{
                    "text": f"Пройти: {skill_name}",
                    "callback_data": f"module:{module_id}"
                }])

        if not buttons:
            return self.send(chat_id, message)

        reply_markup = {"inline_keyboard": buttons}
        return self.send(chat_id, message, reply_markup=reply_markup)
