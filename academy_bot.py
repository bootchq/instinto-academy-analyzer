"""
Telegram бот для Академии INSTINTO.

Обрабатывает:
- Inline-кнопки "Пройти модуль" из еженедельных отчётов
- Показывает урок
- Проводит тест
- Записывает прогресс в Google Sheets

Использование:
    python academy_bot.py

Переменные окружения:
    TELEGRAM_BOT_TOKEN=...
    GOOGLE_SHEETS_ID=...
    GOOGLE_SERVICE_ACCOUNT_JSON=...
"""

from __future__ import annotations

import asyncio
import json
import os
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from sheets import open_spreadsheet, append_to_worksheet

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Загружаем модули
MODULES_PATH = Path(__file__).parent / "modules" / "learning_modules.json"

def load_modules() -> Dict[str, Any]:
    """Загружает модули из JSON."""
    if not MODULES_PATH.exists():
        logger.error(f"Файл модулей не найден: {MODULES_PATH}")
        return {"modules": []}

    with open(MODULES_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

MODULES_DATA = load_modules()
MODULES_BY_ID = {m["id"]: m for m in MODULES_DATA.get("modules", [])}
MODULES_BY_SKILL = {m["skill_key"]: m for m in MODULES_DATA.get("modules", [])}


class AcademyBot:
    """Telegram бот для обучения."""

    def __init__(self, token: str, sheets_id: str, sa_json: str):
        self.bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        self.dp = Dispatcher()
        self.sheets_id = sheets_id
        self.sa_json = sa_json
        self._ss = None

        # Регистрируем handlers
        self._register_handlers()

    def _register_handlers(self):
        """Регистрирует обработчики."""
        # Команды
        self.dp.message.register(self.cmd_start, Command("start"))
        self.dp.message.register(self.cmd_modules, Command("modules"))

        # Callbacks
        self.dp.callback_query.register(self.on_module_start, F.data.startswith("module:"))
        self.dp.callback_query.register(self.on_quiz_answer, F.data.startswith("quiz:"))

    @property
    def spreadsheet(self):
        """Ленивая инициализация spreadsheet."""
        if self._ss is None:
            self._ss = open_spreadsheet(
                spreadsheet_id=self.sheets_id,
                service_account_json_path=self.sa_json
            )
        return self._ss

    async def cmd_start(self, message: Message):
        """Обработчик /start."""
        await message.answer(
            "Привет! Я бот Академии INSTINTO.\n\n"
            "Я помогу тебе прокачать навыки продаж.\n\n"
            "Команды:\n"
            "/modules — список модулей обучения"
        )

    async def cmd_modules(self, message: Message):
        """Показывает список модулей."""
        buttons = []
        for module in MODULES_DATA.get("modules", []):
            buttons.append([
                InlineKeyboardButton(
                    text=f"📚 {module['title']}",
                    callback_data=f"module:{module['id']}"
                )
            ])

        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        await message.answer("Выбери модуль для изучения:", reply_markup=keyboard)

    async def on_module_start(self, callback: CallbackQuery):
        """Обработчик нажатия на кнопку модуля."""
        await callback.answer()

        # Извлекаем module_id из callback_data
        _, module_id = callback.data.split(":", 1)

        module = MODULES_BY_ID.get(module_id)
        if not module:
            await callback.message.answer("Модуль не найден")
            return

        # Записываем начало обучения
        await self._record_progress(
            manager_id=str(callback.from_user.id),
            module_id=module_id,
            action="started"
        )

        # Отправляем урок
        content = module["content"]
        # Разбиваем на части если слишком длинный (Telegram лимит 4096)
        if len(content) > 3500:
            parts = [content[i:i+3500] for i in range(0, len(content), 3500)]
            for part in parts[:-1]:
                await callback.message.answer(part)
            content = parts[-1]

        # Кнопка для теста
        quiz_button = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="📝 Пройти тест",
                callback_data=f"quiz:{module_id}:start"
            )
        ]])

        await callback.message.answer(
            f"<b>{module['title']}</b>\n\n{content}",
            reply_markup=quiz_button
        )

    async def on_quiz_answer(self, callback: CallbackQuery):
        """Обработчик теста."""
        await callback.answer()

        parts = callback.data.split(":")
        module_id = parts[1]
        action = parts[2]

        module = MODULES_BY_ID.get(module_id)
        if not module:
            await callback.message.answer("Модуль не найден")
            return

        quiz = module.get("quiz", {})

        if action == "start":
            # Показываем вопрос с вариантами ответа
            buttons = []
            for i, option in enumerate(quiz.get("options", [])):
                buttons.append([
                    InlineKeyboardButton(
                        text=option,
                        callback_data=f"quiz:{module_id}:answer:{i}"
                    )
                ])

            keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
            await callback.message.answer(
                f"<b>Тест: {module['title']}</b>\n\n{quiz.get('question', '')}",
                reply_markup=keyboard
            )

        elif action == "answer":
            # Проверяем ответ
            answer_idx = int(parts[3])
            correct_idx = quiz.get("correct", 0)
            is_correct = answer_idx == correct_idx

            # Записываем результат
            await self._record_progress(
                manager_id=str(callback.from_user.id),
                module_id=module_id,
                action="completed",
                quiz_correct=is_correct,
                quiz_answer=answer_idx
            )

            if is_correct:
                await callback.message.answer(
                    "✅ <b>Правильно!</b>\n\n"
                    f"Модуль \"{module['title']}\" пройден.\n"
                    "Продолжай в том же духе!"
                )
            else:
                correct_text = quiz.get("options", [])[correct_idx]
                await callback.message.answer(
                    "❌ <b>Неверно</b>\n\n"
                    f"Правильный ответ: {correct_text}\n\n"
                    "Перечитай урок и попробуй ещё раз.\n"
                    f"Используй /modules чтобы открыть модуль \"{module['title']}\" снова."
                )

    async def _record_progress(
        self,
        manager_id: str,
        module_id: str,
        action: str,
        quiz_correct: Optional[bool] = None,
        quiz_answer: Optional[int] = None
    ):
        """Записывает прогресс в Google Sheets."""
        try:
            now = datetime.now(timezone.utc).isoformat()

            row = [
                manager_id,
                module_id,
                now if action == "started" else "",  # started_at
                now if action == "completed" else "",  # completed_at
                "Да" if quiz_correct else ("Нет" if quiz_correct is False else ""),
                str(quiz_answer) if quiz_answer is not None else ""
            ]

            header = ["manager_id", "module_id", "started_at", "completed_at", "quiz_correct", "quiz_answer"]

            append_to_worksheet(
                self.spreadsheet,
                "learning_progress",
                rows=[row],
                header=header
            )
            logger.info(f"Записан прогресс: manager={manager_id}, module={module_id}, action={action}")
        except Exception as e:
            logger.error(f"Ошибка записи прогресса: {e}")

    async def run(self):
        """Запускает бота."""
        logger.info("Бот запущен")
        await self.dp.start_polling(self.bot)


async def main():
    """Точка входа."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    sheets_id = os.environ.get("GOOGLE_SHEETS_ID")
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")

    if not all([token, sheets_id, sa_json]):
        logger.error("Не заданы переменные окружения: TELEGRAM_BOT_TOKEN, GOOGLE_SHEETS_ID, GOOGLE_SERVICE_ACCOUNT_JSON")
        return

    bot = AcademyBot(token=token, sheets_id=sheets_id, sa_json=sa_json)
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
