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
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from shared.sheets_academy import (
    open_spreadsheet,
    append_to_worksheet,
    get_user,
    create_access_request,
    approve_user,
    reject_user,
    get_pending_requests
)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Супер-админ (может одобрять заявки)
ADMIN_ID = 57186925

# WebApp URL для профиля навыков
WEBAPP_URL = os.environ.get("WEBAPP_URL", "")

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
        self.dp.message.register(self.cmd_pending, Command("pending"))
        self.dp.message.register(self.cmd_profile, Command("profile"))

        # Callbacks: система доступа
        self.dp.callback_query.register(self.on_request_access, F.data == "request_access")
        self.dp.callback_query.register(self.on_approve, F.data.startswith("approve:"))
        self.dp.callback_query.register(self.on_reject, F.data.startswith("reject:"))

        # Callbacks: обучение
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
        user_id = message.from_user.id
        username = message.from_user.username
        name = message.from_user.full_name

        # ЛОГИРОВАНИЕ для получения Telegram ID новых пользователей (Бика, Ниса)
        logger.info(f"👤 /start от: {name} (@{username}) | ID: {user_id}")

        # Админ всегда имеет доступ
        if user_id == ADMIN_ID:
            keyboard = None
            if WEBAPP_URL:
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="Профиль навыков", web_app=WebAppInfo(url=WEBAPP_URL))
                ]])
            await message.answer(
                "Привет, админ! Ты управляешь Академией INSTINTO.\n\n"
                "Команды:\n"
                "/modules — список модулей обучения\n"
                "/pending — заявки на рассмотрении\n"
                "/profile — профиль навыков",
                reply_markup=keyboard
            )
            return

        # Проверяем пользователя в базе
        user = get_user(self.spreadsheet, user_id)

        if user is None:
            # Новый пользователь — предлагаем запросить доступ
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="Запросить доступ", callback_data="request_access")
            ]])
            await message.answer(
                "Привет! Я бот Академии INSTINTO.\n\n"
                "Для доступа к обучению нужно одобрение администратора.",
                reply_markup=keyboard
            )
            return

        status = user.get("status", "")
        role = user.get("role", "")

        if status == "pending":
            await message.answer(
                "Твоя заявка на рассмотрении.\n"
                "Как только администратор одобрит — я напишу тебе."
            )
            return

        if status == "rejected":
            await message.answer("К сожалению, твоя заявка была отклонена.")
            return

        if status == "approved":
            role_text = {
                "manager": "менеджер",
                "team_lead": "руководитель",
                "admin": "администратор"
            }.get(role, role)

            keyboard = None
            if WEBAPP_URL:
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="Мой профиль навыков", web_app=WebAppInfo(url=WEBAPP_URL))
                ]])

            await message.answer(
                f"С возвращением! Твоя роль: {role_text}\n\n"
                "Команды:\n"
                "/modules — список модулей обучения\n"
                "/profile — профиль навыков",
                reply_markup=keyboard
            )
            return

        # Неизвестный статус
        await message.answer("Что-то пошло не так. Напиши администратору.")

    async def cmd_modules(self, message: Message):
        """Показывает список модулей."""
        user_id = message.from_user.id

        # Проверяем доступ (админ или approved пользователь)
        if user_id != ADMIN_ID:
            user = get_user(self.spreadsheet, user_id)
            if not user or user.get("status") != "approved":
                await message.answer("У тебя нет доступа. Напиши /start чтобы запросить.")
                return

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

    async def cmd_profile(self, message: Message):
        """Открывает профиль навыков."""
        if not WEBAPP_URL:
            await message.answer("WebApp профиля пока не настроен.")
            return

        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Открыть профиль", web_app=WebAppInfo(url=WEBAPP_URL))
        ]])
        await message.answer("Нажми кнопку для просмотра профиля навыков:", reply_markup=keyboard)

    async def cmd_pending(self, message: Message):
        """Показывает заявки на рассмотрении (только для админа)."""
        if message.from_user.id != ADMIN_ID:
            await message.answer("Эта команда только для администратора.")
            return

        pending = get_pending_requests(self.spreadsheet)

        if not pending:
            await message.answer("Нет заявок на рассмотрении.")
            return

        for req in pending:
            tid = req.get("telegram_id", "")
            name = req.get("name", "Без имени")
            username = req.get("username", "")
            requested_at = req.get("requested_at", "")[:10]  # только дата

            text = f"<b>Заявка на доступ</b>\n\n"
            text += f"Имя: {name}\n"
            if username:
                text += f"Username: @{username}\n"
            text += f"ID: {tid}\n"
            text += f"Дата: {requested_at}"

            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="Менеджер", callback_data=f"approve:{tid}:manager"),
                    InlineKeyboardButton(text="Руководитель", callback_data=f"approve:{tid}:team_lead")
                ],
                [
                    InlineKeyboardButton(text="Отклонить", callback_data=f"reject:{tid}")
                ]
            ])

            await message.answer(text, reply_markup=keyboard)

    async def on_request_access(self, callback: CallbackQuery):
        """Обработчик запроса доступа."""
        await callback.answer()

        user = callback.from_user
        name = user.full_name or user.first_name or "Без имени"
        username = user.username

        # Создаём заявку
        created = create_access_request(
            self.spreadsheet,
            telegram_id=user.id,
            name=name,
            username=username
        )

        if created:
            await callback.message.answer(
                "Заявка отправлена!\n"
                "Как только администратор одобрит — я напишу тебе."
            )

            # Уведомляем админа
            text = f"<b>Новая заявка на доступ</b>\n\n"
            text += f"Имя: {name}\n"
            if username:
                text += f"Username: @{username}\n"
            text += f"ID: {user.id}"

            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="Менеджер", callback_data=f"approve:{user.id}:manager"),
                    InlineKeyboardButton(text="Руководитель", callback_data=f"approve:{user.id}:team_lead")
                ],
                [
                    InlineKeyboardButton(text="Отклонить", callback_data=f"reject:{user.id}")
                ]
            ])

            try:
                await self.bot.send_message(ADMIN_ID, text, reply_markup=keyboard)
            except Exception as e:
                logger.error(f"Не удалось уведомить админа: {e}")
        else:
            await callback.message.answer(
                "Заявка уже была отправлена ранее.\n"
                "Ожидай решения администратора."
            )

    async def on_approve(self, callback: CallbackQuery):
        """Обработчик одобрения заявки."""
        if callback.from_user.id != ADMIN_ID:
            await callback.answer("Только админ может одобрять заявки", show_alert=True)
            return

        await callback.answer()

        # approve:123456:manager
        parts = callback.data.split(":")
        user_tid = parts[1]
        role = parts[2]

        success = approve_user(
            self.spreadsheet,
            telegram_id=user_tid,
            role=role,
            approved_by=ADMIN_ID
        )

        if success:
            role_text = "менеджер" if role == "manager" else "руководитель"

            # Обновляем сообщение админу
            await callback.message.edit_text(
                callback.message.text + f"\n\n✅ Одобрено как {role_text}"
            )

            # Уведомляем пользователя
            try:
                await self.bot.send_message(
                    int(user_tid),
                    f"Твоя заявка одобрена!\n"
                    f"Роль: {role_text}\n\n"
                    f"Напиши /modules чтобы начать обучение."
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить пользователя {user_tid}: {e}")
        else:
            await callback.message.answer("Ошибка при одобрении. Попробуй ещё раз.")

    async def on_reject(self, callback: CallbackQuery):
        """Обработчик отклонения заявки."""
        if callback.from_user.id != ADMIN_ID:
            await callback.answer("Только админ может отклонять заявки", show_alert=True)
            return

        await callback.answer()

        # reject:123456
        user_tid = callback.data.split(":")[1]

        success = reject_user(self.spreadsheet, telegram_id=user_tid)

        if success:
            # Обновляем сообщение админу
            await callback.message.edit_text(
                callback.message.text + "\n\n❌ Отклонено"
            )

            # Уведомляем пользователя
            try:
                await self.bot.send_message(
                    int(user_tid),
                    "К сожалению, твоя заявка была отклонена."
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить пользователя {user_tid}: {e}")
        else:
            await callback.message.answer("Ошибка при отклонении. Попробуй ещё раз.")

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
        # Удаляем webhook если был и сбрасываем старые апдейты
        await self.bot.delete_webhook(drop_pending_updates=True)
        logger.info("Бот запущен")

        # Уведомление об успешном запуске через централизованную систему алертов
        from shared.alerting import alert_success
        alert_success(
            service_name="bot-obrabotchik-komand",
            message="Бот запущен и работает"
        )

        await self.dp.start_polling(self.bot, allowed_updates=["message", "callback_query"])


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
