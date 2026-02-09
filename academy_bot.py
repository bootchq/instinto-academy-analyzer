"""
Telegram бот для Академии INSTINTO.

Обрабатывает:
- Inline-кнопки "Пройти модуль" из еженедельных отчётов
- Показывает урок
- Проводит тест
- Записывает прогресс в Google Sheets
- Веб-авторизация: одобрение заявок и отправка логина/пароля

Использование:
    python academy_bot.py

Переменные окружения:
    TELEGRAM_BOT_TOKEN=...
    GOOGLE_SHEETS_ID=...
    GOOGLE_SERVICE_ACCOUNT_JSON=...
    DATABASE_URL=... (для веб-авторизации)
"""

from __future__ import annotations

import asyncio
import json
import os
import logging

# Устанавливаем Moscow timezone для всех datetime операций
os.environ['TZ'] = 'Europe/Moscow'
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from shared.sheets_academy import (
    open_spreadsheet,
    append_to_worksheet
)
from shared import time_utils

# Веб-авторизация
from web_auth import approve_web_request, reject_web_request, run_api_server, save_telegram_user

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Супер-админ (может одобрять заявки)
ADMIN_ID = 57186925

# Список ID с доступом к /admin (админ + управляющий)
MANAGER_IDS = set(map(int, filter(None, os.environ.get("MANAGER_IDS", "").split(","))))
MANAGER_IDS.add(ADMIN_ID)

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
        self.dp.message.register(self.cmd_admin, Command("admin"))

        # Callbacks: старая система (не используется, оставлено для совместимости)
        # self.dp.callback_query.register(self.on_request_access, F.data == "request_access")
        # self.dp.callback_query.register(self.on_approve, F.data.startswith("approve:"))
        # self.dp.callback_query.register(self.on_reject, F.data.startswith("reject:"))

        # Callbacks: веб-авторизация
        self.dp.callback_query.register(self.on_web_approve, F.data.startswith("web_approve:"))
        self.dp.callback_query.register(self.on_web_reject, F.data.startswith("web_reject:"))

        # Callbacks: обучение
        self.dp.callback_query.register(self.on_module_start, F.data.startswith("module:"))
        self.dp.callback_query.register(self.on_quiz_answer, F.data.startswith("quiz:"))

        # Контакт для авторизации
        self.dp.message.register(self.on_contact_received, F.contact)

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
        username = message.from_user.username or str(user_id)
        name = message.from_user.full_name

        # ЛОГИРОВАНИЕ для получения Telegram ID новых пользователей (Бика, Ниса)
        logger.info(f"👤 /start от: {name} (@{username}) | ID: {user_id}")

        # Сохраняем telegram_id для веб-авторизации
        save_telegram_user(user_id, username, name)

        # Проверяем deep-link параметр (start=access с сайта)
        args = message.text.split(maxsplit=1)
        if len(args) > 1 and args[1] == "access":
            await self._handle_web_access_request(message)
            return

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

        # Проверяем пользователя в PostgreSQL
        from web_auth import get_db

        try:
            conn = get_db()
            cur = conn.cursor()

            # Проверяем, есть ли пользователь с доступом
            cur.execute(
                "SELECT login, role FROM web_users WHERE telegram_username = %s",
                (username,)
            )
            web_user = cur.fetchone()

            if web_user:
                # Пользователь уже авторизован
                cur.close()
                conn.close()

                await message.answer(
                    f"С возвращением!\n\n"
                    f"Ваш логин: <code>{web_user['login']}</code>\n"
                    f"Сайт: https://academy-modules.vercel.app",
                    reply_markup=ReplyKeyboardRemove()
                )
                return

            # Проверяем, есть ли заявка
            cur.execute(
                "SELECT status FROM web_access_requests WHERE telegram_username = %s ORDER BY created_at DESC LIMIT 1",
                (username,)
            )
            request = cur.fetchone()
            cur.close()
            conn.close()

            if request and request["status"] == "pending":
                await message.answer(
                    "Твоя заявка на рассмотрении.\n"
                    "Как только администратор одобрит — я пришлю логин и пароль.",
                    reply_markup=ReplyKeyboardRemove()
                )
                return

            # Новый пользователь — предлагаем запросить доступ
            keyboard = ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="Запросить доступ", request_contact=True)]],
                resize_keyboard=True,
                one_time_keyboard=True
            )
            await message.answer(
                "Привет! Я бот Академии INSTINTO.\n\n"
                "Для доступа нажмите кнопку и поделитесь контактом.",
                reply_markup=keyboard
            )

        except Exception as e:
            logger.error(f"Ошибка проверки пользователя: {e}")
            # При ошибке — предлагаем запросить доступ
            keyboard = ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="Запросить доступ", request_contact=True)]],
                resize_keyboard=True,
                one_time_keyboard=True
            )
            await message.answer(
                "Привет! Я бот Академии INSTINTO.\n\n"
                "Для доступа нажмите кнопку и поделитесь контактом.",
                reply_markup=keyboard
            )

        # Неизвестный статус
        await message.answer("Что-то пошло не так. Напиши администратору.")

    async def cmd_modules(self, message: Message):
        """Показывает список модулей."""
        user_id = message.from_user.id
        username = message.from_user.username or str(user_id)

        # Проверяем доступ (админ или approved пользователь в PostgreSQL)
        if user_id != ADMIN_ID:
            from web_auth import get_db
            try:
                conn = get_db()
                cur = conn.cursor()
                cur.execute("SELECT id FROM web_users WHERE telegram_username = %s", (username,))
                web_user = cur.fetchone()
                cur.close()
                conn.close()

                if not web_user:
                    await message.answer("У тебя нет доступа. Напиши /start чтобы запросить.")
                    return
            except Exception as e:
                logger.error(f"Ошибка проверки доступа: {e}")
                await message.answer("Ошибка проверки доступа. Попробуй позже.")
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

        from web_auth import get_db

        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute("""
                SELECT id, telegram_username, phone, created_at
                FROM web_access_requests
                WHERE status = 'pending'
                ORDER BY created_at DESC
            """)
            pending = cur.fetchall()
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"Ошибка получения заявок: {e}")
            await message.answer("Ошибка получения заявок.")
            return

        if not pending:
            await message.answer("Нет заявок на рассмотрении.")
            return

        for req in pending:
            request_id = req["id"]
            username = req.get("telegram_username", "")
            phone = req.get("phone", "")
            created_at = str(req.get("created_at", ""))[:10]

            text = f"<b>Заявка на доступ</b>\n\n"
            text += f"Username: @{username}\n"
            if phone:
                text += f"Телефон: {phone}\n"
            text += f"Дата: {created_at}\n"
            text += f"ID заявки: {request_id}"

            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="Одобрить", callback_data=f"web_approve:{request_id}"),
                    InlineKeyboardButton(text="Отклонить", callback_data=f"web_reject:{request_id}")
                ]
            ])

            await message.answer(text, reply_markup=keyboard)

    async def cmd_admin(self, message: Message):
        """Показывает прогресс студентов (для админа и управляющего)."""
        if message.from_user.id not in MANAGER_IDS:
            await message.answer("Эта команда доступна только для администрации.")
            return

        from web_auth import get_db

        try:
            conn = get_db()
            cur = conn.cursor()

            cur.execute("""
                SELECT
                    wu.id,
                    wu.login,
                    wu.telegram_username,
                    war.phone,
                    tu.full_name,
                    COUNT(CASE WHEN wp.passed THEN 1 END) as modules_completed,
                    ROUND(AVG(CASE WHEN wp.passed THEN wp.score END)::numeric, 1) as avg_score,
                    COALESCE(SUM(wp.time_spent_seconds), 0) as total_time_seconds
                FROM web_users wu
                LEFT JOIN web_progress wp ON wu.id = wp.user_id
                LEFT JOIN web_access_requests war
                    ON wu.telegram_username = war.telegram_username AND war.status = 'approved'
                LEFT JOIN telegram_users tu ON wu.telegram_username = tu.username
                WHERE wu.role = 'student'
                GROUP BY wu.id, wu.login, wu.telegram_username, war.phone, tu.full_name
                ORDER BY modules_completed DESC, avg_score DESC
            """)

            students = cur.fetchall()
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"Ошибка получения прогресса: {e}")
            await message.answer("Ошибка получения данных.")
            return

        if not students:
            await message.answer("Нет студентов.")
            return

        # Форматируем отчёт
        lines = ["<b>Прогресс студентов Академии</b>\n"]

        for s in students:
            name = s["full_name"] or s["telegram_username"] or s["login"]
            phone = s["phone"] or "—"
            completed = s["modules_completed"] or 0
            avg = s["avg_score"] if s["avg_score"] is not None else "—"
            total_sec = s["total_time_seconds"] or 0

            hours = total_sec // 3600
            minutes = (total_sec % 3600) // 60
            time_str = f"{hours}ч {minutes}м" if hours > 0 else (f"{minutes}м" if total_sec > 0 else "—")

            bar = "\u2588" * completed + "\u2591" * (14 - completed)

            lines.append(
                f"<b>{name}</b>\n"
                f"Тел: {phone}\n"
                f"Модули: {completed}/14 [{bar}]\n"
                f"Ср. балл: {avg}/10\n"
                f"Время: {time_str}\n"
            )

        # Разбиваем на сообщения по 4000 символов
        text = ""
        for line in lines:
            if len(text) + len(line) > 4000:
                await message.answer(text)
                text = ""
            text += line

        if text:
            await message.answer(text)

    async def on_request_access(self, callback: CallbackQuery):
        """Обработчик запроса доступа — запрашиваем контакт."""
        await callback.answer()

        # Запрашиваем контакт для получения телефона
        keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Отправить контакт", request_contact=True)]],
            resize_keyboard=True,
            one_time_keyboard=True
        )

        await callback.message.answer(
            "Для подачи заявки поделитесь своим контактом.\n"
            "Это нужно для верификации.",
            reply_markup=keyboard
        )

    async def _handle_web_access_request(self, message: Message):
        """Обработчик запроса доступа с сайта через deep-link — запрашиваем контакт."""
        from web_auth import get_db

        user = message.from_user
        username = user.username or str(user.id)

        try:
            conn = get_db()
            cur = conn.cursor()

            # Проверяем, есть ли уже пользователь с доступом
            cur.execute(
                "SELECT id, login FROM web_users WHERE telegram_username = %s",
                (username,)
            )
            existing_user = cur.fetchone()

            if existing_user:
                await message.answer(
                    "У вас уже есть доступ к Академии!\n\n"
                    f"Ваш логин: <code>{existing_user['login']}</code>\n\n"
                    "Сайт: https://academy-modules.vercel.app",
                    reply_markup=ReplyKeyboardRemove()
                )
                cur.close()
                conn.close()
                return

            # Проверяем, есть ли уже заявка
            cur.execute(
                "SELECT id, status FROM web_access_requests WHERE telegram_username = %s ORDER BY created_at DESC LIMIT 1",
                (username,)
            )
            existing_request = cur.fetchone()
            cur.close()
            conn.close()

            if existing_request and existing_request["status"] == "pending":
                await message.answer(
                    "Ваша заявка уже отправлена и ожидает рассмотрения.\n"
                    "Как только администратор одобрит — я пришлю вам логин и пароль.",
                    reply_markup=ReplyKeyboardRemove()
                )
                return

            # Запрашиваем контакт
            keyboard = ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="Отправить контакт", request_contact=True)]],
                resize_keyboard=True,
                one_time_keyboard=True
            )

            await message.answer(
                "Для получения доступа к Академии поделитесь своим контактом.\n"
                "Это нужно для верификации.",
                reply_markup=keyboard
            )

        except Exception as e:
            logger.error(f"Ошибка проверки заявки: {e}")
            # Всё равно запрашиваем контакт
            keyboard = ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="Отправить контакт", request_contact=True)]],
                resize_keyboard=True,
                one_time_keyboard=True
            )
            await message.answer(
                "Для получения доступа к Академии поделитесь своим контактом.",
                reply_markup=keyboard
            )

    async def on_contact_received(self, message: Message):
        """Обработчик получения контакта — создаём заявку."""
        from web_auth import get_db

        contact = message.contact
        user = message.from_user

        # Проверяем что это свой контакт
        if contact.user_id != user.id:
            await message.answer(
                "Пожалуйста, отправьте свой контакт, а не чужой.",
                reply_markup=ReplyKeyboardRemove()
            )
            return

        user_id = user.id
        username = user.username or str(user_id)
        name = user.full_name or user.first_name or contact.first_name or "Без имени"
        phone = contact.phone_number

        # Сохраняем telegram_id
        save_telegram_user(user_id, username, name)

        try:
            conn = get_db()
            cur = conn.cursor()

            # Проверяем, есть ли уже пользователь с доступом
            cur.execute(
                "SELECT id, login FROM web_users WHERE telegram_username = %s",
                (username,)
            )
            existing_user = cur.fetchone()

            if existing_user:
                await message.answer(
                    "У вас уже есть доступ к Академии!\n\n"
                    f"Ваш логин: <code>{existing_user['login']}</code>\n\n"
                    "Сайт: https://academy-modules.vercel.app",
                    reply_markup=ReplyKeyboardRemove()
                )
                cur.close()
                conn.close()
                return

            # Проверяем, есть ли уже заявка
            cur.execute(
                "SELECT id, status FROM web_access_requests WHERE telegram_username = %s ORDER BY created_at DESC LIMIT 1",
                (username,)
            )
            existing_request = cur.fetchone()

            if existing_request and existing_request["status"] == "pending":
                await message.answer(
                    "Ваша заявка уже отправлена и ожидает рассмотрения.\n"
                    "Как только администратор одобрит — я пришлю вам логин и пароль.",
                    reply_markup=ReplyKeyboardRemove()
                )
                cur.close()
                conn.close()
                return

            # Создаём заявку с телефоном
            cur.execute(
                "INSERT INTO web_access_requests (telegram_username, phone, status) VALUES (%s, %s, 'pending') RETURNING id",
                (username, phone)
            )
            request_id = cur.fetchone()["id"]
            conn.commit()

            await message.answer(
                "Заявка на доступ отправлена!\n\n"
                "Как только администратор одобрит — я пришлю вам логин и пароль для входа на сайт Академии.",
                reply_markup=ReplyKeyboardRemove()
            )

            # Уведомляем админа
            text = (
                f"<b>Новая заявка на доступ к Академии</b>\n\n"
                f"Имя: {name}\n"
                f"Username: @{username}\n"
                f"Телефон: {phone}\n"
                f"Telegram ID: {user_id}\n"
                f"ID заявки: {request_id}"
            )
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="Одобрить", callback_data=f"web_approve:{request_id}"),
                InlineKeyboardButton(text="Отклонить", callback_data=f"web_reject:{request_id}")
            ]])

            try:
                await self.bot.send_message(ADMIN_ID, text, reply_markup=keyboard)
            except Exception as e:
                logger.error(f"Не удалось уведомить админа: {e}")

            cur.close()
            conn.close()

        except Exception as e:
            logger.error(f"Ошибка создания заявки: {e}")
            await message.answer(
                "Произошла ошибка. Попробуйте позже.",
                reply_markup=ReplyKeyboardRemove()
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

    async def on_web_approve(self, callback: CallbackQuery):
        """Обработчик одобрения веб-заявки."""
        if callback.from_user.id != ADMIN_ID:
            await callback.answer("Только админ может одобрять заявки", show_alert=True)
            return

        await callback.answer()

        # web_approve:123
        request_id = int(callback.data.split(":")[1])

        telegram_username, telegram_id, login, password = approve_web_request(request_id)

        if telegram_username:
            # Обновляем сообщение админу
            await callback.message.edit_text(
                callback.message.text + f"\n\n✅ Одобрено\nЛогин: {login}"
            )

            # Отправляем логин/пароль пользователю
            credentials_text = (
                f"<b>Доступ к Академии INSTINTO одобрен!</b>\n\n"
                f"Ваши данные для входа:\n"
                f"<b>Логин:</b> <code>{login}</code>\n"
                f"<b>Пароль:</b> <code>{password}</code>\n\n"
                f"Сайт: https://academy-modules.vercel.app\n\n"
                f"Скопируйте логин и пароль — они понадобятся для входа."
            )

            if telegram_id:
                # Отправляем напрямую пользователю
                try:
                    await self.bot.send_message(telegram_id, credentials_text)
                    await callback.message.answer(f"Данные отправлены пользователю @{telegram_username}")
                    logger.info(f"Учётные данные отправлены пользователю {telegram_id} (@{telegram_username})")
                except Exception as e:
                    logger.error(f"Не удалось отправить данные пользователю {telegram_id}: {e}")
                    await callback.message.answer(
                        f"Не удалось отправить данные пользователю.\n\n"
                        f"Данные для @{telegram_username}:\n\n{credentials_text}\n\n"
                        f"Перешлите это сообщение пользователю @{telegram_username}"
                    )
            else:
                # Пользователь не писал боту — нужно переслать вручную
                logger.info(f"Учётные данные для @{telegram_username}: login={login} (telegram_id не найден)")
                await callback.message.answer(
                    f"Пользователь @{telegram_username} ещё не писал боту.\n\n"
                    f"Данные для @{telegram_username}:\n\n{credentials_text}\n\n"
                    f"Перешлите это сообщение пользователю @{telegram_username}"
                )
        else:
            await callback.message.answer("Ошибка при одобрении. Заявка не найдена или уже обработана.")

    async def on_web_reject(self, callback: CallbackQuery):
        """Обработчик отклонения веб-заявки."""
        if callback.from_user.id != ADMIN_ID:
            await callback.answer("Только админ может отклонять заявки", show_alert=True)
            return

        await callback.answer()

        # web_reject:123
        request_id = int(callback.data.split(":")[1])

        telegram_username = reject_web_request(request_id)

        if telegram_username:
            await callback.message.edit_text(
                callback.message.text + "\n\n❌ Отклонено"
            )
        else:
            await callback.message.answer("Ошибка при отклонении. Заявка не найдена или уже обработана.")

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
            now = time_utils.utc_now().isoformat()

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
        """Запускает бота и веб-сервер."""
        # Запускаем Flask API в отдельном потоке
        api_port = int(os.environ.get("PORT", 5000))
        api_thread = threading.Thread(
            target=run_api_server,
            kwargs={"host": "0.0.0.0", "port": api_port},
            daemon=True
        )
        api_thread.start()
        logger.info(f"API сервер запущен на порту {api_port}")

        # Удаляем webhook если был и сбрасываем старые апдейты
        await self.bot.delete_webhook(drop_pending_updates=True)
        logger.info("Telegram бот запущен")

        # Уведомление об успешном запуске через централизованную систему алертов
        from shared.alerting import alert_success
        alert_success(
            service_name="bot-obrabotchik-komand",
            message="Бот и API сервер запущены"
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
