"""
Бот обучения менеджеров INSTINTO Academy.

Функции:
- Еженедельные отчёты по навыкам (понедельник 09:00 MSK)
- Микрообучение по слабым местам
- Тесты с записью прогресса в Google Sheets

Переменные окружения:
    TELEGRAM_BOT_TOKEN=...
    GOOGLE_SHEETS_ID=...
    GOOGLE_SERVICE_ACCOUNT_JSON=...
"""

from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

import asyncio
import json
import logging
import os
import traceback
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from sheets import open_spreadsheet, append_to_worksheet

# Импорт анализа чатов
from analyze_chats import main as run_analyze_chats

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Инициализация
bot = Bot(token=os.environ.get("TELEGRAM_BOT_TOKEN", ""))
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone="Europe/Moscow")

# Путь к модулям
MODULES_DIR = Path(__file__).parent / "modules"

# Названия навыков
SKILL_NAMES = {
    "greeting_score": "Приветствие",
    "needs_score": "Выявление потребностей",
    "presentation_score": "Презентация",
    "objection_score": "Работа с возражениями",
    "closing_score": "Закрытие сделки",
    "cross_sell_score": "Допродажа",
}

# Маппинг skill_key -> module_id
SKILL_TO_MODULE = {
    "needs_score": "needs_discovery",
}


def load_module(module_id: str) -> Dict[str, Any] | None:
    """Загружает модуль из JSON."""
    path = MODULES_DIR / f"{module_id}.json"
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_spreadsheet():
    """Подключается к Google Sheets."""
    sheets_id = os.environ.get("GOOGLE_SHEETS_ID")
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not sheets_id or not sa_json:
        raise ValueError("GOOGLE_SHEETS_ID или GOOGLE_SERVICE_ACCOUNT_JSON не заданы")
    return open_spreadsheet(spreadsheet_id=sheets_id, service_account_json_path=sa_json)


def load_analysis_data(ss, days: int = 7) -> List[Dict[str, Any]]:
    """Загружает данные анализа за последние N дней."""
    try:
        ws = ss.worksheet("analysis_raw")
        data = ws.get_all_records()
    except Exception as e:
        logger.error(f"Ошибка чтения analysis_raw: {e}")
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    filtered = []

    for row in data:
        analyzed_at = row.get("analyzed_at", "")
        if analyzed_at:
            try:
                dt = datetime.fromisoformat(analyzed_at.replace("Z", "+00:00"))
                if dt < cutoff:
                    continue
            except ValueError:
                pass
        filtered.append(row)

    return filtered


def aggregate_by_manager(data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Агрегирует оценки по менеджерам."""
    managers: Dict[str, Dict[str, Any]] = {}

    for row in data:
        manager_id = str(row.get("manager_id", "")).strip()
        if not manager_id:
            continue

        if manager_id not in managers:
            managers[manager_id] = {
                "manager_name": row.get("manager_name", "Неизвестный"),
                "chat_count": 0,
                "skills": defaultdict(list),
                "missed_opportunities": [],
            }

        m = managers[manager_id]
        m["chat_count"] += 1

        for skill_key in SKILL_NAMES:
            score = row.get(skill_key, 0)
            if score:
                try:
                    m["skills"][skill_key].append(float(score))
                except (ValueError, TypeError):
                    pass

        missed_raw = row.get("missed_opportunities", "")
        if missed_raw:
            try:
                missed = json.loads(missed_raw) if isinstance(missed_raw, str) else missed_raw
                if isinstance(missed, list):
                    m["missed_opportunities"].extend(missed[:3])
            except json.JSONDecodeError:
                pass

    return managers


def find_weakest_skills(skills: Dict[str, List[float]], top_n: int = 3) -> List[Tuple[str, float]]:
    """Находит N самых слабых навыков."""
    averages = {}
    for skill_key, scores in skills.items():
        if scores:
            averages[skill_key] = round(sum(scores) / len(scores), 1)

    non_zero = [(k, v) for k, v in averages.items() if v > 0]
    sorted_skills = sorted(non_zero, key=lambda x: x[1])
    return sorted_skills[:top_n]


def build_report_keyboard(weakest: List[Tuple[str, float]], manager_id: str) -> InlineKeyboardMarkup:
    """Создаёт клавиатуру с кнопками модулей."""
    buttons = []
    for skill_key, avg in weakest:
        module_id = SKILL_TO_MODULE.get(skill_key)
        if module_id:
            skill_name = SKILL_NAMES.get(skill_key, skill_key)
            buttons.append([
                InlineKeyboardButton(
                    text=f"Пройти: {skill_name}",
                    callback_data=f"learn:{module_id}:{manager_id}"
                )
            ])
    return InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None


def format_report(
    manager_name: str,
    chat_count: int,
    weakest: List[Tuple[str, float]],
    missed_examples: List[str],
) -> str:
    """Форматирует отчёт для Telegram."""
    lines = [
        f"<b>Твой отчёт за неделю</b>",
        "",
        f"Проанализировано чатов: {chat_count}",
        "",
        "<b>Точки роста:</b>",
    ]

    for i, (skill_key, avg) in enumerate(weakest, 1):
        skill_name = SKILL_NAMES.get(skill_key, skill_key)
        example = missed_examples[i - 1] if i - 1 < len(missed_examples) else ""

        lines.append(f"{i}. {skill_name} ({avg})")
        if example:
            example_short = example[:100] + "..." if len(example) > 100 else example
            lines.append(f"   <i>» {example_short}</i>")

    return "\n".join(lines)


async def send_weekly_reports():
    """Отправляет еженедельные отчёты всем менеджерам."""
    logger.info("Запуск еженедельных отчётов...")

    try:
        ss = get_spreadsheet()
        data = load_analysis_data(ss, days=7)

        if not data:
            logger.info("Нет данных за неделю")
            return

        managers = aggregate_by_manager(data)
        logger.info(f"Найдено менеджеров: {len(managers)}")

        # Для теста отправляем на TELEGRAM_CHAT_ID
        test_chat_id = os.environ.get("TELEGRAM_CHAT_ID")

        for manager_id, m in managers.items():
            weakest = find_weakest_skills(m["skills"], top_n=3)
            if not weakest:
                continue

            missed_examples = list(set(m["missed_opportunities"]))[:3]

            report = format_report(
                manager_name=m["manager_name"],
                chat_count=m["chat_count"],
                weakest=weakest,
                missed_examples=missed_examples,
            )

            keyboard = build_report_keyboard(weakest, manager_id)

            # Пока отправляем на тестовый chat_id
            # TODO: в продакшене — на telegram_chat_id менеджера
            if test_chat_id:
                await bot.send_message(
                    chat_id=test_chat_id,
                    text=f"<b>Менеджер:</b> {m['manager_name']}\n\n{report}",
                    parse_mode="HTML",
                    reply_markup=keyboard,
                )
                logger.info(f"Отчёт отправлен: {m['manager_name']}")

    except Exception as e:
        logger.error(f"Ошибка отправки отчётов: {e}\n{traceback.format_exc()}")


# === Обработчики команд ===

@dp.message(Command("start"))
async def cmd_start(message: Message):
    """Приветствие."""
    await message.answer(
        "Привет! Я бот обучения INSTINTO Academy.\n\n"
        "Каждый понедельник ты получаешь отчёт по навыкам с рекомендациями.\n\n"
        "Команды:\n"
        "/report — получить отчёт сейчас\n"
        "/learn — список модулей обучения"
    )


@dp.message(Command("report"))
async def cmd_report(message: Message):
    """Ручной запуск отчёта (для теста)."""
    await message.answer("Формирую отчёт...")
    await send_weekly_reports()


@dp.message(Command("learn"))
async def cmd_learn(message: Message):
    """Список доступных модулей."""
    modules = []
    for path in MODULES_DIR.glob("*.json"):
        module = load_module(path.stem)
        if module:
            modules.append(module)

    if not modules:
        await message.answer("Модули пока не добавлены.")
        return

    buttons = []
    for m in modules:
        buttons.append([
            InlineKeyboardButton(
                text=m["title"],
                callback_data=f"learn:{m['id']}:manual"
            )
        ])

    await message.answer(
        "Доступные модули обучения:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


# === Обработчики callback ===

@dp.callback_query(F.data.startswith("learn:"))
async def cb_start_lesson(callback: CallbackQuery):
    """Начало урока."""
    parts = callback.data.split(":")
    module_id = parts[1]
    manager_id = parts[2] if len(parts) > 2 else "unknown"

    module = load_module(module_id)
    if not module:
        await callback.answer("Модуль не найден", show_alert=True)
        return

    # Записываем начало обучения
    try:
        ss = get_spreadsheet()
        append_to_worksheet(
            ss, "learning_progress",
            rows=[[
                manager_id,
                module_id,
                datetime.now(timezone.utc).isoformat(),
                "",  # completed_at
                "",  # quiz_correct
                "",  # quiz_answer
            ]],
            header=["manager_id", "module_id", "started_at", "completed_at", "quiz_correct", "quiz_answer"]
        )
    except Exception as e:
        logger.error(f"Ошибка записи прогресса: {e}")

    await callback.answer()

    # Отправляем урок
    await callback.message.answer(
        f"<b>{module['title']}</b>\n\n{module['content']}",
        parse_mode="HTML"
    )

    # Через 2 секунды — тест
    await asyncio.sleep(2)

    quiz = module.get("quiz")
    if quiz:
        buttons = []
        for i, option in enumerate(quiz["options"]):
            buttons.append([
                InlineKeyboardButton(
                    text=option,
                    callback_data=f"quiz:{module_id}:{manager_id}:{i}"
                )
            ])

        await callback.message.answer(
            f"<b>Тест:</b>\n\n{quiz['question']}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )


@dp.callback_query(F.data.startswith("quiz:"))
async def cb_quiz_answer(callback: CallbackQuery):
    """Ответ на тест."""
    parts = callback.data.split(":")
    module_id = parts[1]
    manager_id = parts[2]
    answer_idx = int(parts[3])

    module = load_module(module_id)
    if not module or "quiz" not in module:
        await callback.answer("Ошибка теста", show_alert=True)
        return

    quiz = module["quiz"]
    correct = quiz["correct"]
    is_correct = answer_idx == correct

    # Записываем результат
    try:
        ss = get_spreadsheet()
        # Обновляем последнюю запись (упрощённо — добавляем новую)
        append_to_worksheet(
            ss, "learning_progress",
            rows=[[
                manager_id,
                module_id,
                "",  # started_at (уже записано)
                datetime.now(timezone.utc).isoformat(),
                "1" if is_correct else "0",
                str(answer_idx),
            ]],
            header=["manager_id", "module_id", "started_at", "completed_at", "quiz_correct", "quiz_answer"]
        )
    except Exception as e:
        logger.error(f"Ошибка записи результата: {e}")

    await callback.answer()

    if is_correct:
        await callback.message.edit_text(
            f"<b>Правильно!</b>\n\n"
            f"Ты выбрал: {quiz['options'][answer_idx]}\n\n"
            f"Модуль «{module['title']}» пройден.",
            parse_mode="HTML"
        )
    else:
        correct_answer = quiz['options'][correct]
        await callback.message.edit_text(
            f"<b>Неверно</b>\n\n"
            f"Ты выбрал: {quiz['options'][answer_idx]}\n"
            f"Правильный ответ: {correct_answer}\n\n"
            f"Попробуй применить это в следующем диалоге!",
            parse_mode="HTML"
        )


async def run_analyze_chats_async():
    """Запуск анализа чатов в отдельном потоке."""
    logger.info("Запуск анализа чатов...")
    try:
        await asyncio.to_thread(run_analyze_chats)
        logger.info("Анализ чатов завершён")
    except Exception as e:
        logger.error(f"Ошибка анализа чатов: {e}\n{traceback.format_exc()}")


async def main():
    """Запуск бота."""
    logger.info("Запуск бота INSTINTO Academy...")

    # Планировщик: анализ чатов каждый час
    scheduler.add_job(
        run_analyze_chats_async,
        CronTrigger(hour="*", minute=0),
        id="analyze_chats"
    )

    # Планировщик: еженедельные отчёты (понедельник 09:00 MSK)
    scheduler.add_job(
        send_weekly_reports,
        CronTrigger(day_of_week="mon", hour=9, minute=0),
        id="weekly_reports"
    )

    scheduler.start()
    logger.info("Планировщик запущен: анализ каждый час, отчёты пн 09:00 MSK")

    # Запуск polling
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
