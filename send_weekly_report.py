"""
Еженедельный отчёт по навыкам менеджеров.

Читает analysis_raw, агрегирует оценки, отправляет в Telegram.

Использование:
    python send_weekly_report.py

Переменные окружения:
    GOOGLE_SHEETS_ID=...
    GOOGLE_SERVICE_ACCOUNT_JSON=...
    TELEGRAM_BOT_TOKEN=...
    TELEGRAM_CHAT_ID=...
"""

from __future__ import annotations

import json
import os
import traceback
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Tuple, Optional

import requests

from sheets import open_spreadsheet, get_all_users


# ID админа для сводного отчёта
ADMIN_CHAT_ID = "57186925"

# Названия навыков для отчёта
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


def load_analysis_data(ss, days: int = 7) -> List[Dict[str, Any]]:
    """
    Загружает данные анализа за последние N дней.
    """
    try:
        ws = ss.worksheet("analysis_raw")
        data = ws.get_all_records()
    except Exception as e:
        print(f"Ошибка чтения analysis_raw: {e}")
        return []

    # Фильтруем по дате (если есть analyzed_at)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    filtered = []

    for row in data:
        analyzed_at = row.get("analyzed_at", "")
        if analyzed_at:
            try:
                # ISO формат: 2026-01-29T12:00:00+00:00
                dt = datetime.fromisoformat(analyzed_at.replace("Z", "+00:00"))
                if dt < cutoff:
                    continue
            except ValueError:
                pass  # Если не распарсили — включаем

        filtered.append(row)

    return filtered


def aggregate_by_manager(data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Агрегирует оценки по менеджерам.

    Возвращает:
    {
        "manager_id": {
            "manager_name": "Имя",
            "chat_count": 10,
            "skills": {
                "greeting_score": [7, 8, 6, ...],
                ...
            },
            "missed_opportunities": ["пример1", "пример2", ...]
        }
    }
    """
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

        # Собираем оценки
        for skill_key in SKILL_NAMES:
            score = row.get(skill_key, 0)
            if score:
                try:
                    m["skills"][skill_key].append(float(score))
                except (ValueError, TypeError):
                    pass

        # Собираем упущенные возможности
        missed_raw = row.get("missed_opportunities", "")
        if missed_raw:
            try:
                missed = json.loads(missed_raw) if isinstance(missed_raw, str) else missed_raw
                if isinstance(missed, list):
                    m["missed_opportunities"].extend(missed[:3])  # Берём первые 3
            except json.JSONDecodeError:
                pass

    return managers


def calculate_skill_averages(skills: Dict[str, List[float]]) -> Dict[str, float]:
    """Считает средние по навыкам."""
    result = {}
    for skill_key, scores in skills.items():
        if scores:
            result[skill_key] = round(sum(scores) / len(scores), 1)
        else:
            result[skill_key] = 0.0
    return result


def find_weakest_skills(averages: Dict[str, float], top_n: int = 3) -> List[Tuple[str, float]]:
    """
    Находит N самых слабых навыков.
    Возвращает список (skill_key, average).
    """
    # Фильтруем нулевые (нет данных)
    non_zero = [(k, v) for k, v in averages.items() if v > 0]
    # Сортируем по возрастанию (слабые сначала)
    sorted_skills = sorted(non_zero, key=lambda x: x[1])
    return sorted_skills[:top_n]


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
            # Обрезаем длинные примеры
            example_short = example[:100] + "..." if len(example) > 100 else example
            lines.append(f"   <i>» {example_short}</i>")

    return "\n".join(lines)


def build_user_mapping(ss) -> Dict[str, str]:
    """
    Создаёт маппинг manager_name -> telegram_id из таблицы users.
    Возвращает только approved пользователей с ролью manager.
    """
    users = get_all_users(ss)
    mapping = {}

    for user in users:
        if user.get("status") == "approved" and user.get("role") == "manager":
            name = user.get("name", "").strip()
            tid = user.get("telegram_id", "").strip()
            if name and tid:
                mapping[name] = tid
                # Также добавляем по username если есть
                username = user.get("username", "").strip()
                if username:
                    mapping[username] = tid

    return mapping


def main():
    """Основная функция."""
    telegram = TelegramNotifier(
        bot_token=os.environ.get("TELEGRAM_BOT_TOKEN", "")
    )

    try:
        # Проверяем переменные
        sheets_id = os.environ.get("GOOGLE_SHEETS_ID")
        sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        if not sheets_id or not sa_json:
            raise ValueError("GOOGLE_SHEETS_ID или GOOGLE_SERVICE_ACCOUNT_JSON не заданы")

        print("Подключаюсь к Google Sheets...")
        ss = open_spreadsheet(spreadsheet_id=sheets_id, service_account_json_path=sa_json)

        # Загружаем маппинг пользователей
        print("Загружаю маппинг пользователей...")
        user_mapping = build_user_mapping(ss)
        print(f"   Найдено менеджеров с telegram_id: {len(user_mapping)}")

        print("Загружаю данные анализа за 7 дней...")
        data = load_analysis_data(ss, days=7)
        print(f"   Найдено записей: {len(data)}")

        if not data:
            telegram.send(ADMIN_CHAT_ID, "Еженедельный отчёт: нет данных за последние 7 дней")
            print("Нет данных для отчёта")
            return

        print("Агрегирую по менеджерам...")
        managers = aggregate_by_manager(data)
        print(f"   Менеджеров в данных: {len(managers)}")

        if not managers:
            telegram.send(ADMIN_CHAT_ID, "Еженедельный отчёт: нет данных по менеджерам")
            return

        reports_sent = 0
        admin_summary = ["<b>Сводка еженедельных отчётов</b>\n"]

        for manager_id, m in managers.items():
            manager_name = m["manager_name"]

            # Считаем средние
            averages = calculate_skill_averages(m["skills"])

            # Находим слабые места
            weakest = find_weakest_skills(averages, top_n=3)

            if not weakest:
                print(f"   {manager_name}: недостаточно данных для отчёта")
                continue

            # Берём примеры упущенных возможностей
            missed_examples = list(set(m["missed_opportunities"]))[:3]

            # Формируем отчёт
            report = format_report(
                manager_name=manager_name,
                chat_count=m["chat_count"],
                weakest=weakest,
                missed_examples=missed_examples,
            )

            # Собираем skill_keys из слабых мест для кнопок модулей
            skill_keys = [skill_key for skill_key, _ in weakest]

            # Ищем telegram_id менеджера
            manager_tid = user_mapping.get(manager_name)

            if manager_tid:
                # Отправляем персональный отчёт менеджеру
                if telegram.send_with_module_buttons(manager_tid, report, skill_keys):
                    reports_sent += 1
                    admin_summary.append(f"✅ {manager_name}: отправлен")
                    print(f"   {manager_name}: отчёт отправлен на {manager_tid}")
                else:
                    admin_summary.append(f"❌ {manager_name}: ошибка отправки")
                    print(f"   {manager_name}: ОШИБКА отправки")
            else:
                # Менеджер не зарегистрирован в боте
                admin_summary.append(f"⚠️ {manager_name}: не зарегистрирован в боте")
                print(f"   {manager_name}: не найден в users (нужна регистрация)")

        # Отправляем сводку админу
        admin_summary.append(f"\nВсего отправлено: {reports_sent}")
        telegram.send(ADMIN_CHAT_ID, "\n".join(admin_summary))

        print(f"\nОтправлено персональных отчётов: {reports_sent}")

    except Exception as e:
        error_msg = f"<b>Ошибка еженедельного отчёта</b>\n\n<pre>{traceback.format_exc()[-500:]}</pre>"
        telegram.send(ADMIN_CHAT_ID, error_msg)
        print(f"Критическая ошибка: {e}")
        raise


if __name__ == "__main__":
    main()
