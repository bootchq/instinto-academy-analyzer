"""
Еженедельное обучение менеджеров.
ТОЛЬКО МОДУЛИ С КНОПКАМИ (без подробной аналитики).

Отправляет короткое сообщение с кнопками модулей по слабым навыкам.

Использование:
    python send_obuchenie.py

Переменные окружения:
    GOOGLE_SHEETS_ID=...
    GOOGLE_SERVICE_ACCOUNT_JSON=...
    TELEGRAM_BOT_TOKEN=...
"""

from __future__ import annotations

import os
import traceback
from typing import List

from shared.telegram_notifier import TelegramNotifier, SKILL_NAMES
from shared.report_formatter import calculate_skill_averages, find_weakest_skills
from shared.sheets_academy import open_spreadsheet

# Переиспользуем функции загрузки данных из send_reports
from send_reports import load_analysis_data, aggregate_by_manager, build_user_mapping


# ID админа для уведомлений
ADMIN_CHAT_ID = "57186925"


def format_learning_message(manager_name: str, weakest_skills: List[str]) -> str:
    """Форматирует короткое обучающее сообщение."""
    skill_names = [SKILL_NAMES.get(sk, sk) for sk in weakest_skills]

    lines = [
        f"<b>{manager_name}, время развиваться!</b>",
        "",
        "На основе анализа твоих чатов за неделю, предлагаю пройти модули:",
    ]

    for i, name in enumerate(skill_names, 1):
        lines.append(f"{i}. {name}")

    lines.append("")
    lines.append("<i>Нажми на кнопку ниже, чтобы начать обучение</i>")

    return "\n".join(lines)


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
            telegram.send(ADMIN_CHAT_ID, "Еженедельное обучение: нет данных за последние 7 дней")
            print("Нет данных для обучения")
            return

        print("Агрегирую по менеджерам...")
        managers = aggregate_by_manager(data)
        print(f"   Менеджеров в данных: {len(managers)}")

        if not managers:
            telegram.send(ADMIN_CHAT_ID, "Еженедельное обучение: нет данных по менеджерам")
            return

        modules_sent = 0
        admin_summary = ["<b>Сводка еженедельного обучения</b>\n"]

        for manager_id, m in managers.items():
            manager_name = m["manager_name"]

            # Считаем средние
            averages = calculate_skill_averages(m["skills"])

            # Находим слабые места
            weakest = find_weakest_skills(averages, top_n=3)

            if not weakest:
                print(f"   {manager_name}: недостаточно данных для обучения")
                continue

            # Собираем skill_keys для кнопок модулей
            skill_keys = [skill_key for skill_key, _ in weakest]

            # Формируем обучающее сообщение
            message = format_learning_message(manager_name, skill_keys)

            # Ищем telegram_id менеджера
            manager_tid = user_mapping.get(manager_name)

            if manager_tid:
                # Отправляем С КНОПКАМИ модулей
                if telegram.send_with_module_buttons(manager_tid, message, skill_keys):
                    modules_sent += 1
                    admin_summary.append(f"✅ {manager_name}: отправлен")
                    print(f"   {manager_name}: модули отправлены на {manager_tid}")
                else:
                    admin_summary.append(f"❌ {manager_name}: ошибка отправки")
                    print(f"   {manager_name}: ОШИБКА отправки")
            else:
                # Менеджер не зарегистрирован в боте
                admin_summary.append(f"⚠️ {manager_name}: не зарегистрирован в боте")
                print(f"   {manager_name}: не найден в users (нужна регистрация)")

        # Отправляем сводку админу
        admin_summary.append(f"\nВсего отправлено: {modules_sent}")
        telegram.send(ADMIN_CHAT_ID, "\n".join(admin_summary))

        print(f"\nОтправлено модулей обучения: {modules_sent}")

        # Уведомление об успехе через централизованную систему алертов
        from shared.alerting import alert_success
        alert_success(
            service_name="send-weeks-obuchenie",
            message="Модули обучения отправлены",
            stats={
                "Отправлено модулей": modules_sent,
                "Всего менеджеров": len(managers)
            }
        )

    except Exception as e:
        # Уведомление об ошибке через централизованную систему алертов
        from shared.alerting import alert_error
        alert_error(
            service_name="send-weeks-obuchenie",
            error=e,
            context="Ошибка отправки модулей обучения"
        )
        raise


if __name__ == "__main__":
    main()
