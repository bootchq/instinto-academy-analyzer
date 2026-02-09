"""
Ежедневная сводка по менеджерам.
Отправляется админам каждое утро в 10:00 МСК (07:00 UTC).

Переменные окружения:
    GOOGLE_SHEETS_ID
    GOOGLE_SERVICE_ACCOUNT_JSON
    ALERT_BOT_TOKEN
    TELEGRAM_CHAT_ID (ADMIN_ID)
"""

import os
from datetime import datetime, timedelta
from collections import defaultdict

# Устанавливаем Moscow timezone
os.environ['TZ'] = 'Europe/Moscow'
from shared import time_utils
from shared.sheets_academy import open_spreadsheet
from shared.alerting import send_telegram, ADMIN_ID, alert_error


def main():
    try:
        sheets_id = os.environ.get("GOOGLE_SHEETS_ID")
        sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        if not sheets_id or not sa_json:
            raise ValueError("GOOGLE_SHEETS_ID или GOOGLE_SERVICE_ACCOUNT_JSON не заданы")

        print("Подключаюсь к Google Sheets...")
        ss = open_spreadsheet(spreadsheet_id=sheets_id, service_account_json_path=sa_json)
        ws = ss.worksheet("analysis_raw")
        data = ws.get_all_records()

        # Берём анализы за вчера (последние 24 часа)
        yesterday = time_utils.today() - timedelta(days=1)
        cutoff = datetime.combine(yesterday, datetime.min.time())

        recent_results = []
        for row in data:
            analyzed_str = row.get("analyzed_at", "")
            if not analyzed_str:
                continue
            try:
                analyzed = datetime.fromisoformat(analyzed_str.replace("Z", "+00:00"))
                if analyzed.replace(tzinfo=None) >= cutoff:
                    recent_results.append(row)
            except:
                continue

        if not recent_results:
            print("Нет результатов за последние 24 часа")
            return

        # Группируем по менеджерам
        by_manager = defaultdict(list)
        for r in recent_results:
            name = str(r.get("manager_name", "")).strip() or str(r.get("manager_id", "")).strip() or "Неизвестный"
            by_manager[name].append(r)

        # Формируем сводку
        skill_labels = {
            "greeting_score": "Привет",
            "needs_score": "Потреб",
            "presentation_score": "Презент",
            "objection_score": "Возраж",
            "closing_score": "Закрыт",
            "cross_sell_score": "Допрод",
        }
        skill_keys = list(skill_labels.keys())

        lines = [f"<b>📊 Анализ чатов за {yesterday.strftime('%d.%m.%Y')}</b>\n"]

        for mgr_name, mgr_results in sorted(by_manager.items(), key=lambda x: -len(x[1])):
            avgs = {}
            for sk in skill_keys:
                vals = [float(r.get(sk, 0)) / 10 for r in mgr_results if r.get(sk) and float(r.get(sk, 0)) > 0]
                avgs[sk] = round(sum(vals) / len(vals), 1) if vals else 0

            overall_vals = [float(r.get("overall_score", 0)) / 10 for r in mgr_results if r.get("overall_score") and float(r.get("overall_score", 0)) > 10]
            overall = round(sum(overall_vals) / len(overall_vals), 1) if overall_vals else 0

            lines.append(f"<b>{mgr_name}</b>: {len(mgr_results)} чатов, общая оценка: {overall}/10")
            scores_str = " | ".join(f"{skill_labels[sk]}: {avgs[sk]}" for sk in skill_keys if avgs[sk] > 0)
            if scores_str:
                lines.append(f"  {scores_str}")
            lines.append("")

        lines.append(f"Всего проанализировано: {len(recent_results)} чатов")

        send_telegram(ADMIN_ID, "\n".join(lines))
        print(f"Сводка отправлена: {len(recent_results)} чатов, {len(by_manager)} менеджеров")

    except Exception as e:
        alert_error(
            service_name="send-daily-summary",
            error_message=str(e),
            traceback_str=""
        )
        raise


if __name__ == "__main__":
    main()
