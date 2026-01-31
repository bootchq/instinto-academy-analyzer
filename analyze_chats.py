"""
Анализ чатов менеджеров через Groq (Llama 3.3).
Версия для Railway с Telegram уведомлениями.

Логика повторного анализа:
- Новый чат → анализируем
- Появились новые сообщения → переанализируем
- Изменился статус (оплачен/отменён/закрыт) → переанализируем

Переменные окружения:
    GROQ_API_KEY=gsk_...
    GOOGLE_SHEETS_ID=1to83Pw9vjl6p1RnnrJT-qtHc85x5s2U_qYp6jSZKhYM
    GOOGLE_SERVICE_ACCOUNT_JSON={"type":"service_account",...}  # JSON строка
    TELEGRAM_BOT_TOKEN=...      # Для уведомлений
    TELEGRAM_CHAT_ID=...        # Куда слать уведомления
"""

from __future__ import annotations

import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# Лимит чатов за один запуск (при 60с паузе = 10 минут)
MAX_CHATS_PER_RUN = 10

import requests

from shared.sheets_academy import open_spreadsheet, upsert_worksheet, append_to_worksheet, dicts_to_table


# Промпт для анализа чата
ANALYSIS_PROMPT = """Ты эксперт по продажам премиального женского белья бренда INSTINTO.
Проанализируй диалог менеджера с клиентом.

КОНТЕКСТ БРЕНДА:
- Премиальное женское бельё, dark luxury
- Ценовой сегмент: средний+
- Целевая аудитория: женщины 25-45

СЕГМЕНТЫ КЛИЕНТОВ:
1. Невесты - подготовка к свадьбе
2. После родов - возвращение к себе
3. Пары - вернуть искру
4. Экспериментаторы - новые ощущения
5. Подарки - ищут подарок
6. Новая версия себя - трансформация
7. Соло - муж в отъезде
8. Путешественницы - для поездок

ЭТАПЫ ПРОДАЖИ:
1. Приветствие и установление контакта
2. Выявление потребностей
3. Презентация продукта
4. Работа с возражениями
5. Закрытие сделки
6. Допродажа (cross-sell)

ЗАДАЧА:
1. Определи сегмент клиента по сигналам из диалога
2. Оцени каждый этап продажи (1-10, где 10 = идеально)
3. Выдели техники, которые использовал менеджер
4. Укажи упущенные возможности
5. Проверь на манипулятивные практики (давление, ложная срочность)

ДИАЛОГ:
{dialog}

Ответь ТОЛЬКО в JSON формате (без markdown):
{{
  "customer_segment": "название сегмента или unknown",
  "customer_signals": ["сигнал 1", "сигнал 2"],
  "scores": {{
    "greeting": 7,
    "needs_discovery": 5,
    "presentation": 6,
    "objection_handling": 4,
    "closing": 6,
    "cross_sell": 3
  }},
  "overall_score": 5.2,
  "techniques_used": [
    {{"technique": "название", "example": "цитата из диалога"}}
  ],
  "missed_opportunities": ["что можно было сделать лучше"],
  "manipulation_flags": [],
  "is_ethical": true,
  "summary": "Краткое резюме диалога в 1-2 предложения"
}}"""


class TelegramNotifier:
    """Отправляет уведомления в Telegram."""

    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"

    def send(self, message: str) -> bool:
        """Отправить сообщение."""
        if not self.bot_token or not self.chat_id:
            return False
        try:
            resp = requests.post(
                f"{self.base_url}/sendMessage",
                json={"chat_id": self.chat_id, "text": message, "parse_mode": "HTML"},
                timeout=10
            )
            return resp.status_code == 200
        except Exception:
            return False


class GroqClient:
    """Клиент для Groq API."""

    BASE_URL = "https://api.groq.com/openai/v1/chat/completions"

    def __init__(self, api_key: str, model: str = "llama-3.1-8b-instant"):
        self.api_key = api_key
        self.model = model
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        })

    def chat(self, prompt: str, max_tokens: int = 2000) -> str:
        """Отправить запрос к Groq."""
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "temperature": 0.3,
        }

        # Больше попыток с экспоненциальным backoff для rate limit
        for attempt in range(5):
            try:
                resp = self.session.post(self.BASE_URL, json=payload, timeout=90)

                if resp.status_code == 429:
                    # Логируем headers для диагностики
                    retry_after = resp.headers.get("retry-after", "?")
                    limit_requests = resp.headers.get("x-ratelimit-limit-requests", "?")
                    remaining = resp.headers.get("x-ratelimit-remaining-requests", "?")
                    reset = resp.headers.get("x-ratelimit-reset-requests", "?")
                    print(f"  Rate limit headers: retry={retry_after}s, limit={limit_requests}, remaining={remaining}, reset={reset}")

                    # Экспоненциальный backoff: 60, 120, 240, 480 секунд
                    wait = 60 * (2 ** attempt)
                    print(f"  Жду {wait}с (попытка {attempt + 1}/5)...")
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                data = resp.json()
                return data["choices"][0]["message"]["content"]

            except requests.exceptions.RequestException as e:
                if attempt < 4:
                    time.sleep(10 * (attempt + 1))
                    continue
                raise RuntimeError(f"Groq API error: {e}")

        raise RuntimeError("Groq API: превышено число попыток")


def format_dialog(messages: List[Dict[str, Any]]) -> str:
    """Форматирует сообщения в текст диалога."""
    lines = []
    for msg in messages:
        direction = msg.get("direction", "")
        text = msg.get("text", "").strip()
        if not text:
            continue

        role = "Клиент" if direction == "in" else "Менеджер"
        lines.append(f"{role}: {text}")

    return "\n".join(lines)


def parse_llm_response(response: str) -> Optional[Dict[str, Any]]:
    """Парсит JSON из ответа LLM."""
    import re
    text = response.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r'\{[\s\S]*\}', text)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
    return None


def load_chats_from_sheets(ss, limit: int = 50) -> List[Dict[str, Any]]:
    """Загружает чаты и сообщения из Google Sheets."""

    # Заголовки для expected_headers (фикс для дубликатов)
    chats_header = [
        "chat_id", "channel", "manager_id", "manager_name", "client_id", "order_id",
        "has_order", "payment_status", "payment_status_ru", "is_successful",
        "order_count", "status", "created_at", "outcome"
    ]
    messages_header = ["chat_id", "message_id", "sent_at", "direction", "manager_id", "text"]

    try:
        chats_ws = ss.worksheet("chats_raw")
        chats_data = chats_ws.get_all_records(expected_headers=chats_header)
    except Exception as e:
        print(f"Ошибка чтения chats_raw: {e}")
        return []

    try:
        messages_ws = ss.worksheet("messages_raw")
        messages_data = messages_ws.get_all_records(expected_headers=messages_header)
    except Exception as e:
        print(f"Ошибка чтения messages_raw: {e}")
        return []

    messages_by_chat: Dict[str, List[Dict]] = {}
    for msg in messages_data:
        chat_id = str(msg.get("chat_id", ""))
        if chat_id:
            messages_by_chat.setdefault(chat_id, []).append(msg)

    result = []
    for chat in chats_data[:limit]:
        chat_id = str(chat.get("chat_id", ""))
        if not chat_id:
            continue

        messages = messages_by_chat.get(chat_id, [])
        if len(messages) < 2:
            continue

        messages.sort(key=lambda m: m.get("sent_at", ""))

        result.append({
            "chat_id": chat_id,
            "chat": chat,
            "messages": messages
        })

    return result


def load_analyzed_chats(ss) -> Dict[str, Dict[str, Any]]:
    """
    Загружает данные о проанализированных чатах.
    Возвращает dict: chat_id -> {message_count, chat_status, row_index}
    """
    try:
        ws = ss.worksheet("analysis_raw")
        data = ws.get_all_records()
        result = {}
        for i, row in enumerate(data):
            chat_id = str(row.get("chat_id", ""))
            if chat_id:
                result[chat_id] = {
                    "message_count": int(row.get("message_count", 0)),
                    "chat_status": str(row.get("chat_status", "")),
                    "row_index": i + 2,  # +2: заголовок + 0-based index
                }
        return result
    except Exception:
        return {}


def needs_reanalysis(chat_id: str, current_msg_count: int, current_status: str,
                     analyzed: Dict[str, Dict]) -> Tuple[bool, str]:
    """
    Проверяет, нужен ли повторный анализ.
    Возвращает (нужен_ли, причина).
    """
    if chat_id not in analyzed:
        return True, "новый"

    prev = analyzed[chat_id]
    prev_count = prev.get("message_count", 0)
    prev_status = prev.get("chat_status", "")

    if current_msg_count > prev_count:
        return True, f"новые сообщения ({prev_count}→{current_msg_count})"

    if current_status != prev_status and current_status:
        return True, f"статус изменён ({prev_status}→{current_status})"

    return False, ""


def main():
    # Настройка уведомлений
    telegram = TelegramNotifier(
        bot_token=os.environ.get("TELEGRAM_BOT_TOKEN", ""),
        chat_id=os.environ.get("TELEGRAM_CHAT_ID", "")
    )

    try:
        # Проверяем ключи
        groq_key = os.environ.get("GROQ_API_KEY")
        if not groq_key:
            raise ValueError("GROQ_API_KEY не задан")

        sheets_id = os.environ.get("GOOGLE_SHEETS_ID")
        sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        if not sheets_id or not sa_json:
            raise ValueError("GOOGLE_SHEETS_ID или GOOGLE_SERVICE_ACCOUNT_JSON не заданы")

        # Подключаемся
        print("Подключаюсь к Google Sheets...")
        ss = open_spreadsheet(spreadsheet_id=sheets_id, service_account_json_path=sa_json)

        print("Инициализирую Groq...")
        groq = GroqClient(groq_key)

        # Загружаем чаты
        print("Загружаю чаты...")
        chats = load_chats_from_sheets(ss, limit=200)
        print(f"   Найдено чатов: {len(chats)}")

        analyzed = load_analyzed_chats(ss)
        print(f"   Уже проанализировано: {len(analyzed)}")

        # Фильтруем: новые + изменённые
        chats_to_analyze = []
        for c in chats:
            chat_id = c["chat_id"]
            msg_count = len(c["messages"])
            chat_status = c["chat"].get("status", "") or c["chat"].get("outcome", "")

            need, reason = needs_reanalysis(chat_id, msg_count, chat_status, analyzed)
            if need:
                c["reanalysis_reason"] = reason
                c["message_count"] = msg_count
                c["chat_status"] = chat_status
                chats_to_analyze.append(c)

        total_to_analyze = len(chats_to_analyze)
        new_count = sum(1 for c in chats_to_analyze if c["reanalysis_reason"] == "новый")
        updated_count = total_to_analyze - new_count
        print(f"   Всего для анализа: {total_to_analyze} (новых: {new_count}, обновлённых: {updated_count})")

        if not chats_to_analyze:
            telegram.send("Академия INSTINTO: новых/изменённых чатов нет")
            print("Нет чатов для анализа!")
            return

        # Ограничиваем количество за один запуск (Groq rate limit)
        if len(chats_to_analyze) > MAX_CHATS_PER_RUN:
            print(f"   Ограничиваю до {MAX_CHATS_PER_RUN} чатов (остальные в следующий раз)")
            chats_to_analyze = chats_to_analyze[:MAX_CHATS_PER_RUN]

        # Анализируем
        results = []
        errors = 0
        for i, item in enumerate(chats_to_analyze, 1):
            chat_id = item["chat_id"]
            chat = item["chat"]
            messages = item["messages"]
            reason = item["reanalysis_reason"]

            print(f"\n[{i}/{len(chats_to_analyze)}] Анализирую чат {chat_id} ({reason})...")

            dialog_text = format_dialog(messages)
            if len(dialog_text) < 50:
                print(f"  Пропускаю — слишком короткий диалог")
                continue

            if len(dialog_text) > 8000:
                dialog_text = dialog_text[:8000] + "\n[...диалог обрезан...]"

            prompt = ANALYSIS_PROMPT.format(dialog=dialog_text)

            try:
                response = groq.chat(prompt)
                # Логируем первые 200 символов для диагностики
                print(f"  LLM ответ (начало): {response[:200]}...")
                analysis = parse_llm_response(response)

                if not analysis:
                    print(f"  Ошибка парсинга ответа LLM. Полный ответ: {response[:500]}")
                    errors += 1
                    continue

                scores = analysis.get("scores", {})
                result = {
                    "chat_id": chat_id,
                    "manager_id": chat.get("manager_id", ""),
                    "manager_name": chat.get("manager_name", ""),
                    "channel": chat.get("channel", ""),
                    "message_count": item["message_count"],
                    "chat_status": item["chat_status"],
                    "customer_segment": analysis.get("customer_segment", "unknown"),
                    "overall_score": analysis.get("overall_score", 0),
                    "greeting_score": scores.get("greeting", 0),
                    "needs_score": scores.get("needs_discovery", 0),
                    "presentation_score": scores.get("presentation", 0),
                    "objection_score": scores.get("objection_handling", 0),
                    "closing_score": scores.get("closing", 0),
                    "cross_sell_score": scores.get("cross_sell", 0),
                    "techniques": json.dumps(analysis.get("techniques_used", []), ensure_ascii=False),
                    "missed_opportunities": json.dumps(analysis.get("missed_opportunities", []), ensure_ascii=False),
                    "is_ethical": analysis.get("is_ethical", True),
                    "summary": analysis.get("summary", ""),
                    "analyzed_at": datetime.now(timezone.utc).isoformat(),
                }
                results.append(result)

                print(f"  Сегмент: {result['customer_segment']}, оценка: {result['overall_score']}")
                # Пауза 60с между запросами (Groq rate limit на бесплатном плане)
                time.sleep(60)

            except Exception as e:
                print(f"  Ошибка: {e}")
                errors += 1
                continue

        # Записываем результаты
        if results:
            print(f"\nЗаписываю {len(results)} результатов в Google Sheets...")

            header = [
                "chat_id", "manager_id", "manager_name", "channel",
                "message_count", "chat_status",
                "customer_segment", "overall_score",
                "greeting_score", "needs_score", "presentation_score",
                "objection_score", "closing_score", "cross_sell_score",
                "techniques", "missed_opportunities", "is_ethical", "summary",
                "analyzed_at"
            ]

            rows = dicts_to_table(results, header=header)
            append_to_worksheet(ss, "analysis_raw", rows=rows[1:], header=header)

            # Уведомление об успехе
            remaining = total_to_analyze - len(chats_to_analyze)
            msg = (
                f"<b>Академия INSTINTO</b>\n\n"
                f"Анализ завершён:\n"
                f"- Проанализировано: {len(results)}\n"
                f"- Ошибок: {errors}\n"
                f"- Осталось: {remaining} чатов"
            )
            telegram.send(msg)
            print("Готово!")
        else:
            telegram.send("Академия INSTINTO: анализ завершён, но результатов нет (ошибки парсинга)")
            print("Нет результатов для записи")

    except Exception as e:
        # Уведомление об ошибке
        error_msg = f"<b>Академия INSTINTO</b>\n\nОшибка анализа:\n<pre>{traceback.format_exc()[-500:]}</pre>"
        telegram.send(error_msg)
        print(f"Критическая ошибка: {e}")
        raise


if __name__ == "__main__":
    main()
