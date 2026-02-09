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
import time

# Устанавливаем Moscow timezone для всех datetime операций
os.environ['TZ'] = 'Europe/Moscow'
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from shared import time_utils

# Лимит чатов за один запуск (переопределяется через env MAX_CHATS)
MAX_CHATS_PER_RUN = int(os.environ.get("MAX_CHATS", "10"))

import requests

from shared.sheets_academy import open_spreadsheet, append_to_worksheet, dicts_to_table


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
        text = str(msg.get("text", "")).strip()
        if not text:
            continue

        # Поддержка маркера пропуска
        if direction == "system":
            lines.append(f"\n{text}\n")
        elif direction == "in":
            lines.append(f"Клиент: {text}")
        else:
            lines.append(f"Менеджер: {text}")

    return "\n".join(lines)


def smart_truncate_messages(messages: List[Dict[str, Any]], max_messages: int = 50) -> List[Dict[str, Any]]:
    """
    Умная обрезка сообщений: сохраняет начало и конец диалога.

    Args:
        messages: Список сообщений
        max_messages: Максимальное количество сообщений (по умолчанию 50)

    Returns:
        Обрезанный список сообщений с маркером пропуска
    """
    if len(messages) <= max_messages:
        return messages

    # Берём первые 5 и последние 45
    first_n = 5
    last_n = max_messages - first_n

    head = messages[:first_n]
    tail = messages[-last_n:]
    skipped_count = len(messages) - first_n - last_n

    # Вставляем маркер пропуска
    skip_marker = {
        "direction": "system",
        "text": f"[...пропущено {skipped_count} сообщений...]"
    }

    return head + [skip_marker] + tail


def parse_llm_response(response: str) -> Optional[Dict[str, Any]]:
    """Парсит JSON из ответа LLM. Устойчив к обрезанным ответам."""
    import re
    text = response.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)

    # Убираем trailing запятые перед } и ]
    text = re.sub(r',\s*([}\]])', r'\1', text)

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Извлекаем JSON блок
    match = re.search(r'\{[\s\S]*\}', text)
    if match:
        candidate = match.group()
        candidate = re.sub(r',\s*([}\]])', r'\1', candidate)
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass

    # Починка обрезанного JSON: добавляем недостающие скобки
    match = re.search(r'\{[\s\S]*', text)
    if match:
        candidate = match.group().rstrip()
        # Убираем trailing запятую и незавершённые строки
        candidate = re.sub(r',\s*"[^"]*$', '', candidate)
        candidate = re.sub(r',\s*$', '', candidate)
        # Считаем незакрытые скобки
        open_braces = candidate.count('{') - candidate.count('}')
        open_brackets = candidate.count('[') - candidate.count(']')
        candidate += ']' * max(0, open_brackets) + '}' * max(0, open_braces)
        candidate = re.sub(r',\s*([}\]])', r'\1', candidate)
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass

    return None


def _parse_date(date_str: str) -> Optional[datetime]:
    """Парсит дату из строки (ISO формат из Google Sheets)."""
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        # Убираем Z и микросекунды для совместимости
        clean = date_str.replace("Z", "+00:00")
        return datetime.fromisoformat(clean)
    except (ValueError, TypeError):
        return None


def _relevant_message_sheets(all_sheets, days_back: int = 7) -> list:
    """Определяет какие листы messages_YYYY_MM нужно читать."""
    today = time_utils.today()
    # За 7 дней назад могут быть 2 месяца (например 28 янв - 3 фев)
    from datetime import timedelta
    start_date = today - timedelta(days=days_back)

    needed_months = set()
    d = start_date
    while d <= today:
        needed_months.add(f"messages_{d.strftime('%Y_%m')}")
        d += timedelta(days=15)  # Прыгаем на 15 дней чтобы покрыть оба месяца
    needed_months.add(f"messages_{today.strftime('%Y_%m')}")

    # Также добавляем messages_raw как fallback
    result = []
    for sheet in all_sheets:
        if sheet.title in needed_months or sheet.title == "messages_raw":
            result.append(sheet)

    return result


def load_chats_from_sheets(ss, days_back: int = 7) -> List[Dict[str, Any]]:
    """Загружает чаты за последние N дней и их сообщения."""

    from datetime import timedelta

    # Заголовки для expected_headers (фикс для дубликатов в листе)
    chats_header = [
        "chat_id", "channel", "manager_id", "manager_name", "client_id", "order_id",
        "has_order", "payment_status", "payment_status_ru", "is_successful",
        "order_count", "status", "created_at", "outcome"
    ]
    messages_header = ["chat_id", "message_id", "sent_at", "direction", "manager_id", "text"]

    # Определяем границу даты (7 дней назад от сегодня)
    cutoff = datetime.combine(
        time_utils.today() - timedelta(days=days_back),
        datetime.min.time()
    )
    print(f"   Фильтр: чаты с {cutoff.strftime('%Y-%m-%d')} по сегодня")

    try:
        chats_ws = ss.worksheet("chats_raw")
        chats_data = chats_ws.get_all_records(expected_headers=chats_header)
        print(f"   Прочитано чатов из chats_raw: {len(chats_data)}")
    except Exception as e:
        print(f"Ошибка чтения chats_raw: {e}")
        return []

    # Фильтруем чаты по дате created_at за последние N дней
    recent_chats = []
    skipped_old = 0
    skipped_no_date = 0
    for chat in chats_data:
        created = _parse_date(str(chat.get("created_at", "")))
        if not created:
            skipped_no_date += 1
            continue
        # Сравниваем без timezone (naive)
        if created.replace(tzinfo=None) >= cutoff:
            recent_chats.append(chat)
        else:
            skipped_old += 1

    print(f"   За последние {days_back} дней: {len(recent_chats)} чатов")
    print(f"   Пропущено старых: {skipped_old}, без даты: {skipped_no_date}")

    if not recent_chats:
        return []

    # Собираем ID нужных чатов для загрузки сообщений
    needed_chat_ids = {str(c.get("chat_id", "")) for c in recent_chats}

    # Читаем сообщения только из актуальных месяцев
    messages_data = []
    try:
        all_sheets = ss.worksheets()
        message_sheets = _relevant_message_sheets(all_sheets, days_back)

        if not message_sheets:
            print(f"   Не найдены листы с сообщениями за нужный период")
            return []

        print(f"   Читаю сообщения из {len(message_sheets)} листов: {[s.title for s in message_sheets]}")

        for sheet in message_sheets:
            try:
                sheet_data = sheet.get_all_records(expected_headers=messages_header)
                # Фильтруем: берём только сообщения нужных чатов
                relevant = [m for m in sheet_data if str(m.get("chat_id", "")) in needed_chat_ids]
                messages_data.extend(relevant)
                print(f"   {sheet.title}: {len(relevant)}/{len(sheet_data)} сообщений (отфильтровано)")
            except Exception as e:
                print(f"   Ошибка чтения {sheet.title}: {e}")
                continue

        print(f"   Всего сообщений для анализа: {len(messages_data)}")
    except Exception as e:
        print(f"Ошибка чтения сообщений: {e}")
        return []

    messages_by_chat: Dict[str, List[Dict]] = {}
    for msg in messages_data:
        chat_id = str(msg.get("chat_id", ""))
        if chat_id:
            messages_by_chat.setdefault(chat_id, []).append(msg)

    result = []
    skipped_no_id = 0
    skipped_few_msgs = 0

    for chat in recent_chats:
        chat_id = str(chat.get("chat_id", ""))
        if not chat_id:
            skipped_no_id += 1
            continue

        messages = messages_by_chat.get(chat_id, [])
        if len(messages) < 2:
            skipped_few_msgs += 1
            continue

        messages.sort(key=lambda m: m.get("sent_at", ""))

        result.append({
            "chat_id": chat_id,
            "chat": chat,
            "messages": messages
        })

    print(f"   Пропущено без chat_id: {skipped_no_id}")
    print(f"   Пропущено с < 2 сообщений: {skipped_few_msgs}")
    print(f"   Итого чатов с сообщениями: {len(result)}")

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

        # Загружаем чаты за последние 7 дней
        print("Загружаю чаты за последнюю неделю...")
        chats = load_chats_from_sheets(ss, days_back=7)
        print(f"   Найдено чатов: {len(chats)}")

        analyzed = load_analyzed_chats(ss)
        print(f"   Уже проанализировано: {len(analyzed)}")

        # Фильтруем: новые + изменённые + достаточно сообщений (>= 4)
        chats_to_analyze = []
        skipped_few = 0
        for c in chats:
            chat_id = c["chat_id"]
            msg_count = len(c["messages"])
            chat_status = c["chat"].get("status", "") or c["chat"].get("outcome", "")

            # Пропускаем чаты с < 4 сообщениями ДО лимита
            if msg_count < 4:
                skipped_few += 1
                continue

            need, reason = needs_reanalysis(chat_id, msg_count, chat_status, analyzed)
            if need:
                c["reanalysis_reason"] = reason
                c["message_count"] = msg_count
                c["chat_status"] = chat_status
                chats_to_analyze.append(c)

        total_to_analyze = len(chats_to_analyze)
        new_count = sum(1 for c in chats_to_analyze if c["reanalysis_reason"] == "новый")
        updated_count = total_to_analyze - new_count
        print(f"   Пропущено с < 4 сообщений: {skipped_few}")
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

            # Умная обрезка до 50 сообщений
            truncated_messages = smart_truncate_messages(messages, max_messages=50)
            dialog_text = format_dialog(truncated_messages)

            # Дополнительная защита от превышения токенов
            if len(dialog_text) > 12000:  # ~3000 токенов
                print(f"  ⚠️ Диалог длинный ({len(dialog_text)} символов), обрезаю до 12000")
                dialog_text = dialog_text[:12000] + "\n[...диалог обрезан по лимиту символов...]"

            prompt = ANALYSIS_PROMPT.format(dialog=dialog_text)

            try:
                response = groq.chat(prompt, max_tokens=4000)
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
                    "analyzed_at": time_utils.utc_now().isoformat(),
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

            # Уведомление об успехе + сводка по менеджерам
            from shared.alerting import alert_success, send_telegram, ADMIN_ID
            from collections import defaultdict

            unique_managers = len(set(r["manager_id"] for r in results if r["manager_id"]))

            alert_success(
                service_name="analiz_chatov-posredstvom_ai",
                message="Анализ чатов завершён",
                stats={
                    "Проанализировано чатов": len(results),
                    "Обработано менеджеров": unique_managers,
                    "Ошибок": errors
                }
            )

            # Сводка по каждому менеджеру
            by_manager = defaultdict(list)
            for r in results:
                name = r.get("manager_name") or r.get("manager_id") or "Неизвестный"
                by_manager[name].append(r)

            skill_labels = {
                "greeting_score": "Привет",
                "needs_score": "Потреб",
                "presentation_score": "Презент",
                "objection_score": "Возраж",
                "closing_score": "Закрыт",
                "cross_sell_score": "Допрод",
            }
            skill_keys = list(skill_labels.keys())

            lines = ["<b>Анализ чатов за сегодня</b>\n"]
            for mgr_name, mgr_results in by_manager.items():
                avgs = {}
                for sk in skill_keys:
                    vals = [float(r.get(sk, 0)) for r in mgr_results if r.get(sk)]
                    avgs[sk] = round(sum(vals) / len(vals), 1) if vals else 0

                overall_vals = [float(r.get("overall_score", 0)) for r in mgr_results if r.get("overall_score")]
                overall = round(sum(overall_vals) / len(overall_vals), 1) if overall_vals else 0

                lines.append(f"<b>{mgr_name}</b> ({len(mgr_results)} чатов, общ: {overall})")
                scores_str = " | ".join(f"{skill_labels[sk]}: {avgs[sk]}" for sk in skill_keys)
                lines.append(f"  {scores_str}")
                lines.append("")

            send_telegram(ADMIN_ID, "\n".join(lines))
            print("Готово!")
        else:
            msg = f"Академия INSTINTO: анализ завершён, но результатов нет"
            if errors > 0:
                msg += f" ({errors} ошибок парсинга LLM)"
            else:
                msg += " (нет чатов с достаточным количеством сообщений)"
            telegram.send(msg)
            print("Нет результатов для записи")

    except Exception as e:
        # Уведомление об ошибке через централизованную систему алертов
        from shared.alerting import alert_error

        alert_error(
            service_name="analiz_chatov-posredstvom_ai",
            error=e,
            context="Критическая ошибка анализа чатов"
        )
        raise


if __name__ == "__main__":
    main()
