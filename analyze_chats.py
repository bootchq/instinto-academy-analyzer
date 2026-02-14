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
import tempfile

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


REQUIRED_ANALYSIS_FIELDS = {"scores", "overall_score", "customer_segment", "summary"}
REQUIRED_SCORE_KEYS = {"greeting", "needs_discovery", "presentation", "objection_handling", "closing", "cross_sell"}


def validate_analysis(analysis: Dict[str, Any]) -> Tuple[bool, str]:
    """Проверяет LLM-ответ на корректность. Возвращает (ok, reason)."""
    # Обязательные поля
    missing = REQUIRED_ANALYSIS_FIELDS - set(analysis.keys())
    if missing:
        return False, f"нет полей: {missing}"

    # scores — словарь с оценками
    scores = analysis.get("scores")
    if not isinstance(scores, dict):
        return False, "scores не словарь"

    missing_scores = REQUIRED_SCORE_KEYS - set(scores.keys())
    if missing_scores:
        return False, f"нет оценок: {missing_scores}"

    # Диапазон оценок 0-10
    for key, val in scores.items():
        v = _safe_float(val)
        if v < 0 or v > 10:
            return False, f"{key}={v} вне диапазона 0-10"

    overall = _safe_float(analysis.get("overall_score", 0))
    if overall < 0 or overall > 10:
        return False, f"overall_score={overall} вне диапазона 0-10"

    # summary не пустой
    if not str(analysis.get("summary", "")).strip():
        return False, "пустой summary"

    return True, ""


def _safe_float(val) -> float:
    """Безопасная конвертация в float (обход locale: 5,2 → 5.2)."""
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace(",", ".")
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


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


def load_chats_from_sheets(ss, days_back: int = 7) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    """Загружает чаты за последние N дней и их сообщения.

    Возвращает (chats, manager_map) — чаты и маппинг manager_id → name.
    Читает chats_raw ОДИН раз через get_all_values (экономия API запроса).
    """

    from datetime import timedelta

    # Заголовки — совпадают с export_to_sheets_batch.py
    chats_header = [
        "chat_id", "channel", "manager_id", "manager_name", "client_id", "order_id",
        "has_order", "payment_status", "payment_status_ru", "is_successful",
        "order_count", "status", "created_at", "outcome",
        "inbound_count", "outbound_count", "first_response_sec", "unanswered_inbound",
        "is_closed",
    ]
    messages_header = ["chat_id", "message_id", "sent_at", "direction", "manager_id", "text"]

    # Определяем границу даты (7 дней назад, UTC для совместимости с ISO датами из Sheets)
    cutoff = datetime.combine(
        time_utils.today() - timedelta(days=days_back),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
    print(f"   Фильтр: чаты с {cutoff.strftime('%Y-%m-%d')} по сегодня")

    manager_map: Dict[str, str] = {}

    # Сколько строк читать с конца (новые чаты — внизу листа)
    # ~100 чатов/день * 7 дней = ~700, берём 1000 с запасом
    TAIL_ROWS = int(os.environ.get("CHATS_TAIL_ROWS", "1000"))

    try:
        chats_ws = ss.worksheet("chats_raw")

        # Читаем заголовок
        raw_hdr = chats_ws.row_values(1)
        if not raw_hdr:
            print("   chats_raw пуст")
            return [], {}

        # Быстро узнаём сколько строк заполнено (читаем только колонку A — chat_id)
        filled_rows = len(chats_ws.col_values(1))
        if filled_rows < 2:
            print("   chats_raw: нет данных")
            return [], {}

        # Читаем только хвост (последние TAIL_ROWS строк)
        start_row = max(2, filled_rows - TAIL_ROWS + 1)
        last_col = chr(64 + min(len(raw_hdr), 26))  # S для 19 колонок
        tail_range = f"A{start_row}:{last_col}{filled_rows}"
        tail_data = chats_ws.get(tail_range)
        if not tail_data:
            print("   chats_raw: нет данных в хвосте")
            return [], {}

        print(f"   Прочитано {len(tail_data)} строк из chats_raw (хвост, строки {start_row}-{filled_rows})")

        # Конвертируем в list of dicts
        chats_data = []
        for row in tail_data:
            d = {}
            for i, key in enumerate(raw_hdr):
                d[key] = row[i] if i < len(row) else ""
            chats_data.append(d)

        # Строим manager_map из тех же данных
        if "manager_id" in raw_hdr and "manager_name" in raw_hdr:
            mi = raw_hdr.index("manager_id")
            mn = raw_hdr.index("manager_name")
            for row in tail_data:
                if len(row) > max(mi, mn):
                    rid = row[mi].strip()
                    rname = row[mn].strip()
                    if rid and rname and rid != rname:
                        manager_map[rid] = rname

        if manager_map:
            print(f"   Маппинг менеджеров: {manager_map}")
    except Exception as e:
        print(f"Ошибка чтения chats_raw: {e}")
        return [], {}

    # Фильтруем чаты по дате created_at за последние N дней
    recent_chats = []
    skipped_old = 0
    skipped_no_date = 0
    for chat in chats_data:
        created = _parse_date(str(chat.get("created_at", "")))
        if not created:
            skipped_no_date += 1
            continue
        # Если дата naive — считаем UTC
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        if created >= cutoff:
            recent_chats.append(chat)
        else:
            skipped_old += 1

    print(f"   За последние {days_back} дней: {len(recent_chats)} чатов")
    print(f"   Пропущено старых: {skipped_old}, без даты: {skipped_no_date}")

    if not recent_chats:
        return [], manager_map

    # Собираем ID нужных чатов для загрузки сообщений
    needed_chat_ids = {str(c.get("chat_id", "")) for c in recent_chats}

    # Читаем сообщения только из актуальных месяцев
    messages_data = []
    try:
        all_sheets = ss.worksheets()
        message_sheets = _relevant_message_sheets(all_sheets, days_back)

        if not message_sheets:
            print(f"   Не найдены листы с сообщениями за нужный период")
            return [], manager_map

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
        return [], manager_map

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

    return result, manager_map


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

        # Загружаем чаты за последние 7 дней (+ manager_map из тех же данных)
        print("Загружаю чаты за последнюю неделю...")
        chats, manager_map = load_chats_from_sheets(ss, days_back=7)
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
        # Приоритет: сначала новые, потом обновлённые
        if len(chats_to_analyze) > MAX_CHATS_PER_RUN:
            new_chats = [c for c in chats_to_analyze if c["reanalysis_reason"] == "новый"]
            updated_chats = [c for c in chats_to_analyze if c["reanalysis_reason"] != "новый"]
            chats_to_analyze = (new_chats + updated_chats)[:MAX_CHATS_PER_RUN]
            print(f"   Ограничиваю до {MAX_CHATS_PER_RUN} чатов (новых: {min(len(new_chats), MAX_CHATS_PER_RUN)}, остальные в следующий раз)")

        # Анализируем
        results = []
        errors = 0
        failed_chats = []
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
                    print(f"  Ошибка парсинга LLM для чата {chat_id}. Ответ: {response[:500]}")
                    failed_chats.append({"chat_id": chat_id, "reason": "parse_error", "response_head": response[:200]})
                    errors += 1
                    continue

                # Валидация структуры ответа
                valid, reason = validate_analysis(analysis)
                if not valid:
                    print(f"  Невалидный LLM-ответ для чата {chat_id}: {reason}")
                    failed_chats.append({"chat_id": chat_id, "reason": f"validation: {reason}", "response_head": response[:200]})
                    errors += 1
                    continue

                scores = analysis.get("scores", {})
                # Определяем менеджера: сначала из чата, потом из исходящих сообщений
                mgr_id = str(chat.get("manager_id", "")).strip()
                mgr_name = str(chat.get("manager_name", "")).strip()
                if not mgr_id:
                    for m in messages:
                        if m.get("direction") == "out" and str(m.get("manager_id", "")).strip():
                            mgr_id = str(m["manager_id"]).strip()
                            break
                if mgr_id and not mgr_name:
                    mgr_name = manager_map.get(mgr_id, mgr_id)

                result = {
                    "chat_id": chat_id,
                    "manager_id": mgr_id,
                    "manager_name": mgr_name,
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
                print(f"  Ошибка чата {chat_id}: {e}")
                failed_chats.append({"chat_id": chat_id, "reason": "exception", "error": str(e)})
                errors += 1
                continue

        # Записываем результаты с retry (транзакционность)
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

            # Сохраняем буфер в файл на случай краша при записи в Sheets
            buffer_path = os.path.join(tempfile.gettempdir(), "analysis_buffer.json")
            with open(buffer_path, "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False)
            print(f"  Буфер сохранён: {buffer_path}")

            # Retry записи в Sheets (3 попытки)
            write_ok = False
            for attempt in range(3):
                try:
                    rows = dicts_to_table(results, header=header)
                    append_to_worksheet(ss, "analysis_raw", rows=rows[1:], header=header)
                    write_ok = True
                    break
                except Exception as e:
                    print(f"  Ошибка записи (попытка {attempt + 1}/3): {e}")
                    if attempt < 2:
                        time.sleep(10 * (attempt + 1))

            if not write_ok:
                from shared.alerting import alert_error
                alert_error(
                    service_name="analiz_chatov-posredstvom_ai",
                    error=RuntimeError(f"Не удалось записать {len(results)} результатов в Sheets после 3 попыток"),
                    context=f"Буфер сохранён в {buffer_path}"
                )
                raise RuntimeError(f"Sheets write failed. Буфер: {buffer_path}")

            # Удаляем буфер после успешной записи
            try:
                os.unlink(buffer_path)
            except OSError:
                pass

            # Ежедневная сводка админу (БЕЗ alert_success - только сводка)
            from shared.alerting import send_telegram, ADMIN_ID
            from collections import defaultdict

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

            lines = ["<b>📊 Анализ чатов за сегодня</b>\n"]
            for mgr_name, mgr_results in sorted(by_manager.items(), key=lambda x: -len(x[1])):
                avgs = {}
                for sk in skill_keys:
                    vals = [_safe_float(r.get(sk, 0)) for r in mgr_results if r.get(sk) and _safe_float(r.get(sk, 0)) > 0]
                    avgs[sk] = round(sum(vals) / len(vals), 1) if vals else 0

                overall_vals = [_safe_float(r.get("overall_score", 0)) for r in mgr_results if r.get("overall_score") and _safe_float(r.get("overall_score", 0)) > 0]
                overall = round(sum(overall_vals) / len(overall_vals), 1) if overall_vals else 0

                lines.append(f"<b>{mgr_name}</b>: {len(mgr_results)} чатов, общая {overall}/10")
                scores_str = " | ".join(f"{skill_labels[sk]}: {avgs[sk]}" for sk in skill_keys if avgs[sk] > 0)
                if scores_str:
                    lines.append(f"  {scores_str}")
                lines.append("")

            if failed_chats:
                lines.append(f"⚠️ Ошибки ({len(failed_chats)}):")
                for fc in failed_chats[:5]:
                    lines.append(f"  чат {fc['chat_id']}: {fc['reason']}")

            send_telegram(ADMIN_ID, "\n".join(lines))
            print(f"Готово! Проанализировано: {len(results)}, ошибок: {errors}")
            if failed_chats:
                print(f"Детали ошибок: {json.dumps(failed_chats, ensure_ascii=False)}")
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
