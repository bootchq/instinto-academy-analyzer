"""
Извлечение лучших практик из исторических чатов.

Одноразовый скрипт: анализирует ВСЕ чаты в chats_raw,
находит лучшие по метрикам + LLM, извлекает конкретные техники продаж.

Результаты: листы best_practices_* в Google Sheets + Telegram-отчёт.

Переменные окружения:
    GROQ_API_KEY, GOOGLE_SHEETS_ID, GOOGLE_SERVICE_ACCOUNT_JSON
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID (для отчёта)
    ALERT_BOT_TOKEN (для алертов)
    BP_MAX_CANDIDATES=300  — макс. кандидатов для LLM
    BP_MAX_DEEP=50         — макс. чатов для глубокого анализа
    BP_BATCH_SIZE=10       — сохранять каждые N чатов
    BP_PAUSE_SEC=60        — пауза между LLM-запросами
"""

from __future__ import annotations

import json
import os
import time
import tempfile

os.environ['TZ'] = 'Europe/Moscow'
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

from shared import time_utils
from shared.sheets_academy import (
    open_spreadsheet, append_to_worksheet, upsert_worksheet, dicts_to_table,
)
from shared.alerting import send_telegram, alert_error, alert_success, ADMIN_ID

import requests


# === Конфигурация ===

MAX_CANDIDATES = int(os.environ.get("BP_MAX_CANDIDATES", "300"))
MAX_DEEP = int(os.environ.get("BP_MAX_DEEP", "50"))
BATCH_SIZE = int(os.environ.get("BP_BATCH_SIZE", "10"))
PAUSE_SEC = int(os.environ.get("BP_PAUSE_SEC", "60"))

# Минимальные требования к чату-кандидату
MIN_MESSAGES = 6       # >= 6 сообщений в диалоге
MIN_OUTBOUND = 2       # менеджер ответил >= 2 раз


# === Промпт для стандартного анализа (тот же что в analyze_chats.py) ===

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


# === Промпт для глубокого анализа лучших чатов ===

DEEP_ANALYSIS_PROMPT = """Ты тренер по продажам бренда INSTINTO (премиальное женское бельё).
Перед тобой ОДИН ИЗ ЛУЧШИХ диалогов менеджера. Твоя задача — извлечь конкретные техники
и фразы, которые можно использовать как обучающий материал для НОВЫХ менеджеров.

КОНТЕКСТ: Бренд INSTINTO, dark luxury, средний+ ценовой сегмент, ЦА — женщины 25-45.

ДИАЛОГ:
{dialog}

ЗАДАЧА — извлечь КОНКРЕТНЫЕ, ПЕРЕИСПОЛЬЗУЕМЫЕ техники:
1. Приветствие: точная фраза + почему она работает
2. Выявление потребностей: какие вопросы задал, какая техника (SPIN/открытые/уточняющие)
3. Презентация: как описал товар, какие акценты (эмоциональные/рациональные)
4. Возражения: конкретный ответ на конкретное возражение
5. Закрытие: как подвёл к покупке
6. Допродажа: как предложил дополнительный товар
7. Общий стиль: тон, темп, эмпатия

Ответь ТОЛЬКО в JSON формате (без markdown):
{{
  "best_greeting": "точная цитата приветствия менеджера",
  "greeting_why_works": "почему это приветствие эффективно",
  "needs_questions": ["вопрос 1 менеджера", "вопрос 2"],
  "needs_technique": "описание техники выявления потребностей",
  "presentation_phrases": ["фраза описания товара 1", "фраза 2"],
  "presentation_approach": "описание подхода (эмоциональный/рациональный/смешанный)",
  "objection_pairs": [
    {{"objection": "цитата возражения клиента", "response": "цитата ответа менеджера", "technique": "название техники"}}
  ],
  "closing_phrases": ["фраза закрытия 1"],
  "closing_technique": "как именно подвёл к покупке",
  "cross_sell_approach": "как предложил доп. товар (или null если не было)",
  "communication_style": "описание стиля (тёплый/деловой/экспертный/дружеский)",
  "key_success_factors": ["фактор успеха 1", "фактор 2"],
  "reusable_templates": [
    {{"situation": "когда применять", "template": "шаблон фразы/действия"}}
  ]
}}"""


# === Groq клиент (копия из analyze_chats.py) ===

class LLMClient:
    """Мультипровайдерный LLM клиент с автофоллбэком.

    Cerebras (основной) -> SambaNova -> Groq (фоллбэк).
    При rate limit одного провайдера автоматически переключается на другого.
    """

    PROVIDERS = {
        "cerebras": {
            "url": "https://api.cerebras.ai/v1/chat/completions",
            "model": "llama-3.3-70b",
            "env_key": "CEREBRAS_API_KEY",
            "pause": 2,  # 30 RPM
        },
        "sambanova": {
            "url": "https://api.sambanova.ai/v1/chat/completions",
            "model": "Meta-Llama-3.3-70B-Instruct",
            "env_key": "SAMBANOVA_API_KEY",
            "pause": 3,  # 40 RPM
        },
        "groq": {
            "url": "https://api.groq.com/openai/v1/chat/completions",
            "model": "llama-3.1-8b-instant",
            "env_key": "GROQ_API_KEY",
            "pause": 10,  # 6K TPM — медленнее
        },
    }

    def __init__(self):
        # Инициализируем все доступные провайдеры
        self._clients: Dict[str, dict] = {}
        self._order: List[str] = []  # порядок приоритета
        for name in ["cerebras", "sambanova", "groq"]:
            cfg = self.PROVIDERS[name]
            api_key = os.environ.get(cfg["env_key"], "")
            if api_key:
                sess = requests.Session()
                sess.headers.update({
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                })
                self._clients[name] = {
                    "session": sess,
                    "url": cfg["url"],
                    "model": cfg["model"],
                    "pause": cfg["pause"],
                    "stats": {"ok": 0, "rate_limited": 0, "total_wait": 0.0},
                    "blocked_until": 0.0,  # timestamp когда снимается блокировка
                }
                self._order.append(name)
                print(f"  LLM провайдер: {name} / {cfg['model']}")

        if not self._clients:
            raise ValueError("Нет ни одного LLM API ключа (CEREBRAS_API_KEY / GROQ_API_KEY)")

        self._current = self._order[0]

    @property
    def adaptive_pause(self) -> float:
        """Пауза для текущего активного провайдера."""
        return self._clients[self._current]["pause"]

    def _pick_provider(self) -> Optional[str]:
        """Выбрать доступного провайдера (не заблокированного rate limit)."""
        now = time.time()
        for name in self._order:
            if self._clients[name]["blocked_until"] <= now:
                return name
        # Все заблокированы — берём того, кто разблокируется раньше
        earliest = min(self._order, key=lambda n: self._clients[n]["blocked_until"])
        wait = self._clients[earliest]["blocked_until"] - now
        if wait > 0:
            print(f"  Все провайдеры заблокированы, жду {wait:.0f}с ({earliest})...")
            time.sleep(wait)
        return earliest

    def chat(self, prompt: str, max_tokens: int = 1500) -> str:
        last_error = None
        for attempt in range(8):  # больше попыток — можем переключаться
            provider = self._pick_provider()
            if not provider:
                break
            self._current = provider
            client = self._clients[provider]

            payload = {
                "model": client["model"],
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
                "temperature": 0.3,
            }
            try:
                resp = client["session"].post(client["url"], json=payload, timeout=120)
                if resp.status_code == 429:
                    retry_after = resp.headers.get("retry-after")
                    if retry_after:
                        block_sec = min(float(retry_after) + 2, 300)
                    else:
                        block_sec = 60
                    client["blocked_until"] = time.time() + block_sec
                    client["stats"]["rate_limited"] += 1
                    client["stats"]["total_wait"] += block_sec
                    print(f"  {provider}: rate limit, блокирую на {block_sec:.0f}с -> переключаюсь")
                    continue  # следующая итерация выберет другого провайдера
                resp.raise_for_status()
                client["stats"]["ok"] += 1
                return resp.json()["choices"][0]["message"]["content"]
            except requests.exceptions.RequestException as e:
                last_error = e
                # Блокируем ненадолго при ошибке сети
                client["blocked_until"] = time.time() + 15
                print(f"  {provider}: ошибка {e}, переключаюсь...")
                continue

        raise RuntimeError(f"LLM: все провайдеры недоступны. Последняя ошибка: {last_error}")

    def print_stats(self):
        for name, client in self._clients.items():
            s = client["stats"]
            total = s["ok"] + s["rate_limited"]
            if total:
                print(f"  LLM stats ({name}): {s['ok']} OK, {s['rate_limited']} rate-limited, "
                      f"ожидание: {s['total_wait']:.0f}с")


# === Утилиты ===

def _safe_float(val) -> float:
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace(",", ".")
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def _parse_date(date_str: str) -> Optional[datetime]:
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        clean = date_str.replace("Z", "+00:00")
        return datetime.fromisoformat(clean)
    except (ValueError, TypeError):
        return None


def parse_llm_response(response: str) -> Optional[Dict[str, Any]]:
    """Парсит JSON из LLM (устойчив к обрезанным ответам)."""
    import re
    text = response.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)

    text = re.sub(r',\s*([}\]])', r'\1', text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    match = re.search(r'\{[\s\S]*\}', text)
    if match:
        candidate = match.group()
        candidate = re.sub(r',\s*([}\]])', r'\1', candidate)
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass

    match = re.search(r'\{[\s\S]*', text)
    if match:
        candidate = match.group().rstrip()
        candidate = re.sub(r',\s*"[^"]*$', '', candidate)
        candidate = re.sub(r',\s*$', '', candidate)
        open_braces = candidate.count('{') - candidate.count('}')
        open_brackets = candidate.count('[') - candidate.count(']')
        candidate += ']' * max(0, open_brackets) + '}' * max(0, open_braces)
        candidate = re.sub(r',\s*([}\]])', r'\1', candidate)
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass
    return None


def format_dialog(messages: List[Dict[str, Any]]) -> str:
    lines = []
    for msg in messages:
        direction = msg.get("direction", "")
        text = str(msg.get("text", "")).strip()
        if not text:
            continue
        if direction == "system":
            lines.append(f"\n{text}\n")
        elif direction == "in":
            lines.append(f"Клиент: {text}")
        else:
            lines.append(f"Менеджер: {text}")
    return "\n".join(lines)


def smart_truncate(messages: List[Dict[str, Any]], max_messages: int = 50) -> List[Dict[str, Any]]:
    if len(messages) <= max_messages:
        return messages
    first_n = 5
    last_n = max_messages - first_n
    head = messages[:first_n]
    tail = messages[-last_n:]
    skipped = len(messages) - first_n - last_n
    return head + [{"direction": "system", "text": f"[...пропущено {skipped} сообщений...]"}] + tail


# === Фаза 1: Скрининг ===

def phase1_screening(ss) -> List[Dict[str, Any]]:
    """Читает ВСЕ чаты из chats_raw, фильтрует по количественным метрикам."""
    print("\n=== ФАЗА 1: Количественный скрининг ===\n")

    chats_ws = ss.worksheet("chats_raw")
    raw_hdr = chats_ws.row_values(1)
    if not raw_hdr:
        print("chats_raw пуст!")
        return []

    # Показываем ВСЕ листы в документе
    all_sheets = ss.worksheets()
    print(f"  Все листы в документе ({len(all_sheets)}):")
    for s in all_sheets:
        print(f"    {s.title} ({s.row_count} rows)")

    # Читаем ВСЕ данные (не tail)
    print("\nЧитаю ВСЕ чаты из chats_raw...")
    all_values = chats_ws.get_all_values()
    print(f"  Всего строк: {len(all_values) - 1}")

    # Конвертируем в dict
    header = all_values[0]
    all_chats = []
    for row in all_values[1:]:
        d = {header[i]: row[i] if i < len(row) else "" for i in range(len(header))}
        all_chats.append(d)

    print(f"  Загружено чатов: {len(all_chats)}")

    # Статистика по датам
    dates = [_parse_date(c.get("created_at", "")) for c in all_chats]
    valid_dates = [d for d in dates if d]
    if valid_dates:
        print(f"  Диапазон дат: {min(valid_dates).strftime('%Y-%m-%d')} .. {max(valid_dates).strftime('%Y-%m-%d')}")

    # Статистика по менеджерам
    managers = defaultdict(int)
    for c in all_chats:
        name = c.get("manager_name", "").strip() or c.get("manager_id", "").strip() or "Неизвестный"
        managers[name] += 1
    print(f"  Менеджеры: {dict(managers)}")

    # Диагностика: показать первые 3 строки для понимания данных
    print(f"\n  Диагностика (первые 3 чата):")
    for c in all_chats[:3]:
        print(f"    chat_id={c.get('chat_id','?')}, has_order='{c.get('has_order','')}', "
              f"is_successful='{c.get('is_successful','')}', "
              f"outbound='{c.get('outbound_count','')}', inbound='{c.get('inbound_count','')}', "
              f"manager='{c.get('manager_name','')}'")

    # Фильтрация кандидатов
    # outbound_count может быть пустым для старых чатов — НЕ фильтруем по нему жёстко
    candidates = []
    stats = {"no_manager": 0, "passed": 0, "with_order": 0, "successful": 0}

    for chat in all_chats:
        outbound = int(chat.get("outbound_count", 0) or 0)
        inbound = int(chat.get("inbound_count", 0) or 0)
        total_msgs = inbound + outbound
        has_order = str(chat.get("has_order", "")).lower() in ("true", "1", "да", "yes")
        is_successful = str(chat.get("is_successful", "")).lower() in ("true", "1", "да", "yes")
        mgr = chat.get("manager_name", "").strip() or chat.get("manager_id", "").strip()

        # Единственный жёсткий фильтр: должен быть менеджер
        if not mgr or mgr == "Неизвестный":
            stats["no_manager"] += 1
            continue

        # Приоритет: чаты с заказом и успешным исходом получают высший балл
        priority = 0
        if has_order:
            priority += 3
            stats["with_order"] += 1
        if is_successful:
            priority += 4
            stats["successful"] += 1
        # Больше сообщений = более содержательный диалог
        if total_msgs > 0:
            priority += min(total_msgs / 10, 3)
        # Если outbound > 0 — бонус (значит метрики посчитаны)
        if outbound > 0:
            priority += 1

        candidates.append({
            **chat,
            "_priority": priority,
            "_total_msgs": total_msgs if total_msgs > 0 else None,  # None = посчитаем позже
        })
        stats["passed"] += 1

    print(f"\n  Результат скрининга:")
    print(f"    Без менеджера: {stats['no_manager']}")
    print(f"    С заказом: {stats['with_order']}")
    print(f"    Успешных: {stats['successful']}")
    print(f"    Прошли фильтр: {stats['passed']}")

    # Сортируем по приоритету (лучшие первые)
    candidates.sort(key=lambda c: -c["_priority"])

    # Ограничиваем количество
    if len(candidates) > MAX_CANDIDATES:
        print(f"  Ограничиваю до {MAX_CANDIDATES} лучших кандидатов")
        candidates = candidates[:MAX_CANDIDATES]

    return candidates


# === Фаза 2: Сбор оценок ===

def phase2_analysis(ss, groq: GroqClient, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Собирает все оценки: из analysis_raw + новый LLM анализ для чатов с сообщениями."""
    print(f"\n=== ФАЗА 2: Сбор оценок ({len(candidates)} кандидатов) ===\n")

    # === Шаг 1: Берём ВСЕ оценки из analysis_raw (главный источник) ===
    existing_scores: Dict[str, Dict] = {}
    try:
        ws = ss.worksheet("analysis_raw")
        data = ws.get_all_records()
        for row in data:
            cid = str(row.get("chat_id", ""))
            overall = _safe_float(row.get("overall_score", 0))
            if cid and overall > 0:
                existing_scores[cid] = row
        print(f"  Всего оценок в analysis_raw: {len(existing_scores)}")
    except Exception as e:
        print(f"  Не удалось прочитать analysis_raw: {e}")

    # Конвертируем ВСЕ оценки из analysis_raw в результаты
    results = []
    candidate_ids = {str(c.get("chat_id", "")) for c in candidates}
    # Берём данные из chats_raw для обогащения
    chats_by_id = {str(c.get("chat_id", "")): c for c in candidates}

    for cid, existing in existing_scores.items():
        cand = chats_by_id.get(cid, {})
        results.append({
            "chat_id": cid,
            "manager_name": existing.get("manager_name", ""),
            "channel": existing.get("channel", ""),
            "created_at": cand.get("created_at", ""),
            "message_count": int(existing.get("message_count", 0) or 0),
            "has_order": cand.get("has_order", ""),
            "is_successful": cand.get("is_successful", ""),
            "overall_score": _safe_float(existing.get("overall_score", 0)),
            "greeting_score": _safe_float(existing.get("greeting_score", 0)),
            "needs_score": _safe_float(existing.get("needs_score", 0)),
            "presentation_score": _safe_float(existing.get("presentation_score", 0)),
            "objection_score": _safe_float(existing.get("objection_score", 0)),
            "closing_score": _safe_float(existing.get("closing_score", 0)),
            "cross_sell_score": _safe_float(existing.get("cross_sell_score", 0)),
            "customer_segment": existing.get("customer_segment", ""),
            "summary": existing.get("summary", ""),
            "techniques": existing.get("techniques", ""),
            "source": "analysis_raw",
        })

    print(f"  Взято из analysis_raw: {len(results)}")

    # === Шаг 2: Для кандидатов БЕЗ оценки — пробуем LLM анализ ===

    # Читаем уже обработанные в best_practices_analysis (для resume)
    already_done: set = set()
    try:
        ws_bp = ss.worksheet("best_practices_analysis")
        vals = ws_bp.get_all_values()
        if len(vals) > 1:
            hdr_bp = vals[0]
            cid_idx = hdr_bp.index("chat_id") if "chat_id" in hdr_bp else 0
            for row in vals[1:]:
                if cid_idx < len(row) and row[cid_idx]:
                    already_done.add(row[cid_idx].strip())
        print(f"  Уже обработано в best_practices_analysis: {len(already_done)}")
    except Exception:
        pass

    to_analyze = [
        c for c in candidates
        if str(c.get("chat_id", "")) not in existing_scores
        and str(c.get("chat_id", "")) not in already_done
    ]
    total_remaining = len(to_analyze)
    print(f"  Кандидатов без оценки (минус resume): {total_remaining}")
    print(f"  Переиспользовано из analysis_raw: {len(results)}")

    # Лимит чатов за один запуск (Railway timeout ~10 мин)
    MAX_PER_RUN = int(os.environ.get("BP_MAX_PER_RUN", "20"))
    if total_remaining > MAX_PER_RUN:
        print(f"  Ограничиваю до {MAX_PER_RUN} чатов за этот запуск (из {total_remaining})")
        to_analyze = to_analyze[:MAX_PER_RUN]
        remaining_after = total_remaining - MAX_PER_RUN
    else:
        remaining_after = 0

    if not to_analyze:
        print("  Все кандидаты обработаны!")
        return results, 0

    # Загружаем сообщения для чатов, которые нужно анализировать
    needed_ids = {str(c.get("chat_id", "")) for c in to_analyze}
    messages_by_chat = _load_messages_for_chats(ss, needed_ids)
    print(f"  Загружено сообщений для {len(messages_by_chat)} чатов")

    # Анализируем через LLM
    header = [
        "chat_id", "manager_name", "channel", "created_at", "message_count",
        "has_order", "is_successful",
        "overall_score", "greeting_score", "needs_score",
        "presentation_score", "objection_score", "closing_score", "cross_sell_score",
        "customer_segment", "summary", "techniques", "source",
    ]
    batch_results = []
    errors = 0

    for i, cand in enumerate(to_analyze, 1):
        cid = str(cand.get("chat_id", ""))
        msgs = messages_by_chat.get(cid, [])

        if len(msgs) < 4:
            print(f"  [{i}/{len(to_analyze)}] Чат {cid}: мало сообщений ({len(msgs)}), пропускаю")
            continue

        print(f"  [{i}/{len(to_analyze)}] Анализирую чат {cid} ({len(msgs)} сообщений)...")

        truncated = smart_truncate(msgs, max_messages=30)
        dialog_text = format_dialog(truncated)

        if len(dialog_text) > 5000:
            dialog_text = dialog_text[:5000] + "\n[...обрезано...]"

        try:
            response = groq.chat(ANALYSIS_PROMPT.format(dialog=dialog_text), max_tokens=1500)
            analysis = parse_llm_response(response)

            if not analysis:
                print(f"    Ошибка парсинга LLM")
                errors += 1
                continue

            scores = analysis.get("scores", {})
            result = {
                "chat_id": cid,
                "manager_name": cand.get("manager_name", ""),
                "channel": cand.get("channel", ""),
                "created_at": cand.get("created_at", ""),
                "message_count": cand.get("_total_msgs", len(msgs)),
                "has_order": cand.get("has_order", ""),
                "is_successful": cand.get("is_successful", ""),
                "overall_score": _safe_float(analysis.get("overall_score", 0)),
                "greeting_score": _safe_float(scores.get("greeting", 0)),
                "needs_score": _safe_float(scores.get("needs_discovery", 0)),
                "presentation_score": _safe_float(scores.get("presentation", 0)),
                "objection_score": _safe_float(scores.get("objection_handling", 0)),
                "closing_score": _safe_float(scores.get("closing", 0)),
                "cross_sell_score": _safe_float(scores.get("cross_sell", 0)),
                "customer_segment": analysis.get("customer_segment", ""),
                "summary": analysis.get("summary", ""),
                "techniques": json.dumps(analysis.get("techniques_used", []), ensure_ascii=False),
                "source": "llm_new",
            }
            results.append(result)
            batch_results.append(result)

            print(f"    Оценка: {result['overall_score']}/10, сегмент: {result['customer_segment']}")

            # Сохраняем каждые BATCH_SIZE чатов
            if len(batch_results) >= BATCH_SIZE:
                _save_batch(ss, batch_results, header)
                batch_results = []

            time.sleep(groq.adaptive_pause)

        except Exception as e:
            print(f"    Ошибка: {e}")
            errors += 1
            continue

    # Дописываем остаток
    if batch_results:
        _save_batch(ss, batch_results, header)

    groq.print_stats()
    print(f"\n  Итого результатов: {len(results)} (ошибок: {errors}), осталось: {remaining_after}")
    return results, remaining_after


def _save_batch(ss, batch: List[Dict], header: List[str]):
    """Сохраняет батч результатов в best_practices_analysis."""
    rows = [[r.get(k, "") for k in header] for r in batch]
    append_to_worksheet(ss, "best_practices_analysis", rows=rows, header=header)
    print(f"    Сохранено {len(batch)} результатов в best_practices_analysis")


def _load_messages_for_chats(ss, chat_ids: set) -> Dict[str, List[Dict]]:
    """Загружает сообщения из всех messages_YYYY_MM листов для указанных чатов."""
    messages_by_chat: Dict[str, List[Dict]] = {}
    messages_header = ["chat_id", "message_id", "sent_at", "direction", "manager_id", "text"]

    all_sheets = ss.worksheets()
    # Включаем ВСЕ листы с сообщениями (включая messages_raw)
    msg_sheets = [s for s in all_sheets if s.title.startswith("messages_")]

    if not msg_sheets:
        print("  Нет листов messages_YYYY_MM!")
        return {}

    print(f"  Читаю сообщения из {len(msg_sheets)} листов: {[s.title for s in msg_sheets]}")

    for sheet in msg_sheets:
        try:
            data = sheet.get_all_values()
            if len(data) < 2:
                continue

            hdr = data[0]
            cid_idx = hdr.index("chat_id") if "chat_id" in hdr else 0
            dir_idx = hdr.index("direction") if "direction" in hdr else 3
            text_idx = hdr.index("text") if "text" in hdr else 5
            sent_idx = hdr.index("sent_at") if "sent_at" in hdr else 2

            count = 0
            for row in data[1:]:
                if cid_idx >= len(row):
                    continue
                cid = row[cid_idx].strip()
                if cid not in chat_ids:
                    continue

                msg = {
                    "chat_id": cid,
                    "direction": row[dir_idx] if dir_idx < len(row) else "",
                    "text": row[text_idx] if text_idx < len(row) else "",
                    "sent_at": row[sent_idx] if sent_idx < len(row) else "",
                }
                messages_by_chat.setdefault(cid, []).append(msg)
                count += 1

            print(f"    {sheet.title}: {count} сообщений для наших чатов")
        except Exception as e:
            print(f"    Ошибка чтения {sheet.title}: {e}")
            continue

    # Сортируем сообщения по времени
    for cid in messages_by_chat:
        messages_by_chat[cid].sort(key=lambda m: m.get("sent_at", ""))

    return messages_by_chat


# === Фаза 3: Глубокий анализ топ-N ===

def phase3_deep_analysis(ss, groq: GroqClient, all_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Глубокий анализ топ-N чатов: извлечение конкретных техник и фраз."""
    print(f"\n=== ФАЗА 3: Глубокий анализ топ-{MAX_DEEP} чатов ===\n")

    # Сортируем по overall_score (лучшие первые)
    sorted_results = sorted(all_results, key=lambda r: -_safe_float(r.get("overall_score", 0)))
    top_chats = sorted_results[:MAX_DEEP]

    print(f"  Топ-{len(top_chats)} чатов по overall_score:")
    for i, c in enumerate(top_chats[:10], 1):
        print(f"    {i}. {c['chat_id']} — {c.get('manager_name', '?')}: {c.get('overall_score', 0)}/10")

    # Загружаем сообщения для топ-чатов
    needed_ids = {str(c.get("chat_id", "")) for c in top_chats}
    messages_by_chat = _load_messages_for_chats(ss, needed_ids)

    # Проверяем уже обработанные (resume)
    already_deep: set = set()
    try:
        ws = ss.worksheet("best_practices_deep")
        vals = ws.get_all_values()
        if len(vals) > 1:
            hdr = vals[0]
            cid_idx = hdr.index("chat_id") if "chat_id" in hdr else 0
            for row in vals[1:]:
                if cid_idx < len(row) and row[cid_idx]:
                    already_deep.add(row[cid_idx].strip())
        print(f"  Уже обработано в deep: {len(already_deep)}")
    except Exception:
        pass

    deep_header = [
        "chat_id", "manager_name", "overall_score", "customer_segment",
        "best_greeting", "greeting_why_works",
        "needs_questions", "needs_technique",
        "presentation_phrases", "presentation_approach",
        "objection_pairs", "closing_phrases", "closing_technique",
        "cross_sell_approach", "communication_style",
        "key_success_factors", "reusable_templates",
    ]

    deep_results = []
    errors = 0

    for i, chat_result in enumerate(top_chats, 1):
        cid = str(chat_result.get("chat_id", ""))

        if cid in already_deep:
            print(f"  [{i}/{len(top_chats)}] Чат {cid}: уже обработан, пропускаю")
            continue

        msgs = messages_by_chat.get(cid, [])
        if len(msgs) < 4:
            print(f"  [{i}/{len(top_chats)}] Чат {cid}: мало сообщений ({len(msgs)}), пропускаю")
            continue

        print(f"  [{i}/{len(top_chats)}] Глубокий анализ чата {cid} (score: {chat_result.get('overall_score', 0)})...")

        truncated = smart_truncate(msgs, max_messages=60)
        dialog_text = format_dialog(truncated)
        if len(dialog_text) > 14000:
            dialog_text = dialog_text[:14000] + "\n[...обрезано...]"

        try:
            response = groq.chat(DEEP_ANALYSIS_PROMPT.format(dialog=dialog_text), max_tokens=4000)
            deep = parse_llm_response(response)

            if not deep:
                print(f"    Ошибка парсинга")
                errors += 1
                continue

            result = {
                "chat_id": cid,
                "manager_name": chat_result.get("manager_name", ""),
                "overall_score": chat_result.get("overall_score", 0),
                "customer_segment": chat_result.get("customer_segment", ""),
                "best_greeting": deep.get("best_greeting", ""),
                "greeting_why_works": deep.get("greeting_why_works", ""),
                "needs_questions": json.dumps(deep.get("needs_questions", []), ensure_ascii=False),
                "needs_technique": deep.get("needs_technique", ""),
                "presentation_phrases": json.dumps(deep.get("presentation_phrases", []), ensure_ascii=False),
                "presentation_approach": deep.get("presentation_approach", ""),
                "objection_pairs": json.dumps(deep.get("objection_pairs", []), ensure_ascii=False),
                "closing_phrases": json.dumps(deep.get("closing_phrases", []), ensure_ascii=False),
                "closing_technique": deep.get("closing_technique", ""),
                "cross_sell_approach": deep.get("cross_sell_approach") or "",
                "communication_style": deep.get("communication_style", ""),
                "key_success_factors": json.dumps(deep.get("key_success_factors", []), ensure_ascii=False),
                "reusable_templates": json.dumps(deep.get("reusable_templates", []), ensure_ascii=False),
            }
            deep_results.append(result)

            print(f"    Стиль: {result['communication_style'][:60]}...")

            # Сохраняем каждые 5 чатов
            if len(deep_results) % 5 == 0:
                rows = [[r.get(k, "") for k in deep_header] for r in deep_results[-5:]]
                append_to_worksheet(ss, "best_practices_deep", rows=rows, header=deep_header)
                print(f"    Сохранено {len(deep_results)} результатов")

            time.sleep(groq.adaptive_pause)

        except Exception as e:
            print(f"    Ошибка: {e}")
            errors += 1
            continue

    # Дописываем остаток
    remainder = len(deep_results) % 5
    if remainder > 0:
        rows = [[r.get(k, "") for k in deep_header] for r in deep_results[-remainder:]]
        append_to_worksheet(ss, "best_practices_deep", rows=rows, header=deep_header)

    print(f"\n  Глубоких анализов: {len(deep_results)} (ошибок: {errors})")
    return deep_results


# === Фаза 4: Агрегация и отчёт ===

def phase4_aggregate(ss, all_results: List[Dict], deep_results: List[Dict]):
    """Агрегирует техники и создает итоговый отчёт."""
    print(f"\n=== ФАЗА 4: Агрегация и отчёт ===\n")

    # Статистика по менеджерам
    by_manager = defaultdict(list)
    for r in all_results:
        name = r.get("manager_name", "Неизвестный")
        by_manager[name].append(r)

    # Лучшие приветствия из deep
    greetings = []
    needs_qs = []
    objections = []
    templates = []

    for d in deep_results:
        if d.get("best_greeting"):
            greetings.append({
                "phrase": d["best_greeting"],
                "why": d.get("greeting_why_works", ""),
                "manager": d.get("manager_name", ""),
                "score": d.get("overall_score", 0),
            })

        # Вопросы
        try:
            qs = json.loads(d.get("needs_questions", "[]"))
            for q in qs:
                needs_qs.append({"question": q, "manager": d.get("manager_name", "")})
        except Exception:
            pass

        # Возражения
        try:
            pairs = json.loads(d.get("objection_pairs", "[]"))
            for p in pairs:
                if isinstance(p, dict):
                    objections.append({**p, "manager": d.get("manager_name", "")})
        except Exception:
            pass

        # Шаблоны
        try:
            tmpls = json.loads(d.get("reusable_templates", "[]"))
            for t in tmpls:
                if isinstance(t, dict):
                    templates.append({**t, "manager": d.get("manager_name", "")})
        except Exception:
            pass

    # Формируем сводную таблицу
    summary_rows = [["Категория", "Содержание", "Менеджер", "Score"]]

    for g in greetings[:20]:
        summary_rows.append(["Приветствие", f"{g['phrase']} — {g['why']}", g["manager"], g["score"]])

    for q in needs_qs[:30]:
        summary_rows.append(["Вопрос для выявления потребностей", q["question"], q["manager"], ""])

    for o in objections[:30]:
        summary_rows.append([
            "Работа с возражением",
            f"Клиент: {o.get('objection', '')} → Менеджер: {o.get('response', '')} ({o.get('technique', '')})",
            o.get("manager", ""),
            "",
        ])

    for t in templates[:30]:
        summary_rows.append([
            "Шаблон",
            f"Ситуация: {t.get('situation', '')} → {t.get('template', '')}",
            t.get("manager", ""),
            "",
        ])

    # Записываем сводку
    upsert_worksheet(ss, "best_practices_summary", rows=summary_rows)
    print(f"  Записано в best_practices_summary: {len(summary_rows) - 1} записей")

    # Telegram-отчёт
    lines = ["<b>Лучшие практики продаж INSTINTO</b>\n"]
    lines.append(f"Проанализировано: {len(all_results)} чатов")
    lines.append(f"Глубокий анализ: {len(deep_results)} лучших чатов\n")

    # Топ менеджеры
    lines.append("<b>Топ менеджеры (по среднему баллу):</b>")
    manager_avgs = {}
    for name, chats in by_manager.items():
        scores = [_safe_float(c.get("overall_score", 0)) for c in chats if _safe_float(c.get("overall_score", 0)) > 0]
        if scores:
            manager_avgs[name] = (round(sum(scores) / len(scores), 1), len(scores))
    for name, (avg, cnt) in sorted(manager_avgs.items(), key=lambda x: -x[1][0]):
        lines.append(f"  {name}: {avg}/10 ({cnt} чатов)")

    # Топ техники
    lines.append(f"\nПриветствий: {len(greetings)}")
    lines.append(f"Вопросов: {len(needs_qs)}")
    lines.append(f"Работа с возражениями: {len(objections)}")
    lines.append(f"Шаблонов: {len(templates)}")

    if greetings:
        lines.append(f"\n<b>Лучшее приветствие:</b>")
        best = max(greetings, key=lambda g: _safe_float(g.get("score", 0)))
        lines.append(f'"{best["phrase"]}"')
        lines.append(f"— {best['manager']} (score: {best['score']})")

    text = "\n".join(lines)
    send_telegram(ADMIN_ID, text)
    print(f"\n  Отчёт отправлен в Telegram")

    return {
        "total_analyzed": len(all_results),
        "deep_analyzed": len(deep_results),
        "greetings": len(greetings),
        "questions": len(needs_qs),
        "objections": len(objections),
        "templates": len(templates),
    }


# === Main ===

def main():
    print(f"=== Извлечение лучших практик из исторических чатов ===")
    print(f"Время: {time_utils.now().strftime('%Y-%m-%d %H:%M MSK')}")
    print(f"Конфигурация: max_candidates={MAX_CANDIDATES}, max_deep={MAX_DEEP}")
    print(f"              batch_size={BATCH_SIZE}, pause={PAUSE_SEC}с\n")

    try:
        sheets_id = os.environ.get("GOOGLE_SHEETS_ID")
        sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        if not sheets_id or not sa_json:
            raise ValueError("GOOGLE_SHEETS_ID или GOOGLE_SERVICE_ACCOUNT_JSON не заданы")

        ss = open_spreadsheet(spreadsheet_id=sheets_id, service_account_json_path=sa_json)
        groq = LLMClient()  # мультипровайдер: cerebras -> groq автофоллбэк

        # Фаза 1: Скрининг
        candidates = phase1_screening(ss)
        if not candidates:
            print("Нет кандидатов для анализа!")
            send_telegram(ADMIN_ID, "Best Practices: нет кандидатов для анализа")
            return

        # Фаза 2: LLM анализ (обрабатывает до BP_MAX_PER_RUN чатов за запуск)
        all_results, remaining = phase2_analysis(ss, groq, candidates)
        if not all_results:
            print("Нет результатов анализа!")
            send_telegram(ADMIN_ID, "Best Practices: нет результатов анализа")
            return

        if remaining > 0:
            # Ещё не все обработаны — пропускаем фазы 3-4, ждём следующий запуск
            msg = (f"Best Practices: обработано {len(all_results)} чатов, "
                   f"осталось {remaining}. Следующий запуск продолжит.")
            print(msg)
            send_telegram(ADMIN_ID, msg)
            return

        # Все кандидаты обработаны — фазы 3 и 4
        # Фаза 3: Глубокий анализ
        deep_results = phase3_deep_analysis(ss, groq, all_results)

        # Фаза 4: Агрегация
        stats = phase4_aggregate(ss, all_results, deep_results)

        alert_success(
            "extract-best-practices",
            "Анализ лучших практик завершён",
            stats,
        )

    except Exception as e:
        alert_error(
            service_name="extract-best-practices",
            error=e,
            context="Критическая ошибка извлечения лучших практик"
        )
        raise


if __name__ == "__main__":
    main()
