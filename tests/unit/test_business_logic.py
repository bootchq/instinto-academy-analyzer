"""
P2 тесты: бизнес-логика анализа чатов.

1. test_needs_reanalysis_logic — условия переанализа (новый/сообщения/статус)
2. test_filter_chats_min_messages — фильтр чатов с < 4 сообщениями
3. test_parse_llm_response — парсинг JSON из LLM (устойчивость к ошибкам)
"""

import pytest
from analyze_chats import (
    needs_reanalysis,
    parse_llm_response,
    validate_analysis,
    _safe_float,
    smart_truncate_messages,
    format_dialog,
)


# === P2-1: логика переанализа ===

class TestNeedsReanalysis:
    """Контролирует когда тратить Groq API на повторный анализ."""

    def test_new_chat(self):
        """Новый чат (не в analyzed) — всегда анализируем."""
        need, reason = needs_reanalysis("chat_new", 10, "open", analyzed={})
        assert need is True
        assert reason == "новый"

    def test_new_messages(self):
        """Новые сообщения — переанализируем."""
        analyzed = {"chat_1": {"message_count": 10, "chat_status": "open"}}
        need, reason = needs_reanalysis("chat_1", 15, "open", analyzed)
        assert need is True
        assert "10" in reason and "15" in reason

    def test_status_changed(self):
        """Статус изменился — переанализируем."""
        analyzed = {"chat_1": {"message_count": 10, "chat_status": "open"}}
        need, reason = needs_reanalysis("chat_1", 10, "closed", analyzed)
        assert need is True
        assert "open" in reason and "closed" in reason

    def test_no_changes(self):
        """Ничего не изменилось — не анализируем."""
        analyzed = {"chat_1": {"message_count": 10, "chat_status": "closed"}}
        need, reason = needs_reanalysis("chat_1", 10, "closed", analyzed)
        assert need is False
        assert reason == ""

    def test_empty_status_not_triggers(self):
        """Пустой новый статус не должен вызывать переанализ."""
        analyzed = {"chat_1": {"message_count": 10, "chat_status": "open"}}
        need, reason = needs_reanalysis("chat_1", 10, "", analyzed)
        assert need is False


# === P2-2: фильтр по минимальному количеству сообщений ===

class TestFilterChatsMinMessages:
    """Чаты с < 4 сообщениями не анализируются (шум)."""

    def _filter_chats(self, chats, analyzed):
        """Воспроизводит логику фильтрации из main() строки 688-704."""
        result = []
        for c in chats:
            msg_count = len(c["messages"])
            if msg_count < 4:
                continue
            chat_id = c["chat_id"]
            chat_status = c["chat"].get("status", "")
            need, reason = needs_reanalysis(chat_id, msg_count, chat_status, analyzed)
            if need:
                c["reanalysis_reason"] = reason
                result.append(c)
        return result

    def test_chat_with_6_messages_passes(self):
        """6 сообщений >= 4 — проходит."""
        chats = [{"chat_id": "1", "messages": [{}] * 6, "chat": {"status": "open"}}]
        result = self._filter_chats(chats, analyzed={})
        assert len(result) == 1

    def test_chat_with_3_messages_filtered(self):
        """3 сообщения < 4 — отфильтровывается."""
        chats = [{"chat_id": "2", "messages": [{}] * 3, "chat": {"status": "open"}}]
        result = self._filter_chats(chats, analyzed={})
        assert len(result) == 0

    def test_chat_with_4_messages_passes(self):
        """4 сообщения — граничное значение, проходит."""
        chats = [{"chat_id": "3", "messages": [{}] * 4, "chat": {"status": "open"}}]
        result = self._filter_chats(chats, analyzed={})
        assert len(result) == 1

    def test_mixed_chats(self):
        """Смешанный набор: проходят только с >= 4 сообщениями."""
        chats = [
            {"chat_id": "1", "messages": [{}] * 6, "chat": {"status": "open"}},
            {"chat_id": "2", "messages": [{}] * 3, "chat": {"status": "open"}},
            {"chat_id": "3", "messages": [{}] * 4, "chat": {"status": "closed"}},
            {"chat_id": "4", "messages": [{}] * 1, "chat": {"status": "open"}},
        ]
        result = self._filter_chats(chats, analyzed={})
        assert len(result) == 2
        assert {c["chat_id"] for c in result} == {"1", "3"}


# === P2-3: парсинг LLM ответа ===

class TestParseLlmResponse:
    """Парсер должен обрабатывать различные форматы ответа LLM."""

    def test_clean_json(self):
        """Чистый JSON парсится напрямую."""
        response = '{"overall_score": 5.2, "summary": "OK"}'
        result = parse_llm_response(response)
        assert result is not None
        assert result["overall_score"] == 5.2

    def test_markdown_wrapped_json(self):
        """JSON в markdown-обёртке ```json ... ```."""
        response = '```json\n{"overall_score": 5.2, "summary": "OK"}\n```'
        result = parse_llm_response(response)
        assert result is not None
        assert result["overall_score"] == 5.2

    def test_trailing_comma(self):
        """Trailing запятая перед } — исправляется."""
        response = '{"overall_score": 5.2, "summary": "OK",}'
        result = parse_llm_response(response)
        assert result is not None
        assert result["overall_score"] == 5.2

    def test_truncated_json_auto_close(self):
        """Обрезанный JSON (нет закрывающей скобки) — автодобавление }."""
        response = '{"overall_score": 5.2, "summary": "Хороший диалог"'
        result = parse_llm_response(response)
        assert result is not None
        assert result["overall_score"] == 5.2
        assert result["summary"] == "Хороший диалог"

    def test_invalid_response_returns_none(self):
        """Невосстановимый ответ возвращает None."""
        result = parse_llm_response("Я не могу ответить в формате JSON")
        assert result is None

    def test_empty_response(self):
        """Пустая строка возвращает None."""
        result = parse_llm_response("")
        assert result is None


# === Дополнительные: вспомогательные функции ===

class TestSafeFloat:
    """_safe_float обрабатывает разные форматы чисел."""

    def test_int(self):
        assert _safe_float(5) == 5.0

    def test_float(self):
        assert _safe_float(5.2) == 5.2

    def test_string_dot(self):
        assert _safe_float("5.2") == 5.2

    def test_string_comma(self):
        """Европейская локаль: запятая как десятичный разделитель."""
        assert _safe_float("5,2") == 5.2

    def test_invalid_string(self):
        assert _safe_float("invalid") == 0.0

    def test_none(self):
        assert _safe_float(None) == 0.0


class TestSmartTruncateMessages:
    """Умная обрезка сохраняет начало и конец диалога."""

    def test_short_dialog_unchanged(self):
        """Короткий диалог (< max) не обрезается."""
        messages = [{"text": f"msg_{i}", "direction": "in"} for i in range(10)]
        result = smart_truncate_messages(messages, max_messages=50)
        assert len(result) == 10

    def test_long_dialog_truncated(self):
        """Длинный диалог обрезается с маркером."""
        messages = [{"text": f"msg_{i}", "direction": "in"} for i in range(100)]
        result = smart_truncate_messages(messages, max_messages=50)
        # 5 первых + 1 маркер + 45 последних = 51
        assert len(result) == 51

    def test_skip_marker_present(self):
        """Маркер пропуска содержит количество пропущенных сообщений."""
        messages = [{"text": f"msg_{i}", "direction": "in"} for i in range(100)]
        result = smart_truncate_messages(messages, max_messages=50)
        marker = result[5]
        assert marker["direction"] == "system"
        assert "50" in marker["text"]  # пропущено 50 сообщений

    def test_format_dialog_with_skip_marker(self):
        """format_dialog правильно обрабатывает маркер пропуска."""
        messages = [
            {"direction": "in", "text": "Привет"},
            {"direction": "out", "text": "Здравствуйте"},
            {"direction": "system", "text": "[...пропущено 20 сообщений...]"},
            {"direction": "in", "text": "Спасибо"},
        ]
        result = format_dialog(messages)
        assert "Клиент: Привет" in result
        assert "Менеджер: Здравствуйте" in result
        assert "[...пропущено 20 сообщений...]" in result
        assert "Клиент: Спасибо" in result


class TestValidateAnalysis:
    """Валидация структуры LLM-ответа."""

    def _valid_analysis(self):
        return {
            "scores": {
                "greeting": 8, "needs_discovery": 7, "presentation": 6,
                "objection_handling": 5, "closing": 7, "cross_sell": 4,
            },
            "overall_score": 6.2,
            "customer_segment": "Невесты",
            "summary": "Хороший диалог",
        }

    def test_valid_passes(self):
        valid, reason = validate_analysis(self._valid_analysis())
        assert valid is True
        assert reason == ""

    def test_missing_scores_fails(self):
        analysis = self._valid_analysis()
        del analysis["scores"]
        valid, reason = validate_analysis(analysis)
        assert valid is False
        assert "scores" in reason

    def test_score_out_of_range_fails(self):
        analysis = self._valid_analysis()
        analysis["scores"]["greeting"] = 15
        valid, reason = validate_analysis(analysis)
        assert valid is False
        assert "greeting" in reason

    def test_empty_summary_fails(self):
        analysis = self._valid_analysis()
        analysis["summary"] = ""
        valid, reason = validate_analysis(analysis)
        assert valid is False
        assert "summary" in reason
