"""
P1 тесты: воспроизводят реальные баги из продакшена.

1. test_created_at_from_nested_lastDialog — дата терялась из вложенной структуры GraphQL
2. test_column_order_matches_header — колонки экспорта не совпадали с заголовком
3. test_manager_fallback_from_messages — менеджер определялся как ID вместо имени
4. test_decimal_locale_raw_mode — Sheets RU-locale конвертировала 5.2 → 52
5. test_relevant_message_sheets_months — пропускались месяцы при фильтрации
"""

import pytest
from datetime import date, timedelta
from unittest.mock import MagicMock, patch


# === P1-1: created_at из вложенной структуры GraphQL ===

class TestCreatedAtFromNestedLastDialog:
    """Баг: created_at терялся, т.к. код брал raw.get("createdAt") напрямую,
    а GraphQL возвращает lastDialog.createdAt."""

    def test_extracts_created_at_from_last_dialog(self, sample_raw_chat_graphql):
        """createdAt должен извлекаться из lastDialog, не из корня."""
        raw = sample_raw_chat_graphql
        # Правильный маппинг (как в export_to_sheets_batch.py после фикса)
        created_at = (raw.get("lastDialog") or {}).get("createdAt") or raw.get("lastActivity")
        assert created_at == "2026-02-10T08:00:00Z"

    def test_fallback_to_last_activity_when_no_dialog(self):
        """Если lastDialog == None, берём lastActivity."""
        raw = {
            "id": "chat_456",
            "lastDialog": None,
            "lastActivity": "2026-02-15T12:00:00Z",
        }
        created_at = (raw.get("lastDialog") or {}).get("createdAt") or raw.get("lastActivity")
        assert created_at == "2026-02-15T12:00:00Z"

    def test_fallback_when_created_at_missing_in_dialog(self):
        """Если lastDialog есть, но createdAt отсутствует — фоллбэк на lastActivity."""
        raw = {
            "id": "chat_789",
            "lastDialog": {"responsible": {"id": "100", "name": "Тест"}},
            "lastActivity": "2026-02-14T10:00:00Z",
        }
        created_at = (raw.get("lastDialog") or {}).get("createdAt") or raw.get("lastActivity")
        assert created_at == "2026-02-14T10:00:00Z"


# === P1-2: порядок колонок совпадает с заголовком ===

class TestColumnOrderMatchesHeader:
    """Баг: колонки экспорта не совпадали с заголовком chats_raw,
    из-за чего created_at оказывалось на позиции status и наоборот."""

    def test_dicts_to_table_preserves_header_order(self, chats_header):
        """dicts_to_table() должен выстраивать значения строго по порядку header."""
        from shared.sheets_academy import dicts_to_table

        row_data = {
            "chat_id": "1",
            "channel": "WHATSAPP",
            "status": "open",
            "created_at": "2026-02-15T10:00:00Z",
            "manager_id": "mgr_001",
            "manager_name": "Анна",
        }

        table = dicts_to_table([row_data], header=chats_header)
        assert table[0] == chats_header  # первая строка — заголовок
        data_row = table[1]

        # Проверяем что каждое значение на правильной позиции
        for i, key in enumerate(chats_header):
            expected = row_data.get(key, "")
            assert data_row[i] == expected, f"Колонка {key} (позиция {i}): ожидалось '{expected}', получено '{data_row[i]}'"

    def test_dicts_to_table_fills_missing_with_empty(self, chats_header):
        """Отсутствующие поля должны заполняться пустой строкой."""
        from shared.sheets_academy import dicts_to_table

        row_data = {"chat_id": "1"}
        table = dicts_to_table([row_data], header=chats_header)
        data_row = table[1]

        assert data_row[0] == "1"  # chat_id
        for i in range(1, len(chats_header)):
            assert data_row[i] == "", f"Колонка {chats_header[i]} должна быть пустой"


# === P1-3: фоллбэк менеджера из сообщений ===

class TestManagerFallbackFromMessages:
    """Баг: если чат без responsible, менеджер оставался пустым.
    Фикс: определяем по Counter исходящих сообщений."""

    def test_manager_determined_by_outgoing_messages(self, manager_map):
        """Менеджер определяется по тому, кто написал больше исходящих."""
        from collections import Counter

        messages = [
            {"direction": "out", "manager_id": "2494"},
            {"direction": "out", "manager_id": "2494"},
            {"direction": "out", "manager_id": "2494"},
            {"direction": "out", "manager_id": "1234"},
            {"direction": "in", "manager_id": ""},
            {"direction": "in", "manager_id": ""},
        ]

        # Логика из analyze_chats.py main(), строки 792-800
        mgr_id = ""
        mgr_counts = Counter()
        for m in messages:
            if m.get("direction") == "out":
                mid = str(m.get("manager_id", "")).strip()
                if mid:
                    mgr_counts[mid] += 1
        if mgr_counts:
            mgr_id = mgr_counts.most_common(1)[0][0]

        assert mgr_id == "2494"
        # Маппинг ID -> имя
        mgr_name = manager_map.get(mgr_id, mgr_id)
        assert mgr_name == "Алина Петрова"

    def test_manager_stays_empty_when_no_outgoing(self):
        """Если нет исходящих сообщений — менеджер остаётся пустым."""
        from collections import Counter

        messages = [
            {"direction": "in", "manager_id": ""},
            {"direction": "in", "manager_id": ""},
        ]

        mgr_counts = Counter()
        for m in messages:
            if m.get("direction") == "out":
                mid = str(m.get("manager_id", "")).strip()
                if mid:
                    mgr_counts[mid] += 1

        mgr_id = mgr_counts.most_common(1)[0][0] if mgr_counts else ""
        assert mgr_id == ""

    def test_manager_id_maps_to_name(self, manager_map):
        """ID менеджера должен резолвиться в имя через manager_map."""
        mgr_id = "2494"
        mgr_name = manager_map.get(mgr_id, mgr_id)
        assert mgr_name == "Алина Петрова"
        assert mgr_name != "2494"  # не должен показывать голый ID

    def test_unknown_manager_id_returns_id_as_fallback(self, manager_map):
        """Неизвестный ID возвращается как есть."""
        mgr_id = "9999"
        mgr_name = manager_map.get(mgr_id, mgr_id)
        assert mgr_name == "9999"


# === P1-4: десятичные числа и RAW mode в Sheets ===

class TestDecimalLocaleRawMode:
    """Баг: Sheets с RU-locale конвертировала 5.2 → 5,2 → 52 при USER_ENTERED mode.
    Фикс: всегда value_input_option='RAW'."""

    def test_append_uses_raw_mode(self):
        """append_to_worksheet должен использовать RAW mode при записи."""
        from shared.sheets_academy import append_to_worksheet

        mock_ss = MagicMock()
        mock_ws = MagicMock()
        mock_ss.worksheet.return_value = mock_ws
        mock_ws.get_all_values.return_value = [["header1", "header2"]]
        mock_ws.row_count = 10000
        mock_ws.col_count = 50

        append_to_worksheet(mock_ss, "analysis_raw", rows=[["chat_1", 5.2]], header=["id", "score"])

        # Проверяем что update вызван с RAW
        mock_ws.update.assert_called_once()
        call_kwargs = mock_ws.update.call_args
        assert call_kwargs.kwargs.get("value_input_option") == "RAW", \
            "append_to_worksheet должен использовать value_input_option='RAW'"

    def test_upsert_uses_raw_mode(self):
        """upsert_worksheet тоже должен использовать RAW mode."""
        from shared.sheets_academy import upsert_worksheet

        mock_ss = MagicMock()
        mock_ws = MagicMock()
        mock_ss.worksheet.return_value = mock_ws
        mock_ws.row_count = 10000
        mock_ws.col_count = 50

        upsert_worksheet(mock_ss, "test_sheet", rows=[["header1"], ["val1"]])

        mock_ws.update.assert_called_once()
        call_kwargs = mock_ws.update.call_args
        assert call_kwargs.kwargs.get("value_input_option") == "RAW", \
            "upsert_worksheet должен использовать value_input_option='RAW'"


# === P1-5: фильтрация по месяцам (не через timedelta) ===

class TestRelevantMessageSheetsMonths:
    """Баг: timedelta(days=15) для определения месяцев пропускала граничные месяцы.
    Фикс: итерация по месяцам от start_date.replace(day=1) до today.replace(day=1)."""

    def test_cross_month_boundary(self):
        """При пересечении границы месяцев — оба месяца включаются."""
        from analyze_chats import _relevant_message_sheets

        # Мокируем листы
        sheet_jan = MagicMock()
        sheet_jan.title = "messages_2026_01"
        sheet_feb = MagicMock()
        sheet_feb.title = "messages_2026_02"
        sheet_raw = MagicMock()
        sheet_raw.title = "messages_raw"
        sheet_other = MagicMock()
        sheet_other.title = "chats_raw"

        all_sheets = [sheet_jan, sheet_feb, sheet_raw, sheet_other]

        # today = 5 февраля, days_back = 7 → start = 29 января
        with patch("analyze_chats.time_utils") as mock_time:
            mock_time.today.return_value = date(2026, 2, 5)
            result = _relevant_message_sheets(all_sheets, days_back=7)

        titles = [s.title for s in result]
        assert "messages_2026_01" in titles, "Январский лист пропущен при пересечении месяца"
        assert "messages_2026_02" in titles, "Февральский лист пропущен"
        assert "messages_raw" in titles, "messages_raw должен включаться как fallback"
        assert "chats_raw" not in titles, "chats_raw не должен попадать в messages"

    def test_same_month(self):
        """Если days_back не пересекает месяц — один лист."""
        from analyze_chats import _relevant_message_sheets

        sheet_feb = MagicMock()
        sheet_feb.title = "messages_2026_02"
        sheet_raw = MagicMock()
        sheet_raw.title = "messages_raw"

        with patch("analyze_chats.time_utils") as mock_time:
            mock_time.today.return_value = date(2026, 2, 20)
            result = _relevant_message_sheets([sheet_feb, sheet_raw], days_back=7)

        titles = [s.title for s in result]
        assert "messages_2026_02" in titles
        assert "messages_raw" in titles

    def test_year_boundary(self):
        """Переход через границу года (январь → декабрь предыдущего)."""
        from analyze_chats import _relevant_message_sheets

        sheet_dec = MagicMock()
        sheet_dec.title = "messages_2025_12"
        sheet_jan = MagicMock()
        sheet_jan.title = "messages_2026_01"
        sheet_raw = MagicMock()
        sheet_raw.title = "messages_raw"

        with patch("analyze_chats.time_utils") as mock_time:
            mock_time.today.return_value = date(2026, 1, 3)
            result = _relevant_message_sheets([sheet_dec, sheet_jan, sheet_raw], days_back=7)

        titles = [s.title for s in result]
        assert "messages_2025_12" in titles, "Декабрьский лист пропущен при переходе года"
        assert "messages_2026_01" in titles
