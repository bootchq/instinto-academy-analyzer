from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List, Sequence

import gspread
from google.oauth2.service_account import Credentials


def open_spreadsheet(*, spreadsheet_id: str, service_account_json_path: str) -> gspread.Spreadsheet:
    """
    Открывает Google Spreadsheet.
    
    Поддерживает:
    - Путь к JSON-файлу (локально)
    - JSON-строку в переменной окружения (Railway)
    """
    import json
    import tempfile
    
    # Проверяем, это путь к файлу или JSON-строка
    if service_account_json_path.strip().startswith("{"):
        # Это JSON-строка (Railway)
        try:
            json_data = json.loads(service_account_json_path)
            # Создаём временный файл
            temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
            json.dump(json_data, temp_file)
            temp_file.close()
            json_path = temp_file.name
        except json.JSONDecodeError:
            raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON должен быть либо путём к файлу, либо валидным JSON")
    else:
        # Это путь к файлу
        json_path = service_account_json_path
    
    try:
        creds = Credentials.from_service_account_file(
            json_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        gc = gspread.authorize(creds)
        return gc.open_by_key(spreadsheet_id)
    finally:
        # Удаляем временный файл, если он был создан
        if service_account_json_path.strip().startswith("{") and os.path.exists(json_path):
            try:
                os.unlink(json_path)
            except Exception:
                pass


def upsert_worksheet(
    ss: gspread.Spreadsheet,
    title: str,
    *,
    rows: Sequence[Sequence[Any]],
    clear: bool = True,
) -> None:
    try:
        ws = ss.worksheet(title)
    except gspread.WorksheetNotFound:
        # минимальные размеры; gspread сам расширит при update
        ws = ss.add_worksheet(title=title, rows=200, cols=40)

    if clear:
        ws.clear()
    if not rows:
        return
    ws.update(values=list(rows), range_name="A1")


def append_to_worksheet(
    ss: gspread.Spreadsheet,
    title: str,
    *,
    rows: Sequence[Sequence[Any]],
    header: List[str] | None = None,
) -> None:
    """Добавляет строки в существующий лист (не очищает его)."""
    try:
        ws = ss.worksheet(title)
    except gspread.WorksheetNotFound:
        # Создаём новый лист с большим размером
        ws = ss.add_worksheet(title=title, rows=10000, cols=50)
        # Записываем заголовок, если передан
        if header:
            ws.update(values=[header], range_name="A1")
            existing_values = [header]
        else:
            existing_values = []
    else:
        existing_values = ws.get_all_values()
        # Если лист пустой и передан заголовок, добавляем его
        if not existing_values and header:
            ws.update(values=[header], range_name="A1")
            existing_values = [header]
        
        # Проверяем и увеличиваем размер листа, если нужно
        try:
            current_rows = ws.row_count
            current_cols = ws.col_count
            needed_rows = len(existing_values) + len(rows) + 100  # +100 для запаса
            needed_cols = max(len(header) if header else 0, max((len(r) for r in rows), default=0)) + 5
            
            if needed_rows > current_rows or needed_cols > current_cols:
                # Увеличиваем размер листа
                ws.resize(rows=max(needed_rows, 10000), cols=max(needed_cols, 50))
        except Exception as e:
            # Если не получилось увеличить, продолжаем (может быть ограничение API)
            pass
    
    # Добавляем новые строки
    if rows:
        next_row = len(existing_values) + 1
        # Используем batch_update для больших объёмов (более надёжно)
        if len(rows) > 100:
            # Для больших объёмов используем batch_update
            body = {
                "valueInputOption": "RAW",
                "data": [{
                    "range": f"{title}!A{next_row}",
                    "values": list(rows)
                }]
            }
            ws.spreadsheet.values_batch_update(body)
        else:
            # Для малых объёмов используем обычный update
            ws.update(values=list(rows), range_name=f"A{next_row}")


def get_existing_chat_ids(ss: gspread.Spreadsheet, worksheet_name: str = "chats_raw") -> set:
    """Читает chat_id уже обработанных чатов из таблицы."""
    try:
        ws = ss.worksheet(worksheet_name)
        values = ws.get_all_values()
        if not values or len(values) < 2:
            return set()
        header = values[0]
        chat_id_idx = header.index("chat_id") if "chat_id" in header else None
        if chat_id_idx is None:
            return set()
        existing_ids = set()
        for row in values[1:]:
            if chat_id_idx < len(row) and row[chat_id_idx]:
                existing_ids.add(str(row[chat_id_idx]).strip())
        return existing_ids
    except Exception as e:
        print(f"⚠️ Ошибка при чтении существующих чатов: {e}")
        return set()


def dicts_to_table(dict_rows: Iterable[Dict[str, Any]], *, header: List[str]) -> List[List[Any]]:
    out: List[List[Any]] = [header]
    for r in dict_rows:
        out.append([r.get(k, "") for k in header])
    return out


