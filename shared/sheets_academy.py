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


# === Функции для работы с пользователями ===

USERS_HEADER = ["telegram_id", "name", "username", "role", "status", "requested_at", "approved_at", "approved_by"]


def get_user(ss: gspread.Spreadsheet, telegram_id: int | str) -> Dict[str, Any] | None:
    """Получает пользователя по telegram_id."""
    try:
        ws = ss.worksheet("users")
        values = ws.get_all_values()
        if len(values) < 2:
            return None

        header = values[0]
        tid_idx = header.index("telegram_id") if "telegram_id" in header else 0

        for row in values[1:]:
            if tid_idx < len(row) and str(row[tid_idx]) == str(telegram_id):
                return {header[i]: row[i] for i in range(min(len(header), len(row)))}
        return None
    except gspread.WorksheetNotFound:
        return None
    except Exception as e:
        print(f"Ошибка при получении пользователя: {e}")
        return None


def get_all_users(ss: gspread.Spreadsheet) -> List[Dict[str, Any]]:
    """Получает всех пользователей."""
    try:
        ws = ss.worksheet("users")
        values = ws.get_all_values()
        if len(values) < 2:
            return []

        header = values[0]
        users = []
        for row in values[1:]:
            if row and row[0]:  # есть telegram_id
                users.append({header[i]: row[i] for i in range(min(len(header), len(row)))})
        return users
    except gspread.WorksheetNotFound:
        return []
    except Exception as e:
        print(f"Ошибка при получении пользователей: {e}")
        return []


def create_access_request(
    ss: gspread.Spreadsheet,
    telegram_id: int | str,
    name: str,
    username: str | None = None
) -> bool:
    """Создаёт заявку на доступ."""
    from datetime import datetime, timezone

    try:
        # Проверяем, нет ли уже заявки
        existing = get_user(ss, telegram_id)
        if existing:
            return False

        row = [
            str(telegram_id),
            name,
            username or "",
            "",  # role - пусто пока не одобрено
            "pending",  # status
            datetime.now(timezone.utc).isoformat(),  # requested_at
            "",  # approved_at
            ""   # approved_by
        ]

        append_to_worksheet(ss, "users", rows=[row], header=USERS_HEADER)
        return True
    except Exception as e:
        print(f"Ошибка при создании заявки: {e}")
        return False


def approve_user(
    ss: gspread.Spreadsheet,
    telegram_id: int | str,
    role: str,
    approved_by: int | str
) -> bool:
    """Одобряет пользователя и назначает роль."""
    from datetime import datetime, timezone

    try:
        ws = ss.worksheet("users")
        values = ws.get_all_values()
        if len(values) < 2:
            return False

        header = values[0]
        tid_idx = header.index("telegram_id") if "telegram_id" in header else 0
        role_idx = header.index("role") if "role" in header else 3
        status_idx = header.index("status") if "status" in header else 4
        approved_at_idx = header.index("approved_at") if "approved_at" in header else 6
        approved_by_idx = header.index("approved_by") if "approved_by" in header else 7

        for row_num, row in enumerate(values[1:], start=2):
            if tid_idx < len(row) and str(row[tid_idx]) == str(telegram_id):
                # Обновляем ячейки
                ws.update_cell(row_num, role_idx + 1, role)
                ws.update_cell(row_num, status_idx + 1, "approved")
                ws.update_cell(row_num, approved_at_idx + 1, datetime.now(timezone.utc).isoformat())
                ws.update_cell(row_num, approved_by_idx + 1, str(approved_by))
                return True
        return False
    except Exception as e:
        print(f"Ошибка при одобрении пользователя: {e}")
        return False


def reject_user(ss: gspread.Spreadsheet, telegram_id: int | str) -> bool:
    """Отклоняет заявку пользователя."""
    try:
        ws = ss.worksheet("users")
        values = ws.get_all_values()
        if len(values) < 2:
            return False

        header = values[0]
        tid_idx = header.index("telegram_id") if "telegram_id" in header else 0
        status_idx = header.index("status") if "status" in header else 4

        for row_num, row in enumerate(values[1:], start=2):
            if tid_idx < len(row) and str(row[tid_idx]) == str(telegram_id):
                ws.update_cell(row_num, status_idx + 1, "rejected")
                return True
        return False
    except Exception as e:
        print(f"Ошибка при отклонении пользователя: {e}")
        return False


def get_pending_requests(ss: gspread.Spreadsheet) -> List[Dict[str, Any]]:
    """Получает список заявок на рассмотрении."""
    users = get_all_users(ss)
    return [u for u in users if u.get("status") == "pending"]


