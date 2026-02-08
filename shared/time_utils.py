"""
Централизованная работа со временем в Moscow timezone.

ПРАВИЛО: ВСЕ операции с датой/временем должны использовать этот модуль.

Использование:
    from shared.time_utils import now, today, MSK

    current_time = now()  # datetime с MSK timezone
    current_date = today()  # date
"""

from datetime import datetime, date
from zoneinfo import ZoneInfo

MSK = ZoneInfo("Europe/Moscow")


def now() -> datetime:
    """Возвращает текущее время в Moscow timezone."""
    return datetime.now(MSK)


def today() -> date:
    """Возвращает текущую дату в Moscow timezone."""
    return now().date()


def utc_now() -> datetime:
    """Возвращает текущее время в UTC (для совместимости с legacy кодом)."""
    from datetime import timezone
    return datetime.now(timezone.utc)
