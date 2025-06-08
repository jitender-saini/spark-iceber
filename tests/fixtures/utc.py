from datetime import datetime, timezone
from typing import Optional


def datetime_utc(year: int, month: Optional[int] = None, day: Optional[int] = None, hour=0, minute=0, second=0, microsecond=0):
    return datetime(year, month, day, hour, minute, second, microsecond, tzinfo=timezone.utc)


def as_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc)
