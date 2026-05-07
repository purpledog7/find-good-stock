from __future__ import annotations

from datetime import date, datetime, timedelta


DATE_FORMAT = "%Y-%m-%d"


def normalize_date(value: str | date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    for date_format in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(value, date_format).date()
        except ValueError:
            continue
    raise ValueError("날짜는 YYYY-MM-DD 또는 YYYYMMDD 형식이어야 해.")


def is_trading_day(value: str | date | datetime) -> bool:
    target = normalize_date(value)
    if target.weekday() >= 5:
        return False
    return target not in korean_holidays(target.year)


def next_trading_day(value: str | date | datetime, include_current: bool = True) -> str:
    current = normalize_date(value)
    if not include_current:
        current += timedelta(days=1)
    while not is_trading_day(current):
        current += timedelta(days=1)
    return current.strftime(DATE_FORMAT)


def add_trading_days(value: str | date | datetime, days: int) -> str:
    current = normalize_date(value)
    added = 0
    while added < days:
        current += timedelta(days=1)
        if is_trading_day(current):
            added += 1
    return current.strftime(DATE_FORMAT)


def korean_holidays(year: int) -> set[date]:
    try:
        import holidays

        return set(holidays.KR(years=[year]).keys())
    except Exception:
        return fallback_korean_holidays(year)


def fallback_korean_holidays(year: int) -> set[date]:
    # Fixed-date holidays only. The optional holidays package adds lunar holidays.
    return {
        date(year, 1, 1),
        date(year, 3, 1),
        date(year, 5, 5),
        date(year, 6, 6),
        date(year, 8, 15),
        date(year, 10, 3),
        date(year, 10, 9),
        date(year, 12, 25),
    }
