#!/usr/bin/env python3
"""取引所の営業日カレンダー算出用共通関数。"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Iterable, List, Optional, Set, Union

DateLike = Union[date, datetime]


def _to_date(value: DateLike) -> date:
    if isinstance(value, datetime):
        return value.date()
    return value


def _normalize_holidays(extra_holidays: Optional[Iterable[DateLike]]) -> Set[date]:
    if extra_holidays is None:
        return set()
    return {_to_date(day) for day in extra_holidays}


def is_exchange_holiday(
    target_date: DateLike,
    extra_holidays: Optional[Iterable[DateLike]] = None,
) -> bool:
    """取引所休場日を判定する。

    休場日の定義:
    - 土日
    - 年末年始（12/31, 1/1, 1/2, 1/3）
    - extra_holidays で渡された任意休場日
    """
    day = _to_date(target_date)

    if day.weekday() >= 5:
        return True

    if (day.month == 12 and day.day == 31) or (day.month == 1 and day.day in (1, 2, 3)):
        return True

    holidays = _normalize_holidays(extra_holidays)
    if day in holidays:
        return True

    return False


def is_exchange_business_day(
    target_date: DateLike,
    extra_holidays: Optional[Iterable[DateLike]] = None,
) -> bool:
    """取引所営業日かどうかを返す。"""
    return not is_exchange_holiday(target_date, extra_holidays)


def calculate_exchange_business_days(
    start_date: DateLike,
    end_date: DateLike,
    extra_holidays: Optional[Iterable[DateLike]] = None,
) -> List[date]:
    """開始日〜終了日（両端含む）の営業日一覧を返す。"""
    start = _to_date(start_date)
    end = _to_date(end_date)
    if start > end:
        return []

    holidays = _normalize_holidays(extra_holidays)
    output: List[date] = []
    current = start
    while current <= end:
        if not is_exchange_holiday(current, holidays):
            output.append(current)
        current += timedelta(days=1)

    return output


def shift_exchange_business_day(
    base_date: DateLike,
    offset: int,
    extra_holidays: Optional[Iterable[DateLike]] = None,
) -> date:
    """基準日から営業日ベースでoffset日移動した日付を返す。"""
    day = _to_date(base_date)
    holidays = _normalize_holidays(extra_holidays)

    if offset == 0:
        return day

    step = 1 if offset > 0 else -1
    remaining = abs(offset)
    current = day

    while remaining > 0:
        current += timedelta(days=step)
        if not is_exchange_holiday(current, holidays):
            remaining -= 1

    return current
