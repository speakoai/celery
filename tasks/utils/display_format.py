"""
House display format for customer- and merchant-facing text (SMS bodies).

    date      -> "15 Jul 2026 (Tue)"
    time      -> "07:00PM"
    datetime  -> "15 Jul 2026 (Tue) 07:00PM"

⚠️ DISPLAY ONLY. Never use these for Redis keys, cache payloads, DB writes,
metric bucket keys, R2 object names, or anything the AI agent reads — those
are wire formats and must keep their existing `strftime` patterns. In
particular, agent knowledge deliberately uses 24-hour times (the agent reads
it aloud); see docs/plans/date-time-display-standardization.md §5.3.

Why the month/weekday names are hardcoded rather than `%b` / `%a` / `%p`:
those `strftime` directives are locale-dependent, so a worker started under a
non-C locale can silently emit "juil."/"mar."/"pm". Hardcoding keeps every
message identical regardless of the worker's environment.

Booking timestamps are naive local wall-clock values (`timestamp without time
zone`); these helpers format them as-is and never convert timezones.
"""

from datetime import date as _date, datetime as _datetime
from typing import Optional, Union

_MONTHS_SHORT = (
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
)

# Monday == 0, matching datetime.weekday()
_WEEKDAYS_SHORT = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


def format_display_date(value: Optional[Union[_datetime, _date]]) -> str:
    """Format a date as "15 Jul 2026 (Tue)". Returns "" for None."""
    if value is None:
        return ""
    return (
        f"{value.day:02d} {_MONTHS_SHORT[value.month - 1]} {value.year:04d} "
        f"({_WEEKDAYS_SHORT[value.weekday()]})"
    )


def format_display_time(value: Optional[_datetime]) -> str:
    """
    Format a time as 12-hour, zero-padded, no space before the meridiem.

    e.g. "07:00PM", "09:35AM", "12:00AM" (midnight), "12:00PM" (noon).
    Returns "" for None.
    """
    if value is None:
        return ""
    hour24 = value.hour
    hour12 = hour24 % 12 or 12
    meridiem = "AM" if hour24 < 12 else "PM"
    return f"{hour12:02d}:{value.minute:02d}{meridiem}"


def format_display_datetime(value: Optional[_datetime]) -> str:
    """Format as "15 Jul 2026 (Tue) 07:00PM". Returns "" for None."""
    if value is None:
        return ""
    return f"{format_display_date(value)} {format_display_time(value)}"
