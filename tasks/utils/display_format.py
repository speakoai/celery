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


def format_display_duration(total_minutes: Optional[int]) -> str:
    """
    Format a length as "2h 30min" / "1h" / "45min". Returns "" for None or a
    non-positive value.

    Mirrors `formatDurationLabel` in speako-web's `src/lib/flexible-duration.ts`
    so a customer sees the same wording in an SMS and in the confirmation email.
    """
    if total_minutes is None:
        return ""
    try:
        minutes = int(total_minutes)
    except (TypeError, ValueError):
        return ""
    if minutes <= 0:
        return ""
    if minutes < 60:
        return f"{minutes}min"
    hours, rem = divmod(minutes, 60)
    return f"{hours}h" if rem == 0 else f"{hours}h {rem}min"


def format_display_booking_window(
    start_time: Optional[_datetime],
    end_time: Optional[_datetime],
    duration_minutes: Optional[int],
    is_flexible: bool,
) -> str:
    """
    How a booking's time is stated to the customer.

    A FIXED booking says "21 Aug 2026 (Fri) 12:15PM" and always has — the length
    belongs to the venue or the service and the customer never chose it.

    A FLEXIBLE booking is the opposite: the customer picked how long, so the
    start alone does not say when the room stops being theirs. Those read
    "21 Aug 2026 (Fri) 12:15PM - 02:45PM (2h 30min)".

    Falls back to the fixed form whenever the extra parts cannot be derived, so
    a missing end time can never produce a half-finished sentence.
    """
    base = format_display_datetime(start_time)
    if not is_flexible or start_time is None:
        return base

    minutes = duration_minutes
    if not minutes and end_time is not None:
        minutes = int((end_time - start_time).total_seconds() // 60)

    length = format_display_duration(minutes)
    end_label = format_display_time(end_time)
    if not length or not end_label:
        return base

    return f"{base} - {end_label} ({length})"
