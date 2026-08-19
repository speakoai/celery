"""
Tests for the flexible-duration wording in customer- and merchant-facing SMS
(tasks/utils/display_format.py).

Pure formatting — no network, DB or Twilio.

The rule these pin: a FIXED booking says "12:15PM" and always has, because its
length belongs to the venue or the service and the customer never chose it. A
FLEXIBLE booking says the window and the length, because the customer picked
how long and the start alone does not say when the room stops being theirs.

Run:  python -m pytest test_flexible_duration_sms.py -q
"""

from datetime import datetime

from tasks.utils.display_format import (
    format_display_booking_window,
    format_display_datetime,
    format_display_duration,
)

START = datetime(2026, 8, 21, 12, 15)
END = datetime(2026, 8, 21, 14, 45)


class TestFormatDisplayDuration:
    def test_under_an_hour(self):
        assert format_display_duration(45) == "45min"

    def test_whole_hours_drop_the_minutes(self):
        assert format_display_duration(60) == "1h"
        assert format_display_duration(240) == "4h"

    def test_hours_and_minutes(self):
        assert format_display_duration(90) == "1h 30min"
        assert format_display_duration(150) == "2h 30min"

    def test_matches_the_web_helper_wording(self):
        # Mirrors formatDurationLabel in speako-web/src/lib/flexible-duration.ts,
        # so an SMS and the confirmation email read identically.
        assert format_display_duration(300) == "5h"
        assert format_display_duration(75) == "1h 15min"

    def test_missing_or_nonsense_yields_empty(self):
        for bad in (None, 0, -30, "abc"):
            assert format_display_duration(bad) == ""


class TestFormatDisplayBookingWindow:
    def test_flexible_states_the_window_and_the_length(self):
        assert (
            format_display_booking_window(START, END, 150, True)
            == "21 Aug 2026 (Fri) 12:15PM - 02:45PM (2h 30min)"
        )

    def test_fixed_is_unchanged(self):
        # The exact string every fixed booking has always sent.
        assert format_display_booking_window(START, END, 150, False) == (
            format_display_datetime(START)
        )

    def test_length_is_derived_when_duration_was_never_stored(self):
        # Rows written before every path persisted bookings.duration.
        assert (
            format_display_booking_window(START, END, None, True)
            == "21 Aug 2026 (Fri) 12:15PM - 02:45PM (2h 30min)"
        )

    def test_stored_duration_wins_over_the_window(self):
        assert "(1h)" in format_display_booking_window(START, END, 60, True)

    def test_falls_back_rather_than_emitting_half_a_sentence(self):
        # A missing end time must never produce "12:15PM -  ()".
        assert format_display_booking_window(START, None, None, True) == (
            format_display_datetime(START)
        )
        assert format_display_booking_window(None, END, 150, True) == ""

    def test_spans_midnight(self):
        late = datetime(2026, 8, 21, 23, 0)
        after = datetime(2026, 8, 22, 1, 30)
        assert (
            format_display_booking_window(late, after, 150, True)
            == "21 Aug 2026 (Fri) 11:00PM - 01:30AM (2h 30min)"
        )

    def test_noon_and_midnight_read_correctly(self):
        noon = datetime(2026, 8, 21, 12, 0)
        assert format_display_booking_window(noon, datetime(2026, 8, 21, 13, 0), 60, True) == (
            "21 Aug 2026 (Fri) 12:00PM - 01:00PM (1h)"
        )
