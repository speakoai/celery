"""One booking-time grid per location — restaurant booking maturity S7.1."""
from tasks.utils.availability_helpers import annotate_bookable_starts


def _venue(start, end, duration="90"):
    return {1: {"id": 1, "slots": [{"start": start, "end": end, "service_duration": duration}]}}


def test_default_counts_from_the_free_range_start():
    out = annotate_bookable_starts(_venue("12:40:00", "15:00:00"))
    assert out[1]["slots"][0]["bookable_starts"] == ["12:40", "12:55", "13:10", "13:25"]


def test_clock_aligned_15_minute_grid_after_a_turnover_gap():
    # 11:00–12:30 booking + 10 min turnover → table free from 12:40
    out = annotate_bookable_starts(_venue("12:40:00", "15:00:00"), 15, align_to_clock=True)
    assert out[1]["slots"][0]["bookable_starts"] == ["12:45", "13:00", "13:15", "13:30"]


def test_clock_aligned_10_minute_grid_and_on_grid_start():
    out = annotate_bookable_starts(_venue("17:00:00", "19:00:00", "90"), 10, align_to_clock=True)
    assert out[1]["slots"][0]["bookable_starts"] == ["17:00", "17:10", "17:20", "17:30"]


def test_off_grid_seconds_round_up_and_nothing_fits():
    out = annotate_bookable_starts(_venue("13:30:30", "15:00:00"), 15, align_to_clock=True)
    assert out[1]["slots"][0]["bookable_starts"] == []
