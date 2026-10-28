"""Combinations in the availability cache — restaurant booking maturity S6a.
Run: venv/bin/python -m pytest test_combination_availability.py"""
from datetime import time

from tasks.utils.combination_availability import (
    combination_entries,
    combinations_for_day,
    intersect_slots,
    member_hours,
)


class FakeCursor:
    def __init__(self, rows):
        self.rows, self.sql, self.params = rows, None, None

    def execute(self, sql, params):
        self.sql, self.params = sql, params

    def fetchall(self):
        return self.rows


def slot(a, b, d="105"):
    return {"start": a, "end": b, "service_duration": d}


def test_intersection_is_time_every_member_is_free():
    t2 = [slot("17:00:00", "19:00:00"), slot("20:45:00", "21:30:00")]
    t3 = [slot("17:00:00", "21:30:00")]
    t4 = [slot("18:00:00", "21:30:00")]
    assert intersect_slots([t2, t3, t4]) == [slot("18:00:00", "19:00:00"), slot("20:45:00", "21:30:00")]
    assert intersect_slots([t2, []]) == []  # a member with no hours → no slots


def test_entries_get_bookable_starts_and_shape():
    [e] = combination_entries(
        [{"id": 8, "name": "2+3", "min_capacity": 3, "max_capacity": 4, "booking_rank": None, "member_ids": [36, 37]}],
        {36: [slot("17:00:00", "21:30:00")], 37: [slot("19:00:00", "21:30:00")]},
    )
    assert e["kind"] == "combination" and e["capacity"] == 4 and e["min_capacity"] == 3 and e["member_ids"] == [36, 37]
    assert e["slots"][0]["start"] == "19:00:00"
    assert e["slots"][0]["bookable_starts"][0] == "19:00" and e["slots"][0]["bookable_starts"][-1] == "19:45"


def test_member_hours_one_off_overrides_weekly_and_closed_means_none():
    rows = [
        (36, "recurring", False, time(17), time(21, 30), 105),
        (37, "one_time", False, time(18), time(20), 60),
        (37, "recurring", False, time(17), time(21, 30), 105),
        (38, "one_time", True, time(0), time(0), 0),
        (38, "recurring", False, time(17), time(21, 30), 105),
    ]
    h = member_hours(FakeCursor(rows), 2, 34, "2026-11-24", 2, [36, 37, 38, 39])
    assert h[36] == [slot("17:00:00", "21:30:00")]
    assert h[37] == [slot("18:00:00", "20:00:00", "60")]
    assert h[38] == [] and h[39] == []


def test_bookings_on_any_member_remove_the_time():
    rows = [(36, "recurring", False, time(17), time(21, 30), 105), (37, "recurring", False, time(17), time(21, 30), 105)]
    combos = [{"id": 8, "name": "2+3", "min_capacity": 3, "max_capacity": 4, "booking_rank": None, "member_ids": [36, 37]}]
    bookings = [{"venue_unit_id": 37, "customer_id": None, "start_time": "2026-11-24 19:00:00", "end_time": "2026-11-24 20:45:00"}]
    [e] = combinations_for_day(FakeCursor(rows), 2, 34, "2026-11-24", 2, combos, bookings)
    assert [(s["start"], s["end"]) for s in e["slots"]] == [("17:00:00", "19:00:00"), ("20:45:00", "21:30:00")]
