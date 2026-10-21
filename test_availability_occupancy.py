"""Occupancy helper — restaurant booking maturity S4. Run: venv/bin/python -m pytest test_availability_occupancy.py"""
from datetime import datetime

import pytest

from tasks.utils.availability_helpers import (
    OCCUPYING_STATUSES_SQL,
    fetch_occupancy,
    fetch_staff_occupancy,
    fetch_table_occupancy,
)


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.sql = None
        self.params = None

    def execute(self, sql, params):
        self.sql, self.params = sql, params

    def fetchall(self):
        return self.rows


ROWS = [(41, 7, datetime(2026, 10, 21, 12, 0), datetime(2026, 10, 21, 12, 45))]


def test_table_occupancy_shape_and_query():
    cur = FakeCursor(ROWS)
    out = fetch_table_occupancy(cur, 2, 34, "2026-10-21")
    assert out == [{"venue_unit_id": 41, "customer_id": 7, "start_time": "2026-10-21 12:00:00", "end_time": "2026-10-21 12:45:00"}]
    assert "SELECT venue_unit_id, customer_id, start_time, end_time" in cur.sql
    assert f"status IN {OCCUPYING_STATUSES_SQL}" in cur.sql
    assert cur.params == (2, 34, "2026-10-21", "2026-10-21")


def test_staff_occupancy_uses_staff_column():
    cur = FakeCursor(ROWS)
    out = fetch_staff_occupancy(cur, 1, 5, "2026-10-21")
    assert out[0]["staff_id"] == 41 and "SELECT staff_id," in cur.sql


def test_occupying_statuses_mirror_the_db_exclude():
    assert OCCUPYING_STATUSES_SQL == "('confirmed', 'pending_guarantee')"


def test_rejects_unknown_columns():
    with pytest.raises(ValueError):
        fetch_occupancy(FakeCursor([]), 1, 1, "2026-10-21", "customer_id; DROP TABLE x")
