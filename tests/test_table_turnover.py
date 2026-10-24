"""S7 — turnover, needs clearing and overrunning seated parties in table occupancy."""

from tasks.utils.availability_helpers import apply_table_hold_rules

DAY = "2026-11-17"


def row(start, end, status="confirmed", seated=False, unit=50):
    return {"venue_unit_id": unit, "start_time": f"{DAY} {start}:00", "end_time": f"{DAY} {end}:00", "status": status, "seated": seated}


def held(rows, **kw):
    return [(o["start_time"][11:16], o["end_time"][11:16]) for o in apply_table_hold_rules(rows, DAY, **kw)]


def test_turnover_off_is_unchanged():
    assert held([row("18:00", "19:45")]) == [("18:00", "19:45")]
    assert held([row("18:00", "19:45", status="completed")]) == []  # finished frees at once


def test_turnover_pads_both_sides():
    assert held([row("18:00", "19:45")], turnover_minutes=20) == [("17:40", "20:05")]


def test_finished_booking_needs_clearing():
    # Finish records the departure as end_time; the table needs T minutes
    assert held([row("18:00", "18:30", status="completed")], turnover_minutes=20) == [("18:30", "18:50")]


def test_overrunning_seated_party_keeps_the_table():
    r = row("18:00", "19:45", seated=True)
    assert held([r], now_wall=f"{DAY} 19:50:00") == [("18:00", "20:05")]                      # now + 15
    assert held([r], turnover_minutes=20, now_wall=f"{DAY} 19:50:00") == [("17:40", "20:25")]
    assert held([r], now_wall=f"{DAY} 19:00:00") == [("18:00", "19:45")]                       # not over yet
    assert held([row("18:00", "19:45")], now_wall=f"{DAY} 19:50:00") == [("18:00", "19:45")]   # not seated: no hold


def test_padding_is_clipped_to_the_day():
    assert held([row("00:10", "01:00")], turnover_minutes=20) == [("00:00", "01:20")]
    assert held([row("23:00", "23:50")], turnover_minutes=20) == [("22:40", "23:59")]


def test_forgotten_finish_stops_holding_after_the_cap():
    r = row("12:00", "13:45", seated=True)
    assert held([r], now_wall=f"{DAY} 16:50:00") == [("12:00", "13:45")]  # > 3 h past end: forgotten
