"""Per-period "Last booking at" + "Shorten to closing time" — restaurant booking maturity S8."""
from tasks.utils.availability_helpers import annotate_bookable_starts, period_length

DINNER = {"start": 17 * 60, "end": 21 * 60 + 30, "last_start": None, "shorten": False, "min": None}


def _p(**kw):
    return {**DINNER, **kw}


def _venue(start="17:00:00", end="21:30:00", duration="105"):
    return {1: {"id": 1, "slots": [{"start": start, "end": end, "service_duration": duration}]}}


def _ann(periods, **kw):
    out = annotate_bookable_starts(_venue(**kw), 15, align_to_clock=True, periods=periods)
    return out[1]["slots"][0]


def test_no_rules_is_today():
    today = annotate_bookable_starts(_venue(), 15, align_to_clock=True)
    assert _ann([])["bookable_starts"] == today[1]["slots"][0]["bookable_starts"]
    assert today[1]["slots"][0]["bookable_starts"][-1] == "19:45"


def test_period_length_rule():
    assert period_length(19 * 60 + 45, 105, _p()) == 105
    assert period_length(20 * 60, 105, _p(last_start=20 * 60)) == 105  # refused later by the fit check
    assert period_length(20 * 60 + 15, 105, _p(last_start=20 * 60, shorten=True)) is None
    assert period_length(20 * 60, 105, _p(shorten=True, min=60)) == 90
    assert period_length(20 * 60 + 45, 105, _p(shorten=True, min=60)) == 105  # 45 left < 60 → not shortened
    assert period_length(21 * 60 + 15, 105, _p(shorten=True)) == 15  # no minimum → any time left


def test_last_start_only_keeps_19_45_and_refuses_20_00():
    slot = _ann([_p(last_start=20 * 60)])
    assert slot["bookable_starts"][-1] == "19:45"
    assert "durations" not in slot


def test_shorten_to_close_with_last_start_and_minimum():
    slot = _ann([_p(last_start=20 * 60, shorten=True, min=60)])
    assert slot["bookable_starts"][-2:] == ["19:45", "20:00"]
    assert slot["durations"] == {"20:00": 90}


def test_shorten_without_minimum_or_last_start():
    slot = _ann([_p(shorten=True)])
    assert slot["bookable_starts"][-1] == "21:15"
    assert slot["durations"]["21:15"] == 15


def test_no_squeeze_before_a_booking():
    # Table booked from 21:00: the free gap ends 21:00, so 20:00 (shortened to 21:30) doesn't fit
    slot = _ann([_p(last_start=20 * 60, shorten=True, min=60)], end="21:00:00")
    assert "20:00" not in slot["bookable_starts"]
    assert slot["bookable_starts"][-1] == "19:15"
