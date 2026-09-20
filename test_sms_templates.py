"""
Segment budget for the customer SMS templates (WP2).

These templates were rewritten to fit ONE segment, and the margin is small —
a real HRT reminder lands at 156 of the 160 characters a single GSM-7 segment
holds. Wording changes are therefore priced, not free: adding "your booking"
back, or restoring the year, pushes ~900 messages a month into a second
segment. This file exists so that shows up as a failing test rather than on
the Twilio invoice.

Shapes are duplicated from tasks/sms.py deliberately — tasks/sms.py imports
tasks.celery_app, which needs a broker, so it cannot be imported in a plain
unit test. `test_gsm7.py::test_every_sms_builder_folds_its_fields` guards the
other half of that duplication.

Run:  python -m pytest test_sms_templates.py -q
"""

from datetime import datetime

import pytest

from tasks.utils.display_format import format_display_booking_window
from tasks.utils.gsm7 import sms_segments, to_gsm7

# One real production booking from HRT Acupuncture, September 2026. The staff
# name carries the Chinese honorific that made this tenant 80% of the bill.
START = datetime(2026, 9, 7, 10, 0)
END = datetime(2026, 9, 7, 10, 30)
REF = "3382"
LOCATION_SHORT = "HRT Sunnybank"                      # locations.sms_display_name
LOCATION_FULL = "HRT Acupuncture and Massage Centre -Sunnybank Plaza"
STAFF_RAW = "Juliana Chang 張醫師"
SERVICE = "General Consultation Initial"
SERVICE_LONGEST = "Acupuncture with Herbal Consultation Follow up"
URL = "https://speako.ai/m/" + "X" * 8               # 8-char base62 short code
SIGNATURE = " [Speako AI]"


def _when(compact=True):
    return format_display_booking_window(START, END, None, False, compact=compact)


def _service_body(kind, location=LOCATION_SHORT, service=SERVICE):
    staff = to_gsm7(STAFF_RAW)
    svc = to_gsm7(service, titles=False)
    bodies = {
        "confirm": f"Ref {REF} confirmed: {location} on {_when()} with {staff} for {svc}.",
        "remind": f"Reminder: Ref {REF} at {location} on {_when()} with {staff} for {svc}.",
        "cancel": f"Ref {REF} at {location} on {_when()} with {staff} for {svc} CANCELLED.",
        "modify": f"Ref {REF} moved: {location} on {_when()} with {staff} for {svc}.",
    }
    return bodies[kind] + f" {URL}" + SIGNATURE


@pytest.mark.parametrize("kind", ["confirm", "remind", "cancel", "modify"])
def test_the_typical_service_booking_fits_one_segment(kind):
    body = _service_body(kind)
    assert sms_segments(body) == 1, f"{len(body)} chars: {body}"


def test_the_signature_survived_the_rewrite():
    """Owner decision: shorten everything EXCEPT '[Speako AI]'."""
    assert _service_body("confirm").endswith(SIGNATURE)


def test_the_weekday_survived_but_the_year_did_not():
    """Owner decision D1. The weekday is how people sanity-check an
    appointment; the year is always inferable."""
    when = _when()
    assert "(Mon)" in when
    assert "2026" not in when


def test_the_honorific_is_kept_as_an_english_title():
    assert "Dr Juliana Chang" in _service_body("confirm")
    assert "張" not in _service_body("confirm")


def test_a_restaurant_booking_fits_one_segment():
    body = (
        f"Ref {REF} for 4 confirmed: Beyond Chinese on {_when()}."
        f" {URL}{SIGNATURE}"
    )
    assert sms_segments(body) == 1, f"{len(body)} chars: {body}"


# --- the margin, stated explicitly -------------------------------------------

def test_the_longest_real_service_name_is_the_known_two_segment_case():
    """
    "Acupuncture with Herbal Consultation Follow up" is 45 characters and does
    not fit. 249 of 1674 rendered September bodies are in this bucket; the fix
    is a shorter service name, not a shorter template.
    """
    assert sms_segments(_service_body("remind", service=SERVICE_LONGEST)) == 2


def test_a_null_short_name_still_beats_the_old_template():
    """
    sms_display_name is nullable and the builders fall back to locations.name,
    so WP2 can ship before any name is written. That fallback costs a second
    segment, but the pre-WP1 body cost four.
    """
    fallback = _service_body("remind", location=LOCATION_FULL)
    assert sms_segments(fallback) == 2

    old = (
        f"Reminder: Hi Juecha, your booking (Ref: {REF}) at {LOCATION_FULL} is on "
        f"{format_display_booking_window(START, END, None, False)} with {STAFF_RAW} "
        f"for {SERVICE}. Manage your booking: https://tinyurl.com/mr2jvuy7{SIGNATURE}"
    )
    assert sms_segments(old) == 4


def test_restoring_any_one_dropped_element_costs_a_segment():
    """
    The budget has no slack: each of the pieces removed in WP2 is on its own
    enough to break the single-segment fit. This is why the rewrite was
    all-or-nothing.
    """
    base = _service_body("confirm")
    assert sms_segments(base) == 1

    with_greeting = "Hi Juecha, " + base
    with_year = base.replace("07 Sep (Mon)", "07 Sep 2026 (Mon)")
    with_label = base.replace(f" {URL}", f" Manage your booking: {URL}")
    with_long_ref = base.replace(f"Ref {REF}", f"your booking (Ref: {REF})")

    for name, variant in [
        ("greeting", with_greeting),
        ("year", with_year),
        ("link label", with_label),
        ("long ref phrase", with_long_ref),
    ]:
        assert sms_segments(variant) == 2, f"{name} unexpectedly still fits"
