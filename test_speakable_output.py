"""
Unit tests for the speakable formatting helpers in tasks/sync_speako_data.py.

These are pure functions with no DB or network dependency, so this runs
standalone:  python test_speakable_output.py

Covers the two defects from docs/plans/speakable-agent-output/:
  1. 24-hour times spoken as digit strings ("18:00" -> "one eight zero zero")
  2. emails read as pseudo-words, and hyphen-joined spellings losing letters
"""

import sys
import types

# The helpers under test are pure, but their module imports Celery and the task
# DB layer at import time. Stub those so this file runs anywhere — including
# environments where the celery/kombu pairing in venv/ is broken.
for _name in ("celery", "celery.utils", "celery.utils.log"):
    sys.modules.setdefault(_name, types.ModuleType(_name))
sys.modules["celery.utils.log"].get_task_logger = lambda *_a, **_k: types.SimpleNamespace(
    info=lambda *a, **k: None,
    warning=lambda *a, **k: None,
    error=lambda *a, **k: None,
)

_celery_app = types.ModuleType("tasks.celery_app")
_celery_app.app = types.SimpleNamespace(task=lambda *_a, **_k: (lambda fn: fn))
sys.modules["tasks.celery_app"] = _celery_app

_task_db = types.ModuleType("tasks.utils.task_db")
for _fn in ("mark_task_running", "mark_task_failed", "mark_task_succeeded",
            "upsert_tenant_integration_param"):
    setattr(_task_db, _fn, lambda *a, **k: None)
sys.modules["tasks.utils.task_db"] = _task_db

from tasks.sync_speako_data import (
    _format_day_hours_markdown,
    _format_exceptions_markdown,
    _format_staff_day_hours_markdown,
    _format_week_schedule_markdown,
    _speakable_clock,
    _speakable_email,
    _speakable_hours,
    _speakable_website,
    _spell_out,
)

_failures = []


def check(label, actual, expected):
    if actual == expected:
        print(f"  ok   {label}")
    else:
        print(f"  FAIL {label}\n         expected: {expected!r}\n         actual:   {actual!r}")
        _failures.append(label)


# ── _speakable_clock ────────────────────────────────────────────────────────
print("_speakable_clock")
check("on the hour drops :00", _speakable_clock(7, 0), "7 AM")
check("minutes preserved", _speakable_clock(9, 30), "9:30 AM")
check("afternoon", _speakable_clock(16, 0), "4 PM")
check("evening with minutes", _speakable_clock(20, 45), "8:45 PM")
check("midnight", _speakable_clock(0, 0), "midnight")
check("24:00 is also midnight", _speakable_clock(24, 0), "midnight")
check("noon", _speakable_clock(12, 0), "noon")
check("just past midnight", _speakable_clock(0, 30), "12:30 AM")
check("just past noon", _speakable_clock(12, 15), "12:15 PM")

# ── _speakable_hours ────────────────────────────────────────────────────────
print("_speakable_hours")
check(
    "en dash range (what the DB layer emits)",
    _speakable_hours("07:00–11:00"),
    "7 AM to 11 AM",
)
check("hyphen range", _speakable_hours("07:00-11:00"), "7 AM to 11 AM")
check("em dash range", _speakable_hours("07:00—11:00"), "7 AM to 11 AM")
check(
    "slot label preserved",
    _speakable_hours("16:00–20:00 (Temple Services)"),
    "4 PM to 8 PM (Temple Services)",
)
check(
    "multiple ranges in one string",
    _speakable_hours("07:00–11:00 (Temple Services), 16:00–20:00 (Temple Services)"),
    "7 AM to 11 AM (Temple Services), 4 PM to 8 PM (Temple Services)",
)
check("overnight range", _speakable_hours("22:00–02:00"), "10 PM to 2 AM")
check("range ending at midnight", _speakable_hours("18:00–00:00"), "6 PM to midnight")
check("half-hour boundaries", _speakable_hours("09:30–17:30"), "9:30 AM to 5:30 PM")
check("Closed passes through", _speakable_hours("Closed"), "Closed")
check("Off Duty passes through", _speakable_hours("Off Duty"), "Off Duty")
check("empty string", _speakable_hours(""), "")
check("no range present", _speakable_hours("By appointment only"), "By appointment only")

# Idempotency — knowledge re-syncs are frequent, so f(f(x)) must equal f(x).
_once = _speakable_hours("07:00–11:00 (Temple Services), 16:00–20:00 (Temple Services)")
check("idempotent", _speakable_hours(_once), _once)
check("idempotent (minutes)", _speakable_hours(_speakable_hours("09:30–17:30")), "9:30 AM to 5:30 PM")

# ── markdown call sites ─────────────────────────────────────────────────────
print("markdown call sites")
check(
    "_format_day_hours_markdown",
    _format_day_hours_markdown(["07:00–11:00 (Temple Services)", "16:00–20:00 (Temple Services)"]),
    "7 AM to 11 AM (Temple Services), 4 PM to 8 PM (Temple Services)",
)
check("_format_day_hours_markdown empty -> Closed", _format_day_hours_markdown([]), "Closed")
check(
    "_format_staff_day_hours_markdown",
    _format_staff_day_hours_markdown(["09:00–17:00"]),
    "9 AM to 5 PM",
)
check("_format_staff_day_hours_markdown empty -> Off Duty", _format_staff_day_hours_markdown([]), "Off Duty")
check(
    "_format_week_schedule_markdown (Monday line)",
    _format_week_schedule_markdown({"mon": ["07:00–11:00 (Temple Services)"]}).split("\n")[0],
    "- **Monday**: 7 AM to 11 AM (Temple Services)",
)
check(
    "_format_week_schedule_markdown (missing day -> Closed)",
    _format_week_schedule_markdown({"mon": ["07:00–11:00"]}).split("\n")[6],
    "- **Sunday**: Closed",
)
check(
    "_format_exceptions_markdown special hours",
    _format_exceptions_markdown(
        [{"date": "2026-12-25", "type": "special_hours", "status": "open", "hours": ["10:00–14:00"]}]
    ),
    "- **2026-12-25**: 10 AM to 2 PM (Special Hours)",
)
check(
    "_format_exceptions_markdown closure keeps holiday name",
    _format_exceptions_markdown(
        [{"date": "2026-12-25", "type": "public_holiday", "status": "closed", "holiday_name": "Christmas Day"}]
    ),
    "- **2026-12-25**: Closed (Christmas Day)",
)

# ── _spell_out ──────────────────────────────────────────────────────────────
print("_spell_out")
check(
    "the reported email local part",
    _spell_out("secretaryssds"),
    "S. E. C., R. E. T., A. R. Y., S. S. D. S.",
)
check("no hyphens anywhere", "-" in _spell_out("secretaryssds"), False)
check("short run", _spell_out("aj"), "A. J.")
check("exact multiple of three", _spell_out("abc"), "A. B. C.")
check(
    "orphan letter merged into previous group",
    _spell_out("abcd"),
    "A. B. C. D.",
)
check("two full groups", _spell_out("abcdef"), "A. B. C., D. E. F.")
check("digits spoken", _spell_out("ab12"), "A. B. 1. 2.")
check("dot spoken as a word", _spell_out("a.b"), "A. dot B.")
check("underscore spoken", _spell_out("a_b"), "A. underscore B.")
check("whitespace ignored", _spell_out("a b"), "A. B.")
check("empty", _spell_out(""), "")

# ── _speakable_email ────────────────────────────────────────────────────────
print("_speakable_email")
check(
    "the production defect",
    _speakable_email("secretaryssds@gmail.com"),
    "S. E. C., R. E. T., A. R. Y., S. S. D. S., at gmail dot com",
)
check(
    "multi-label domain",
    _speakable_email("info@temple.org.au"),
    "I. N. F. O., at temple dot org dot au",
)
check(
    "dotted local part",
    _speakable_email("john.smith@gmail.com"),
    "J. O. H., N. dot S., M. I. T. H., at gmail dot com",
)
check("surrounding whitespace tolerated", _speakable_email("  a@b.com  "), "A., at b dot com")
check("not an email", _speakable_email("not-an-email"), "")
check("empty", _speakable_email(""), "")
check("none", _speakable_email(None), "")

# ── _speakable_website ──────────────────────────────────────────────────────
print("_speakable_website")
check("scheme and www stripped", _speakable_website("https://www.example.com"), "example dot com")
check("bare host", _speakable_website("example.org.au"), "example dot org dot au")
check("path dropped", _speakable_website("https://example.com/contact/us"), "example dot com")
check("http scheme", _speakable_website("http://example.com/"), "example dot com")
check("empty", _speakable_website(""), "")
check("none", _speakable_website(None), "")

print()
if _failures:
    print(f"{len(_failures)} FAILED: {', '.join(_failures)}")
    sys.exit(1)
print("All speakable-output tests passed.")
