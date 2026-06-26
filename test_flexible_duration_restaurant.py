"""Tests for restaurant venue-wide flexible duration (Option B, R2 + R3).

Covers:
  - resolve_location_flex_bounds: the pure predicate the availability writer
    (gen_availability_venue) uses to decide venue-wide flexibility (valid /
    disabled / invalid-bounds).
  - build_flexible_activity_prompt_line: the auto-derived per-location prompt
    line (present for a flexible rest location, None otherwise, optional
    location_info.ai_prompt override). Because callers append this to the
    system prompt BEFORE building source-hash inputs, "line present" is the
    source-hash/idempotency coverage too.

Runs with the celery venv (no pytest needed): `venv/bin/python test_flexible_duration_restaurant.py`
Also discoverable by pytest as test_* functions.
"""

import os
import sys
import types
import logging
import contextlib

# The committed venv's `celery` is incompatible with this Python (a pre-existing
# importlib.metadata breakage), so stub the celery bits the import chain needs.
# This lets us import the PURE helpers under test without a working broker.
_fake_celery = types.ModuleType("celery")
_fake_celery.Celery = object
sys.modules.setdefault("celery", _fake_celery)
_log = types.ModuleType("celery.utils.log")
_log.get_task_logger = lambda name=None: logging.getLogger(name or "test")
sys.modules.setdefault("celery.utils", types.ModuleType("celery.utils"))
sys.modules["celery.utils.log"] = _log


class _FakeApp:
    """Stub Celery app: @app.task and @app.task(...) both return the function."""
    def task(self, *a, **k):
        if len(a) == 1 and callable(a[0]) and not k:
            return a[0]
        return lambda f: f


_celery_app = types.ModuleType("tasks.celery_app")
_celery_app.app = _FakeApp()
sys.modules["tasks.celery_app"] = _celery_app

# The import chain has a top-level debug print of DB/Redis URLs — suppress it so
# no credentials reach stdout/CI logs (the values live only in .env).
with contextlib.redirect_stdout(open(os.devnull, "w")):
    from tasks.availability_gen_regen import resolve_location_flex_bounds
    from tasks.utils import publish_db
    from tasks.utils.publish_db import build_flexible_activity_prompt_line


# ── R2: resolve_location_flex_bounds ──────────────────────────────────────────

def test_bounds_valid():
    assert resolve_location_flex_bounds(True, 60, 240, 60) == {"min": 60, "max": 240, "increment": 60}

def test_bounds_disabled_is_none():
    assert resolve_location_flex_bounds(False, 60, 240, 60) is None

def test_bounds_min_gt_max_is_none():
    assert resolve_location_flex_bounds(True, 240, 60, 60) is None

def test_bounds_min_not_divisible_is_none():
    assert resolve_location_flex_bounds(True, 90, 240, 60) is None

def test_bounds_max_not_divisible_is_none():
    assert resolve_location_flex_bounds(True, 60, 250, 60) is None

def test_bounds_increment_zero_is_none():
    assert resolve_location_flex_bounds(True, 60, 240, 0) is None

def test_bounds_missing_is_none():
    assert resolve_location_flex_bounds(True, None, 240, 60) is None

def test_bounds_coerces_ints():
    out = resolve_location_flex_bounds(True, 60, 120, 60)
    assert all(isinstance(v, int) for v in out.values())


# ── R3: build_flexible_activity_prompt_line (stubbed DB) ─────────────────────

class _FakeCursor:
    def __init__(self, row): self._row = row
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, *a, **k): pass
    def fetchone(self): return self._row

class _FakeConn:
    def __init__(self, row): self._row = row
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def cursor(self): return _FakeCursor(self._row)
    def close(self): pass

def _line_for(row):
    """Run the real helper with _get_conn stubbed to return `row`."""
    original = publish_db._get_conn
    publish_db._get_conn = lambda: _FakeConn(row)
    try:
        return build_flexible_activity_prompt_line(1, 1)
    finally:
        publish_db._get_conn = original

# row order: location_type, enabled, label, min, max, increment, ai_prompt

def test_line_present_for_flexible_rest():
    line = _line_for(("rest", True, "Karaoke Room", 60, 240, 60, None))
    assert line is not None
    assert "Karaoke Room" in line
    assert "from 60 to 240 minutes" in line
    assert "60-minute" in line
    assert "duration_minutes" in line

def test_line_none_for_service_location():
    assert _line_for(("service", True, "X", 60, 240, 60, None)) is None

def test_line_none_when_disabled():
    assert _line_for(("rest", False, "X", 60, 240, 60, None)) is None

def test_line_none_when_bounds_invalid():
    assert _line_for(("rest", True, "X", 240, 60, 60, None)) is None      # min>max
    assert _line_for(("rest", True, "X", 90, 240, 60, None)) is None      # wrong step

def test_line_none_when_no_row():
    assert _line_for(None) is None

def test_line_override_appended_after_baseline():
    line = _line_for(("rest", True, "Karaoke", 60, 120, 60, "Rooms include 2 free drinks."))
    assert line is not None
    assert "duration_minutes" in line                       # baseline present
    assert line.rstrip().endswith("Rooms include 2 free drinks.")  # override layered after

def test_line_blank_label_falls_back():
    line = _line_for(("rest", True, "   ", 60, 120, 60, None))
    assert line is not None and "this activity" in line


if __name__ == "__main__":
    import sys
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"  FAIL {t.__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
