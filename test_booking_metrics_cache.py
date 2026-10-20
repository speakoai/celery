"""
Tests for tasks/utils/booking_metrics_cache.py (restaurant booking maturity S1.5).

Run:  venv/bin/python -m pytest test_booking_metrics_cache.py -q
"""

import ast
import json
from datetime import date
from pathlib import Path

from tasks.utils import booking_metrics_cache as bmc


class FakePipeline:
    def __init__(self, store):
        self.store, self.ops = store, []

    def hdel(self, key, *fields):
        self.ops.append(("hdel", key, fields))

    def hset(self, key, field=None, value=None, mapping=None):
        self.ops.append(("hset", key, field, value, mapping))

    def execute(self):
        for op in self.ops:
            if op[0] == "hdel":
                h = self.store.setdefault(op[1], {})
                for f in op[2]:
                    h.pop(f, None)
            else:
                _, key, field, value, mapping = op
                h = self.store.setdefault(key, {})
                if mapping:
                    h.update({k: str(v) for k, v in mapping.items()})
                if field is not None:
                    h[field] = str(value)


class FakeRedis:
    def __init__(self):
        self.store = {}

    def pipeline(self, transaction=True):
        return FakePipeline(self.store)

    def hscan_iter(self, key, count=None):
        return list(self.store.get(key, {}).items())

    def hdel(self, key, *fields):
        for f in fields:
            self.store.get(key, {}).pop(f, None)


class FakeCursor:
    def __init__(self, rows):
        self.rows, self.executed = rows, []

    def execute(self, sql, params):
        self.executed.append(params)

    def fetchall(self):
        return self.rows


def _chart_constant(name):
    """
    Read a module-level literal from tasks/generate_dashboard_metrics.py without
    importing it — task modules import tasks.celery_app, which can't be imported
    in a plain test run (same convention as test_sms_templates.py).
    """
    tree = ast.parse(Path(__file__).with_name("tasks").joinpath("generate_dashboard_metrics.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == name for t in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found in generate_dashboard_metrics.py")


def test_bands_match_the_bookings_overview_chart():
    """The cache and the existing chart must classify sources identically."""
    chart = {field.replace("bookings_", ""): tuple(sources) for field, sources in _chart_constant("CHANNEL_BANDS")}
    cache = {band: tuple(sources) for band, sources in bmc.CHANNEL_BANDS}
    assert cache == chart


def test_counted_statuses_match_the_chart():
    assert bmc.COUNTED_STATUSES_SQL == _chart_constant("COUNTED_STATUSES")


def test_band_and_referral_mapping():
    assert bmc.band_for_source("voice-ai") == "ai"
    assert bmc.band_for_source("onboarding") == "dashboard"
    assert bmc.band_for_source("web-widget") == "chat"
    assert bmc.band_for_source("walk-in") is None  # not a chart band yet — same as the chart
    assert bmc.referral_key("other") == "website"
    assert bmc.referral_key(None) == "unknown"


def test_compute_cells_counts_bands_and_origin_referrals():
    rows = [
        ("2026-11-11", "web", "web", "google", 2),
        ("2026-11-11", "web", "web", None, 1),
        # Made online via Instagram, later rescheduled by staff: the band follows
        # the row's latest source (like the chart), the referral follows origin.
        ("2026-11-11", "dashboard", "web", "instagram", 1),
        ("2026-11-12", "voice-ai", "voice-ai", None, 3),
    ]
    cells = bmc.compute_cells(34, rows)
    assert cells == {
        "34|2026-11-11|ch|web": 3,
        "34|2026-11-11|ref|google": 2,
        "34|2026-11-11|ref|unknown": 1,
        "34|2026-11-11|ch|dashboard": 1,
        "34|2026-11-11|ref|instagram": 1,
        "34|2026-11-12|ch|ai": 3,
    }


def test_refresh_overwrites_totals_and_clears_dates_that_emptied():
    r = FakeRedis()
    key = bmc.cache_key(2)
    # Stale state: a booking on 11 Nov that has since been cancelled, plus a
    # field for another location that must be left alone.
    r.store[key] = {"34|2026-11-11|ch|web": "5", "35|2026-11-11|ch|web": "7"}
    cur = FakeCursor([("2026-11-12", "voice-ai", "voice-ai", None, 1)])

    cells = bmc.refresh_booking_metrics(cur, r, 2, 34, date(2026, 11, 11), date(2026, 11, 13), "Australia/Sydney")

    assert cells == 1
    assert cur.executed == [(2, 34, "2026-11-11", "2026-11-13")]
    h = r.store[key]
    assert "34|2026-11-11|ch|web" not in h  # recounted to zero → removed
    assert h["34|2026-11-12|ch|ai"] == "1"
    assert h["35|2026-11-11|ch|web"] == "7"  # other location untouched
    assert json.loads(h["_meta"])["version"] == bmc.CACHE_VERSION
    assert h["_tz|34"] == "Australia/Sydney"


def test_refresh_is_idempotent():
    r = FakeRedis()
    rows = [("2026-11-11", "web", "web", "google", 2)]
    bmc.refresh_booking_metrics(FakeCursor(rows), r, 2, 34, "2026-11-11", "2026-11-11")
    bmc.refresh_booking_metrics(FakeCursor(rows), r, 2, 34, "2026-11-11", "2026-11-11")
    assert r.store[bmc.cache_key(2)]["34|2026-11-11|ref|google"] == "2"  # not 4


def test_prune_removes_only_older_dates():
    r = FakeRedis()
    key = bmc.cache_key(2)
    r.store[key] = {
        "34|2026-07-01|ch|ai": "1",
        "34|2026-07-23|ch|ai": "1",
        "34|2026-11-11|ref|google": "1",
        "_meta": "{}",
        "_tz|34": "Australia/Sydney",
    }
    assert bmc.prune_booking_metrics(r, 2, "2026-07-23") == 1
    assert set(r.store[key]) == {"34|2026-07-23|ch|ai", "34|2026-11-11|ref|google", "_meta", "_tz|34"}


def test_date_range_is_inclusive_and_crosses_years():
    assert bmc.date_range("2025-12-31", "2026-01-02") == ["2025-12-31", "2026-01-01", "2026-01-02"]
    assert bmc.date_range("2026-01-02", "2026-01-01") == []
