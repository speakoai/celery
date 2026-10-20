"""
Booking metrics cache — restaurant booking maturity S1.5.

Per-tenant Redis hash that the dashboard reads INSTEAD of querying `bookings`
on page load (Billy, 2026-10-20). It backs:
  - the "Upcoming" view of the Bookings overview chart (next 7/30/60 days)
  - the "Booking page sources" panel (past 90 days and upcoming)

Key:    booking_metrics:<CACHE_VERSION>:<tenant_id>          (speako-cache, REDIS_URL)
Fields: <location_id>|<YYYY-MM-DD>|ch|<band>      counted bookings that day, by channel band
        <location_id>|<YYYY-MM-DD>|ref|<referral> counted bookings whose ORIGIN is the booking
                                                  page, by where the guest came from
        _tz|<location_id>                         the location's IANA timezone, so the web
                                                  can tell the venue's "today" without a DB call
        _meta                                     {"version", "built_at"}

Who writes it:
  - the per-date availability rebuild (tasks/availability_gen_regen.py) — every
    booking make/modify/cancel from any source already triggers it for the
    affected date, so it recounts those dates here (16 of 19 booking writers
    reach it; the other three are covered by small snippets in voice-ai and
    speako-web, see docs/plans/restaurant-booking-maturity.md §S1.5)
  - the midnight full availability rebuild (next 60 days)
  - the 3am dashboard metrics job (past 180 days, then prune)

Totals, not increments: each refresh RECOUNTS the given dates and overwrites
their fields, so a missed, duplicated or retried trigger self-heals on the next
rebuild of that date. Zero counts are not stored.

Counting rules match the Bookings overview chart: statuses confirmed+completed,
date = start_time::date (local wall-clock), bands from CHANNEL_BANDS. The
referral comes from the first row of the reschedule chain (a reschedule inserts
a new row and overwrites `source`), walked via original_booking_id — never
booking_ref, which collides within a tenant.
"""

import json
from datetime import date, datetime, timedelta, timezone

CACHE_VERSION = "v1"

# Same statuses as the Bookings overview chart (generate_dashboard_metrics.COUNTED_STATUSES).
COUNTED_STATUSES_SQL = "('confirmed','completed')"

# Same source → band mapping as generate_dashboard_metrics.CHANNEL_BANDS, keyed
# by the short band name the cache uses. test_booking_metrics_cache.py asserts
# the two never drift.
CHANNEL_BANDS = (
    ("ai", ("voice-ai",)),
    ("web", ("web",)),
    ("dashboard", ("dashboard", "onboarding")),
    ("chat", ("facebook", "instagram", "web-widget")),
)
BANDS = tuple(band for band, _ in CHANNEL_BANDS)
REFERRALS = ("google", "instagram", "facebook", "website", "unknown")

PAST_DAYS = 180      # booking-page sources panel: 90-day Past view + the 90 days before it (change figure)
UPCOMING_DAYS = 60   # matches the full availability rebuild horizon

# Counted bookings in a date range for one location, grouped by what the cache
# needs. The LATERAL walks each row's reschedule chain to its first row (depth
# capped; the longest real chain on DEV is 4). Every step is a PK lookup.
COUNT_SQL = """
    SELECT to_char(b.start_time::date, 'YYYY-MM-DD') AS booking_date,
           b.source::text                            AS source,
           origin.origin_source,
           origin.origin_referral_source,
           COUNT(*)                                  AS n
      FROM bookings b
      LEFT JOIN LATERAL (
        WITH RECURSIVE chain AS (
          SELECT o.booking_id, o.original_booking_id, o.source, o.referral_source, 0 AS depth
            FROM bookings o
           WHERE o.tenant_id = b.tenant_id AND o.booking_id = b.booking_id
          UNION ALL
          SELECT p.booking_id, p.original_booking_id, p.source, p.referral_source, c.depth + 1
            FROM chain c
            JOIN bookings p ON p.tenant_id = b.tenant_id AND p.booking_id = c.original_booking_id
           WHERE c.depth < 50
        )
        SELECT source::text AS origin_source, referral_source AS origin_referral_source
          FROM chain ORDER BY depth DESC LIMIT 1
      ) origin ON true
     WHERE b.tenant_id = %s
       AND b.location_id = %s
       AND b.start_time >= %s::date
       AND b.start_time < (%s::date + 1)
       AND b.status IN """ + COUNTED_STATUSES_SQL + """
     GROUP BY 1, 2, 3, 4
"""


def cache_key(tenant_id):
    return f"booking_metrics:{CACHE_VERSION}:{tenant_id}"


def band_for_source(source):
    """Channel band for a booking source, or None (counted in no band — same as the chart)."""
    for band, sources in CHANNEL_BANDS:
        if source in sources:
            return band
    return None


def referral_key(referral):
    if referral in ("google", "instagram", "facebook", "website"):
        return referral
    if referral == "other":
        return "website"
    return "unknown"


def _as_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def date_range(start, end):
    """Inclusive list of 'YYYY-MM-DD' strings."""
    d, last = _as_date(start), _as_date(end)
    out = []
    while d <= last:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def field_names(location_id, day):
    """Every field a (location, date) can own — deleted before rewriting that date."""
    return [f"{location_id}|{day}|ch|{b}" for b in BANDS] + [
        f"{location_id}|{day}|ref|{r}" for r in REFERRALS
    ]


def compute_cells(location_id, rows):
    """
    rows: (booking_date, source, origin_source, origin_referral_source, n)
    Returns {field: count} for non-zero cells.
    """
    cells = {}
    for booking_date, source, origin_source, origin_referral, n in rows:
        n = int(n or 0)
        if n <= 0:
            continue
        band = band_for_source(source)
        if band:
            f = f"{location_id}|{booking_date}|ch|{band}"
            cells[f] = cells.get(f, 0) + n
        if origin_source == "web":
            f = f"{location_id}|{booking_date}|ref|{referral_key(origin_referral)}"
            cells[f] = cells.get(f, 0) + n
    return cells


def write_cells(redis_client, tenant_id, location_id, days, cells, now=None, location_tz=None):
    """Atomically replace the given dates' fields for one location."""
    key = cache_key(tenant_id)
    stale = [f for day in days for f in field_names(location_id, day)]
    pipe = redis_client.pipeline(transaction=True)
    if stale:
        pipe.hdel(key, *stale)
    if cells:
        pipe.hset(key, mapping={f: int(v) for f, v in cells.items()})
    if location_tz:
        pipe.hset(key, f"_tz|{location_id}", str(location_tz))
    built_at = (now or datetime.now(timezone.utc)).isoformat()
    pipe.hset(key, "_meta", json.dumps({"version": CACHE_VERSION, "built_at": built_at}))
    pipe.execute()


def refresh_booking_metrics(cur, redis_client, tenant_id, location_id, start, end, location_tz=None):
    """
    Recount [start, end] (inclusive, local dates) for one location and overwrite
    those dates in the tenant's hash. Returns the number of non-zero cells.
    `cur` is an open psycopg2 cursor; callers own the connection.
    """
    days = date_range(start, end)
    if not days:
        return 0
    cur.execute(COUNT_SQL, (tenant_id, location_id, days[0], days[-1]))
    cells = compute_cells(location_id, cur.fetchall())
    write_cells(redis_client, tenant_id, location_id, days, cells, location_tz=location_tz)
    return len(cells)


def prune_booking_metrics(redis_client, tenant_id, oldest_day):
    """Delete fields dated before `oldest_day` (YYYY-MM-DD). Returns how many were removed."""
    key = cache_key(tenant_id)
    oldest = _as_date(oldest_day).isoformat()
    doomed = []
    for field, _ in redis_client.hscan_iter(key, count=500):
        parts = str(field).split("|")
        if field.startswith("_"):
            continue  # _meta, _tz|<location>
        if len(parts) >= 2 and parts[1] < oldest:
            doomed.append(field)
    for i in range(0, len(doomed), 500):
        redis_client.hdel(key, *doomed[i : i + 500])
    return len(doomed)
