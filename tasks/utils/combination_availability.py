# tasks/utils/combination_availability.py
"""
Table combinations in the availability cache — restaurant booking maturity S6a.

A combination ("virtual table", speako-web S5) is free only when ALL its
member tables are free: each member's own hours for the day (a one-off entry
overrides the weekly one; a closed one-off day means no hours), minus every
booking holding that member (booking_venue_units — a member held as the extra
table of another combination counts too).

Written per date under a separate `combinations` key, so voice-ai and the
online page — which book `tables[].id` as a venue_unit_id — ignore them until
S6b teaches them to book a combination. Member tables marked walk-in only are
still used here (the venue chose to let guests book that combination), but
never appear on their own.

Left out: switched-off and walk-in-only combinations, and any combination with
a switched-off member.
"""

from datetime import timedelta

from tasks.utils.availability_helpers import (
    annotate_bookable_starts,
    reconstruct_venue_availability,
)


def load_bookable_combinations(cur, tenant_id, location_id):
    """Active, externally bookable combinations whose member tables are all active."""
    cur.execute(
        """
        SELECT c.combination_id, c.name, c.min_capacity, c.max_capacity, c.booking_rank,
               array_agg(m.venue_unit_id ORDER BY m.venue_unit_id) AS member_ids
          FROM venue_unit_combinations c
          JOIN venue_unit_combination_members m
            ON m.tenant_id = c.tenant_id AND m.combination_id = c.combination_id
          JOIN venue_unit vu
            ON vu.tenant_id = m.tenant_id AND vu.venue_unit_id = m.venue_unit_id
         WHERE c.tenant_id = %s AND c.location_id = %s
           AND c.is_active AND NOT c.is_walk_in
         GROUP BY c.tenant_id, c.combination_id
        HAVING bool_and(vu.is_active) AND bool_and(vu.location_id = c.location_id)
         ORDER BY c.booking_rank NULLS LAST, c.max_capacity, c.name
        """,
        (tenant_id, location_id),
    )
    return [
        {
            "id": r[0],
            "name": r[1],
            "min_capacity": r[2],
            "max_capacity": r[3],
            "booking_rank": r[4],
            "member_ids": list(r[5]),
        }
        for r in cur.fetchall()
    ]


def member_hours(cur, tenant_id, location_id, day, db_day, unit_ids):
    """{venue_unit_id: [{start, end, service_duration}]} — a unit's own hours for `day`.

    Same precedence as gen_availability_venue: one-off rows for the date win
    (a closed one-off means no hours); otherwise the weekly rows for the
    weekday, skipping any row pinned to this date.
    """
    if not unit_ids:
        return {}
    cur.execute(
        """
        SELECT va.venue_unit_id, va.type, va.is_closed, va.start_time, va.end_time, va.service_duration
          FROM venue_availability va
         WHERE va.tenant_id = %s AND va.location_id = %s AND va.is_active = TRUE
           AND va.venue_unit_id = ANY(%s)
           AND ((va.type = 'one_time' AND va.specific_date = %s)
             OR (va.type = 'recurring' AND va.day_of_week = %s
                 AND (va.specific_date IS NULL OR va.specific_date <> %s)))
         ORDER BY va.venue_unit_id, va.start_time
        """,
        (tenant_id, location_id, list(unit_ids), day, db_day, day),
    )
    one_time, recurring = {}, {}
    for uid, kind, is_closed, start, end, duration in cur.fetchall():
        bucket = one_time if kind == "one_time" else recurring
        bucket.setdefault(uid, []).append((bool(is_closed), str(start), str(end), duration))
    hours = {}
    for uid in unit_ids:
        if uid in one_time:
            rows = one_time[uid]
            hours[uid] = [] if any(c for c, *_ in rows) else [
                {"start": s, "end": e, "service_duration": str(d)} for _, s, e, d in rows
            ]
        else:
            hours[uid] = [{"start": s, "end": e, "service_duration": str(d)} for _, s, e, d in recurring.get(uid, [])]
    return hours


def _secs(t):
    h, m, *rest = (int(x) for x in t.split(":"))
    return h * 3600 + m * 60 + (rest[0] if rest else 0)


def _hms(secs):
    return str(timedelta(seconds=secs)).rjust(8, "0")


def intersect_slots(slot_lists):
    """Free intervals common to every member. The first member's
    service_duration is kept for each overlap (members share the location's
    periods in practice)."""
    if not slot_lists or any(not s for s in slot_lists):
        return []
    acc = [(_secs(s["start"]), _secs(s["end"]), s.get("service_duration")) for s in slot_lists[0]]
    for slots in slot_lists[1:]:
        nxt = []
        for a_start, a_end, dur in acc:
            for s in slots:
                lo, hi = max(a_start, _secs(s["start"])), min(a_end, _secs(s["end"]))
                if lo < hi:
                    nxt.append((lo, hi, dur))
        acc = nxt
        if not acc:
            return []
    acc.sort()
    return [{"start": _hms(a), "end": _hms(b), "service_duration": d} for a, b, d in acc]


def combination_entries(combos, free_by_unit, flex_bounds=None, slot_interval_minutes=None, periods=None):
    """Cache entries for one date. `free_by_unit` is each member's free slots
    after bookings. Combinations with no common free time are still listed
    (empty slots), like a fully booked table."""
    if not combos:
        return []
    venue_like = {}
    for c in combos:
        slots = intersect_slots([free_by_unit.get(uid, []) for uid in c["member_ids"]])
        entry = {
            "id": c["id"],
            "kind": "combination",
            "name": c["name"],
            "min_capacity": c["min_capacity"],
            "capacity": c["max_capacity"],
            "member_ids": c["member_ids"],
            "booking_rank": c["booking_rank"],
            "slots": slots,
        }
        if flex_bounds is not None:
            entry["is_flexible"] = True
            entry["duration_bounds"] = flex_bounds
        venue_like[c["id"]] = entry
    if slot_interval_minutes:
        # S7.1: the location's clock grid, same as single tables
        annotate_bookable_starts(venue_like, slot_interval_minutes, align_to_clock=True, periods=periods)
    else:
        annotate_bookable_starts(venue_like)
    return list(venue_like.values())


def combinations_for_day(cur, tenant_id, location_id, day, db_day, combos, bookings, flex_bounds=None, slot_interval_minutes=None, periods=None):
    """Everything for one date: member hours, minus bookings, intersected per combination."""
    if not combos:
        return []
    unit_ids = sorted({uid for c in combos for uid in c["member_ids"]})
    hours = member_hours(cur, tenant_id, location_id, day, db_day, unit_ids)
    free = reconstruct_venue_availability(bookings, {uid: {"slots": hours.get(uid, [])} for uid in unit_ids})
    return combination_entries(combos, {uid: v["slots"] for uid, v in free.items()}, flex_bounds, slot_interval_minutes, periods)
