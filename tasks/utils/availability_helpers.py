# tasks/utils/availability_helpers.py

from datetime import datetime, timedelta
import copy


DEFAULT_SLOT_INTERVAL_MINUTES = 15


# ─── Occupancy: which bookings hold a table / staff member, when ─────────────
#
# Restaurant booking maturity S4: the ONE place availability generation learns
# what is booked. Later sections change only this — S6 (a booking holding
# several tables), S7 (turnover buffer; seated parties past their end time
# keep the table) — instead of patching each generator.
#
# Occupying statuses mirror the DB EXCLUDE constraints (no_double_booking_*)
# and speako-web's OCCUPYING_BOOKING_STATUSES.
OCCUPYING_STATUSES_SQL = "('confirmed', 'pending_guarantee')"

_OCCUPANCY_COLUMNS = ("venue_unit_id", "staff_id")


def fetch_occupancy(cur, tenant_id, location_id, day, resource_column):
    """Bookings that occupy a resource on `day` ("YYYY-MM-DD", location wall
    clock), as the dicts reconstruct_*_availability consume:
    {resource_column, customer_id, start_time, end_time} with wall-clock
    "YYYY-MM-DD HH:MM:SS" strings."""
    if resource_column not in _OCCUPANCY_COLUMNS:
        raise ValueError(f"unsupported occupancy column: {resource_column}")
    cur.execute(f"""
                    SELECT {resource_column}, customer_id, start_time, end_time
                    FROM bookings
                    WHERE tenant_id = %s AND location_id = %s
                    AND start_time >= %s AND start_time < %s::date + INTERVAL '1 day'
                    AND status IN {OCCUPYING_STATUSES_SQL}
                """, (tenant_id, location_id, day, day))
    return [
        {
            resource_column: r[0],
            "customer_id": r[1],
            "start_time": r[2].strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": r[3].strftime("%Y-%m-%d %H:%M:%S"),
        }
        for r in cur.fetchall()
    ]


# ─── S7: turnover, needs-clearing, overrunning seated parties ──────────────
#
# Owner 2026-10-24 (restaurant booking maturity S7):
#  - turnover T (locations.table_turnover_*; 0 when off): a live booking holds
#    its tables over [start - T, end + T);
#  - a finished booking (status completed) holds them for [end, end + T)
#    ("Needs clearing"; "Cleared" moves end back so the tail ends at once);
#  - a seated, not finished party past its end keeps them until now + 15 min
#    (+ T) — independent of turnover (it's what stops the AI giving away a
#    table that's still in use). Nothing is written to the booking.
# With T = 0 and no overrunning party, the output equals the S6a occupancy.
OVERRUN_HOLD_MINUTES = 15
# A party nobody finished stops holding the table this long after its booked
# end (forgotten Finish), so it can't block the AI for the rest of the night.
OVERRUN_CAP_MINUTES = 180


def apply_table_hold_rules(rows, day, turnover_minutes=0, now_wall=None):
    """rows: dicts {venue_unit_id, start_time, end_time ("YYYY-MM-DD HH:MM:SS"),
    status, seated}. Returns occupancy dicts {venue_unit_id, customer_id,
    start_time, end_time}, clipped to `day` (reconstruct_venue_availability
    reads the date from the bookings)."""
    fmt = "%Y-%m-%d %H:%M:%S"
    t = timedelta(minutes=int(turnover_minutes or 0))
    day_start = datetime.strptime(f"{day} 00:00:00", fmt)
    day_end = datetime.strptime(f"{day} 23:59:59", fmt)
    now = datetime.strptime(now_wall, fmt) if now_wall else None
    out = []
    for r in rows:
        start = datetime.strptime(r["start_time"], fmt)
        end = datetime.strptime(r["end_time"], fmt)
        if r["status"] == "completed":
            if not t:
                continue
            hold_start, hold_end = end, end + t  # needs clearing
        else:
            if r.get("seated") and now is not None and end <= now < end + timedelta(minutes=OVERRUN_CAP_MINUTES):
                end = now + timedelta(minutes=OVERRUN_HOLD_MINUTES)  # still there
            hold_start, hold_end = start - t, end + t
        hold_start, hold_end = max(hold_start, day_start), min(hold_end, day_end)
        if hold_end <= hold_start:
            continue
        out.append({
            "venue_unit_id": r["venue_unit_id"],
            "customer_id": None,
            "start_time": hold_start.strftime(fmt),
            "end_time": hold_end.strftime(fmt),
        })
    return out


def fetch_table_occupancy(cur, tenant_id, location_id, day, turnover_minutes=0, now_wall=None):
    """Restaurant tables (venue units) held on `day`.

    Reads booking_venue_units (S6a): every table a booking holds — its primary
    table AND a combination's other tables — with the booking's time and status
    kept in sync by trg_sync_booking_venue_units. S7 adds the turnover, the
    needs-clearing tail of finished bookings and overrunning seated parties
    (apply_table_hold_rules). Same row shape as fetch_occupancy, so
    reconstruct_venue_availability is unchanged.
    """
    statuses = "('confirmed', 'pending_guarantee', 'completed')" if turnover_minutes else OCCUPYING_STATUSES_SQL
    cur.execute(f"""
                    SELECT x.venue_unit_id, x.start_time, x.end_time, x.status,
                           COALESCE(b.service_status_tag_ids && ARRAY(
                               SELECT lt.tag_id::int FROM location_tag lt
                                WHERE lt.tenant_id = x.tenant_id AND lt.category_id = 6 AND lt.slug = 'seated'), false) AS seated
                    FROM booking_venue_units x
                    JOIN bookings b ON b.tenant_id = x.tenant_id AND b.booking_id = x.booking_id
                    WHERE x.tenant_id = %s AND x.location_id = %s
                    AND x.start_time >= %s AND x.start_time < %s::date + INTERVAL '1 day'
                    AND x.status IN {statuses}
                """, (tenant_id, location_id, day, day))
    rows = [
        {
            "venue_unit_id": r[0],
            "start_time": r[1].strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": r[2].strftime("%Y-%m-%d %H:%M:%S"),
            "status": r[3],
            "seated": bool(r[4]),
        }
        for r in cur.fetchall()
    ]
    return apply_table_hold_rules(rows, day, turnover_minutes, now_wall)


def fetch_staff_occupancy(cur, tenant_id, location_id, day):
    """Staff members booked on `day` (service businesses)."""
    return fetch_occupancy(cur, tenant_id, location_id, day, "staff_id")


def annotate_bookable_starts(venue_dict, slot_interval_minutes=DEFAULT_SLOT_INTERVAL_MINUTES):
    """Add bookable_starts (list of "HH:MM") to each slot in a venue_dict.

    Fixed venues (the default): a start is bookable when
    start + service_duration <= slot_end, walked at slot_interval_minutes ticks.
    Empty list means the slot exists as a leftover gap but cannot fit a booking.

    Flexible venues (Phase 2 — venue marked ``is_flexible`` with
    ``duration_bounds``): the booking length is customer-chosen, so a start is
    bookable when at least the MINIMUM duration fits
    (start + min_duration <= slot_end). The slot additionally carries the bounds
    (``flexible``/``min_duration``/``max_duration``/``increment``) so consumers
    can validate a chosen ``(start, duration)`` at request time; start ticks stay
    on the same slot_interval grid as fixed bookings. The
    ``no_double_booking_venue_unit`` EXCLUDE constraint remains the final
    backstop.

    Additive: fixed venues produce the exact same output as before — the
    flexible branch only runs for venues that opted in.

    Mutates and returns venue_dict; caller already owns a fresh copy from
    reconstruct_venue_availability so in-place mutation is safe.
    """
    interval = timedelta(minutes=int(slot_interval_minutes or DEFAULT_SLOT_INTERVAL_MINUTES))
    for venue in venue_dict.values():
        bounds = venue.get('duration_bounds') if venue.get('is_flexible') else None
        for slot in venue.get('slots', []):
            slot_start = datetime.strptime(slot['start'], "%H:%M:%S")
            slot_end = datetime.strptime(slot['end'], "%H:%M:%S")
            if bounds:
                # Flexible: fit the minimum; expose bounds for request-time validation.
                min_d = int(bounds['min'])
                latest = slot_end - timedelta(minutes=min_d)
                starts = []
                if min_d > 0 and latest >= slot_start:
                    t = slot_start
                    while t <= latest:
                        starts.append(t.strftime("%H:%M"))
                        t += interval
                slot['flexible'] = True
                slot['min_duration'] = min_d
                slot['max_duration'] = int(bounds['max'])
                slot['increment'] = int(bounds['increment'])
                slot['bookable_starts'] = starts
            else:
                # Fixed: unchanged behavior.
                duration_raw = slot.get('service_duration')
                duration = int(duration_raw) if duration_raw is not None else 0
                latest = slot_end - timedelta(minutes=duration)
                starts = []
                if duration > 0 and latest >= slot_start:
                    t = slot_start
                    while t <= latest:
                        starts.append(t.strftime("%H:%M"))
                        t += interval
                slot['bookable_starts'] = starts
    return venue_dict


def _to_seconds(time_str):
    """Convert 'HH:MM' or 'HH:MM:SS' to seconds-since-midnight."""
    parts = time_str.split(':')
    h = int(parts[0])
    m = int(parts[1]) if len(parts) > 1 else 0
    s = int(parts[2]) if len(parts) > 2 else 0
    return h * 3600 + m * 60 + s


def _from_seconds(secs):
    h = secs // 3600
    m = (secs % 3600) // 60
    s = secs % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def intersect_slots_with_open_hours(staff_dict, open_hours):
    """Clamp each staff member's slots to the union of the location's open_hours.

    `open_hours` is the list emitted in the cache: [{'start': 'HH:MM', 'end': 'HH:MM'}, ...].
    Any slot piece that falls outside open hours is dropped. A staff whose
    slots are entirely outside open hours is removed from the dict so the
    cache never advertises bookable windows the location can't honour.

    Mutates and returns a deep-copied dict to match the convention of
    reconstruct_staff_availability.
    """
    if not open_hours:
        # Location closed for the day — emit no staff slots regardless of overrides
        return {}

    open_intervals = sorted(
        ((_to_seconds(oh['start']), _to_seconds(oh['end'])) for oh in open_hours),
        key=lambda x: x[0],
    )

    updated = copy.deepcopy(staff_dict)
    for sid in list(updated.keys()):
        clamped = []
        for slot in updated[sid].get('slots', []):
            slot_s = _to_seconds(slot['start'])
            slot_e = _to_seconds(slot['end'])
            for oh_s, oh_e in open_intervals:
                start = max(slot_s, oh_s)
                end = min(slot_e, oh_e)
                if start < end:
                    clamped.append({
                        'start': _from_seconds(start),
                        'end': _from_seconds(end),
                    })
        if not clamped:
            del updated[sid]
        else:
            updated[sid]['slots'] = clamped
    return updated

def parse_time(dt_str):
    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")

def parse_slot_time(time_str, date):
    return datetime.strptime(f"{date} {time_str}", "%Y-%m-%d %H:%M:%S")

def subtract_booking_from_slot(slot, booking):
    new_slots = []
    if booking['start_time'] > slot['start'] and booking['end_time'] < slot['end']:
        new_slots.append({'start': slot['start'], 'end': booking['start_time']})
        new_slots.append({'start': booking['end_time'], 'end': slot['end']})
    elif booking['start_time'] <= slot['start'] < booking['end_time'] < slot['end']:
        new_slots.append({'start': booking['end_time'], 'end': slot['end']})
    elif slot['start'] < booking['start_time'] < slot['end'] <= booking['end_time']:
        new_slots.append({'start': slot['start'], 'end': booking['start_time']})
    elif booking['start_time'] <= slot['start'] and booking['end_time'] >= slot['end']:
        pass
    else:
        new_slots.append(slot)
    return new_slots

def reconstruct_staff_availability(bookings, staff_dict):
    updated_staff_dict = copy.deepcopy(staff_dict)
    bookings_by_staff = {}
    for b in bookings:
        bookings_by_staff.setdefault(b['staff_id'], []).append({
            'start_time': parse_time(b['start_time']),
            'end_time': parse_time(b['end_time'])
        })

    for sid, staff in updated_staff_dict.items():
        if sid not in bookings_by_staff:
            continue
        staff_bookings = sorted(bookings_by_staff[sid], key=lambda b: b['start_time'])
        if not staff_bookings:
            continue
        date = staff_bookings[0]['start_time'].strftime('%Y-%m-%d')
        updated_slots = []
        for slot in staff['slots']:
            slot_start = parse_slot_time(slot['start'], date)
            slot_end = parse_slot_time(slot['end'], date)
            current_slots = [{'start': slot_start, 'end': slot_end}]
            for booking in staff_bookings:
                temp = []
                for s in current_slots:
                    temp.extend(subtract_booking_from_slot(s, booking))
                current_slots = temp
            updated_slots.extend([{
                'start': s['start'].strftime('%H:%M:%S'),
                'end': s['end'].strftime('%H:%M:%S')
            } for s in current_slots])
        staff['slots'] = updated_slots
    return updated_staff_dict


def reconstruct_venue_availability(bookings, venue_dict):
    updated_venue_dict = copy.deepcopy(venue_dict)
    bookings_by_venue = {}
    for b in bookings:
        bookings_by_venue.setdefault(b['venue_unit_id'], []).append({
            'start_time': parse_time(b['start_time']),
            'end_time': parse_time(b['end_time'])
        })

    for sid, venue in updated_venue_dict.items():
        if sid not in bookings_by_venue:
            continue
        venue_bookings = sorted(bookings_by_venue[sid], key=lambda b: b['start_time'])
        if not venue_bookings:
            continue
        date = venue_bookings[0]['start_time'].strftime('%Y-%m-%d')
        updated_slots = []
        for slot in venue['slots']:
            slot_start = parse_slot_time(slot['start'], date)
            slot_end = parse_slot_time(slot['end'], date)
            current_slots = [{'start': slot_start, 'end': slot_end}]
            for booking in venue_bookings:
                temp = []
                for s in current_slots:
                    temp.extend(subtract_booking_from_slot(s, booking))
                current_slots = temp
            for s in current_slots:
                updated_slots.append({
                    'start': s['start'].strftime('%H:%M:%S'),
                    'end': s['end'].strftime('%H:%M:%S'),
                    'service_duration': slot.get('service_duration')  # this still corresponds to this slot
                })
        venue['slots'] = updated_slots
    return updated_venue_dict
