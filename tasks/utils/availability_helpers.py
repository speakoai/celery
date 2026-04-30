# tasks/utils/availability_helpers.py

from datetime import datetime
import copy


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
