# tasks/utils/availability_helpers.py

from datetime import datetime
import copy

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
            updated_slots.extend([{
                'start': s['start'].strftime('%H:%M:%S'),
                'end': s['end'].strftime('%H:%M:%S')
            } for s in current_slots])
        venue['slots'] = updated_slots
    return updated_venue_dict
