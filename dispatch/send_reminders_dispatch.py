"""
Customer reminder dispatcher (Phase 4 / WP-C).

Runs every 5 minutes (Render cron). For each location with customer reminders enabled
and one or more configured offsets, finds confirmed, still-upcoming bookings whose
send-time (booking start − offset) has just become due — evaluated in the booking's
OWN time_zone so DST is correct — and whose offset has not yet been recorded in
booking_notification, then enqueues one `tasks.sms.send_reminder` task per
(booking, offset).

This script only finds CANDIDATES; exactly-once delivery is guaranteed by the atomic
claim inside `send_reminder` (booking_notification.reminders[offset]). Modeled on
dispatch/cancel_unpaid_guarantees_dispatch.py.

Notes on the due-window guards:
- `start_time` is stored as LOCAL wall-clock, so `start_time AT TIME ZONE time_zone`
  yields the true UTC instant (DST-correct).
- `created_at` is stored UTC-naive, so it is cast `AT TIME ZONE 'UTC'`.
- We only fire a reminder whose scheduled time is AFTER the booking was created (a
  booking made after its reminder time never gets a retroactive reminder) and no more
  than 1 day late (bounds catch-up after downtime; normal ops fire within 5 min).

Usage: PYTHONPATH=. python dispatch/send_reminders_dispatch.py
"""

import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

from tasks.celery_app import app  # noqa: E402  (import after load_dotenv so broker env is set)


DUE_REMINDERS_SQL = """
SELECT b.booking_id, off.offset_minutes
  FROM bookings b
  JOIN locations l
    ON l.tenant_id = b.tenant_id AND l.location_id = b.location_id
  CROSS JOIN LATERAL unnest(l.reminder_offsets_minutes) AS off(offset_minutes)
  LEFT JOIN booking_notification bn
    ON bn.tenant_id = b.tenant_id AND bn.booking_id = b.booking_id
 WHERE l.customer_reminders_enabled = true
   AND b.status = 'confirmed'
   AND (b.start_time AT TIME ZONE b.time_zone) > now()                                            -- still upcoming
   AND (b.start_time AT TIME ZONE b.time_zone) - make_interval(mins => off.offset_minutes) <= now()          -- due
   AND (b.start_time AT TIME ZONE b.time_zone) - make_interval(mins => off.offset_minutes) > now() - interval '1 day'  -- bound catch-up
   AND (b.start_time AT TIME ZONE b.time_zone) - make_interval(mins => off.offset_minutes) >= (b.created_at AT TIME ZONE 'UTC')  -- scheduled after booking made
   AND NOT COALESCE(bn.reminders ? off.offset_minutes::text, false)                               -- not already recorded
 ORDER BY b.booking_id
 LIMIT 500
"""


def get_db_connection():
    """psycopg2 connection to DATABASE_URL (= dev by convention; prod in the prod env)."""
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(db_url)


def dispatch_reminders():
    conn = None
    cur = None
    enqueued = 0
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(DUE_REMINDERS_SQL)
        rows = cur.fetchall()
        print(f"[REMINDER-DISPATCH] {len(rows)} due (booking, offset) pair(s)")
        for booking_id, offset_minutes in rows:
            try:
                app.send_task("tasks.sms.send_reminder", args=[int(booking_id), int(offset_minutes)])
                enqueued += 1
                print(f"[REMINDER-DISPATCH] enqueued booking {booking_id} offset {offset_minutes}m")
            except Exception as e:
                # One bad enqueue must not abort the batch; the next run retries it.
                print(f"[REMINDER-DISPATCH] enqueue failed booking {booking_id} offset {offset_minutes}m: {e}")
        print(f"[REMINDER-DISPATCH] done — {enqueued}/{len(rows)} enqueued")
    except Exception as e:
        print(f"[REMINDER-DISPATCH] error: {e}")
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    dispatch_reminders()
