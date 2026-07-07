"""
Dispatch script: auto-cancel unpaid Booking Guarantee holds.

Runs every 5 minutes via Render.com cron. Cancels bookings that were created as
`pending_guarantee` but whose card was never secured within the tenant's
configured hold window.

Selection:
  - bookings.status = 'pending_guarantee'
  - bookings.guarantee_status = 'pending'
  - bookings.created_at older than the location's hold window, read per-booking
    from the booking_manager tool JSON
    (tenant_integration_params.value_json->'properties'->>'guarantee_hold_minutes',
    keyed by param_code booking_manager_<location_type>). Defaults to 30 minutes
    if the setting is missing.

For each match: status='cancelled', cancel_reason='unpaid_guarantee',
guarantee_status='released'.

IDEMPOTENT: the WHERE clause only matches still-pending holds, so once a booking
is cancelled it can never match again — the script is safe to run repeatedly and
most runs are no-ops.

NOTE: this script does NOT send a cancellation SMS (Phase 4 scope is the state
transition only). A "cancelled due to unpaid guarantee" SMS can be added later.

Usage:
    PYTHONPATH=. python dispatch/cancel_unpaid_guarantees_dispatch.py
"""

from datetime import datetime, timezone
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

from tasks.celery_app import app


CANCEL_UNPAID_GUARANTEES_SQL = """
    UPDATE bookings b
    SET status = 'cancelled',
        cancel_reason = 'unpaid_guarantee',
        guarantee_status = 'released',
        updated_at = NOW()
    WHERE b.status = 'pending_guarantee'
      AND b.guarantee_status = 'pending'
      AND b.created_at < NOW() - make_interval(mins => COALESCE(
            (SELECT NULLIF(tip.value_json->'properties'->>'guarantee_hold_minutes', '')::int
             FROM tenant_integration_params tip
             JOIN locations l
               ON l.tenant_id = b.tenant_id AND l.location_id = b.location_id
             WHERE tip.tenant_id = b.tenant_id
               AND tip.location_id = b.location_id
               AND tip.service = 'tool'
               AND tip.provider = 'speako'
               AND tip.param_code = 'booking_manager_' || l.location_type
             LIMIT 1),
            30))
    RETURNING b.booking_id, b.tenant_id, b.location_id, b.booking_ref;
"""


def get_db_connection():
    """Get PostgreSQL database connection (DATABASE_URL = dev by convention)."""
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL environment variable not set")
    return psycopg2.connect(db_url)


def cancel_unpaid_guarantees():
    print("=" * 80)
    print("[DISPATCH] Cancel unpaid Booking Guarantee holds")
    print(f"[DISPATCH] Started at: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 80)

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(CANCEL_UNPAID_GUARANTEES_SQL)
            rows = cur.fetchall()
        conn.commit()

        if not rows:
            print("[INFO] No unpaid guarantee holds to cancel")
            print("[DISPATCH] Done (no-op)")
            return

        print(f"[INFO] Cancelled {len(rows)} unpaid guarantee booking(s):")
        for booking_id, tenant_id, location_id, booking_ref in rows:
            print(
                f"  - booking_id={booking_id} ref={booking_ref} "
                f"tenant={tenant_id} location={location_id}"
            )
            # Notify the customer their booking was cancelled (unpaid guarantee).
            try:
                app.send_task("tasks.sms.send_sms_guarantee_cancelled", args=[booking_id])
            except Exception as e:
                print(f"    [WARN] Failed to enqueue cancellation SMS for {booking_id}: {e}")
    except Exception as e:
        if conn is not None:
            conn.rollback()
        print(f"[ERROR] Failed to cancel unpaid guarantees: {e}")
        raise
    finally:
        if conn is not None:
            conn.close()
        print(f"[DISPATCH] Completed at: {datetime.now(timezone.utc).isoformat()}")
        print("=" * 80)


if __name__ == "__main__":
    cancel_unpaid_guarantees()
