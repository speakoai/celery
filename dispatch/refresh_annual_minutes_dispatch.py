"""
Dispatch script for refreshing monthly minute quotas for annual subscribers.

Runs daily at 00:15 UTC via Render.com cron. Queries all tenants with
billing_period='annual' and active subscriptions whose current
billing_usage_periods.period_end_date has passed. Dispatches a Celery
task per tenant to create the next monthly period and grant minutes.

Most daily runs will be no-ops (no tenants needing refresh). When a
tenant's monthly period expires, the next run picks it up within 24h.

Usage:
    PYTHONPATH=. python dispatch/refresh_annual_minutes_dispatch.py
"""

from datetime import datetime, timezone
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

from tasks.refresh_annual_minutes import refresh_annual_minutes_for_tenant


def get_db_connection():
    """Get PostgreSQL database connection."""
    try:
        db_url = os.environ.get('DATABASE_URL')
        if not db_url:
            raise ValueError("DATABASE_URL environment variable not set")
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        print(f"[ERROR] Failed to connect to database: {e}")
        raise


def fetch_annual_tenants_needing_refresh():
    """
    Find annual subscribers whose current billing period has ended.

    Criteria:
    - tenant_plans.billing_period = 'annual'
    - tenant_plans.active = true
    - tenant_plans.subscription_status = 'active'
    - billing_usage_periods.is_current_period = true
    - billing_usage_periods.billing_type = 'subscription'
    - billing_usage_periods.period_end_date <= NOW()

    Returns list of dicts with tenant info.
    """
    tenants = []

    try:
        conn = get_db_connection()

        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    tp.tenant_id,
                    tp.plan_key,
                    tp.voice_minutes_included,
                    bup.period_end_date,
                    t.name AS company_name
                FROM tenant_plans tp
                JOIN billing_usage_periods bup
                    ON bup.tenant_id = tp.tenant_id
                    AND bup.is_current_period = true
                    AND bup.billing_type = 'subscription'
                JOIN tenants t
                    ON t.tenant_id = tp.tenant_id
                WHERE tp.billing_period = 'annual'
                    AND tp.active = true
                    AND tp.subscription_status = 'active'
                    AND bup.period_end_date <= NOW()
                ORDER BY tp.tenant_id
            """)

            for row in cur.fetchall():
                tenants.append({
                    "tenant_id": row[0],
                    "plan_key": row[1],
                    "voice_minutes_included": row[2],
                    "period_end_date": row[3],
                    "company_name": row[4],
                })

        conn.close()

        return tenants

    except Exception as e:
        print(f"[ERROR] Failed to fetch annual tenants: {e}")
        return tenants


def dispatch_annual_refresh():
    """
    Main dispatch function.

    Queries annual tenants needing refresh and dispatches Celery tasks.
    """
    print("=" * 80)
    print("[DISPATCH] Annual Subscriber Monthly Minute Refresh")
    print(f"[DISPATCH] Started at: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 80)

    tenants = fetch_annual_tenants_needing_refresh()

    if not tenants:
        print("[INFO] No annual tenants need minute refresh")
        print("[DISPATCH] Done (no-op)")
        return

    print(f"[INFO] Found {len(tenants)} tenant(s) needing refresh:")

    for tenant in tenants:
        print(
            f"  - Tenant {tenant['tenant_id']}: "
            f"{tenant['company_name']} ({tenant['plan_key']}, "
            f"{tenant['voice_minutes_included']} min, "
            f"period ended {tenant['period_end_date']})"
        )

    print()
    print("[INFO] Dispatching Celery tasks...")
    print()

    dispatched = 0
    failed = 0

    for tenant in tenants:
        try:
            result = refresh_annual_minutes_for_tenant.delay(
                tenant['tenant_id']
            )
            print(
                f"[DISPATCHED] Tenant {tenant['tenant_id']} "
                f"({tenant['company_name']}): Task ID = {result.id}"
            )
            dispatched += 1
        except Exception as e:
            print(
                f"[ERROR] Failed to dispatch for tenant "
                f"{tenant['tenant_id']}: {e}"
            )
            failed += 1

    print()
    print("=" * 80)
    print("[DISPATCH] Summary:")
    print(f"  Total tenants: {len(tenants)}")
    print(f"  Tasks dispatched: {dispatched}")
    print(f"  Failed: {failed}")
    print(f"[DISPATCH] Completed at: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 80)


if __name__ == "__main__":
    dispatch_annual_refresh()
