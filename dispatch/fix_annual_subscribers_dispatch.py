"""
ONE-TIME FIX: Retroactively fix annual subscribers' billing periods.

For each annual subscriber:
1. Fix tenant_plans.current_period_end to be +1 year from start
2. Fix existing billing_usage_periods to monthly sub-periods
3. Create missing monthly periods and grants for elapsed months
4. Ensure exactly one period has is_current_period = true (the current month)

Usage:
    PYTHONPATH=. python dispatch/fix_annual_subscribers_dispatch.py

    # Dry-run mode (no changes, just diagnosis):
    PYTHONPATH=. python dispatch/fix_annual_subscribers_dispatch.py --dry-run

IMPORTANT: Run this once after deploying the annual minute refresh feature,
then archive or delete this script.
"""

import sys
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DRY_RUN = "--dry-run" in sys.argv


def get_db_connection():
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        raise ValueError("DATABASE_URL environment variable not set")
    return psycopg2.connect(db_url)


def fix_annual_subscribers():
    conn = get_db_connection()

    print("=" * 80)
    print("[FIX] Annual Subscriber Billing Period Retroactive Fix")
    print(f"[FIX] Mode: {'DRY-RUN (no changes)' if DRY_RUN else 'LIVE'}")
    print(f"[FIX] Started at: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 80)

    try:
        with conn.cursor() as cur:
            # Find all annual subscribers with their current billing state
            cur.execute("""
                SELECT
                    tp.tenant_id,
                    tp.plan_key,
                    tp.voice_minutes_included,
                    tp.current_period_start,
                    tp.current_period_end,
                    tp.stripe_subscription_id,
                    tp.stripe_customer_id,
                    tp.subscription_status,
                    t.name AS company_name,
                    bup.period_id AS current_period_id,
                    bup.period_start_date,
                    bup.period_end_date,
                    bup.billing_type
                FROM tenant_plans tp
                JOIN tenants t ON t.tenant_id = tp.tenant_id
                LEFT JOIN billing_usage_periods bup
                    ON bup.tenant_id = tp.tenant_id
                    AND bup.is_current_period = true
                    AND bup.billing_type = 'subscription'
                WHERE tp.billing_period = 'annual'
                    AND tp.active = true
                ORDER BY tp.tenant_id
            """)

            tenants = cur.fetchall()

        if not tenants:
            print("[INFO] No annual subscribers found. Nothing to fix.")
            return

        print(f"[INFO] Found {len(tenants)} annual subscriber(s)")

        fixed = 0
        skipped = 0
        errors = 0

        for tenant in tenants:
            (
                tenant_id, plan_key, voice_minutes,
                tp_start, tp_end,
                stripe_sub_id, stripe_cust_id, sub_status,
                company_name,
                current_period_id,
                period_start, period_end, billing_type,
            ) = tenant

            print(f"\n{'=' * 60}")
            print(f"[Tenant {tenant_id}] {company_name}")
            print(f"  Plan: {plan_key}, {voice_minutes} min/month, status: {sub_status}")
            print(f"  tenant_plans period: {tp_start} -> {tp_end}")
            print(f"  current billing_usage_periods: {period_start} -> {period_end} (id={current_period_id})")

            if not tp_start:
                print("  [SKIP] No current_period_start in tenant_plans")
                skipped += 1
                continue

            if sub_status != 'active':
                print(f"  [SKIP] Subscription status is '{sub_status}', not active")
                skipped += 1
                continue

            # Compute correct annual boundaries
            annual_start = tp_start
            annual_end = annual_start + relativedelta(years=1)

            # Check if tenant_plans.current_period_end is already correct (annual)
            tp_end_delta = (tp_end - tp_start).days if tp_end else 0
            needs_tp_fix = tp_end_delta < 60  # Less than 60 days = wrong for annual

            print(f"  Correct annual cycle: {annual_start} -> {annual_end}")
            print(f"  tenant_plans period length: {tp_end_delta} days {'(NEEDS FIX)' if needs_tp_fix else '(OK)'}")

            now = datetime.now(timezone.utc).replace(tzinfo=None)

            # Determine how many months have elapsed since subscription start
            months_elapsed = 0
            check_date = annual_start
            while check_date + relativedelta(months=1) <= now and check_date < annual_end:
                months_elapsed += 1
                check_date = annual_start + relativedelta(months=months_elapsed)

            # The current month index (which monthly period should be active now)
            current_month_index = months_elapsed

            print(f"  Months elapsed: {months_elapsed}, current month index: {current_month_index}")

            if DRY_RUN:
                print("  [DRY-RUN] Would fix this tenant (skipping actual changes)")
                fixed += 1
                continue

            # Begin transaction for this tenant
            try:
                with conn.cursor() as cur:
                    # Step 1: Fix tenant_plans.current_period_end if wrong
                    if needs_tp_fix:
                        cur.execute("""
                            UPDATE tenant_plans
                            SET current_period_end = %s, updated_at = NOW()
                            WHERE tenant_id = %s AND active = true
                        """, (annual_end, tenant_id))
                        print(f"  [FIX] Updated tenant_plans.current_period_end = {annual_end}")

                    # Step 2: Mark all existing subscription periods as not current
                    cur.execute("""
                        UPDATE billing_usage_periods
                        SET is_current_period = false, updated_at = NOW()
                        WHERE tenant_id = %s AND is_current_period = true
                    """, (tenant_id,))

                    # Step 3: Fix the existing wrong period to be month 0
                    if current_period_id and period_start:
                        month0_start = annual_start
                        month0_end = annual_start + relativedelta(months=1)

                        cur.execute("""
                            UPDATE billing_usage_periods
                            SET period_start_date = %s,
                                period_end_date = %s,
                                is_current_period = false,
                                updated_at = NOW()
                            WHERE period_id = %s
                        """, (month0_start, month0_end, current_period_id))
                        print(f"  [FIX] Corrected period {current_period_id}: {month0_start} -> {month0_end}")

                    # Step 4: Create missing monthly periods for months 1 through current
                    # Month 0 was already handled above (the original webhook-created period)
                    for m in range(1, current_month_index + 1):
                        m_start = annual_start + relativedelta(months=m)
                        m_end = annual_start + relativedelta(months=m + 1)

                        # Cap at annual end
                        if m_end > annual_end:
                            m_end = annual_end
                        if m_start >= annual_end:
                            break

                        # Check if period already exists
                        cur.execute("""
                            SELECT period_id FROM billing_usage_periods
                            WHERE tenant_id = %s
                              AND billing_type = 'subscription'
                              AND DATE(period_start_date) = DATE(%s)
                            LIMIT 1
                        """, (tenant_id, m_start))

                        existing = cur.fetchone()
                        if existing:
                            print(f"  [SKIP] Period for month {m} already exists (id={existing[0]})")
                            continue

                        is_current = (m == current_month_index)

                        cur.execute("""
                            INSERT INTO billing_usage_periods (
                                tenant_id, plan_key, billing_type,
                                period_start_date, period_end_date,
                                voice_minutes_included, voice_minutes_used,
                                overage_minutes, is_current_period,
                                stripe_subscription_id, stripe_customer_id,
                                created_at, updated_at
                            ) VALUES (
                                %s, %s, 'subscription', %s, %s,
                                %s, 0, 0, %s, %s, %s,
                                NOW(), NOW()
                            )
                            RETURNING period_id
                        """, (
                            tenant_id, plan_key, m_start, m_end,
                            voice_minutes, is_current,
                            stripe_sub_id, stripe_cust_id,
                        ))

                        new_pid = cur.fetchone()[0]
                        print(
                            f"  [CREATE] Month {m}: {m_start} -> {m_end} "
                            f"(period_id={new_pid}, current={is_current})"
                        )

                        # Grant minutes for this month
                        cur.execute("""
                            SELECT 1 FROM billing_minute_ledger
                            WHERE tenant_id = %s
                              AND source = 'plan_grant'
                              AND usage_bucket = 'plan'
                              AND created_at >= %s
                              AND created_at < %s
                            LIMIT 1
                        """, (tenant_id, m_start, m_end))

                        if cur.fetchone():
                            print(f"  [SKIP] Grant for month {m} already exists")
                        else:
                            seconds = voice_minutes * 60
                            cur.execute("""
                                INSERT INTO billing_minute_ledger (
                                    tenant_id, source, usage_bucket,
                                    seconds_delta, created_at
                                ) VALUES (%s, 'plan_grant', 'plan', %s, %s)
                            """, (tenant_id, seconds, m_start))
                            print(f"  [GRANT] Month {m}: {voice_minutes} min ({seconds}s)")

                    # Step 5: Ensure the current month has is_current_period = true
                    current_m_start = annual_start + relativedelta(months=current_month_index)
                    cur.execute("""
                        UPDATE billing_usage_periods
                        SET is_current_period = true, updated_at = NOW()
                        WHERE tenant_id = %s
                          AND billing_type = 'subscription'
                          AND DATE(period_start_date) = DATE(%s)
                    """, (tenant_id, current_m_start))

                    conn.commit()
                    print(f"  [DONE] Tenant {tenant_id} fixed successfully")
                    fixed += 1

            except Exception as e:
                conn.rollback()
                print(f"  [ERROR] Tenant {tenant_id}: {e}")
                errors += 1

        print(f"\n{'=' * 80}")
        print("[FIX] Summary:")
        print(f"  Fixed: {fixed}")
        print(f"  Skipped: {skipped}")
        print(f"  Errors: {errors}")
        print(f"[FIX] Completed at: {datetime.now(timezone.utc).isoformat()}")
        print("=" * 80)

    finally:
        conn.close()


if __name__ == "__main__":
    fix_annual_subscribers()
