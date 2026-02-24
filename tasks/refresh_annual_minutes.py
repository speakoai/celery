"""
Celery task to refresh monthly minute quota for annual subscribers.

Creates a new billing_usage_periods row and grants plan_grant minutes
for the next month. Handles edge cases:
- Catches up if multiple months were missed (processes one month per invocation;
  the daily dispatch will catch up over subsequent runs)
- Stops at annual_period_end (year boundary) so the webhook handles year 2
- Idempotent: checks for existing period/grant before creating
"""

from dotenv import load_dotenv
load_dotenv()

from tasks.celery_app import app
from celery.utils.log import get_task_logger
import os
import psycopg2
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

logger = get_task_logger(__name__)


def get_db_connection():
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        raise ValueError("DATABASE_URL environment variable not set")
    return psycopg2.connect(db_url)


@app.task(
    name='tasks.refresh_annual_minutes.refresh_annual_minutes_for_tenant',
    bind=True,
    max_retries=3,
    default_retry_delay=60,
)
def refresh_annual_minutes_for_tenant(self, tenant_id):
    """
    Create the next monthly billing period and grant plan minutes
    for an annual subscriber.

    Logic:
    1. Read current billing_usage_periods (is_current_period=true)
    2. Read tenant_plans for plan details and annual boundary
    3. Compute next month period (current period_end -> +1 month)
    4. Verify next month doesn't exceed annual_period_end
    5. In a transaction:
       a. Mark current period as not current
       b. Insert new period
       c. Insert plan_grant ledger entry (created_at = new period start)
    6. Idempotency: if period already exists, skip
    """
    logger.info(f"[Tenant {tenant_id}] Starting annual minute refresh")

    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            # Step 1: Get current period and plan info
            cur.execute("""
                SELECT
                    tp.plan_key,
                    tp.voice_minutes_included,
                    tp.billing_period,
                    tp.subscription_status,
                    tp.stripe_subscription_id,
                    tp.stripe_customer_id,
                    tp.current_period_end AS annual_period_end,
                    bup.period_id,
                    bup.period_start_date,
                    bup.period_end_date
                FROM tenant_plans tp
                JOIN billing_usage_periods bup
                    ON bup.tenant_id = tp.tenant_id
                    AND bup.is_current_period = true
                    AND bup.billing_type = 'subscription'
                WHERE tp.tenant_id = %s
                    AND tp.billing_period = 'annual'
                    AND tp.active = true
                ORDER BY bup.period_end_date DESC
                LIMIT 1
            """, (tenant_id,))

            row = cur.fetchone()

            if not row:
                logger.warning(
                    f"[Tenant {tenant_id}] No active annual plan or current "
                    f"subscription period found, skipping"
                )
                return {"status": "skipped", "reason": "no_active_annual_plan"}

            (
                plan_key, voice_minutes, billing_period, sub_status,
                stripe_sub_id, stripe_cust_id, annual_period_end,
                current_period_id, current_period_start, current_period_end,
            ) = row

            if sub_status != 'active':
                logger.info(
                    f"[Tenant {tenant_id}] Subscription status is "
                    f"'{sub_status}', skipping refresh"
                )
                return {"status": "skipped", "reason": f"sub_status_{sub_status}"}

            # Step 2: Check if period has actually expired
            now = datetime.now(timezone.utc)
            period_end_aware = current_period_end.replace(tzinfo=timezone.utc) \
                if current_period_end.tzinfo is None else current_period_end

            if period_end_aware > now:
                logger.info(
                    f"[Tenant {tenant_id}] Current period hasn't ended yet "
                    f"(ends {current_period_end}), skipping"
                )
                return {"status": "skipped", "reason": "period_not_expired"}

            # Step 3: Compute next month boundaries
            new_period_start = current_period_end
            new_period_end = new_period_start + relativedelta(months=1)

            # Step 4: Verify we haven't exceeded the annual boundary
            if annual_period_end:
                annual_end_aware = annual_period_end.replace(tzinfo=timezone.utc) \
                    if annual_period_end.tzinfo is None else annual_period_end

                if new_period_start.replace(tzinfo=timezone.utc) \
                        if new_period_start.tzinfo is None else new_period_start \
                        >= annual_end_aware:
                    logger.info(
                        f"[Tenant {tenant_id}] Next period start "
                        f"({new_period_start}) >= annual boundary "
                        f"({annual_period_end}), skipping - "
                        f"webhook will handle year 2 renewal"
                    )
                    return {"status": "skipped", "reason": "annual_boundary_reached"}

                # Cap period_end at annual boundary if it would exceed
                new_period_end_aware = new_period_end.replace(tzinfo=timezone.utc) \
                    if new_period_end.tzinfo is None else new_period_end
                if new_period_end_aware > annual_end_aware:
                    new_period_end = annual_period_end
                    logger.info(
                        f"[Tenant {tenant_id}] Capped period_end at annual "
                        f"boundary: {new_period_end}"
                    )

            # Step 5: Idempotency check - does this period already exist?
            cur.execute("""
                SELECT period_id FROM billing_usage_periods
                WHERE tenant_id = %s
                  AND billing_type = 'subscription'
                  AND DATE(period_start_date) = DATE(%s)
                LIMIT 1
            """, (tenant_id, new_period_start))

            existing = cur.fetchone()
            if existing:
                logger.info(
                    f"[Tenant {tenant_id}] Period already exists for "
                    f"{new_period_start} (period_id={existing[0]}), "
                    f"skipping (idempotent)"
                )
                return {"status": "skipped", "reason": "period_exists"}

            # Step 6: Transaction - create new period and grant minutes
            logger.info(
                f"[Tenant {tenant_id}] Creating new period: "
                f"{new_period_start} -> {new_period_end}, "
                f"granting {voice_minutes} minutes"
            )

            # Mark previous period as not current
            cur.execute("""
                UPDATE billing_usage_periods
                SET is_current_period = false, updated_at = NOW()
                WHERE tenant_id = %s AND is_current_period = true
            """, (tenant_id,))

            # Insert new period
            cur.execute("""
                INSERT INTO billing_usage_periods (
                    tenant_id, plan_key, billing_type,
                    period_start_date, period_end_date,
                    voice_minutes_included, voice_minutes_used, overage_minutes,
                    is_current_period, stripe_subscription_id, stripe_customer_id,
                    created_at, updated_at
                ) VALUES (
                    %s, %s, 'subscription', %s, %s,
                    %s, 0, 0, true, %s, %s,
                    NOW(), NOW()
                )
                RETURNING period_id
            """, (
                tenant_id, plan_key,
                new_period_start, new_period_end,
                voice_minutes, stripe_sub_id, stripe_cust_id,
            ))

            new_period_id = cur.fetchone()[0]

            # Check for existing grant in this period window (belt and suspenders)
            cur.execute("""
                SELECT 1 FROM billing_minute_ledger
                WHERE tenant_id = %s
                  AND source = 'plan_grant'
                  AND usage_bucket = 'plan'
                  AND created_at >= %s
                  AND created_at < %s
                LIMIT 1
            """, (tenant_id, new_period_start, new_period_end))

            if cur.fetchone():
                logger.warning(
                    f"[Tenant {tenant_id}] plan_grant already exists in new "
                    f"period window, skipping grant"
                )
            else:
                # Grant minutes (created_at = period start so it falls in window)
                seconds_delta = voice_minutes * 60
                cur.execute("""
                    INSERT INTO billing_minute_ledger (
                        tenant_id, source, usage_bucket,
                        seconds_delta, created_at
                    ) VALUES (%s, 'plan_grant', 'plan', %s, %s)
                """, (tenant_id, seconds_delta, new_period_start))

                logger.info(
                    f"[Tenant {tenant_id}] Granted {voice_minutes} min "
                    f"({seconds_delta}s) for period {new_period_id}"
                )

            conn.commit()
            logger.info(
                f"[Tenant {tenant_id}] Successfully created period "
                f"{new_period_id}: {new_period_start} -> {new_period_end}"
            )

            return {
                "status": "refreshed",
                "tenant_id": tenant_id,
                "period_id": new_period_id,
                "period_start": str(new_period_start),
                "period_end": str(new_period_end),
                "minutes_granted": voice_minutes,
            }

    except Exception as e:
        conn.rollback()
        logger.error(f"[Tenant {tenant_id}] Error during refresh: {e}")
        raise self.retry(exc=e)
    finally:
        try:
            conn.close()
        except Exception:
            pass
