#!/usr/bin/env python3
"""
Re-analyse stored call-log artifacts with the CURRENT analyzer and refresh
`call_quality_findings`.

Why this exists: findings are inserted `ON CONFLICT DO NOTHING`, which is what
makes the live sweep idempotent and stops it resurrecting a finding the admin
dismissed. The cost is that a calibration change never reaches rows already
written. After Phase 6 halved the false-critical rate, every finding raised by
an older analyzer is stale — and some of them are the exact false positives the
calibration removed.

Run:
    python scripts/refresh_call_quality_findings.py --dev --dry-run
    python scripts/refresh_call_quality_findings.py --dev
    python scripts/refresh_call_quality_findings.py --prod        # requires care

⚠️ SAFETY — a human's triage decision is never destroyed.

Rows the admin has acted on (status != 'open') are LEFT ALONE entirely, even if
the current analyzer no longer produces them. Only `open` rows are refreshed:
deleted if the analyzer no longer raises them, re-inserted from the fresh
analysis otherwise. A finding that vanishes is reported by rule id so the change
is visible rather than silent.
"""

import argparse
import gzip
import json
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from dotenv import load_dotenv

from tasks.utils.call_quality_rules import ANALYZER_VERSION, analyze, index_catalogue
from tasks.utils.call_quality_store import insert_findings, load_catalogue

load_dotenv()

# R2 access is inlined rather than imported from tasks.utils.publish_r2: that
# module imports celery, which cannot load under Python 3.13 (the deployed pin
# is 3.11). A maintenance script has to run on whatever interpreter is to hand.


def _r2():
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("R2_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
    )


def download_call_log_from_r2(r2_key, use_dev):
    bucket = os.getenv("R2_BUCKET_NAME_DEV" if use_dev else "R2_BUCKET_NAME")
    if not bucket:
        raise RuntimeError("R2 bucket env var not set")
    return _r2().get_object(Bucket=bucket, Key=r2_key)["Body"].read()


def main():
    ap = argparse.ArgumentParser()
    env = ap.add_mutually_exclusive_group(required=True)
    env.add_argument("--dev", action="store_true")
    env.add_argument("--prod", action="store_true")
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would change; write nothing")
    ap.add_argument("--limit", type=int, default=0, help="cap conversations processed")
    args = ap.parse_args()

    is_dev = args.dev
    db_url = os.getenv("DATABASE_URL" if is_dev else "DATABASE_URL_PROD")
    if not db_url:
        sys.exit(f"{'DATABASE_URL' if is_dev else 'DATABASE_URL_PROD'} not set")

    catalogue = load_catalogue()
    print(f"target      : {'DEV' if is_dev else 'PRODUCTION'}")
    print(f"analyzer    : {ANALYZER_VERSION}")
    print(f"catalogue   : {catalogue['catalogue_version']}")
    print(f"mode        : {'DRY RUN (no writes)' if args.dry_run else 'WRITING'}\n")

    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT f.location_conversation_id, f.tenant_id, f.location_id,
               f.call_sid, c.raw_metadata->>'log_r2_key'
          FROM call_quality_findings f
          JOIN location_conversations c
            ON c.location_conversation_id = f.location_conversation_id
         WHERE f.analyzer_version <> %s
           AND c.raw_metadata ? 'log_r2_key'
         ORDER BY 1
        """,
        (ANALYZER_VERSION,),
    )
    targets = cur.fetchall()
    if args.limit:
        targets = targets[: args.limit]
    print(f"conversations carrying stale findings: {len(targets)}\n")

    removed = Counter()
    added = Counter()
    kept_human = 0
    unreadable = 0

    for conv_id, tenant_id, location_id, call_sid, r2_key in targets:
        try:
            text = gzip.decompress(
                download_call_log_from_r2(r2_key, use_dev=is_dev)
            ).decode()
        except Exception as exc:
            unreadable += 1
            print(f"  conv{conv_id}: artifact unreadable — left untouched ({type(exc).__name__})")
            continue

        fresh = analyze(text, catalogue)
        fresh_ids = {f["rule_id"] for f in fresh}

        cur.execute(
            """SELECT rule_id, status FROM call_quality_findings
                WHERE location_conversation_id = %s""",
            (conv_id,),
        )
        existing = cur.fetchall()
        stale_open = {r for r, s in existing if s == "open"}
        human = {r for r, s in existing if s != "open"}
        kept_human += len(human)

        gone = stale_open - fresh_ids
        new = fresh_ids - stale_open - human

        if gone or new:
            print(f"  conv{conv_id} (t{tenant_id}/l{location_id})")
            for r in sorted(gone):
                print(f"      - {r}")
                removed[r] += 1
            for r in sorted(new):
                print(f"      + {r}")
                added[r] += 1

        if args.dry_run:
            continue

        # Drop only untouched rows. Anything the admin acted on stays.
        cur.execute(
            """DELETE FROM call_quality_findings
                WHERE location_conversation_id = %s AND status = 'open'""",
            (conv_id,),
        )
        insert_findings(
            conn,
            tenant_id=tenant_id, location_id=location_id,
            conversation_id=conv_id, call_sid=call_sid,
            findings=[f for f in fresh if f["rule_id"] not in human],
        )
        # Keep the conversation-list badge consistent with the rows.
        cur.execute(
            """UPDATE location_conversations
                  SET raw_metadata = COALESCE(raw_metadata,'{}'::jsonb) || %s::jsonb
                WHERE location_conversation_id = %s""",
            (json.dumps(_summary(fresh)), conv_id),
        )

    if not args.dry_run:
        conn.commit()
    conn.close()

    print("\n" + ("would remove" if args.dry_run else "removed") + ":")
    for r, n in removed.most_common() or [("(nothing)", 0)]:
        print(f"   {r:<30} {n}")
    print(("would add" if args.dry_run else "added") + ":")
    for r, n in added.most_common() or [("(nothing)", 0)]:
        print(f"   {r:<30} {n}")
    print(f"\nleft alone because a human had triaged them: {kept_human}")
    if unreadable:
        print(f"artifacts unreadable (skipped): {unreadable}")


def _summary(findings):
    if not findings:
        return {"quality_severity": None, "quality_count": 0, "quality_rules": []}
    sev = ("critical" if any(f["severity"] == "critical" for f in findings)
           else "moderate" if any(f["severity"] == "moderate" for f in findings)
           else "info")
    return {"quality_severity": sev, "quality_count": len(findings),
            "quality_rules": sorted(f["rule_id"] for f in findings)}


if __name__ == "__main__":
    main()
