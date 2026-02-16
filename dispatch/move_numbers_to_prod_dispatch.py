"""
Move extra available phone numbers from DEV DB to PROD DB.

Compares DEV DB available numbers against DEV reserve targets and moves
excess numbers to PROD. Numbers that already exist in PROD are skipped.

Usage:
    # Dry run (no DB changes):
    python dispatch/move_numbers_to_prod_dispatch.py --dry-run

    # Execute move:
    python dispatch/move_numbers_to_prod_dispatch.py
"""

import os
import sys
import argparse
from collections import defaultdict
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

import psycopg2
from tasks.purchase_twilio_number import COUNTRY_CONFIG, TWILIO_NUMBER_MODE


def move_numbers_to_prod(dry_run: bool = True):
    """
    Move extra available numbers from DEV to PROD.

    1. Fetch available numbers from DEV, grouped by country
    2. Compare against DEV reserve targets
    3. Move extras to PROD (insert into PROD, delete from DEV)
    """
    dev_url = os.getenv("DATABASE_URL")
    prod_url = os.getenv("DATABASE_URL_PROD")

    if not dev_url:
        print("ERROR: DATABASE_URL is not set")
        sys.exit(1)
    if not prod_url:
        print("ERROR: DATABASE_URL_PROD is not set")
        sys.exit(1)

    print("=" * 70)
    print("MOVE EXTRA NUMBERS: DEV → PROD")
    print(f"TWILIO_NUMBER_MODE: {TWILIO_NUMBER_MODE}")
    if dry_run:
        print("DRY RUN MODE - No DB changes will be made")
    else:
        print("EXECUTE MODE - DBs will be updated")
    print("=" * 70)
    print()

    # Step 1: Fetch available numbers from DEV
    print("[1/3] Fetching available numbers from DEV database...")
    dev_conn = psycopg2.connect(dev_url)
    dev_cur = dev_conn.cursor()
    dev_cur.execute("""
        SELECT phone_number_id, phone_number, friendly_name, country_code,
               area_code, region, twilio_sid
        FROM twilio_phone_numbers
        WHERE status = 'available'
        ORDER BY country_code, phone_number_id
    """)
    dev_rows = dev_cur.fetchall()

    by_country = defaultdict(list)
    for row in dev_rows:
        by_country[row[3]].append({
            'phone_number_id': row[0],
            'phone_number': row[1],
            'friendly_name': row[2],
            'country_code': row[3],
            'area_code': row[4],
            'region': row[5],
            'twilio_sid': row[6],
        })

    for country in sorted(by_country.keys()):
        print(f"  {country}: {len(by_country[country])} available")
    print()

    # Step 2: Fetch existing PROD numbers (to avoid duplicates)
    print("[2/3] Fetching existing numbers from PROD database...")
    prod_conn = psycopg2.connect(prod_url)
    prod_cur = prod_conn.cursor()
    prod_cur.execute("SELECT phone_number FROM twilio_phone_numbers")
    prod_phones = {row[0] for row in prod_cur.fetchall()}
    print(f"  Found {len(prod_phones)} numbers in PROD\n")

    # Step 3: Identify extras and move
    print("[3/3] Identifying extras to move...\n")
    to_move = []

    for country, numbers in sorted(by_country.items()):
        config = COUNTRY_CONFIG.get(country)
        if not config:
            print(f"  [{country}] No config found, skipping")
            continue

        targets = config['targets_dev']
        total_target = sum(targets.values())
        extra_count = len(numbers) - total_target

        if extra_count <= 0:
            print(f"  [{country}] {len(numbers)} available, target {total_target} — no extras")
            continue

        print(f"  [{country}] {len(numbers)} available, target {total_target} — {extra_count} extras to move:")

        # Keep the first `total_target` numbers, move the rest
        extras = numbers[total_target:]
        for n in extras:
            already_in_prod = n['phone_number'] in prod_phones
            status = "SKIP (already in PROD)" if already_in_prod else "MOVE"
            print(f"    {status}: #{n['phone_number_id']} {n['phone_number']} area={n['area_code']} region={n['region']}")

            if not already_in_prod:
                to_move.append(n)

    if not to_move:
        print("\n  (nothing to move)")
    elif not dry_run:
        print(f"\nExecuting {len(to_move)} moves...")
        for n in to_move:
            # Insert into PROD
            prod_cur.execute("""
                INSERT INTO twilio_phone_numbers (
                    phone_number, friendly_name, country_code, area_code, region,
                    twilio_sid, status, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, 'available', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (phone_number) DO NOTHING
            """, (
                n['phone_number'], n['friendly_name'], n['country_code'],
                n['area_code'], n['region'], n['twilio_sid']
            ))

            # Delete from DEV
            dev_cur.execute(
                "DELETE FROM twilio_phone_numbers WHERE phone_number_id = %s",
                (n['phone_number_id'],)
            )
            print(f"  Moved: {n['phone_number']}")

        prod_conn.commit()
        dev_conn.commit()

    dev_cur.close()
    dev_conn.close()
    prod_cur.close()
    prod_conn.close()

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    if dry_run:
        print("DRY RUN — no changes made")
    print(f"  {'Would move' if dry_run else 'Moved'}: {len(to_move)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Move extra available phone numbers from DEV DB to PROD DB'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Only report what would be moved, do not modify databases'
    )

    args = parser.parse_args()
    move_numbers_to_prod(dry_run=args.dry_run)
