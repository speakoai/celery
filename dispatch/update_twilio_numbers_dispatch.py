"""
Update NULL area_code/region/twilio_sid on existing database entries.

Parses phone number format to fill in missing area_code and region,
and matches against Twilio API to fill missing twilio_sid.

Usage:
    # Dry run (no DB changes):
    python dispatch/update_twilio_numbers_dispatch.py --dry-run

    # Execute updates on DATABASE_URL:
    python dispatch/update_twilio_numbers_dispatch.py
"""

import sys
import argparse
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

import os
import psycopg2
from tasks.purchase_twilio_number import get_twilio_client


# AU area code to region mapping
AU_PREFIX_REGION = {
    '+612': 'NSW', '+6126': 'ACT',
    '+613': 'VIC', '+61362': 'TAS', '+61363': 'TAS', '+61364': 'TAS',
    '+617': 'QLD',
    '+618': 'SA',  '+6189': 'WA', '+61899': 'WA', '+6188': 'SA',
    '+61860': 'WA', '+61861': 'WA', '+61862': 'WA', '+61863': 'WA',
    '+61864': 'WA', '+61865': 'WA',
}


def guess_region_from_number(phone_number: str, country_code: str) -> str | None:
    """Best-effort region guess from phone number format."""
    if country_code == 'AU':
        for prefix_len in [8, 7, 6, 5, 4]:
            prefix = phone_number[:prefix_len]
            if prefix in AU_PREFIX_REGION:
                return AU_PREFIX_REGION[prefix]
    return None


def guess_area_code_from_number(phone_number: str, country_code: str) -> str | None:
    """Extract area code from phone number."""
    if country_code == 'US' or country_code == 'CA':
        if phone_number.startswith('+1') and len(phone_number) >= 5:
            return phone_number[2:5]
    elif country_code == 'AU':
        if phone_number.startswith('+61') and len(phone_number) >= 5:
            return '0' + phone_number[3]
    elif country_code == 'NZ':
        if phone_number.startswith('+64') and len(phone_number) >= 5:
            return '0' + phone_number[3]
    return None


def update_twilio_numbers(dry_run: bool = True, use_prod: bool = False):
    """
    Update NULL area_code, region, and twilio_sid on existing DB entries.

    1. Fetch all numbers from Twilio API (for twilio_sid lookup)
    2. Fetch DB entries with NULL fields
    3. Fill in missing values from phone number parsing or Twilio metadata
    """
    db_label = "PROD" if use_prod else "DEV"
    db_url_key = "DATABASE_URL_PROD" if use_prod else "DATABASE_URL"
    db_url = os.getenv(db_url_key)

    if not db_url:
        print(f"ERROR: {db_url_key} is not set")
        sys.exit(1)

    print("=" * 70)
    print(f"UPDATE TWILIO NUMBER METADATA → {db_label} DB")
    if dry_run:
        print("DRY RUN MODE - No DB changes will be made")
    else:
        print("EXECUTE MODE - DB will be updated")
    print("=" * 70)
    print()

    # Step 1: Fetch from Twilio (for twilio_sid lookup)
    print("[1/3] Fetching numbers from Twilio API...")
    client = get_twilio_client()
    all_twilio_numbers = client.incoming_phone_numbers.list()
    twilio_numbers = [tn for tn in all_twilio_numbers if tn.friendly_name and tn.friendly_name[0].isdigit()]
    twilio_by_phone = {tn.phone_number: tn for tn in twilio_numbers}
    print(f"  Loaded {len(twilio_numbers)} Speako numbers for lookup\n")

    # Step 2: Fetch DB entries
    print(f"[2/3] Fetching numbers from {db_label} database...")
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute("""
        SELECT phone_number_id, phone_number, country_code, area_code, region, twilio_sid
        FROM twilio_phone_numbers
    """)
    db_rows = cur.fetchall()
    print(f"  Found {len(db_rows)} numbers in database\n")

    db_by_phone = {}
    for row in db_rows:
        db_by_phone[row[1]] = {
            'phone_number_id': row[0],
            'phone_number': row[1],
            'country_code': row[2],
            'area_code': row[3],
            'region': row[4],
            'twilio_sid': row[5],
        }

    # Step 3: Update NULL fields
    print("[3/3] Updating NULL fields...\n")
    updated = 0

    for phone, db_row in db_by_phone.items():
        needs_update = False
        new_area_code = db_row['area_code']
        new_region = db_row['region']
        new_sid = db_row['twilio_sid']

        if not db_row['area_code']:
            guessed = guess_area_code_from_number(phone, db_row['country_code'])
            if guessed:
                new_area_code = guessed
                needs_update = True

        if not db_row['region']:
            guessed = guess_region_from_number(phone, db_row['country_code'])
            if guessed:
                new_region = guessed
                needs_update = True

        if not db_row['twilio_sid'] and phone in twilio_by_phone:
            new_sid = twilio_by_phone[phone].sid
            needs_update = True

        if needs_update:
            changes = []
            if new_area_code != db_row['area_code']:
                changes.append(f"area_code: NULL→{new_area_code}")
            if new_region != db_row['region']:
                changes.append(f"region: NULL→{new_region}")
            if new_sid != db_row['twilio_sid']:
                changes.append(f"twilio_sid: NULL→{new_sid}")

            print(f"  UPDATE #{db_row['phone_number_id']} {phone}: {', '.join(changes)}")

            if not dry_run:
                safe_area_code = new_area_code[:10] if new_area_code else None
                cur.execute("""
                    UPDATE twilio_phone_numbers
                    SET area_code = COALESCE(%s, area_code),
                        region = COALESCE(%s, region),
                        twilio_sid = COALESCE(%s, twilio_sid),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE phone_number_id = %s
                """, (safe_area_code, new_region, new_sid, db_row['phone_number_id']))
                updated += 1

    if updated == 0 and not any(
        not db_by_phone[p]['area_code'] or not db_by_phone[p]['region'] or not db_by_phone[p]['twilio_sid']
        for p in db_by_phone
    ):
        print("  (none — all fields populated)")

    if not dry_run:
        conn.commit()

    cur.close()
    conn.close()

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    if dry_run:
        print("DRY RUN — no changes made")
    total_needing_update = sum(
        1 for p in db_by_phone
        if not db_by_phone[p]['area_code'] or not db_by_phone[p]['region'] or not db_by_phone[p]['twilio_sid']
    )
    print(f"  DB numbers: {len(db_rows)}")
    print(f"  {'Would update' if dry_run else 'Updated'}: {total_needing_update if dry_run else updated}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Update NULL area_code/region/twilio_sid on existing DB entries'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Only report what would be updated, do not modify database'
    )

    parser.add_argument(
        '--prod',
        action='store_true',
        help='Target DATABASE_URL_PROD instead of DATABASE_URL'
    )

    args = parser.parse_args()
    update_twilio_numbers(dry_run=args.dry_run, use_prod=args.prod)
