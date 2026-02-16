"""
Import missing Twilio phone numbers into the database.

Fetches all incoming phone numbers from Twilio, filters to Speako-owned
numbers (friendly_name starts with a digit), and inserts any that are
missing from the target database.

Usage:
    # Dry run (no DB changes):
    python dispatch/import_twilio_numbers_dispatch.py --dry-run

    # Insert into DATABASE_URL:
    python dispatch/import_twilio_numbers_dispatch.py

    # Insert into DATABASE_URL_PROD:
    python dispatch/import_twilio_numbers_dispatch.py --prod
"""

import os
import sys
import argparse
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

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


def guess_country_code(phone_number: str) -> str:
    """Derive country code from phone number prefix."""
    if phone_number.startswith('+1'):
        return 'US'
    elif phone_number.startswith('+44'):
        return 'GB'
    elif phone_number.startswith('+61'):
        return 'AU'
    elif phone_number.startswith('+64'):
        return 'NZ'
    return 'XX'


def import_twilio_numbers(dry_run: bool = True, use_prod: bool = False):
    """
    Insert Twilio phone numbers missing from both DEV and PROD databases.

    1. Fetch all numbers from Twilio API (filtered to Speako numbers)
    2. Cross-reference with both DEV and PROD databases
    3. Insert numbers missing from BOTH into the target database
    """
    db_label = "PROD" if use_prod else "DEV"
    db_url_key = "DATABASE_URL_PROD" if use_prod else "DATABASE_URL"
    db_url = os.getenv(db_url_key)

    if not db_url:
        print(f"ERROR: {db_url_key} is not set")
        sys.exit(1)

    print("=" * 70)
    print(f"IMPORT TWILIO NUMBERS → {db_label} DB")
    if dry_run:
        print("DRY RUN MODE - No DB changes will be made")
    else:
        print("EXECUTE MODE - DB will be updated")
    print("=" * 70)
    print()

    # Step 1: Fetch from Twilio
    print("[1/3] Fetching numbers from Twilio API...")
    client = get_twilio_client()
    all_twilio_numbers = client.incoming_phone_numbers.list()
    # Filter: only Speako numbers (default friendly_name starts with digit)
    twilio_numbers = [tn for tn in all_twilio_numbers if tn.friendly_name and tn.friendly_name[0].isdigit()]
    skipped = len(all_twilio_numbers) - len(twilio_numbers)
    print(f"  Found {len(all_twilio_numbers)} total numbers on Twilio")
    print(f"  Filtered to {len(twilio_numbers)} Speako numbers (skipped {skipped} with custom names)\n")

    twilio_by_phone = {tn.phone_number: tn for tn in twilio_numbers}

    # Step 2: Fetch from BOTH databases to find truly orphaned numbers
    dev_url = os.getenv("DATABASE_URL")
    prod_url = os.getenv("DATABASE_URL_PROD")

    all_db_phones = set()

    if dev_url:
        print("[2/4] Fetching numbers from DEV database...")
        dev_conn = psycopg2.connect(dev_url)
        dev_cur = dev_conn.cursor()
        dev_cur.execute("SELECT phone_number FROM twilio_phone_numbers")
        dev_phones = {row[0] for row in dev_cur.fetchall()}
        dev_cur.close()
        dev_conn.close()
        all_db_phones.update(dev_phones)
        print(f"  Found {len(dev_phones)} numbers in DEV database")

    if prod_url:
        print("[3/4] Fetching numbers from PROD database...")
        prod_conn = psycopg2.connect(prod_url)
        prod_cur = prod_conn.cursor()
        prod_cur.execute("SELECT phone_number FROM twilio_phone_numbers")
        prod_phones = {row[0] for row in prod_cur.fetchall()}
        prod_cur.close()
        prod_conn.close()
        all_db_phones.update(prod_phones)
        print(f"  Found {len(prod_phones)} numbers in PROD database")

    print(f"  Combined unique numbers across both DBs: {len(all_db_phones)}\n")

    # Connect to target DB for inserts
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    # Step 4: Insert orphaned numbers (missing from BOTH databases)
    print(f"[4/4] Inserting orphaned numbers into {db_label} DB...\n")
    missing = []
    for phone, tn in twilio_by_phone.items():
        if phone not in all_db_phones:
            country_code = guess_country_code(phone)
            area_code = guess_area_code_from_number(phone, country_code)
            region = guess_region_from_number(phone, country_code)
            missing.append((phone, tn, country_code, area_code, region))

    if not missing:
        print("  (none — all Twilio numbers already in DB)")
    else:
        for phone, tn, country_code, area_code, region in missing:
            print(f"  INSERT: {phone} (country={country_code}, area={area_code}, region={region}, sid={tn.sid})")

            if not dry_run:
                safe_area_code = area_code[:10] if area_code else None
                cur.execute("""
                    INSERT INTO twilio_phone_numbers (
                        phone_number, friendly_name, country_code, area_code, region,
                        twilio_sid, status, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, 'available', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT (phone_number) DO NOTHING
                """, (phone, tn.friendly_name, country_code, safe_area_code, region, tn.sid))

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
    print(f"  Target DB: {db_label}")
    print(f"  Twilio numbers (filtered): {len(twilio_numbers)}")
    print(f"  Numbers in both DBs: {len(all_db_phones)}")
    print(f"  Orphaned (not in either DB): {len(missing)}")
    print(f"  {'Would insert' if dry_run else 'Inserted'}: {len(missing)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Import missing Twilio phone numbers into the database'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Only report what would be inserted, do not modify database'
    )
    parser.add_argument(
        '--prod',
        action='store_true',
        help='Target DATABASE_URL_PROD instead of DATABASE_URL'
    )

    args = parser.parse_args()
    import_twilio_numbers(dry_run=args.dry_run, use_prod=args.prod)
