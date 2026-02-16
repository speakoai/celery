"""
Sync Twilio phone numbers between Twilio API and local database.

Fetches all incoming phone numbers from Twilio and ensures the local
database is in sync — inserts missing numbers and updates NULL area_code
or region fields using Twilio metadata.

Usage:
    python dispatch/sync_twilio_numbers_dispatch.py

    # Dry run (no DB changes):
    python dispatch/sync_twilio_numbers_dispatch.py --dry-run
"""

import sys
import argparse
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from tasks.purchase_twilio_number import get_twilio_client, get_db_connection


# Map Twilio's iso_country to our country_code format (they should match, but just in case)
COUNTRY_MAP = {
    'AU': 'AU', 'US': 'US', 'GB': 'GB', 'CA': 'CA', 'NZ': 'NZ',
}

# AU area code to region mapping
AU_AREA_CODE_REGION = {
    '02': 'NSW',  # Also ACT, but default to NSW
    '03': 'VIC',  # Also TAS
    '04': None,   # Mobile
    '07': 'QLD',
    '08': 'SA',   # Also WA, NT
}

# More specific AU prefix-to-region mapping
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
        # Try most specific prefix first
        for prefix_len in [8, 7, 6, 5, 4]:
            prefix = phone_number[:prefix_len]
            if prefix in AU_PREFIX_REGION:
                return AU_PREFIX_REGION[prefix]
    return None


def guess_area_code_from_number(phone_number: str, country_code: str) -> str | None:
    """Extract area code from phone number."""
    if country_code == 'US' or country_code == 'CA':
        # +1NXXNXXXXXX — area code is digits 2-4
        if phone_number.startswith('+1') and len(phone_number) >= 5:
            return phone_number[2:5]
    elif country_code == 'AU':
        # +61XXXXXXXXX — area code is digit after +61
        if phone_number.startswith('+61') and len(phone_number) >= 5:
            return '0' + phone_number[3]
    elif country_code == 'NZ':
        # +64XXXXXXXXX — area code is 1 digit after +64
        if phone_number.startswith('+64') and len(phone_number) >= 5:
            return '0' + phone_number[3]
    return None


def sync_twilio_numbers(dry_run: bool = True):
    """
    Sync Twilio incoming phone numbers with local database.

    1. Fetch all numbers from Twilio API
    2. Cross-reference with database
    3. Insert missing numbers into DB
    4. Update NULL area_code/region from Twilio metadata or phone parsing
    """
    print("=" * 70)
    print("SYNC TWILIO PHONE NUMBERS")
    if dry_run:
        print("DRY RUN MODE - No DB changes will be made")
    else:
        print("EXECUTE MODE - DB will be updated")
    print("=" * 70)
    print()

    # Step 1: Fetch all numbers from Twilio
    print("[1/3] Fetching numbers from Twilio API...")
    client = get_twilio_client()
    twilio_numbers = client.incoming_phone_numbers.list()
    print(f"  Found {len(twilio_numbers)} numbers on Twilio\n")

    # Build Twilio lookup by phone number
    twilio_by_phone = {}
    for tn in twilio_numbers:
        twilio_by_phone[tn.phone_number] = tn

    # Step 2: Fetch all numbers from database
    print("[2/3] Fetching numbers from database...")
    conn = get_db_connection()
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

    # Step 3: Sync
    print("[3/3] Syncing...\n")

    inserted = 0
    updated = 0
    skipped = 0

    # 3a: Numbers on Twilio but not in DB → insert
    print("--- Missing from DB (will insert) ---")
    for phone, tn in twilio_by_phone.items():
        if phone not in db_by_phone:
            country_code = COUNTRY_MAP.get(tn.phone_number[:3].replace('+', ''), None)
            # Derive country from phone prefix if not in map
            if not country_code:
                if phone.startswith('+1'):
                    country_code = 'US'  # Default +1 to US (could be CA)
                elif phone.startswith('+44'):
                    country_code = 'GB'
                elif phone.startswith('+61'):
                    country_code = 'AU'
                elif phone.startswith('+64'):
                    country_code = 'NZ'
                else:
                    country_code = 'XX'

            area_code = guess_area_code_from_number(phone, country_code)
            region = guess_region_from_number(phone, country_code)

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
                inserted += 1

    if inserted == 0 and dry_run:
        for phone in twilio_by_phone:
            if phone not in db_by_phone:
                inserted += 1
        if inserted == 0:
            print("  (none)")

    # 3b: Numbers in DB with NULL area_code or region → update
    print("\n--- DB entries with NULL area_code or region (will update) ---")
    for phone, db_row in db_by_phone.items():
        needs_update = False
        new_area_code = db_row['area_code']
        new_region = db_row['region']

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

        # Also update twilio_sid if missing
        new_sid = db_row['twilio_sid']
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
        else:
            skipped += 1

    if updated == 0 and not any(
        not db_by_phone[p]['area_code'] or not db_by_phone[p]['region']
        for p in db_by_phone
    ):
        print("  (none)")

    # 3c: Numbers in DB but not on Twilio (informational only)
    print("\n--- DB entries not found on Twilio (informational) ---")
    orphaned_db = 0
    for phone, db_row in db_by_phone.items():
        if phone not in twilio_by_phone:
            print(f"  WARNING: #{db_row['phone_number_id']} {phone} (status={db_row.get('twilio_sid', 'N/A')}) — not found on Twilio")
            orphaned_db += 1
    if orphaned_db == 0:
        print("  (none)")

    if not dry_run:
        conn.commit()

    cur.close()
    conn.close()

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    if dry_run:
        print(f"DRY RUN — no changes made")
    print(f"  Twilio numbers: {len(twilio_numbers)}")
    print(f"  DB numbers: {len(db_rows)}")
    print(f"  Would insert: {inserted}" if dry_run else f"  Inserted: {inserted}")
    print(f"  Would update: {sum(1 for p in db_by_phone if not db_by_phone[p]['area_code'] or not db_by_phone[p]['region'])}" if dry_run else f"  Updated: {updated}")
    print(f"  DB-only (not on Twilio): {orphaned_db}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Sync Twilio phone numbers with local database'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Only report what would be changed, do not modify database'
    )

    args = parser.parse_args()
    sync_twilio_numbers(dry_run=args.dry_run)
