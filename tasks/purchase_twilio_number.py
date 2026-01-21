"""
Twilio Phone Number Purchase Task

Purchase new Twilio phone numbers via API, configure webhooks, and save to database.

CLI Usage:
    # Search for available numbers
    python -m tasks.purchase_twilio_number search --country AU --region NSW
    
    # Purchase a specific number
    python -m tasks.purchase_twilio_number buy --phone-number +61212345678
    
    # Auto-purchase first available match
    python -m tasks.purchase_twilio_number buy --country AU --region VIC --auto-select
    
    # List all purchased numbers in database
    python -m tasks.purchase_twilio_number list
"""

import os
from pathlib import Path

# Load .env from the project root (celery directory)
from dotenv import load_dotenv
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

import argparse
import psycopg2
from psycopg2.extras import RealDictCursor
from twilio.rest import Client

# Only import Celery when running as a task, not for CLI
try:
    from tasks.celery_app import app
    CELERY_AVAILABLE = True
except ImportError:
    CELERY_AVAILABLE = False
    app = None

# Twilio credentials
TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')
TWILIO_WEBHOOK_URL = os.getenv('TWILIO_WEBHOOK_URL')
DATABASE_URL = os.getenv('DATABASE_URL')

# Mode: DEV or PROD - affects availability targets
TWILIO_NUMBER_MODE = os.getenv('TWILIO_NUMBER_MODE', 'DEV').upper()

# Default address SID for Australian numbers (Sydney CBD address)
# TODO: Move to env variable after testing
DEFAULT_ADDRESS_SID = "AD19502cc95b72134e88f2f069c4a78007"

# Australian region code to full name mapping (for Twilio API)
# Twilio requires full state names for accurate region filtering
AUSTRALIAN_REGION_NAMES = {
    'NSW': 'New South Wales',
    'ACT': 'Australian Capital Territory',
    'VIC': 'Victoria',
    'TAS': 'Tasmania',
    'QLD': 'Queensland',
    'SA': 'South Australia',
    'WA': 'Western Australia',
    'NT': 'Northern Territory',
}

# Reverse mapping: full name -> abbreviation
AUSTRALIAN_REGION_ABBREV = {v: k for k, v in AUSTRALIAN_REGION_NAMES.items()}

# Australian area code to region mapping
# Note: Some area codes are shared across multiple regions
# Area code 02: NSW, ACT
# Area code 03: VIC, TAS
# Area code 07: QLD
# Area code 08: SA, WA, NT
AUSTRALIAN_AREA_CODES = {
    'NSW': '02',
    'ACT': '02',  # Shares with NSW
    'VIC': '03',
    'TAS': '03',  # Shares with VIC
    'QLD': '07',
    'SA': '08',
    'WA': '08',   # Shares with SA
    'NT': '08',   # Shares with SA/WA
}

# Region availability targets for Australia (PROD mode)
# In DEV mode, all targets become 1
REGION_AVAILABILITY_TARGETS_PROD = {
    'NSW': 3,
    'ACT': 1,
    'VIC': 2,
    'TAS': 1,
    'QLD': 2,
    'SA': 1,
    'WA': 1,
    'NT': 1,
}

# DEV mode: just 1 per region for testing
REGION_AVAILABILITY_TARGETS_DEV = {region: 1 for region in REGION_AVAILABILITY_TARGETS_PROD}

# Select targets based on mode
REGION_AVAILABILITY_TARGETS = (
    REGION_AVAILABILITY_TARGETS_PROD if TWILIO_NUMBER_MODE == 'PROD' 
    else REGION_AVAILABILITY_TARGETS_DEV
)

# Maximum numbers to purchase in a single run (safety limit)
MAX_PURCHASE_PER_RUN = 10


def get_twilio_client():
    """Initialize and return Twilio client."""
    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
        raise ValueError("TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN must be set in environment")
    return Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)


def get_db_connection():
    """Get database connection."""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL must be set in environment")
    return psycopg2.connect(DATABASE_URL)


def list_addresses() -> list:
    """
    List all verified addresses in the Twilio account.
    
    Returns:
        List of address dictionaries with SID, name, and details
    """
    client = get_twilio_client()
    addresses = client.addresses.list()
    
    results = []
    for addr in addresses:
        results.append({
            'sid': addr.sid,
            'friendly_name': addr.friendly_name,
            'customer_name': addr.customer_name,
            'street': addr.street,
            'city': addr.city,
            'region': addr.region,
            'postal_code': addr.postal_code,
            'country_code': addr.iso_country,
            'validated': addr.validated,
            'verified': addr.verified,
        })
    
    return results


def search_available_numbers(
    country_code: str,
    region: str = None,
    area_code: str = None,
    locality: str = None,
    contains: str = None,
    number_type: str = "local",
    limit: int = 10
) -> list:
    """
    Search for available Twilio phone numbers.
    
    Args:
        country_code: ISO country code (e.g., 'AU', 'US', 'GB')
        region: State/province code (e.g., 'NSW', 'CA')
        area_code: Area code filter (e.g., '02', '415')
        locality: City name (e.g., 'Sydney', 'Melbourne')
        contains: Pattern matching (e.g., '*PIZZA*')
        number_type: 'local', 'toll_free', or 'mobile'
        limit: Maximum number of results
        
    Returns:
        List of available phone numbers with details
    """
    client = get_twilio_client()
    
    # Build search parameters
    search_params = {
        'voice_enabled': True,
        'limit': limit
    }
    
    if region:
        # For Australian numbers, convert abbreviation to full name for accurate API filtering
        # Twilio API requires full state names (e.g., "Tasmania" not "TAS")
        if country_code == 'AU' and region in AUSTRALIAN_REGION_NAMES:
            search_params['in_region'] = AUSTRALIAN_REGION_NAMES[region]
        else:
            search_params['in_region'] = region
    if area_code:
        search_params['area_code'] = area_code
    if locality:
        search_params['in_locality'] = locality
    if contains:
        search_params['contains'] = contains
    
    # Select the appropriate number type endpoint
    if number_type == "toll_free":
        available_numbers = client.available_phone_numbers(country_code).toll_free.list(**search_params)
    elif number_type == "mobile":
        available_numbers = client.available_phone_numbers(country_code).mobile.list(**search_params)
    else:  # local
        available_numbers = client.available_phone_numbers(country_code).local.list(**search_params)
    
    results = []
    for number in available_numbers:
        # Get the region from API and convert to abbreviation for storage
        api_region = getattr(number, 'region', None)
        region_abbrev = AUSTRALIAN_REGION_ABBREV.get(api_region, api_region) if api_region else None
        
        results.append({
            'phone_number': number.phone_number,
            'friendly_name': number.friendly_name,
            'locality': getattr(number, 'locality', None),
            'region': region_abbrev,  # Store abbreviation (e.g., 'TAS' not 'Tasmania')
            'region_full': api_region,  # Also keep full name for reference
            'postal_code': getattr(number, 'postal_code', None),
            'country_code': country_code,
            'capabilities': {
                'voice': number.capabilities.get('voice', False),
                'sms': number.capabilities.get('sms', False),
                'mms': number.capabilities.get('mms', False),
            },
            'address_requirements': getattr(number, 'address_requirements', 'none'),
        })
    
    return results


def purchase_number(
    phone_number: str,
    friendly_name: str = None,
    area_code: str = None,
    region: str = None,
    country_code: str = "AU",
    address_sid: str = None
) -> dict:
    """
    Purchase a Twilio phone number, configure webhook, and save to database.
    
    Args:
        phone_number: Phone number in E.164 format (e.g., '+61212345678')
        friendly_name: Optional display name
        area_code: Area code (for database storage)
        region: State/province (for database storage)
        country_code: ISO country code
        address_sid: Twilio Address SID for address verification
        
    Returns:
        Dictionary with purchase details
    """
    client = get_twilio_client()
    
    # Step 1: Purchase the number
    print(f"🛒 Purchasing {phone_number}...")
    
    purchase_params = {
        'phone_number': phone_number
    }
    if friendly_name:
        purchase_params['friendly_name'] = friendly_name
    if address_sid:
        purchase_params['address_sid'] = address_sid
        print(f"   Using address: {address_sid}")
    
    purchased_number = client.incoming_phone_numbers.create(**purchase_params)
    
    print(f"✅ Successfully purchased!")
    print(f"   SID: {purchased_number.sid}")
    print(f"   Phone: {purchased_number.phone_number}")
    
    # Step 2: Configure webhook
    webhook_configured = False
    if TWILIO_WEBHOOK_URL:
        print(f"\n🔗 Configuring webhook...")
        try:
            updated_number = client.incoming_phone_numbers(purchased_number.sid).update(
                voice_url=TWILIO_WEBHOOK_URL,
                voice_method='POST'
            )
            webhook_configured = True
            print(f"✅ Webhook configured!")
            print(f"   Voice URL: {updated_number.voice_url}")
            print(f"   Voice Method: {updated_number.voice_method}")
        except Exception as e:
            print(f"⚠️  Warning: Failed to configure webhook: {e}")
            print(f"   Please configure manually in Twilio console")
    else:
        print(f"\n⚠️  TWILIO_WEBHOOK_URL not set, skipping webhook configuration")
    
    # Step 3: Save to database
    print(f"\n💾 Saving to database...")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            INSERT INTO twilio_phone_numbers (
                phone_number,
                friendly_name,
                country_code,
                area_code,
                region,
                twilio_sid,
                status,
                created_at,
                updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING phone_number_id
        """, (
            purchased_number.phone_number,
            friendly_name or purchased_number.friendly_name,
            country_code,
            area_code,
            region,
            purchased_number.sid,
            'available'
        ))
        
        phone_number_id = cur.fetchone()[0]
        conn.commit()
        print(f"✅ Saved to database (phone_number_id: {phone_number_id})")
        
    except Exception as e:
        conn.rollback()
        print(f"⚠️  Warning: Failed to save to database: {e}")
        phone_number_id = None
    finally:
        cur.close()
        conn.close()
    
    return {
        'phone_number_id': phone_number_id,
        'phone_number': purchased_number.phone_number,
        'twilio_sid': purchased_number.sid,
        'friendly_name': purchased_number.friendly_name,
        'country_code': country_code,
        'area_code': area_code,
        'region': region,
        'webhook_configured': webhook_configured,
        'webhook_url': TWILIO_WEBHOOK_URL if webhook_configured else None
    }


def list_purchased_numbers() -> list:
    """List all purchased numbers from database."""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        cur.execute("""
            SELECT 
                phone_number_id,
                phone_number,
                friendly_name,
                country_code,
                area_code,
                region,
                twilio_sid,
                status,
                assigned_to_tenant_id,
                assigned_to_location_id,
                created_at
            FROM twilio_phone_numbers
            ORDER BY created_at DESC
        """)
        
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def get_available_count_by_region(conn) -> dict:
    """Get count of available numbers per region."""
    cur = conn.cursor()
    cur.execute("""
        SELECT region, COUNT(*) as count
        FROM twilio_phone_numbers
        WHERE status = 'available' AND region IS NOT NULL
        GROUP BY region
    """)
    result = {row[0]: row[1] for row in cur.fetchall()}
    cur.close()
    return result


def maintain_phone_number_availability(dry_run: bool = True) -> dict:
    """
    Maintain phone number availability per region.
    
    Checks current availability against REGION_AVAILABILITY_TARGETS
    and purchases numbers to fill any shortfall.
    
    Respects TWILIO_NUMBER_MODE:
    - DEV: Maintain 1 available number per region
    - PROD: Use full targets per region
    
    Args:
        dry_run: If True, only report what would be purchased (no actual purchases)
        
    Returns:
        Dictionary with summary of actions taken
    """
    print("=" * 70)
    print("MAINTAIN PHONE NUMBER AVAILABILITY")
    print(f"📍 Mode: {TWILIO_NUMBER_MODE}")
    if dry_run:
        print("🔍 DRY RUN MODE - No purchases will be made")
    else:
        print("⚠️  EXECUTE MODE - Purchases will be made!")
    print("=" * 70)
    print()
    
    # Connect to database
    conn = get_db_connection()
    
    # Get current availability
    print("📋 Checking availability per region...\n")
    current_counts = get_available_count_by_region(conn)
    
    # Calculate shortfall
    shortfall = {}
    total_needed = 0
    
    print(f"{'Region':<8} {'Target':<8} {'Current':<9} {'Needed':<8}")
    print("-" * 35)
    
    for region, target in REGION_AVAILABILITY_TARGETS.items():
        current = current_counts.get(region, 0)
        needed = max(0, target - current)
        shortfall[region] = needed
        total_needed += needed
        print(f"{region:<8} {target:<8} {current:<9} {needed:<8}")
    
    print()
    print(f"Total to purchase: {total_needed} numbers")
    
    if total_needed == 0:
        print("\n✅ All regions have sufficient availability. No purchases needed.")
        conn.close()
        return {
            'success': True,
            'total_needed': 0,
            'purchased': 0,
            'failed': 0,
            'details': {}
        }
    
    # Apply safety limit
    if total_needed > MAX_PURCHASE_PER_RUN:
        print(f"\n⚠️  Limiting to {MAX_PURCHASE_PER_RUN} purchases (safety limit)")
        # Distribute limit proportionally
        remaining_limit = MAX_PURCHASE_PER_RUN
        for region in shortfall:
            if shortfall[region] > 0:
                take = min(shortfall[region], remaining_limit)
                shortfall[region] = take
                remaining_limit -= take
                if remaining_limit <= 0:
                    break
    
    conn.close()
    
    # Purchase numbers for each region with shortfall
    results = {
        'success': True,
        'total_needed': total_needed,
        'purchased': 0,
        'failed': 0,
        'details': {}
    }
    
    print("\n" + "-" * 70)
    if dry_run:
        print("Numbers that would be purchased:")
    else:
        print("Purchasing...")
    print("-" * 70)
    
    client = get_twilio_client()
    
    for region, needed in shortfall.items():
        if needed == 0:
            continue
        
        print(f"\n{region} (need {needed}):")
        results['details'][region] = {'needed': needed, 'purchased': [], 'errors': []}
        
        # Search for available numbers in this region
        try:
            available_numbers = search_available_numbers(
                country_code='AU',
                region=region,
                number_type='local',
                limit=needed
            )
            
            if not available_numbers:
                msg = f"No available numbers found for {region}"
                print(f"  ⚠️  {msg}")
                results['details'][region]['errors'].append(msg)
                results['failed'] += needed
                continue
            
            # Purchase each number
            for num_info in available_numbers[:needed]:
                phone = num_info['phone_number']
                
                if dry_run:
                    print(f"  Would purchase: {phone}")
                    print(f"    Location: {num_info.get('locality', 'N/A')}, {region}")
                else:
                    try:
                        result = purchase_number(
                            phone_number=phone,
                            friendly_name=None,
                            area_code=num_info.get('locality'),
                            region=region,
                            country_code='AU',
                            address_sid=DEFAULT_ADDRESS_SID
                        )
                        
                        if result.get('phone_number_id'):
                            results['purchased'] += 1
                            results['details'][region]['purchased'].append(result)
                        else:
                            results['failed'] += 1
                            results['details'][region]['errors'].append(f"Failed to save {phone}")
                    except Exception as e:
                        error_msg = f"Error purchasing {phone}: {e}"
                        print(f"  ❌ {error_msg}")
                        results['details'][region]['errors'].append(error_msg)
                        results['failed'] += 1
                        
        except Exception as e:
            error_msg = f"Error searching for {region} numbers: {e}"
            print(f"  ❌ {error_msg}")
            results['details'][region]['errors'].append(error_msg)
            results['failed'] += needed
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    if dry_run:
        print(f"\n🔍 DRY RUN - No purchases were made")
        print(f"   Would purchase: {total_needed} numbers")
        print("\n" + "-" * 70)
        print("To execute, run:")
        print("  python -m tasks.purchase_twilio_number maintain --execute")
    else:
        print(f"\n✅ Purchased: {results['purchased']} numbers")
        if results['failed'] > 0:
            print(f"❌ Failed: {results['failed']} numbers")
    
    return results


# ============================================
# Celery Tasks
# ============================================

# Celery task for replenishing Twilio phone numbers
if CELERY_AVAILABLE and app:
    @app.task(bind=True, name='tasks.purchase_twilio_number.replenish_twilio_numbers')
    def replenish_twilio_numbers(self, dry_run: bool = False) -> dict:
        """
        Celery task to replenish Twilio phone numbers per region.
        
        This task checks current availability against REGION_AVAILABILITY_TARGETS
        and purchases numbers to fill any shortfall.
        
        Args:
            dry_run: If True, only report what would be purchased (no actual purchases)
            
        Returns:
            Dictionary with summary of actions taken
        """
        return maintain_phone_number_availability(dry_run=dry_run)
else:
    # Fallback for CLI usage
    replenish_twilio_numbers = None


def _purchase_twilio_number_task_impl(
    country_code: str,
    phone_number: str = None,
    region: str = None,
    area_code: str = None,
    locality: str = None,
    contains: str = None,
    number_type: str = "local",
    auto_select: bool = False,
    friendly_name: str = None
) -> dict:
    """
    Celery task to purchase a Twilio phone number.
    
    If phone_number is provided, purchases that specific number.
    If auto_select is True, searches and purchases the first available match.
    
    Args:
        country_code: ISO country code (required)
        phone_number: Specific number to purchase (optional)
        region: State/province filter for search
        area_code: Area code filter for search
        locality: City filter for search
        contains: Pattern filter for search
        number_type: 'local', 'toll_free', 'mobile'
        auto_select: If True, auto-purchase first search result
        friendly_name: Display name for the number
        
    Returns:
        Dictionary with purchase result
    """
    if phone_number:
        # Purchase specific number
        return purchase_number(
            phone_number=phone_number,
            friendly_name=friendly_name,
            area_code=area_code,
            region=region,
            country_code=country_code
        )
    
    if auto_select:
        # Search and purchase first match
        results = search_available_numbers(
            country_code=country_code,
            region=region,
            area_code=area_code,
            locality=locality,
            contains=contains,
            number_type=number_type,
            limit=1
        )
        
        if not results:
            return {
                'success': False,
                'error': 'No available numbers found matching criteria'
            }
        
        selected = results[0]
        return purchase_number(
            phone_number=selected['phone_number'],
            friendly_name=friendly_name,
            area_code=area_code or selected.get('locality'),
            region=region or selected.get('region'),
            country_code=country_code
        )
    
    return {
        'success': False,
        'error': 'Either phone_number or auto_select must be provided'
    }


# Register as Celery task if Celery is available
if CELERY_AVAILABLE and app is not None:
    purchase_twilio_number_task = app.task(_purchase_twilio_number_task_impl)
else:
    purchase_twilio_number_task = _purchase_twilio_number_task_impl


# ============================================
# CLI Interface
# ============================================

def cmd_search(args):
    """Handle search command."""
    print(f"🔍 Searching for numbers in {args.country}", end="")
    if args.region:
        print(f" (region: {args.region})", end="")
    if args.area_code:
        print(f" (area code: {args.area_code})", end="")
    if args.city:
        print(f" (city: {args.city})", end="")
    print("...\n")
    
    results = search_available_numbers(
        country_code=args.country,
        region=args.region,
        area_code=args.area_code,
        locality=args.city,
        contains=args.contains,
        number_type=args.type,
        limit=args.limit
    )
    
    if not results:
        print("❌ No available numbers found matching your criteria")
        return
    
    print(f"Found {len(results)} available number(s):\n")
    
    for i, num in enumerate(results, 1):
        caps = []
        if num['capabilities']['voice']:
            caps.append('Voice ✓')
        if num['capabilities']['sms']:
            caps.append('SMS ✓')
        if num['capabilities']['mms']:
            caps.append('MMS ✓')
        
        location = []
        if num['locality']:
            location.append(num['locality'])
        if num['region']:
            location.append(num['region'])
        
        location_str = ', '.join(location) if location else 'N/A'
        caps_str = ' | '.join(caps) if caps else 'N/A'
        
        print(f"  {i}. {num['phone_number']}")
        print(f"     Location: {location_str}")
        print(f"     Capabilities: {caps_str}")
        if num['address_requirements'] != 'none':
            print(f"     ⚠️  Address required: {num['address_requirements']}")
        print()
    
    print(f"To purchase, run:")
    print(f"  python -m tasks.purchase_twilio_number buy --phone-number <NUMBER>")


def cmd_buy(args):
    """Handle buy command."""
    # Use default address SID if not provided
    address_sid = args.address_sid or DEFAULT_ADDRESS_SID
    
    if args.phone_number:
        # Purchase specific number
        result = purchase_number(
            phone_number=args.phone_number,
            friendly_name=args.friendly_name,
            area_code=args.area_code,
            region=args.region,
            country_code=args.country,
            address_sid=address_sid
        )
    elif args.auto_select:
        if not args.country:
            print("❌ Error: --country is required when using --auto-select")
            return
        
        print(f"🔍 Searching for numbers in {args.country}...\n")
        
        results = search_available_numbers(
            country_code=args.country,
            region=args.region,
            area_code=args.area_code,
            locality=args.city,
            contains=args.contains,
            number_type=args.type,
            limit=1
        )
        
        if not results:
            print("❌ No available numbers found matching your criteria")
            return
        
        selected = results[0]
        print(f"📞 Selected: {selected['phone_number']}")
        if selected['locality'] or selected['region']:
            print(f"   Location: {selected['locality'] or ''}, {selected['region'] or ''}")
        print()
        
        result = purchase_number(
            phone_number=selected['phone_number'],
            friendly_name=args.friendly_name,
            area_code=args.area_code or selected.get('locality'),
            region=args.region or selected.get('region'),
            country_code=args.country,
            address_sid=address_sid
        )
    else:
        print("❌ Error: Either --phone-number or --auto-select is required")
        return
    
    print(f"\n{'='*50}")
    print("PURCHASE SUMMARY")
    print(f"{'='*50}")
    print(f"Phone Number: {result.get('phone_number')}")
    print(f"Twilio SID: {result.get('twilio_sid')}")
    print(f"Country: {result.get('country_code')}")
    print(f"Region: {result.get('region') or 'N/A'}")
    print(f"Area Code: {result.get('area_code') or 'N/A'}")
    print(f"Webhook: {'✓ Configured' if result.get('webhook_configured') else '✗ Not configured'}")
    print(f"Database ID: {result.get('phone_number_id') or 'Not saved'}")


def cmd_list(args):
    """Handle list command."""
    print("📋 Fetching purchased numbers from database...\n")
    
    numbers = list_purchased_numbers()
    
    if not numbers:
        print("No numbers found in database")
        return
    
    print(f"Found {len(numbers)} number(s):\n")
    print(f"{'ID':<5} {'Phone Number':<18} {'Country':<8} {'Region':<10} {'Status':<12} {'Tenant':<8}")
    print("-" * 70)
    
    for num in numbers:
        tenant = str(num['assigned_to_tenant_id']) if num['assigned_to_tenant_id'] else '-'
        print(f"{num['phone_number_id']:<5} {num['phone_number']:<18} {num['country_code']:<8} {num['region'] or '-':<10} {num['status']:<12} {tenant:<8}")


def cmd_addresses(args):
    """Handle addresses command - list verified addresses in Twilio account."""
    print("📍 Fetching verified addresses from Twilio account...\n")
    
    addresses = list_addresses()
    
    if not addresses:
        print("No addresses found in your Twilio account")
        print("\nTo add an address, go to:")
        print("  https://console.twilio.com/us1/develop/phone-numbers/regulatory-compliance/addresses")
        return
    
    print(f"Found {len(addresses)} address(es):\n")
    
    for addr in addresses:
        status = "✓ Verified" if addr['verified'] else ("⏳ Validated" if addr['validated'] else "❌ Pending")
        print(f"SID: {addr['sid']}")
        print(f"  Name: {addr['friendly_name'] or addr['customer_name']}")
        print(f"  Address: {addr['street']}, {addr['city']}, {addr['region']} {addr['postal_code']}")
        print(f"  Country: {addr['country_code']}")
        print(f"  Status: {status}")
        print()
    
    print("To use an address when purchasing, run:")
    print("  python -m tasks.purchase_twilio_number buy --phone-number <NUMBER> --address-sid <SID>")


def main():
    parser = argparse.ArgumentParser(
        description='Twilio Phone Number Purchase Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search for available numbers')
    search_parser.add_argument('--country', required=True, help='Country code (e.g., AU, US, GB)')
    search_parser.add_argument('--region', help='State/province (e.g., NSW, CA)')
    search_parser.add_argument('--area-code', help='Area code (e.g., 02, 415)')
    search_parser.add_argument('--city', help='City name (e.g., Sydney)')
    search_parser.add_argument('--contains', help='Pattern match (e.g., *PIZZA*)')
    search_parser.add_argument('--type', choices=['local', 'toll_free', 'mobile'], default='local', help='Number type')
    search_parser.add_argument('--limit', type=int, default=10, help='Max results')
    
    # Buy command
    buy_parser = subparsers.add_parser('buy', help='Purchase a phone number')
    buy_parser.add_argument('--phone-number', help='Specific number to purchase (E.164 format)')
    buy_parser.add_argument('--country', default='AU', help='Country code (default: AU)')
    buy_parser.add_argument('--region', help='State/province for search')
    buy_parser.add_argument('--area-code', help='Area code for search')
    buy_parser.add_argument('--city', help='City for search')
    buy_parser.add_argument('--contains', help='Pattern match for search')
    buy_parser.add_argument('--type', choices=['local', 'toll_free', 'mobile'], default='local', help='Number type')
    buy_parser.add_argument('--auto-select', action='store_true', help='Auto-purchase first available match')
    buy_parser.add_argument('--friendly-name', help='Display name for the number')
    buy_parser.add_argument('--address-sid', help='Twilio Address SID for address verification (run "addresses" to list)')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List purchased numbers from database')
    
    # Addresses command
    addresses_parser = subparsers.add_parser('addresses', help='List verified addresses in Twilio account')
    
    # Maintain command
    maintain_parser = subparsers.add_parser('maintain', help='Maintain phone number availability per region')
    maintain_parser.add_argument('--execute', action='store_true', 
                                  help='Actually purchase numbers (default is dry-run)')
    maintain_parser.add_argument('--dry-run', action='store_true', 
                                  help='Show what would be purchased without buying (default)')
    
    args = parser.parse_args()
    
    if args.command == 'search':
        cmd_search(args)
    elif args.command == 'buy':
        cmd_buy(args)
    elif args.command == 'list':
        cmd_list(args)
    elif args.command == 'addresses':
        cmd_addresses(args)
    elif args.command == 'maintain':
        dry_run = not args.execute
        maintain_phone_number_availability(dry_run=dry_run)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
