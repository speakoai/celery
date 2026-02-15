"""
Dispatch script for replenishing Twilio phone numbers.

This script runs the Twilio number replenishment task directly (not via Celery).
It checks current phone number availability per country/region and purchases
numbers to meet the configured targets.

Supported countries: AU, US, GB, CA, NZ

Respects TWILIO_NUMBER_MODE:
- DEV: Maintain 1 available number per country
- PROD: Use full targets per country/region

Usage:
    python dispatch/replenish_twilio_numbers_dispatch.py

    # Dry run (no purchases):
    python dispatch/replenish_twilio_numbers_dispatch.py --dry-run
"""

import os
import sys
import argparse
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Debug: Print Twilio env vars (masked)
import os
_sid = os.getenv('TWILIO_ACCOUNT_SID', '')
_token = os.getenv('TWILIO_AUTH_TOKEN', '')
_webhook = os.getenv('TWILIO_WEBHOOK_URL', '')
print(f"[DEBUG] TWILIO_ACCOUNT_SID: {'SET (' + _sid[:6] + '...)' if _sid else 'NOT SET'}")
print(f"[DEBUG] TWILIO_AUTH_TOKEN: {'SET (' + _token[:4] + '...)' if _token else 'NOT SET'}")
print(f"[DEBUG] TWILIO_WEBHOOK_URL: {_webhook if _webhook else 'NOT SET'}")

from tasks.purchase_twilio_number import (
    maintain_phone_number_availability,
    TWILIO_NUMBER_MODE,
    COUNTRY_AVAILABILITY_TARGETS
)


def dispatch_replenish_task(dry_run: bool = False):
    """
    Main dispatch function.
    
    Runs the Twilio number replenishment directly (synchronously).
    This is intended for cron job execution.
    
    Args:
        dry_run: If True, only report what would be purchased
    """
    print("=" * 80)
    print("[DISPATCH] Twilio Phone Number Replenishment")
    print(f"[DISPATCH] Started at: {datetime.now().isoformat()}")
    print(f"[DISPATCH] Mode: {TWILIO_NUMBER_MODE}")
    print(f"[DISPATCH] Dry Run: {dry_run}")
    print("=" * 80)
    print()
    
    # Show configured targets per country
    print("[INFO] Configured availability targets:")
    for country_code, targets in COUNTRY_AVAILABILITY_TARGETS.items():
        print(f"  [{country_code}]")
        for region, target in targets.items():
            display_region = region if region != '_national' else '(national)'
            print(f"    - {display_region}: {target}")
    print()
    
    # Run the task directly (not via Celery)
    try:
        result = maintain_phone_number_availability(dry_run=dry_run)
        
        # Print summary
        print()
        print("=" * 80)
        print("[DISPATCH] Task Summary:")
        print(f"  Success: {result.get('success', False)}")
        print(f"  Total needed: {result.get('total_needed', 0)}")
        print(f"  Purchased: {result.get('purchased', 0)}")
        print(f"  Failed: {result.get('failed', 0)}")
        print(f"[DISPATCH] Completed at: {datetime.now().isoformat()}")
        print("=" * 80)
        
        # Return exit code based on success
        if result.get('failed', 0) > 0:
            return 1  # Partial failure
        return 0  # Success
        
    except Exception as e:
        print(f"[ERROR] Task failed with exception: {e}")
        print(f"[DISPATCH] Failed at: {datetime.now().isoformat()}")
        return 2  # Error


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Dispatch Twilio phone number replenishment task'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Only report what would be purchased, do not make actual purchases'
    )
    
    args = parser.parse_args()
    
    exit_code = dispatch_replenish_task(dry_run=args.dry_run)
    sys.exit(exit_code)
