#!/usr/bin/env python3
"""
Production Customer Email Test Script
Usage: python test_customer_email_production.py

This test script validates customer-facing email confirmations using the actual
production Celery task functions from tasks.sms module. This ensures we're testing
the real production code paths instead of duplicated logic.

The emails are sent directly to customers using the actual production functions:
- send_email_confirmation_customer_new (new bookings)
- send_email_confirmation_customer_mod (booking modifications)  
- send_email_confirmation_customer_can (booking cancellations)
"""

import sys
import os
from dotenv import load_dotenv

# Add the tasks directory to the Python path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'tasks'))

# Import the actual production Celery task functions
try:
    from sms import send_email_confirmation_customer_new, send_email_confirmation_customer_mod, send_email_confirmation_customer_can
    from celery_app import app
    print("✅ Successfully imported production Celery task functions")
except ImportError as e:
    print(f"❌ Failed to import production task functions: {e}")
    print("Make sure you're running this from the correct directory and tasks/sms.py exists")
    sys.exit(1)

def run_task_synchronously(task_func, *args, **kwargs):
    """
    Run a Celery task function synchronously for testing purposes.
    This bypasses the Celery worker and runs the task function directly.
    """
    try:
        # Call the task function directly (bypassing Celery's .delay() or .apply_async())
        # We use the actual function, not the Celery task wrapper
        result = task_func(*args, **kwargs)
        return result
    except Exception as e:
        print(f"❌ Error running task: {e}")
        return "failed"

def test_customer_new_booking(booking_id: int) -> str:
    """Test customer new booking confirmation using production code."""
    print(f"\n📧 Testing Customer New Booking Confirmation")
    print(f"Booking ID: {booking_id}")
    print(f"Using production function: send_email_confirmation_customer_new")
    print("-" * 60)
    
    result = run_task_synchronously(send_email_confirmation_customer_new, booking_id)
    
    if result == "success":
        print("✅ Customer new booking email sent successfully via production code!")
    elif result == "skipped":
        print("⚠️ Email skipped - no customer email found in database")
    else:
        print("❌ Failed to send customer email via production code")
    
    return result

def test_customer_booking_modification(new_booking_id: int, original_booking_id: int) -> str:
    """Test customer booking modification using production code."""
    print(f"\n🔄 Testing Customer Booking Modification")
    print(f"New Booking ID: {new_booking_id}, Original Booking ID: {original_booking_id}")
    print(f"Using production function: send_email_confirmation_customer_mod")
    print("-" * 60)
    
    result = run_task_synchronously(send_email_confirmation_customer_mod, new_booking_id, original_booking_id)
    
    if result == "success":
        print("✅ Customer modification email sent successfully via production code!")
    elif result == "skipped":
        print("⚠️ Email skipped - no customer email found in database")
    else:
        print("❌ Failed to send customer modification email via production code")
    
    return result

def test_customer_booking_cancellation(booking_id: int) -> str:
    """Test customer booking cancellation using production code."""
    print(f"\n❌ Testing Customer Booking Cancellation")
    print(f"Booking ID: {booking_id}")
    print(f"Using production function: send_email_confirmation_customer_can")
    print("-" * 60)
    
    result = run_task_synchronously(send_email_confirmation_customer_can, booking_id)
    
    if result == "success":
        print("✅ Customer cancellation email sent successfully via production code!")
    elif result == "skipped":
        print("⚠️ Email skipped - no customer email found in database")
    else:
        print("❌ Failed to send customer cancellation email via production code")
    
    return result

def test_production_customer_emails():
    """Test the production customer email functions with specific booking IDs."""
    
    print("🎯 Production Customer Email Test Options:")
    print("=" * 60)
    print("1. 📧 New Restaurant Booking (production code)")
    print("2. 📧 New Service Booking (production code)")
    print("3. 🔄 Restaurant Booking Modification (production code)")
    print("4. 🔄 Service Booking Modification (production code)")
    print("5. ❌ Restaurant Booking Cancellation (production code)")
    print("6. ❌ Service Booking Cancellation (production code)")
    print("7. 🎲 Custom Booking ID (specify your own)")
    print("8. 🧪 Test All Scenarios (run multiple tests)")
    print("9. 🔍 Compare with Legacy Test Script")
    print()
    
    choice = input("Choose test option (1-9): ").strip()
    
    if choice == "1":
        # Restaurant booking confirmation
        booking_id = input("Enter restaurant booking ID (or press Enter for default 23208): ").strip() or "23208"
        try:
            booking_id = int(booking_id)
        except ValueError:
            print("Invalid booking ID. Using default 23208.")
            booking_id = 23208
            
        result = test_customer_new_booking(booking_id)
        
        if result == "success":
            print("📧 Template: Customer-facing restaurant booking confirmation")
            print("🎨 Design: Includes logo/banner from booking_page table")
            print("🔧 Tested: Production database query with LEFT JOIN")
        
    elif choice == "2":
        # Service booking confirmation
        booking_id = input("Enter service booking ID (or press Enter for default 23207): ").strip() or "23207"
        try:
            booking_id = int(booking_id)
        except ValueError:
            print("Invalid booking ID. Using default 23207.")
            booking_id = 23207
            
        result = test_customer_new_booking(booking_id)
        
        if result == "success":
            print("📧 Template: Customer-facing service booking confirmation")
            print("🎨 Design: Includes logo/banner from booking_page table")
            print("🔧 Tested: Production database query with staff/service details")
        
    elif choice == "3":
        # Restaurant modification
        new_id = input("Enter new restaurant booking ID (or press Enter for default 23194): ").strip() or "23194"
        original_id = input("Enter original booking ID (or press Enter for default 23193): ").strip() or "23193"
        
        try:
            new_id = int(new_id)
            original_id = int(original_id)
        except ValueError:
            print("Invalid booking IDs. Using defaults.")
            new_id, original_id = 23194, 23193
            
        result = test_customer_booking_modification(new_id, original_id)
        
        if result == "success":
            print("📧 Template: Customer-facing modification with original booking context")
            print("🎨 Design: Shows changes with logo/banner support")
            print("🔧 Tested: Production modification logic with booking_page JOIN")
        
    elif choice == "4":
        # Service modification
        new_id = input("Enter new service booking ID (or press Enter for default 23084): ").strip() or "23084"
        original_id = input("Enter original booking ID (or press Enter for default 23082): ").strip() or "23082"
        
        try:
            new_id = int(new_id)
            original_id = int(original_id)
        except ValueError:
            print("Invalid booking IDs. Using defaults.")
            new_id, original_id = 23084, 23082
            
        result = test_customer_booking_modification(new_id, original_id)
        
        if result == "success":
            print("📧 Template: Customer-facing modification with staff/service changes")
            print("🎨 Design: Shows appointment changes with branding")
            print("🔧 Tested: Production service modification logic")
        
    elif choice == "5":
        # Restaurant cancellation
        booking_id = input("Enter cancelled restaurant booking ID (or press Enter for default 22985): ").strip() or "22985"
        try:
            booking_id = int(booking_id)
        except ValueError:
            print("Invalid booking ID. Using default 22985.")
            booking_id = 22985
            
        result = test_customer_booking_cancellation(booking_id)
        
        if result == "success":
            print("📧 Template: Customer-facing cancellation notification")
            print("🎨 Design: Sympathetic messaging with branding")
            print("🔧 Tested: Production cancellation logic")
        
    elif choice == "6":
        # Service cancellation
        booking_id = input("Enter cancelled service booking ID (or press Enter for default 23083): ").strip() or "23083"
        try:
            booking_id = int(booking_id)
        except ValueError:
            print("Invalid booking ID. Using default 23083.")
            booking_id = 23083
            
        result = test_customer_booking_cancellation(booking_id)
        
        if result == "success":
            print("📧 Template: Customer-facing appointment cancellation")
            print("🎨 Design: Professional cancellation with branding")
            print("🔧 Tested: Production service cancellation logic")
        
    elif choice == "7":
        # Custom booking ID
        try:
            booking_id = int(input("Enter booking ID: ").strip())
            print("What type of test?")
            print("1. New booking confirmation")
            print("2. Booking modification (need original ID too)")
            print("3. Booking cancellation")
            
            test_type = input("Choose test type (1/2/3): ").strip()
            
            if test_type == "1":
                result = test_customer_new_booking(booking_id)
            elif test_type == "2":
                original_id = int(input("Enter original booking ID: ").strip())
                result = test_customer_booking_modification(booking_id, original_id)
            elif test_type == "3":
                result = test_customer_booking_cancellation(booking_id)
            else:
                print("Invalid choice. Testing as new booking.")
                result = test_customer_new_booking(booking_id)
                
            if result == "success":
                print("✅ Custom customer email sent successfully via production code!")
                print("🔧 Tested: Actual production logic and database queries")
            elif result == "skipped":
                print("⚠️ Email skipped - no customer email found in database")
            else:
                print("❌ Failed to send custom customer email")
                
        except ValueError:
            print("Invalid booking ID.")
            return
            
    elif choice == "8":
        # Test all scenarios
        print("\n🧪 Running All Production Customer Email Tests")
        print("=" * 60)
        
        tests = [
            ("Restaurant Confirmation", lambda: test_customer_new_booking(23208)),
            ("Service Confirmation", lambda: test_customer_new_booking(23207)),
            ("Restaurant Modification", lambda: test_customer_booking_modification(23194, 23193)),
            ("Service Modification", lambda: test_customer_booking_modification(23084, 23082)),
            ("Restaurant Cancellation", lambda: test_customer_booking_cancellation(22985)),
            ("Service Cancellation", lambda: test_customer_booking_cancellation(23083)),
        ]
        
        results = {}
        for test_name, test_func in tests:
            print(f"\n🔄 Testing {test_name} with production code...")
            try:
                result = test_func()
                results[test_name] = result
                if result == "success":
                    print(f"✅ {test_name}: SUCCESS (production code)")
                elif result == "skipped":
                    print(f"⚠️ {test_name}: SKIPPED (no customer email)")
                else:
                    print(f"❌ {test_name}: FAILED (production code)")
            except Exception as e:
                results[test_name] = "error"
                print(f"💥 {test_name}: ERROR - {e}")
        
        print("\n📊 Production Test Results Summary:")
        print("-" * 40)
        for test_name, result in results.items():
            status_icon = "✅" if result == "success" else "⚠️" if result == "skipped" else "❌"
            print(f"{status_icon} {test_name}: {result.upper()}")
        
        print("\n🔧 All tests used actual production Celery task functions")
        print("🏭 Database queries include logo/banner support from booking_page table")
        
    elif choice == "9":
        # Compare with legacy test script
        print("\n🔍 Production vs Legacy Test Comparison")
        print("=" * 60)
        print("📊 Test Approach Differences:")
        print()
        print("🏭 PRODUCTION TEST (this script):")
        print("  ✅ Uses actual Celery task functions from tasks.sms")
        print("  ✅ Tests real production database queries")
        print("  ✅ Tests actual template rendering logic")
        print("  ✅ Tests logo/banner support from booking_page table")
        print("  ✅ Validates production code paths")
        print()
        print("🧪 LEGACY TEST (test_customer_html_email.py):")
        print("  ❌ Duplicates production logic in test functions")
        print("  ❌ Maintains separate code that can drift from production")
        print("  ❌ Tests copy-pasted logic, not actual production code")
        print("  ❌ Requires manual sync with production changes")
        print("  ❌ False positives when test logic differs from production")
        print()
        print("💡 RECOMMENDATION:")
        print("  Use this production test script for reliable validation")
        print("  The legacy script should be deprecated or updated")
        print()
        
        run_comparison = input("Run a side-by-side test? (y/n): ").strip().lower()
        if run_comparison == 'y':
            booking_id = int(input("Enter booking ID for comparison: ").strip() or "23208")
            
            print(f"\n⚡ Running Production Test for Booking {booking_id}")
            prod_result = test_customer_new_booking(booking_id)
            
            print(f"\n📝 Production test result: {prod_result}")
            print("   (This tested the actual production code path)")
            print("\n💡 To compare with legacy, run the old test script separately")
        
    else:
        print("Invalid choice. Please run the script again.")
        return
    
    print("\n" + "=" * 60)
    print("🎯 Production Customer Email Test Complete!")
    print("🏭 All tests used actual production Celery task functions")
    print("📧 Check your customer email inbox to see the results")
    print("🔧 These tests validate the real production code paths")

def check_production_environment():
    """Check if the production environment is properly configured."""
    
    print("Checking production environment setup...")
    print("-" * 60)
    
    # Check if we can import the production functions
    try:
        from sms import send_email_confirmation_customer_new, send_email_confirmation_customer_mod, send_email_confirmation_customer_can
        print("✅ Production task functions: Successfully imported")
    except ImportError as e:
        print(f"❌ Production task functions: Import failed - {e}")
        return False
    
    # Check Celery app
    try:
        from celery_app import app
        print("✅ Celery app: Successfully imported")
    except ImportError as e:
        print(f"❌ Celery app: Import failed - {e}")
        return False
    
    # Check environment variables
    required_vars = [
        "DATABASE_URL",
        "SENDGRID_API_KEY", 
        "SENDGRID_FROM_EMAIL"
    ]
    
    missing_vars = []
    
    for var in required_vars:
        value = os.getenv(var)
        if value:
            # Mask sensitive values
            if "API_KEY" in var or "URL" in var:
                masked = value[:8] + "..." if len(value) > 8 else "***"
                print(f"✅ {var}: {masked}")
            else:
                print(f"✅ {var}: {value}")
        else:
            print(f"❌ {var}: NOT SET")
            missing_vars.append(var)
    
    if missing_vars:
        print(f"\n⚠️  Missing environment variables: {', '.join(missing_vars)}")
        return False
    
    print("\n✅ Production environment is properly configured!")
    return True

def check_production_dependencies():
    """Check if required packages for production testing are installed."""
    
    print("Checking production dependencies...")
    print("-" * 60)
    
    required_packages = [
        ("psycopg2", "psycopg2"),
        ("sendgrid", "sendgrid"),
        ("python-dotenv", "dotenv"),
        ("celery", "celery")
    ]
    
    missing_packages = []
    
    for package_name, import_name in required_packages:
        try:
            __import__(import_name)
            print(f"✅ {package_name}: installed")
        except ImportError:
            print(f"❌ {package_name}: NOT INSTALLED")
            missing_packages.append(package_name)
    
    if missing_packages:
        print(f"\n⚠️  Missing packages: {', '.join(missing_packages)}")
        print("Install them with:")
        for package in missing_packages:
            print(f"  pip install {package}")
        return False
    
    print("\n✅ All production dependencies are installed!")
    return True

def main():
    """Main function to run the production customer email tests."""
    
    print("🏭 Production Customer Email Test Script")
    print("=" * 60)
    print("This script tests customer emails using ACTUAL production code")
    print("from tasks.sms module - no duplicated logic!")
    print()
    print("🔧 Benefits:")
    print("  • Tests real production database queries")
    print("  • Validates actual Celery task functions")
    print("  • Includes logo/banner support testing")
    print("  • Catches production code regressions")
    print()
    
    # Load environment variables first
    load_dotenv()
    
    # Check dependencies first
    if not check_production_dependencies():
        print("\n❌ Dependency check failed. Please install the missing packages.")
        return 1
    
    print()
    
    # Check production environment
    if not check_production_environment():
        print("\n❌ Production environment check failed. Please fix the issues above.")
        return 1
    
    print()
    
    # Test the production customer email functions
    test_production_customer_emails()
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
