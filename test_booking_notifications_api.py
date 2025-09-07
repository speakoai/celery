#!/usr/bin/env python3
"""
Booking Notifications API Test Script
Usage: python test_booking_notifications_api.py

This test script validates the /api/booking/notifications/send endpoint which triggers
SMS and email notifications for booking actions. It tests the complete notification
orchestration including customer and merchant communications.

The API endpoint triggers multiple Celery tasks:
- SMS notifications (when notify_customer=true)
- Customer email notifications (when notify_customer=true) 
- Merchant email notifications (always)

Test scenarios include new bookings, modifications, and cancellations for both
restaurant and service business types.
"""

import sys
import os
import json
import time
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv

try:
    import requests
    print("✅ Successfully imported requests library")
except ImportError:
    print("❌ Failed to import requests library")
    print("Install it with: pip install requests")
    sys.exit(1)

class BookingNotificationsAPITester:
    """Test client for the booking notifications API endpoint."""
    
    def __init__(self, base_url: str = None, api_key: str = None):
        """Initialize the API tester with configuration."""
        self.base_url = base_url or os.getenv('API_BASE_URL', 'http://localhost:5000')
        self.api_key = api_key or os.getenv('API_SECRET_KEY')
        self.session = requests.Session()
        
        if self.api_key:
            self.session.headers.update({
                'X-API-Key': self.api_key,
                'Content-Type': 'application/json'
            })
        
        # Test data from production environment (same as test_customer_email_production.py)
        self.test_data = {
            'restaurant': {
                'new': 23208,
                'modify': {'new': 23194, 'original': 23193},
                'cancel': 22985
            },
            'service': {
                'new': 23207,
                'modify': {'new': 23084, 'original': 23082},
                'cancel': 23083
            }
        }
    
    def check_api_connectivity(self) -> bool:
        """Check if the API is accessible and properly configured."""
        print("Checking API connectivity and authentication...")
        print("-" * 60)
        
        try:
            # Test health endpoint first (no auth required)
            health_url = f"{self.base_url}/api/health"
            print(f"🔍 Testing: {health_url}")
            response = self.session.get(health_url, timeout=30)  # Increased timeout
            
            if response.status_code == 200:
                print(f"✅ API Health Check: {health_url}")
                health_data = response.json()
                print(f"   Status: {health_data.get('status', 'unknown')}")
                print(f"   Service: {health_data.get('service', 'unknown')}")
            else:
                print(f"❌ API Health Check Failed: HTTP {response.status_code}")
                print(f"   Response: {response.text}")
                return False
                
        except requests.exceptions.Timeout:
            print(f"❌ API Health Check Failed: Request timed out after 30 seconds")
            print(f"   This suggests the Flask app might be starting up or blocked")
            print(f"   Check the Flask app console for any startup errors")
            return False
        except requests.exceptions.ConnectionError as e:
            print(f"❌ API Health Check Failed: Connection error - {e}")
            print(f"   Make sure the Flask app is running on {self.base_url}")
            return False
        
        # Test authentication with a simple request
        if not self.api_key:
            print("❌ API Key: NOT SET (required for authenticated endpoints)")
            print("   Set API_SECRET_KEY environment variable")
            return False
        
        try:
            # Test task status endpoint with dummy task ID (should return 500 but with auth)
            test_url = f"{self.base_url}/api/task/dummy-task-id"
            response = self.session.get(test_url, timeout=10)
            
            if response.status_code in [200, 500]:  # 500 is expected for dummy task ID
                print(f"✅ API Authentication: Working")
                print(f"   API Key: {self.api_key[:8]}..." if len(self.api_key) > 8 else "***")
            elif response.status_code == 401:
                print(f"❌ API Authentication: Failed (HTTP 401)")
                print("   Check API_SECRET_KEY environment variable")
                return False
            else:
                print(f"⚠️  Unexpected response: HTTP {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            print(f"❌ API Authentication Test Failed: {e}")
            return False
        
        print(f"✅ Base URL: {self.base_url}")
        print("\n✅ API connectivity and authentication verified!")
        return True
    
    def send_notification_request(self, booking_id: int, action: str, business_type: str, 
                                notify_customer: any = True, original_booking_id: int = None) -> Dict:
        """Send a notification request to the API and return the response."""
        
        url = f"{self.base_url}/api/booking/notifications/send"
        
        payload = {
            'booking_id': booking_id,
            'action': action,
            'business_type': business_type,
            'notify_customer': notify_customer
        }
        
        if original_booking_id:
            payload['original_booking_id'] = original_booking_id
        
        print(f"📤 Sending API request to: {url}")
        print(f"   Payload: {json.dumps(payload, indent=2)}")
        
        try:
            response = self.session.post(url, json=payload, timeout=30)
            
            print(f"📨 Response: HTTP {response.status_code}")
            
            if response.status_code == 202:
                data = response.json()
                print(f"✅ Request successful!")
                print(f"   Message: {data.get('message', 'No message')}")
                print(f"   Total Tasks: {data.get('total_tasks', 0)}")
                
                if 'tasks' in data:
                    print(f"   Task Details:")
                    for i, task in enumerate(data['tasks'], 1):
                        print(f"     {i}. {task['type']}: {task['description']}")
                        print(f"        Task ID: {task['task_id']}")
                
                return {'success': True, 'data': data, 'status_code': response.status_code}
                
            else:
                try:
                    error_data = response.json()
                    print(f"❌ Request failed: {error_data.get('error', 'Unknown error')}")
                    if 'message' in error_data:
                        print(f"   Details: {error_data['message']}")
                except:
                    print(f"❌ Request failed: HTTP {response.status_code}")
                    print(f"   Response: {response.text}")
                
                return {'success': False, 'error': response.text, 'status_code': response.status_code}
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Network error: {e}")
            return {'success': False, 'error': str(e), 'status_code': 0}
    
    def monitor_task_status(self, task_id: str, timeout: int = 60) -> Dict:
        """Monitor a specific task until completion or timeout."""
        
        url = f"{self.base_url}/api/task/{task_id}"
        start_time = time.time()
        
        print(f"🔍 Monitoring task: {task_id}")
        
        while time.time() - start_time < timeout:
            try:
                response = self.session.get(url, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    status = data.get('status', 'unknown')
                    ready = data.get('ready', False)
                    
                    if ready:
                        success = data.get('success', False)
                        if success:
                            print(f"✅ Task completed successfully")
                            result = data.get('result', 'No result')
                            print(f"   Result: {result}")
                        else:
                            print(f"❌ Task failed")
                            error = data.get('error', 'Unknown error')
                            print(f"   Error: {error}")
                        
                        return data
                    else:
                        print(f"⏳ Task status: {status} (waiting...)")
                        time.sleep(2)
                else:
                    print(f"❌ Failed to check task status: HTTP {response.status_code}")
                    break
                    
            except requests.exceptions.RequestException as e:
                print(f"❌ Error checking task status: {e}")
                break
        
        print(f"⏰ Task monitoring timed out after {timeout} seconds")
        return {'status': 'timeout', 'ready': False}
    
    def monitor_all_tasks(self, task_list: List[Dict], timeout: int = 60) -> Dict:
        """Monitor multiple tasks and return summary."""
        
        print(f"\n🔍 Monitoring {len(task_list)} tasks...")
        print("-" * 60)
        
        results = {}
        for task in task_list:
            task_id = task['task_id']
            task_type = task['type']
            description = task['description']
            
            print(f"\n📋 Monitoring {task_type}: {description}")
            result = self.monitor_task_status(task_id, timeout)
            results[task_id] = {
                'type': task_type,
                'description': description,
                'result': result
            }
        
        # Summary
        print(f"\n📊 Task Monitoring Summary:")
        print("-" * 40)
        
        successful = 0
        failed = 0
        timeout_count = 0
        
        for task_id, info in results.items():
            task_type = info['type']
            result = info['result']
            
            if result.get('ready') and result.get('success'):
                print(f"✅ {task_type}: SUCCESS")
                successful += 1
            elif result.get('ready') and not result.get('success'):
                print(f"❌ {task_type}: FAILED")
                failed += 1
            else:
                print(f"⏰ {task_type}: TIMEOUT")
                timeout_count += 1
        
        print(f"\n📈 Results: {successful} successful, {failed} failed, {timeout_count} timeout")
        
        return results
    
    def test_new_booking(self, business_type: str, notify_customer: any = True) -> Dict:
        """Test new booking notification."""
        
        booking_id = self.test_data[business_type]['new']
        
        print(f"\n📧 Testing New {business_type.title()} Booking Notification")
        print(f"Booking ID: {booking_id}")
        print(f"Business Type: {business_type}")
        print(f"Notify Customer: {notify_customer}")
        print("-" * 60)
        
        result = self.send_notification_request(
            booking_id=booking_id,
            action='new',
            business_type=business_type,
            notify_customer=notify_customer
        )
        
        if result['success']:
            expected_tasks = 3 if notify_customer else 1
            actual_tasks = result['data'].get('total_tasks', 0)
            
            print(f"\n📊 Task Count Validation:")
            print(f"   Expected: {expected_tasks} tasks")
            print(f"   Actual: {actual_tasks} tasks")
            
            if actual_tasks == expected_tasks:
                print(f"✅ Task count matches expectation")
            else:
                print(f"❌ Task count mismatch!")
            
            # Monitor tasks if requested
            if input("\nMonitor task execution? (y/n): ").strip().lower() == 'y':
                self.monitor_all_tasks(result['data']['tasks'])
        
        return result
    
    def test_modify_booking(self, business_type: str, notify_customer: any = True) -> Dict:
        """Test booking modification notification."""
        
        modify_data = self.test_data[business_type]['modify']
        new_booking_id = modify_data['new']
        original_booking_id = modify_data['original']
        
        print(f"\n🔄 Testing {business_type.title()} Booking Modification Notification")
        print(f"New Booking ID: {new_booking_id}")
        print(f"Original Booking ID: {original_booking_id}")
        print(f"Business Type: {business_type}")
        print(f"Notify Customer: {notify_customer}")
        print("-" * 60)
        
        result = self.send_notification_request(
            booking_id=new_booking_id,
            action='modify',
            business_type=business_type,
            notify_customer=notify_customer,
            original_booking_id=original_booking_id
        )
        
        if result['success']:
            expected_tasks = 3 if notify_customer else 1
            actual_tasks = result['data'].get('total_tasks', 0)
            
            print(f"\n📊 Task Count Validation:")
            print(f"   Expected: {expected_tasks} tasks")
            print(f"   Actual: {actual_tasks} tasks")
            
            if actual_tasks == expected_tasks:
                print(f"✅ Task count matches expectation")
            else:
                print(f"❌ Task count mismatch!")
            
            # Monitor tasks if requested
            if input("\nMonitor task execution? (y/n): ").strip().lower() == 'y':
                self.monitor_all_tasks(result['data']['tasks'])
        
        return result
    
    def test_cancel_booking(self, business_type: str, notify_customer: any = True) -> Dict:
        """Test booking cancellation notification."""
        
        booking_id = self.test_data[business_type]['cancel']
        
        print(f"\n❌ Testing {business_type.title()} Booking Cancellation Notification")
        print(f"Booking ID: {booking_id}")
        print(f"Business Type: {business_type}")
        print(f"Notify Customer: {notify_customer}")
        print("-" * 60)
        
        result = self.send_notification_request(
            booking_id=booking_id,
            action='cancel',
            business_type=business_type,
            notify_customer=notify_customer
        )
        
        if result['success']:
            expected_tasks = 3 if notify_customer else 1
            actual_tasks = result['data'].get('total_tasks', 0)
            
            print(f"\n📊 Task Count Validation:")
            print(f"   Expected: {expected_tasks} tasks")
            print(f"   Actual: {actual_tasks} tasks")
            
            if actual_tasks == expected_tasks:
                print(f"✅ Task count matches expectation")
            else:
                print(f"❌ Task count mismatch!")
            
            # Monitor tasks if requested
            if input("\nMonitor task execution? (y/n): ").strip().lower() == 'y':
                self.monitor_all_tasks(result['data']['tasks'])
        
        return result
    
    def test_notify_customer_variations(self) -> Dict:
        """Test different notify_customer parameter variations."""
        
        print(f"\n🧪 Testing notify_customer Parameter Variations")
        print("=" * 60)
        
        booking_id = self.test_data['restaurant']['new']
        
        test_cases = [
            ("true (boolean)", True, 3),
            ("false (boolean)", False, 1),
            ('empty string ""', "", 3),  # Should be treated as true
            ("missing parameter", None, 3),  # Should default to true
        ]
        
        results = {}
        
        for case_name, notify_value, expected_tasks in test_cases:
            print(f"\n📋 Testing notify_customer = {case_name}")
            print(f"   Expected task count: {expected_tasks}")
            
            payload = {
                'booking_id': booking_id,
                'action': 'new',
                'business_type': 'rest'
            }
            
            if notify_value is not None:
                payload['notify_customer'] = notify_value
            
            url = f"{self.base_url}/api/booking/notifications/send"
            
            try:
                response = self.session.post(url, json=payload, timeout=30)
                
                if response.status_code == 202:
                    data = response.json()
                    actual_tasks = data.get('total_tasks', 0)
                    
                    if actual_tasks == expected_tasks:
                        print(f"✅ PASS: Got {actual_tasks} tasks (expected {expected_tasks})")
                        results[case_name] = 'PASS'
                    else:
                        print(f"❌ FAIL: Got {actual_tasks} tasks (expected {expected_tasks})")
                        results[case_name] = 'FAIL'
                else:
                    print(f"❌ FAIL: HTTP {response.status_code}")
                    results[case_name] = 'ERROR'
                    
            except Exception as e:
                print(f"❌ ERROR: {e}")
                results[case_name] = 'ERROR'
        
        print(f"\n📊 notify_customer Variation Test Results:")
        print("-" * 50)
        for case, result in results.items():
            status_icon = "✅" if result == "PASS" else "❌"
            print(f"{status_icon} {case}: {result}")
        
        return results
    
    def test_all_scenarios(self) -> Dict:
        """Test all booking scenarios comprehensively."""
        
        print(f"\n🧪 Running All Booking Notification Scenarios")
        print("=" * 60)
        
        scenarios = [
            ("Restaurant New Booking", lambda: self.test_new_booking('rest', True)),
            ("Restaurant New Booking (No Customer Notify)", lambda: self.test_new_booking('rest', False)),
            ("Service New Booking", lambda: self.test_new_booking('service', True)),
            ("Service New Booking (No Customer Notify)", lambda: self.test_new_booking('service', False)),
            ("Restaurant Modification", lambda: self.test_modify_booking('rest', True)),
            ("Restaurant Modification (No Customer Notify)", lambda: self.test_modify_booking('rest', False)),
            ("Service Modification", lambda: self.test_modify_booking('service', True)),
            ("Service Modification (No Customer Notify)", lambda: self.test_modify_booking('service', False)),
            ("Restaurant Cancellation", lambda: self.test_cancel_booking('rest', True)),
            ("Restaurant Cancellation (No Customer Notify)", lambda: self.test_cancel_booking('rest', False)),
            ("Service Cancellation", lambda: self.test_cancel_booking('service', True)),
            ("Service Cancellation (No Customer Notify)", lambda: self.test_cancel_booking('service', False)),
        ]
        
        results = {}
        
        for scenario_name, test_func in scenarios:
            print(f"\n🔄 Testing: {scenario_name}")
            print("-" * 40)
            
            try:
                result = test_func()
                if result['success']:
                    results[scenario_name] = 'SUCCESS'
                    print(f"✅ {scenario_name}: SUCCESS")
                else:
                    results[scenario_name] = 'FAILED'
                    print(f"❌ {scenario_name}: FAILED")
            except Exception as e:
                results[scenario_name] = 'ERROR'
                print(f"💥 {scenario_name}: ERROR - {e}")
        
        print(f"\n📊 All Scenarios Test Results Summary:")
        print("=" * 50)
        
        success_count = sum(1 for r in results.values() if r == 'SUCCESS')
        total_count = len(results)
        
        for scenario, result in results.items():
            status_icon = "✅" if result == "SUCCESS" else "❌"
            print(f"{status_icon} {scenario}: {result}")
        
        print(f"\n📈 Overall Results: {success_count}/{total_count} scenarios successful")
        
        return results
    
    def run_interactive_menu(self):
        """Run the interactive test menu."""
        
        while True:
            print(f"\n🎯 Booking Notifications API Test Menu")
            print("=" * 60)
            print("1. 📧 Test New Restaurant Booking")
            print("2. 📧 Test New Service Booking")
            print("3. 🔄 Test Restaurant Modification")
            print("4. 🔄 Test Service Modification")
            print("5. ❌ Test Restaurant Cancellation")
            print("6. ❌ Test Service Cancellation")
            print("7. 🧪 Test notify_customer Variations")
            print("8. 🎲 Custom Booking Test")
            print("9. 🔍 Monitor Specific Task ID")
            print("10. 🧪 Test All Scenarios")
            print("11. 📊 API Health Check")
            print("12. 🔚 Exit")
            print()
            
            choice = input("Choose test option (1-12): ").strip()
            
            try:
                if choice == "1":
                    self.test_new_booking('rest')
                    
                elif choice == "2":
                    self.test_new_booking('service')
                    
                elif choice == "3":
                    self.test_modify_booking('rest')
                    
                elif choice == "4":
                    self.test_modify_booking('service')
                    
                elif choice == "5":
                    self.test_cancel_booking('rest')
                    
                elif choice == "6":
                    self.test_cancel_booking('service')
                    
                elif choice == "7":
                    self.test_notify_customer_variations()
                    
                elif choice == "8":
                    self._custom_booking_test()
                    
                elif choice == "9":
                    self._monitor_task_test()
                    
                elif choice == "10":
                    self.test_all_scenarios()
                    
                elif choice == "11":
                    self.check_api_connectivity()
                    
                elif choice == "12":
                    print("👋 Goodbye!")
                    break
                    
                else:
                    print("❌ Invalid choice. Please try again.")
                    
            except KeyboardInterrupt:
                print("\n\n⚠️  Test interrupted by user")
                continue
            except Exception as e:
                print(f"\n💥 Unexpected error: {e}")
                continue
    
    def _custom_booking_test(self):
        """Handle custom booking test input."""
        
        print(f"\n🎲 Custom Booking Test")
        print("-" * 30)
        
        try:
            booking_id = int(input("Enter booking ID: ").strip())
            
            print("Available actions:")
            print("1. new")
            print("2. modify") 
            print("3. cancel")
            action_choice = input("Choose action (1/2/3): ").strip()
            
            action_map = {'1': 'new', '2': 'modify', '3': 'cancel'}
            action = action_map.get(action_choice)
            
            if not action:
                print("❌ Invalid action choice")
                return
            
            print("Available business types:")
            print("1. rest (restaurant)")
            print("2. service")
            business_choice = input("Choose business type (1/2): ").strip()
            
            business_map = {'1': 'rest', '2': 'service'}
            business_type = business_map.get(business_choice)
            
            if not business_type:
                print("❌ Invalid business type choice")
                return
            
            notify_input = input("Notify customer? (y/n/empty): ").strip().lower()
            if notify_input == 'y':
                notify_customer = True
            elif notify_input == 'n':
                notify_customer = False
            elif notify_input == 'empty':
                notify_customer = ""
            else:
                notify_customer = True  # default
            
            original_booking_id = None
            if action == 'modify':
                original_booking_id = int(input("Enter original booking ID: ").strip())
            
            result = self.send_notification_request(
                booking_id=booking_id,
                action=action,
                business_type=business_type,
                notify_customer=notify_customer,
                original_booking_id=original_booking_id
            )
            
            if result['success'] and input("\nMonitor tasks? (y/n): ").strip().lower() == 'y':
                self.monitor_all_tasks(result['data']['tasks'])
                
        except ValueError:
            print("❌ Invalid input. Please enter valid numbers.")
        except Exception as e:
            print(f"❌ Error: {e}")
    
    def _monitor_task_test(self):
        """Handle task monitoring input."""
        
        print(f"\n🔍 Monitor Specific Task")
        print("-" * 25)
        
        task_id = input("Enter task ID to monitor: ").strip()
        if task_id:
            self.monitor_task_status(task_id)
        else:
            print("❌ Task ID cannot be empty")


def check_environment() -> bool:
    """Check if the environment is properly configured."""
    
    print("Checking environment setup...")
    print("-" * 60)
    
    # Check required environment variables
    required_vars = ['API_SECRET_KEY']
    optional_vars = ['API_BASE_URL']
    
    missing_required = []
    
    for var in required_vars:
        value = os.getenv(var)
        if value:
            masked = value[:8] + "..." if len(value) > 8 else "***"
            print(f"✅ {var}: {masked}")
        else:
            print(f"❌ {var}: NOT SET")
            missing_required.append(var)
    
    for var in optional_vars:
        value = os.getenv(var)
        if value:
            print(f"✅ {var}: {value}")
        else:
            default_value = 'http://localhost:5000' if var == 'API_BASE_URL' else 'N/A'
            print(f"⚠️  {var}: NOT SET (will use default: {default_value})")
    
    if missing_required:
        print(f"\n❌ Missing required environment variables: {', '.join(missing_required)}")
        print("Set them in your .env file or environment:")
        for var in missing_required:
            print(f"  export {var}='your_value_here'")
        return False
    
    print("\n✅ Environment configuration is valid!")
    return True


def main():
    """Main function to run the API tests."""
    
    print("🚀 Booking Notifications API Test Script")
    print("=" * 60)
    print("This script tests the /api/booking/notifications/send endpoint")
    print("which orchestrates SMS and email notifications for booking actions.")
    print()
    print("🔧 Features:")
    print("  • Tests all notification types (SMS, customer email, merchant email)")
    print("  • Validates notify_customer parameter handling")
    print("  • Monitors Celery task execution")
    print("  • Tests both restaurant and service business types")
    print()
    
    # Load environment variables
    load_dotenv()
    
    # Check environment
    if not check_environment():
        print("\n❌ Environment check failed. Please fix the issues above.")
        return 1
    
    print()
    
    # Initialize API tester
    tester = BookingNotificationsAPITester()
    
    # Check API connectivity
    if not tester.check_api_connectivity():
        print("\n❌ API connectivity check failed. Please fix the issues above.")
        print("Make sure:")
        print("  1. Flask app is running")
        print("  2. API_SECRET_KEY is correct")
        print("  3. Base URL is accessible")
        return 1
    
    print()
    
    # Run interactive menu
    tester.run_interactive_menu()
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
