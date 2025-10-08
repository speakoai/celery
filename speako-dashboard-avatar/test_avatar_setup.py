#!/usr/bin/env python3
"""
Test script for avatar scanner setup and connectivity.

This script tests:
1. Environment variables setup
2. R2 connectivity
3. OpenAI API connectivity
4. Basic functionality
"""

import os
import sys
from dotenv import load_dotenv
import boto3
import requests
import json

# Load environment variables
load_dotenv()

def test_environment_variables():
    """Test if all required environment variables are set."""
    print("🔧 Testing Environment Variables...")
    
    required_vars = [
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY", 
        "R2_ENDPOINT_URL",
        "R2_BUCKET_NAME",
        "R2_ACCOUNT_ID"
    ]
    
    optional_vars = [
        "OPENAI_API_KEY"
    ]
    
    missing_required = []
    missing_optional = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_required.append(var)
        else:
            print(f"  ✅ {var}: {'*' * 8}{os.getenv(var)[-4:]}")
    
    for var in optional_vars:
        if not os.getenv(var):
            missing_optional.append(var)
        elif os.getenv(var) == "your_openai_api_key_here":
            missing_optional.append(var)
        else:
            print(f"  ✅ {var}: {'*' * 8}{os.getenv(var)[-4:]}")
    
    if missing_required:
        print(f"  ❌ Missing required variables: {', '.join(missing_required)}")
        return False
    
    if missing_optional:
        print(f"  ⚠️  Missing optional variables: {', '.join(missing_optional)}")
        print("     Note: OPENAI_API_KEY is required for analysis functionality")
    
    print("  ✅ Environment variables check passed!")
    return True

def test_r2_connectivity():
    """Test R2 bucket connectivity."""
    print("\n☁️  Testing R2 Connectivity...")
    
    try:
        r2_client = boto3.client(
            's3',
            endpoint_url=os.getenv("R2_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
            region_name='auto'
        )
        
        # Test bucket access
        bucket_name = os.getenv("R2_BUCKET_NAME")
        response = r2_client.head_bucket(Bucket=bucket_name)
        print(f"  ✅ Successfully connected to bucket: {bucket_name}")
        
        # Test listing objects in avatar folder
        target_folder = "staff-profiles/avatar/"
        response = r2_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=target_folder,
            MaxKeys=10
        )
        
        if 'Contents' in response:
            file_count = len([obj for obj in response['Contents'] if obj['Key'] != target_folder])
            print(f"  ✅ Found {file_count} files in {target_folder}")
            
            # Show first few files
            for i, obj in enumerate(response['Contents'][:3]):
                if obj['Key'] != target_folder:
                    print(f"    📁 {obj['Key']} ({obj['Size']} bytes)")
        else:
            print(f"  ⚠️  No files found in {target_folder}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ R2 connectivity failed: {str(e)}")
        return False

def test_openai_connectivity():
    """Test OpenAI API connectivity."""
    print("\n🤖 Testing OpenAI Connectivity...")
    
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or api_key == "your_openai_api_key_here":
        print("  ⚠️  OpenAI API key not configured")
        return False
    
    try:
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        # Test with a simple completion
        payload = {
            "model": "gpt-3.5-turbo",
            "messages": [{"role": "user", "content": "Hello, this is a test."}],
            "max_tokens": 10
        }
        
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            print("  ✅ OpenAI API connection successful")
            
            # Test if gpt-4o (vision model) is accessible
            payload["model"] = "gpt-4o"
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                print("  ✅ GPT-4 Vision (gpt-4o) model accessible")
            else:
                print("  ⚠️  GPT-4 Vision model not accessible, falling back to GPT-3.5")
            
            return True
        else:
            print(f"  ❌ OpenAI API error: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"  ❌ OpenAI connectivity failed: {str(e)}")
        return False

def test_dependencies():
    """Test if required Python packages are installed."""
    print("\n📦 Testing Dependencies...")
    
    required_packages = [
        ("boto3", "AWS SDK for R2 access"),
        ("requests", "HTTP requests for OpenAI API"),
        ("python-dotenv", "Environment variable loading"),
        ("flask", "Web framework for API")
    ]
    
    optional_packages = [
        ("openai", "OpenAI official SDK (alternative)")
    ]
    
    all_good = True
    
    for package, description in required_packages:
        try:
            __import__(package.replace("-", "_"))
            print(f"  ✅ {package}: {description}")
        except ImportError:
            print(f"  ❌ {package}: Missing - {description}")
            all_good = False
    
    for package, description in optional_packages:
        try:
            __import__(package.replace("-", "_"))
            print(f"  ✅ {package}: {description}")
        except ImportError:
            print(f"  ⚠️  {package}: Not installed - {description}")
    
    return all_good

def main():
    """Run all tests."""
    print("🚀 Avatar Scanner Setup Test")
    print("=" * 50)
    
    tests = [
        ("Environment Variables", test_environment_variables),
        ("Dependencies", test_dependencies),
        ("R2 Connectivity", test_r2_connectivity),
        ("OpenAI Connectivity", test_openai_connectivity)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"  ❌ {test_name} test failed with error: {str(e)}")
            results.append((test_name, False))
    
    print("\n" + "=" * 50)
    print("📊 Test Summary:")
    
    all_passed = True
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status}: {test_name}")
        if not result:
            all_passed = False
    
    print("\n" + "=" * 50)
    
    if all_passed:
        print("🎉 All tests passed! You're ready to run the avatar scanner.")
        print("\nNext steps:")
        print("1. Make sure your OpenAI API key is set in .env file")
        print("2. Run: python avatar_scanner.py")
        print("3. Check the generated avatar_catalog.json file")
    else:
        print("⚠️  Some tests failed. Please fix the issues above before running the scanner.")
        print("\nTroubleshooting:")
        print("1. Check your .env file has all required variables")
        print("2. Verify your R2 credentials and bucket access")
        print("3. Ensure your OpenAI API key is valid and has sufficient credits")
        print("4. Run: pip install -r requirements.txt")
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
