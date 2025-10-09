#!/usr/bin/env python3
"""Test script to check the current avatar API response format."""

import sys
import os
sys.path.append('.')

from app import avatar_api

def test_api_response():
    """Test what the API is actually returning."""
    print("Testing Avatar API Response Format")
    print("=" * 50)
    
    # Test get_all_avatars
    print("\n1. Testing get_all_avatars():")
    result = avatar_api.get_all_avatars()
    
    print(f"Success: {result.get('success')}")
    print(f"Total avatars: {result.get('total')}")
    print(f"Metadata keys: {list(result.get('metadata', {}).keys())}")
    
    # Check first avatar structure
    avatars = result.get('data', [])
    if avatars:
        first_avatar = avatars[0]
        print(f"\nFirst avatar structure:")
        print(f"Keys: {list(first_avatar.keys())}")
        print(f"ID: {first_avatar.get('id')}")
        print(f"URL: {first_avatar.get('url')}")
        print(f"Tags (first 5): {first_avatar.get('tags', [])[:5]}")
        
        # Check if it's using old format (would have detailed analysis fields)
        if 'analysis' in first_avatar or 'physical_description' in first_avatar:
            print("❌ PROBLEM: Still using old complex format!")
        else:
            print("✅ GOOD: Using new simplified format!")
    
    # Test search function
    print("\n2. Testing search_avatars():")
    search_result = avatar_api.search_avatars({"gender": "female"})
    print(f"Female avatars found: {search_result.get('total')}")
    
    if search_result.get('data'):
        sample_female = search_result['data'][0]
        print(f"Sample female avatar tags: {sample_female.get('tags', [])[:5]}")

if __name__ == "__main__":
    test_api_response()
