#!/usr/bin/env python3
"""Simple test to check the avatar catalog format."""

import json
import os

def test_catalog_format():
    """Test the avatar catalog format directly."""
    catalog_path = "speako-dashboard-avatar/avatar_catalog_simple.json"
    
    print("Testing Avatar Catalog Format")
    print("=" * 50)
    print(f"Looking for catalog at: {catalog_path}")
    print(f"Current directory: {os.getcwd()}")
    print(f"File exists: {os.path.exists(catalog_path)}")
    
    if not os.path.exists(catalog_path):
        print("❌ Catalog file not found!")
        return
    
    try:
        with open(catalog_path, 'r', encoding='utf-8') as f:
            catalog = json.load(f)
        
        print(f"\n✅ Successfully loaded catalog")
        print(f"Total avatars: {catalog.get('metadata', {}).get('total', 0)}")
        
        avatars = catalog.get('avatars', [])
        if avatars:
            first_avatar = avatars[0]
            print(f"\nFirst avatar structure:")
            print(f"Keys: {list(first_avatar.keys())}")
            print(f"ID: {first_avatar.get('id')}")
            print(f"URL: {first_avatar.get('url')}")
            print(f"Tags count: {len(first_avatar.get('tags', []))}")
            print(f"Tags sample: {first_avatar.get('tags', [])[:5]}")
            
            # Check if it's using old format
            old_format_keys = ['analysis', 'physical_description', 'clothing', 'expression', 'hair']
            has_old_format = any(key in first_avatar for key in old_format_keys)
            
            if has_old_format:
                print("❌ PROBLEM: Still using old complex format!")
                print(f"Found these old format keys: {[k for k in old_format_keys if k in first_avatar]}")
            else:
                print("✅ GOOD: Using new simplified format!")
        
        # Check a few more avatars
        print(f"\nChecking structure consistency across avatars...")
        consistent = True
        for i, avatar in enumerate(avatars[:5]):  # Check first 5
            expected_keys = {'id', 'url', 'tags'}
            actual_keys = set(avatar.keys())
            if actual_keys != expected_keys:
                print(f"❌ Avatar {i+1} has inconsistent structure: {actual_keys}")
                consistent = False
        
        if consistent:
            print("✅ All checked avatars have consistent simplified structure")
            
    except Exception as e:
        print(f"❌ Error loading catalog: {e}")

if __name__ == "__main__":
    test_catalog_format()
