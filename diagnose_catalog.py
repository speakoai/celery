#!/usr/bin/env python3
"""Diagnostic script to see what the app is actually loading."""

import os
import json

def diagnose_catalog_loading():
    """Check what catalog file would be loaded."""
    print("Avatar Catalog Loading Diagnosis")
    print("=" * 50)
    
    # Check current directory
    current_dir = os.getcwd()
    print(f"Current directory: {current_dir}")
    
    # Check the paths the app would look for
    simple_path = "speako-dashboard-avatar/avatar_catalog_simple.json"
    old_path = "speako-dashboard-avatar/avatar_catalog.json"
    
    print(f"\nChecking file paths:")
    print(f"Simple catalog: {simple_path}")
    print(f"  - Exists: {os.path.exists(simple_path)}")
    print(f"  - Full path: {os.path.abspath(simple_path)}")
    
    print(f"Old catalog: {old_path}")
    print(f"  - Exists: {os.path.exists(old_path)}")
    print(f"  - Full path: {os.path.abspath(old_path)}")
    
    # Check file sizes and modification times
    if os.path.exists(simple_path):
        stat = os.stat(simple_path)
        print(f"  - Size: {stat.st_size} bytes")
        print(f"  - Modified: {stat.st_mtime}")
        
        # Check content briefly
        with open(simple_path, 'r') as f:
            data = json.load(f)
            avatars = data.get('avatars', [])
            if avatars:
                first_avatar = avatars[0]
                print(f"  - First avatar keys: {list(first_avatar.keys())}")
    
    if os.path.exists(old_path):
        stat = os.stat(old_path)
        print(f"  - Size: {stat.st_size} bytes")
        print(f"  - Modified: {stat.st_mtime}")
        
        # Check content briefly
        with open(old_path, 'r') as f:
            data = json.load(f)
            avatars = data.get('avatars', [])
            if avatars:
                first_avatar = avatars[0]
                print(f"  - First avatar keys: {list(first_avatar.keys())}")
    
    # Check for any environment variables that might affect file loading
    print(f"\nEnvironment variables:")
    relevant_vars = [k for k in os.environ.keys() if 'avatar' in k.lower() or 'catalog' in k.lower()]
    if relevant_vars:
        for var in relevant_vars:
            print(f"  - {var}: {os.environ[var]}")
    else:
        print("  - No avatar/catalog related environment variables found")

if __name__ == "__main__":
    diagnose_catalog_loading()
