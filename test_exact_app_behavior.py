#!/usr/bin/env python3
"""Test that exactly mimics the Flask app's catalog loading logic."""

import os
import json

class AvatarAPI:
    """Exact copy of the AvatarAPI class from app.py."""
    
    def __init__(self, catalog_file: str = "speako-dashboard-avatar/avatar_catalog_simple.json"):
        """Initialize with catalog file path."""
        self.catalog_file = catalog_file
        self.catalog = self.load_catalog()
    
    def load_catalog(self):
        """Load avatar catalog from JSON file."""
        try:
            if os.path.exists(self.catalog_file):
                with open(self.catalog_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                return {"avatars": [], "metadata": {"total": 0}}
        except Exception as e:
            print(f"Error loading avatar catalog: {str(e)}")
            return {"avatars": [], "metadata": {"total": 0}}
    
    def get_all_avatars(self):
        """Get all avatars with metadata."""
        return {
            "success": True,
            "data": self.catalog.get("avatars", []),
            "metadata": self.catalog.get("metadata", {}),
            "total": len(self.catalog.get("avatars", []))
        }

def test_exact_app_behavior():
    """Test the exact same behavior as the Flask app."""
    print("Testing Exact Flask App Behavior")
    print("=" * 50)
    
    # Initialize exactly like the Flask app does
    avatar_api = AvatarAPI()
    
    print(f"Catalog file path: {avatar_api.catalog_file}")
    print(f"File exists: {os.path.exists(avatar_api.catalog_file)}")
    
    # Get all avatars like the API endpoint would
    result = avatar_api.get_all_avatars()
    
    print(f"API Success: {result.get('success')}")
    print(f"Total avatars: {result.get('total')}")
    print(f"Metadata keys: {list(result.get('metadata', {}).keys())}")
    
    # Check the structure of the first avatar
    avatars = result.get('data', [])
    if avatars:
        first_avatar = avatars[0]
        print(f"\nFirst avatar structure:")
        print(f"Keys: {list(first_avatar.keys())}")
        print(f"ID: {first_avatar.get('id')}")
        print(f"URL length: {len(first_avatar.get('url', ''))}")
        print(f"Tags count: {len(first_avatar.get('tags', []))}")
        print(f"Tags sample: {first_avatar.get('tags', [])[:3]}")
        
        # Check if it's old or new format
        if len(first_avatar.keys()) == 3 and set(first_avatar.keys()) == {'id', 'url', 'tags'}:
            print("✅ CONFIRMED: Flask app would serve NEW simplified format!")
        else:
            print("❌ PROBLEM: Flask app would serve OLD complex format!")
            print(f"   Unexpected keys: {[k for k in first_avatar.keys() if k not in ['id', 'url', 'tags']]}")
    
    # Test a search operation too
    print(f"\nTesting search functionality:")
    
    # Simulate a search for female avatars
    female_avatars = []
    for avatar in avatars:
        tags = avatar.get("tags", [])
        tags_lower = [tag.lower() for tag in tags]
        if "female" in tags_lower:
            female_avatars.append(avatar)
    
    print(f"Found {len(female_avatars)} female avatars")
    if female_avatars:
        print(f"Sample female avatar tags: {female_avatars[0].get('tags', [])[:5]}")

if __name__ == "__main__":
    test_exact_app_behavior()
