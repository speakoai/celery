#!/usr/bin/env python3
"""
Test script to show the final API output format
"""

import json
import os

# Simulate the API response
def get_sample_api_response():
    """Get sample API response in the new simplified format."""
    
    # Load simplified catalog
    catalog_path = "avatar_catalog_simple.json"
    if os.path.exists(catalog_path):
        with open(catalog_path, 'r', encoding='utf-8') as f:
            catalog = json.load(f)
    else:
        return {"error": "Catalog not found"}
    
    # Simulate pagination (first page, 3 items for demo)
    avatars = catalog.get("avatars", [])
    page_data = avatars[:3]  # First 3 items
    
    # API response format
    response = {
        "success": True,
        "data": page_data,
        "metadata": catalog.get("metadata", {}),
        "total": len(avatars),
        "pagination": {
            "page": 1,
            "per_page": 3,
            "total": len(avatars),
            "pages": (len(avatars) + 2) // 3,
            "paginated": True,
            "has_next": len(avatars) > 3,
            "has_prev": False,
            "next_page": 2 if len(avatars) > 3 else None,
            "prev_page": None,
            "start_index": 1,
            "end_index": min(3, len(avatars))
        }
    }
    
    return response

if __name__ == "__main__":
    print("🚀 Avatar API - Simplified Format")
    print("=" * 50)
    
    response = get_sample_api_response()
    print(json.dumps(response, indent=2, ensure_ascii=False))
    
    # Show size comparison
    if "data" in response:
        data_size = len(json.dumps(response["data"]))
        print(f"\n📏 Data size for 3 avatars: {data_size:,} characters")
        print(f"💾 Average per avatar: {data_size // len(response['data']):,} characters")
        
        # Show what each avatar contains
        print(f"\n📋 Each avatar contains:")
        avatar = response["data"][0]
        print(f"   • ID: {avatar['id']}")
        print(f"   • URL: {avatar['url']}")
        print(f"   • Tags: {len(avatar['tags'])} items ({', '.join(avatar['tags'][:3])}...)")
