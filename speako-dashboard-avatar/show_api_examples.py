#!/usr/bin/env python3
"""
Show various API request examples and their outputs
"""

import json
import os

def load_catalog():
    """Load the simplified catalog."""
    catalog_path = "avatar_catalog_simple.json"
    if os.path.exists(catalog_path):
        with open(catalog_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"avatars": [], "metadata": {}}

def simulate_search(filters, page=1, per_page=5):
    """Simulate search functionality."""
    catalog = load_catalog()
    avatars = catalog.get("avatars", [])
    filtered_avatars = []
    
    for avatar in avatars:
        tags = avatar.get("tags", [])
        tags_lower = [tag.lower() for tag in tags]
        match = True
        
        # Apply filters
        for filter_key, filter_value in filters.items():
            if filter_key == "search":
                search_term = filter_value.lower()
                if not any(search_term in tag for tag in tags_lower):
                    match = False
                    break
            else:
                if filter_value.lower() not in tags_lower:
                    match = False
                    break
        
        if match:
            filtered_avatars.append(avatar)
    
    # Apply pagination
    total = len(filtered_avatars)
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    page_data = filtered_avatars[start_idx:end_idx]
    
    return {
        "success": True,
        "data": page_data,
        "total": total,
        "filters_applied": filters,
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total,
            "pages": (total + per_page - 1) // per_page,
            "paginated": True,
            "has_next": page * per_page < total,
            "has_prev": page > 1
        }
    }

def print_example(title, request_url, response):
    """Print a formatted example."""
    print(f"\n{'='*60}")
    print(f"🔗 {title}")
    print(f"📡 Request: {request_url}")
    print(f"📊 Results: {response['total']} total, showing {len(response['data'])}")
    print(f"📝 Response:")
    print(json.dumps(response, indent=2)[:800] + "..." if len(json.dumps(response)) > 800 else json.dumps(response, indent=2))

if __name__ == "__main__":
    print("🚀 Avatar API - Request/Response Examples")
    
    # Example 1: Basic pagination
    catalog = load_catalog()
    avatars = catalog.get("avatars", [])[:3]
    basic_response = {
        "success": True,
        "data": avatars,
        "metadata": catalog.get("metadata", {}),
        "total": len(catalog.get("avatars", [])),
        "pagination": {
            "page": 1,
            "per_page": 3,
            "total": len(catalog.get("avatars", [])),
            "pages": 37,
            "paginated": True,
            "has_next": True,
            "has_prev": False
        }
    }
    print_example(
        "Basic Request (Default Pagination)",
        "GET /api/avatars?page=1&per_page=3",
        basic_response
    )
    
    # Example 2: Search by gender
    gender_response = simulate_search({"gender": "female"}, page=1, per_page=3)
    print_example(
        "Filter by Gender",
        "GET /api/avatars?gender=female&per_page=3",
        gender_response
    )
    
    # Example 3: Search by occupation
    occupation_response = simulate_search({"occupation": "businessman"}, page=1, per_page=3)
    print_example(
        "Filter by Occupation",
        "GET /api/avatars?occupation=businessman&per_page=3",
        occupation_response
    )
    
    # Example 4: Text search
    search_response = simulate_search({"search": "cartoon"}, page=1, per_page=3)
    print_example(
        "Text Search",
        "GET /api/avatars?search=cartoon&per_page=3",
        search_response
    )
    
    # Example 5: Single avatar
    single_avatar = avatars[0] if avatars else {}
    single_response = {
        "success": True,
        "data": single_avatar
    }
    print_example(
        "Get Single Avatar",
        f"GET /api/avatars/{single_avatar.get('id', 'avatar_001')}",
        single_response
    )
    
    print(f"\n{'='*60}")
    print("✅ All examples show the simplified format:")
    print("   • Only 3 fields per avatar: id, url, tags")
    print("   • Tags contain all analysis data consolidated")
    print("   • Dramatically reduced JSON size")
    print("   • Perfect for public API consumption")
