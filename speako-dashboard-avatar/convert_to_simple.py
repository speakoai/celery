#!/usr/bin/env python3
"""
Script to convert the detailed avatar catalog to a simplified format.
This script reads the full avatar_catalog.json and creates a simplified version
with only essential data: id, url, and consolidated tags.
"""

import json
import os
from datetime import datetime

def convert_to_simple_format():
    """Convert detailed catalog to simplified format."""
    
    # Load the full catalog
    full_catalog_path = "avatar_catalog.json"
    simple_catalog_path = "avatar_catalog_simple.json"
    
    if not os.path.exists(full_catalog_path):
        print(f"Error: {full_catalog_path} not found!")
        return
    
    with open(full_catalog_path, 'r', encoding='utf-8') as f:
        full_catalog = json.load(f)
    
    # Create simplified structure
    simple_catalog = {
        "metadata": {
            "total": full_catalog.get("metadata", {}).get("total_files", 0),
            "last_updated": full_catalog.get("metadata", {}).get("last_updated", datetime.now().isoformat()),
            "domain": full_catalog.get("metadata", {}).get("custom_domain", "https://assets.speako.ai")
        },
        "avatars": []
    }
    
    # Process each avatar
    for avatar in full_catalog.get("avatars", []):
        # Skip incomplete entries
        if not avatar.get("id") or not avatar.get("public_url"):
            continue
            
        # Extract analysis data
        analysis = avatar.get("analysis", {})
        
        # Consolidate all characteristics into tags
        tags = []
        
        # Add basic attributes
        if analysis.get("gender"):
            tags.append(analysis["gender"])
        if analysis.get("age_group"):
            tags.append(analysis["age_group"])
        if analysis.get("race"):
            tags.append(analysis["race"])
        if analysis.get("occupation"):
            tags.append(analysis["occupation"])
        if analysis.get("style"):
            tags.append(analysis["style"])
        if analysis.get("expression"):
            tags.append(analysis["expression"])
        
        # Add outfit items
        outfit = analysis.get("outfit", [])
        if outfit:
            tags.extend(outfit)
        
        # Add hair description
        if analysis.get("hair"):
            hair_parts = analysis["hair"].split(", ")
            tags.extend(hair_parts)
        
        # Add existing tags
        existing_tags = analysis.get("tags", [])
        if existing_tags:
            tags.extend(existing_tags)
        
        # Remove duplicates and empty strings, maintain order
        seen = set()
        unique_tags = []
        for tag in tags:
            if tag and tag.strip() and tag.lower() not in seen:
                unique_tags.append(tag.strip())
                seen.add(tag.lower())
        
        # Create simplified avatar entry
        simple_avatar = {
            "id": avatar["id"],
            "url": avatar["public_url"],
            "tags": unique_tags
        }
        
        simple_catalog["avatars"].append(simple_avatar)
    
    # Update metadata with actual count
    simple_catalog["metadata"]["total"] = len(simple_catalog["avatars"])
    
    # Save simplified catalog
    with open(simple_catalog_path, 'w', encoding='utf-8') as f:
        json.dump(simple_catalog, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Simplified catalog created: {simple_catalog_path}")
    print(f"📊 Total avatars: {len(simple_catalog['avatars'])}")
    print(f"💾 File size reduced significantly")
    
    # Show sample of first few avatars
    print("\n📋 Sample avatars:")
    for i, avatar in enumerate(simple_catalog["avatars"][:3]):
        print(f"  {i+1}. {avatar['id']}: {len(avatar['tags'])} tags")
        print(f"     Tags: {', '.join(avatar['tags'][:5])}{'...' if len(avatar['tags']) > 5 else ''}")

if __name__ == "__main__":
    convert_to_simple_format()
