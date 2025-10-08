#!/usr/bin/env python3
"""
URL Updater for Avatar JSON Files

Updates all Cloudflare R2 URLs to use custom domain mapping.
"""

import json
import os
from datetime import datetime

def update_urls_in_file(file_path, old_base_url, new_base_url):
    """
    Update URLs in a JSON file.
    
    Args:
        file_path: Path to the JSON file
        old_base_url: Old URL base to replace
        new_base_url: New URL base to use
    """
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return False
    
    try:
        # Read the JSON file
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Count updates
        update_count = 0
        
        # Update URLs based on file structure
        if 'avatars' in data:
            # This is avatar_catalog.json or similar
            for avatar in data['avatars']:
                if 'public_url' in avatar and old_base_url in avatar['public_url']:
                    avatar['public_url'] = avatar['public_url'].replace(old_base_url, new_base_url)
                    update_count += 1
            
            # Update metadata timestamp
            if 'metadata' in data:
                data['metadata']['url_updated'] = datetime.now().isoformat()
                data['metadata']['custom_domain'] = new_base_url
        
        elif 'files' in data:
            # This is avatar_filelist.json
            for file_item in data['files']:
                if 'url' in file_item and old_base_url in file_item['url']:
                    file_item['url'] = file_item['url'].replace(old_base_url, new_base_url)
                    update_count += 1
            
            # Update metadata
            data['url_updated'] = datetime.now().isoformat()
            data['custom_domain'] = new_base_url
        
        elif 'sample_avatars' in data:
            # This is avatar_summary.json
            for avatar in data['sample_avatars']:
                if 'public_url' in avatar and old_base_url in avatar['public_url']:
                    avatar['public_url'] = avatar['public_url'].replace(old_base_url, new_base_url)
                    update_count += 1
            
            # Update metadata
            data['url_updated'] = datetime.now().isoformat()
            data['custom_domain'] = new_base_url
        
        # Write the updated JSON back
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"✅ Updated {update_count} URLs in {os.path.basename(file_path)}")
        return True
        
    except Exception as e:
        print(f"❌ Error updating {file_path}: {str(e)}")
        return False

def main():
    """Main function to update all avatar JSON files."""
    print("🔄 Updating Avatar JSON Files with Custom Domain")
    print("=" * 60)
    
    # Define URL mapping
    old_base_url = "https://189f2871b19565e4c130bc247237643c.r2.cloudflarestorage.com/speako-public-assets"
    new_base_url = "https://assets.speako.ai"
    
    print(f"Old URL: {old_base_url}")
    print(f"New URL: {new_base_url}")
    print()
    
    # Files to update
    files_to_update = [
        "avatar_catalog.json",
        "avatar_filelist.json", 
        "avatar_summary.json"
    ]
    
    updated_files = 0
    total_files = len(files_to_update)
    
    for file_name in files_to_update:
        print(f"🔄 Processing {file_name}...")
        if update_urls_in_file(file_name, old_base_url, new_base_url):
            updated_files += 1
        print()
    
    print("=" * 60)
    print(f"📊 Summary: {updated_files}/{total_files} files updated successfully")
    
    if updated_files == total_files:
        print("🎉 All files updated with custom domain!")
        print(f"🌐 Your avatars are now accessible via: {new_base_url}/staff-profiles/avatar/")
        
        # Show some example URLs
        print("\n🔗 Example updated URLs:")
        try:
            with open("avatar_filelist.json", 'r') as f:
                data = json.load(f)
                for i, file_item in enumerate(data['files'][:3], 1):
                    print(f"  {i}. {file_item['url']}")
        except:
            pass
    else:
        print("⚠️  Some files could not be updated. Please check the errors above.")

if __name__ == "__main__":
    main()
