#!/usr/bin/env python3
"""
Quick test to verify custom domain URLs are working.
"""

import requests
import json

def test_avatar_urls():
    """Test a few avatar URLs to make sure they're accessible."""
    print("🔍 Testing Custom Domain Avatar URLs")
    print("=" * 50)
    
    try:
        # Load the filelist to get some URLs to test
        with open("avatar_filelist.json", 'r') as f:
            data = json.load(f)
        
        # Test first 3 URLs
        test_urls = [file_item['url'] for file_item in data['files'][:3]]
        
        for i, url in enumerate(test_urls, 1):
            print(f"\n🔗 Testing URL {i}:")
            print(f"   {url}")
            
            try:
                response = requests.head(url, timeout=10)
                if response.status_code == 200:
                    print(f"   ✅ Success! (Status: {response.status_code})")
                    if 'content-length' in response.headers:
                        size_kb = int(response.headers['content-length']) / 1024
                        print(f"   📏 Size: {size_kb:.1f} KB")
                    if 'content-type' in response.headers:
                        print(f"   📄 Type: {response.headers['content-type']}")
                else:
                    print(f"   ⚠️  Unexpected status: {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                print(f"   ❌ Error: {str(e)}")
        
        print(f"\n🎉 URL testing complete!")
        print(f"💡 All your avatars should now be accessible via: https://assets.speako.ai/")
        
    except Exception as e:
        print(f"❌ Error loading filelist: {str(e)}")

if __name__ == "__main__":
    test_avatar_urls()
