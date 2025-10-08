#!/usr/bin/env python3
"""
Quick preview of avatar files in R2 bucket.
"""

import os
import boto3
from dotenv import load_dotenv

load_dotenv()

def preview_avatar_files():
    """Preview files in the avatar folder."""
    print("🔍 Previewing Avatar Files in R2 Bucket")
    print("=" * 50)
    
    # Setup R2 client
    r2_client = boto3.client(
        's3',
        endpoint_url=os.getenv("R2_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
        region_name='auto'
    )
    
    bucket_name = os.getenv("R2_BUCKET_NAME")
    folder_path = "staff-profiles/avatar/"
    
    try:
        response = r2_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=folder_path
        )
        
        if 'Contents' in response:
            files = [obj for obj in response['Contents'] if obj['Key'] != folder_path]
            
            print(f"📁 Bucket: {bucket_name}")
            print(f"📂 Folder: {folder_path}")
            print(f"📊 Total Files: {len(files)}")
            print("\n📋 File List:")
            
            total_size = 0
            for i, obj in enumerate(files, 1):
                filename = os.path.basename(obj['Key'])
                size_kb = obj['Size'] / 1024
                total_size += obj['Size']
                
                print(f"  {i:2d}. {filename}")
                print(f"      Size: {size_kb:.1f} KB")
                print(f"      Modified: {obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')}")
                print()
            
            print(f"💾 Total Size: {total_size / 1024:.1f} KB")
            
            # Generate sample public URLs
            print("\n🌐 Sample Public URLs:")
            base_url = os.getenv("R2_ENDPOINT_URL").replace('https://', '').replace('.r2.cloudflarestorage.com', '')
            for i, obj in enumerate(files[:3]):
                public_url = f"https://{base_url}.r2.cloudflarestorage.com/{bucket_name}/{obj['Key']}"
                print(f"  {i+1}. {public_url}")
            
        else:
            print(f"❌ No files found in {folder_path}")
            
    except Exception as e:
        print(f"❌ Error accessing bucket: {str(e)}")

if __name__ == "__main__":
    preview_avatar_files()
