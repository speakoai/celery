import boto3
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_r2_connectivity():
    """Test basic connectivity to Cloudflare R2 bucket"""
    
    # Get R2 credentials from environment
    access_key = os.getenv("R2_ACCESS_KEY_ID")
    secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
    endpoint_url = os.getenv("R2_ENDPOINT_URL")
    bucket_name = os.getenv("R2_BUCKET_NAME")
    account_id = os.getenv("R2_ACCOUNT_ID")
    
    print("R2 Configuration:")
    print(f"Access Key ID: {access_key[:8]}..." if access_key else "Access Key ID: Not found")
    print(f"Endpoint URL: {endpoint_url}")
    print(f"Bucket Name: {bucket_name}")
    print(f"Account ID: {account_id}")
    print("-" * 50)
    
    if not all([access_key, secret_key, endpoint_url, bucket_name]):
        print("❌ Missing required R2 configuration variables")
        return False
    
    try:
        # Create S3 client for R2
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='auto'  # R2 uses 'auto' as region
        )
        
        print("🔄 Testing R2 connectivity...")
        
        # Test 1: Try to access our specific bucket (skip listing all buckets)
        print("✅ R2 client created successfully")
        
        # Test 2: Check if our specific bucket exists and is accessible
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"✅ Bucket '{bucket_name}' is accessible")
        except Exception as e:
            print(f"❌ Cannot access bucket '{bucket_name}': {str(e)}")
            return False
        
        # Test 3: List objects in bucket (just to verify read permissions)
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=5)
            object_count = response.get('KeyCount', 0)
            print(f"✅ Successfully listed objects in bucket (found {object_count} objects)")
        except Exception as e:
            print(f"⚠️  Could not list objects in bucket: {str(e)}")
        
        # Test 4: Try to upload a small test file
        try:
            test_content = "Hello from R2 connectivity test!"
            test_key = "test/connectivity_test.txt"
            
            s3_client.put_object(
                Bucket=bucket_name,
                Key=test_key,
                Body=test_content.encode('utf-8'),
                ContentType='text/plain'
            )
            print(f"✅ Successfully uploaded test file: {test_key}")
            
            # Clean up test file
            s3_client.delete_object(Bucket=bucket_name, Key=test_key)
            print("✅ Successfully deleted test file")
            
        except Exception as e:
            print(f"❌ Upload test failed: {str(e)}")
            return False
        
        print("\n🎉 All R2 connectivity tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Failed to create R2 client: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_r2_connectivity()
    if success:
        print("\n✅ R2 is ready for file uploads!")
    else:
        print("\n❌ R2 connectivity issues detected. Please check your configuration.")
