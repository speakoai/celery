import requests
import os
import tempfile

def test_booking_page_upload():
    """Test the /booking_page route with a sample file upload"""
    
    # Create a temporary test file
    test_content = "This is a test file for the booking page upload functionality.\nTimestamp: " + str(os.times())
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as temp_file:
        temp_file.write(test_content)
        temp_file_path = temp_file.name
    
    try:
        # Test file upload
        url = "http://127.0.0.1:8000/booking_page"
        
        with open(temp_file_path, 'rb') as file:
            files = {'file': ('test_booking_upload.txt', file, 'text/plain')}
            
            print("🔄 Testing file upload to /booking_page...")
            response = requests.post(url, files=files)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    print("✅ Upload successful!")
                    print(f"📁 Original filename: {data.get('original_filename')}")
                    print(f"🔗 File URL: {data.get('file_url')}")
                    print(f"📦 File size: {data.get('file_size')} bytes")
                    print(f"📄 Content type: {data.get('content_type')}")
                    print(f"🆔 File key: {data.get('file_key')}")
                    return True
                else:
                    print(f"❌ Upload failed: {data.get('error', 'Unknown error')}")
                    return False
            else:
                print(f"❌ HTTP Error: {response.status_code}")
                print(f"Response: {response.text}")
                return False
                
    except requests.exceptions.ConnectionError:
        print("❌ Could not connect to the Flask server. Make sure it's running on port 8000.")
        return False
    except Exception as e:
        print(f"❌ Test failed with error: {str(e)}")
        return False
    finally:
        # Clean up temporary file
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)

def test_booking_page_get():
    """Test the GET request to /booking_page (should return HTML)"""
    try:
        url = "http://127.0.0.1:8000/booking_page"
        response = requests.get(url)
        
        if response.status_code == 200:
            if "Booking Page" in response.text:
                print("✅ GET /booking_page returns the correct HTML page")
                return True
            else:
                print("❌ GET /booking_page returned unexpected content")
                return False
        else:
            print(f"❌ GET /booking_page failed with status {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ GET test failed: {str(e)}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("Testing /booking_page route functionality")
    print("=" * 60)
    
    # Test GET request first
    print("\n1. Testing GET request...")
    get_success = test_booking_page_get()
    
    # Test POST (file upload)
    print("\n2. Testing file upload...")
    upload_success = test_booking_page_upload()
    
    print("\n" + "=" * 60)
    if get_success and upload_success:
        print("🎉 All tests passed! The /booking_page route is working correctly.")
    else:
        print("⚠️  Some tests failed. Please check the output above.")
    print("=" * 60)
