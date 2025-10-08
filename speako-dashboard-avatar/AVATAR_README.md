# Avatar Scanner & API

A comprehensive solution for scanning, analyzing, and cataloging avatar images stored in Cloudflare R2 storage using OpenAI's vision capabilities.

## Features

- 🔍 **Automatic Scanning**: Scans specified folders in Cloudflare R2 storage
- 🤖 **AI Analysis**: Uses OpenAI GPT-4 Vision to analyze avatar characteristics
- 🏷️ **Smart Tagging**: Extracts occupation, race, gender, outfit, style, and more
- 📝 **Intelligent Renaming**: Renames files based on AI analysis for better organization
- 🌐 **Web API**: Provides REST API endpoints for accessing avatar catalog
- 📊 **Statistics**: Generates comprehensive statistics and summaries
- 🔍 **Search & Filter**: Advanced filtering by multiple criteria

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Update your `.env` file with the required credentials:

```env
# Cloudflare R2 Configuration (Already configured)
R2_ACCESS_KEY_ID=your_r2_access_key
R2_SECRET_ACCESS_KEY=your_r2_secret_key
R2_ENDPOINT_URL=your_r2_endpoint
R2_BUCKET_NAME=your_bucket_name
R2_ACCOUNT_ID=your_account_id

# OpenAI Configuration (Add your API key)
OPENAI_API_KEY=your_openai_api_key_here
```

### 3. Test Setup

Run the test script to verify everything is configured correctly:

```bash
python test_avatar_setup.py
```

This will check:
- Environment variables
- R2 connectivity
- OpenAI API access
- Required dependencies

## Usage

### Scan and Analyze Avatars

Run the main scanner script:

```bash
python avatar_scanner.py
```

This will:
1. Scan the `staff-profiles/avatar/` folder in your R2 bucket
2. Download each image and analyze it with OpenAI
3. Rename files based on analysis (e.g., `male_caucasian_doctor_glasses.png`)
4. Generate a comprehensive JSON catalog
5. Create summary statistics

### Output Files

The scanner generates two main files:

#### `avatar_catalog.json`
Complete catalog with detailed information for each avatar:

```json
{
  "metadata": {
    "generated_at": "2024-01-15T10:30:00",
    "total_files": 25,
    "processed": 24,
    "errors": 1,
    "folder_scanned": "staff-profiles/avatar/",
    "bucket": "speako-public-assets"
  },
  "avatars": [
    {
      "id": "avatar_001",
      "original_filename": "avatar1.png",
      "current_filename": "male_caucasian_doctor_glasses.png",
      "file_key": "staff-profiles/avatar/male_caucasian_doctor_glasses.png",
      "public_url": "https://189f2871b19565e4c130bc247237643c.r2.cloudflarestorage.com/speako-public-assets/staff-profiles/avatar/male_caucasian_doctor_glasses.png",
      "file_size": 156789,
      "last_modified": "2024-01-15T10:25:30",
      "analysis": {
        "occupation": "doctor",
        "race": "caucasian",
        "gender": "male",
        "age_group": "middle-aged",
        "outfit": ["glasses", "white coat"],
        "style": "professional",
        "expression": "friendly",
        "hair": "short brown",
        "pose": "front-facing",
        "background": "white",
        "confidence_score": 0.95,
        "tags": ["medical", "professional", "glasses", "friendly"]
      },
      "processed_at": "2024-01-15T10:30:15"
    }
  ]
}
```

#### `avatar_summary.json`
Statistical summary with counts and samples:

```json
{
  "total_avatars": 24,
  "processing_date": "2024-01-15T10:30:00",
  "occupations": {
    "doctor": 5,
    "engineer": 4,
    "teacher": 3,
    "businessman": 3
  },
  "races": {
    "caucasian": 8,
    "asian": 6,
    "black": 5,
    "hispanic": 3
  },
  "genders": {
    "male": 12,
    "female": 12
  },
  "styles": {
    "professional": 15,
    "casual": 6,
    "cartoon": 3
  }
}
```

## Web API

### Setup API Routes

Add avatar API routes to your existing Flask app:

```python
from avatar_api import add_avatar_routes_to_app

# In your app.py
avatar_api = add_avatar_routes_to_app(app)
```

### API Endpoints

#### Get All Avatars
```
GET /api/avatars
```

Query parameters:
- `gender`: Filter by gender (male, female, non-binary)
- `race`: Filter by race (caucasian, black, asian, hispanic, indian, etc.)
- `occupation`: Filter by occupation (doctor, engineer, teacher, etc.)
- `style`: Filter by style (professional, casual, cartoon, etc.)
- `age_group`: Filter by age group (young, middle-aged, senior)
- `tags`: Filter by tags (comma-separated)
- `outfit`: Filter by outfit items (comma-separated)
- `search`: Text search across all fields
- `page`: Page number (default: 1)
- `per_page`: Items per page (default: 20, max: 100)

Examples:
```bash
# Get all avatars
curl "http://localhost:5000/api/avatars"

# Get male doctors
curl "http://localhost:5000/api/avatars?gender=male&occupation=doctor"

# Search for avatars with glasses
curl "http://localhost:5000/api/avatars?outfit=glasses"

# Text search
curl "http://localhost:5000/api/avatars?search=professional"

# Pagination
curl "http://localhost:5000/api/avatars?page=2&per_page=10"
```

#### Get Specific Avatar
```
GET /api/avatars/<avatar_id>
```

Example:
```bash
curl "http://localhost:5000/api/avatars/avatar_001"
```

#### Get Statistics
```
GET /api/avatars/stats
```

Returns comprehensive statistics about all avatars.

#### Refresh Catalog
```
POST /api/avatars/refresh
```

Reloads the avatar catalog from the JSON file.

#### Health Check
```
GET /api/avatars/health
```

Returns API health status and basic information.

### API Response Format

All API responses follow this format:

```json
{
  "success": true,
  "data": [...],
  "total": 24,
  "pagination": {
    "page": 1,
    "per_page": 20,
    "total": 24,
    "pages": 2
  }
}
```

## Configuration

### Folder Structure
The scanner is configured to scan: `staff-profiles/avatar/`

To change the target folder, modify the `target_folder` variable in `avatar_scanner.py`:

```python
self.target_folder = "your-custom-folder/"
```

### OpenAI Analysis Prompt
The AI analysis can be customized by modifying the prompt in the `analyze_avatar_with_openai` method.

### File Naming
File naming logic can be customized in the `generate_new_filename` method.

## Troubleshooting

### Common Issues

1. **R2 Connection Failed**
   - Verify your R2 credentials in `.env`
   - Check bucket name and permissions
   - Ensure endpoint URL is correct

2. **OpenAI API Errors**
   - Verify API key is valid
   - Check account has sufficient credits
   - Ensure gpt-4o model access

3. **No Files Found**
   - Check folder path exists in your bucket
   - Verify files are actually images
   - Check file permissions

4. **Import Errors**
   - Run `pip install -r requirements.txt`
   - Activate your virtual environment

### Logs

The scanner creates detailed logs in `avatar_scanner.log` for debugging.

### Test Script

Always run the test script first:
```bash
python test_avatar_setup.py
```

## Security Considerations

1. **API Keys**: Never commit API keys to version control
2. **Rate Limiting**: Consider implementing rate limiting for the API
3. **Access Control**: Add authentication to API endpoints in production
4. **CORS**: Configure CORS settings appropriately

## Performance

- Processing speed depends on image sizes and OpenAI API response times
- Typical processing: 5-10 seconds per image
- Large batches may take considerable time
- Consider implementing progress tracking for large sets

## License

This project is part of the Speako AI system. Please refer to your organization's licensing terms.
