# API Usage Examples

## Environment Variables Required

Add these to your `.env` file and Render environment variables:

```bash
# Required for API authentication
API_SECRET_KEY=your-very-long-random-api-key-here-make-it-secure

# Optional: Restrict origins (comma-separated)
ALLOWED_ORIGINS=https://your-nextjs-app.render.com,http://localhost:3000

# Existing variables you already have
DATABASE_URL=your-postgres-url
REDIS_URL=your-redis-url
FLASK_SECRET_KEY=your-flask-secret

# For knowledge upload and analysis (must be set on BOTH web and worker services)
R2_ACCESS_KEY_ID=...
R2_SECRET_ACCESS_KEY=...
R2_ENDPOINT_URL=...
R2_BUCKET_NAME=...
R2_PUBLIC_BASE_URL=https://assets.speako.ai
OPENAI_API_KEY=...
OPENAI_KNOWLEDGE_MODEL=gpt-4o-mini
```

## API Endpoints

### 1. Generate Availability (Enhanced with business_type)
```bash
POST /api/availability/generate
Content-Type: application/json
X-API-Key: your-api-key-here

# For restaurant/venue availability
{
  "tenant_id": "123",
  "location_id": "456",
  "location_tz": "America/New_York",
  "business_type": "rest"
}

# For service/staff availability  
{
  "tenant_id": "123",
  "location_id": "456",
  "location_tz": "America/New_York",
  "business_type": "service"
}

# For regeneration (specific date)
{
  "tenant_id": "123",
  "location_id": "456", 
  "location_tz": "America/New_York",
  "business_type": "rest",
  "affected_date": "2025-08-15"
}
```

### 2. Generate Venue Availability
```bash
POST /api/availability/generate-venue
Content-Type: application/json
X-API-Key: your-api-key-here

{
  "tenant_id": "123",
  "location_id": "456",
  "location_tz": "America/New_York",
  "affected_date": "2025-08-15"
}
```

### 3. Check Task Status
```bash
GET /api/task/{task_id}
X-API-Key: your-api-key-here
```

### 4. Health Check
```bash
GET /api/health
# No authentication required
```

### 5. Upload Knowledge File (background analysis)
```bash
POST /api/knowledge/upload-knowledge-file
X-API-Key: your-api-key-here
Content-Type: multipart/form-data

Form fields:
- tenant_id: string (required)
- location_id: string (required)
- knowledge_type: one of [menu, faq, policy, events] (required)
- file: .doc/.docx/.xls/.xlsx/.pdf/.csv/.txt (<= 5MB)

# Example using curl
curl -X POST https://your-celery-app.render.com/api/knowledge/upload-knowledge-file \
  -H "X-API-Key: your-api-key-here" \
  -F tenant_id=123 \
  -F location_id=456 \
  -F knowledge_type=menu \
  -F file=@/path/to/menu.pdf

# Response (201):
# {
#   "success": true,
#   "message": "Knowledge file uploaded successfully",
#   "data": {
#     "tenant_id": "123",
#     "location_id": "456",
#     "knowledge_type": "menu",
#     "filename": "123_456_menu_ab12cd34.pdf",
#     "key": "knowledges/123/456/123_456_menu_ab12cd34.pdf",
#     "url": "https://assets.speako.ai/knowledges/123/456/123_456_menu_ab12cd34.pdf",
#     "size": 102400,
#     "content_type": "application/pdf",
#     "analysis": { "status": "queued", "mode": "background", "task_id": "<celery-task-id>" }
#   }
# }

# Poll the task status (until ready=true)
GET /api/task/<celery-task-id>

# When complete, task result includes analysis artifact location:
# {
#   "task_id": "...",
#   "status": "SUCCESS",
#   "ready": true,
#   "result": {
#     "success": true,
#     "file": { ... },
#     "analysis": { "status": "success|raw", "model": "gpt-4o-mini", "file_id": "file_..." },
#     "artifacts": { "analysis_key": "knowledges/123/456/analysis/123_456_menu_ab12cd34.json", "analysis_url": "https://assets.speako.ai/knowledges/123/456/analysis/123_456_menu_ab12cd34.json" },
#     "job": { "task_id": "...", "duration_ms": 12345 }
#   },
#   "success": true
# }
```

### Troubleshooting

- If task result shows `{"analysis": {"status": "skipped", "reason": "storage_not_configured"}}`:
  - Your Celery worker is missing one or more R2 env vars. Ensure `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT_URL`, and `R2_BUCKET_NAME` are set on the worker service (not just the web service).
  - The task result may include a `missing_env` array to indicate which variables are absent.
- If `OpenAI not configured` appears, set `OPENAI_API_KEY` (and optionally `OPENAI_KNOWLEDGE_MODEL`) on the worker service.

## Next.js Integration Example

```typescript
// lib/celery-api.ts
const CELERY_API_URL = process.env.CELERY_API_URL || 'https://your-celery-app.render.com';
const API_KEY = process.env.CELERY_API_KEY!;

interface GenerateAvailabilityRequest {
  tenant_id: string;
  location_id: string;
  location_tz: string;
  business_type: 'rest' | 'service'; // Required - determines which task to run
  affected_date?: string; // Optional for regeneration
}

interface TaskResponse {
  task_id: string;
  status: string;
  message: string;
  tenant_id: string;
  location_id: string;
  business_type: string;
  task_type: string; // 'venue' or 'staff'
  is_regeneration: boolean;
}

interface TaskStatus {
  task_id: string;
  status: 'PENDING' | 'SUCCESS' | 'FAILURE' | 'RETRY' | 'REVOKED';
  ready: boolean;
  result?: any;
  error?: string;
  success?: boolean;
  message?: string;
}

export class CeleryAPI {
  private async makeRequest(endpoint: string, options: RequestInit = {}) {
    const response = await fetch(`${CELERY_API_URL}${endpoint}`, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': API_KEY,
        ...options.headers,
      },
    });

    if (!response.ok) {
      const error = await response.json().catch(() => ({ error: 'Unknown error' }));
      throw new Error(error.error || `HTTP ${response.status}`);
    }

    return response.json();
  }

  async generateAvailability(data: GenerateAvailabilityRequest): Promise<TaskResponse> {
    return this.makeRequest('/api/availability/generate', {
      method: 'POST',
      body: JSON.stringify(data),
    });
  }

  // Legacy methods for backward compatibility
  async generateStaffAvailability(data: Omit<GenerateAvailabilityRequest, 'business_type'>): Promise<TaskResponse> {
    return this.generateAvailability({ ...data, business_type: 'service' });
  }

  async generateVenueAvailability(data: Omit<GenerateAvailabilityRequest, 'business_type'>): Promise<TaskResponse> {
    return this.generateAvailability({ ...data, business_type: 'rest' });
  }

  async getTaskStatus(taskId: string): Promise<TaskStatus> {
    return this.makeRequest(`/api/task/${taskId}`);
  }

  async waitForTask(taskId: string, maxWaitTime = 30000): Promise<TaskStatus> {
    const startTime = Date.now();
    
    while (Date.now() - startTime < maxWaitTime) {
      const status = await this.getTaskStatus(taskId);
      
      if (status.ready) {
        return status;
      }
      
      // Wait 1 second before checking again
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    throw new Error('Task timeout');
  }

  async healthCheck() {
    return this.makeRequest('/api/health');
  }
}

// Usage example in a React component
export default function AvailabilityManager() {
  const [isGenerating, setIsGenerating] = useState(false);
  const [taskStatus, setTaskStatus] = useState<string>('');

  const handleGenerateAvailability = async () => {
    setIsGenerating(true);
    const celeryAPI = new CeleryAPI();
    
    try {
      // Start the task (restaurant/venue availability)
      const taskResponse = await celeryAPI.generateAvailability({
        tenant_id: '123',
        location_id: '456',
        location_tz: 'America/New_York',
        business_type: 'rest' // or 'service' for staff
      });
      
      setTaskStatus(`Task started: ${taskResponse.task_id} (${taskResponse.task_type})`);
      
      // Wait for completion
      const finalStatus = await celeryAPI.waitForTask(taskResponse.task_id);
      
      if (finalStatus.success) {
        setTaskStatus('✅ Availability generated successfully!');
        console.log('Result:', finalStatus.result);
      } else {
        setTaskStatus(`❌ Task failed: ${finalStatus.error}`);
      }
      
    } catch (error) {
      setTaskStatus(`❌ Error: ${error.message}`);
    } finally {
      setIsGenerating(false);
    }
  };

  return (
    <div>
      <button 
        onClick={handleGenerateAvailability} 
        disabled={isGenerating}
      >
        {isGenerating ? 'Generating...' : 'Generate Availability'}
      </button>
      <p>{taskStatus}</p>
    </div>
  );
}
```

## cURL Examples

```bash
# 1. Generate availability (restaurant/venue)
curl -X POST https://your-celery-app.render.com/api/availability/generate \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key-here" \
  -d '{
    "tenant_id": "123",
    "location_id": "456",
    "location_tz": "America/New_York",
    "business_type": "rest"
  }'

# Response:
# {
#   "task_id": "abc-123-def",
#   "status": "pending",
#   "message": "Venue availability generation task started",
#   "tenant_id": "123",
#   "location_id": "456",
#   "business_type": "rest",
#   "task_type": "venue",
#   "is_regeneration": false
# }

# 1b. Generate availability (service/staff)
curl -X POST https://your-celery-app.render.com/api/availability/generate \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key-here" \
  -d '{
    "tenant_id": "123",
    "location_id": "456",
    "location_tz": "America/New_York",
    "business_type": "service"
  }'

# 2. Check task status
curl -X GET https://your-celery-app.render.com/api/task/abc-123-def \
  -H "X-API-Key: your-api-key-here"

# Response when complete:
# {
#   "task_id": "abc-123-def",
#   "status": "SUCCESS",
#   "ready": true,
#   "result": {"status": "success"},
#   "success": true
# }
```

## Error Responses

```json
// Missing API key
{
  "error": "API key required",
  "code": "MISSING_API_KEY"
}

// Invalid API key
{
  "error": "Unauthorized", 
  "code": "INVALID_API_KEY"
}

// Missing required fields
{
  "error": "Missing required fields",
  "missing_fields": ["business_type"]
}

// Invalid business_type
{
  "error": "Invalid business_type",
  "message": "business_type must be either \"rest\" or \"service\"",
  "provided": "invalid_value"
}

// Task failed
{
  "task_id": "abc-123",
  "status": "FAILURE",
  "ready": true,
  "error": "Database connection failed",
  "success": false
}
```
