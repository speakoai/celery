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
```

## API Endpoints

### 1. Generate Staff Availability
```bash
POST /api/availability/generate
Content-Type: application/json
X-API-Key: your-api-key-here

{
  "tenant_id": "123",
  "location_id": "456",
  "location_tz": "America/New_York"
}

# For regeneration (specific date)
{
  "tenant_id": "123",
  "location_id": "456", 
  "location_tz": "America/New_York",
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

## Next.js Integration Example

```typescript
// lib/celery-api.ts
const CELERY_API_URL = process.env.CELERY_API_URL || 'https://your-celery-app.render.com';
const API_KEY = process.env.CELERY_API_KEY!;

interface GenerateAvailabilityRequest {
  tenant_id: string;
  location_id: string;
  location_tz: string;
  affected_date?: string; // Optional for regeneration
}

interface TaskResponse {
  task_id: string;
  status: string;
  message: string;
  tenant_id: string;
  location_id: string;
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

  async generateStaffAvailability(data: GenerateAvailabilityRequest): Promise<TaskResponse> {
    return this.makeRequest('/api/availability/generate', {
      method: 'POST',
      body: JSON.stringify(data),
    });
  }

  async generateVenueAvailability(data: GenerateAvailabilityRequest): Promise<TaskResponse> {
    return this.makeRequest('/api/availability/generate-venue', {
      method: 'POST',
      body: JSON.stringify(data),
    });
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
      // Start the task
      const taskResponse = await celeryAPI.generateStaffAvailability({
        tenant_id: '123',
        location_id: '456',
        location_tz: 'America/New_York'
      });
      
      setTaskStatus(`Task started: ${taskResponse.task_id}`);
      
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
# 1. Generate staff availability
curl -X POST https://your-celery-app.render.com/api/availability/generate \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key-here" \
  -d '{
    "tenant_id": "123",
    "location_id": "456",
    "location_tz": "America/New_York"
  }'

# Response:
# {
#   "task_id": "abc-123-def",
#   "status": "pending",
#   "message": "Availability generation task started",
#   "tenant_id": "123",
#   "location_id": "456",
#   "is_regeneration": false
# }

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
  "missing_fields": ["tenant_id", "location_tz"]
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
