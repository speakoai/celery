# ElevenLabs Post-Conversation Webhook Setup

## What is HMAC Authentication?

**HMAC (Hash-based Message Authentication Code)** is a security mechanism that ensures:
1. The webhook request is actually from ElevenLabs (authentication)
2. The payload hasn't been tampered with (integrity)

## How HMAC Works

### On ElevenLabs' Side:
1. ElevenLabs has a **secret key** (shared with you in their dashboard)
2. When they send a webhook, they:
   - Take the entire request body (raw bytes)
   - Create a signature using HMAC-SHA256: `HMAC(secret_key, request_body)`
   - Send this signature in the `elevenlabs-signature` header

### On Your Side (What the Code Does):
1. You store the same **secret key** in environment variable `ELEVENLABS_WEBHOOK_SECRET`
2. When you receive a webhook:
   - Get the raw request body (same bytes ElevenLabs used)
   - Compute your own signature: `HMAC(your_secret_key, request_body)`
   - Compare your signature with the one in the header
   - If they match ✅ → Request is authentic from ElevenLabs
   - If they don't match ❌ → Reject the request (could be an attacker)

## Setup Steps

### 1. Get Your Webhook Secret from ElevenLabs
- Log into your ElevenLabs dashboard
- Go to your agent/conversational AI settings
- Find the webhook configuration section
- Look for a "Webhook Secret" or "Signing Secret" field
- Copy this secret key

### 2. Add Secret to Your Environment Variables

On Render.com (or your hosting platform):
- Go to your service settings
- Add an environment variable:
  - **Key**: `ELEVENLABS_WEBHOOK_SECRET`
  - **Value**: The secret you copied from ElevenLabs
- Save and redeploy if necessary

### 3. Configure the Webhook in ElevenLabs
- Set the webhook URL to: `https://app-3k7t.onrender.com/webhook/post_conversation`
- Set the auth method to: **HMAC**
- Save the configuration

## Testing

### What the Endpoint Does:
The `/webhook/post_conversation` endpoint will:
1. ✅ Receive the webhook POST request
2. ✅ Log ALL headers to console
3. ✅ Log the raw payload bytes
4. ✅ Parse and log the JSON payload
5. ✅ Verify the HMAC signature (if secret is configured)
6. ✅ Print detailed verification results

### How to Test:
1. Make a call to your ElevenLabs agent
2. Hang up or let the call disconnect
3. Check your Render logs (or wherever your Flask app logs go)
4. You'll see a detailed printout like:

```
================================================================================
ELEVENLABS POST-CONVERSATION WEBHOOK RECEIVED
================================================================================
Timestamp: 2025-12-10T12:34:56.789012Z

Headers:
  Host: app-3k7t.onrender.com
  User-Agent: ElevenLabs-Webhook/1.0
  Content-Type: application/json
  elevenlabs-signature: abc123def456...
  ...

Raw Payload (bytes length): 1234
Raw Payload (first 500 chars): b'{"conversation_id": "...", ...}'

Parsed JSON Payload:
{
  "conversation_id": "...",
  "agent_id": "...",
  "call_duration": 123,
  "transcript": [...],
  ...
}

--- HMAC Verification ---
Received Signature: abc123def456...
Webhook Secret Set: True
Computed Signature: abc123def456...
Signature Valid: True

✅ Webhook processed successfully
================================================================================
```

## Security Notes

### If You Don't Set the Secret:
- The endpoint will still work and log everything
- **BUT** you'll see a warning: `⚠️ WARNING: ELEVENLABS_WEBHOOK_SECRET not set`
- Anyone could potentially send fake webhooks to your endpoint

### If You Set the Secret:
- Only requests with valid signatures will be accepted
- Invalid signatures get a `401 Unauthorized` response
- Your endpoint is protected from unauthorized access

## Current Status

✅ **Endpoint is ready**: `/webhook/post_conversation`
✅ **Logging configured**: Will print everything to console
✅ **HMAC verification**: Implemented and ready
⚠️ **Action needed**: Set `ELEVENLABS_WEBHOOK_SECRET` environment variable for security

## Next Steps

1. Add `ELEVENLABS_WEBHOOK_SECRET` to your Render environment variables
2. Make a test call
3. Check the logs to see what data ElevenLabs sends
4. Once you see the data structure, you can add processing logic as needed
