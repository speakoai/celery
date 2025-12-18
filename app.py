import os
import json
import secrets
import hmac
import base64
import psycopg2
import redis
from functools import wraps
from flask import Flask, flash, render_template, redirect, request, jsonify
from tasks.demo_task import add
from tasks.availability_gen_regen import gen_availability, gen_availability_venue
from tasks.sms import (
    send_sms_confirmation_new, send_sms_confirmation_mod, send_sms_confirmation_can,
    send_email_confirmation_new_rest, send_email_confirmation_new, 
    send_email_confirmation_mod_rest, send_email_confirmation_mod,
    send_email_confirmation_can_rest, send_email_confirmation_can,
    send_email_confirmation_customer_new, send_email_confirmation_customer_mod, send_email_confirmation_customer_can
)
from tasks.celery_app import app as celery_app
from tasks.analyze_knowledge import analyze_knowledge_file
from tasks.scrape_url import scrape_url_to_markdown
from tasks.sync_speako_data import sync_speako_data
from tasks.publish_elevenlabs_agent import publish_elevenlabs_agent
from tasks.create_ai_agent import create_conversation_ai_agent
# Additional imports for R2 uploads
import boto3
from werkzeug.utils import secure_filename
from datetime import datetime
import hashlib
import time
# Imports for webhook processing
from tasks.utils.elevenlabs_client import get_conversation_details
from tasks.utils.publish_r2 import upload_audio_to_r2
from zoneinfo import ZoneInfo
# OpenAI SDK (optional)
try:
    from openai import OpenAI
    _openai_import_error = None
except Exception as _openai_e:
    OpenAI = None
    _openai_import_error = repr(_openai_e)
# New: helpers for file_url mode
import mimetypes
from urllib.parse import urlparse

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', "super-secret")

# --- Startup debug for OpenAI config (does NOT print secrets) ---
try:
    _openai_client_available = OpenAI is not None
    _openai_key_present = bool(os.getenv('OPENAI_API_KEY'))
    _openai_model = os.getenv('OPENAI_KNOWLEDGE_MODEL', 'gpt-4o-mini')
    # Try to detect SDK version even if OpenAI class import fails
    try:
        import openai as _openai_mod
        _openai_version = getattr(_openai_mod, '__version__', 'unknown')
    except Exception:
        _openai_version = None
    # Use warning level so it always shows in Render logs
    app.logger.warning(
        f"[Startup] OpenAI client available: {_openai_client_available}; OPENAI_API_KEY set: {_openai_key_present}; model: {_openai_model}; sdk_version: {_openai_version}; import_error: {_openai_import_error}"
    )
except Exception as _e:
    print(f"[Startup] OpenAI debug logging failed: {_e}")

# ----------------------------
# Cloudflare R2 (S3-compatible) configuration for uploads
# ----------------------------
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
# Optional: custom CDN domain mapped to the bucket (defaults to assets.speako.ai)
R2_PUBLIC_BASE_URL = os.getenv("R2_PUBLIC_BASE_URL", "https://assets.speako.ai")

r2_client = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT_URL,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
) if all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT_URL, R2_BUCKET_NAME]) else None

# Database configuration for webhooks
DATABASE_URL = os.getenv("DATABASE_URL")

# Redis configuration for agent context caching
REDIS_URL = os.getenv("REDIS_URL")
AGENT_CONTEXT_REDIS_PREFIX = "agent_context"
AGENT_CONTEXT_TTL = 86400  # 24 hours

# Webhook configuration
WEBHOOK_MAX_AUDIO_SIZE = 100 * 1024 * 1024  # 100MB in bytes

# Usage notification threshold (percentage)
USAGE_WARNING_THRESHOLD = 70

# Trial minutes from environment (default 15)
TRIAL_MINUTES = int(os.getenv('TRIAL_MINUTES', '15'))

# =============================================================================
# DEMO AGENT NOTIFICATION CONFIGURATION
# =============================================================================
# Demo agents skip database recording and only send SMS/email notifications.
# To add a new demo agent, add an entry with:
#   - "name": Display name for the agent
#   - "notify_sms": Phone number to receive SMS (E.164 format)
#   - "notify_email": Email address to receive notification
# =============================================================================
DEMO_AGENT_NOTIFY_CONFIG = {
    "agent_1801k73qzzqbe90t1zym8wc3mg8a": {
        "name": "Surpass Demo Agent",
        "notify_sms": "+61410248573",
        "notify_email": "pedro@surpass.com.au",
    },
    # Add more demo agents here:
    # "agent_xxxxx": {
    #     "name": "Another Demo",
    #     "notify_sms": "+61...",
    #     "notify_email": "someone@example.com",
    # },
}


def send_demo_agent_notification(conversation_id: str, agent_config: dict) -> dict:
    """
    Send SMS and email notification for demo agent conversations.
    No database recording - notification only.
    
    Args:
        conversation_id: ElevenLabs conversation ID
        agent_config: Dict with 'name', 'notify_sms', 'notify_email'
    
    Returns:
        Dict with success status and details
    """
    from twilio.rest import Client as TwilioClient
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail
    from tasks.utils.elevenlabs_client import get_conversation_audio
    from tasks.utils.publish_r2 import upload_audio_to_r2
    
    result = {
        'success': True,
        'conversation_id': conversation_id,
        'agent_name': agent_config.get('name', 'Demo Agent'),
        'sms_sent': False,
        'email_sent': False,
        'audio_uploaded': False,
        'errors': []
    }
    
    print(f"\n[Demo Agent] Processing notification for {agent_config.get('name')}")
    print(f"[Demo Agent] Conversation ID: {conversation_id}")
    
    # Fetch caller context from Redis (if available)
    caller_name = "not available"
    caller_company = "not available"
    try:
        if REDIS_URL:
            redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
            context_key = f"{AGENT_CONTEXT_REDIS_PREFIX}:{conversation_id}"
            context_data = redis_client.get(context_key)
            if context_data:
                context_json = json.loads(context_data)
                caller_name = context_json.get('caller_name') or 'not available'
                caller_company = context_json.get('caller_company') or 'not available'
                print(f"[Demo Agent] ✅ Retrieved caller context from Redis: name={caller_name}, company={caller_company}")
            else:
                print(f"[Demo Agent] ⚠️  No caller context found in Redis for {conversation_id}")
    except Exception as redis_err:
        print(f"[Demo Agent] ⚠️  Failed to fetch caller context from Redis: {redis_err}")
    
    # Fetch conversation details from ElevenLabs API
    details = None
    try:
        details = get_conversation_details(conversation_id)
        print(f"[Demo Agent] ✅ Retrieved conversation details from ElevenLabs API")
    except Exception as e:
        print(f"[Demo Agent] ⚠️  Failed to fetch conversation details: {e}")
        result['errors'].append(f"Failed to fetch details: {str(e)}")
    
    # Download and upload audio to R2
    audio_url = None
    try:
        print(f"[Demo Agent] 🎵 Downloading conversation audio...")
        audio_bytes, content_type = get_conversation_audio(conversation_id)
        print(f"[Demo Agent] ✅ Downloaded audio: {len(audio_bytes)} bytes, type={content_type}")
        
        # Upload to R2 under demo-agents folder
        r2_key, audio_url = upload_audio_to_r2(
            tenant_id="demo-agents",
            location_id=agent_config.get('name', 'demo').replace(' ', '-').lower(),
            conversation_id=conversation_id,
            audio_bytes=audio_bytes,
            content_type=content_type
        )
        result['audio_uploaded'] = True
        result['audio_url'] = audio_url
        print(f"[Demo Agent] ✅ Uploaded audio to R2: {audio_url}")
    except Exception as e:
        print(f"[Demo Agent] ⚠️  Failed to download/upload audio: {e}")
        result['errors'].append(f"Audio upload failed: {str(e)}")
    
    # Extract data from conversation details
    caller_phone = "Unknown"
    duration_secs = 0
    duration_str = "Unknown"
    status = "unknown"
    status_emoji = "❓"
    summary_title = "No summary available"
    summary_text = ""
    call_time_str = "Unknown"
    language = "Unknown"
    termination_reason = "Unknown"
    
    if details:
        metadata = details.get('metadata', {})
        analysis = details.get('analysis', {})
        phone_call = metadata.get('phone_call', {})
        dynamic_vars = details.get('conversation_initiation_client_data', {}).get('dynamic_variables', {})
        
        # Caller phone (try multiple sources)
        caller_phone = (
            phone_call.get('external_number') or
            dynamic_vars.get('system__caller_id') or
            "Unknown"
        )
        
        # Duration
        duration_secs = metadata.get('call_duration_secs', 0) or 0
        mins, secs = divmod(int(duration_secs), 60)
        duration_str = f"{mins}m {secs}s" if mins > 0 else f"{secs}s"
        
        # Status
        call_successful = analysis.get('call_successful', '')
        if call_successful == 'success':
            status = "Successful"
            status_emoji = "✅"
        elif call_successful:
            status = call_successful.title()
            status_emoji = "⚠️"
        else:
            status = details.get('status', 'unknown').title()
            status_emoji = "✅" if status.lower() in ['done', 'completed'] else "⚠️"
        
        # Summary
        summary_title = analysis.get('call_summary_title', '') or 'Call completed'
        summary_text = analysis.get('transcript_summary', '') or ''
        
        # Call time
        start_unix = metadata.get('start_time_unix_secs')
        timezone_str = metadata.get('timezone', 'UTC')
        if start_unix:
            try:
                from zoneinfo import ZoneInfo
                utc_dt = datetime.fromtimestamp(start_unix, tz=ZoneInfo('UTC'))
                local_dt = utc_dt.astimezone(ZoneInfo(timezone_str))
                call_time_str = local_dt.strftime('%d %b %Y, %I:%M %p')
            except Exception:
                call_time_str = datetime.fromtimestamp(start_unix).strftime('%d %b %Y, %I:%M %p')
        
        # Language
        language = metadata.get('main_language', '') or details.get('language', 'Unknown')
        if language:
            language = language.upper() if len(language) == 2 else language.title()
        
        # Termination reason
        termination_reason = metadata.get('termination_reason', 'Unknown')
    
    # Call type for SMS (use summary_title or default)
    call_type = summary_title if summary_title and summary_title != 'Call completed' else 'Cannot analysis call type'
    
    # Build SMS message
    sms_body = (
        f"🤖 Speako AI - New Call\n\n"
        f"From: {caller_phone}\n"
        f"Name: {caller_name}\n"
        f"Company: {caller_company}\n"
        f"Duration: {duration_str}\n"
        f"Status: {status} {status_emoji}\n"
        f"Call Type: {call_type}\n"
    )
    if audio_url:
        sms_body += f"\n🎧 Listen: {audio_url}\n"
    sms_body += f"\nID: {conversation_id}"
    
    # Build email HTML
    email_subject = "You have a new call conversation from Speako AI"
    # Build audio player section for email (if audio available)
    audio_section = ""
    if audio_url:
        audio_section = f"""
        <div style="margin: 20px 0; padding: 15px; background: #f0f7ff; border-radius: 8px; border-left: 4px solid #0066cc;">
            <h3 style="color: #0066cc; margin: 0 0 10px 0;">🎧 Listen to Recording</h3>
            <audio controls style="width: 100%; margin-bottom: 10px;">
                <source src="{audio_url}" type="audio/mpeg">
                Your browser does not support the audio element.
            </audio>
            <p style="margin: 0; font-size: 12px;">
                <a href="{audio_url}" style="color: #0066cc;">Download audio file</a>
            </p>
        </div>
        """
    
    email_html = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <h2 style="color: #333;">📞 New Call Conversation</h2>
        <p style="color: #666;">A new conversation has been completed on <strong>{agent_config.get('name', 'Demo Agent')}</strong>.</p>
        
        <table style="width: 100%; border-collapse: collapse; margin: 20px 0;">
            <tr><td style="padding: 8px 0; border-bottom: 1px solid #eee; color: #888;">Caller</td>
                <td style="padding: 8px 0; border-bottom: 1px solid #eee;"><strong>{caller_phone}</strong></td></tr>
            <tr><td style="padding: 8px 0; border-bottom: 1px solid #eee; color: #888;">Name</td>
                <td style="padding: 8px 0; border-bottom: 1px solid #eee;">{caller_name}</td></tr>
            <tr><td style="padding: 8px 0; border-bottom: 1px solid #eee; color: #888;">Company</td>
                <td style="padding: 8px 0; border-bottom: 1px solid #eee;">{caller_company}</td></tr>
            <tr><td style="padding: 8px 0; border-bottom: 1px solid #eee; color: #888;">Duration</td>
                <td style="padding: 8px 0; border-bottom: 1px solid #eee;">{duration_str}</td></tr>
            <tr><td style="padding: 8px 0; border-bottom: 1px solid #eee; color: #888;">Date/Time</td>
                <td style="padding: 8px 0; border-bottom: 1px solid #eee;">{call_time_str}</td></tr>
            <tr><td style="padding: 8px 0; border-bottom: 1px solid #eee; color: #888;">Status</td>
                <td style="padding: 8px 0; border-bottom: 1px solid #eee;">{status_emoji} {status}</td></tr>
            <tr><td style="padding: 8px 0; border-bottom: 1px solid #eee; color: #888;">Language</td>
                <td style="padding: 8px 0; border-bottom: 1px solid #eee;">{language}</td></tr>
            <tr><td style="padding: 8px 0; border-bottom: 1px solid #eee; color: #888;">Ended By</td>
                <td style="padding: 8px 0; border-bottom: 1px solid #eee;">{termination_reason}</td></tr>
        </table>
        
        {audio_section}
        
        <h3 style="color: #333;">📝 Summary: {summary_title}</h3>
        <p style="color: #555; line-height: 1.6; background: #f9f9f9; padding: 15px; border-radius: 8px;">
            {summary_text if summary_text else '<em>No summary available</em>'}
        </p>
        
        <p style="color: #999; font-size: 12px; margin-top: 30px; border-top: 1px solid #eee; padding-top: 15px;">
            Conversation ID: {conversation_id}<br>
            Speako AI • Powered by ElevenLabs
        </p>
    </div>
    """
    
    # Send SMS via Twilio
    notify_sms = agent_config.get('notify_sms')
    if notify_sms:
        try:
            twilio_sid = os.getenv("TWILIO_ACCOUNT_SID")
            twilio_token = os.getenv("TWILIO_AUTH_TOKEN")
            twilio_from = os.getenv("TWILIO_SEND_SMS_NUMBER")
            
            if twilio_sid and twilio_token and twilio_from:
                client = TwilioClient(twilio_sid, twilio_token)
                message = client.messages.create(
                    body=sms_body,
                    from_=twilio_from,
                    to=notify_sms
                )
                result['sms_sent'] = True
                result['sms_sid'] = message.sid
                print(f"[Demo Agent] ✅ SMS sent to {notify_sms} (SID: {message.sid})")
            else:
                print(f"[Demo Agent] ⚠️  Twilio credentials not configured")
                result['errors'].append("Twilio credentials not configured")
        except Exception as e:
            print(f"[Demo Agent] ❌ SMS failed: {e}")
            result['errors'].append(f"SMS failed: {str(e)}")
    
    # Send Email via SendGrid
    notify_email = agent_config.get('notify_email')
    if notify_email:
        try:
            sendgrid_key = os.getenv("SENDGRID_API_KEY")
            
            if sendgrid_key:
                sg = SendGridAPIClient(sendgrid_key)
                message = Mail(
                    from_email="no-reply@speako.ai",
                    to_emails=notify_email,
                    subject=email_subject,
                    html_content=email_html
                )
                response = sg.send(message)
                result['email_sent'] = True
                result['email_status_code'] = response.status_code
                print(f"[Demo Agent] ✅ Email sent to {notify_email} (Status: {response.status_code})")
            else:
                print(f"[Demo Agent] ⚠️  SendGrid API key not configured")
                result['errors'].append("SendGrid API key not configured")
        except Exception as e:
            print(f"[Demo Agent] ❌ Email failed: {e}")
            result['errors'].append(f"Email failed: {str(e)}")
    
    # Set overall success based on at least one notification sent
    result['success'] = result['sms_sent'] or result['email_sent'] or not (notify_sms or notify_email)
    
    return result


def get_db_connection():
    """Get PostgreSQL database connection for webhook processing."""
    return psycopg2.connect(DATABASE_URL)


def trigger_usage_notification(tenant_id: int, conn) -> None:
    """
    Fire-and-forget function to check if usage threshold is crossed and send notification.
    
    This function:
    1. Gets current usage state (minutes used, minutes included, period start)
    2. Checks if usage >= 70% threshold
    3. Checks if notification was already sent for this billing period
    4. Creates notification if needed
    
    Args:
        tenant_id: The tenant ID to check usage for
        conn: Database connection (will create new cursor, won't commit)
    """
    print(f"\n[Notification] Checking usage notification for tenant {tenant_id}")
    
    try:
        # Step 1: Get current usage state
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    tp.voice_minutes_included,
                    tmb.current_period_start,
                    tmb.current_period_end,
                    bup.billing_type,
                    bup.period_start_date,
                    bup.period_end_date,
                    COALESCE((
                        SELECT ABS(SUM(l.seconds_delta))
                        FROM billing_minute_ledger l
                        WHERE l.tenant_id = tp.tenant_id
                          AND l.source = 'call_usage'
                          AND l.usage_bucket = 'plan'
                          AND l.created_at >= bup.period_start_date
                          AND l.created_at < bup.period_end_date
                    ), 0) AS seconds_used_in_period
                FROM tenant_plans tp
                LEFT JOIN tenant_minute_balance tmb
                    ON tmb.tenant_id = tp.tenant_id
                LEFT JOIN billing_usage_periods bup
                    ON bup.tenant_id = tp.tenant_id
                    AND bup.is_current_period = true
                WHERE tp.tenant_id = %s
                  AND tp.active = true
                LIMIT 1
            """, (tenant_id,))
            
            row = cur.fetchone()
        
        if not row:
            print(f"[Notification] No active plan found for tenant {tenant_id}")
            return
        
        voice_minutes_included, current_period_start, current_period_end, billing_type, period_start_date, period_end_date, seconds_used_in_period = row
        
        # Determine if trial
        is_trial = (billing_type == 'trial')
        
        # Calculate minutes
        seconds_used = float(seconds_used_in_period or 0)
        minutes_used = seconds_used / 60.0
        
        # Calculate included minutes based on trial vs paid
        if is_trial:
            minutes_included = TRIAL_MINUTES
        else:
            minutes_included = float(voice_minutes_included or 0)
        
        # Calculate usage percentage
        if minutes_included > 0:
            usage_percent = min(100.0, (minutes_used / minutes_included) * 100.0)
        else:
            usage_percent = 0.0
        
        # Round to 1 decimal
        usage_percent = round(usage_percent, 1)
        minutes_used = round(minutes_used, 1)
        
        print(f"[Notification] Tenant {tenant_id}: {minutes_used} / {minutes_included} minutes ({usage_percent}%)")
        
        # Step 2: Check if below threshold
        if usage_percent < USAGE_WARNING_THRESHOLD:
            print(f"[Notification] Usage {usage_percent}% is below {USAGE_WARNING_THRESHOLD}% threshold - no notification needed")
            return
        
        # Step 3: Check for duplicate notification in this billing period
        period_start = period_start_date or current_period_start
        
        if not period_start:
            print(f"[Notification] No period start date found - skipping duplicate check")
        else:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM tenant_notifications tn
                    JOIN notifications n ON n.notification_id = tn.notification_id
                    WHERE tn.tenant_id = %s 
                      AND n.type_key = 'usage'
                      AND (n.metadata->>'threshold')::int = %s
                      AND n.created_at >= %s
                    LIMIT 1
                """, (tenant_id, USAGE_WARNING_THRESHOLD, period_start))
                
                already_notified = cur.fetchone()
            
            if already_notified:
                print(f"[Notification] Already notified for this billing period - skipping")
                return
        
        # Step 4: Create notification
        print(f"[Notification] Creating usage warning notification for tenant {tenant_id}")
        
        notification_title = f"AI minutes usage at {int(usage_percent)}%"
        notification_message = (
            f"You've used {minutes_used} minutes of your {int(minutes_included)} minutes "
            f"AI minutes limit. Consider upgrading your plan to avoid service interruption."
        )
        notification_metadata = json.dumps({
            "threshold": USAGE_WARNING_THRESHOLD,
            "percentage": usage_percent,
            "resource": "AI minutes",
            "minutes_used": minutes_used,
            "minutes_included": minutes_included
        })
        
        with conn.cursor() as cur:
            # Insert into notifications table
            cur.execute("""
                INSERT INTO notifications (type_key, title, message, link_url, link_label, metadata, is_broadcast)
                VALUES ('usage', %s, %s, '/dashboard/billing', 'View Usage', %s, false)
                RETURNING notification_id
            """, (notification_title, notification_message, notification_metadata))
            
            notification_id = cur.fetchone()[0]
            
            # Insert into tenant_notifications table
            cur.execute("""
                INSERT INTO tenant_notifications (tenant_id, notification_id)
                VALUES (%s, %s)
            """, (tenant_id, notification_id))
        
        # Note: We don't commit here - the caller will commit as part of the main transaction
        print(f"✅ [Notification] Usage warning notification created (notification_id={notification_id})")
        
    except Exception as e:
        print(f"⚠️  [Notification] Error creating usage notification: {e}")
        # Don't re-raise - this is fire-and-forget
        import traceback
        traceback.print_exc()


def allowed_knowledge_file(filename: str) -> bool:
    """Return True if filename has an allowed knowledge extension (doc/x, xls/x, pdf, csv, txt)."""
    if not filename:
        return False
    allowed_ext = {'.doc', '.docx', '.xls', '.xlsx', '.pdf', '.csv', '.txt'}
    ext = os.path.splitext(filename.lower())[1]
    return ext in allowed_ext


def generate_knowledge_filename(tenant_id: str, location_id: str, knowledge_type: str, file_extension: str) -> str:
    """Generate deterministic unique filename for knowledge uploads."""
    base = f"{tenant_id}_{location_id}_{knowledge_type}_{int(time.time())}"
    digest = hashlib.md5(base.encode()).hexdigest()[:8]
    return f"{tenant_id}_{location_id}_{knowledge_type}_{digest}{file_extension}"

# Import avatar API functionality
import sys
import json
from pathlib import Path

# Avatar API class for serving avatar catalog data
class AvatarAPI:
    """API class for serving avatar catalog data."""
    
    def __init__(self, catalog_file: str = "speako-dashboard-avatar/avatar_catalog_simple.json"):
        """Initialize with catalog file path."""
        self.catalog_file = catalog_file
        self.catalog = self.load_catalog()
    
    def load_catalog(self):
        """Load avatar catalog from JSON file."""
        try:
            if os.path.exists(self.catalog_file):
                with open(self.catalog_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                return {"avatars": [], "metadata": {"total": 0}}
        except Exception as e:
            print(f"Error loading avatar catalog: {str(e)}")
            return {"avatars": [], "metadata": {"total": 0}}
    
    def refresh_catalog(self):
        """Refresh catalog from file."""
        try:
            self.catalog = self.load_catalog()
            return True
        except Exception:
            return False
    
    def get_all_avatars(self):
        """Get all avatars with metadata."""
        return {
            "success": True,
            "data": self.catalog.get("avatars", []),
            "metadata": self.catalog.get("metadata", {}),
            "total": len(self.catalog.get("avatars", []))
        }
    
    def search_avatars(self, filters):
        """Search avatars based on filters."""
        avatars = self.catalog.get("avatars", [])
        filtered_avatars = []
        
        for avatar in avatars:
            tags = avatar.get("tags", [])
            tags_lower = [tag.lower() for tag in tags]
            match = True
            
            # Apply filters based on tags
            if "gender" in filters and filters["gender"]:
                if filters["gender"].lower() not in tags_lower:
                    match = False
            
            if "race" in filters and filters["race"]:
                if filters["race"].lower() not in tags_lower:
                    match = False
            
            if "occupation" in filters and filters["occupation"]:
                if filters["occupation"].lower() not in tags_lower:
                    match = False
            
            if "style" in filters and filters["style"]:
                if filters["style"].lower() not in tags_lower:
                    match = False
            
            if "age_group" in filters and filters["age_group"]:
                if filters["age_group"].lower() not in tags_lower:
                    match = False
            
            # Tags search (any of the provided tags must match)
            if "tags" in filters and filters["tags"]:
                search_tags = [tag.lower() for tag in filters["tags"]]
                if not any(tag in tags_lower for tag in search_tags):
                    match = False
            
            # Text search across all tags
            if "search" in filters and filters["search"]:
                search_term = filters["search"].lower()
                searchable_text = " ".join(tags).lower()
                
                if search_term not in searchable_text:
                    match = False
            
            if match:
                filtered_avatars.append(avatar)
        
        return {
            "success": True,
            "data": filtered_avatars,
            "total": len(filtered_avatars),
            "filters_applied": filters
        }
    
    def get_avatar_by_id(self, avatar_id):
        """Get specific avatar by ID."""
        avatars = self.catalog.get("avatars", [])
        
        for avatar in avatars:
            if avatar.get("id") == avatar_id:
                return {
                    "success": True,
                    "data": avatar
                }
        
        return {
            "success": False,
            "error": "Avatar not found",
            "data": None
        }
    
    def get_avatar_stats(self):
        """Get statistics about avatars."""
        avatars = self.catalog.get("avatars", [])
        
        # Count occurrences of each tag
        tag_counts = {}
        total_avatars = len(avatars)
        
        for avatar in avatars:
            tags = avatar.get("tags", [])
            for tag in tags:
                tag_lower = tag.lower()
                tag_counts[tag_lower] = tag_counts.get(tag_lower, 0) + 1
        
        # Categorize tags
        gender_tags = {}
        occupation_tags = {}
        race_tags = {}
        style_tags = {}
        age_tags = {}
        other_tags = {}
        
        # Common categorization patterns
        genders = ["male", "female", "non-binary"]
        occupations = ["businessman", "businessperson", "teacher", "farmer", "adventurer", "fashion enthusiast"]
        races = ["caucasian", "black", "hispanic", "asian", "indian"]
        styles = ["cartoon", "professional", "casual", "formal"]
        ages = ["young", "middle-aged", "senior"]
        
        for tag, count in tag_counts.items():
            if tag in genders:
                gender_tags[tag] = count
            elif tag in occupations:
                occupation_tags[tag] = count
            elif tag in races:
                race_tags[tag] = count
            elif tag in styles:
                style_tags[tag] = count
            elif tag in ages:
                age_tags[tag] = count
            else:
                other_tags[tag] = count
        
        return {
            "success": True,
            "data": {
                "total_avatars": total_avatars,
                "genders": gender_tags,
                "occupations": occupation_tags,
                "races": race_tags,
                "styles": style_tags,
                "age_groups": age_tags,
                "other_tags": other_tags,
                "all_tags": tag_counts
            }
        }

# Initialize avatar API
avatar_api = AvatarAPI()

# API Authentication decorator
def require_api_key(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check API key
        api_key = request.headers.get('X-API-Key')
        expected_key = os.getenv('API_SECRET_KEY')
        
        if not api_key or not expected_key:
            return jsonify({'error': 'API key required', 'code': 'MISSING_API_KEY'}), 401
            
        if not secrets.compare_digest(api_key, expected_key):
            return jsonify({'error': 'Unauthorized', 'code': 'INVALID_API_KEY'}), 401
        
        # Optional: Check allowed origins (if needed)
        origin = request.headers.get('Origin')
        allowed_origins = os.getenv('ALLOWED_ORIGINS', '').split(',')
        if allowed_origins and allowed_origins != [''] and origin not in allowed_origins:
            return jsonify({'error': 'Forbidden origin'}), 403
            
        return f(*args, **kwargs)
    return decorated_function


@app.route('/')
def main():
    return render_template('main.html')


@app.route('/add', methods=['POST'])
def add_inputs():
    x = int(request.form['x'] or 0)
    y = int(request.form['y'] or 0)
    add.delay(x, y)
    flash("Your addition job has been submitted.")
    return redirect('/')


# =============================================================================
# API ENDPOINTS FOR EXTERNAL ACCESS
# =============================================================================

@app.route('/api/availability/generate', methods=['POST'])
@require_api_key
def api_generate_availability():
    """
    Generate availability for a location based on business type
    Expected JSON payload:
    {
        "tenant_id": "123",
        "location_id": "456", 
        "location_tz": "America/New_York",
        "business_type": "rest" | "service",  // mandatory - "rest" for restaurant/venue, "service" for staff
        "affected_date": "2025-08-15"  // optional, for regeneration
    }
    """
    try:
        # Be tolerant of clients missing Content-Type or sending invalid JSON
        data = request.get_json(silent=True)
        if not data:
            return jsonify({
                'error': 'JSON payload required',
                'message': 'Send a valid JSON body with Content-Type: application/json',
                'content_type': request.headers.get('Content-Type', None)
            }), 400
        
        # Validate required fields
        required_fields = ['tenant_id', 'location_id', 'location_tz', 'business_type']
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            return jsonify({
                'error': 'Missing required fields', 
                'missing_fields': missing_fields
            }), 400
        
        # Extract parameters
        tenant_id = data['tenant_id']
        location_id = data['location_id']
        location_tz = data['location_tz']
        business_type = data['business_type']
        affected_date = data.get('affected_date')  # Optional for regeneration
        speako_task_id = data.get('speako_task_id')  # Optional for task tracking
        
        # Validate business_type
        if business_type not in ['rest', 'service']:
            return jsonify({
                'error': 'Invalid business_type',
                'message': 'business_type must be either "rest" or "service"',
                'provided': business_type
            }), 400
        
        # Route to appropriate task based on business_type
        if business_type == 'rest':
            task = gen_availability_venue.delay(tenant_id, location_id, location_tz, affected_date, task_id=speako_task_id)
            task_type = 'venue'
        else:
            task = gen_availability.delay(tenant_id, location_id, location_tz, affected_date, task_id=speako_task_id)
            task_type = 'staff'
        
        response_data = {
            'task_id': task.id,
            'status': 'pending',
            'message': f'{task_type.title()} availability generation task started',
            'tenant_id': tenant_id,
            'location_id': location_id,
            'business_type': business_type,
            'task_type': task_type,
            'is_regeneration': affected_date is not None
        }
        
        # Include speako_task_id in response if provided
        if speako_task_id:
            response_data['speako_task_id'] = speako_task_id
        
        return jsonify(response_data), 202
        
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/api/availability/generate-venue', methods=['POST'])
@require_api_key
def api_generate_venue_availability():
    """
    Generate venue availability for a location
    Expected JSON payload:
    {
        "tenant_id": "123",
        "location_id": "456", 
        "location_tz": "America/New_York",
        "affected_date": "2025-08-15"  // optional, for regeneration
    }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'JSON payload required'}), 400
        
        # Validate required fields
        required_fields = ['tenant_id', 'location_id', 'location_tz']
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            return jsonify({
                'error': 'Missing required fields', 
                'missing_fields': missing_fields
            }), 400
        
        # Extract parameters
        tenant_id = data['tenant_id']
        location_id = data['location_id']
        location_tz = data['location_tz']
        affected_date = data.get('affected_date')  # Optional for regeneration
        speako_task_id = data.get('speako_task_id')  # Optional for task tracking
        
        # Trigger the celery task
        task = gen_availability_venue.delay(tenant_id, location_id, location_tz, affected_date, task_id=speako_task_id)
        
        response_data = {
            'task_id': task.id,
            'status': 'pending',
            'message': 'Venue availability generation task started',
            'tenant_id': tenant_id,
            'location_id': location_id,
            'is_regeneration': affected_date is not None
        }
        
        # Include speako_task_id in response if provided
        if speako_task_id:
            response_data['speako_task_id'] = speako_task_id
        
        return jsonify(response_data), 202
        
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/api/task/<task_id>', methods=['GET'])
@require_api_key
def api_get_task_status(task_id):
    """
    Get the status of a celery task
    Returns task status, result, and any error information
    """
    try:
        # Get task result from celery
        task_result = celery_app.AsyncResult(task_id)
        
        response = {
            'task_id': task_id,
            'status': task_result.status,
            'ready': task_result.ready()
        }
        
        if task_result.ready():
            if task_result.successful():
                response['result'] = task_result.result
                response['success'] = True
            else:
                response['error'] = str(task_result.info)
                response['success'] = False
        else:
            # Task is still pending/running
            response['message'] = 'Task is still processing'
        
        return jsonify(response), 200
        
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e),
            'task_id': task_id
        }), 500


@app.route('/api/booking/notifications/send', methods=['POST'])
@require_api_key
def api_send_sms():
    """
    Send SMS notification and email notification for booking actions
    Expected JSON payload:
    {
        "booking_id": 123,
        "action": "new" | "modify" | "cancel",
        "business_type": "service" | "rest",
        "notify_customer": true | false,  // Optional, defaults to true
        "original_booking_id": 456  // Required only for "modify" action
    }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'JSON payload required'}), 400
        
        # Validate required fields
        required_fields = ['booking_id', 'action', 'business_type']
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            return jsonify({
                'error': 'Missing required fields', 
                'missing_fields': missing_fields
            }), 400
        
        # Extract parameters
        booking_id = data['booking_id']
        action = data['action']
        business_type = data['business_type']
        
        # Handle notify_customer - default to True if not provided or empty string
        notify_customer_raw = data.get('notify_customer', True)
        if notify_customer_raw == "":
            notify_customer = True  # Treat empty string as True
        else:
            notify_customer = notify_customer_raw
        
        original_booking_id = data.get('original_booking_id')
        
        # Validate booking_id is an integer
        try:
            booking_id = int(booking_id)
        except (ValueError, TypeError):
            return jsonify({
                'error': 'Invalid booking_id',
                'message': 'booking_id must be a valid integer',
                'provided': booking_id
            }), 400
        
        # Validate notify_customer is a boolean (after handling empty string)
        if not isinstance(notify_customer, bool):
            return jsonify({
                'error': 'Invalid notify_customer',
                'message': 'notify_customer must be a boolean value (true or false)',
                'provided': notify_customer
            }), 400
        
        # Validate action
        valid_actions = ['new', 'modify', 'cancel']
        if action not in valid_actions:
            return jsonify({
                'error': 'Invalid action',
                'message': f'action must be one of: {", ".join(valid_actions)}',
                'provided': action
            }), 400
        
        # Validate business_type
        valid_business_types = ['service', 'rest']
        if business_type not in valid_business_types:
            return jsonify({
                'error': 'Invalid business_type',
                'message': f'business_type must be one of: {", ".join(valid_business_types)}',
                'provided': business_type
            }), 400
        
        # Validate original_booking_id for modify action
        if action == 'modify':
            if not original_booking_id:
                return jsonify({
                    'error': 'Missing required field for modify action',
                    'message': 'original_booking_id is required when action is "modify"'
                }), 400
            try:
                original_booking_id = int(original_booking_id)
            except (ValueError, TypeError):
                return jsonify({
                    'error': 'Invalid original_booking_id',
                    'message': 'original_booking_id must be a valid integer',
                    'provided': original_booking_id
                }), 400
        
        # Start SMS and Email tasks based on action and business_type
        tasks = []
        task_descriptions = []
        
        if action == 'new':
            # SMS task (only if notify_customer is True)
            if notify_customer:
                sms_task = send_sms_confirmation_new.delay(booking_id)
                tasks.append({
                    'task_id': sms_task.id,
                    'type': 'sms',
                    'description': 'new booking SMS confirmation'
                })
                
                # === EMAIL DISABLED - Now handled by external service ===
                # Customer email task (only if notify_customer is True)
                # customer_email_task = send_email_confirmation_customer_new.delay(booking_id)
                # tasks.append({
                #     'task_id': customer_email_task.id,
                #     'type': 'customer_email',
                #     'description': 'new booking customer email confirmation'
                # })
                # === END EMAIL DISABLED ===
            
            # === EMAIL DISABLED - Now handled by external service ===
            # Merchant email task (different based on business type) - always runs
            # if business_type == 'rest':
            #     email_task = send_email_confirmation_new_rest.delay(booking_id)
            #     email_description = 'restaurant new booking merchant email confirmation'
            # else:  # business_type == 'service'
            #     email_task = send_email_confirmation_new.delay(booking_id)
            #     email_description = 'service new booking merchant email confirmation'
            # 
            # tasks.append({
            #     'task_id': email_task.id,
            #     'type': 'merchant_email',
            #     'description': email_description
            # })
            # === END EMAIL DISABLED ===
            
            action_description = 'new booking confirmation'
            
        elif action == 'modify':
            # SMS task (only if notify_customer is True)
            if notify_customer:
                sms_task = send_sms_confirmation_mod.delay(booking_id)
                tasks.append({
                    'task_id': sms_task.id,
                    'type': 'sms',
                    'description': 'booking modification SMS confirmation'
                })
                
                # === EMAIL DISABLED - Now handled by external service ===
                # Customer email task (only if notify_customer is True)
                # customer_email_task = send_email_confirmation_customer_mod.delay(booking_id, original_booking_id)
                # tasks.append({
                #     'task_id': customer_email_task.id,
                #     'type': 'customer_email',
                #     'description': 'booking modification customer email confirmation'
                # })
                # === END EMAIL DISABLED ===
            
            # === EMAIL DISABLED - Now handled by external service ===
            # Merchant email task (different based on business type) - always runs
            # if business_type == 'rest':
            #     email_task = send_email_confirmation_mod_rest.delay(booking_id, original_booking_id)
            #     email_description = 'restaurant booking modification merchant email confirmation'
            # else:  # business_type == 'service'
            #     email_task = send_email_confirmation_mod.delay(booking_id, original_booking_id)
            #     email_description = 'service booking modification merchant email confirmation'
            # 
            # tasks.append({
            #     'task_id': email_task.id,
            #     'type': 'merchant_email',
            #     'description': email_description
            # })
            # === END EMAIL DISABLED ===
            
            action_description = 'booking modification confirmation'
            
        elif action == 'cancel':
            # SMS task (only if notify_customer is True)
            if notify_customer:
                sms_task = send_sms_confirmation_can.delay(booking_id)
                tasks.append({
                    'task_id': sms_task.id,
                    'type': 'sms',
                    'description': 'booking cancellation SMS confirmation'
                })
                
                # === EMAIL DISABLED - Now handled by external service ===
                # Customer email task (only if notify_customer is True)
                # customer_email_task = send_email_confirmation_customer_can.delay(booking_id)
                # tasks.append({
                #     'task_id': customer_email_task.id,
                #     'type': 'customer_email',
                #     'description': 'booking cancellation customer email confirmation'
                # })
                # === END EMAIL DISABLED ===
            
            # === EMAIL DISABLED - Now handled by external service ===
            # Merchant email task (different based on business type) - always runs
            # if business_type == 'rest':
            #     email_task = send_email_confirmation_can_rest.delay(booking_id)
            #     email_description = 'restaurant booking cancellation merchant email confirmation'
            # else:  # business_type == 'service'
            #     email_task = send_email_confirmation_can.delay(booking_id)
            #     email_description = 'service booking cancellation merchant email confirmation'
            # 
            # tasks.append({
            #     'task_id': email_task.id,
            #     'type': 'merchant_email',
            #     'description': email_description
            # })
            # === END EMAIL DISABLED ===
            
            action_description = 'booking cancellation confirmation'
            
        else:
            # This should never happen due to validation above, but added for safety
            return jsonify({
                'error': 'Undefined action',
                'message': f'No action is defined for "{action}". No SMS or email notifications have been sent.',
                'booking_id': booking_id,
                'action': action,
                'valid_actions': ['new', 'modify', 'cancel']
            }), 400
        
        response = {
            'message': f'{action_description.title()} tasks started',
            'booking_id': booking_id,
            'action': action,
            'business_type': business_type,
            'notify_customer': notify_customer,
            'action_description': action_description,
            'tasks': tasks,
            'total_tasks': len(tasks)
        }
        
        # Add original_booking_id to response if applicable
        if action == 'modify' and original_booking_id:
            response['original_booking_id'] = original_booking_id
        
        return jsonify(response), 202
        
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/api/agent/sms', methods=['POST'])
@require_api_key
def api_agent_send_sms():
    """
    Send SMS with URL to caller's phone number from AI agent conversation.
    
    Expected JSON payload:
    {
        "kind": "map_location" | "ecommerce_site" | "company_website" | "product_url",
        "message": "Your message text here",
        "url": "https://example.com/...",
        "callerPhoneNumber": "+61412345678",
        "conversationId": "conv_xxx",  // For logging only
        "agentId": "agent_xxx"         // For logging only
    }
    
    Authentication: X-API-Key header required (API_SECRET_KEY env var)
    """
    from twilio.rest import Client as TwilioClient
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'JSON payload required'}), 400
        
        # Validate required fields
        required_fields = ['kind', 'message', 'url', 'callerPhoneNumber']
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            return jsonify({
                'error': 'Missing required fields',
                'missing_fields': missing_fields
            }), 400
        
        # Extract parameters
        kind = data['kind']
        message = data['message']
        url = data['url']
        caller_phone = data['callerPhoneNumber']
        conversation_id = data.get('conversationId', '')
        agent_id = data.get('agentId', '')
        
        # Validate kind enum
        valid_kinds = ['map_location', 'ecommerce_site', 'company_website', 'product_url']
        if kind not in valid_kinds:
            return jsonify({
                'error': 'Invalid kind',
                'message': f'kind must be one of: {", ".join(valid_kinds)}',
                'provided': kind
            }), 400
        
        # Validate URL format (basic check)
        if not url.startswith(('http://', 'https://')):
            return jsonify({
                'error': 'Invalid URL',
                'message': 'URL must start with http:// or https://',
                'provided': url
            }), 400
        
        # Validate phone number (basic E.164 format check)
        if not caller_phone.startswith('+'):
            return jsonify({
                'error': 'Invalid phone number',
                'message': 'Phone number must be in E.164 format (e.g., +61412345678)',
                'provided': caller_phone
            }), 400
        
        # Build SMS body: message + newline + URL
        sms_body = f"{message}\r\n{url}"
        
        # Log the request
        print(f"\n[Agent SMS] Sending SMS for conversation: {conversation_id}")
        print(f"[Agent SMS] Agent: {agent_id}")
        print(f"[Agent SMS] Kind: {kind}")
        print(f"[Agent SMS] To: {caller_phone}")
        print(f"[Agent SMS] Message length: {len(message)} chars")
        print(f"[Agent SMS] URL: {url}")
        
        # Get Twilio credentials
        twilio_sid = os.getenv("TWILIO_ACCOUNT_SID")
        twilio_token = os.getenv("TWILIO_AUTH_TOKEN")
        twilio_from = os.getenv("TWILIO_SEND_SMS_NUMBER")
        
        if not all([twilio_sid, twilio_token, twilio_from]):
            print(f"[Agent SMS] ❌ Twilio credentials not configured")
            return jsonify({
                'error': 'SMS service not configured',
                'message': 'Twilio credentials are missing'
            }), 500
        
        # Send SMS via Twilio
        try:
            client = TwilioClient(twilio_sid, twilio_token)
            twilio_message = client.messages.create(
                body=sms_body,
                from_=twilio_from,
                to=caller_phone
            )
            
            print(f"[Agent SMS] ✅ SMS sent successfully (SID: {twilio_message.sid})")
            
            return jsonify({
                'success': True,
                'message': 'SMS sent successfully',
                'data': {
                    'sms_sid': twilio_message.sid,
                    'to': caller_phone,
                    'kind': kind,
                    'conversation_id': conversation_id,
                    'agent_id': agent_id
                }
            }), 200
            
        except Exception as sms_error:
            print(f"[Agent SMS] ❌ Failed to send SMS: {sms_error}")
            return jsonify({
                'error': 'SMS sending failed',
                'message': str(sms_error),
                'conversation_id': conversation_id
            }), 500
        
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/api/agent/user/context', methods=['POST'])
@require_api_key
def api_agent_user_context():
    """
    Store caller context in Redis for later retrieval by webhook handler.
    This should be called during an AI agent conversation to capture caller info.
    
    Expected JSON payload:
    {
        "caller_name": "John Smith",
        "caller_company": "Acme Corp",
        "caller_phone_number": "+61412345678",
        "conversation_id": "conv_xxx"
    }
    
    Authentication: X-API-Key header required (API_SECRET_KEY env var)
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'JSON payload required'}), 400
        
        # Validate required field
        if not data.get('conversation_id'):
            return jsonify({
                'error': 'Missing required field',
                'missing_fields': ['conversation_id']
            }), 400
        
        # Extract parameters
        conversation_id = data['conversation_id']
        caller_name = data.get('caller_name', '')
        caller_company = data.get('caller_company', '')
        caller_phone_number = data.get('caller_phone_number', '')
        
        # Log the request
        print(f"\n[Agent Context] Storing context for conversation: {conversation_id}")
        print(f"[Agent Context] Name: {caller_name}")
        print(f"[Agent Context] Company: {caller_company}")
        print(f"[Agent Context] Phone: {caller_phone_number}")
        
        # Check Redis configuration
        if not REDIS_URL:
            print(f"[Agent Context] ❌ Redis URL not configured")
            return jsonify({
                'error': 'Cache service not configured',
                'message': 'REDIS_URL is not set'
            }), 500
        
        # Store in Redis
        try:
            redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
            context_key = f"{AGENT_CONTEXT_REDIS_PREFIX}:{conversation_id}"
            context_data = json.dumps({
                'caller_name': caller_name,
                'caller_company': caller_company,
                'caller_phone_number': caller_phone_number,
                'stored_at': datetime.utcnow().isoformat() + 'Z'
            })
            
            redis_client.setex(context_key, AGENT_CONTEXT_TTL, context_data)
            
            print(f"[Agent Context] ✅ Context stored in Redis (TTL: {AGENT_CONTEXT_TTL}s)")
            
            return jsonify({
                'success': True,
                'message': 'Caller context stored successfully',
                'data': {
                    'conversation_id': conversation_id,
                    'caller_name': caller_name,
                    'caller_company': caller_company,
                    'caller_phone_number': caller_phone_number,
                    'ttl_seconds': AGENT_CONTEXT_TTL
                }
            }), 200
            
        except Exception as redis_error:
            print(f"[Agent Context] ❌ Failed to store in Redis: {redis_error}")
            return jsonify({
                'error': 'Cache storage failed',
                'message': str(redis_error)
            }), 500
        
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/api/health', methods=['GET'])
def api_health_check():
    """
    Simple health check endpoint (no authentication required)
    """
    return jsonify({
        'status': 'healthy',
        'service': 'speako-celery-api',
        'timestamp': os.popen('date').read().strip()
    }), 200


@app.route('/api/knowledge/analyze-knowledge-file', methods=['POST'])
@require_api_key
def api_upload_knowledge_file():
    """
    [aiknowledges] Upload a knowledge file to Cloudflare R2 OR provide a remote file_url for analysis.

    Accepts either:
    - multipart/form-data with fields: tenant_id, location_id, knowledge_type, file (binary)
    - JSON or form-data with fields: tenant_id, location_id, knowledge_type, file_url (string)
    - Optional: speako_task_id (string) for client correlation
    - Optional: tenantIntegrationParam (object) for integration metadata

    Constraints for file uploads:
    - Allowed types: .doc/.docx, .xls/.xlsx, .pdf, .csv, .txt
    - Max size: 5MB

    Upload path in bucket: knowledges/{tenant_id}/{location_id}/
    """
    try:
        # Gather inputs from form or JSON
        data_json = request.get_json(silent=True) or {}
        file_url = request.form.get('file_url') or data_json.get('file_url')
        speako_task_id = request.form.get('speako_task_id') or data_json.get('speako_task_id')
        
        # Extract tenantIntegrationParam (only available in JSON mode)
        tenant_integration_param = data_json.get('tenantIntegrationParam')

        tenant_id = request.form.get('tenant_id') or data_json.get('tenant_id')
        location_id = request.form.get('location_id') or data_json.get('location_id')
        knowledge_type = request.form.get('knowledge_type') or data_json.get('knowledge_type')

        # Validate required fields
        missing = [k for k, v in {
            'tenant_id': tenant_id,
            'location_id': location_id,
            'knowledge_type': knowledge_type,
        }.items() if not v]
        if missing:
            return jsonify({'error': 'Missing required fields', 'missing_fields': missing}), 400

        # Validate knowledge_type
        valid_types = ['business_info', 'service_menu', 'food_menu', 'locations', 'staff', 'faq', 'service_policy', 'special_promotion', 'custom_message']
        if knowledge_type not in valid_types:
            return jsonify({
                'error': 'Invalid knowledge_type',
                'message': f'knowledge_type must be one of: {", ".join(valid_types)}',
                'provided': knowledge_type
            }), 400

        # Branch: file_url mode (skip R2 upload)
        if file_url:
            parsed = urlparse(file_url)
            ext = os.path.splitext(parsed.path.lower())[1]

            # Optional: enforce allowed extensions when present in URL
            if ext and not allowed_knowledge_file(f"dummy{ext}"):
                return jsonify({'error': 'Unsupported file type', 'message': 'Allowed: .doc, .docx, .xls, .xlsx, .pdf, .csv, .txt', 'provided_ext': ext}), 400

            # Guess content type and extension when missing
            guessed_ct, _ = mimetypes.guess_type(parsed.path)
            content_type = data_json.get('content_type') or guessed_ct or 'application/octet-stream'
            if not ext:
                guessed_ext = mimetypes.guess_extension(content_type) or '.bin'
                ext = guessed_ext

            unique_filename = generate_knowledge_filename(tenant_id, location_id, knowledge_type, ext)
            key = f"knowledges/{tenant_id}/{location_id}/{unique_filename}"

            response_data = {
                'tenant_id': tenant_id,
                'location_id': location_id,
                'knowledge_type': knowledge_type,
                'filename': unique_filename,
                'key': key,
                'url': file_url,
                'size': None,
                'content_type': content_type,
                'source': 'file_url',
                **({'speako_task_id': speako_task_id} if speako_task_id else {})
            }

            # Enqueue background analysis task using the remote URL
            try:
                task = analyze_knowledge_file.delay(
                    tenant_id=tenant_id,
                    location_id=location_id,
                    knowledge_type=knowledge_type,
                    key=key,
                    unique_filename=unique_filename,
                    content_type=content_type,
                    public_url=file_url,
                    file_url=file_url,
                    speako_task_id=speako_task_id,
                    tenant_integration_param=tenant_integration_param,
                )
                response_data['analysis'] = {
                    'status': 'queued',
                    'mode': 'background',
                    'celery_task_id': task.id
                }
            except Exception as ae:
                app.logger.error(f"Failed to enqueue analysis task (file_url mode): {ae}")
                response_data['analysis'] = {
                    'status': 'error',
                    'message': 'Failed to enqueue analysis task',
                    'detail': str(ae)
                }

            return jsonify({
                'success': True,
                'message': 'Knowledge analysis started from remote URL',
                'data': response_data
            }), 202

        # Otherwise: file upload mode (requires R2)
        if not r2_client:
            return jsonify({'error': 'Storage not configured', 'message': 'Cloudflare R2 credentials are missing'}), 500

        # Validate file presence
        if 'file' not in request.files:
            return jsonify({'error': 'File is required', 'message': 'No file part in the request'}), 400
        file = request.files['file']

        if file.filename == '':
            return jsonify({'error': 'Invalid file', 'message': 'No selected file'}), 400

        # Validate extension
        if not allowed_knowledge_file(file.filename):
            return jsonify({'error': 'Unsupported file type', 'message': 'Allowed: .doc, .docx, .xls, .xlsx, .pdf, .csv, .txt'}), 400

        # Enforce 5MB limit
        try:
            file.stream.seek(0, os.SEEK_END)
            size = file.stream.tell()
            file.stream.seek(0)
        except Exception:
            # Fallback: read into memory
            data_peek = file.read()
            size = len(data_peek)
            file.stream.seek(0)
        if size > 5 * 1024 * 1024:
            return jsonify({'error': 'File too large', 'message': 'File size must be less than or equal to 5MB'}), 400

        # Prepare upload
        file_extension = os.path.splitext(secure_filename(file.filename))[1].lower()
        unique_filename = generate_knowledge_filename(tenant_id, location_id, knowledge_type, file_extension)
        key = f"knowledges/{tenant_id}/{location_id}/{unique_filename}"

        file_content = file.read()
        content_type = file.mimetype or 'application/octet-stream'

        # Upload to R2
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=key,
            Body=file_content,
            ContentType=content_type,
            Metadata={
                'original_filename': file.filename,
                'upload_timestamp': datetime.utcnow().isoformat() + 'Z',
                'tenant_id': str(tenant_id),
                'location_id': str(location_id),
                'knowledge_type': knowledge_type,
                'group': 'aiknowledges'
            }
        )

        public_url = f"{R2_PUBLIC_BASE_URL}/{key}"

        response_data = {
            'tenant_id': tenant_id,
            'location_id': location_id,
            'knowledge_type': knowledge_type,
            'filename': unique_filename,
            'key': key,
            'url': public_url,
            'size': len(file_content),
            'content_type': content_type,
            'source': 'upload',
            **({'speako_task_id': speako_task_id} if speako_task_id else {})
        }

        # Enqueue background analysis task
        try:
            task = analyze_knowledge_file.delay(
                tenant_id=tenant_id,
                location_id=location_id,
                knowledge_type=knowledge_type,
                key=key,
                unique_filename=unique_filename,
                content_type=content_type,
                public_url=public_url,
                speako_task_id=speako_task_id,
                tenant_integration_param=tenant_integration_param,
            )
            response_data['analysis'] = {
                'status': 'queued',
                'mode': 'background',
                'celery_task_id': task.id
            }
        except Exception as ae:
            # If enqueue fails, return upload success but analysis enqueue error
            app.logger.error(f"Failed to enqueue analysis task: {ae}")
            response_data['analysis'] = {
                'status': 'error',
                'message': 'Failed to enqueue analysis task',
                'detail': str(ae)
            }

        return jsonify({
            'success': True,
            'message': 'Knowledge file uploaded successfully',
            'data': response_data
        }), 201

    except Exception as e:
        return jsonify({'error': 'Internal server error', 'message': str(e)}), 500


# =============================================================================
# AVATAR API ENDPOINTS
# =============================================================================

@app.route('/api/avatars', methods=['GET'])
@require_api_key
def api_get_avatars():
    """
    Get all avatars or search with filters
    
    Query parameters:
    - gender: Filter by gender (male, female, non-binary)
    - race: Filter by race (caucasian, black, asian, hispanic, etc.)
    - occupation: Filter by occupation (doctor, engineer, teacher, etc.)
    - style: Filter by style (professional, casual, cartoon, etc.)
    - age_group: Filter by age group (young, middle-aged, senior)
    - search: Text search across all fields
    - tags: Comma-separated list of tags
    - page: Page number (default: 1, use 0 or 'all' to get all results)
    - per_page: Items per page (default: 20, max: 100, use 'all' to get all results)
    - all: Set to 'true' to bypass pagination and get all results
    """
    try:
        # Get query parameters
        filters = {}
        
        # Single value filters
        for param in ['gender', 'race', 'occupation', 'style', 'age_group', 'search']:
            value = request.args.get(param)
            if value:
                filters[param] = value
        
        # Multi-value filters (comma-separated)
        tags = request.args.get('tags')
        if tags:
            filters['tags'] = [tag.strip() for tag in tags.split(',')]
        
        # Check if user wants all results (bypass pagination)
        all_results = request.args.get('all', '').lower() == 'true'
        page_param = request.args.get('page', '1')
        per_page_param = request.args.get('per_page', '20')
        
        # Handle special cases for getting all results
        if all_results or page_param == '0' or page_param.lower() == 'all' or per_page_param.lower() == 'all':
            # Get results without pagination
            if filters:
                result = avatar_api.search_avatars(filters)
            else:
                result = avatar_api.get_all_avatars()
            
            # Add pagination info indicating no pagination was applied
            result["pagination"] = {
                "page": "all",
                "per_page": "all",
                "total": result["total"],
                "pages": 1,
                "paginated": False,
                "message": "All results returned (pagination bypassed)"
            }
            
            return jsonify(result), 200
        
        # Normal pagination
        try:
            page = int(page_param)
            per_page = int(per_page_param)
        except ValueError:
            return jsonify({
                'error': 'Invalid pagination parameters',
                'message': 'page and per_page must be integers, or use "all" to get all results'
            }), 400
        
        # Validate pagination parameters
        if page < 1:
            page = 1
        if per_page < 1:
            per_page = 20
        if per_page > 100:
            per_page = 100
        
        # Get results
        if filters:
            result = avatar_api.search_avatars(filters)
        else:
            result = avatar_api.get_all_avatars()
        
        # Apply pagination
        total = result["total"]
        total_pages = (total + per_page - 1) // per_page
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        
        # Check if page number is valid
        if page > total_pages and total > 0:
            return jsonify({
                'error': 'Page not found',
                'message': f'Page {page} does not exist. Total pages: {total_pages}',
                'total_pages': total_pages,
                'total_results': total
            }), 404
        
        result["data"] = result["data"][start_idx:end_idx]
        result["pagination"] = {
            "page": page,
            "per_page": per_page,
            "total": total,
            "pages": total_pages,
            "paginated": True,
            "has_next": page < total_pages,
            "has_prev": page > 1,
            "next_page": page + 1 if page < total_pages else None,
            "prev_page": page - 1 if page > 1 else None,
            "start_index": start_idx + 1 if total > 0 else 0,
            "end_index": min(end_idx, total)
        }
        
        return jsonify(result), 200
        
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/api/avatars/<avatar_id>', methods=['GET'])
@require_api_key
def api_get_avatar(avatar_id):
    """
    Get specific avatar by ID
    
    Parameters:
    - avatar_id: The avatar ID (e.g., 'avatar_001')
    """
    try:
        result = avatar_api.get_avatar_by_id(avatar_id)
        
        if result["success"]:
            return jsonify(result), 200
        else:
            return jsonify(result), 404
            
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e),
            'avatar_id': avatar_id
        }), 500


@app.route('/api/avatars/stats', methods=['GET'])
@require_api_key
def api_get_avatar_statistics():
    """
    Get avatar statistics and breakdowns
    
    Returns counts by occupation, race, gender, style, age group
    """
    try:
        result = avatar_api.get_avatar_stats()
        return jsonify(result), 200
        
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/api/avatars/refresh', methods=['POST'])
@require_api_key
def api_refresh_avatar_catalog():
    """
    Refresh avatar catalog from file
    
    Use this endpoint to reload the catalog after updates
    """
    try:
        success = avatar_api.refresh_catalog()
        
        if success:
            return jsonify({
                "success": True,
                "message": "Avatar catalog refreshed successfully",
                "total_avatars": len(avatar_api.catalog.get("avatars", []))
            }), 200
        else:
            return jsonify({
                "success": False,
                "message": "Failed to refresh avatar catalog"
            }), 500
            
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/api/avatars/health', methods=['GET'])
def api_avatar_health_check():
    """
    Avatar API health check (no authentication required)
    """
    try:
        catalog_loaded = len(avatar_api.catalog.get("avatars", [])) > 0
        
        # Get sample avatar structure for debugging
        sample_avatar = None
        avatar_structure = None
        if catalog_loaded:
            avatars = avatar_api.catalog.get("avatars", [])
            if avatars:
                sample_avatar = avatars[0]
                avatar_structure = list(sample_avatar.keys())
        
        return jsonify({
            "success": True,
            "status": "healthy",
            "service": "avatar-api",
            "catalog_loaded": catalog_loaded,
            "total_avatars": len(avatar_api.catalog.get("avatars", [])),
            "catalog_file": avatar_api.catalog_file,
            "catalog_file_exists": os.path.exists(avatar_api.catalog_file),
            "sample_avatar_structure": avatar_structure,
            "is_simplified_format": avatar_structure == ['id', 'url', 'tags'] if avatar_structure else None,
            "working_directory": os.getcwd(),
            "metadata": avatar_api.catalog.get("metadata", {})
        }), 200
        
    except Exception as e:
        return jsonify({
            "success": False,
            "status": "unhealthy",
            "error": str(e)
        }), 500


@app.route('/api/avatars/debug', methods=['GET'])
def api_avatar_debug():
    """
    Debug endpoint to check avatar catalog loading (no authentication required)
    """
    try:
        debug_info = {
            "working_directory": os.getcwd(),
            "catalog_file_path": avatar_api.catalog_file,
            "absolute_catalog_path": os.path.abspath(avatar_api.catalog_file),
            "catalog_file_exists": os.path.exists(avatar_api.catalog_file),
            "total_avatars_loaded": len(avatar_api.catalog.get("avatars", [])),
            "metadata": avatar_api.catalog.get("metadata", {}),
            "files_in_speako_dashboard_avatar": []
        }
        
        # Check what files exist in the speako-dashboard-avatar directory
        avatar_dir = "speako-dashboard-avatar"
        if os.path.exists(avatar_dir):
            files = os.listdir(avatar_dir)
            catalog_files = [f for f in files if f.startswith('avatar_catalog') and f.endswith('.json')]
            debug_info["files_in_speako_dashboard_avatar"] = catalog_files
            
            # Get file sizes for catalog files
            debug_info["catalog_file_sizes"] = {}
            for file in catalog_files:
                file_path = os.path.join(avatar_dir, file)
                if os.path.exists(file_path):
                    debug_info["catalog_file_sizes"][file] = os.path.getsize(file_path)
        
        # Check avatar structure
        avatars = avatar_api.catalog.get("avatars", [])
        if avatars:
            first_avatar = avatars[0]
            debug_info["first_avatar_keys"] = list(first_avatar.keys())
            debug_info["is_simplified_format"] = set(first_avatar.keys()) == {'id', 'url', 'tags'}
            debug_info["first_avatar_sample"] = {
                "id": first_avatar.get("id"),
                "url": len(first_avatar.get("url", "")),
                "tags_count": len(first_avatar.get("tags", []))
            }
        
        return jsonify(debug_info), 200
        
    except Exception as e:
        return jsonify({
            "error": "Debug endpoint failed",
            "message": str(e)
        }), 500


def build_knowledge_prompt(knowledge_type: str) -> str:
    """Return a strict JSON-only extraction prompt per knowledge type."""
    if knowledge_type == 'menu':
        return (
            "You are given a document that may contain a service or food menu. "
            "Extract menu items into the following strict JSON schema. Output ONLY JSON, no prose.\n"
            "{\n"
            "  \"type\": \"menu\",\n"
            "  \"items\": [\n"
            "    {\n"
            "      \"name\": string,\n"
            "      \"description\": string|null,\n"
            "      \"prices\": [ { \"label\": string|null, \"amount\": number, \"currency\": string|null } ],\n"
            "      \"options\": [ { \"name\": string, \"price_delta\": number|null } ],\n"
            "      \"allergens\": [ string ]\n"
            "    }\n"
            "  ],\n"
            "  \"source_confidence\": number\n"
            "}"
        )
    if knowledge_type == 'faq':
        return (
            "You are given a document that may contain FAQ content. "
            "Extract FAQs into the following strict JSON schema. Output ONLY JSON.\n"
            "{\n"
            "  \"type\": \"faq\",\n"
            "  \"faqs\": [ { \"question\": string, \"answer\": string } ],\n"
            "  \"source_confidence\": number\n"
            "}"
        )
    if knowledge_type == 'events' or knowledge_type == 'event' or knowledge_type == 'promotions':
        return (
            "You are given a document that may contain information about events and/or promotions. "
            "Extract items into the following strict JSON schema. Output ONLY JSON. Do NOT include prose.\n"
            "{\n"
            "  \"type\": \"events_promotions\",\n"
            "  \"items\": [\n"
            "    {\n"
            "      \"title\": string,\n"
            "      \"category\": \"event\" | \"promotion\",\n"
            "      \"description\": string|null,\n"
            "      \"start_datetime\": string|null,  // ISO8601 if available, else null\n"
            "      \"end_datetime\": string|null,    // ISO8601 if available, else null\n"
            "      \"dates\": [ string ],            // optional list of ISO dates for multiple occurrences\n"
            "      \"recurrence\": { \"rule\": string|null, \"notes\": string|null } | null,\n"
            "      \"location\": { \"venue\": string|null, \"address\": string|null, \"city\": string|null },\n"
            "      \"price\": { \"amount\": number|null, \"currency\": string|null } | null,\n"
            "      \"promotion\": {\n"
            "         \"discount_type\": \"percentage\" | \"amount\" | null,\n"
            "         \"value\": number|null,\n"
            "         \"promo_code\": string|null,\n"
            "         \"conditions\": string|null,\n"
            "         \"valid_from\": string|null,   // ISO8601 date or datetime\n"
            "         \"valid_until\": string|null    // ISO8601 date or datetime\n"
            "      } | null,\n"
            "      \"audience\": string|null,\n"
            "      \"url\": string|null,\n"
            "      \"tags\": [ string ]\n"
            "    }\n"
            "  ],\n"
            "  \"source_confidence\": number\n"
            "}"
        )
    # policy
    return (
        "You are given a document that may contain policies and terms & conditions. "
        "Extract into the following strict JSON schema. Output ONLY JSON.\n"
        "{\n"
        "  \"type\": \"policy\",\n"
        "  \"policies\": [ { \"title\": string, \"body\": string } ],\n"
        "  \"terms\": [ { \"title\": string, \"body\": string } ],\n"
        "  \"source_confidence\": number\n"
        "}"
    )


@app.route('/api/knowledge/scrape-url', methods=['POST'])
@require_api_key
def api_scrape_url():
    """
    [aiknowledges] Scrape a public URL into LLM-ready Markdown and store artifacts in R2.

    Expected JSON payload:
    {
      "tenant_id": "123",           // required
      "location_id": "456",         // required
      "url": "https://...",         // required
      "pipeline": "markdown-only" | "analyze", // optional, default: markdown-only
      "knowledge_type": "menu|faq|policy|events", // required if pipeline=analyze
      "save_raw_html": false,          // optional
      "speako_task_id": "abc-123",    // optional correlation ID
      "tenantIntegrationParam": {...}  // optional integration metadata
    }

    Returns 202 with celery_task_id for polling at /api/task/<task_id>.
    """
    try:
        # Be tolerant of clients missing Content-Type or sending invalid JSON
        data = request.get_json(silent=True)
        if not data:
            return jsonify({
                'error': 'JSON payload required',
                'message': 'Send a valid JSON body with Content-Type: application/json',
                'content_type': request.headers.get('Content-Type', None)
            }), 400

        tenant_id = data.get('tenant_id')
        location_id = data.get('location_id')
        url = data.get('url')
        pipeline = data.get('pipeline', 'markdown-only')
        knowledge_type = data.get('knowledge_type')
        save_raw_html = bool(data.get('save_raw_html', False))
        speako_task_id = data.get('speako_task_id')
        tenant_integration_param = data.get('tenantIntegrationParam')

        missing = [k for k in ['tenant_id', 'location_id', 'url'] if not data.get(k)]
        if missing:
            return jsonify({'error': 'Missing required fields', 'missing_fields': missing}), 400

        if pipeline not in ['markdown-only', 'analyze']:
            return jsonify({'error': 'Invalid pipeline', 'message': 'pipeline must be markdown-only or analyze'}), 400

        if pipeline == 'analyze':
            valid_types = ['business_info', 'service_menu', 'food_menu', 'locations', 'staff', 'faq', 'service_policy', 'special_promotion', 'custom_message']
            if knowledge_type not in valid_types:
                return jsonify({
                    'error': 'Invalid or missing knowledge_type',
                    'message': f'knowledge_type required for analyze; one of: {", ".join(valid_types)}'
                }), 400

        # Enqueue scrape task
        task = scrape_url_to_markdown.delay(
            tenant_id=tenant_id,
            location_id=location_id,
            url=url,
            pipeline=pipeline,
            knowledge_type=knowledge_type,
            save_raw_html=save_raw_html,
            speako_task_id=speako_task_id,
            tenant_integration_param=tenant_integration_param,
        )

        # Align response shape with analyze_knowledge endpoint
        return jsonify({
            'success': True,
            'message': 'URL scrape task started',
            'data': {
                'analysis': {
                    'status': 'queued',
                    'mode': 'background',
                    'celery_task_id': task.id
                },
                'tenant_id': tenant_id,
                'location_id': location_id,
                'knowledge_type': knowledge_type,
                'url': url,
                'source': 'scrape_url',
                'pipeline': pipeline,
                **({'speako_task_id': speako_task_id} if speako_task_id else {})
            }
        }), 202

    except Exception as e:
        return jsonify({'error': 'Internal server error', 'message': str(e)}), 500


@app.route('/api/knowledge/sync-with-speako', methods=['POST'])
@require_api_key
def api_sync_with_speako():
    """
    [aiknowledges] Sync knowledge data directly from Speako's internal database.

    Expected JSON payload:
    {
      "tenant_id": "123",           // required
      "location_id": "456",         // required
      "knowledge_type": "business_info|service_menu|locations|staff", // required
      "speako_task_id": "abc-123",    // optional correlation ID
      "tenantIntegrationParam": {...}  // optional integration metadata
    }

    Note: This endpoint does NOT accept file uploads or URLs. It syncs data directly
    from Speako's internal database based on tenant_id and location_id.

    Returns 202 with celery_task_id for polling at /api/task/<task_id>.
    """
    try:
        # Be tolerant of clients missing Content-Type or sending invalid JSON
        data = request.get_json(silent=True)
        if not data:
            return jsonify({
                'error': 'JSON payload required',
                'message': 'Send a valid JSON body with Content-Type: application/json',
                'content_type': request.headers.get('Content-Type', None)
            }), 400

        tenant_id = data.get('tenant_id')
        location_id = data.get('location_id')
        knowledge_type = data.get('knowledge_type')
        speako_task_id = data.get('speako_task_id')
        tenant_integration_param = data.get('tenantIntegrationParam')

        # Validate required fields
        missing = [k for k in ['tenant_id', 'location_id', 'knowledge_type'] if not data.get(k)]
        if missing:
            return jsonify({'error': 'Missing required fields', 'missing_fields': missing}), 400

        # Validate knowledge_type - only allow sync-compatible types
        valid_types = ['business_info', 'service_menu', 'locations', 'staff']
        if knowledge_type not in valid_types:
            return jsonify({
                'error': 'Invalid knowledge_type',
                'message': f'knowledge_type must be one of: {", ".join(valid_types)}',
                'provided': knowledge_type
            }), 400

        # Enqueue sync task
        task = sync_speako_data.delay(
            tenant_id=tenant_id,
            location_id=location_id,
            knowledge_type=knowledge_type,
            speako_task_id=speako_task_id,
            tenant_integration_param=tenant_integration_param,
        )

        # Return response similar to other knowledge endpoints
        return jsonify({
            'success': True,
            'message': 'Speako data sync task started',
            'data': {
                'analysis': {
                    'status': 'queued',
                    'mode': 'background',
                    'celery_task_id': task.id
                },
                'tenant_id': tenant_id,
                'location_id': location_id,
                'knowledge_type': knowledge_type,
                'source': 'sync_speako_data',
                **({'speako_task_id': speako_task_id} if speako_task_id else {})
            }
        }), 202

    except Exception as e:
        return jsonify({'error': 'Internal server error', 'message': str(e)}), 500


@app.route('/api/agent/publish/add', methods=['POST'])
@require_api_key
def api_create_ai_agent():
    """
    [aiagent] Create ElevenLabs AI agent.

    Expected JSON payload:
    {
      "location_id": "123",                                // required
      "location_name": "Happy Sushi",                      // required
      "location_timezone": "Australia/Sydney",             // required
      "speako_task_id": "550e8400-e29b-41d4-a716-446655440000",  // optional correlation ID
    }

    Returns 202 with celery_task_id for polling at /api/task/<task_id>.
    """
    try:
        # Be tolerant of clients missing Content-Type or sending invalid JSON
        data = request.get_json(silent=True)
        if not data:
            return jsonify({
                'error': 'JSON payload required',
                'message': 'Send a valid JSON body with Content-Type: application/json',
                'content_type': request.headers.get('Content-Type', None)
            }), 400

        location_id = data.get('location_id')
        location_name = data.get('location_name')
        location_timezone = data.get('location_timezone')
        speako_task_id = data.get('speako_task_id')

        # Validate required fields
        missing = [k for k in ['location_id', 'location_name', 'location_timezone'] if not data.get(k)]
        if missing:
            return jsonify({'error': 'Missing required fields', 'missing_fields': missing}), 400

        # Enqueue create agent task
        task = create_conversation_ai_agent.delay(
            location_id=location_id,
            location_name=location_name,
            location_timezone=location_timezone,
        )

        # Return response similar to other agent endpoints
        return jsonify({
            'success': True,
            'message': 'ElevenLabs agent creation task started',
            'data': {
                'analysis': {
                    'status': 'queued',
                    'mode': 'background',
                    'celery_task_id': task.id
                },
                'location_id': location_id,
                'location_name': location_name,
                'location_timezone': location_timezone,
                'source': 'create_conversation_ai_agent',
                **({'speako_task_id': speako_task_id} if speako_task_id else {})
            }
        }), 202

    except Exception as e:
        return jsonify({'error': 'Internal server error', 'message': str(e)}), 500


@app.route('/api/agent/publish/update', methods=['POST'])
@require_api_key
def api_publish_elevenlabs_agent():
    """
    [aiagent] Publish ElevenLabs AI agent.

    Expected JSON payload:
    {
      "tenant_id": "1",                                    // required
      "location_id": "123",                                // required
      "publish_job_id": "42",                              // required
      "speako_task_id": "550e8400-e29b-41d4-a716-446655440000",  // optional correlation ID
      "tenantIntegrationParam": {...}                      // optional integration metadata
    }

    Returns 202 with celery_task_id for polling at /api/task/<task_id>.
    """
    try:
        # Be tolerant of clients missing Content-Type or sending invalid JSON
        data = request.get_json(silent=True)
        if not data:
            return jsonify({
                'error': 'JSON payload required',
                'message': 'Send a valid JSON body with Content-Type: application/json',
                'content_type': request.headers.get('Content-Type', None)
            }), 400

        tenant_id = data.get('tenant_id')
        location_id = data.get('location_id')
        publish_job_id = data.get('publish_job_id')
        speako_task_id = data.get('speako_task_id')
        tenant_integration_param = data.get('tenantIntegrationParam')

        # Validate required fields
        missing = [k for k in ['tenant_id', 'location_id', 'publish_job_id'] if not data.get(k)]
        if missing:
            return jsonify({'error': 'Missing required fields', 'missing_fields': missing}), 400

        # Enqueue publish task
        task = publish_elevenlabs_agent.delay(
            tenant_id=tenant_id,
            location_id=location_id,
            publish_job_id=publish_job_id,
            speako_task_id=speako_task_id,
            tenant_integration_param=tenant_integration_param,
        )

        # Return response similar to other knowledge endpoints
        return jsonify({
            'success': True,
            'message': 'ElevenLabs agent publish task started',
            'data': {
                'analysis': {
                    'status': 'queued',
                    'mode': 'background',
                    'celery_task_id': task.id
                },
                'tenant_id': tenant_id,
                'location_id': location_id,
                'publish_job_id': publish_job_id,
                'source': 'publish_elevenlabs_agent',
                **({'speako_task_id': speako_task_id} if speako_task_id else {})
            }
        }), 202

    except Exception as e:
        return jsonify({'error': 'Internal server error', 'message': str(e)}), 500


@app.route('/webhook/post_conversation', methods=['POST'])
def elevenlabs_post_conversation_webhook():
    """
    ElevenLabs Post-Conversation Webhook Endpoint
    
    This endpoint receives webhook notifications from ElevenLabs after a conversation ends.
    Processes audio upload and inserts conversation data into database.
    
    HMAC Authentication:
    - ElevenLabs signs the webhook payload with a secret key using HMAC-SHA256
    - The signature is sent in the 'elevenlabs-signature' header
    
    Setup:
    - Set ELEVENLABS_WEBHOOK_SECRET environment variable with the secret from ElevenLabs dashboard
    """
    
    # Get the raw request body for HMAC verification
    payload_bytes = request.get_data()
    
    # Get the signature from headers
    received_signature = request.headers.get('elevenlabs-signature', '')
    
    # Get the webhook secret from environment variable
    webhook_secret = os.getenv('ELEVENLABS_WEBHOOK_SECRET', '')
    
    # Log webhook received
    print("=" * 80)
    print("ELEVENLABS POST-CONVERSATION WEBHOOK RECEIVED")
    print("=" * 80)
    print(f"Timestamp: {datetime.utcnow().isoformat()}Z")
    print(f"Payload size: {len(payload_bytes)} bytes")
    
    # Pre-parse JSON to check for demo agent (before HMAC verification)
    # Demo agents bypass HMAC to allow testing without signature setup
    try:
        pre_parse_json = json.loads(payload_bytes.decode('utf-8'))
        pre_agent_id = pre_parse_json.get('data', {}).get('agent_id', '')
        is_demo_agent = pre_agent_id in DEMO_AGENT_NOTIFY_CONFIG
        if is_demo_agent:
            print(f"🎯 Demo agent detected: {pre_agent_id} - bypassing HMAC verification")
    except Exception:
        is_demo_agent = False
    
    # HMAC Verification (skip for demo agents)
    if not is_demo_agent and webhook_secret and received_signature:
        computed_signature = hmac.new(
            key=webhook_secret.encode('utf-8'),
            msg=payload_bytes,
            digestmod=hashlib.sha256
        ).hexdigest()
        
        signature_valid = hmac.compare_digest(computed_signature, received_signature)
        
        if not signature_valid:
            print(f"⚠️  HMAC SIGNATURE MISMATCH - Request rejected")
            print("=" * 80)
            return jsonify({
                'error': 'Invalid signature',
                'message': 'HMAC verification failed'
            }), 401
        
        print("✅ HMAC signature verified")
    elif is_demo_agent:
        print("⏭️  HMAC verification skipped (demo agent)")
    else:
        if not webhook_secret:
            print(f"⚠️  WARNING: ELEVENLABS_WEBHOOK_SECRET not set")
        if not received_signature:
            print(f"⚠️  WARNING: No signature received in headers")
    
    # Parse JSON payload
    try:
        payload_json = request.get_json(force=True)
    except Exception as e:
        print(f"❌ Failed to parse JSON: {e}")
        print("=" * 80)
        return jsonify({
            'error': 'Invalid JSON',
            'message': str(e)
        }), 400
    
    # Extract webhook data
    webhook_type = payload_json.get('type')
    event_timestamp = payload_json.get('event_timestamp')
    data = payload_json.get('data', {})
    
    agent_id = data.get('agent_id')
    conversation_id = data.get('conversation_id')
    full_audio_base64 = data.get('full_audio')
    
    print(f"Webhook type: {webhook_type}")
    print(f"Agent ID: {agent_id}")
    print(f"Conversation ID: {conversation_id}")
    print(f"Has audio: {bool(full_audio_base64)}")
    
    # Validate required fields
    if not agent_id or not conversation_id:
        print(f"❌ Missing required fields: agent_id={agent_id}, conversation_id={conversation_id}")
        print("=" * 80)
        return jsonify({
            'error': 'Missing required fields',
            'message': 'agent_id and conversation_id are required'
        }), 400
    
    # ==========================================================================
    # DEMO AGENT HANDLING - Skip DB, send notification only
    # ==========================================================================
    if agent_id in DEMO_AGENT_NOTIFY_CONFIG:
        agent_config = DEMO_AGENT_NOTIFY_CONFIG[agent_id]
        
        # Log entry into demo agent branch
        print("\n" + "=" * 80)
        print("🎯 DEMO AGENT BRANCH ENTERED")
        print("=" * 80)
        print(f"Timestamp: {datetime.utcnow().isoformat()}Z")
        print(f"Agent ID: {agent_id}")
        print(f"Agent Name: {agent_config.get('name')}")
        print(f"Conversation ID: {conversation_id}")
        print(f"Notify SMS: {agent_config.get('notify_sms')}")
        print(f"Notify Email: {agent_config.get('notify_email')}")
        print(f"Webhook Type: {webhook_type}")
        print(f"Event Timestamp: {event_timestamp}")
        print("-" * 80)
        print("⏭️  Skipping database recording - notification only mode")
        print("-" * 80)
        
        try:
            result = send_demo_agent_notification(conversation_id, agent_config)
            
            # Log success summary
            print("\n" + "-" * 80)
            print("📊 DEMO AGENT NOTIFICATION SUMMARY")
            print("-" * 80)
            print(f"Overall Success: {result.get('success')}")
            print(f"SMS Sent: {result.get('sms_sent')} → {agent_config.get('notify_sms')}")
            if result.get('sms_sid'):
                print(f"   SMS SID: {result.get('sms_sid')}")
            print(f"Email Sent: {result.get('email_sent')} → {agent_config.get('notify_email')}")
            if result.get('email_status_code'):
                print(f"   Email Status Code: {result.get('email_status_code')}")
            if result.get('errors'):
                print(f"Errors: {result.get('errors')}")
            print("=" * 80)
            print(f"✅ Demo agent webhook completed successfully")
            print("=" * 80 + "\n")
            
            return jsonify({
                'success': True,
                'message': 'Demo agent notification sent',
                'demo_agent': True,
                'agent_name': agent_config.get('name'),
                'conversation_id': conversation_id,
                'sms_sent': result.get('sms_sent', False),
                'email_sent': result.get('email_sent', False),
                'errors': result.get('errors', [])
            }), 200
            
        except Exception as e:
            # Log failure
            print("\n" + "-" * 80)
            print("❌ DEMO AGENT NOTIFICATION FAILED")
            print("-" * 80)
            print(f"Error: {e}")
            print(f"Error Type: {type(e).__name__}")
            import traceback
            traceback.print_exc()
            print("=" * 80 + "\n")
            
            # Return 200 to prevent retries
            return jsonify({
                'success': False,
                'message': 'Demo agent notification failed',
                'demo_agent': True,
                'conversation_id': conversation_id,
                'error': str(e)
            }), 200
    
    # ==========================================================================
    # NORMAL PROCESSING - Database recording, billing, etc.
    # ==========================================================================
    
    # Process webhook
    try:
        conn = get_db_connection()
        
        try:
            # Step 1: Lookup location information
            print(f"\n[Step 1] Looking up location for agent_id: {agent_id}")
            
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT tenant_id, location_id, name, timezone
                    FROM locations
                    WHERE elevenlabs_agent_id = %s
                    AND is_active = true
                    LIMIT 1
                """, (agent_id,))
                
                location_row = cur.fetchone()
            
            if not location_row:
                print(f"⚠️  No location found for agent_id: {agent_id}")
                print(f"⚠️  ORPHANED CONVERSATION: {conversation_id}")
                print(f"⚠️  This conversation cannot be inserted without location mapping")
                print("=" * 80)
                
                # Return 200 to prevent retries, but log critical error
                return jsonify({
                    'success': True,
                    'message': 'Webhook received but no location mapping found',
                    'warning': 'Orphaned conversation - needs manual intervention',
                    'conversation_id': conversation_id,
                    'agent_id': agent_id
                }), 200
            
            tenant_id, location_id, location_name, timezone_str = location_row
            print(f"✅ Found location: tenant_id={tenant_id}, location_id={location_id}, name={location_name}")
            
            # Step 2: Check for duplicate conversation (idempotency)
            print(f"\n[Step 2] Checking for duplicate conversation")
            
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT location_conversation_id, audio_r2_path
                    FROM location_conversations
                    WHERE eleven_conversation_id = %s
                """, (conversation_id,))
                
                existing_row = cur.fetchone()
            
            if existing_row:
                existing_id, existing_audio = existing_row
                print(f"✅ Conversation already exists: location_conversation_id={existing_id}")
                
                if existing_audio:
                    print(f"✅ Audio already uploaded: {existing_audio}")
                    print("=" * 80)
                    return jsonify({
                        'success': True,
                        'message': 'Conversation already processed',
                        'conversation_id': conversation_id,
                        'location_conversation_id': existing_id
                    }), 200
                else:
                    print(f"⚠️  Audio missing, will attempt to upload")
            
            # Step 3: Fetch full conversation details from ElevenLabs API
            print(f"\n[Step 3] Fetching full conversation details from ElevenLabs API")
            
            details = None
            try:
                details = get_conversation_details(conversation_id)
                print(f"✅ Retrieved full conversation details from API")
            except Exception as e:
                print(f"⚠️  Failed to fetch conversation details from API: {e}")
                print(f"⚠️  Will use minimal webhook data only")
            
            # Step 4: Decode and validate audio
            audio_bytes = None
            audio_r2_path = None
            
            if full_audio_base64:
                print(f"\n[Step 4] Decoding base64 audio")
                
                try:
                    audio_bytes = base64.b64decode(full_audio_base64)
                    audio_size = len(audio_bytes)
                    print(f"✅ Decoded audio: {audio_size} bytes ({audio_size / 1024 / 1024:.2f} MB)")
                    
                    # Validate size
                    if audio_size > WEBHOOK_MAX_AUDIO_SIZE:
                        print(f"⚠️  Audio size exceeds limit: {audio_size} > {WEBHOOK_MAX_AUDIO_SIZE}")
                        audio_bytes = None
                    
                except Exception as e:
                    print(f"⚠️  Failed to decode audio: {e}")
                    audio_bytes = None
            else:
                print(f"\n[Step 4] No audio data in webhook")
            
            # Step 5: Upload audio to R2
            if audio_bytes:
                print(f"\n[Step 5] Uploading audio to R2")
                
                try:
                    r2_key, public_url = upload_audio_to_r2(
                        str(tenant_id),
                        str(location_id),
                        conversation_id,
                        audio_bytes,
                        content_type='audio/mpeg'
                    )
                    
                    audio_r2_path = public_url  # Use full URL with CDN base
                    print(f"✅ Audio uploaded to R2: {public_url}")
                    
                except Exception as e:
                    print(f"⚠️  Failed to upload audio to R2: {e}")
                    audio_r2_path = None
            else:
                print(f"\n[Step 5] Skipping audio upload (no valid audio data)")
            
            # Step 6: If conversation exists, just update audio path
            if existing_row:
                if audio_r2_path:
                    print(f"\n[Step 6] Updating audio path for existing conversation")
                    
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE location_conversations
                            SET audio_r2_path = %s, updated_at = CURRENT_TIMESTAMP
                            WHERE location_conversation_id = %s
                        """, (audio_r2_path, existing_id))
                    
                    conn.commit()
                    print(f"✅ Updated audio path")
                
                print("=" * 80)
                return jsonify({
                    'success': True,
                    'message': 'Conversation updated with audio',
                    'conversation_id': conversation_id,
                    'location_conversation_id': existing_id
                }), 200
            
            # Step 7: Insert new conversation record
            print(f"\n[Step 6] Inserting conversation into database")
            
            # Helper function to convert timestamp to location timezone
            def convert_timestamp(unix_ts):
                if unix_ts is None:
                    return None
                try:
                    utc_dt = datetime.fromtimestamp(unix_ts, tz=ZoneInfo('UTC'))
                    local_dt = utc_dt.astimezone(ZoneInfo(timezone_str))
                    return local_dt.replace(tzinfo=None)
                except Exception:
                    return None
            
            # Extract fields from API details or use webhook fallbacks
            if details:
                metadata = details.get('metadata', {})
                transcript = details.get('transcript', [])
                
                agent_name = details.get('agent_name') or location_name
                call_start_time = convert_timestamp(metadata.get('start_time_unix_secs'))
                call_accepted_time = convert_timestamp(metadata.get('end_time_unix_secs'))
                call_duration_secs = metadata.get('call_duration_secs')
                message_count = len(transcript) if transcript else 0
                status = details.get('status', 'completed')
                
                call_successful_str = details.get('call_successful')
                if call_successful_str:
                    call_successful = (call_successful_str == 'success')
                else:
                    call_successful = (status in ['done', 'completed'])
                
                main_language = details.get('language') or details.get('detected_language')
                transcript_summary = (
                    details.get('transcript_summary') or
                    details.get('call_summary_title') or
                    (details.get('analysis', {}).get('summary') if isinstance(details.get('analysis'), dict) else None)
                )
                
                raw_metadata = json.dumps(details)
            else:
                # Fallback to minimal webhook data
                agent_name = location_name
                call_start_time = convert_timestamp(event_timestamp)
                call_accepted_time = None
                call_duration_secs = None
                message_count = 0
                status = 'webhook_only'
                call_successful = True
                main_language = None
                transcript_summary = None
                raw_metadata = json.dumps(payload_json)
                transcript = []
            
            # Insert conversation record
            location_conversation_id = None
            
            conn.rollback()  # Start fresh transaction
            
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO location_conversations (
                            tenant_id, location_id, eleven_conversation_id, eleven_agent_id,
                            agent_name, call_start_time, call_accepted_time, call_duration_secs,
                            message_count, status, call_successful, main_language,
                            transcript_summary, audio_r2_path, raw_metadata
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        RETURNING location_conversation_id
                    """, (
                        tenant_id, location_id, conversation_id, agent_id,
                        agent_name, call_start_time, call_accepted_time, call_duration_secs,
                        message_count, status, call_successful, main_language,
                        transcript_summary, audio_r2_path, raw_metadata
                    ))
                    
                    location_conversation_id = cur.fetchone()[0]
                
                print(f"✅ Inserted conversation: location_conversation_id={location_conversation_id}")
                
                # Insert transcript details if available
                if transcript and location_conversation_id:
                    print(f"[Step 7] Inserting {len(transcript)} transcript messages")
                    
                    with conn.cursor() as cur:
                        for idx, message in enumerate(transcript):
                            role = message.get('role', 'unknown')
                            time_in_call_secs = message.get('time_in_call_secs') or message.get('timestamp')
                            message_text = message.get('message') or message.get('text') or message.get('content')
                            
                            tool_calls = json.dumps(message.get('tool_calls')) if message.get('tool_calls') else None
                            tool_results = json.dumps(message.get('tool_results')) if message.get('tool_results') else None
                            llm_override = message.get('llm_override')
                            
                            conversation_turn_metrics = message.get('metrics') or message.get('turn_metrics')
                            if conversation_turn_metrics:
                                conversation_turn_metrics = json.dumps(conversation_turn_metrics)
                            
                            rag_retrieval_info = message.get('rag_info') or message.get('rag_retrieval')
                            if rag_retrieval_info:
                                rag_retrieval_info = json.dumps(rag_retrieval_info)
                            
                            cur.execute("""
                                INSERT INTO location_conversation_details (
                                    location_conversation_id, message_index, role, time_in_call_secs,
                                    message, tool_calls, tool_results, llm_override,
                                    conversation_turn_metrics, rag_retrieval_info
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (
                                location_conversation_id, idx, role, time_in_call_secs,
                                message_text, tool_calls, tool_results, llm_override,
                                conversation_turn_metrics, rag_retrieval_info
                            ))
                    
                    print(f"✅ Inserted {len(transcript)} transcript messages")
                
                # Step 8: Process billing (post-call usage recording)
                print(f"\n[Step 8] Processing billing")
                
                # Normalize call duration to an integer number of seconds
                call_seconds = None
                if call_duration_secs is not None:
                    try:
                        # Handle int, float, or string values consistently
                        call_seconds = int(float(call_duration_secs))
                    except (TypeError, ValueError):
                        call_seconds = None
                
                if not call_seconds or call_seconds <= 0:
                    print(f"[Billing] Skipping billing: no valid call_duration_secs for conversation {conversation_id}")
                else:
                    # Check if this conversation was already billed (idempotency)
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT 1 
                            FROM billing_minute_ledger
                            WHERE tenant_id = %s 
                              AND location_conversation_id = %s 
                              AND source = 'call_usage'
                            LIMIT 1
                        """, (tenant_id, location_conversation_id))
                        
                        already_billed = cur.fetchone()
                    
                    if already_billed:
                        print(f"[Billing] Skipping billing: call_usage already recorded for location_conversation_id={location_conversation_id}")
                    else:
                        # Query current balances
                        with conn.cursor() as cur:
                            cur.execute("""
                                SELECT plan_seconds_balance, package_seconds_balance
                                FROM tenant_total_seconds_balance
                                WHERE tenant_id = %s
                            """, (tenant_id,))
                            
                            balance_row = cur.fetchone()
                        
                        if balance_row:
                            plan_balance, package_balance = balance_row
                        else:
                            plan_balance, package_balance = 0, 0
                            print(f"[Billing] No balance row for tenant {tenant_id}, using plan=0, package=0")
                        
                        # Ensure non-negative
                        plan_balance = max(plan_balance or 0, 0)
                        package_balance = max(package_balance or 0, 0)
                        
                        print(f"[Billing] Tenant {tenant_id} balances: plan={plan_balance}s, package={package_balance}s")
                        
                        # Split usage: consume plan pool first, then package pool
                        call_secs = call_seconds
                        
                        # Consume plan pool first
                        plan_use = min(call_secs, plan_balance)
                        remaining = call_secs - plan_use
                        
                        # Consume package pool second
                        package_use = min(remaining, package_balance)
                        
                        # Calculate unbilled leftover
                        leftover = call_secs - plan_use - package_use
                        
                        print(f"[Billing] Tenant {tenant_id}, conv {location_conversation_id}: call={call_secs}s, plan_use={plan_use}s, package_use={package_use}s, leftover={leftover}s")
                        
                        # Insert call_usage rows into billing_minute_ledger
                        with conn.cursor() as cur:
                            # Insert plan usage (if any)
                            if plan_use > 0:
                                cur.execute("""
                                    INSERT INTO billing_minute_ledger (
                                        tenant_id,
                                        location_conversation_id,
                                        source,
                                        usage_bucket,
                                        seconds_delta
                                    ) VALUES (%s, %s, 'call_usage', 'plan', %s)
                                """, (tenant_id, location_conversation_id, -plan_use))
                                
                                print(f"[Billing] Inserted plan usage: -{plan_use}s")
                            
                            # Insert package usage (if any)
                            if package_use > 0:
                                cur.execute("""
                                    INSERT INTO billing_minute_ledger (
                                        tenant_id,
                                        location_conversation_id,
                                        source,
                                        usage_bucket,
                                        seconds_delta
                                    ) VALUES (%s, %s, 'call_usage', 'package', %s)
                                """, (tenant_id, location_conversation_id, -package_use))
                                
                                print(f"[Billing] Inserted package usage: -{package_use}s")
                            
                            # Insert overage debt if leftover exists
                            if leftover > 0:
                                cur.execute("""
                                    INSERT INTO billing_minute_ledger (
                                        tenant_id,
                                        location_conversation_id,
                                        source,
                                        usage_bucket,
                                        seconds_delta
                                    ) VALUES (%s, %s, 'call_usage_overage', NULL, %s)
                                """, (tenant_id, location_conversation_id, -leftover))
                                
                                print(f"[Billing] Inserted overage usage: -{leftover}s")
                        
                        print(f"✅ Billing processed successfully")
                
                # Step 9: Fire-and-forget usage notification check
                # This runs before commit so notification is included in same transaction
                try:
                    trigger_usage_notification(tenant_id, conn)
                except Exception as notif_err:
                    print(f"⚠️  [Notification] Error checking usage notification (ignored): {notif_err}")
                
                # Commit transaction
                conn.commit()
                print(f"✅ Transaction committed successfully")
                
            except Exception as e:
                conn.rollback()
                print(f"❌ Database insert failed: {e}")
                import traceback
                traceback.print_exc()
                raise
            
            print("=" * 80)
            print(f"✅ Webhook processed successfully")
            print(f"   Conversation ID: {conversation_id}")
            print(f"   Location Conversation ID: {location_conversation_id}")
            print(f"   Audio uploaded: {bool(audio_r2_path)}")
            print(f"   Transcript messages: {len(transcript) if transcript else 0}")
            print("=" * 80)
            
            return jsonify({
                'success': True,
                'message': 'Conversation processed successfully',
                'conversation_id': conversation_id,
                'location_conversation_id': location_conversation_id,
                'audio_uploaded': bool(audio_r2_path),
                'transcript_messages': len(transcript) if transcript else 0
            }), 200
            
        finally:
            conn.close()
            
    except Exception as e:
        print(f"❌ Fatal error processing webhook: {e}")
        import traceback
        traceback.print_exc()
        print("=" * 80)
        
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500
