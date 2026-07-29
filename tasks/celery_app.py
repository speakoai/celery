from celery import Celery
import os
from dotenv import load_dotenv

load_dotenv()

app = Celery('myapp', broker=os.getenv('CELERY_BROKER_URL'))

# Uncomment the lines below to add result backend and persistence configuration
# app = Celery('myapp',
#             broker=os.getenv('CELERY_BROKER_URL'),
#             backend=os.getenv('CELERY_BROKER_URL'))

# Enable a result backend so task states (e.g., STARTED) are persisted
_result_backend = os.getenv('CELERY_BROKER_URL')
if _result_backend:
	app.conf.update(result_backend=_result_backend)

# Track when tasks start so polling can show STARTED (not only PENDING)
app.conf.update(task_track_started=True)

# Optionally emit task events for Flower/monitoring
app.conf.update(worker_send_task_events=True, task_send_sent_event=True)

# ── Worker memory hygiene (added 2026-07-29) ─────────────────────────────────
# This is a single monolithic worker that imports EVERY task module below
# (playwright, pandas, OpenAI, agent-publishing, scraping, ...), so each prefork
# child carries a heavy baseline. On the 512MB instance that let the worker OOM
# under a small burst of concurrent tasks (which silently dropped in-flight
# availability-regen tasks). These settings bound per-child memory and recycle
# children so RSS can't grow unbounded over the worker's lifetime. The durable
# fix (splitting heavy tasks onto a dedicated queue/worker) is sketched in
# docs/plans/celery-worker-memory-hardening.md.
app.conf.update(
    worker_max_tasks_per_child=50,        # recycle a forked child after 50 tasks
    worker_max_memory_per_child=200000,   # KB (~200MB): recycle a child once it exceeds this
    worker_prefetch_multiplier=1,         # fetch one task at a time so a slow/heavy task
                                          # can't hold light tasks prefetched alongside it
)
# NOTE: task_acks_late is intentionally NOT set globally — SMS and Twilio-purchase
# tasks are NOT idempotent and must never double-run after a worker restart. Late
# ack is applied PER-TASK only to the idempotent availability-regen tasks (see
# acks_late in tasks/availability_gen_regen.py) so those survive a restart instead
# of being silently lost.

# Configure Celery for better task persistence and monitoring
# app.conf.update(
#     result_expires=86400,  # Results expire after 24 hours
#     task_track_started=True,  # Track when tasks start
#     task_serializer='json',
#     accept_content=['json'],
#     result_serializer='json',
#     timezone='UTC',
#     enable_utc=True,
#     # Store task results in Redis with proper settings
#     result_backend_transport_options={
#         'retry_policy': {
#             'timeout': 5.0
#         }
#     },
#     # Enable task events for monitoring
#     worker_send_task_events=True,
#     task_send_sent_event=True,
# )

app.autodiscover_tasks(['tasks'])

# 👇 Add this line
import tasks.availability
import tasks.sms
import tasks.availability_gen_regen
import tasks.analyze_knowledge
import tasks.scrape_url
import tasks.scrape_business_profile
import tasks.sync_speako_data
import tasks.publish_elevenlabs_agent
import tasks.sync_elevenlabs_conversations
import tasks.generate_dashboard_metrics
import tasks.create_ai_agent
import tasks.purchase_twilio_number
import tasks.update_twilio_friendly_name
import tasks.refresh_annual_minutes
import tasks.publish_native_agent
import tasks.retry_audio_upload
import tasks.provision_sip_location
import tasks.rebuild_knowledge_chunks
import tasks.embed_knowledge_param
import tasks.summarize_chat_sessions

# ── Heavy-task queue routing (added 2026-07-29) ──────────────────────────────
# OFF by default. When ENABLE_HEAVY_QUEUE is truthy, the memory-heavy /
# long-running tasks below are routed to a dedicated 'heavy' queue drained by a
# separate low-concurrency worker (see render.yaml: celery-worker-heavy*), so a
# heavy job can never OOM-kill or starve the booking-critical light path
# (availability regen, SMS, Twilio) that stays on the default queue.
#
# IMPORTANT: Celery resolves task_routes at SEND time, so EVERY producer that
# enqueues one of these tasks must also have ENABLE_HEAVY_QUEUE set for the
# route to take effect — that means this worker, the Flask app (app.py), AND the
# cron dispatchers. Enable the flag ONLY after the heavy worker is provisioned
# and consuming 'heavy'; otherwise routed tasks would strand in an unconsumed
# queue. Rollout runbook: docs/plans/celery-worker-memory-hardening.md.
_HEAVY_QUEUE_ENABLED = os.getenv('ENABLE_HEAVY_QUEUE', '').strip().lower() in ('1', 'true', 'yes', 'on')
_HEAVY_TASK_NAMES = [
    'tasks.scrape_url.scrape_url_to_markdown',
    'tasks.scrape_business_profile',
    'tasks.sync_speako_data.sync_speako_data',
    'tasks.rebuild_knowledge_chunks.rebuild_knowledge_chunks',
    'tasks.embed_knowledge_param.embed_knowledge_param',
    'tasks.generate_dashboard_metrics.generate_dashboard_metrics_for_tenant',
    'tasks.analyze_knowledge.analyze_knowledge_file',
    'tasks.sync_elevenlabs_conversations.sync_conversations_for_location',
    'tasks.publish_native_agent',
    'tasks.publish_elevenlabs_agent',
    'tasks.create_ai_agent.create_conversation_ai_agent',
    'tasks.retry_audio_upload',
]
if _HEAVY_QUEUE_ENABLED:
    app.conf.task_routes = {name: {'queue': 'heavy'} for name in _HEAVY_TASK_NAMES}
    # Loud-but-nonfatal: warn if a routed name isn't a registered task (typo/rename),
    # so a silent routing miss is visible in the worker log at startup.
    _unregistered = [n for n in _HEAVY_TASK_NAMES if n not in app.tasks]
    if _unregistered:
        import logging
        logging.getLogger(__name__).warning(
            "ENABLE_HEAVY_QUEUE on but %d routed name(s) are not registered tasks "
            "(their routing is a no-op): %s", len(_unregistered), _unregistered,
        )
    print(f"[celery_app] Heavy-queue routing ENABLED for {len(_HEAVY_TASK_NAMES)} task(s) -> queue 'heavy'")
else:
    print("[celery_app] Heavy-queue routing OFF (set ENABLE_HEAVY_QUEUE=true on all producers to enable)")