"""
Publish Native Agent Config Task

Composes a self-contained JSON blob for native providers (OpenAI Realtime,
Azure Voice Live) and writes it to locations.native_agent_config.

INDEPENDENCE GUARANTEE: This task reads ONLY from the shared source-of-truth DB
tables (tenant_integration_params, ai_tool_types, prompt_fragments, etc.) and
composes the system prompt from scratch.  It does NOT depend on the ElevenLabs
publish task having run first, does NOT read from tenant_ai_prompts, and does
NOT call any external API.  The two publish paths share DB tables but have
completely independent code paths.
"""

import hashlib
import json
import os
from datetime import datetime, timezone

from celery.utils.log import get_task_logger

from tasks.celery_app import app
from .utils.task_db import mark_task_running, mark_task_succeeded, mark_task_failed
from .utils.publish_db import (
    collect_full_agent_params,
    collect_tool_params,
    get_business_name,
    get_location_name,
    get_location_operation_hours,
    get_location_type,
    get_privacy_url,
    get_prompt_fragments,
    get_tool_prompt_template,
    get_tool_service_prompts,
)

logger = get_task_logger(__name__)

# Default OpenAI Realtime voice when the tenant hasn't picked one yet
DEFAULT_OPENAI_VOICE = os.getenv("DEFAULT_OPENAI_VOICE", "cedar")
DEFAULT_OPENAI_MODEL = os.getenv("OPENAI_REALTIME_MODEL", "gpt-realtime")
DEFAULT_TRANSCRIPTION_MODEL = os.getenv(
    "OPENAI_TRANSCRIPTION_MODEL", "gpt-4o-mini-transcribe"
)

# OpenAI-allowed bounds
TEMPERATURE_MIN, TEMPERATURE_MAX = 0.6, 1.2
SPEED_MIN, SPEED_MAX = 0.25, 1.5
CONFIG_SIZE_LIMIT = 256 * 1024  # 256 KB hard ceiling

OPENAI_VOICES = frozenset(
    ["alloy", "ash", "ballad", "coral", "echo", "sage", "shimmer", "verse", "marin", "cedar"]
)

# ── Tool schema registry ────────────────────────────────────────────
# Maps tool param_code (from ai_tool_types.key) to OpenAI function-tool
# schema.  Only tools listed here can appear in native_agent_config.
#
# This is the ONLY place that knows the OpenAI function schema for each
# tool.  If a new tool is added, its schema MUST be registered here.

_GENERIC_DATETIME_PROPS = {
    "dateTime": {
        "type": "string",
        "description": "Natural language date and time requested by the caller.",
    },
    "date": {
        "type": "string",
        "description": "Date or natural language date reference from the caller.",
    },
}


def _tool_schema_registry():
    """Return the full registry of tool_key → OpenAI function schema."""
    return {
        "search_knowledge": {
            "type": "function",
            "name": "search_knowledge",
            "description": (
                "Search the business knowledge base for information about menu items, "
                "prices, services, catalogs, or any detailed business information. "
                "Use this when the caller asks about specific items, prices, or details "
                "that are not in your general instructions. "
                "Before calling this tool, say: 'Let me look that up for you, one moment please.' "
                "After receiving the result, speak the spoken_reply naturally to the caller."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The search query — what the caller is asking about",
                    }
                },
                "required": ["query"],
                "additionalProperties": False,
            },
        },
        "end_call": {
            "type": "function",
            "name": "end_call",
            "description": (
                "End the phone call and hang up. Call this tool when the caller says "
                "goodbye, thanks bye, that's all, I'm done, I'm all good, or any "
                "similar closing phrase. After calling this tool, you will receive a "
                "spoken_reply in the result — you MUST speak that reply warmly and "
                "naturally to the caller as your farewell. Do NOT say just 'Goodbye' — "
                "always use the spoken_reply from the tool result. "
                "Do NOT ask 'is there anything else' after the caller has already "
                "indicated they are done."
            ),
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        "transfer_to_human": {
            "type": "function",
            "name": "transfer_to_human",
            "description": (
                "Transfer the caller to a human agent. Use when the caller explicitly "
                "asks for a person, is frustrated, asks for something out of scope, or "
                "the AI cannot complete the task safely."
            ),
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        "transfer_booking_call": {
            "type": "function",
            "name": "transfer_booking_call",
            "description": (
                "Transfer the caller to the booking team when the booking flow requires "
                "a human, such as special cases, repeated failures, or caller insistence."
            ),
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        "get_today_date": {
            "type": "function",
            "name": "get_today_date",
            "description": "Get today's date in the business timezone.",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        "check_latest_booking": {
            "type": "function",
            "name": "check_latest_booking",
            "description": (
                "Look up the caller's existing booking by phone number or booking "
                "reference before any modify or cancel action."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "bookingRef": {
                        "type": "string",
                        "description": "Booking reference provided by the caller.",
                    }
                },
                "additionalProperties": True,
            },
        },
        "modify_booking": {
            "type": "function",
            "name": "modify_booking",
            "description": (
                "Modify an existing booking only after the correct booking has been "
                "identified and the caller has clearly stated the new preferred date or time."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    **_GENERIC_DATETIME_PROPS,
                    "bookingRef": {
                        "type": "string",
                        "description": "Booking reference if the caller mentions it again.",
                    },
                },
                "additionalProperties": True,
            },
        },
        "cancel_booking": {
            "type": "function",
            "name": "cancel_booking",
            "description": (
                "Cancel the caller's booking only after the correct booking has been "
                "identified and the caller clearly confirms cancellation."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "bookingRef": {
                        "type": "string",
                        "description": "Booking reference if the caller provides it.",
                    }
                },
                "additionalProperties": True,
            },
        },
        "make_order": {
            "type": "function",
            "name": "make_order",
            "description": (
                "Use when the caller wants to place an order. Explains that direct "
                "ordering is unavailable and sets up the send_order_link flow."
            ),
            "parameters": {"type": "object", "properties": {}, "additionalProperties": True},
        },
        "send_order_link": {
            "type": "function",
            "name": "send_order_link",
            "description": "Send the online order link to the caller via SMS.",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": True},
        },
        "send_booking_link": {
            "type": "function",
            "name": "send_booking_link",
            "description": "Send the online booking link to the caller via SMS.",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": True},
        },
        "send_google_map_link": {
            "type": "function",
            "name": "send_google_map_link",
            "description": "Send the business location link to the caller via SMS.",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": True},
        },
        "send_ecommerce_link": {
            "type": "function",
            "name": "send_ecommerce_link",
            "description": "Send the ecommerce or shop link to the caller via SMS.",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": True},
        },
        # ── Restaurant-specific tools ──
        "check_availabilities": {
            "type": "function",
            "name": "check_availabilities",
            "description": (
                "Check restaurant table availability. Use after collecting enough "
                "information, especially date or time and party size."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    **_GENERIC_DATETIME_PROPS,
                    "partyNum": {
                        "type": "integer",
                        "description": "Number of diners in the party.",
                    },
                    "CustomerName": {
                        "type": "string",
                        "description": "Caller name if shared during the booking flow.",
                    },
                },
                "additionalProperties": True,
            },
        },
        "make_booking": {
            "type": "function",
            "name": "make_booking",
            "description": (
                "Create a restaurant booking only after a valid slot has been found "
                "and the caller is ready to confirm it."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    **_GENERIC_DATETIME_PROPS,
                    "partySize": {
                        "type": "integer",
                        "description": "Number of diners for the final booking.",
                    },
                    "CustomerName": {
                        "type": "string",
                        "description": "Caller name if provided.",
                    },
                },
                "additionalProperties": True,
            },
        },
        "check_modify_availabilities": {
            "type": "function",
            "name": "check_modify_availabilities",
            "description": (
                "Check alternative restaurant table times for an existing booking "
                "after the booking has already been identified."
            ),
            "parameters": {
                "type": "object",
                "properties": _GENERIC_DATETIME_PROPS,
                "additionalProperties": True,
            },
        },
        # ── Service-specific tools ──
        "check_availabilities_service": {
            "type": "function",
            "name": "check_availabilities_service",
            "description": (
                "Check service appointment availability. Use after collecting enough "
                "information such as date or time and, if available, service name or "
                "staff preference."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    **_GENERIC_DATETIME_PROPS,
                    "CustomerName": {
                        "type": "string",
                        "description": "Caller name if shared during the booking flow.",
                    },
                    "staffName": {
                        "type": "string",
                        "description": "Preferred staff member if the caller asks for one.",
                    },
                    "serviceName": {
                        "type": "string",
                        "description": "Requested service name.",
                    },
                },
                "additionalProperties": True,
            },
        },
        "make_booking_service": {
            "type": "function",
            "name": "make_booking_service",
            "description": (
                "Create a service appointment only after a valid slot has been found "
                "and the caller is ready to confirm it."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    **_GENERIC_DATETIME_PROPS,
                    "CustomerName": {
                        "type": "string",
                        "description": "Caller name if provided.",
                    },
                    "staffName": {
                        "type": "string",
                        "description": "Preferred staff member if selected.",
                    },
                    "serviceName": {
                        "type": "string",
                        "description": "Service name if selected.",
                    },
                },
                "additionalProperties": True,
            },
        },
        "check_modify_availabilities_service": {
            "type": "function",
            "name": "check_modify_availabilities_service",
            "description": (
                "Check alternative service appointment times for an existing booking "
                "after the booking has already been identified."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    **_GENERIC_DATETIME_PROPS,
                    "staffName": {
                        "type": "string",
                        "description": "Preferred staff member.",
                    },
                    "serviceName": {
                        "type": "string",
                        "description": "Requested service name.",
                    },
                },
                "additionalProperties": True,
            },
        },
    }


# ── Helpers ──────────────────────────────────────────────────────────


def _clamp(value, lo, hi):
    return max(lo, min(hi, value))


def _deterministic_hash(obj) -> str:
    """sha256 of a JSON-serialised object (sorted keys, no whitespace)."""
    raw = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return f"sha256:{hashlib.sha256(raw.encode()).hexdigest()}"


def _extract_voice_id(voice_dict_params: list, provider: str = "openai") -> str:
    """Extract voice ID from voice_dict_params, falling back to default per provider."""
    for p in voice_dict_params:
        if p.get("param_code") == "voice_id" and p.get("value_text"):
            v = p["value_text"].strip()
            if provider == "azure":
                # Azure voices are like "zh-HK-HiuMaan:DragonHDOmniLatestNeural"
                return v
            if v in OPENAI_VOICES:
                return v
            logger.warning(
                f"[publish_native] voice_id '{v}' is not a valid {provider} voice, using default"
            )
    return DEFAULT_OPENAI_VOICE


def _extract_speed(voice_dict_params: list) -> float:
    for p in voice_dict_params:
        if p.get("param_code") == "speed" and p.get("value_numeric") is not None:
            return _clamp(float(p["value_numeric"]), SPEED_MIN, SPEED_MAX)
    return 1.0


def _extract_temperature(personality_params: list) -> float:
    for p in personality_params:
        if p.get("param_code") == "temperature" and p.get("value_numeric") is not None:
            return _clamp(float(p["value_numeric"]), TEMPERATURE_MIN, TEMPERATURE_MAX)
    return 0.8


def _extract_greetings(greetings_params: list) -> dict:
    """Build the greeting variants dict from greeting params."""
    greetings = {
        "first_message": "",
        "first_message_after": "",
        "first_message_customer": "",
        "first_message_customer_after": "",
        "recording_disclosure": "",
        "variables_available": [
            "business_name",
            "location_name",
            "customer_name",
            "operation_hours",
        ],
    }
    code_map = {
        "initial_greeting": "first_message",
        "after_hours": "first_message_after",
        "return_customer": "first_message_customer",
        "return_customer_after_hours": "first_message_customer_after",
        "recording_disclosure": "recording_disclosure",
    }
    for p in greetings_params:
        key = code_map.get(p.get("param_code"))
        if key and p.get("value_text"):
            greetings[key] = p["value_text"]

    # Ensure a fallback first_message always exists
    if not greetings["first_message"]:
        greetings["first_message"] = (
            "Hi, thanks for calling {{business_name}}! How can I help you today?"
        )
    return greetings


def _extract_pronunciation_hints(dictionary_entry: dict | None) -> list:
    """Extract pronunciation rules from the dictionary entry."""
    if not dictionary_entry:
        return []
    value_json = dictionary_entry.get("value_json")
    if not value_json or not isinstance(value_json, dict):
        return []
    rules = value_json.get("rules", [])
    hints = []
    for r in rules:
        if isinstance(r, dict) and r.get("string_to_replace") and r.get("replacement"):
            hints.append({"from": r["string_to_replace"], "to": r["replacement"]})
    return hints


def _build_enabled_tools(tool_params: list) -> list:
    """
    Build the list of OpenAI function-tool objects for only the enabled tools.

    tool_params comes from collect_full_agent_params → tool_params partition,
    which filters for service='tool', provider='speako', status in ('configured','published').
    Each entry has param_code (= ai_tool_types.key) and value_json (= {enabled: bool}).
    """
    # Bundle param_codes map to multiple individual tools
    BUNDLE_MAP = {
        "booking_manager_rest": [
            "check_availabilities", "make_booking", "check_latest_booking",
            "check_modify_availabilities", "modify_booking", "cancel_booking",
        ],
        "booking_manager_service": [
            "check_availabilities_service", "make_booking_service", "check_latest_booking",
            "check_modify_availabilities_service", "modify_booking", "cancel_booking",
        ],
    }

    registry = _tool_schema_registry()
    tools = []
    enabled_keys = []

    for p in tool_params:
        param_code = p.get("param_code", "")
        value_json = p.get("value_json") or {}
        if not value_json.get("enabled", False):
            continue

        # Check if this is a bundle that expands to multiple tools
        if param_code in BUNDLE_MAP:
            for tool_key in BUNDLE_MAP[param_code]:
                if tool_key not in enabled_keys:
                    schema = registry.get(tool_key)
                    if schema:
                        tools.append(schema)
                        enabled_keys.append(tool_key)
            continue

        schema = registry.get(param_code)
        if schema:
            tools.append(schema)
            enabled_keys.append(param_code)
        else:
            logger.warning(
                f"[publish_openai] Enabled tool '{param_code}' has no OpenAI schema, skipping"
            )

    # Always include system-level tools that aren't tenant-configurable
    system_tools = ["end_call"]
    for st_key in system_tools:
        if st_key not in enabled_keys:
            schema = registry.get(st_key)
            if schema:
                tools.insert(0, schema)
                enabled_keys.insert(0, st_key)

    logger.info(f"[publish_openai] Enabled tools: {enabled_keys}")
    return tools, enabled_keys


def _collect_native_agent_params(tenant_id: str, location_id: str, provider: str = "openai") -> dict:
    """
    Collect all parameters needed for OpenAI agent publishing.

    This is the OpenAI-path equivalent of collect_full_agent_params() in
    publish_db.py, but WITHOUT requiring elevenlabs_agent_id.  It reads
    the same tenant_integration_params rows and partitions them identically.
    """
    import psycopg2
    from psycopg2.extras import RealDictCursor

    result = {
        "location": None,
        "greetings_params": [],
        "voice_dict_params": [],
        "language_config": None,
        "personality_params": [],
        "tool_params": [],
        "dictionary_entry": None,
    }

    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Query 1: Location info (no elevenlabs_agent_id requirement)
                cur.execute(
                    """
                    SELECT name, timezone, location_type
                    FROM locations
                    WHERE tenant_id = %s AND location_id = %s
                    """,
                    (tenant_id, location_id),
                )
                row = cur.fetchone()
                if not row:
                    raise ValueError(
                        f"Location not found: tenant_id={tenant_id}, location_id={location_id}"
                    )

                result["location"] = {
                    "name": row["name"] or f"Location {location_id}",
                    "timezone": row["timezone"] or "UTC",
                    "location_type": row.get("location_type") or "service",
                }

                # Query 2: All tenant_integration_params (same query as publish_db)
                cur.execute(
                    """
                    SELECT
                        param_id, provider, service, param_code,
                        value_text, value_json, value_numeric, status
                    FROM tenant_integration_params
                    WHERE tenant_id = %s
                      AND location_id = %s
                      AND status IN ('configured', 'published')
                      AND (
                        (provider = 'speako' AND service = 'greetings') OR
                        (service IN ('agents', 'turn', 'conversation', 'tts')) OR
                        (service = 'agents' AND provider = 'elevenlabs') OR
                        (service = 'agents' AND provider IN ('openai', 'azure')) OR
                        (service = 'tool' AND provider = 'speako') OR
                        (service = 'dictionary')
                      )
                    ORDER BY created_at ASC
                    """,
                    (tenant_id, location_id),
                )
                rows = cur.fetchall()

                for row in rows:
                    row_dict = dict(row)
                    row_provider = row_dict.get("provider")
                    service = row_dict.get("service")
                    param_code = row_dict.get("param_code")
                    status = row_dict.get("status")

                    if row_provider == "speako" and service == "greetings" and status == "configured":
                        result["greetings_params"].append(row_dict)
                    elif (
                        service == "agents"
                        and row_provider == provider
                        and param_code == "voice_id"
                    ):
                        # Voice_id for the target provider only
                        result["voice_dict_params"].append(row_dict)
                    elif (
                        service == "agents"
                        and row_provider == "elevenlabs"
                        and param_code
                        in ("traits", "tone_of_voice", "response_style", "temperature", "custom_instruction")
                        and status in ("configured", "published")
                    ):
                        result["personality_params"].append(row_dict)
                    elif service in ("turn", "conversation", "tts") and status == "configured":
                        result["voice_dict_params"].append(row_dict)
                    elif service == "tool" and row_provider == "speako":
                        result["tool_params"].append(row_dict)
                    elif service == "dictionary" and status == "configured" and result["dictionary_entry"] is None:
                        result["dictionary_entry"] = row_dict

                # Query 3: language_config for this provider
                cur.execute(
                    """
                    SELECT value_json
                    FROM tenant_integration_params
                    WHERE tenant_id = %s AND location_id = %s
                      AND param_code = 'language_config'
                      AND provider = %s
                      AND status IN ('configured', 'published')
                    LIMIT 1
                    """,
                    (tenant_id, location_id, provider),
                )
                lc_row = cur.fetchone()
                if lc_row and lc_row.get("value_json"):
                    result["language_config"] = lc_row["value_json"]

                logger.info(
                    f"[publish_native] Collected params: "
                    f"greetings={len(result['greetings_params'])}, "
                    f"voice_dict={len(result['voice_dict_params'])}, "
                    f"personality={len(result['personality_params'])}, "
                    f"tools={len(result['tool_params'])}, "
                    f"dictionary={'yes' if result['dictionary_entry'] else 'no'}, "
                    f"language_config={'yes' if result['language_config'] else 'no'}"
                )

                return result
    finally:
        conn.close()


def _resolve_variables(text: str, variables: dict) -> str:
    """Replace {{variable}} placeholders in text."""
    result = text
    for var_name, var_value in variables.items():
        placeholder = f"{{{{{var_name}}}}}"
        if placeholder in result:
            result = result.replace(placeholder, var_value or "")
    return result


def _ensure_fragments_and_compose(
    tenant_id: str,
    location_id: str,
    all_params: dict,
) -> tuple[str, float | None]:
    """
    Ensure prompt fragments exist in tenant_ai_prompts, then compose the
    system prompt by reading them back via compose_prompts_by_sort_order().

    This makes tenant_ai_prompts the SINGLE SOURCE OF TRUTH for both the
    ElevenLabs and OpenAI paths. The fragment-writing logic mirrors what
    publish_helpers.py publish_full_agent does (Steps 3–5), using the same
    upsert_ai_prompt calls and the same prompt_fragments templates.

    If fragments already exist (e.g., from a prior ElevenLabs publish),
    upsert_ai_prompt will deactivate the old version and insert a fresh one —
    this is safe and idempotent.

    Returns (composed_system_prompt, temperature_value).
    """
    from .utils.publish_helpers import get_human_friendly_operation_hours
    from .utils.publish_db import (
        upsert_ai_prompt,
        compose_prompts_by_sort_order,
        process_context_prompts,
    )

    greetings_params = all_params.get("greetings_params", [])
    personality_params = all_params.get("personality_params", [])
    temperature_value = None

    # ── 1. Context prompts (role, knowledge_scope) ──
    # process_context_prompts reads from tenant_integration_params and writes
    # to tenant_ai_prompts — shared by both paths.
    try:
        process_context_prompts(tenant_id=tenant_id, location_id=location_id)
    except Exception as e:
        logger.warning(f"[publish_openai] Context prompts processing failed: {e}")

    # ── 2. Greetings → write first_message prompt fragments ──
    if greetings_params:
        variables = {}
        schedule_json = get_location_operation_hours(tenant_id, location_id)
        variables["operation_hours"] = get_human_friendly_operation_hours(schedule_json)
        variables["business_name"] = get_business_name(tenant_id)
        variables["location_name"] = get_location_name(tenant_id, location_id)
        variables["privacy_url"] = get_privacy_url(tenant_id)

        greeting_map = {
            e["param_code"]: e["value_text"]
            for e in greetings_params
            if e.get("param_code")
        }
        if "agent_name" in greeting_map:
            variables["ai_agent_name"] = _resolve_variables(
                greeting_map["agent_name"], variables
            )
        if "after_hours" in greeting_map:
            variables["after_hours_message"] = _resolve_variables(
                greeting_map["after_hours"], variables
            )
        if "recording_disclosure" in greeting_map:
            variables["recording_disclosure"] = _resolve_variables(
                greeting_map["recording_disclosure"], variables
            )

        for entry in greetings_params:
            original_text = entry.get("value_text", "")
            param_code = entry.get("param_code", "")
            param_id = entry.get("param_id")
            if not original_text:
                continue

            resolved_text = _resolve_variables(original_text, variables)

            if param_code in ("initial_greeting", "return_customer"):
                if param_code == "initial_greeting":
                    base_type, base_name = "first_message", "Initial Greeting"
                    after_type, after_name = "first_message_after", "Initial Greeting With After Hour Message"
                else:
                    base_type, base_name = "first_message_customer", "Return Customer Greeting"
                    after_type, after_name = "first_message_customer_after", "Return Customer Greeting With After Hour Message"

                base_text = resolved_text.replace("{{after_hours_message}}", "").strip()
                try:
                    upsert_ai_prompt(
                        tenant_id=tenant_id, location_id=location_id,
                        type_code=base_type, title=base_name, name=base_name,
                        body_template=base_text,
                        metadata={"source_param_id": param_id, "param_code": param_code, "version": "base"},
                    )
                except Exception as e:
                    logger.error(f"[publish_openai] Error creating greeting {base_type}: {e}")

                after_text = resolved_text.replace(
                    "{{after_hours_message}}", variables.get("after_hours_message", "")
                ).strip()
                try:
                    upsert_ai_prompt(
                        tenant_id=tenant_id, location_id=location_id,
                        type_code=after_type, title=after_name, name=after_name,
                        body_template=after_text,
                        metadata={"source_param_id": param_id, "param_code": param_code, "version": "after_hours"},
                    )
                except Exception as e:
                    logger.error(f"[publish_openai] Error creating greeting {after_type}: {e}")

    # ── 3. Personality → write personality + custom_instruction fragments ──
    if personality_params:
        param_map = {}
        for param in personality_params:
            param_code = param.get("param_code")
            value_json = param.get("value_json")
            value_text = param.get("value_text")

            if param_code == "temperature":
                if param.get("value_numeric") is not None:
                    temperature_value = float(param["value_numeric"])
            elif param_code == "custom_instruction":
                param_map["custom_instruction"] = value_text or ""
            elif param_code in ("traits", "tone_of_voice"):
                if value_json and isinstance(value_json, list) and len(value_json) > 0:
                    param_map[param_code] = ", ".join(str(v) for v in value_json)
                else:
                    param_map[param_code] = ""
            elif param_code == "response_style":
                if value_json and isinstance(value_json, list) and len(value_json) > 0:
                    param_map["response_style"] = str(value_json[0])
                else:
                    param_map["response_style"] = ""

        fragments = get_prompt_fragments([
            "personality", "response_style_concise", "response_style_balanced",
            "response_style_detailed", "custom_instruction",
        ])

        # Personality template
        pf = fragments.get("personality", {})
        p_template = pf.get("template_text") or ""
        p_sort_order = pf.get("sort_order", 0)
        if p_template:
            p_template = p_template.replace("{{traits}}", param_map.get("traits") or "")
            p_template = p_template.replace("{{tone_of_voice}}", param_map.get("tone_of_voice") or "")
            rs_value = (param_map.get("response_style") or "").strip()
            rs_key = {
                "Concise": "response_style_concise",
                "Balanced": "response_style_balanced",
                "Detailed": "response_style_detailed",
            }.get(rs_value, "")
            rs_text = fragments.get(rs_key, {}).get("template_text") or "" if rs_key else ""
            p_template = p_template.replace("{{response_style}}", rs_text)

            try:
                upsert_ai_prompt(
                    tenant_id=tenant_id, location_id=location_id,
                    name="Personality", type_code="personality",
                    title="Agent Personality Configuration",
                    body_template=p_template, sort_order=p_sort_order,
                )
            except Exception as e:
                logger.error(f"[publish_openai] Error creating personality prompt: {e}")

        # Custom instruction
        ci_value = (param_map.get("custom_instruction") or "").strip()
        if ci_value:
            ci_frag = fragments.get("custom_instruction", {})
            ci_template = ci_frag.get("template_text") or ""
            ci_sort_order = ci_frag.get("sort_order", 0)
            if ci_template:
                ci_template = ci_template.replace("{{custom_instruction}}", ci_value)
                try:
                    upsert_ai_prompt(
                        tenant_id=tenant_id, location_id=location_id,
                        name="Custom Instruction", type_code="custom_instruction",
                        title="Custom Instruction",
                        body_template=ci_template, sort_order=ci_sort_order,
                    )
                except Exception as e:
                    logger.error(f"[publish_openai] Error creating custom instruction: {e}")

    # ── 4. Tools → write use_of_tools fragment ──
    try:
        tool_params_with_ids = collect_tool_params(tenant_id, location_id)
        all_tool_ids = []
        for param in tool_params_with_ids:
            pc = param.get("param_code", "")
            vj = param.get("value_json", {})
            if pc == "greetings":
                continue
            is_enabled = vj.get("enabled", False) if isinstance(vj, dict) else False
            if not is_enabled:
                continue
            tids = param.get("tool_ids", [])
            if tids:
                all_tool_ids.extend(tids)

        unique_tool_ids = list(set(all_tool_ids))
        if unique_tool_ids:
            location_type = get_location_type(tenant_id, location_id)
            template_data = get_tool_prompt_template()
            template = template_data["template_text"]
            template_sort_order = template_data["sort_order"]

            tool_prompts_data = get_tool_service_prompts(unique_tool_ids)
            extracted = []
            service_key = "rest" if location_type == "rest" else "service"
            for td in (tool_prompts_data or []):
                sp = td.get("service_prompts")
                if not sp:
                    continue
                for po in sp.get("by_service_type", {}).get(service_key, []):
                    md = po.get("markdown", "")
                    if md:
                        extracted.append(md)

            if extracted:
                all_tool_prompts = "\n\n".join(extracted).replace("\\n", "\n")
                final_prompt = f"{template}\n\n{all_tool_prompts}"
            else:
                final_prompt = template

            try:
                upsert_ai_prompt(
                    tenant_id=tenant_id, location_id=location_id,
                    name="Use of Tools", type_code="use_of_tools",
                    title="Use of Tools",
                    body_template=final_prompt, sort_order=template_sort_order,
                )
            except Exception as e:
                logger.error(f"[publish_openai] Error creating tools prompt: {e}")
    except Exception as e:
        logger.warning(f"[publish_openai] Tools prompt composition failed: {e}")

    # ── 5. Read the composed system prompt from tenant_ai_prompts ──
    # This is the single source of truth — all fragments written above
    # (and any written by prior ElevenLabs publishes) are joined here.
    composed = compose_prompts_by_sort_order(tenant_id, location_id)

    if not composed:
        logger.warning(
            f"[publish_openai] Composed system prompt is empty for "
            f"tenant_id={tenant_id}, location_id={location_id}"
        )

    # ── 6. Collect and inline knowledge (food_menu, service_menu, etc.) ──
    # In ElevenLabs, these are uploaded as RAG docs. For OpenAI Realtime
    # (which has no built-in RAG), we inline them into the system prompt
    # if total size < 30KB. Larger docs will use a vector store (Tier 2).
    knowledge_sections = []
    knowledge_total_size = 0
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor

        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT param_code, value_text
                    FROM tenant_integration_params
                    WHERE tenant_id = %s
                      AND location_id = %s
                      AND service = 'knowledge'
                      AND provider = 'speako'
                      AND status IN ('configured', 'published')
                      AND value_text IS NOT NULL
                      AND value_text != ''
                    ORDER BY param_code
                    """,
                    (tenant_id, location_id),
                )
                rows = cur.fetchall()

                for row in rows:
                    param_code = row["param_code"]
                    value_text = row["value_text"]

                    # Skip special knowledge already inlined via tenant_ai_prompts
                    if param_code in ("business_info", "locations"):
                        continue

                    knowledge_sections.append({
                        "param_code": param_code,
                        "content": value_text,
                    })
                    knowledge_total_size += len(value_text)

        conn.close()
    except Exception as e:
        logger.warning(f"[publish_openai] Failed to collect knowledge: {e}")

    INLINE_THRESHOLD = 30 * 1024  # 30KB

    if knowledge_sections and knowledge_total_size < INLINE_THRESHOLD:
        # Tier 1: Inline into system prompt (small knowledge, < 30KB)
        knowledge_md = "\n\n# Knowledge Base\n"
        knowledge_md += (
            "IMPORTANT: This knowledge is for REFERENCE ONLY when answering caller questions.\n"
            "- Summarize and speak naturally — do NOT read items, prices, or lists verbatim.\n"
            "- For menus: describe categories and highlights. Only give specific item details or prices when the caller asks about a specific item.\n"
            "- Never read out markdown formatting, 'null', or raw data artifacts.\n"
            "- Say currency names naturally in the caller's language (e.g. '澳元' not 'AUD', 'dollars' not '$').\n"
            "- Keep responses concise — a few sentences, not a full listing.\n\n"
        )
        for section in knowledge_sections:
            title = section["param_code"].replace("_", " ").title()
            knowledge_md += f"\n## {title}\n\n{section['content']}\n"
        composed += knowledge_md
        logger.info(
            f"[publish_openai] Inlined {len(knowledge_sections)} knowledge entries "
            f"({knowledge_total_size} bytes) into system prompt"
        )
    elif knowledge_sections:
        # Tier 2: Too large for inline — will use OpenAI vector store + search_knowledge tool
        logger.info(
            f"[publish_openai] {len(knowledge_sections)} knowledge entries "
            f"({knowledge_total_size} bytes) — will use vector store (> 30KB)"
        )

    return composed, temperature_value, knowledge_sections, knowledge_total_size


def _upload_knowledge_to_vector_store(
    tenant_id: str,
    location_id: str,
    knowledge_sections: list,
) -> str | None:
    """
    Upload knowledge to an OpenAI Vector Store.
    Creates the store if it doesn't exist, replaces files if it does.
    Returns the vector_store_id, or None if upload fails.
    """
    import psycopg2
    from psycopg2.extras import RealDictCursor
    import requests
    import tempfile

    openai_key = os.getenv("OPENAI_API_KEY")
    if not openai_key:
        logger.error("[publish_openai] OPENAI_API_KEY not set, cannot create vector store")
        return None

    headers = {
        "Authorization": f"Bearer {openai_key}",
        "OpenAI-Beta": "assistants=v2",
    }

    # Check if location already has a vector store
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT openai_vector_store_id FROM locations WHERE tenant_id = %s AND location_id = %s",
                (tenant_id, location_id),
            )
            row = cur.fetchone()
            existing_vs_id = row["openai_vector_store_id"] if row else None
    finally:
        conn.close()

    vector_store_id = existing_vs_id

    # Create vector store if it doesn't exist
    if not vector_store_id:
        logger.info("[publish_openai] Creating new vector store for tenant=%s location=%s", tenant_id, location_id)
        resp = requests.post(
            "https://api.openai.com/v1/vector_stores",
            headers=headers,
            json={
                "name": f"speako-{tenant_id}-{location_id}",
                "metadata": {"tenant_id": str(tenant_id), "location_id": str(location_id)},
            },
            timeout=30,
        )
        if resp.ok:
            vector_store_id = resp.json().get("id")
            logger.info("[publish_openai] Created vector store: %s", vector_store_id)

            # Save to DB
            conn = psycopg2.connect(os.getenv("DATABASE_URL"))
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE locations SET openai_vector_store_id = %s WHERE tenant_id = %s AND location_id = %s",
                        (vector_store_id, tenant_id, location_id),
                    )
                conn.commit()
            finally:
                conn.close()
        else:
            logger.error("[publish_openai] Failed to create vector store: %s %s", resp.status_code, resp.text[:300])
            return None
    else:
        # Delete existing files from the store before re-uploading
        logger.info("[publish_openai] Clearing existing files from vector store %s", vector_store_id)
        try:
            list_resp = requests.get(
                f"https://api.openai.com/v1/vector_stores/{vector_store_id}/files",
                headers=headers,
                timeout=15,
            )
            if list_resp.ok:
                for f in list_resp.json().get("data", []):
                    file_id = f.get("id")
                    requests.delete(
                        f"https://api.openai.com/v1/vector_stores/{vector_store_id}/files/{file_id}",
                        headers=headers,
                        timeout=10,
                    )
                    logger.info("[publish_openai] Deleted file %s from vector store", file_id)
        except Exception as e:
            logger.warning("[publish_openai] Error clearing vector store files: %s", e)

    # Aggregate all knowledge into one markdown file
    md_content = ""
    for section in knowledge_sections:
        title = section["param_code"].replace("_", " ").title()
        md_content += f"# {title}\n\n{section['content']}\n\n---\n\n"

    # Upload as a file
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as tmp:
            tmp.write(md_content)
            tmp_path = tmp.name

        with open(tmp_path, "rb") as f:
            upload_resp = requests.post(
                "https://api.openai.com/v1/files",
                headers={"Authorization": f"Bearer {openai_key}"},
                files={"file": (f"knowledge-{tenant_id}-{location_id}.md", f, "text/markdown")},
                data={"purpose": "assistants"},
                timeout=60,
            )

        os.unlink(tmp_path)

        if not upload_resp.ok:
            logger.error("[publish_openai] File upload failed: %s %s", upload_resp.status_code, upload_resp.text[:300])
            return vector_store_id

        file_id = upload_resp.json().get("id")
        logger.info("[publish_openai] Uploaded file %s (%d bytes)", file_id, len(md_content))

        # Attach file to vector store
        attach_resp = requests.post(
            f"https://api.openai.com/v1/vector_stores/{vector_store_id}/files",
            headers=headers,
            json={"file_id": file_id},
            timeout=30,
        )
        if attach_resp.ok:
            logger.info("[publish_openai] Attached file %s to vector store %s", file_id, vector_store_id)
        else:
            logger.error("[publish_openai] File attach failed: %s %s", attach_resp.status_code, attach_resp.text[:300])

    except Exception as e:
        logger.error("[publish_openai] Vector store upload error: %s", e)

    return vector_store_id


def _compose_native_agent_config(
    tenant_id: str,
    location_id: str,
    celery_task_id: str,
    config_version: int,
    provider: str = "openai",
) -> dict:
    """
    Compose the complete native_agent_config JSON blob.

    Reads everything from raw DB source tables. Fully independent of the
    ElevenLabs publish path — does not read tenant_ai_prompts composed rows,
    does not call any external API.
    """
    # ── 1. Collect all params (OpenAI-independent, no elevenlabs_agent_id required) ──
    all_params = _collect_native_agent_params(str(tenant_id), str(location_id), provider=provider)

    # ── 2. Extract individual param groups ──
    voice_dict_params = all_params.get("voice_dict_params", [])
    personality_params = all_params.get("personality_params", [])
    greetings_params = all_params.get("greetings_params", [])
    tool_params = all_params.get("tool_params", [])
    dictionary_entry = all_params.get("dictionary_entry")

    # ── 3. Write fragments to tenant_ai_prompts then compose system prompt ──
    # tenant_ai_prompts is the single source of truth — both ElevenLabs and
    # OpenAI paths write to it and read from it. This ensures identical prompts.
    system_prompt, composed_temperature, knowledge_sections, knowledge_total_size = _ensure_fragments_and_compose(
        str(tenant_id), str(location_id), all_params
    )

    # ── 4. Extract voice/speed/temperature/azure-specific settings ──
    voice = _extract_voice_id(voice_dict_params, provider=provider)
    speed = _extract_speed(voice_dict_params)
    temperature = composed_temperature if composed_temperature is not None else _extract_temperature(personality_params)
    greetings = _extract_greetings(greetings_params)
    pronunciation_hints = _extract_pronunciation_hints(dictionary_entry)

    # Azure-specific: VAD, pitch, volume
    vad_threshold = 0.7
    vad_silence_duration_ms = 1000
    pitch = "+0%"
    volume = "+0%"
    for p in voice_dict_params:
        pc = p.get("param_code")
        if pc == "vad_threshold" and p.get("value_numeric") is not None:
            vad_threshold = float(p["value_numeric"])
        elif pc == "vad_silence_duration_ms" and p.get("value_numeric") is not None:
            vad_silence_duration_ms = int(p["value_numeric"])
        elif pc == "pitch" and p.get("value_text"):
            pitch = p["value_text"]
        elif pc == "volume" and p.get("value_text"):
            volume = p["value_text"]

    # ── 5. Build enabled tools ──
    tools, enabled_tool_keys = _build_enabled_tools(tool_params)

    # ── 5b. Add search_knowledge tool for Tier 2 (large knowledge, >= 30KB) ──
    # Tier 1 (< 30KB): already inlined into system prompt by _ensure_fragments_and_compose
    # Tier 2 (>= 30KB): upload to OpenAI Vector Store + add search_knowledge tool
    INLINE_THRESHOLD = 30 * 1024  # 30KB — must match _ensure_fragments_and_compose
    vector_store_id = None
    knowledge_is_inline = knowledge_sections and knowledge_total_size < INLINE_THRESHOLD

    if knowledge_sections and not knowledge_is_inline:
        # Tier 2: large knowledge — needs vector store + search_knowledge tool
        registry = _tool_schema_registry()
        search_tool = registry.get("search_knowledge")
        if search_tool and "search_knowledge" not in enabled_tool_keys:
            tools.append(search_tool)
            enabled_tool_keys.append("search_knowledge")
            logger.info("[publish_openai] Added search_knowledge tool (%d knowledge entries, %d bytes — Tier 2)", len(knowledge_sections), knowledge_total_size)

        # Upload knowledge to OpenAI Vector Store
        vector_store_id = _upload_knowledge_to_vector_store(
            str(tenant_id), str(location_id), knowledge_sections
        )

    # ── 6. Append pronunciation hints to instructions ──
    instructions = system_prompt
    if pronunciation_hints:
        hint_lines = [f'- Pronounce "{h["from"]}" as "{h["to"]}".' for h in pronunciation_hints]
        instructions += "\n\n# Pronunciation guide\n" + "\n".join(hint_lines)

    # ── 6b. Build language config and append language enforcement directive ──
    language_config_raw = all_params.get("language_config")
    language_block = None
    if language_config_raw:
        primary_lang = language_config_raw.get("primary_language", "en")
        detection_enabled = language_config_raw.get("language_detection_enabled", False)
        presets = language_config_raw.get("language_presets", {})
        enabled_langs = [lang for lang, cfg in presets.items() if cfg.get("enabled")]
        # Deduplicate: primary language should not also appear in enabled secondary languages
        enabled_langs = [l for l in enabled_langs if l != primary_lang]
        supported = [primary_lang] + enabled_langs

        language_block = {
            "primary": primary_lang,
            "detection_enabled": detection_enabled,
            "supported": supported,
        }

        # Fetch language names and speaking style hints
        import psycopg2
        from psycopg2.extras import RealDictCursor as _RDC
        lang_hints = {}  # language_code -> {name, hint}
        _hint_conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        try:
            with _hint_conn:
                with _hint_conn.cursor(cursor_factory=_RDC) as _cur:
                    _cur.execute(
                        """SELECT language_code, language_name, speaking_style_hint
                           FROM ai_language_presets
                           WHERE language_code = ANY(%s)""",
                        (supported,),
                    )
                    for row in _cur.fetchall():
                        lang_hints[row["language_code"]] = {
                            "name": row["language_name"],
                            "hint": row["speaking_style_hint"],
                        }
        finally:
            _hint_conn.close()

        # Flag whether primary language has a speaking style hint
        primary_hint = lang_hints.get(primary_lang, {}).get("hint")
        language_block["primary_has_style_hint"] = bool(primary_hint)

        # Build language→voice mapping for runtime voice switching
        primary_voice_id = language_config_raw.get("primary_voice_id", "")
        voices_map = {primary_lang: primary_voice_id} if primary_voice_id else {}
        for lang, cfg in presets.items():
            if cfg.get("enabled") and cfg.get("voice_id") and lang != primary_lang:
                voices_map[lang] = cfg["voice_id"]
        language_block["voices"] = voices_map

        # Build human-readable language name for primary
        primary_name = lang_hints.get(primary_lang, {}).get("name", primary_lang)

        # Append language enforcement directive to instructions
        lang_lines = [f"Your primary language is {primary_name} ({primary_lang}). Start the conversation in this language."]
        if enabled_langs:
            secondary_names = [
                f"{lang_hints.get(l, {}).get('name', l)} ({l})"
                for l in enabled_langs
            ]
            lang_lines.append(f"You also support: {', '.join(secondary_names)}.")
            lang_lines.append(
                "CRITICAL: If the caller speaks ANY utterance in a different supported language, "
                "you MUST call switch_language immediately — even if it sounds like a question about "
                "language capability. Do NOT respond in your current language first. Do NOT ask for "
                "confirmation. Do NOT explain which languages you support. Just call the tool. "
                "The caller may not understand your current language at all."
            )
        lang_lines.append("If the caller requests a language not listed above, tell them which languages are available.")

        # Append speaking style hints for all supported languages that have one
        for lang_code in supported:
            hint = lang_hints.get(lang_code, {}).get("hint")
            if hint:
                lang_lines.append("")
                lang_lines.append(hint)

        instructions += "\n\n# Language rules\n" + "\n".join(lang_lines)

    # ── 7. Assemble the blob ──
    now = datetime.now(timezone.utc).isoformat()

    # Collect all inputs for deterministic hash
    hash_inputs = {
        "system_prompt": system_prompt,
        "voice": voice,
        "speed": speed,
        "temperature": temperature,
        "enabled_tool_keys": sorted(enabled_tool_keys),
        "pronunciation_hints": pronunciation_hints,
        "greetings": greetings,
        "knowledge_total_size": knowledge_total_size,
        "knowledge_keys": sorted([s["param_code"] for s in knowledge_sections]) if knowledge_sections else [],
        "language": language_block,
    }
    source_hash = _deterministic_hash(hash_inputs)

    config = {
        # ── metadata ──
        "schema_version": 1,
        "config_version": config_version,
        "agent_id": f"{provider}:tenant_{tenant_id}:location_{location_id}",
        "tenant_id": int(tenant_id),
        "location_id": int(location_id),
        "provider": provider,
        "composed_at": now,
        "composed_by_task_id": celery_task_id,
        "source_hash": source_hash,
        "composer": {
            "enabled_tool_keys": enabled_tool_keys,
        },
        # ── session: directly spreadable into OpenAI session.update ──
        "session": {
            "type": "realtime",
            "model": DEFAULT_OPENAI_MODEL,
            "output_modalities": ["audio"],
            "instructions": instructions,
            "temperature": temperature,
            "max_response_output_tokens": "inf",
            "audio": {
                "input": {
                    "format": {"type": "audio/pcm", "rate": 24000},
                    "transcription": {"model": DEFAULT_TRANSCRIPTION_MODEL},
                    "turn_detection": {"type": "semantic_vad"},
                },
                "output": {
                    "format": {"type": "audio/pcm", "rate": 24000},
                    "voice": voice,
                    "speed": speed,
                    "pitch": pitch,
                    "volume": volume,
                },
            },
            "vad": {
                "threshold": vad_threshold,
                "silence_duration_ms": vad_silence_duration_ms,
            },
            "tools": tools,
            "tool_choice": "auto",
        },
        # ── greeting variants ──
        "greeting": greetings,
        # ── language configuration ──
        "language": language_block,
        # ── knowledge ──
        "knowledge": {
            "inline_snippets": [
                {"type": s["param_code"], "content": s["content"][:200] + "…"} for s in knowledge_sections
            ] if knowledge_is_inline else [],
            "vector_store": {
                "vector_store_id": vector_store_id,
            } if vector_store_id else None,
            "total_knowledge_size": knowledge_total_size,
            "tier": "inline" if knowledge_is_inline else ("vector_store" if knowledge_sections else "none"),
            "knowledge_entries": [s["param_code"] for s in knowledge_sections] if knowledge_sections else [],
        },
        # ── pronunciation (kept for audit, already baked into instructions) ──
        "pronunciation_hints": pronunciation_hints,
        # ── dynamic variable defaults ──
        "dynamic_variable_defaults": {
            "force_phone_transfer": False,
            "mode": "prod",
            "known_customer_name": "",
        },
        # ── runtime overlay contract ──
        "runtime_overlay": {
            "mutable_session_fields": ["instructions"],
            "injects_at_call_start": [
                "caller_first_name",
                "caller_phone_e164",
                "resolved_greeting",
                "call_language_hint",
            ],
        },
    }

    # ── 7. Size guard ──
    config_json = json.dumps(config)
    if len(config_json.encode("utf-8")) > CONFIG_SIZE_LIMIT:
        raise ValueError(
            f"Composed config exceeds {CONFIG_SIZE_LIMIT // 1024}KB limit "
            f"({len(config_json.encode('utf-8'))} bytes). "
            "Knowledge may be leaking into inline instructions."
        )

    return config


def _get_current_config_version(tenant_id: str, location_id: str) -> int:
    """Read the current config_version from the DB, return next version."""
    import psycopg2
    from psycopg2.extras import RealDictCursor

    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT native_agent_config->>'config_version' AS cv
                    FROM locations
                    WHERE tenant_id = %s AND location_id = %s
                    """,
                    (tenant_id, location_id),
                )
                row = cur.fetchone()
                if row and row["cv"]:
                    return int(row["cv"]) + 1
                return 1
    finally:
        conn.close()


def _write_config_to_db(tenant_id: str, location_id: str, config: dict):
    """Write the composed config to locations.native_agent_config."""
    import psycopg2
    from psycopg2.extras import Json

    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE locations
                    SET native_agent_config = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE tenant_id = %s AND location_id = %s
                    """,
                    (Json(config), tenant_id, location_id),
                )
                if cur.rowcount == 0:
                    raise ValueError(
                        f"No location found to update: "
                        f"tenant_id={tenant_id}, location_id={location_id}"
                    )
        logger.info(
            f"[publish_openai] Wrote config v{config.get('config_version')} "
            f"to locations.native_agent_config"
        )
    finally:
        conn.close()


def _read_current_source_hash(tenant_id: str, location_id: str) -> str | None:
    """Read the current source_hash from the stored config."""
    import psycopg2
    from psycopg2.extras import RealDictCursor

    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT native_agent_config->>'source_hash' AS sh
                    FROM locations
                    WHERE tenant_id = %s AND location_id = %s
                    """,
                    (tenant_id, location_id),
                )
                row = cur.fetchone()
                return row["sh"] if row else None
    finally:
        conn.close()


# ── Celery task ──────────────────────────────────────────────────────


@app.task(bind=True, name="tasks.publish_native_agent")
def publish_native_agent(
    self,
    tenant_id: str,
    location_id: str,
    publish_job_id: str,
    speako_task_id: str = None,
    tenant_integration_param: dict = None,
    provider: str = "openai",
):
    """
    Compose and store the native agent config for a location.

    Supports both OpenAI Realtime and Azure Voice Live providers.
    Reads all config from the same DB rows the ElevenLabs publish task uses.
    Does NOT call any external API (except OpenAI vector store for large knowledge).
    """
    celery_task_id = self.request.id

    logger.info(
        f"[publish_native_agent] Started — "
        f"tenant_id={tenant_id}, location_id={location_id}, "
        f"publish_job_id={publish_job_id}, speako_task_id={speako_task_id}, "
        f"celery_task_id={celery_task_id}"
    )

    try:
        if speako_task_id:
            mark_task_running(task_id=speako_task_id, celery_task_id=celery_task_id)

        # ── Compose ──
        config_version = _get_current_config_version(tenant_id, location_id)
        config = _compose_native_agent_config(
            tenant_id=tenant_id,
            location_id=location_id,
            celery_task_id=celery_task_id,
            config_version=config_version,
            provider=provider,
        )

        # ── Idempotency check ──
        current_hash = _read_current_source_hash(tenant_id, location_id)
        if current_hash == config["source_hash"]:
            logger.info(
                f"[publish_native_agent] No changes detected (hash={current_hash}), "
                f"skipping write"
            )
            if speako_task_id:
                mark_task_succeeded(
                    task_id=speako_task_id,
                    celery_task_id=celery_task_id,
                    details={"result": "no_changes", "source_hash": current_hash},
                )
            return {
                "status": "no_changes",
                "source_hash": current_hash,
                "config_version": config_version - 1,
            }

        # ── Write to DB ──
        _write_config_to_db(tenant_id, location_id, config)

        if speako_task_id:
            mark_task_succeeded(
                task_id=speako_task_id,
                celery_task_id=celery_task_id,
                details={
                    "result": "published",
                    "config_version": config_version,
                    "source_hash": config["source_hash"],
                    "tool_count": len(config["session"]["tools"]),
                    "instructions_length": len(config["session"]["instructions"]),
                    "voice": config["session"]["audio"]["output"]["voice"],
                },
            )

        logger.info(
            f"[publish_native_agent] Success — config_version={config_version}, "
            f"tools={len(config['session']['tools'])}, "
            f"prompt_len={len(config['session']['instructions'])}"
        )

        return {
            "status": "published",
            "config_version": config_version,
            "source_hash": config["source_hash"],
        }

    except Exception as exc:
        logger.exception(f"[publish_native_agent] Failed: {exc}")
        if speako_task_id:
            mark_task_failed(
                task_id=speako_task_id,
                celery_task_id=celery_task_id,
                error_message=str(exc)[:500],
            )
        raise
