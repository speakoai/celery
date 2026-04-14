"""
Publish OpenAI Realtime Agent Config Task

Composes a self-contained JSON blob for the OpenAI Realtime path and writes it
to locations.openai_agent_config.

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
# schema.  Only tools listed here can appear in openai_agent_config.
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


def _extract_voice_id(voice_dict_params: list) -> str:
    """Extract OpenAI voice from voice_dict_params, falling back to default."""
    for p in voice_dict_params:
        if p.get("param_code") == "voice_id" and p.get("value_text"):
            v = p["value_text"].strip()
            if v in OPENAI_VOICES:
                return v
            logger.warning(
                f"[publish_openai] voice_id '{v}' is not an OpenAI voice, using default"
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
    registry = _tool_schema_registry()
    tools = []
    enabled_keys = []

    for p in tool_params:
        param_code = p.get("param_code", "")
        value_json = p.get("value_json") or {}
        if not value_json.get("enabled", False):
            continue

        schema = registry.get(param_code)
        if schema:
            tools.append(schema)
            enabled_keys.append(param_code)
        else:
            logger.warning(
                f"[publish_openai] Enabled tool '{param_code}' has no OpenAI schema, skipping"
            )

    logger.info(f"[publish_openai] Enabled tools: {enabled_keys}")
    return tools, enabled_keys


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
    from .publish_helpers import get_human_friendly_operation_hours
    from .publish_db import (
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

    return composed, temperature_value


def _compose_openai_agent_config(
    tenant_id: str,
    location_id: str,
    celery_task_id: str,
    config_version: int,
) -> dict:
    """
    Compose the complete openai_agent_config JSON blob.

    Reads everything from raw DB source tables. Fully independent of the
    ElevenLabs publish path — does not read tenant_ai_prompts composed rows,
    does not call any external API.
    """
    # ── 1. Collect all params in one aggregated query ──
    all_params = collect_full_agent_params(str(tenant_id), str(location_id))

    # ── 2. Extract individual param groups ──
    voice_dict_params = all_params.get("voice_dict_params", [])
    personality_params = all_params.get("personality_params", [])
    greetings_params = all_params.get("greetings_params", [])
    tool_params = all_params.get("tool_params", [])
    dictionary_entry = all_params.get("dictionary_entry")

    # ── 3. Write fragments to tenant_ai_prompts then compose system prompt ──
    # tenant_ai_prompts is the single source of truth — both ElevenLabs and
    # OpenAI paths write to it and read from it. This ensures identical prompts.
    system_prompt, composed_temperature = _ensure_fragments_and_compose(
        str(tenant_id), str(location_id), all_params
    )

    # ── 4. Extract voice/speed/temperature ──
    voice = _extract_voice_id(voice_dict_params)
    speed = _extract_speed(voice_dict_params)
    temperature = composed_temperature if composed_temperature is not None else _extract_temperature(personality_params)
    greetings = _extract_greetings(greetings_params)
    pronunciation_hints = _extract_pronunciation_hints(dictionary_entry)

    # ── 5. Build enabled tools ──
    tools, enabled_tool_keys = _build_enabled_tools(tool_params)

    # ── 6. Append pronunciation hints to instructions ──
    instructions = system_prompt
    if pronunciation_hints:
        hint_lines = [f'- Pronounce "{h["from"]}" as "{h["to"]}".' for h in pronunciation_hints]
        instructions += "\n\n# Pronunciation guide\n" + "\n".join(hint_lines)

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
    }
    source_hash = _deterministic_hash(hash_inputs)

    config = {
        # ── metadata ──
        "schema_version": 1,
        "config_version": config_version,
        "agent_id": f"openai:tenant_{tenant_id}:location_{location_id}",
        "tenant_id": int(tenant_id),
        "location_id": int(location_id),
        "provider": "openai",
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
                },
            },
            "tools": tools,
            "tool_choice": "auto",
        },
        # ── greeting variants ──
        "greeting": greetings,
        # ── knowledge ──
        "knowledge": {
            "inline_snippets": [],
            "vector_store": None,
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
                    SELECT openai_agent_config->>'config_version' AS cv
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
    """Write the composed config to locations.openai_agent_config."""
    import psycopg2
    from psycopg2.extras import Json

    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE locations
                    SET openai_agent_config = %s,
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
            f"to locations.openai_agent_config"
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
                    SELECT openai_agent_config->>'source_hash' AS sh
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


@app.task(bind=True, name="tasks.publish_openai_agent")
def publish_openai_agent(
    self,
    tenant_id: str,
    location_id: str,
    publish_job_id: str,
    speako_task_id: str = None,
    tenant_integration_param: dict = None,
):
    """
    Compose and store the OpenAI Realtime agent config for a location.

    Reads all config from the same DB rows the ElevenLabs publish task uses.
    Does NOT call any external API.
    """
    celery_task_id = self.request.id

    logger.info(
        f"[publish_openai_agent] Started — "
        f"tenant_id={tenant_id}, location_id={location_id}, "
        f"publish_job_id={publish_job_id}, speako_task_id={speako_task_id}, "
        f"celery_task_id={celery_task_id}"
    )

    try:
        if speako_task_id:
            mark_task_running(task_id=speako_task_id, celery_task_id=celery_task_id)

        # ── Compose ──
        config_version = _get_current_config_version(tenant_id, location_id)
        config = _compose_openai_agent_config(
            tenant_id=tenant_id,
            location_id=location_id,
            celery_task_id=celery_task_id,
            config_version=config_version,
        )

        # ── Idempotency check ──
        current_hash = _read_current_source_hash(tenant_id, location_id)
        if current_hash == config["source_hash"]:
            logger.info(
                f"[publish_openai_agent] No changes detected (hash={current_hash}), "
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
            f"[publish_openai_agent] Success — config_version={config_version}, "
            f"tools={len(config['session']['tools'])}, "
            f"prompt_len={len(config['session']['instructions'])}"
        )

        return {
            "status": "published",
            "config_version": config_version,
            "source_hash": config["source_hash"],
        }

    except Exception as exc:
        logger.exception(f"[publish_openai_agent] Failed: {exc}")
        if speako_task_id:
            mark_task_failed(
                task_id=speako_task_id,
                celery_task_id=celery_task_id,
                error_message=str(exc)[:500],
            )
        raise
