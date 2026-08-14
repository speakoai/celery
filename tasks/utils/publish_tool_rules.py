"""
Pure publisher decisions shared with speako-web's TypeScript native publisher
(src/lib/publish/native/tools.ts).

Deliberately free of celery, DB and network imports so the rules can be unit
tested directly — the same reason call_quality_rules.py is a separate module.
Both publishers compose the same agent config, so a divergence between these
functions and their TS twins means a tenant's agent changes behaviour depending
on which publisher last ran.
"""

from typing import Any, Dict, List, Optional, Sequence


def resolve_booking_bundle(
    param_code: str,
    bundle: Sequence[str],
    value_json: Optional[Dict[str, Any]],
) -> List[str]:
    """Booking Manager's `handling_mode` decides WHICH tools the bundle expands to.

    Parity with `resolveBookingBundle`, itself ported from
    `getBookingManagerToolIds` in the legacy ElevenLabs publisher. Neither
    native publisher honoured the setting before, so a tenant who chose
    "Transfer to Staff" or "Send Booking Link" still got the full
    AI-handles-bookings tool set.

    An unrecognised mode keeps the full bundle: losing the booking tools
    entirely is far worse than ignoring a bad value.
    """
    bundle = list(bundle or [])
    if not param_code.startswith("booking_manager"):
        return bundle

    props = (value_json or {}).get("properties") or {}
    mode = props.get("handling_mode") or "ai_handles"

    if mode == "transfer_to_human":
        return ["transfer_booking_call"]
    if mode == "send_booking_link":
        return ["send_booking_link"]
    return bundle


def prompt_gate_passes(gate: Optional[str], owner_props: Sequence[Optional[dict]]) -> bool:
    """Whether a `requires_property`-gated prompt entry should be published.

    Parity with `promptGatePasses`. `owner_props` is the `properties` dict of
    every enabled param that owns the tool the prompt belongs to. Ungated
    entries always publish; a gated one needs the named property switched
    explicitly on — anything other than boolean True keeps it out, because a
    half-configured option must not put instructions in front of a caller.
    """
    if not gate:
        return True
    return any((p or {}).get(gate) is True for p in owner_props)


def prompt_tool_gate_passes(required_tool, enabled_param_codes) -> bool:
    """Whether a prompt entry's `requires_tool` dependency is satisfied.

    Belt to `prompt_gate_passes`' braces. The dashboard is supposed to stop a
    tenant enabling "also offer to transfer the caller" without configuring
    Transfer to Human — but that gate has been bypassed for real (it sat below
    the deferred save's early return, so it never ran), leaving a location with
    the option on and the transfer tool off. Publishing the offer text in that
    state gives the caller an agent that offers to put them through and then has
    no tool to do it.

    The publisher therefore refuses to ship a prompt whose tool is not enabled,
    regardless of what the config says.
    """
    if not required_tool:
        return True
    return required_tool in (enabled_param_codes or set())


# Tools that mean "this agent takes bookings itself". Parity with
# BOOKING_TOOL_KEYS in speako-web src/lib/publish/native/tools.ts.
#
# When Booking Manager is set to "Transfer to Staff" or "Send Booking Link" the
# bundle resolves to a single tool and none of these are published — at which
# point every instruction about collecting a name, a duration, or checking
# availability is not merely redundant but actively harmful. A real dev call
# showed the agent collecting a name and a karaoke duration, saying "I'll check
# availability now, please hold", then sitting silent until the watchdog hung up,
# because the tool it had been told to call did not exist.
BOOKING_TOOL_KEYS = frozenset({
    "check_availabilities",
    "check_availabilities_service",
    "make_booking",
    "make_booking_service",
    "modify_booking",
    "cancel_booking",
    "check_modify_availabilities",
    "check_modify_availabilities_service",
})


def publishes_booking_tools(enabled_tool_keys) -> bool:
    return any(k in BOOKING_TOOL_KEYS for k in (enabled_tool_keys or ()))
