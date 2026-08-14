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
