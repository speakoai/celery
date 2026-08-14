"""
Parity tests for the two publisher decisions shared with speako-web
(src/lib/publish/native/tools.ts).

Both publishers compose the same agent config, so a divergence here means a
tenant's agent changes behaviour depending on which one last ran. Pure logic —
no network, DB or R2.

Run:  python -m pytest test_order_transfer_parity.py -q
"""

import pytest

from tasks.utils.publish_tool_rules import prompt_gate_passes, resolve_booking_bundle


REST_BUNDLE = [
    "check_availabilities", "make_booking", "check_latest_booking",
    "check_modify_availabilities", "modify_booking", "cancel_booking",
]


# ── handling_mode → which tools the bundle expands to ──────────────────────

def test_ai_handles_keeps_the_full_bundle():
    out = resolve_booking_bundle(
        "booking_manager_rest", REST_BUNDLE, {"properties": {"handling_mode": "ai_handles"}}
    )
    assert out == REST_BUNDLE


def test_missing_mode_defaults_to_ai_handles():
    """Every existing location has no explicit mode — they must not change."""
    assert resolve_booking_bundle("booking_manager_rest", REST_BUNDLE, {}) == REST_BUNDLE
    assert resolve_booking_bundle(
        "booking_manager_rest", REST_BUNDLE, {"properties": {}}
    ) == REST_BUNDLE


def test_transfer_to_staff_replaces_the_bundle():
    """The AI must stop taking bookings, not merely gain a transfer tool."""
    out = resolve_booking_bundle(
        "booking_manager_rest", REST_BUNDLE,
        {"properties": {"handling_mode": "transfer_to_human"}},
    )
    assert out == ["transfer_booking_call"]
    assert "make_booking" not in out


def test_send_booking_link_replaces_the_bundle():
    out = resolve_booking_bundle(
        "booking_manager_service", REST_BUNDLE,
        {"properties": {"handling_mode": "send_booking_link"}},
    )
    assert out == ["send_booking_link"]


def test_unknown_mode_falls_back_to_the_bundle():
    """An unrecognised value must not strip the agent of its booking tools."""
    out = resolve_booking_bundle(
        "booking_manager_rest", REST_BUNDLE, {"properties": {"handling_mode": "wat"}}
    )
    assert out == REST_BUNDLE


def test_non_booking_params_are_untouched():
    assert resolve_booking_bundle(
        "send_order_link", ["send_order_link"],
        {"properties": {"handling_mode": "transfer_to_human"}},
    ) == ["send_order_link"]


# ── requires_property prompt gating ────────────────────────────────────────

def test_ungated_prompts_always_publish():
    assert prompt_gate_passes(None, []) is True
    assert prompt_gate_passes("", [{"anything": True}]) is True


def test_gated_prompt_publishes_when_the_option_is_on():
    assert prompt_gate_passes("offer_human_transfer", [{"offer_human_transfer": True}]) is True


def test_gated_prompt_is_withheld_when_the_option_is_off_or_absent():
    assert prompt_gate_passes("offer_human_transfer", [{"offer_human_transfer": False}]) is False
    assert prompt_gate_passes("offer_human_transfer", [{}]) is False
    assert prompt_gate_passes("offer_human_transfer", []) is False


@pytest.mark.parametrize("truthy_but_not_true", ["true", 1, "yes", [1]])
def test_only_boolean_true_opens_the_gate(truthy_but_not_true):
    """A half-configured option must not put instructions in front of a caller."""
    assert prompt_gate_passes(
        "offer_human_transfer", [{"offer_human_transfer": truthy_but_not_true}]
    ) is False


def test_any_owning_param_may_open_the_gate():
    """A tool can be owned by more than one enabled param."""
    assert prompt_gate_passes(
        "offer_human_transfer",
        [{"offer_human_transfer": False}, {"offer_human_transfer": True}],
    ) is True


def test_none_properties_do_not_explode():
    assert prompt_gate_passes("offer_human_transfer", [None]) is False
