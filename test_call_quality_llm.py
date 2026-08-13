"""
Tests for the LLM triage pass (tasks/utils/call_quality_llm.py).

No network and no API key: the OpenAI client is injected, so every test
exercises real prompt construction and real response validation.

Validation gets the most coverage on purpose — a model asked "what went wrong?"
will always find something, and the closed vocabulary + citation rules are the
only things standing between that and an inbox full of noise.

Run:  python -m pytest test_call_quality_llm.py -q
"""

import json
from pathlib import Path

import pytest

from tasks.utils import call_quality_llm as llm
from tasks.utils.call_quality_rules import index_catalogue, parse_artifact

CATALOGUE = index_catalogue(json.loads(
    (Path(__file__).parent / "tasks" / "data" / "call_issue_catalogue.json").read_text()
))


def artifact(msgs):
    """msgs: list of (seq, message). Returns classified events."""
    lines = [json.dumps({"_artifact": "speako.call_server_log.v1",
                         "sequence_gaps": [], "call_complete": True})]
    lines += [json.dumps({"seq": s, "ts": "2026-08-13T10:00:00.000Z",
                          "level": "info", "msg": m}) for s, m in msgs]
    _, events = parse_artifact("\n".join(lines))
    return events


CONVERSATION = [
    (10, "[Azure] Caller transcript: Hi, do you do gel nails?"),
    (12, "[Azure] Assistant transcript: We're open until 6pm on Saturdays."),
    (20, "[Azure] Caller transcript: I asked about gel nails."),
    (22, "[Azure] Assistant transcript: Yes, we offer gel manicures."),
]


class FakeClient:
    """Minimal stand-in for the OpenAI client."""

    def __init__(self, payload):
        self.payload = payload
        self.captured = None
        outer = self

        class Completions:
            def create(self, **kwargs):
                outer.captured = kwargs
                content = (outer.payload if isinstance(outer.payload, str)
                           else json.dumps(outer.payload))
                return type("R", (), {
                    "choices": [type("C", (), {
                        "message": type("M", (), {"content": content})()
                    })()]
                })()

        self.chat = type("Chat", (), {"completions": Completions()})()


# ── Enablement ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("value,expected", [
    ("1", True), ("true", True), ("TRUE", True), ("yes", True), ("on", True),
    ("0", False), ("false", False), ("", False), ("maybe", False),
])
def test_enable_flag(monkeypatch, value, expected):
    monkeypatch.setenv(llm.ENABLED_ENV, value)
    assert llm.is_enabled() is expected


def test_disabled_when_unset(monkeypatch):
    monkeypatch.delenv(llm.ENABLED_ENV, raising=False)
    assert llm.is_enabled() is False


# ── Transcript reconstruction ────────────────────────────────────────────────

def test_transcript_carries_the_seq_the_model_must_cite():
    lines = llm.build_transcript(artifact(CONVERSATION))
    assert lines == [
        "[10] caller: Hi, do you do gel nails?",
        "[12] agent: We're open until 6pm on Saturdays.",
        "[20] caller: I asked about gel nails.",
        "[22] agent: Yes, we offer gel manicures.",
    ]


def test_transcript_marks_failed_and_empty_transcription():
    lines = llm.build_transcript(artifact([
        (5, "[Azure] EMPTY CALLER TRANSCRIPT — speech detected but STT produced no text"),
        (6, "[Azure] Caller transcription FAILED: timeout"),
    ]))
    assert "nothing was transcribed" in lines[0]
    assert "transcription failed" in lines[1]


def test_transcript_includes_tool_use():
    lines = llm.build_transcript(artifact([
        (8, "[Azure] Tool call: check_availabilities call_id=x params={}"),
    ]))
    assert lines == ["[8] (agent used tool: check_availabilities)"]


# ── Prompt ───────────────────────────────────────────────────────────────────

def test_prompt_offers_only_llm_catalogue_entries():
    prompt = llm.build_prompt(llm.build_transcript(artifact(CONVERSATION)), [], CATALOGUE)
    assert "answered_different_question" in prompt          # an llm entry
    assert "short_utterance_dropped" not in prompt          # a rule entry
    assert "vad_config_mismatch" not in prompt


def test_prompt_passes_rule_findings_as_context_only():
    prompt = llm.build_prompt(
        llm.build_transcript(artifact(CONVERSATION)),
        [{"rule_id": "dead_air"}],
        CATALOGUE,
    )
    assert "dead_air" in prompt
    assert "do not repeat them" in prompt


# ── Validation: the part that matters ────────────────────────────────────────

def test_accepts_a_well_formed_finding():
    transcript = llm.build_transcript(artifact(CONVERSATION))
    findings = llm.validate(
        {"findings": [{"rule_id": "answered_different_question",
                       "seq": [12], "detail": "Agent gave opening hours."}]},
        transcript, CATALOGUE,
    )
    assert len(findings) == 1
    finding = findings[0]
    assert finding["detector"] == "llm"
    assert finding["severity"] == "critical"
    assert finding["evidence"]["cited_seq"] == [12]
    assert "Agent gave opening hours." in finding["explanation"]
    # Catalogue text is attached, so the UI needs no copy of the catalogue.
    assert finding["suggested_fix"]


def test_rejects_a_rule_id_outside_the_catalogue():
    findings = llm.validate(
        {"findings": [{"rule_id": "agent_was_rude", "seq": [12], "detail": "x"}]},
        llm.build_transcript(artifact(CONVERSATION)), CATALOGUE,
    )
    assert findings == []


def test_rejects_a_deterministic_rule_id():
    """Rule detectors are the engine's job; the model must not claim them."""
    findings = llm.validate(
        {"findings": [{"rule_id": "dead_air", "seq": [12], "detail": "slow"}]},
        llm.build_transcript(artifact(CONVERSATION)), CATALOGUE,
    )
    assert findings == []


def test_rejects_invented_citations():
    """A seq the model never saw means the evidence is fabricated."""
    findings = llm.validate(
        {"findings": [{"rule_id": "answered_different_question",
                       "seq": [999], "detail": "made up"}]},
        llm.build_transcript(artifact(CONVERSATION)), CATALOGUE,
    )
    assert findings == []


def test_keeps_only_the_citations_that_exist():
    findings = llm.validate(
        {"findings": [{"rule_id": "answered_different_question",
                       "seq": [12, 999], "detail": "partly real"}]},
        llm.build_transcript(artifact(CONVERSATION)), CATALOGUE,
    )
    assert findings[0]["evidence"]["cited_seq"] == [12]


def test_rejects_a_finding_with_no_citation():
    findings = llm.validate(
        {"findings": [{"rule_id": "answered_different_question", "detail": "trust me"}]},
        llm.build_transcript(artifact(CONVERSATION)), CATALOGUE,
    )
    assert findings == []


def test_collapses_duplicate_rule_ids():
    """UNIQUE (location_conversation_id, rule_id) — one per rule per call."""
    findings = llm.validate(
        {"findings": [
            {"rule_id": "answered_different_question", "seq": [12], "detail": "a"},
            {"rule_id": "answered_different_question", "seq": [22], "detail": "b"},
        ]},
        llm.build_transcript(artifact(CONVERSATION)), CATALOGUE,
    )
    assert len(findings) == 1
    assert findings[0]["evidence"]["cited_seq"] == [12]


@pytest.mark.parametrize("raw", [
    "not json at all",
    "{}",
    '{"findings": null}',
    '{"findings": "a string"}',
    '{"findings": [null, 42, "x"]}',
    '{"findings": [{"seq": [12]}]}',            # no rule_id
])
def test_malformed_responses_are_survivable(raw):
    assert llm.validate(raw, llm.build_transcript(artifact(CONVERSATION)), CATALOGUE) == []


def test_empty_findings_is_the_expected_answer():
    assert llm.validate('{"findings": []}',
                        llm.build_transcript(artifact(CONVERSATION)), CATALOGUE) == []


# ── End to end with an injected client ───────────────────────────────────────

def test_triage_end_to_end():
    client = FakeClient({"findings": [
        {"rule_id": "answered_different_question", "seq": [12],
         "detail": "Caller asked about gel nails; agent gave opening hours."},
    ]})
    findings = llm.triage(artifact(CONVERSATION), [], CATALOGUE, client=client)

    assert [f["rule_id"] for f in findings] == ["answered_different_question"]
    assert client.captured["temperature"] == 0
    assert client.captured["response_format"] == {"type": "json_object"}
    assert client.captured["max_tokens"] == llm.MAX_OUTPUT_TOKENS


def test_triage_skips_calls_too_short_to_judge():
    """No API call at all for a two-line call — cheapest possible skip."""
    client = FakeClient({"findings": [
        {"rule_id": "answered_different_question", "seq": [10], "detail": "x"},
    ]})
    findings = llm.triage(artifact(CONVERSATION[:2]), [], CATALOGUE, client=client)
    assert findings == []
    assert client.captured is None
