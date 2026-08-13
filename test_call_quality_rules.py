"""
Tests for the deterministic call-quality rule engine
(tasks/utils/call_quality_rules.py).

Pure logic — no network, DB or R2. Line formats below are copied from the
actual logging calls in speako-voice-ai/app_azure.py, so a format change there
should break these tests rather than silently produce zero findings.

Run:  python -m pytest test_call_quality_rules.py -q
"""

import json
from pathlib import Path

import pytest

from tasks.utils.call_quality_rules import (
    analyze,
    build_turns,
    index_catalogue,
    parse_artifact,
)

CATALOGUE = index_catalogue(json.loads(
    (Path(__file__).parent / "tasks" / "data" / "call_issue_catalogue.json").read_text()
))


# ── Artifact construction ────────────────────────────────────────────────────

def line(seq, msg, ts=None, level="info"):
    return json.dumps({
        "seq": seq,
        "ts": ts or f"2026-08-13T10:00:{seq % 60:02d}.000Z",
        "level": level,
        "msg": msg,
    })


def artifact(lines, *, gaps=None, incomplete=None, call_complete=True):
    header = {
        "_artifact": "speako.call_server_log.v1",
        "call_sid": "CAtest",
        "tenant_id": 72,
        "location_id": 35,
        "sequence_gaps": gaps or [],
        "incomplete": bool(gaps) if incomplete is None else incomplete,
        "call_complete": call_complete,
    }
    return "\n".join([json.dumps(header)] + lines)


def at(second):
    return f"2026-08-13T10:00:{second:02d}.000Z"


def rules(findings):
    return {f["rule_id"] for f in findings}


def one(findings, rule_id):
    matches = [f for f in findings if f["rule_id"] == rule_id]
    assert matches, f"expected {rule_id}, got {sorted(rules(findings))}"
    return matches[0]


# Building blocks using the real log formats.
SPEECH_START = "[Azure] Caller speech started"
SPEECH_STOP = "[Azure] Caller stopped speaking — VAD end-of-turn"
ASSISTANT = "[Azure] Assistant transcript: Sure, a table for two at 7pm."
CALL_END = "[CallLifecycle] Call ended: duration=93s messages=8 status=completed"


def caller(text):
    return f"[Azure] Caller transcript: {text}"


# ── Parsing ──────────────────────────────────────────────────────────────────

def test_parses_header_and_classifies_lines():
    header, events = parse_artifact(artifact([
        line(1, SPEECH_START),
        line(2, caller("Booking")),
        line(3, ASSISTANT),
        line(4, "some unclassified noise"),
    ]))
    assert header["call_sid"] == "CAtest"
    assert [e.kind for e in events] == [
        "caller_speech_start", "caller_transcript", "assistant_transcript",
    ]


def test_barge_in_variant_matches_before_plain_speech_start():
    _, events = parse_artifact(artifact([
        line(1, "[Azure] Caller speech started (barge-in: generating=True "
                "buffered_marks=4); stopping playback"),
    ]))
    assert events[0].kind == "caller_speech_start_bargein"
    assert events[0].data["generating"] == "True"
    assert events[0].data["marks"] == "4"


# ── The headline case ────────────────────────────────────────────────────────

def test_short_utterance_dropped_is_critical():
    findings = analyze(artifact([
        line(1, SPEECH_START),
        line(2, SPEECH_STOP),
        line(3, caller("yeah")),
        line(4, SPEECH_START),          # caller speaks again, agent never replied
        line(5, caller("hello? are you there")),
        line(6, ASSISTANT),
    ]), CATALOGUE)

    finding = one(findings, "short_utterance_dropped")
    assert finding["severity"] == "critical"
    assert finding["confidence"] == "high"
    assert finding["evidence"]["transcript"] == "yeah"
    assert finding["title"]           # catalogue text was copied in
    assert finding["suggested_fix"]
    assert "agent_no_response" not in rules(findings)   # short variant wins


def test_long_utterance_unanswered_is_agent_no_response():
    findings = analyze(artifact([
        line(1, SPEECH_START),
        line(2, caller("I would like to book a table for four people on Friday")),
        line(3, SPEECH_START),
        line(4, caller("hello")),
        line(5, ASSISTANT),
    ]), CATALOGUE)
    assert "agent_no_response" in rules(findings)
    assert "short_utterance_dropped" not in rules(findings)


def test_answered_turn_raises_nothing():
    findings = analyze(artifact([
        line(1, SPEECH_START),
        line(2, SPEECH_STOP),
        line(3, caller("yeah")),
        line(4, ASSISTANT),
        line(5, CALL_END),
    ]), CATALOGUE)
    assert findings == []


def test_final_turn_never_transcribed_is_not_reported_as_ignored():
    """A turn with no transcript at all is `status='none'` — we cannot claim
    the agent ignored something we never heard."""
    findings = analyze(artifact([
        line(1, SPEECH_START),
        line(2, SPEECH_STOP),
        line(3, CALL_END),
    ]), CATALOGUE)
    assert "agent_no_response" not in rules(findings)
    assert "short_utterance_dropped" not in rules(findings)


def test_call_ended_mid_turn_when_last_words_unanswered():
    findings = analyze(artifact([
        line(1, SPEECH_START),
        line(2, caller("yeah")),
        line(3, CALL_END),
    ]), CATALOGUE)
    assert {"short_utterance_dropped", "call_ended_mid_turn"} <= rules(findings)


# ── Honesty rules ────────────────────────────────────────────────────────────

def test_sequence_gap_in_the_turn_suppresses_the_no_response_finding():
    """Render drops >6k lines/min silently, so absence inside a hole is not
    evidence of silence."""
    findings = analyze(artifact([
        line(1, SPEECH_START),
        line(2, caller("yeah")),
        line(9, SPEECH_START),
        line(10, caller("hello")),
        line(11, ASSISTANT),
    ], gaps=[[3, 8]]), CATALOGUE)
    assert "short_utterance_dropped" not in rules(findings)
    assert "agent_no_response" not in rules(findings)


def test_gap_elsewhere_does_not_suppress_but_downgrades_confidence():
    findings = analyze(artifact([
        line(1, SPEECH_START),
        line(2, caller("yeah")),
        line(3, SPEECH_START),
        line(4, caller("hello")),
        line(5, ASSISTANT),
        line(20, CALL_END),
    ], gaps=[[6, 19]]), CATALOGUE)
    assert one(findings, "short_utterance_dropped")["confidence"] == "degraded"


def test_late_response_does_not_count_as_answering_the_turn():
    """A silence-policy check-in arrives long after the caller spoke. It must
    not be credited as an answer — see the module docstring."""
    findings = analyze(artifact([
        line(1, SPEECH_START, ts=at(10)),
        line(2, SPEECH_STOP, ts=at(11)),
        line(3, caller("yeah"), ts=at(11)),
        line(4, ASSISTANT, ts=at(40)),     # ~29s later
    ]), CATALOGUE)
    assert "short_utterance_dropped" in rules(findings)


def test_missing_timestamps_do_not_manufacture_findings():
    lines = [
        json.dumps({"seq": 1, "ts": None, "level": "info", "msg": SPEECH_START}),
        json.dumps({"seq": 2, "ts": None, "level": "info", "msg": caller("yeah")}),
        json.dumps({"seq": 3, "ts": None, "level": "info", "msg": ASSISTANT}),
    ]
    assert analyze(artifact(lines), CATALOGUE) == []


# ── Remaining deterministic rules ────────────────────────────────────────────

def test_empty_transcript():
    findings = analyze(artifact([
        line(1, SPEECH_START),
        line(2, "[Azure] EMPTY CALLER TRANSCRIPT — speech detected but STT "
                "produced no text", level="warning"),
    ]), CATALOGUE)
    assert one(findings, "stt_empty_transcript")["severity"] == "critical"


def test_transcription_failed():
    findings = analyze(artifact([
        line(1, SPEECH_START),
        line(2, "[Azure] Caller transcription FAILED: timeout", level="warning"),
    ]), CATALOGUE)
    assert "stt_transcription_failed" in rules(findings)


def test_dead_air_measured_from_the_caller_finishing():
    findings = analyze(artifact([
        line(1, SPEECH_START, ts=at(10)),
        line(2, SPEECH_STOP, ts=at(12)),
        line(3, caller("a table for four please"), ts=at(12)),
        line(4, ASSISTANT, ts=at(17)),      # 5s > 3.5s threshold
    ]), CATALOGUE)
    assert one(findings, "dead_air")["evidence"]["seconds"] == 5.0


def test_prompt_response_is_not_dead_air():
    findings = analyze(artifact([
        line(1, SPEECH_START, ts=at(10)),
        line(2, SPEECH_STOP, ts=at(12)),
        line(3, caller("a table for four please"), ts=at(12)),
        line(4, ASSISTANT, ts=at(13)),
    ]), CATALOGUE)
    assert "dead_air" not in rules(findings)


def test_talked_over_caller():
    findings = analyze(artifact([
        line(1, "[Azure] Caller speech started (barge-in: generating=True "
                "buffered_marks=6); stopping playback"),
        line(2, caller("actually make it six")),
        line(3, ASSISTANT),
    ]), CATALOGUE)
    assert one(findings, "agent_talked_over_caller")["evidence"]["marks_outstanding"] == 6


def test_no_barge_in_finding_when_nothing_was_playing():
    findings = analyze(artifact([
        line(1, "[Azure] Caller speech started (barge-in: generating=False "
                "buffered_marks=0); stopping playback"),
        line(2, caller("actually make it six")),
        line(3, ASSISTANT),
    ]), CATALOGUE)
    assert "agent_talked_over_caller" not in rules(findings)


def test_vad_config_mismatch():
    findings = analyze(artifact([
        line(1, "[Azure] VAD config mismatch: requested threshold=0.4 "
                "effective=0.5", level="warning"),
    ]), CATALOGUE)
    assert "vad_config_mismatch" in rules(findings)


def test_tool_failure_detected_and_success_ignored():
    failed = analyze(artifact([
        line(1, "[Azure] Tool result: check_availabilities call_id=abc "
                "result={\"error\": \"upstream timeout\"}"),
    ]), CATALOGUE)
    assert one(failed, "tool_call_failed")["evidence"]["tool"] == "check_availabilities"

    ok = analyze(artifact([
        line(1, "[Azure] Tool result: check_availabilities call_id=abc "
                "result={\"slots\": [\"19:00\", \"19:30\"]}"),
    ]), CATALOGUE)
    assert "tool_call_failed" not in rules(ok)


def test_repeated_no_response_escalates():
    findings = analyze(artifact([
        line(1, SPEECH_START), line(2, caller("yeah")),
        line(3, SPEECH_START), line(4, caller("okay")),
        line(5, SPEECH_START), line(6, caller("hello")),
        line(7, ASSISTANT),
    ]), CATALOGUE)
    finding = one(findings, "repeated_no_response")
    assert finding["evidence"]["count"] == 2
    assert finding["severity"] == "critical"


# ── Output contract ──────────────────────────────────────────────────────────

def test_one_finding_per_rule_per_call_with_occurrence_count():
    """The DB has UNIQUE (location_conversation_id, rule_id), so the engine
    must collapse repeats rather than let an insert fail."""
    findings = analyze(artifact([
        line(1, SPEECH_START), line(2, caller("yeah")),
        line(3, SPEECH_START), line(4, caller("okay")),
        line(5, SPEECH_START), line(6, caller("hello")),
        line(7, ASSISTANT),
    ]), CATALOGUE)
    assert len(findings) == len({f["rule_id"] for f in findings})
    assert one(findings, "short_utterance_dropped")["evidence"]["occurrences"] == 2


def test_findings_carry_everything_the_db_row_needs():
    findings = analyze(artifact([
        line(1, SPEECH_START), line(2, caller("yeah")), line(3, CALL_END),
    ]), CATALOGUE)
    required = {"rule_id", "severity", "detector", "confidence", "title",
                "explanation", "suggested_fix", "evidence",
                "analyzer_version", "catalogue_version"}
    for finding in findings:
        assert required <= set(finding)
        assert finding["detector"] == "rule"
        assert finding["severity"] in ("critical", "moderate", "info")
        assert finding["confidence"] in ("high", "degraded")


def test_empty_and_malformed_input_are_survivable():
    assert analyze("", CATALOGUE) == []
    assert analyze("not json\n{}\n", CATALOGUE) == []


@pytest.mark.parametrize("rule_id", [
    "short_utterance_dropped", "agent_no_response", "repeated_no_response",
    "call_ended_mid_turn", "stt_empty_transcript", "stt_transcription_failed",
    "dead_air", "agent_talked_over_caller", "vad_config_mismatch",
    "tool_call_failed",
])
def test_every_deterministic_catalogue_entry_is_implemented(rule_id):
    """Guards against a catalogue entry marked `rule` that no code ever emits."""
    assert rule_id in CATALOGUE["by_id"]
    assert CATALOGUE["by_id"][rule_id]["detector"] == "rule"
