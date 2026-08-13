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
    analyze_call,
    index_catalogue,
    parse_artifact,
    summarize,
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
        line(1, SPEECH_START, ts=at(10)),
        line(2, SPEECH_STOP, ts=at(11)),
        line(3, caller("yeah"), ts=at(11)),
        # Caller waits, gets nothing, and gives up on it 9s later. A gap this
        # long is a real non-response, not a hesitation pause mid-sentence.
        line(4, SPEECH_START, ts=at(20)),
        line(5, caller("hello? are you there"), ts=at(21)),
        line(6, ASSISTANT, ts=at(22)),
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
        line(1, SPEECH_START, ts=at(10)),
        line(2, caller("I would like to book a table for four people on Friday"), ts=at(12)),
        line(3, SPEECH_START, ts=at(22)),
        line(4, caller("hello"), ts=at(23)),
        line(5, ASSISTANT, ts=at(24)),
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

def test_sequence_gaps_do_not_suppress_findings():
    """REGRESSION. Gaps are structural, not evidence of loss: voice-ai's
    CallContextFilter advances the sequence for every log record but marks only
    the ~ct~ channel subset, so every real artifact has holes — both dev
    artifacts showed 24 gap ranges on a complete call.

    An earlier version suppressed absence-based findings whenever a gap spanned
    the turn, which disabled the headline detector on every production call
    while looking like 'no issues found'.
    """
    findings = analyze(artifact([
        line(1, SPEECH_START),
        line(2, caller("yeah")),
        line(9, SPEECH_START),
        line(10, caller("hello")),
        line(11, ASSISTANT),
    ], gaps=[[3, 8]]), CATALOGUE)
    assert "short_utterance_dropped" in rules(findings)
    assert one(findings, "short_utterance_dropped")["confidence"] == "high"


def test_realistic_gap_density_still_yields_findings():
    """Mirrors the shape of a real artifact: heavy structural gaps, incomplete
    flagged true, call complete."""
    findings = analyze(artifact([
        line(1, SPEECH_START),
        line(12, SPEECH_STOP),
        line(13, caller("yeah")),
        line(30, SPEECH_START),
        line(40, caller("hello are you there")),
        line(41, ASSISTANT),
        line(60, CALL_END),
    ], gaps=[[2, 11], [14, 29], [31, 39], [42, 59]], incomplete=True), CATALOGUE)
    assert "short_utterance_dropped" in rules(findings)


def test_missing_call_end_downgrades_confidence():
    """`call_complete: false` is the one honest header signal — we never saw
    the call end, so the tail of the trace really is missing."""
    findings = analyze(artifact([
        line(1, SPEECH_START, ts=at(10)),
        line(2, caller("yeah"), ts=at(11)),
        line(3, SPEECH_START, ts=at(25)),
        line(4, caller("hello"), ts=at(26)),
        line(5, ASSISTANT, ts=at(27)),
    ], call_complete=False), CATALOGUE)
    assert one(findings, "short_utterance_dropped")["confidence"] == "degraded"


def test_slow_answer_is_dead_air_not_a_missing_response():
    """REGRESSION. Taken from a real dev call: the agent replied 7.88s after
    the caller finished. An answer-window cutoff reported that genuine reply as
    a critical `agent_no_response`. Lateness is dead air, never absence.
    """
    findings = analyze(artifact([
        line(1, SPEECH_START, ts=at(10)),
        line(2, SPEECH_STOP, ts=at(12)),
        line(3, caller("Hi, can I make a reservation for next Wednesday"), ts=at(12)),
        line(4, ASSISTANT, ts=at(25)),
        # 8000 bytes = 1s of speech; confirmed at 26s, so playback began at 25s
        line(5, "[Azure] Response completed: id=r1 status=completed audio_chunks=1 "
                "audio_bytes=8000 twilio_sent=1 transcript='ok'", ts=at(25)),
        line(6, "[PlaybackClock] response playback confirmed (ack-last): id=r1 gen=1", ts=at(26)),
    ]), CATALOGUE)
    assert "agent_no_response" not in rules(findings)
    assert "short_utterance_dropped" not in rules(findings)
    finding = one(findings, "dead_air")
    assert finding["evidence"]["seconds"] == 13.0
    assert finding["evidence"]["possible_reengagement"] is True


def test_moderately_slow_answer_is_not_flagged_as_reengagement():
    findings = analyze(artifact([
        line(1, SPEECH_START, ts=at(10)),
        line(2, SPEECH_STOP, ts=at(12)),
        line(3, caller("a table for four please"), ts=at(12)),
        line(4, "[Azure] Response completed: id=r1 status=completed audio_chunks=1 "
                "audio_bytes=8000 twilio_sent=1 transcript='ok'", ts=at(22)),
        line(5, "[PlaybackClock] response playback confirmed (ack-last): id=r1 gen=1", ts=at(23)),
    ]), CATALOGUE)
    assert "possible_reengagement" not in one(findings, "dead_air")["evidence"]


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
        line(4, "[Azure] Response completed: id=r1 status=completed audio_chunks=1 "
                "audio_bytes=8000 twilio_sent=1 transcript='ok'", ts=at(22)),
        line(5, "[PlaybackClock] response playback confirmed (ack-last): id=r1 gen=1", ts=at(23)),
    ]), CATALOGUE)
    assert one(findings, "dead_air")["evidence"]["seconds"] == 10.0


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
        line(1, SPEECH_START, ts=at(10)), line(2, caller("yeah"), ts=at(11)),
        line(3, SPEECH_START, ts=at(25)), line(4, caller("okay"), ts=at(26)),
        line(5, SPEECH_START, ts=at(40)), line(6, caller("hello"), ts=at(41)),
        line(7, ASSISTANT, ts=at(42)),
    ]), CATALOGUE)
    finding = one(findings, "repeated_no_response")
    assert finding["evidence"]["count"] == 2
    assert finding["severity"] == "critical"


# ── Output contract ──────────────────────────────────────────────────────────

def test_one_finding_per_rule_per_call_with_occurrence_count():
    """The DB has UNIQUE (location_conversation_id, rule_id), so the engine
    must collapse repeats rather than let an insert fail."""
    findings = analyze(artifact([
        line(1, SPEECH_START, ts=at(10)), line(2, caller("yeah"), ts=at(11)),
        line(3, SPEECH_START, ts=at(25)), line(4, caller("okay"), ts=at(26)),
        line(5, SPEECH_START, ts=at(40)), line(6, caller("hello"), ts=at(41)),
        line(7, ASSISTANT, ts=at(42)),
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


# ── Phase 3: the in-memory path used by pull_render_logs ─────────────────────

def test_in_memory_path_matches_the_stored_artifact_path():
    """analyze_call() runs on pull_render_logs' live call dict; analyze() runs
    on the stored artifact during backfill. They must agree, or a backfilled
    finding would differ from the one raised live for the same call."""
    lines = [
        {"seq": 1, "ts": at(10), "level": "info", "msg": SPEECH_START},
        {"seq": 2, "ts": at(11), "level": "info", "msg": SPEECH_STOP},
        {"seq": 3, "ts": at(11), "level": "info", "msg": caller("yeah")},
        {"seq": 4, "ts": at(12), "level": "info", "msg": CALL_END},
    ]
    call = {"call_sid": "CAtest", "lines": lines, "gaps": [], "complete": True}

    from_memory = analyze_call(call, CATALOGUE)
    from_stored = analyze(artifact([json.dumps(line_dict) for line_dict in lines]),
                          CATALOGUE)

    assert rules(from_memory) == rules(from_stored)
    assert "short_utterance_dropped" in rules(from_memory)


def test_summary_takes_the_worst_severity():
    findings = analyze(artifact([
        line(1, SPEECH_START),
        line(2, caller("yeah")),
        line(3, "[Azure] VAD config mismatch: threshold", level="warning"),
        line(4, CALL_END),
    ]), CATALOGUE)
    summary = summarize(findings)
    assert summary["quality_severity"] == "critical"   # not the moderate VAD one
    assert summary["quality_count"] == len(findings)
    assert summary["quality_rules"] == sorted(summary["quality_rules"])


def test_summary_of_a_clean_call():
    assert summarize([]) == {
        "quality_severity": None, "quality_count": 0, "quality_rules": [],
    }


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


# ── Phase 6 calibration fixes ────────────────────────────────────────────────

def test_vad_split_is_moderate_not_a_critical_non_response():
    """REGRESSION, from a real dev call. The caller said 'Can you make it like
    tomorrow morning at…', paused ~1.2s, then '11 A.m.'. The agent correctly
    waited. Before this rule that was reported as a CRITICAL agent_no_response.
    """
    findings = analyze(artifact([
        line(1, SPEECH_START, ts=at(10)),
        line(2, SPEECH_STOP, ts=at(15)),
        line(3, caller("Can you make it like tomorrow morning at"), ts=at(15)),
        line(4, SPEECH_START, ts=at(16)),          # caller carries on, 1.2s later
        line(5, caller("11 A.m."), ts=at(18)),
        line(6, ASSISTANT, ts=at(20)),
    ]), CATALOGUE)
    finding = one(findings, "vad_split_caller_sentence")
    assert finding["severity"] == "moderate"
    assert "agent_no_response" not in rules(findings)
    assert "short_utterance_dropped" not in rules(findings)
    assert "call_ended_mid_turn" not in rules(findings)


def test_cancelled_response_with_no_audio_is_not_an_answer():
    """A response that ended `cancelled` having sent nothing to Twilio was
    never heard by the caller, so it cannot count as answering the turn."""
    silent = ("[Azure] Response completed: id=resp_x status=cancelled "
              "audio_chunks=0 audio_bytes=0 twilio_sent=0 transcript=''")
    findings = analyze(artifact([
        line(1, SPEECH_START, ts=at(10)),
        line(2, caller("I would like to book a table for four on Friday"), ts=at(12)),
        line(3, silent, ts=at(13)),
        line(4, SPEECH_START, ts=at(30)),
        line(5, caller("hello"), ts=at(31)),
        line(6, ASSISTANT, ts=at(32)),
    ]), CATALOGUE)
    assert "agent_no_response" in rules(findings)


def test_completed_response_that_reached_twilio_is_an_answer():
    heard = ("[Azure] Response completed: id=resp_x status=completed "
             "audio_chunks=27 audio_bytes=103509 twilio_sent=27 transcript='ok'")
    findings = analyze(artifact([
        line(1, SPEECH_START, ts=at(10)),
        line(2, caller("I would like to book a table for four on Friday"), ts=at(12)),
        line(3, heard, ts=at(13)),
        line(4, CALL_END, ts=at(20)),
    ]), CATALOGUE)
    assert "agent_no_response" not in rules(findings)


def test_transfer_to_human_is_not_an_unanswered_turn():
    """REGRESSION, calibrated against 87 prod artifacts. `call_ended_mid_turn`
    was firing on 23% of prod calls; 13 of 14 inspected were callers asking for
    a person where transfer_to_human HAD fired and the call ended because it
    was handed to a human. That is the success path."""
    findings = analyze(artifact([
        line(1, SPEECH_START, ts=at(10)),
        line(2, caller("I want to speak to a person"), ts=at(12)),
        line(3, "[Azure] Tool call: transfer_to_human call_id=x params={}", ts=at(13)),
        line(4, CALL_END, ts=at(15)),
    ]), CATALOGUE)
    assert "agent_no_response" not in rules(findings)
    assert "call_ended_mid_turn" not in rules(findings)


def test_a_genuine_drop_still_fires_when_no_terminal_tool_ran():
    findings = analyze(artifact([
        line(1, SPEECH_START, ts=at(10)),
        line(2, caller("I want to speak to a person"), ts=at(12)),
        line(3, CALL_END, ts=at(15)),
    ]), CATALOGUE)
    assert {"agent_no_response", "call_ended_mid_turn"} <= rules(findings)


def test_caller_barging_in_over_the_agent_is_not_an_ignored_turn():
    """REGRESSION, from a prod spot-check where 7 of 11 headline criticals were
    this. `generating=True` on the barge-in means the agent was ALREADY
    producing a reply to the turn now closing; its Response-audio-complete line
    is logged after the barge-in marker, so a naive span check blamed the agent
    for a turn it was actively answering."""
    findings = analyze(artifact([
        line(1, SPEECH_START, ts=at(10)),
        line(2, caller("I want to make a booking"), ts=at(12)),
        line(3, "[Azure] Caller speech started (barge-in: generating=True "
                "buffered_marks=2); stopping playback", ts=at(14)),
        line(4, "[Azure] Response audio complete: azure_chunks=12 azure_bytes=44000 "
                "twilio_sent=12", ts=at(14)),
        line(5, caller("actually make it seven"), ts=at(16)),
        line(6, ASSISTANT, ts=at(18)),
    ]), CATALOGUE)
    assert "agent_no_response" not in rules(findings)
    assert "short_utterance_dropped" not in rules(findings)


def test_barge_in_while_the_agent_was_idle_still_reports_the_drop():
    """generating=False means nothing was being produced — the turn really did
    go unanswered."""
    findings = analyze(artifact([
        line(1, SPEECH_START, ts=at(10)),
        line(2, caller("I want to make a booking"), ts=at(12)),
        line(3, "[Azure] Caller speech started (barge-in: generating=False "
                "buffered_marks=0); stopping playback", ts=at(30)),
        line(4, caller("hello?"), ts=at(31)),
        line(5, ASSISTANT, ts=at(33)),
    ]), CATALOGUE)
    assert "agent_no_response" in rules(findings)


def test_a_tool_call_counts_as_the_agent_acting_on_the_turn():
    """switch_language rebuilds the session before speaking, so the reply lands
    after the caller has spoken again. Acting on the request is not ignoring it."""
    findings = analyze(artifact([
        line(1, SPEECH_START, ts=at(10)),
        line(2, caller("मंदिर में कितने ब्राह्मण हैं?"), ts=at(12)),
        line(3, "[Azure] Tool call: switch_language call_id=x params={'language': 'hi'}", ts=at(13)),
        line(4, SPEECH_START, ts=at(30)),
        line(5, caller("hello"), ts=at(31)),
        line(6, ASSISTANT, ts=at(33)),
    ]), CATALOGUE)
    assert "agent_no_response" not in rules(findings)


def test_dead_air_is_not_the_length_of_a_long_reply():
    """REGRESSION, the biggest measurement error in the project. Every log
    marker (Response audio complete / Assistant transcript / Response completed)
    fires only once the WHOLE reply has been generated. Measuring to one of them
    measured how long the agent TALKED, not how long the caller WAITED.

    Real prod case: a caller asked "you tell me your service", was reported as
    19.1s of dead air, and had in fact been answered in 1.6s — the agent then
    spoke for 43 seconds listing services.

    Here: caller stops at 12s, the reply is 40s of speech (320000 bytes)
    confirmed at 54s, so playback began at 14s — a 2s wait, not 30s.
    """
    findings = analyze(artifact([
        line(1, SPEECH_START, ts=at(10)),
        line(2, SPEECH_STOP, ts=at(12)),
        line(3, caller("you tell me your service"), ts=at(12)),
        line(4, "[Azure] Response completed: id=r9 status=completed audio_chunks=80 "
                "audio_bytes=320000 twilio_sent=80 transcript='...'", ts=at(42)),
        line(5, "[PlaybackClock] response playback confirmed (ack-last): id=r9 gen=1", ts=at(54)),
    ]), CATALOGUE)
    assert "dead_air" not in rules(findings)


def test_dead_air_abstains_when_playback_was_never_confirmed():
    """A response the caller interrupted never gets a playback confirmation, so
    its start cannot be derived. Abstain rather than fall back to a marker that
    overstates the wait."""
    findings = analyze(artifact([
        line(1, SPEECH_START, ts=at(10)),
        line(2, SPEECH_STOP, ts=at(12)),
        line(3, caller("a table for four please"), ts=at(12)),
        line(4, ASSISTANT, ts=at(40)),
    ]), CATALOGUE)
    assert "dead_air" not in rules(findings)
