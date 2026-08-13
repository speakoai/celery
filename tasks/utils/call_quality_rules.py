"""
Deterministic call-quality detection over a per-call server-log artifact.

Phase 2 of docs/plans/call-quality-issues.md (speako-workspace).

Input is the artifact written by tasks/utils/render_log_parse.py — a header
line plus one ``{seq, ts, level, msg}`` object per captured log line. Output is
a list of findings, each keyed to an entry in
``tasks/data/call_issue_catalogue.json``.

Pure functions only: no celery, DB, R2 or network imports, so this can be
unit-tested directly under a modern Python (the deployed celery pin cannot even
be imported). tasks/analyze_call_quality.py owns the I/O.

──────────────────────────────────────────────────────────────────────────────
WHY THIS DOES NOT READ ``t_response_created`` FROM ``[TurnMetrics]``
──────────────────────────────────────────────────────────────────────────────
The obvious implementation — "if the turn's ``t_response_created`` is null the
agent never answered" — is WRONG against the data we actually have.

``speako-voice-ai/modules/azure_turn_metrics.py`` finalises a turn inside
``on_transcription()`` and writes its ``[TurnMetrics]`` line THERE.
``on_response_created()`` runs afterwards and mutates the record in memory
only; ``_finalize()`` is guarded against re-entry, so the line is emitted once,
early. The logged JSON therefore carries ``t_response_created: null`` for
essentially every turn, answered or not.

A second gap: ``_finalize`` is reached only from ``on_transcription`` or from
being superseded by the next turn, so a final turn that is never transcribed
and never superseded is never logged at all — precisely the "caller said 'yeah'
at the end and the agent said nothing" case this feature exists to catch.

So response presence is derived from the LINE SEQUENCE instead: does an
assistant-side line appear after the caller's transcript and before the next
caller turn? That is also the only method that works for backfill, because the
artifacts already stored in R2 contain the incomplete `[TurnMetrics]` lines and
no forward fix can repair them.

``[TurnMetrics]`` is still parsed — ``barge_in`` and ``marks_outstanding`` are
written before the line is emitted and so ARE trustworthy.

──────────────────────────────────────────────────────────────────────────────
KNOWN LIMITATION — SILENCE-POLICY RE-ENGAGEMENT IS NOT CAPTURED
──────────────────────────────────────────────────────────────────────────────
``[SilencePolicy]`` lines are NOT in voice-ai's ~ct~ allowlist, so a check-in
prompt ("are you still there?") is indistinguishable from a genuine answer by
tag alone — both surface as an assistant transcript.

That direction of error hides bugs rather than inventing them: a check-in
arriving long after an ignored turn would otherwise read as "the agent
answered". We bound it by time — a response only counts as answering the turn
if it begins within ``answer_window_seconds``; later ones are reported as dead
air instead. Adding ``[SilencePolicy]`` to the allowlist would remove the
ambiguity entirely and is recorded as a follow-up in the plan.
"""

import json
import re
from datetime import datetime

ANALYZER_VERSION = "2026-08-13.1"

# ── Line classification ──────────────────────────────────────────────────────
# Matched against `msg`, which render_log_parse has already stripped of the
# `~ct~<call_sid>#<seq> ` stamp. Order matters: the first match wins, so the
# barge-in variant of "Caller speech started" is tested before the plain one.

_PATTERNS = (
    ("caller_speech_start_bargein",
     re.compile(r"^\[Azure\] Caller speech started \(barge-in: "
                r"generating=(?P<generating>\w+) buffered_marks=(?P<marks>\d+)\)")),
    ("caller_speech_start",      re.compile(r"^\[Azure\] Caller speech started")),
    ("caller_speech_stop",       re.compile(r"^\[Azure\] Caller stopped speaking")),
    ("caller_committed",         re.compile(r"^\[Azure\] Input audio buffer committed")),
    ("caller_transcript",        re.compile(r"^\[Azure\] Caller transcript: (?P<text>.*)", re.S)),
    ("caller_transcript_empty",  re.compile(r"^\[Azure\] EMPTY CALLER TRANSCRIPT")),
    ("caller_transcript_failed", re.compile(r"^\[Azure\] Caller transcription FAILED")),
    ("assistant_transcript",     re.compile(r"^\[Azure\] Assistant transcript:")),
    ("response_completed",       re.compile(r"^\[Azure\] Response completed: id=\S* "
                                            r"status=(?P<status>\S+)")),
    ("response_audio_complete",  re.compile(r"^\[Azure\] Response audio complete:")),
    ("turn_metrics",             re.compile(r"^\[TurnMetrics\] (?P<json>\{.*)", re.S)),
    ("vad_mismatch",             re.compile(r"^\[Azure\] VAD config mismatch")),
    ("tool_call",                re.compile(r"^\[Azure\] Tool call: (?P<name>\S+)")),
    ("tool_result",              re.compile(r"^\[Azure\] Tool result: (?P<name>\S+) "
                                            r"call_id=\S* result=(?P<result>.*)", re.S)),
    ("call_ended",               re.compile(r"^\[CallLifecycle\] Call ended")),
    ("stream_stopped",           re.compile(r"^\[Twilio/Azure\] Stream stopped")),
)

# An assistant-side line means the agent produced something for this turn.
_ASSISTANT_KINDS = frozenset({
    "assistant_transcript", "response_completed", "response_audio_complete",
})

# A tool result carrying any of these is treated as failed. Deliberately
# conservative and substring-based: the payload is truncated to 300 chars at
# the source, so structured parsing is not reliably possible.
_TOOL_ERROR_MARKERS = ("error", '"success": false', '"success":false', "exception", "traceback")


class Event:
    """One classified log line."""

    __slots__ = ("seq", "ts", "level", "kind", "msg", "data")

    def __init__(self, seq, ts, level, kind, msg, data):
        self.seq, self.ts, self.level = seq, ts, level
        self.kind, self.msg, self.data = kind, msg, data

    def __repr__(self):  # pragma: no cover - debugging aid
        return f"<Event #{self.seq} {self.kind}>"


class Turn:
    """One caller turn and whatever the agent did about it."""

    __slots__ = ("index", "start_seq", "start_ts", "stop_ts", "transcript",
                 "transcript_chars", "status", "responses", "end_seq",
                 "barge_in", "marks_outstanding", "ended_by_call_end")

    def __init__(self, index, start_seq, start_ts):
        self.index = index
        self.start_seq = start_seq
        self.start_ts = start_ts
        self.stop_ts = None
        self.transcript = None
        self.transcript_chars = 0
        self.status = "none"          # ok | empty | failed | none
        self.responses = []           # Event, assistant-side
        self.end_seq = None           # seq bounding this turn (next turn / call end)
        self.barge_in = False
        self.marks_outstanding = 0
        self.ended_by_call_end = False

    @property
    def answered(self):
        return bool(self.responses)

    def first_response_delay(self):
        """Seconds from the caller finishing to the agent's first line, or None."""
        base = self.stop_ts or self.start_ts
        if base is None or not self.responses:
            return None
        first = next((r.ts for r in self.responses if r.ts is not None), None)
        if first is None:
            return None
        return max(0.0, (first - base).total_seconds())


# ── Parsing ──────────────────────────────────────────────────────────────────

def _parse_ts(value):
    """Render timestamps are RFC3339; tolerate `Z` and missing values."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_artifact(jsonl_text):
    """Split an unzipped artifact into ``(header, [Event, ...])``.

    Unclassifiable lines are dropped — they carry no signal for these rules but
    still consumed a sequence number, which is why gap detection reads the
    header rather than recomputing from what survives here.
    """
    header, events = {}, []
    for raw in jsonl_text.splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except ValueError:
            continue
        if "_artifact" in obj:
            header = obj
            continue
        msg = obj.get("msg") or ""
        for kind, pattern in _PATTERNS:
            match = pattern.match(msg)
            if match:
                events.append(Event(
                    seq=obj.get("seq"),
                    ts=_parse_ts(obj.get("ts")),
                    level=(obj.get("level") or "").lower(),
                    kind=kind,
                    msg=msg,
                    data=match.groupdict(),
                ))
                break
    events.sort(key=lambda e: (e.seq is None, e.seq))
    return header, events


def build_turns(events, answer_window_seconds):
    """Group events into caller turns.

    A turn opens on a caller speech start and closes at the next one (or at
    call end). Assistant lines inside that span count as answering the turn
    only if they begin within ``answer_window_seconds`` of the caller
    finishing — see the silence-policy note in the module docstring. Lines
    with no usable timestamp are counted as answers, since dropping them would
    manufacture false "agent ignored the caller" findings.
    """
    turns, current, index = [], None, 0

    for event in events:
        if event.kind in ("caller_speech_start", "caller_speech_start_bargein"):
            if current is not None:
                current.end_seq = event.seq
                turns.append(current)
            index += 1
            current = Turn(index, event.seq, event.ts)
            if event.kind == "caller_speech_start_bargein":
                current.barge_in = str(event.data.get("generating", "")).lower() == "true"
                current.marks_outstanding = int(event.data.get("marks") or 0)
            continue

        if current is None:
            continue

        if event.kind == "caller_speech_stop":
            current.stop_ts = event.ts
        elif event.kind == "caller_transcript":
            current.transcript = (event.data.get("text") or "").strip()
            current.transcript_chars = len(current.transcript)
            current.status = "ok" if current.transcript else "empty"
        elif event.kind == "caller_transcript_empty":
            current.status = "empty"
        elif event.kind == "caller_transcript_failed":
            current.status = "failed"
        elif event.kind == "turn_metrics":
            _apply_turn_metrics(current, event)
        elif event.kind in _ASSISTANT_KINDS:
            base = current.stop_ts or current.start_ts
            if base is None or event.ts is None:
                current.responses.append(event)
            elif (event.ts - base).total_seconds() <= answer_window_seconds:
                current.responses.append(event)
        elif event.kind in ("call_ended", "stream_stopped"):
            current.end_seq = event.seq
            current.ended_by_call_end = True
            turns.append(current)
            current = None

    if current is not None:
        turns.append(current)
    return turns


def _apply_turn_metrics(turn, event):
    """Take only the fields written BEFORE the line is emitted (see docstring)."""
    try:
        record = json.loads(event.data.get("json") or "{}")
    except ValueError:
        return
    if record.get("barge_in"):
        turn.barge_in = True
    marks = record.get("marks_outstanding_at_speech_start") or 0
    turn.marks_outstanding = max(turn.marks_outstanding, int(marks))
    status = record.get("transcription_status")
    if status in ("ok", "empty", "failed") and turn.status == "none":
        turn.status = status


# ── Gap handling ─────────────────────────────────────────────────────────────

def _gap_overlaps(gaps, start_seq, end_seq):
    """Did we fail to retrieve any line inside this turn's span?

    Render drops application logs above 6,000 lines/min per instance without
    marking the gap, so an absence inside a hole is not evidence of silence.
    """
    if start_seq is None:
        return False
    upper = end_seq if end_seq is not None else float("inf")
    return any(not (gap_end < start_seq or gap_start > upper)
               for gap_start, gap_end in (gaps or []))


# ── Findings ─────────────────────────────────────────────────────────────────

def _finding(catalogue, rule_id, *, evidence, confidence="high", severity=None):
    """Build a finding, copying the catalogue's display text at write time so
    speako-agent-admin never needs its own copy of the catalogue."""
    entry = catalogue["by_id"][rule_id]
    return {
        "rule_id": rule_id,
        "severity": severity or entry["severity"],
        "detector": "rule",
        "confidence": confidence,
        "title": entry["title"],
        "explanation": entry["symptom"] + " " + entry["likely_cause"],
        "suggested_fix": entry["suggested_fix"],
        "evidence": evidence,
        "analyzer_version": ANALYZER_VERSION,
        "catalogue_version": catalogue["catalogue_version"],
    }


def index_catalogue(catalogue):
    """Prepare a catalogue dict for use: id lookup plus resolved thresholds."""
    defaults = catalogue.get("defaults", {})
    by_id = {entry["id"]: entry for entry in catalogue["entries"]}
    return {
        "catalogue_version": catalogue["catalogue_version"],
        "by_id": by_id,
        "short_max_chars": by_id["short_utterance_dropped"]
            .get("threshold", {})
            .get("max_chars", defaults.get("short_utterance_max_chars", 12)),
        "dead_air_seconds": by_id["dead_air"]
            .get("threshold", {})
            .get("seconds", defaults.get("dead_air_seconds", 3.5)),
        "min_repeat": by_id["repeated_no_response"]
            .get("threshold", {})
            .get("min_occurrences", 2),
        "answer_window_seconds": defaults.get("long_dead_air_seconds", 7.0),
    }


def evaluate(header, events, catalogue):
    """Run every deterministic rule. Returns a list of finding dicts.

    ``catalogue`` must be the output of :func:`index_catalogue`.
    """
    gaps = header.get("sequence_gaps") or []
    trace_incomplete = bool(header.get("incomplete"))
    turns = build_turns(events, catalogue["answer_window_seconds"])
    findings = []
    unanswered = []

    for turn in turns:
        # Absence-based rules are only trustworthy if we actually retrieved
        # every line in the turn's span.
        span_has_gap = _gap_overlaps(gaps, turn.start_seq, turn.end_seq)
        confidence = "degraded" if (span_has_gap or trace_incomplete) else "high"

        if turn.status == "empty":
            findings.append(_finding(
                catalogue, "stt_empty_transcript",
                evidence={"turn": turn.index, "seq": turn.start_seq},
            ))
        elif turn.status == "failed":
            findings.append(_finding(
                catalogue, "stt_transcription_failed",
                evidence={"turn": turn.index, "seq": turn.start_seq},
            ))

        # "No response" is only meaningful for a turn we actually heard.
        if turn.status == "ok" and not turn.answered and not span_has_gap:
            unanswered.append(turn)
            evidence = {
                "turn": turn.index,
                "seq": turn.start_seq,
                "transcript": turn.transcript,
                "transcript_chars": turn.transcript_chars,
            }
            if turn.transcript_chars <= catalogue["short_max_chars"]:
                findings.append(_finding(
                    catalogue, "short_utterance_dropped",
                    evidence=evidence, confidence=confidence,
                ))
            else:
                findings.append(_finding(
                    catalogue, "agent_no_response",
                    evidence=evidence, confidence=confidence,
                ))
            if turn.ended_by_call_end:
                findings.append(_finding(
                    catalogue, "call_ended_mid_turn",
                    evidence=evidence, confidence=confidence,
                ))

        delay = turn.first_response_delay()
        if delay is not None and delay > catalogue["dead_air_seconds"]:
            findings.append(_finding(
                catalogue, "dead_air",
                evidence={"turn": turn.index, "seq": turn.start_seq,
                          "seconds": round(delay, 2)},
            ))

        if turn.barge_in and turn.marks_outstanding > 0:
            findings.append(_finding(
                catalogue, "agent_talked_over_caller",
                evidence={"turn": turn.index, "seq": turn.start_seq,
                          "marks_outstanding": turn.marks_outstanding},
            ))

    if len(unanswered) >= catalogue["min_repeat"]:
        findings.append(_finding(
            catalogue, "repeated_no_response",
            evidence={"count": len(unanswered),
                      "turns": [t.index for t in unanswered],
                      "seq": unanswered[0].start_seq},
            confidence="degraded" if trace_incomplete else "high",
        ))

    for event in events:
        if event.kind == "vad_mismatch":
            findings.append(_finding(
                catalogue, "vad_config_mismatch",
                evidence={"seq": event.seq, "detail": event.msg[:300]},
            ))
        elif event.kind == "tool_result":
            result = (event.data.get("result") or "").lower()
            if any(marker in result for marker in _TOOL_ERROR_MARKERS):
                findings.append(_finding(
                    catalogue, "tool_call_failed",
                    evidence={"seq": event.seq,
                              "tool": event.data.get("name"),
                              "detail": event.msg[:300]},
                ))

    return _dedupe(findings)


def _dedupe(findings):
    """One finding per rule per call — the DB enforces this too
    (UNIQUE (location_conversation_id, rule_id)), so collapse here rather than
    letting an insert fail. The first occurrence keeps its evidence and a
    repeat count is recorded."""
    kept = {}
    for finding in findings:
        rule_id = finding["rule_id"]
        if rule_id in kept:
            kept[rule_id]["evidence"].setdefault("occurrences", 1)
            kept[rule_id]["evidence"]["occurrences"] += 1
            # A single degraded observation makes the aggregate degraded.
            if finding["confidence"] == "degraded":
                kept[rule_id]["confidence"] = "degraded"
        else:
            kept[rule_id] = finding
    return list(kept.values())


def analyze(jsonl_text, catalogue):
    """Convenience end-to-end entry point: artifact text → findings."""
    header, events = parse_artifact(jsonl_text)
    return evaluate(header, events, catalogue)
