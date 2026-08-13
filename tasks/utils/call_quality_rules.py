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
arriving long after an ignored turn reads as "the agent answered".

An earlier version tried to bound this by time — crediting a response only if
it arrived within a window. Verified against a real dev call, that was worse:
the agent replied 7.88s after the caller finished, and a 7.0s window reported a
genuine answer as ``agent_no_response``, critical severity. Turning slow
answers into missing ones is a far more damaging error than crediting a
check-in.

So any assistant line before the next caller turn now counts as answering it,
and lateness is reported as ``dead_air``. Past ``long_dead_air_seconds`` the
finding carries ``possible_reengagement: true`` so the ambiguity is visible
without being decided. Adding ``[SilencePolicy]`` to voice-ai's allowlist would
resolve it properly — Phase 7.
"""

import json
import re
from datetime import datetime

# Bump on any change to detection semantics. Findings record it, so a
# recalibration can be told apart from the run that first raised an issue.
#   .1  initial ten detectors
#   .2  gap-suppression removed (structurally always-on) and the answer-window
#       cutoff removed (turned slow answers into critical false positives) —
#       both found by running .1 against real dev artifacts
# 2026-08-14.1  Phase 6 calibration against 87 prod artifacts: terminal-tool
#       handoff suppression, vad_split_caller_sentence, and cancelled/silent
#       responses no longer count as answers. Critical rate 34% -> 16%.
# 2026-08-14.2  Prod spot-check of the surviving criticals: a caller barging in
#       over a response the agent was already generating is not an ignored
#       turn (7 of 11 were this), and a tool call counts as acting on the turn.
#       Critical rate 16% -> 11%.
ANALYZER_VERSION = "2026-08-14.2"

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
                                            r"status=(?P<status>\S+).*?"
                                            r"twilio_sent=(?P<twilio_sent>\d+)", re.S)),
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
#
# `response_completed` is conditional: a response that ended `cancelled` having
# sent nothing to Twilio produced NO audible speech, so the caller experienced
# silence. Counting it as an answer would mask exactly the failure this feature
# exists to catch. See `_is_audible_response`.
_ASSISTANT_KINDS = frozenset({
    "assistant_transcript", "response_completed", "response_audio_complete",
})


def _is_audible_response(event):
    """Did the caller actually hear something from this event?"""
    if event.kind != "response_completed":
        return True
    try:
        if int(event.data.get("twilio_sent") or 0) > 0:
            return True
    except (TypeError, ValueError):
        return True          # unparseable — assume audible rather than invent a fault
    return False

# A tool result carrying any of these is treated as failed. Deliberately
# conservative and substring-based: the payload is truncated to 300 chars at
# the source, so structured parsing is not reliably possible.
_TOOL_ERROR_MARKERS = ("error", '"success": false', '"success":false', "exception", "traceback")

# Tools that legitimately END the conversation. After one of these fires, the
# agent going quiet and the stream stopping is the SUCCESS path, not silence.
#
# Calibrated against 87 prod artifacts (2026-08-14): `call_ended_mid_turn` was
# firing on 23% of calls, and 13 of the 14 inspected were callers saying "I want
# to speak to a person" — where `transfer_to_human` HAD been invoked and the
# call ended because it was handed to a human. Reporting those as failures would
# have been the single largest source of false criticals.
_TERMINAL_TOOLS = ("transfer", "forward", "end_call", "hangup")


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
                 "barge_in", "marks_outstanding", "ended_by_call_end",
                 "next_start_ts")

    def __init__(self, index, start_seq, start_ts):
        self.index = index
        self.start_seq = start_seq
        self.start_ts = start_ts
        self.next_start_ts = None     # when the caller began speaking again
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

    def resumed_within(self, seconds):
        """Did the caller start speaking again almost immediately?

        If so, this "turn" was never a finished thought — VAD ended it on a
        hesitation pause and the caller carried on. The agent staying quiet was
        correct behaviour, not a failure to respond.

        Verified on a real call: the caller said "Can you make it like tomorrow
        morning at…", paused ~1.2s, then "11 A.m.". Reported as a critical
        `agent_no_response` before this existed.
        """
        base = self.stop_ts or self.start_ts
        if base is None or self.next_start_ts is None:
            return False
        return 0 <= (self.next_start_ts - base).total_seconds() <= seconds

    def first_response_delay(self):
        """Seconds from the caller finishing until they HEAR the agent, or None.

        Only audible events count. `responses` also holds tool calls and
        barge-in markers, which are evidence the agent acted but are silent to
        the caller — measuring to those would report a call as responsive while
        the caller sat listening to nothing.
        """
        base = self.stop_ts or self.start_ts
        if base is None:
            return None
        first = next((r.ts for r in self.responses
                      if r.ts is not None and r.kind in _ASSISTANT_KINDS), None)
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


def _classify(obj):
    """One line dict → an Event, or None if it carries no signal for us."""
    msg = obj.get("msg") or ""
    for kind, pattern in _PATTERNS:
        match = pattern.match(msg)
        if match:
            return Event(
                seq=obj.get("seq"),
                ts=_parse_ts(obj.get("ts")),
                level=(obj.get("level") or "").lower(),
                kind=kind,
                msg=msg,
                data=match.groupdict(),
            )
    return None


def classify_lines(lines):
    """Classify already-structured ``{seq, ts, level, msg}`` dicts.

    This is the in-memory path: pull_render_logs holds exactly this list at the
    moment it stores an artifact, so analysis there costs no serialisation and
    no R2 round-trip. :func:`parse_artifact` is the stored-artifact path used
    for backfill.

    Unclassifiable lines are dropped — they carry no signal for these rules but
    still consumed a sequence number, which is why gap detection reads the
    header rather than recomputing from what survives here.
    """
    events = [event for event in (_classify(obj) for obj in lines) if event]
    events.sort(key=lambda e: (e.seq is None, e.seq))
    return events


def header_from_call(call):
    """Build the header fields `evaluate` needs from pull_render_logs' in-memory
    call dict, so the in-memory and stored-artifact paths agree on gap handling."""
    gaps = call.get("gaps") or []
    return {
        "sequence_gaps": gaps,
        "incomplete": bool(gaps),
        "call_complete": bool(call.get("complete")),
    }


def parse_artifact(jsonl_text):
    """Split an unzipped stored artifact into ``(header, [Event, ...])``."""
    header, lines = {}, []
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
        else:
            lines.append(obj)
    return header, classify_lines(lines)


def build_turns(events):
    """Group events into caller turns.

    A turn opens on a caller speech start and closes at the next one (or at
    call end). Every assistant line inside that span counts as answering the
    turn, regardless of how late it arrives — lateness is `dead_air`'s job, not
    a reason to call the turn unanswered.
    """
    turns, current, index = [], None, 0

    for event in events:
        if event.kind in ("caller_speech_start", "caller_speech_start_bargein"):
            if current is not None:
                current.end_seq = event.seq
                current.next_start_ts = event.ts
                # `generating=True` on the barge-in means the agent was ALREADY
                # producing a response to the turn now closing. Its
                # `Response audio complete` line is logged after this marker, so
                # a naive span check attributes it to the next turn and reports
                # the answered turn as ignored.
                #
                # Measured on prod: 7 of 11 headline criticals were this. The
                # caller interrupting the agent is not the agent ignoring the
                # caller — it is the opposite.
                if (event.kind == "caller_speech_start_bargein"
                        and str(event.data.get("generating", "")).lower() == "true"):
                    current.responses.append(event)
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
        elif event.kind == "tool_call":
            # The agent acting on what the caller said IS a response to it. The
            # spoken reply follows the tool, often after the caller has spoken
            # again — `switch_language` in particular rebuilds the session first.
            # Silence *after* a tool is dead air, not a turn the agent ignored.
            current.responses.append(event)
        elif event.kind in _ASSISTANT_KINDS and _is_audible_response(event):
            # Any audible assistant line before the next caller turn counts as
            # answering it, however slow. Gating this on a time window turned
            # merely-slow answers into critical "agent ignored the caller"
            # findings — verified against a real dev call where the agent
            # replied at 7.88s and a 7.0s window reported it as silence.
            # Slowness is what `dead_air` is for.
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

def sequence_gaps_are_structural():
    """``header['sequence_gaps']`` CANNOT be used to detect dropped lines.

    Verified against real dev artifacts on 2026-08-13: both showed 24 gap
    ranges and ``incomplete: true`` on a normal, complete 90-second call.

    The cause is by design. speako-voice-ai's ``CallContextFilter`` increments
    the per-call sequence for EVERY log record, but stamps the ``~ct~`` channel
    marker on only the audio-timeline subset. The Render query retrieves the
    marked lines alone, so every unmarked line leaves a hole. A gap therefore
    means "a line we never intended to capture", not "a line Render dropped" —
    which is what ``_sequence_gaps`` in render_log_parse.py means by leaving the
    judgement to a human.

    An earlier version of this module suppressed absence-based findings whenever
    a gap spanned the turn. Because gaps are universal, that silently disabled
    ``agent_no_response`` and ``short_utterance_dropped`` entirely — the whole
    point of the feature — while appearing to work.

    Genuine drop detection needs a SECOND counter in voice-ai that advances only
    on channel lines; a hole in that one would be real evidence. Recorded as
    Phase 7 in the plan. Until then the exposure is: if Render ever does drop an
    assistant line (needs >6,000 lines/min on one instance — far above current
    volume), that turn could be misreported as unanswered.
    """
    return True


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
        "long_dead_air_seconds": defaults.get("long_dead_air_seconds", 7.0),
        "vad_split_seconds": by_id["vad_split_caller_sentence"]
            .get("threshold", {})
            .get("resume_within_seconds", 2.0),
    }


def evaluate(header, events, catalogue):
    """Run every deterministic rule. Returns a list of finding dicts.

    ``catalogue`` must be the output of :func:`index_catalogue`.
    """
    # NOT header['sequence_gaps'] / header['incomplete'] — both are structurally
    # true on every call and say nothing about loss. See
    # :func:`sequence_gaps_are_structural`. `call_complete` is the one honest
    # signal in the header: false means we never saw the call end, so the tail
    # of the trace really is missing.
    tail_missing = header.get("call_complete") is False
    confidence = "degraded" if tail_missing else "high"

    turns = build_turns(events)
    findings = []
    unanswered = []

    # Sequence at which the conversation was deliberately handed off or ended.
    # A caller turn at or after this point is not owed a spoken reply.
    terminal_seq = next(
        (e.seq for e in events
         if e.kind == "tool_call"
         and any(t in (e.data.get("name") or "").lower() for t in _TERMINAL_TOOLS)),
        None,
    )

    for turn in turns:
        handed_off = (
            terminal_seq is not None
            and turn.start_seq is not None
            and turn.end_seq is not None
            and turn.start_seq <= terminal_seq <= turn.end_seq
        )

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

        # "No response" is only meaningful for a turn we actually heard, and
        # only if the agent still owed the caller a reply.
        if turn.status == "ok" and not turn.answered and not handed_off:
            evidence = {
                "turn": turn.index,
                "seq": turn.start_seq,
                "transcript": turn.transcript,
                "transcript_chars": turn.transcript_chars,
            }

            # The caller carried straight on — VAD ended the turn on a pause,
            # it was not the agent ignoring them. Different problem, different
            # fix, and NOT critical.
            if turn.resumed_within(catalogue["vad_split_seconds"]):
                findings.append(_finding(
                    catalogue, "vad_split_caller_sentence",
                    evidence=evidence, confidence=confidence,
                ))
            else:
                unanswered.append(turn)
                findings.append(_finding(
                    catalogue,
                    "short_utterance_dropped"
                    if turn.transcript_chars <= catalogue["short_max_chars"]
                    else "agent_no_response",
                    evidence=evidence, confidence=confidence,
                ))
            if not turn.resumed_within(catalogue["vad_split_seconds"]) and turn.ended_by_call_end:
                findings.append(_finding(
                    catalogue, "call_ended_mid_turn",
                    evidence=evidence, confidence=confidence,
                ))

        delay = turn.first_response_delay()
        if delay is not None and delay > catalogue["dead_air_seconds"]:
            evidence = {"turn": turn.index, "seq": turn.start_seq,
                        "seconds": round(delay, 2)}
            # Past this point the "answer" may actually be a silence-policy
            # re-engagement prompt rather than a reply to what the caller said.
            # [SilencePolicy] lines are not in voice-ai's capture allowlist, so
            # we can flag the ambiguity but not resolve it — see Phase 7.
            if delay > catalogue["long_dead_air_seconds"]:
                evidence["possible_reengagement"] = True
            findings.append(_finding(catalogue, "dead_air", evidence=evidence))

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
            confidence=confidence,
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
    """Stored-artifact entry point: artifact text → findings. Used by backfill."""
    header, events = parse_artifact(jsonl_text)
    return evaluate(header, events, catalogue)


def analyze_call(call, catalogue):
    """In-memory entry point: pull_render_logs' call dict → findings."""
    return evaluate(header_from_call(call), classify_lines(call.get("lines") or []),
                    catalogue)


def summarize(findings):
    """Compact summary merged into ``location_conversations.raw_metadata`` so the
    conversations list can badge a row without joining or fetching R2."""
    if not findings:
        return {"quality_severity": None, "quality_count": 0, "quality_rules": []}
    severity = ("critical" if any(f["severity"] == "critical" for f in findings)
                else "moderate" if any(f["severity"] == "moderate" for f in findings)
                else "info")
    return {
        "quality_severity": severity,
        "quality_count": len(findings),
        "quality_rules": sorted(f["rule_id"] for f in findings),
    }
