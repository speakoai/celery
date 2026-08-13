"""
LLM triage over a per-call trace.

Phase 5 of docs/plans/call-quality-issues.md (speako-workspace).

The deterministic engine in call_quality_rules.py catches what arithmetic can
catch. This module handles the open-ended half — the agent answered a different
question, looped, spoke the wrong language, read an address unspeakably — by
having a model read the reconstructed conversation with the catalogue as its
checklist.

Structure mirrors render_log_parse / pull_render_logs: everything except the
API call itself is a pure function, so prompt construction and (especially)
response validation are unit-testable without a network or an API key.

──────────────────────────────────────────────────────────────────────────────
WHY VALIDATION IS THE IMPORTANT PART
──────────────────────────────────────────────────────────────────────────────
A model asked "what went wrong on this call?" will always find something. Three
constraints keep that from becoming noise:

1. **Closed vocabulary.** Only `rule_id`s marked ``detector: "llm"`` in the
   catalogue are accepted. A finding the catalogue does not describe cannot be
   raised, so every finding has a human-written explanation and suggested fix.
2. **Citations must exist.** Every finding must cite the `seq` numbers it relied
   on, and each must appear in the trace we sent. A model that invents evidence
   loses the finding.
3. **The transcript is the only input.** No audio, no summary, no other calls —
   so a claim can always be checked against the log lines shown beside it.

⚠️ **PII.** The prompt contains caller utterances: names, phone numbers, booking
details. This is consistent with existing practice (tasks/summarize_chat_sessions
sends chat transcripts to the same provider) but it is a real data-egress
decision, and it is why the pass is env-flagged off by default.
"""

import json
import os

# Off unless explicitly enabled. Turning this on for a Render CRON requires a
# Manual Deploy afterwards — crons snapshot env at deploy time, so saving the
# variable alone leaves every scheduled run on the previous snapshot.
ENABLED_ENV = "CALL_QUALITY_LLM_ENABLED"
MODEL_ENV = "OPENAI_CALL_QUALITY_MODEL"
DEFAULT_MODEL = "gpt-4o-mini"

LLM_ANALYZER_VERSION = "2026-08-13.1"

# Bounds. The sweep runs in a 5-minute cron; an unbounded triage pass would
# turn a backlog into an overrun.
MAX_CALLS_PER_RUN = 10
REQUEST_TIMEOUT_SECONDS = 25
MAX_OUTPUT_TOKENS = 900
MAX_TRANSCRIPT_TURNS = 60

_SYSTEM = (
    "You review transcripts of phone calls handled by an AI voice agent for a "
    "service business. You identify moments that would embarrass the business "
    "or frustrate the caller.\n\n"
    "Rules you must follow:\n"
    "- Only report problems that match one of the issue types given to you. "
    "Use the exact id.\n"
    "- Cite the `seq` numbers of the lines that show the problem. Only cite "
    "numbers that appear in the transcript.\n"
    "- Report nothing if the call went acceptably. An empty list is a good "
    "answer and is expected for most calls.\n"
    "- Judge only what the transcript shows. Do not speculate about audio "
    "quality, tone, or anything not written down.\n"
    "- One finding per issue type at most."
)


def is_enabled():
    return os.getenv(ENABLED_ENV, "").strip().lower() in ("1", "true", "yes", "on")


def build_transcript(events):
    """Reconstruct the conversation from classified trace events.

    Only what was said, with the `seq` that carries it — the model is asked to
    cite those numbers, so they must be the ones it can see. Timings come from
    the deterministic pass; repeating them here invites the model to
    second-guess arithmetic it cannot do.
    """
    lines = []
    for event in events:
        if event.kind == "caller_transcript":
            text = (event.data.get("text") or "").strip()
            if text:
                lines.append(f"[{event.seq}] caller: {text}")
        elif event.kind == "caller_transcript_empty":
            lines.append(f"[{event.seq}] caller: (spoke, but nothing was transcribed)")
        elif event.kind == "caller_transcript_failed":
            lines.append(f"[{event.seq}] caller: (transcription failed)")
        elif event.kind == "assistant_transcript":
            text = event.msg.split(":", 1)[1].strip() if ":" in event.msg else ""
            if text:
                lines.append(f"[{event.seq}] agent: {text}")
        elif event.kind == "tool_call":
            lines.append(f"[{event.seq}] (agent used tool: {event.data.get('name')})")
    return lines[:MAX_TRANSCRIPT_TURNS]


def build_prompt(transcript_lines, rule_findings, catalogue):
    """The user message: issue checklist, deterministic hints, transcript."""
    checklist = [
        entry for entry in catalogue["by_id"].values()
        if entry.get("detector") == "llm"
    ]
    checklist_text = "\n".join(
        f"- {entry['id']}: {entry['title']} — {entry['symptom']}"
        for entry in sorted(checklist, key=lambda e: e["id"])
    )
    hints = ", ".join(sorted({f["rule_id"] for f in rule_findings})) or "none"

    return (
        "Issue types you may report:\n"
        f"{checklist_text}\n\n"
        "Automated checks already flagged these on this call (context only — do "
        "not repeat them, and do not assume they are related to what you find): "
        f"{hints}\n\n"
        "Transcript, one line per turn, prefixed with its log sequence number:\n"
        + "\n".join(transcript_lines)
        + "\n\nRespond with JSON: "
        '{"findings": [{"rule_id": "...", "seq": [12, 14], "detail": '
        '"one sentence naming what the caller experienced"}]}'
    )


# An LLM finding the deterministic pass has already explained differently.
# The model reads a transcript and sees the caller apparently restating
# themselves; the rule engine, which has the timings, knows the turn was split
# by VAD mid-sentence. The rule wins — it has evidence the model cannot see.
_SUPPRESSED_BY = {
    "caller_repeated_themselves": {"vad_split_caller_sentence"},
}

# LLM findings a TOOL CALL disproves outright. The model reads a transcript in
# which the caller asks for a person and the conversation stops, and concludes
# the request went nowhere — but the call ends precisely BECAUSE the handoff
# succeeded and the stream moved to the conference.
#
# Measured on the prod backfill: `dead_end_transfer` fired 13 times and
# `transfer_to_human` had actually run on 13 of 13. This is the same false
# positive already fixed in the rule engine, returning by another route, so it
# is settled with the same evidence rather than by asking the model nicely.
_DISPROVED_BY_TOOL = {
    "dead_end_transfer": ("transfer", "forward"),
}


def validate(raw, transcript_lines, catalogue, rule_findings=(), tools_used=()):
    """Turn a model response into findings, dropping anything unsupported.

    Silently discards rather than raising: a malformed response must cost us the
    triage pass for one call, never the sweep.
    """
    allowed = {
        entry_id for entry_id, entry in catalogue["by_id"].items()
        if entry.get("detector") == "llm"
    }
    valid_seqs = set()
    for line in transcript_lines:
        if line.startswith("["):
            try:
                valid_seqs.add(int(line[1:line.index("]")]))
            except (ValueError, IndexError):
                continue

    try:
        payload = json.loads(raw) if isinstance(raw, str) else raw
        candidates = payload.get("findings") or []
    except (ValueError, AttributeError):
        return []

    already = {f["rule_id"] for f in rule_findings}
    findings, seen = [], set()
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        rule_id = candidate.get("rule_id")
        if rule_id not in allowed or rule_id in seen:
            continue
        if _SUPPRESSED_BY.get(rule_id, set()) & already:
            continue
        markers = _DISPROVED_BY_TOOL.get(rule_id)
        if markers and any(m in (name or "").lower()
                           for name in tools_used for m in markers):
            continue

        cited = [s for s in _as_ints(candidate.get("seq")) if s in valid_seqs]
        if not cited:
            # No checkable evidence — the whole point of the citation rule.
            continue

        seen.add(rule_id)
        entry = catalogue["by_id"][rule_id]
        detail = str(candidate.get("detail") or "").strip()[:400]
        findings.append({
            "rule_id": rule_id,
            "severity": entry["severity"],
            "detector": "llm",
            "confidence": "high",
            "title": entry["title"],
            "explanation": (detail + " " if detail else "") + entry["likely_cause"],
            "suggested_fix": entry["suggested_fix"],
            "evidence": {"seq": cited[0], "cited_seq": cited, "model_detail": detail},
            "analyzer_version": LLM_ANALYZER_VERSION,
            "catalogue_version": catalogue["catalogue_version"],
        })
    return findings


def _as_ints(value):
    if isinstance(value, (int, float)):
        return [int(value)]
    if isinstance(value, list):
        out = []
        for item in value:
            try:
                out.append(int(item))
            except (TypeError, ValueError):
                continue
        return out
    return []


def triage(events, rule_findings, catalogue, client=None):
    """Run the triage pass for one call. Returns [] on any failure.

    ``client`` is injectable so tests never construct a real OpenAI client.
    """
    transcript = build_transcript(events)
    # Two turns is not a conversation; there is nothing for a model to judge.
    if len(transcript) < 3:
        return []

    if client is None:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return []
        from openai import OpenAI
        client = OpenAI(api_key=api_key, timeout=REQUEST_TIMEOUT_SECONDS)

    response = client.chat.completions.create(
        model=os.getenv(MODEL_ENV, DEFAULT_MODEL),
        messages=[
            {"role": "system", "content": _SYSTEM},
            {"role": "user", "content": build_prompt(transcript, rule_findings, catalogue)},
        ],
        response_format={"type": "json_object"},
        max_tokens=MAX_OUTPUT_TOKENS,
        temperature=0,
    )
    return validate(response.choices[0].message.content, transcript, catalogue,
                    rule_findings=rule_findings,
                    tools_used=[e.data.get("name") for e in events
                                if e.kind == "tool_call"])
