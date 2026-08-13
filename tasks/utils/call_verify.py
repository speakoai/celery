"""
Pure verification core: does the recording support what the log claimed?

Split out of tasks/verify_call_findings.py so it can be unit-tested — that task
imports celery, which cannot load under Python 3.13 while the deployed pin is
3.11. Same reason render_log_parse and call_quality_rules are pure.

No DB, R2 or network here: callers hand in the log artifact text and the audio
bytes, and get back the evidence to merge onto a finding.
"""

from tasks.utils import call_audio as audio
from tasks.utils.call_quality_rules import parse_artifact


def log_seconds(iso_ts):
    """`2026-08-13T13:39:29.471Z` -> seconds past the hour.

    Only differences within one call are ever used, so an hour-relative clock is
    sufficient — and it sidesteps timezone handling entirely. A call spanning an
    hour boundary would wrap; at that point the offset simply fails to anchor
    and the verdict becomes `unaligned`, which is the honest outcome.
    """
    try:
        parts = str(iso_ts)[11:23].split(":")
        return int(parts[1]) * 60 + float(parts[2])
    except (TypeError, ValueError, IndexError):
        return None


def _at(dt):
    """Seconds past the hour, straight off the datetime.

    NOT via isoformat(): that omits microseconds entirely when they are zero,
    which silently shifted a fixed-offset string slice and made every whole-
    second timestamp unparseable.
    """
    return dt.minute * 60 + dt.second + dt.microsecond / 1_000_000


def _first_agent_log_seconds(events):
    for e in events:
        if e.kind == "assistant_transcript" and e.ts is not None:
            return _at(e.ts)
    return None


def _target_log_seconds(events, seq):
    for e in events:
        if e.seq == seq and e.ts is not None:
            return _at(e.ts)
    return None


def unverifiable(reason):
    return {"audio_verified": "unverifiable", "audio_reason": reason}


def verify_finding(finding, log_text, audio_bytes):
    """Evidence to merge onto one finding. Never raises for data reasons.

    A `spoke` verdict also records how loud the utterance was relative to the
    rest of what the same caller said on the same call — that ratio is the
    actual diagnostic, since it distinguishes "spoke normally and STT failed"
    from "spoke very quietly".
    """
    _, events = parse_artifact(log_text)
    if not events:
        return unverifiable("no_log_events")

    try:
        analysis = audio.analyze_caller(audio_bytes)
    except audio.MonoRecording:
        return unverifiable("mono")
    except Exception:
        return unverifiable("undecodable")

    target = _target_log_seconds(events, (finding.get("evidence") or {}).get("seq"))
    if target is None:
        return unverifiable("no_timestamp")

    offset = audio.estimate_offset(
        analysis["agent_segments"], _first_agent_log_seconds(events))
    verdict, detail = audio.caller_spoke_near(
        analysis["caller_segments"], target, offset)

    out = {"audio_verified": verdict, **detail}
    if offset is not None:
        out["audio_offset_s"] = round(offset, 2)
    if verdict != "spoke":
        return out

    seg = detail["utterance_s"]
    key = f"{seg[0]}-{seg[1]}"
    levels = analysis["caller_rms"]
    out["utterance_rms"] = levels.get(key)
    others = sorted(v for k, v in levels.items() if k != key)
    if others and out["utterance_rms"] is not None:
        typical = others[len(others) // 2]
        out["caller_typical_rms"] = typical
        if typical:
            out["relative_level"] = round(out["utterance_rms"] / typical, 3)
    return out
