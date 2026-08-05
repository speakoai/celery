"""
Parsing and grouping for Render server-log artifacts.

Pure functions, deliberately free of celery/DB/R2/network imports so they can
be unit-tested directly (the deployed celery pin cannot even be imported under
newer Pythons). tasks/pull_render_logs.py owns the I/O; this module owns the
shape of the data.

Input is a Render log entry as ``GET /v1/logs`` returns it:
``{id, timestamp, message, labels[{name, value}]}``. Output is one artifact per
Twilio call, keyed by the ``~ct~<call_sid>#<seq>`` stamp that
speako-voice-ai/modules/call_log_context.py writes onto every line.
"""

import gzip
import json
import re
from datetime import datetime, timezone

# Channel marker written by speako-voice-ai/modules/call_log_context.py.
CHANNEL_MARKER = "~ct~"

# `~ct~CA1234…#00042 [Azure] Caller transcript: …`
_STAMP_RE = re.compile(r"~ct~(?P<call_sid>[A-Za-z0-9]+)#(?P<seq>\d+)\s")
_STREAM_START_RE = re.compile(
    r"Stream started .*?tenant_id=(?P<tenant_id>\d+) location_id=(?P<location_id>\d+)"
)
# A call is finished once one of these has been logged.
_TERMINAL_MARKERS = ("[CallLifecycle] Call ended", "[Twilio/Azure] Stream stopped")


# ── Grouping ─────────────────────────────────────────────────────────────────

def _group_by_call(entries):
    """Split raw log entries into per-call traces keyed by call_sid."""
    calls = {}
    for entry in entries:
        message = entry.get("message") or ""
        match = _STAMP_RE.search(message)
        if not match:
            continue

        call_sid = match.group("call_sid")
        call = calls.setdefault(call_sid, {
            "call_sid": call_sid,
            "lines": [],
            "tenant_id": None,
            "location_id": None,
            "instance": None,
            "complete": False,
        })

        labels = {label["name"]: label["value"] for label in entry.get("labels", [])}
        call["lines"].append({
            "seq": int(match.group("seq")),
            "ts": entry.get("timestamp"),
            "level": labels.get("level"),
            "msg": message[match.end():],
        })
        call["instance"] = call["instance"] or labels.get("instance")

        if call["tenant_id"] is None:
            start = _STREAM_START_RE.search(message)
            if start:
                call["tenant_id"] = int(start.group("tenant_id"))
                call["location_id"] = int(start.group("location_id"))

        if any(marker in message for marker in _TERMINAL_MARKERS):
            call["complete"] = True

    for call in calls.values():
        call["lines"].sort(key=lambda line: line["seq"])
        call["gaps"] = _sequence_gaps(call["lines"])
    return calls


def _sequence_gaps(lines):
    """Missing sequence numbers — the ONLY evidence that Render dropped lines.

    Render discards application logs above 6,000 lines/min per instance without
    marking the gap, so a trace with holes would otherwise read as complete.
    Note that non-channel lines consume sequence numbers too, so a gap here
    means "lines we didn't retrieve", not necessarily "lines Render dropped";
    the artifact records the ranges and lets a human judge.
    """
    gaps = []
    for previous, current in zip(lines, lines[1:]):
        if current["seq"] > previous["seq"] + 1:
            gaps.append([previous["seq"] + 1, current["seq"] - 1])
    return gaps


def _build_artifact(call, service_id, window):
    """Header line + one JSON object per log line, gzipped."""
    header = {
        "_artifact": "speako.call_server_log.v1",
        "call_sid": call["call_sid"],
        "tenant_id": call["tenant_id"],
        "location_id": call["location_id"],
        "render_service_id": service_id,
        "render_instance_id": call["instance"],
        "line_count": len(call["lines"]),
        "first_seq": call["lines"][0]["seq"] if call["lines"] else None,
        "last_seq": call["lines"][-1]["seq"] if call["lines"] else None,
        "sequence_gaps": call["gaps"],
        "incomplete": bool(call["gaps"]),
        "call_complete": call["complete"],
        "pulled_at": datetime.now(timezone.utc).isoformat(),
        "window": window,
    }
    body = "\n".join(
        [json.dumps(header)] + [json.dumps(line) for line in call["lines"]]
    )
    return gzip.compress(body.encode("utf-8"))
