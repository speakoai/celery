"""
Tests for the Render server-log puller (tasks/pull_render_logs.py).

Covers the pure parsing/grouping logic — no network, no DB, no R2. The API
contract itself (literal-substring `text` filter, backward pagination, 30/min
rate limit) was verified against the live Render API on 2026-08-05 and is
recorded in docs/plans/call-server-log-capture.md §11.6.

Run:  python -m pytest test_pull_render_logs.py -q
"""

import gzip
import json

import pytest

from tasks.utils.render_log_parse import (
    CHANNEL_MARKER,
    _build_artifact,
    _group_by_call,
    _sequence_gaps,
    _STAMP_RE,
)

CALL_A = "CA635ba4b3996df5ff06582599496185f6"
CALL_B = "CAaaaabbbbccccddddeeeeffff00001111"


def entry(call_sid, seq, msg, ts="2026-08-05T10:59:42.000Z", level="info",
          instance="srv-abc-8l5xf"):
    """A Render log entry as the API actually returns it."""
    return {
        "id": f"{call_sid}-{seq}",
        "timestamp": ts,
        "message": f"INFO:root:{CHANNEL_MARKER}{call_sid}#{seq:05d} {msg}",
        "labels": [
            {"name": "level", "value": level},
            {"name": "type", "value": "app"},
            {"name": "instance", "value": instance},
        ],
    }


# ── Stamp parsing ──

def test_stamp_regex_extracts_call_and_sequence():
    m = _STAMP_RE.search(
        f"INFO:root:{CHANNEL_MARKER}{CALL_A}#00042 [Azure] Caller transcript: Booking."
    )
    assert m.group("call_sid") == CALL_A
    assert int(m.group("seq")) == 42


def test_unstamped_lines_are_ignored():
    calls = _group_by_call([
        {"id": "x", "timestamp": "2026-08-05T10:00:00Z", "labels": [],
         "message": "INFO:root:[Azure] a line from before the rollout"},
    ])
    assert calls == {}


# ── Grouping ──

def test_splits_interleaved_calls():
    """The whole point: two concurrent calls in one stream, cleanly separated."""
    calls = _group_by_call([
        entry(CALL_A, 1, "[Azure] Caller transcript: one"),
        entry(CALL_B, 1, "[Azure] Caller transcript: two"),
        entry(CALL_A, 2, "[Azure] Assistant transcript: hi"),
        entry(CALL_B, 2, "[Azure] Assistant transcript: hello"),
    ])
    assert set(calls) == {CALL_A, CALL_B}
    assert [line["seq"] for line in calls[CALL_A]["lines"]] == [1, 2]
    assert "one" in calls[CALL_A]["lines"][0]["msg"]
    assert "two" in calls[CALL_B]["lines"][0]["msg"]


def test_lines_are_sorted_by_sequence_not_arrival():
    """Pagination walks backward, so entries arrive newest-first."""
    calls = _group_by_call([
        entry(CALL_A, 9, "[Azure] Response completed: id=z"),
        entry(CALL_A, 3, "[Azure] Caller transcript: mid"),
        entry(CALL_A, 1, "[Twilio/Azure] Stream started"),
    ])
    assert [line["seq"] for line in calls[CALL_A]["lines"]] == [1, 3, 9]


def test_extracts_tenant_and_location_from_stream_started():
    calls = _group_by_call([
        entry(CALL_A, 1,
              "[Twilio/Azure] Stream started stream_sid=MZ123 call_sid=CA1 "
              "tenant_id=11 location_id=6 location_type=rest caller=+61422336220"),
    ])
    assert calls[CALL_A]["tenant_id"] == 11
    assert calls[CALL_A]["location_id"] == 6


def test_missing_stream_started_leaves_call_unattached():
    """A call rejected before the media stream never logs `Stream started`; it
    still gets an artifact, just under the unattached prefix."""
    calls = _group_by_call([entry(CALL_A, 1, "[Twilio/Azure] Incoming call to: +61721004548")])
    assert calls[CALL_A]["tenant_id"] is None
    assert calls[CALL_A]["location_id"] is None


def test_captures_render_instance():
    calls = _group_by_call([entry(CALL_A, 1, "[Azure] x", instance="srv-xyz-abcde")])
    assert calls[CALL_A]["instance"] == "srv-xyz-abcde"


@pytest.mark.parametrize("marker", [
    "[CallLifecycle] Call ended: duration=93s messages=16",
    "[Twilio/Azure] Stream stopped stream_sid=MZ123",
])
def test_terminal_markers_mark_the_call_complete(marker):
    calls = _group_by_call([entry(CALL_A, 1, marker)])
    assert calls[CALL_A]["complete"] is True


def test_call_without_terminal_marker_is_incomplete():
    calls = _group_by_call([entry(CALL_A, 1, "[Azure] Caller transcript: mid-call")])
    assert calls[CALL_A]["complete"] is False


# ── Gap detection ──

def test_no_gaps_in_a_contiguous_sequence():
    assert _sequence_gaps([{"seq": n} for n in (1, 2, 3, 4)]) == []


def test_gaps_report_the_missing_ranges():
    """Render silently drops app logs above 6,000 lines/min/instance. A hole in
    the sequence is the only evidence, so the ranges must be reported."""
    assert _sequence_gaps([{"seq": n} for n in (1, 2, 7, 8, 20)]) == [[3, 6], [9, 19]]


def test_single_line_has_no_gaps():
    assert _sequence_gaps([{"seq": 5}]) == []


# ── Artifact ──

def test_artifact_is_gzipped_jsonl_with_a_header():
    calls = _group_by_call([
        entry(CALL_A, 1, "[Twilio/Azure] Stream started tenant_id=11 location_id=6"),
        entry(CALL_A, 2, "[Azure] Caller transcript: Booking."),
    ])
    blob = _build_artifact(calls[CALL_A], "srv-test", {"start": "s", "end": "e"})
    lines = gzip.decompress(blob).decode().split("\n")

    header = json.loads(lines[0])
    assert header["_artifact"] == "speako.call_server_log.v1"
    assert header["call_sid"] == CALL_A
    assert header["tenant_id"] == 11
    assert header["line_count"] == 2
    assert header["sequence_gaps"] == []
    assert header["incomplete"] is False
    assert header["render_service_id"] == "srv-test"

    assert json.loads(lines[1])["seq"] == 1
    assert json.loads(lines[2])["msg"] == "[Azure] Caller transcript: Booking."


def test_artifact_flags_a_trace_with_holes():
    calls = _group_by_call([
        entry(CALL_A, 1, "[Azure] a"),
        entry(CALL_A, 50, "[Azure] b"),
    ])
    header = json.loads(
        gzip.decompress(_build_artifact(calls[CALL_A], "srv", {})).decode().split("\n")[0]
    )
    assert header["incomplete"] is True
    assert header["sequence_gaps"] == [[2, 49]]


def test_artifact_strips_the_stamp_from_the_message():
    """The prefix is transport, not content — it must not clutter the viewer."""
    calls = _group_by_call([entry(CALL_A, 7, "[Azure] Caller transcript: Booking.")])
    body = gzip.decompress(_build_artifact(calls[CALL_A], "srv", {})).decode()
    assert CHANNEL_MARKER not in body.split("\n")[1]
    assert json.loads(body.split("\n")[1])["msg"] == "[Azure] Caller transcript: Booking."


def test_artifact_compresses_a_realistic_call_well_under_a_page():
    """~68 channel lines for a 93s call; the gzipped artifact should be tiny."""
    lines = [entry(CALL_A, n, f"[Azure] Response completed: id=resp_{n} status=completed "
                              f"audio_chunks=35 audio_bytes=133857 twilio_sent=35")
             for n in range(1, 69)]
    calls = _group_by_call(lines)
    blob = _build_artifact(calls[CALL_A], "srv", {})
    assert len(blob) < 4096
    assert json.loads(gzip.decompress(blob).decode().split("\n")[0])["line_count"] == 68
