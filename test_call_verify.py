"""
Tests for the pure verification core (tasks/utils/call_verify.py).

Run:  python -m pytest test_call_verify.py -q
"""

import io
import json

import numpy as np
import soundfile as sf

from tasks.utils import call_verify as cv

SR = 22050


def _tone(sec, amp):
    t = np.arange(int(sec * SR)) / SR
    w = (np.sin(2 * np.pi * 300 * t) + 0.5 * np.sin(2 * np.pi * 600 * t)
         + 0.3 * np.sin(2 * np.pi * 1050 * t)) * (1 + 0.4 * np.sin(2 * np.pi * 4 * t))
    return (w / np.abs(w).max() * amp).astype(np.int16)


def _sil(sec):
    return np.zeros(int(sec * SR), dtype=np.int16)


def stereo(caller, agent):
    n = max(len(caller), len(agent))
    c = np.pad(caller, (0, n - len(caller)))
    a = np.pad(agent, (0, n - len(agent)))
    buf = io.BytesIO()
    sf.write(buf, np.stack([c, a], axis=1), SR, format="WAV", subtype="PCM_16")
    return buf.getvalue()


def mono(x):
    buf = io.BytesIO()
    sf.write(buf, x, SR, format="WAV", subtype="PCM_16")
    return buf.getvalue()


def log(lines):
    """lines: [(seq, seconds_past_minute, msg)]"""
    out = [json.dumps({"_artifact": "speako.call_server_log.v1",
                       "sequence_gaps": [], "call_complete": True})]
    out += [json.dumps({"seq": s, "ts": f"2026-08-13T10:00:{sec:06.3f}Z",
                        "level": "info", "msg": m}) for s, sec, m in lines]
    return "\n".join(out)


AGENT_AT_2S = (10, 2.0, "[Azure] Assistant transcript: Hello, welcome.")
EMPTY_AT_9S = (20, 9.0, "[Azure] EMPTY CALLER TRANSCRIPT — speech detected but STT produced no text")
FINDING = {"rule_id": "stt_empty_transcript", "evidence": {"seq": 20}}


def test_confirms_the_caller_spoke_and_reports_how_quiet():
    """The headline case: agent speaks at audio t=1, caller at audio t=8, and
    the log puts the agent at 2s and the empty transcript at 9s — so the offset
    is 1s and the caller utterance lands on the finding."""
    audio = stereo(
        caller=np.concatenate([_sil(8.0), _tone(1.0, 9000), _sil(3.0),
                               _tone(1.5, 26000)]),
        agent=np.concatenate([_sil(1.0), _tone(1.5, 9000), _sil(10.0)]),
    )
    out = cv.verify_finding(FINDING, log([AGENT_AT_2S, EMPTY_AT_9S]), audio)
    assert out["audio_verified"] == "spoke"
    assert out["duration_s"] > 0.3
    # the diagnostic: quiet relative to the same caller's other speech
    assert out["relative_level"] < 0.5
    assert out["caller_typical_rms"] > out["utterance_rms"]


def test_reports_silent_when_the_caller_said_nothing_there():
    audio = stereo(
        caller=np.concatenate([_sil(20.0)]),
        agent=np.concatenate([_sil(1.0), _tone(1.5, 9000), _sil(10.0)]),
    )
    out = cv.verify_finding(FINDING, log([AGENT_AT_2S, EMPTY_AT_9S]), audio)
    assert out["audio_verified"] == "silent"
    assert "relative_level" not in out


def test_abstains_on_mono_audio():
    out = cv.verify_finding(FINDING, log([AGENT_AT_2S, EMPTY_AT_9S]),
                            mono(_tone(3.0, 9000)))
    assert out == {"audio_verified": "unverifiable", "audio_reason": "mono"}


def test_abstains_on_undecodable_audio():
    out = cv.verify_finding(FINDING, log([AGENT_AT_2S, EMPTY_AT_9S]), b"not audio")
    assert out["audio_reason"] == "undecodable"


def test_abstains_when_the_finding_seq_has_no_timestamp():
    audio = stereo(_sil(5.0), np.concatenate([_sil(1.0), _tone(1.0, 9000)]))
    out = cv.verify_finding({"rule_id": "stt_empty_transcript", "evidence": {"seq": 999}},
                            log([AGENT_AT_2S, EMPTY_AT_9S]), audio)
    assert out["audio_reason"] == "no_timestamp"


def test_abstains_when_it_cannot_anchor_the_clock():
    """No agent utterance in the audio means no offset, so no verdict."""
    audio = stereo(np.concatenate([_sil(8.0), _tone(1.0, 9000)]), _sil(12.0))
    out = cv.verify_finding(FINDING, log([AGENT_AT_2S, EMPTY_AT_9S]), audio)
    assert out["audio_verified"] == "unaligned"


def test_abstains_when_two_utterances_are_equally_plausible():
    audio = stereo(
        caller=np.concatenate([_sil(6.5), _tone(0.8, 9000), _sil(1.0),
                               _tone(0.8, 9000), _sil(4.0)]),
        agent=np.concatenate([_sil(1.0), _tone(1.5, 9000), _sil(10.0)]),
    )
    out = cv.verify_finding(FINDING, log([AGENT_AT_2S, EMPTY_AT_9S]), audio)
    assert out["audio_verified"] == "ambiguous"
    assert len(out["candidates"]) == 2


def test_empty_log_is_survivable():
    audio = stereo(_sil(3.0), _sil(3.0))
    assert cv.verify_finding(FINDING, "", audio)["audio_reason"] == "no_log_events"


def test_log_seconds_parsing():
    assert cv.log_seconds("2026-08-13T10:07:29.471Z") == 449.471
    assert cv.log_seconds(None) is None
    assert cv.log_seconds("garbage") is None
