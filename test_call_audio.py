"""
Tests for caller-channel speech detection (tasks/utils/call_audio.py).

Audio is synthesised here rather than shipped as fixtures — a real recording
carries caller phone numbers and full conversations, so committing one would put
customer audio in the repo.

Run:  python -m pytest test_call_audio.py -q
"""

import io

import numpy as np
import pytest
import soundfile as sf

from tasks.utils import call_audio as ca

SR = 22050          # what the stored recordings actually use


def _tone(seconds, amplitude, sr=SR, freq=300.0):
    """A voiced-ish burst: a low tone with harmonics, which webrtcvad accepts
    as speech. A pure sine is often rejected."""
    t = np.arange(int(seconds * sr)) / sr
    wave = (np.sin(2 * np.pi * freq * t)
            + 0.5 * np.sin(2 * np.pi * freq * 2 * t)
            + 0.3 * np.sin(2 * np.pi * freq * 3.5 * t))
    wave *= 1 + 0.4 * np.sin(2 * np.pi * 4 * t)          # syllable-rate envelope
    return (wave / np.abs(wave).max() * amplitude).astype(np.int16)


def _silence(seconds, sr=SR):
    return np.zeros(int(seconds * sr), dtype=np.int16)


def build(caller_parts, agent_parts, sr=SR):
    """parts: list of ('speech'|'silence', seconds, amplitude) -> stereo mp3-less WAV bytes."""
    def chan(parts):
        out = []
        for kind, secs, amp in parts:
            out.append(_tone(secs, amp, sr) if kind == "speech" else _silence(secs, sr))
        return np.concatenate(out) if out else np.zeros(0, dtype=np.int16)

    c, a = chan(caller_parts), chan(agent_parts)
    n = max(len(c), len(a))
    c = np.pad(c, (0, n - len(c)))
    a = np.pad(a, (0, n - len(a)))
    buf = io.BytesIO()
    sf.write(buf, np.stack([c, a], axis=1), sr, format="WAV", subtype="PCM_16")
    return buf.getvalue()


# ── Channel handling ─────────────────────────────────────────────────────────

def test_mono_is_refused_not_guessed():
    buf = io.BytesIO()
    sf.write(buf, _tone(1.0, 8000), SR, format="WAV", subtype="PCM_16")
    with pytest.raises(ca.MonoRecording):
        ca.load(buf.getvalue())


def test_channels_are_read_as_caller_then_agent():
    audio = build([("speech", 1.0, 9000)], [("silence", 1.0, 0)])
    caller, agent, sr = ca.load(audio)
    assert sr == SR
    assert np.abs(caller).max() > 1000
    assert np.abs(agent).max() == 0


# ── The phantom-speech regression ────────────────────────────────────────────

def test_digital_silence_is_not_speech():
    """REGRESSION. webrtcvad alone classified mp3-decoded silence as speech —
    9.8s of it on a call whose true caller total was ~6s. The energy gate is
    what stops it."""
    audio = build([("silence", 6.0, 0)], [("silence", 6.0, 0)])
    caller, _, sr = ca.load(audio)
    assert ca.segments(ca.speech_mask(caller, sr)) == []


def test_very_quiet_noise_is_not_speech():
    audio = build([("speech", 4.0, 40)], [("silence", 4.0, 0)])   # under the floor
    caller, _, sr = ca.load(audio)
    assert ca.segments(ca.speech_mask(caller, sr)) == []


def test_the_floor_admits_quiet_speech_and_rejects_line_noise():
    """The gate is tested directly with the levels measured on real recordings.

    End-to-end synthetic audio cannot cover this: webrtcvad scores spectral
    shape, and a synthesised tone at a realistic quiet level is rejected as
    not-speech regardless of the gate, while the real quiet utterance is
    accepted. Testing the gate against real numbers is honest; asserting on a
    synthetic quiet tone would just be asserting the fixture's limitations.
    """
    loud = np.full(200, 3000.0)          # this call's normal speech level
    energy = np.concatenate([loud, np.full(1800, 8.0)])
    floor = ca.energy_floor(energy)

    assert floor > 107, "must reject the line noise measured at RMS 81-107"
    assert floor < 294, "must admit the confirmed dropped utterance at RMS 294"


def test_the_absolute_floor_holds_when_a_call_has_no_loud_speech():
    assert ca.energy_floor(np.full(100, 5.0)) == ca.ABSOLUTE_FLOOR
    assert ca.energy_floor(np.zeros(0)) == ca.ABSOLUTE_FLOOR


# ── Segmentation ─────────────────────────────────────────────────────────────

def test_finds_each_utterance_with_plausible_bounds():
    audio = build(
        [("silence", 1.0, 0), ("speech", 1.0, 9000),
         ("silence", 2.0, 0), ("speech", 1.5, 9000), ("silence", 1.0, 0)],
        [("silence", 6.5, 0)],
    )
    caller, _, sr = ca.load(audio)
    segs = ca.segments(ca.speech_mask(caller, sr))
    assert len(segs) == 2
    assert 0.7 <= segs[0][0] <= 1.3
    assert 3.7 <= segs[1][0] <= 4.3


def test_a_blip_shorter_than_the_minimum_is_ignored():
    audio = build([("silence", 1.0, 0), ("speech", 0.08, 9000), ("silence", 1.0, 0)],
                  [("silence", 2.1, 0)])
    caller, _, sr = ca.load(audio)
    assert ca.segments(ca.speech_mask(caller, sr)) == []


def test_segment_rms_separates_loud_from_quiet():
    audio = build([("speech", 1.0, 12000)], [("silence", 1.0, 0)])
    caller, _, sr = ca.load(audio)
    assert ca.segment_rms(caller, sr, 0.1, 0.9) > 1000
    assert ca.segment_rms(caller, sr, 0.0, 0.0) == 0.0


# ── Alignment and the verdict ────────────────────────────────────────────────

def test_offset_anchors_on_the_agent_first_utterance():
    assert ca.estimate_offset([(2.0, 5.0)], 12.0) == 10.0
    assert ca.estimate_offset([], 12.0) is None
    assert ca.estimate_offset([(2.0, 5.0)], None) is None


def test_verdict_spoke_when_one_utterance_is_in_the_window():
    verdict, detail = ca.caller_spoke_near([(30.0, 30.5)], target_log_s=40.2, offset=10.0)
    assert verdict == "spoke"
    assert detail["utterance_s"] == [30.0, 30.5]
    assert detail["duration_s"] == 0.5


def test_verdict_silent_when_nothing_is_near():
    verdict, _ = ca.caller_spoke_near([(5.0, 6.0)], target_log_s=40.0, offset=10.0)
    assert verdict == "silent"


def test_verdict_ambiguous_when_two_utterances_are_in_the_window():
    """Alignment drifts ~1-1.5s, so two nearby utterances cannot be attributed
    to the right moment. Abstain rather than pick one."""
    verdict, detail = ca.caller_spoke_near(
        [(29.0, 29.4), (31.0, 31.4)], target_log_s=40.2, offset=10.0)
    assert verdict == "ambiguous"
    assert len(detail["candidates"]) == 2


def test_verdict_unaligned_without_an_offset():
    verdict, _ = ca.caller_spoke_near([(30.0, 30.5)], target_log_s=40.0, offset=None)
    assert verdict == "unaligned"


def test_window_tolerance_covers_the_measured_drift():
    """A 1.5s misalignment must still find the utterance."""
    verdict, _ = ca.caller_spoke_near([(30.0, 30.5)], target_log_s=41.7, offset=10.0)
    assert verdict == "spoke"


# ── End to end ───────────────────────────────────────────────────────────────

def test_analyze_caller_reports_segments_and_levels():
    audio = build([("silence", 1.0, 0), ("speech", 1.0, 9000), ("silence", 1.0, 0)],
                  [("silence", 0.5, 0), ("speech", 0.5, 9000), ("silence", 2.0, 0)])
    out = ca.analyze_caller(audio)
    assert out["sample_rate"] == SR
    assert len(out["caller_segments"]) == 1
    assert len(out["agent_segments"]) == 1
    assert all(v > 0 for v in out["caller_rms"].values())
