"""
Caller-channel speech detection over a stored call recording.

Phase 8 of docs/plans/call-quality-audio-verify.md (speako-workspace).

Answers exactly one question, deliberately: **when the log says STT produced
nothing, did the caller actually speak?** Everything else the prototype measured
(dead air, overlap, turn timing) is excluded — those numbers flagged 9/9 calls
and disagreed with the log by ~2x where ground truth existed.

Pure functions: decoding and signal processing only, no DB, R2 or network.
tasks/verify_call_findings.py owns the I/O.

──────────────────────────────────────────────────────────────────────────────
TWO THINGS THAT LOOK LIKE THEY SHOULD WORK AND DO NOT
──────────────────────────────────────────────────────────────────────────────
1. **webrtcvad alone.** It assumes live microphone input with a noise floor.
   These recordings are mp3-decoded telephony where the gaps are near-digital
   silence (median 20ms frame energy ~0.6), so decoder ringing classifies as
   speech — measured 9.8s of phantom speech on a call whose true caller total
   was ~6s. VAD must be ANDed with an energy gate.

2. **An absolute energy floor.** Levels vary per call and per caller. The floor
   is derived from the call's own loud percentile instead, so a quiet caller is
   not silently discarded.

The recording is Twilio dual-channel (`recording_channels="dual"` in
speako-voice-ai/modules/call_lifecycle.py): caller LEFT, agent RIGHT. Verified
genuinely separated — envelope correlation ~0 between channels, and caller
energy during agent speech ~0.001 of the agent's. Mono input means a provider
that does not support separation (elevenlabs), and is refused rather than
guessed at.
"""

import io

import numpy as np

VAD_RATE = 16000
FRAME_MS = 20
FRAME_S = FRAME_MS / 1000.0
AGGRESSIVENESS = 3          # strictest; lower fires on mp3 decoder noise
ABSOLUTE_FLOOR = 60.0       # 20ms RMS below this is line silence on any call
# ...or this fraction of the call's own loud level, whichever is higher.
# Calibrated against three real recordings, where the levels are known:
#   transcribed speech  RMS 2200-3600
#   the confirmed dropped utterance  RMS 294   <- must be ADMITTED
#   line noise carrying no speech    RMS  81-107  <- must be REJECTED
# 0.08 admitted it with only 1.4x headroom; 0.04 let the noise back in (14 -> 16
# utterances on the same call). 0.05 keeps every true detection, holds the floor
# above the noise, and gives the quiet case 2.3x headroom.
RELATIVE_FLOOR = 0.05
MIN_UTTERANCE_S = 0.25
HANGOVER_FRAMES = 8         # 160ms — bridges breath gaps inside one utterance


class MonoRecording(Exception):
    """Single-channel audio: caller and agent cannot be told apart."""


def load(data):
    """bytes | path -> (caller, agent, sample_rate) as int16 arrays."""
    import soundfile as sf

    src = io.BytesIO(data) if isinstance(data, (bytes, bytearray)) else data
    samples, sr = sf.read(src, dtype="int16", always_2d=True)
    if samples.shape[1] < 2:
        raise MonoRecording("recording has a single channel")
    return samples[:, 0].copy(), samples[:, 1].copy(), sr


def _resample(x, sr, target=VAD_RATE):
    """Nearest-neighbour decimation. Crude, but VAD and RMS both care about
    envelope rather than fidelity, and it avoids a scipy dependency on a worker
    that has already OOMed once."""
    if sr == target:
        return x
    n = int(round(len(x) * target / sr))
    if n <= 1:
        return x[:0]
    idx = (np.arange(n) * (len(x) - 1) / (n - 1)).astype(np.int64)
    return x[idx]


def frame_energy(mono_int16, sr):
    x = _resample(mono_int16, sr)
    fl = int(VAD_RATE * FRAME_S)
    n = len(x) // fl
    if n == 0:
        return np.zeros(0), x, fl, 0
    frames = x[: n * fl].astype(np.float64).reshape(-1, fl)
    return np.sqrt((frames ** 2).mean(axis=1)), x, fl, n


def energy_floor(energy):
    """Per-call gate. `ABSOLUTE_FLOOR` alone would discard a quiet caller;
    `RELATIVE_FLOOR` alone would admit noise on a call with no speech."""
    if energy.size == 0:
        return ABSOLUTE_FLOOR
    loud = energy[energy > np.percentile(energy, 90)]
    if loud.size == 0:
        return ABSOLUTE_FLOOR
    return max(ABSOLUTE_FLOOR, float(np.median(loud)) * RELATIVE_FLOOR)


def speech_mask(mono_int16, sr):
    """Bool array over 20ms frames: VAD **and** energy must both agree."""
    import webrtcvad

    energy, x, fl, n = frame_energy(mono_int16, sr)
    if n == 0:
        return np.zeros(0, dtype=bool)

    vad = webrtcvad.Vad(AGGRESSIVENESS)
    buf = x[: n * fl].tobytes()
    raw = np.fromiter(
        (vad.is_speech(buf[i * fl * 2:(i + 1) * fl * 2], VAD_RATE) for i in range(n)),
        dtype=bool, count=n,
    )
    raw &= energy > energy_floor(energy)

    out = raw.copy()
    run = 0
    for i, v in enumerate(raw):
        if v:
            run = HANGOVER_FRAMES
        elif run:
            out[i] = True
            run -= 1
    return out


def segments(mask, min_s=MIN_UTTERANCE_S):
    """Frame mask -> [(start_s, end_s)] of contiguous speech."""
    out, on, start = [], False, 0
    for i, v in enumerate(mask):
        if v and not on:
            on, start = True, i
        elif not v and on:
            on = False
            if (i - start) * FRAME_S >= min_s:
                out.append((round(start * FRAME_S, 2), round(i * FRAME_S, 2)))
    if on and (len(mask) - start) * FRAME_S >= min_s:
        out.append((round(start * FRAME_S, 2), round(len(mask) * FRAME_S, 2)))
    return out


def segment_rms(mono_int16, sr, start_s, end_s):
    a, b = int(start_s * sr), int(end_s * sr)
    seg = mono_int16[a:b].astype(np.float64)
    return round(float(np.sqrt((seg ** 2).mean())), 1) if seg.size else 0.0


def estimate_offset(agent_segments, first_agent_log_s):
    """Seconds to add to an audio timestamp to get a log timestamp.

    There is no shared clock. The only usable anchor is the agent's first
    utterance, which appears in both. Measured drift ~1–1.5s, which is why
    callers must search a window rather than an instant, and why this returns
    None when it cannot anchor at all.
    """
    if not agent_segments or first_agent_log_s is None:
        return None
    return first_agent_log_s - agent_segments[0][0]


def caller_spoke_near(caller_segments, target_log_s, offset, window_s=3.0):
    """Was the caller speaking around `target_log_s` (a LOG timestamp)?

    Returns (verdict, detail) where verdict is one of:
      "spoke"        — exactly one caller utterance in the window
      "silent"       — none
      "ambiguous"    — more than one, so it cannot be attributed to this moment
      "unaligned"    — no offset could be established

    Deliberately abstains rather than guessing: a wrong confirmation is worse
    than no confirmation.
    """
    if offset is None:
        return "unaligned", {}
    target_audio_s = target_log_s - offset
    hits = [
        (s, e) for s, e in caller_segments
        if s - window_s <= target_audio_s <= e + window_s
    ]
    if not hits:
        return "silent", {"searched_audio_s": round(target_audio_s, 2),
                          "window_s": window_s}
    if len(hits) > 1:
        return "ambiguous", {"searched_audio_s": round(target_audio_s, 2),
                             "candidates": hits}
    start, end = hits[0]
    return "spoke", {"searched_audio_s": round(target_audio_s, 2),
                     "utterance_s": [start, end],
                     "duration_s": round(end - start, 2)}


def analyze_caller(audio_bytes):
    """Everything the verifier needs from one recording, in one pass."""
    caller, agent, sr = load(audio_bytes)
    cseg = segments(speech_mask(caller, sr))
    aseg = segments(speech_mask(agent, sr))
    return {
        "sample_rate": sr,
        "duration_s": round(len(caller) / sr, 2),
        "caller_segments": cseg,
        "agent_segments": aseg,
        "caller_rms": {f"{s}-{e}": segment_rms(caller, sr, s, e) for s, e in cseg},
    }
