"""
processor.py — windowed signal processing for the biosignal pipeline.

Processing follows the spec exactly (section 3 of the assessment):
- Window w covers sample indexes 400w … 400w + 399 of each sensor.
- Outputs: missing, complete, mean/min/max per channel, magnitude, heart_rate, mode.
- Heart-rate rule uses a 5-second (2000-sample) look-back window and requires
  peaks found by a strict 4-condition rule.
"""

import math
from typing import Optional


RATE = 400          # samples per second
WINDOW_SIZE = 400   # samples per 1-second window


# ── helpers ───────────────────────────────────────────────────────────────────

def _round1(v) -> Optional[float]:
    """Round to 1 decimal place, return None unchanged."""
    if v is None:
        return None
    return round(float(v), 1)


# ── peak detection ────────────────────────────────────────────────────────────

def find_peaks(samples: list) -> list:
    """
    Return indexes (within *samples*) that satisfy the 4-condition peak rule.

    samples: list of numeric values for sensor 1 channel 2, in order.
    The global index of samples[i] = start_global + i.
    """
    peaks = []
    prev_peak_local = -1  # local index of last accepted peak

    for i, val in enumerate(samples):
        # Condition 1: 800 samples before and 1 sample after must exist.
        if i < 800 or i + 1 >= len(samples):
            continue
        # Condition 2: larger than both immediate neighbours.
        if not (val > samples[i - 1] and val > samples[i + 1]):
            continue
        # Condition 3: larger than half the max of the 800 samples before it.
        window_800 = samples[i - 800: i]
        max_800 = max(window_800)
        if val <= max_800 / 2:
            continue
        # Condition 4: at least 100 samples after the previous peak.
        if prev_peak_local >= 0 and (i - prev_peak_local) < 100:
            continue
        peaks.append(i)
        prev_peak_local = i

    return peaks


def compute_heart_rate(window_w: int, ch2_samples: dict) -> Optional[float]:
    """
    Compute heart rate for window w.

    ch2_samples: dict mapping global_index -> value for sensor 1, channel 2.
                 Must cover the range needed (global indexes 400*(w-4) to
                 400*(w+1) - 1), plus prior history for peak detection context.

    Returns bpm (float, 1 dp) or None.
    """
    if window_w < 4:
        return None

    start = RATE * (window_w - 4)   # 400*(w-4)
    end   = RATE * (window_w + 1)   # exclusive, 2000 samples total

    # Check all 2000 samples exist in the 5-second lookback.
    for idx in range(start, end):
        if idx not in ch2_samples:
            return None

    # Scan for peaks starting from prior history to establish refractory state
    scan_start = max(800, start - 800)
    last_peak = -999999
    peaks_in_window = []

    for i in range(scan_start, end):
        # Condition 1: 800 samples before and 1 sample after available.
        if (i - 800) not in ch2_samples or (i + 1) not in ch2_samples:
            continue
        val = ch2_samples[i]
        # Condition 2: larger than both immediate neighbours.
        if not (val > ch2_samples[i - 1] and val > ch2_samples[i + 1]):
            continue
        # Condition 3: larger than half the max of the 800 samples before it.
        max_800 = max(ch2_samples[k] for k in range(i - 800, i))
        if val <= max_800 / 2:
            continue
        # Condition 4: at least 100 samples after the previous peak.
        if (i - last_peak) < 100:
            continue
        last_peak = i
        if start <= i < end:
            peaks_in_window.append(i)

    if len(peaks_in_window) < 2:
        return None

    peak_times = [p / RATE for p in peaks_in_window]
    gaps = [peak_times[j + 1] - peak_times[j] for j in range(len(peak_times) - 1)]
    mean_gap = sum(gaps) / len(gaps)
    if mean_gap == 0:
        return None
    return _round1(60.0 / mean_gap)


# ── per-channel stats ─────────────────────────────────────────────────────────

def channel_stats(values: list) -> dict:
    """Return mean, min, max for a list of numbers, or None if empty."""
    if not values:
        return {"mean": None, "min": None, "max": None}
    return {
        "mean": _round1(sum(values) / len(values)),
        "min":  _round1(min(values)),
        "max":  _round1(max(values)),
    }


# ── main window processor ─────────────────────────────────────────────────────

def process_window(window_w: int, s1_samples: dict, s2_samples: dict,
                   ch2_history: dict, mode: str) -> dict:
    """
    Produce the section-3 output for window *window_w*.

    s1_samples: dict global_index -> [ch1, ch2, ch3]  (sensor 1)
    s2_samples: dict global_index -> [ch1, ch2]        (sensor 2)
    ch2_history: dict global_index -> value            (sensor 1 ch2, ≥2000 samples back)
    mode: 'online' or 'offline'
    """
    s1_start = WINDOW_SIZE * window_w
    s2_start = WINDOW_SIZE * window_w

    # ── Sensor 1 ──────────────────────────────────────────────────────────────
    s1_present = {idx: s1_samples[idx] for idx in range(s1_start, s1_start + WINDOW_SIZE)
                  if idx in s1_samples}
    missing_1 = WINDOW_SIZE - len(s1_present)

    s1_ch1 = [s1_present[i][0] for i in sorted(s1_present)]
    s1_ch2 = [s1_present[i][1] for i in sorted(s1_present)]
    s1_ch3 = [s1_present[i][2] for i in sorted(s1_present)]

    # ── Sensor 2 ──────────────────────────────────────────────────────────────
    s2_present = {idx: s2_samples[idx] for idx in range(s2_start, s2_start + WINDOW_SIZE)
                  if idx in s2_samples}
    missing_2 = WINDOW_SIZE - len(s2_present)

    s2_ch1 = [s2_present[i][0] for i in sorted(s2_present)]
    s2_ch2 = [s2_present[i][1] for i in sorted(s2_present)]

    complete = (missing_1 == 0 and missing_2 == 0)

    # ── Magnitude (over available s2 samples) ─────────────────────────────────
    mag_vals = []
    for i in sorted(s2_present):
        c1, c2 = s2_present[i][0], s2_present[i][1]
        mag_vals.append(math.sqrt(c1 ** 2 + c2 ** 2))
    magnitude = _round1(sum(mag_vals) / len(mag_vals)) if mag_vals else None

    # ── Heart rate ────────────────────────────────────────────────────────────
    heart_rate = compute_heart_rate(window_w, ch2_history)

    return {
        "window": window_w,
        "missing_1": missing_1,
        "missing_2": missing_2,
        "complete": complete,
        "sensor_1": {
            "ch1": channel_stats(s1_ch1),
            "ch2": channel_stats(s1_ch2),
            "ch3": channel_stats(s1_ch3),
        },
        "sensor_2": {
            "ch1": channel_stats(s2_ch1),
            "ch2": channel_stats(s2_ch2),
        },
        "magnitude": magnitude,
        "heart_rate": heart_rate,
        "mode": mode,
    }
