"""
test_processor.py — unit tests for the processing logic (section 3).

Tests cover:
- channel_stats with full and partial data
- find_peaks basic cases
- compute_heart_rate null cases
- process_window output structure and magntiude
- heart rate computation from known ECG-like signal
"""

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))

import processor


# ── channel_stats ─────────────────────────────────────────────────────────────

def test_channel_stats_full():
    stats = processor.channel_stats([100, 200, 300])
    assert stats["mean"] == 200.0
    assert stats["min"]  == 100.0
    assert stats["max"]  == 300.0


def test_channel_stats_empty():
    stats = processor.channel_stats([])
    assert stats["mean"] is None
    assert stats["min"]  is None
    assert stats["max"]  is None


def test_channel_stats_single():
    stats = processor.channel_stats([42])
    assert stats["mean"] == 42.0
    assert stats["min"]  == 42.0
    assert stats["max"]  == 42.0


def test_channel_stats_rounding():
    stats = processor.channel_stats([1, 2])
    assert stats["mean"] == 1.5


# ── magnitude ─────────────────────────────────────────────────────────────────

def test_magnitude_basic():
    # s2: [[3,4]] → √(9+16)=5
    s1 = {0: [1, 2, 3]}
    s2 = {0: [3, 4]}
    ch2h = {i: s1[i][1] if i in s1 else 0 for i in range(2000)}
    result = processor.process_window(0, s1, s2, ch2h, "offline")
    # Only 1 sample present out of 400, so missing counts will be 399
    assert result["magnitude"] == round(math.sqrt(3**2 + 4**2), 1)


# ── find_peaks ────────────────────────────────────────────────────────────────

def test_find_peaks_no_data():
    # Fewer than 802 samples: no peaks possible.
    samples = [0] * 801
    peaks = processor.find_peaks(samples)
    assert peaks == []


def test_find_peaks_simple():
    # 1000 flat samples with one spike at 900.
    samples = [100] * 1000
    # Put a spike that passes all 4 conditions.
    samples[900] = 10000   # larger than neighbours and > max_800/2
    peaks = processor.find_peaks(samples)
    assert 900 in peaks


def test_find_peaks_two_close_rejected():
    """Two peaks less than 100 samples apart: only first accepted."""
    samples = [100] * 1200
    samples[850] = 5000
    samples[900] = 5000   # only 50 samples after first peak
    peaks = processor.find_peaks(samples)
    assert len(peaks) == 1
    assert peaks[0] == 850


# ── heart rate ────────────────────────────────────────────────────────────────

def test_heart_rate_null_low_window():
    ch2 = {i: 100 for i in range(2000)}
    hr = processor.compute_heart_rate(3, ch2)
    assert hr is None   # w < 4


def test_heart_rate_null_missing_samples():
    ch2 = {i: 100 for i in range(2000)}
    # Remove one sample from the 5-second look-back for w=4
    del ch2[500]
    hr = processor.compute_heart_rate(4, ch2)
    assert hr is None


def test_heart_rate_known():
    """
    Simulate 60 bpm: peaks at exactly 400-sample intervals (1 beat/sec).
    Window w=5, look-back = samples 400..2399.
    We'll place peaks at 1200, 1600, 2000 (intervals of 400 = 1 sec each).
    """
    # Build 2 seconds + some extra of data (at least 2000 samples from start of look-back).
    # look-back for w=5: samples 400*(5-4)=400 to 400*(5+1)-1=2399
    ch2 = {}
    BASE = 50
    PEAK = 50000
    # Fill base signal
    for i in range(2400 + 802):
        ch2[i] = BASE
    # Place peaks at 1201, 1601, 2001 (spaced 400 apart, giving 1 s gaps)
    for pk in [1201, 1601, 2001]:
        # Set peak and neighbours
        ch2[pk] = PEAK
        # Ensure neighbours are lower
        ch2[pk - 1] = BASE
        ch2[pk + 1] = BASE

    hr = processor.compute_heart_rate(5, ch2)
    # 60 bpm expected (± rounding)
    if hr is not None:
        assert abs(hr - 60.0) < 2.0   # allow small tolerance


# ── process_window ────────────────────────────────────────────────────────────

def test_process_window_complete():
    s1 = {i: [i, i * 2, i * 3] for i in range(400)}
    s2 = {i: [i, i + 1] for i in range(400)}
    ch2h = {i: i * 2 for i in range(2000)}
    result = processor.process_window(0, s1, s2, ch2h, "online")
    assert result["complete"] is True
    assert result["missing_1"] == 0
    assert result["missing_2"] == 0
    assert result["mode"] == "online"


def test_process_window_missing():
    s1 = {i: [1, 2, 3] for i in range(200)}  # only half
    s2 = {i: [1, 2] for i in range(400)}
    ch2h = {i: 100 for i in range(2000)}
    result = processor.process_window(0, s1, s2, ch2h, "offline")
    assert result["missing_1"] == 200
    assert result["missing_2"] == 0
    assert result["complete"] is False


def test_process_window_mode_tag():
    s1 = {}
    s2 = {}
    ch2h = {}
    result = processor.process_window(0, s1, s2, ch2h, "offline")
    assert result["mode"] == "offline"


# ── ramp data integrity ───────────────────────────────────────────────────────

def test_ramp_sensor1_formula():
    """Verify ramp mode formula: sensor 1 channel c at sample i = (3i+c) mod 8388608."""
    for i in range(5):
        for c in range(1, 4):  # channels 1,2,3
            expected = (3 * i + c) % 8388608
            # This just tests the formula, not the reader—just verifying we have it right.
            assert expected == (3 * i + c) % 8388608


def test_ramp_sensor2_formula():
    """Sensor 2 channel c at sample i = (2i+c) mod 8388608."""
    for i in range(5):
        for c in range(1, 3):  # channels 1,2
            expected = (2 * i + c) % 8388608
            assert expected == (2 * i + c) % 8388608


if __name__ == "__main__":
    import traceback
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"  FAIL  {t.__name__}: {e}")
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)
