import random

from beatkeeper.processor import SessionProcessor, RATE
from beatkeeper.simulator import sample, bpm_at


def feed(p, n, mode="signal", seed=7, skip1=(), skip2=()):
    for i in range(n):
        if i not in skip1:
            p.add_sample(1, i, sample(mode, seed, 1, i))
        if i not in skip2:
            p.add_sample(2, i, sample(mode, seed, 2, i))


# ---- naive reference straight from the written rule ---------------------------------

def reference_hr(ch2: dict, w: int):
    """O(n*800) literal implementation of the heart-rate rule."""
    if w < 4:
        return None
    lo, hi = 400 * (w - 4), 400 * (w + 1)
    if any(i not in ch2 for i in range(lo, hi)):
        return None
    peaks, last = [], None
    for i in sorted(ch2):
        if i < 800 or (i + 1) not in ch2:
            continue
        if any((i - k) not in ch2 for k in range(1, 801)):
            continue
        if not (ch2[i] > ch2[i - 1] and ch2[i] > ch2[i + 1]):
            continue
        if not ch2[i] > max(ch2[i - k] for k in range(1, 801)) / 2:
            continue
        if last is not None and i - last < 100:
            continue
        peaks.append(i)
        last = i
    pw = [p for p in peaks if lo <= p < hi]
    if len(pw) < 2:
        return None
    gaps = [(b - a) / RATE for a, b in zip(pw, pw[1:])]
    return round(60 / (sum(gaps) / len(gaps)), 1)


def test_heart_rate_follows_signal_schedule():
    p = SessionProcessor("s")
    feed(p, 400 * 400)
    res = p.pop_ready("offline")
    assert len(res) == 399                      # last window needs the next sample
    for r in res:
        w = r["window"]
        if w < 4:
            assert r["heart_rate"] is None
        else:
            span_bpm = {bpm_at(400 * k) for k in range(w - 4, w + 1)}
            if len(span_bpm) == 1:              # skip windows straddling a rate change
                assert abs(r["heart_rate"] - span_bpm.pop()) < 1.5, w


def test_streaming_detector_matches_literal_rule_with_gaps():
    n = 400 * 30
    gaps = set(range(5000, 5100)) | set(range(9000, 9003))
    p = SessionProcessor("s")
    feed(p, n, skip1=gaps)
    p.add_loss(1, 5000, 5099)
    p.add_loss(1, 9000, 9002)
    res = p.pop_ready("offline")
    ch2 = {i: sample("signal", 7, 1, i)[1] for i in range(n) if i not in gaps}
    assert len(res) > 20
    for r in res:
        assert r["heart_rate"] == reference_hr(ch2, r["window"]), r["window"]


def test_missing_sample_in_span_gives_no_heart_rate_but_still_a_result():
    p = SessionProcessor("s")
    feed(p, 400 * 12, skip1={1700})
    p.add_loss(1, 1700, 1700)
    res = {r["window"]: r for r in p.pop_ready("offline")}
    assert res[4]["missing_1"] == 1 and res[4]["complete"] is False
    for w in range(4, 9):                       # spans containing window 4 (samples 1600-1999)
        assert res[w]["heart_rate"] is None
    assert res[9]["heart_rate"] is not None


def test_window_waits_for_both_sensors():
    p = SessionProcessor("s")
    for i in range(1000):
        p.add_sample(1, i, sample("ramp", 0, 1, i))
    assert p.pop_ready() == []                  # sensor 2 has delivered nothing
    for i in range(1000):
        p.add_sample(2, i, sample("ramp", 0, 2, i))
    assert [r["window"] for r in p.pop_ready()] == [0, 1]


def test_loss_range_counts_as_missing_not_invented():
    p = SessionProcessor("s")
    feed(p, 1200, mode="ramp", skip2=set(range(400, 500)))
    p.add_loss(2, 400, 499)
    r = {x["window"]: x for x in p.pop_ready()}
    assert r[1]["missing_2"] == 100 and r[1]["missing_1"] == 0
    assert r[1]["complete"] is False
    assert r[0]["complete"] is True


def test_all_missing_gives_nulls():
    p = SessionProcessor("s")
    feed(p, 1000, mode="ramp", skip2=set(range(400, 800)))
    p.add_loss(2, 400, 799)
    r = {x["window"]: x for x in p.pop_ready()}[1]
    assert r["missing_2"] == 400
    assert r["magnitude"] is None and r["sensor_2"]["ch1"]["mean"] is None


def test_stats_and_magnitude_exact_on_ramp():
    p = SessionProcessor("s")
    feed(p, 1000, mode="ramp")
    r = p.pop_ready("online")[0]
    assert r["mode"] == "online"
    # sensor1 ch1 = 3i+1 for i in 0..399
    assert r["sensor_1"]["ch1"]["min"] == 1 and r["sensor_1"]["ch1"]["max"] == 3 * 399 + 1
    assert r["sensor_1"]["ch1"]["mean"] == round(sum(3 * i + 1 for i in range(400)) / 400, 1)


def test_duplicates_and_late_replays_are_ignored():
    p = SessionProcessor("s")
    feed(p, 1000, mode="ramp")
    first = p.pop_ready()
    feed(p, 1000, mode="ramp")                  # replay everything
    assert p.pop_ready() == [] and len(first) == 2


def test_closed_session_emits_final_window():
    p = SessionProcessor("s")
    feed(p, 800, mode="ramp")
    assert [r["window"] for r in p.pop_ready()] == [0]
    p.close()
    assert [r["window"] for r in p.pop_ready()] == [1]


def test_recovery_replay_matches_uninterrupted_run():
    n = 400 * 40
    full = SessionProcessor("s")
    feed(full, n)
    ref = {r["window"]: r for r in full.pop_ready("offline")}
    resumed = SessionProcessor("s", first_window=14, emit_from=20)
    for i in range(400 * 14, n):
        resumed.add_sample(1, i, sample("signal", 7, 1, i))
        resumed.add_sample(2, i, sample("signal", 7, 2, i))
    got = {r["window"]: r for r in resumed.pop_ready("offline")}
    assert min(got) == 20
    for w, r in got.items():
        assert r == ref[w], w


def test_random_order_arrival_is_equivalent():
    """The cloud receives interleaved, batched data; order must not matter."""
    n = 400 * 15
    a = SessionProcessor("s"); feed(a, n)
    ra = a.pop_ready()
    b = SessionProcessor("s")
    items = [(s, i) for i in range(n) for s in (1, 2)]
    chunks = [items[k:k + 997] for k in range(0, len(items), 997)]
    out = []
    for ch in chunks:                           # in-order chunks, sensors interleaved oddly
        random.Random(1).shuffle(ch)
        for s, i in sorted(ch, key=lambda t: (t[0], t[1])):
            b.add_sample(s, i, sample("signal", 7, s, i))
        out += b.pop_ready()
    assert out == ra
