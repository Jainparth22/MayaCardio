import json

from beatkeeper.cloud_server import CloudState
from beatkeeper.simulator import sample


def sample_rows(sid, lo, hi):
    return [(sid, s, i, json.dumps(sample("ramp", 0, s, i), separators=(",", ":")))
            for i in range(lo, hi) for s in (1, 2)]


def windows(state, sid):
    return {w: m for w, m in state.db.execute(
        "SELECT window_index, mode FROM windows WHERE session_id=?", (sid,))}


def test_online_windows_are_produced_as_samples_arrive(tmp_path):
    st = CloudState(str(tmp_path / "c.db"))
    st.ingest_samples(sample_rows("S", 0, 1000))
    assert windows(st, "S") == {0: "online", 1: "online"}
    st.ingest_samples(sample_rows("S", 1000, 1201))
    assert set(windows(st, "S")) == {0, 1, 2}


def test_ingest_is_idempotent(tmp_path):
    st = CloudState(str(tmp_path / "c.db"))
    rows = sample_rows("S", 0, 900)
    st.ingest_samples(rows)
    st.ingest_samples(rows)
    st.ingest_losses([("S", 1, 5000, 5100, "x")] * 3)
    assert st.db.execute("SELECT COUNT(*) FROM samples").fetchone()[0] == 1800
    assert st.db.execute("SELECT COUNT(*) FROM losses").fetchone()[0] == 1


def test_device_offline_windows_are_kept_not_recomputed(tmp_path):
    st = CloudState(str(tmp_path / "c.db"))
    st.ingest_windows([("S", 1, "offline", json.dumps({"window": 1, "marker": "device"}))])
    st.ingest_samples(sample_rows("S", 0, 1300))
    modes = windows(st, "S")
    assert modes[1] == "offline" and modes[0] == "online" and modes[2] == "online"
    rj = st.db.execute("SELECT result_json FROM windows WHERE window_index=1").fetchone()[0]
    assert json.loads(rj)["marker"] == "device"


def test_cloud_restart_fills_hole_before_offline_results(tmp_path):
    """Regression: resuming at MAX(window)+1 skipped windows that preceded
    offline results already uploaded by the device."""
    db = str(tmp_path / "c.db")
    st = CloudState(db)
    st.ingest_samples(sample_rows("S", 0, 1700))             # windows 0..3 done online
    assert set(windows(st, "S")) == {0, 1, 2, 3}
    # device was cut off, then uploaded offline results for 6 and 7 first
    st.ingest_windows([("S", 6, "offline", json.dumps({"window": 6})),
                       ("S", 7, "offline", json.dumps({"window": 7}))])
    del st                                                   # cloud restarts
    st2 = CloudState(db)
    st2.ingest_samples(sample_rows("S", 1700, 3300))         # backlog arrives
    assert set(windows(st2, "S")) == set(range(8))           # 4 and 5 were not skipped
    assert windows(st2, "S")[6] == "offline"


def test_session_end_finalises_last_window(tmp_path):
    st = CloudState(str(tmp_path / "c.db"))
    st.ingest_samples(sample_rows("S", 0, 800))
    assert set(windows(st, "S")) == {0}
    st.ingest_session_end(["S"])
    assert set(windows(st, "S")) == {0, 1}
