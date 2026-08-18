"""Whole system in one process: simulator -> engine -> cloud service."""
import json
import threading
import time

from beatkeeper import cloud_server
from beatkeeper.cloud import HttpBackend
from beatkeeper.engine import Engine
from beatkeeper.verify import check

from conftest import free_port


def wait_until(cond, timeout, step=0.1):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if cond():
            return True
        time.sleep(step)
    return False


def device_lines(store, sid):
    for s, i, vj in store.iter_samples(sid):
        yield json.dumps({"sensor": s, "index": i, "values": json.loads(vj)})
    for s, a, b, why in store.losses(sid):
        yield json.dumps({"sensor": s, "from_index": a, "to_index": b, "reason": why})


def start_cloud(tmp_path, port):
    srv = cloud_server.make_server(str(tmp_path / "cloud.db"), port, host="127.0.0.1")
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def cloud_lines(state, sid):
    h = cloud_server.Handler.__new__(cloud_server.Handler)
    h.state = state
    return list(h._export_lines(sid))


def norm(lines):
    return sorted(json.dumps(json.loads(l), sort_keys=True) for l in lines)


def finished(engine):
    sid = engine.store.latest_session()
    return bool(sid and engine.store.session_ended(sid)
                and engine.store.counts(None)["backlog_all_sessions"] == 0)


def test_online_run_is_complete_identical_in_cloud_and_processed_in_cloud(tmp_path, start_gateway):
    gw, gport = start_gateway(mode="signal", duration=50, speed=6, corrupt_rate=0.05, dup_rate=0.05)
    cport = free_port()
    srv = start_cloud(tmp_path, cport)
    eng = Engine(tmp_path / "dev", HttpBackend(f"http://127.0.0.1:{cport}"), "127.0.0.1", gport)
    try:
        eng.start()
        assert wait_until(lambda: finished(eng), 40), eng.status()
        sid = eng.store.latest_session()

        dev = list(device_lines(eng.store, sid))
        rep = check(dev, "signal", 7, 50 * 400)
        assert rep["pass"], rep
        assert rep["flagged_lost"] == {1: 0, 2: 0}           # nothing was lost at all

        cloud = cloud_lines(cloud_server.Handler.state, sid)
        assert norm(cloud) == norm(dev)                   # exactly-once, complete copy

        # online processing really happened in the cloud, with identical logic
        rows = cloud_server.Handler.state.db.execute(
            "SELECT window_index, result_json, mode FROM windows WHERE session_id=?", (sid,)).fetchall()
        assert len(rows) == 50 and {m for _, _, m in rows} == {"online"}
        local = {w["window"]: w for w in eng.store.windows(sid)}
        for w, rj, _ in rows:
            assert json.loads(rj) | {"mode": "online"} == local[w], w
        assert any(local[w]["heart_rate"] for w in local)
    finally:
        eng.stop()
        srv.shutdown()


def test_outage_then_catch_up_gives_complete_cloud_copy_and_offline_results(tmp_path, start_gateway):
    gw, gport = start_gateway(mode="signal", duration=60, speed=4)
    cport = free_port()
    eng = Engine(tmp_path / "dev", HttpBackend(f"http://127.0.0.1:{cport}"), "127.0.0.1", gport)
    srv = None
    try:
        eng.start()                                           # cloud is down from the start
        assert wait_until(lambda: eng.mode == "offline", 10)
        assert wait_until(lambda: eng.store.last_window(eng.store.latest_session() or "") is not None
                          and eng.store.last_window(eng.store.latest_session()) >= 6, 20)
        srv = start_cloud(tmp_path, cport)                    # network "returns"
        assert wait_until(lambda: finished(eng), 60), eng.status()
        sid = eng.store.latest_session()

        assert check(device_lines(eng.store, sid), "signal", 7, 60 * 400)["pass"]
        assert norm(cloud_lines(cloud_server.Handler.state, sid)) == norm(device_lines(eng.store, sid))

        modes = {}
        for w, rj, m in cloud_server.Handler.state.db.execute(
                "SELECT window_index, result_json, mode FROM windows WHERE session_id=?", (sid,)):
            modes[w] = m
        assert len(modes) == 60
        assert "offline" in modes.values() and "online" in modes.values()
        # offline results (computed on the device) equal what the device stored
        local = {(w["window"], w["mode"]): w for w in eng.store.windows(sid)}
        for w, rj, m in cloud_server.Handler.state.db.execute(
                "SELECT window_index, result_json, mode FROM windows WHERE session_id=?", (sid,)):
            if m == "offline":
                assert json.loads(rj) | {"mode": "offline"} == local[(w, "offline")]
    finally:
        eng.stop()
        if srv:
            srv.shutdown()


def test_crash_and_restart_resumes_without_loss_or_duplicates(tmp_path, start_gateway):
    gw, gport = start_gateway(mode="signal", duration=60, speed=3)
    d = tmp_path / "dev"
    from beatkeeper.cloud import NoopBackend
    e1 = Engine(d, NoopBackend(), "127.0.0.1", gport)
    e1.start()
    assert wait_until(lambda: (e1.store.max_index(e1.store.latest_session() or "", 1) or 0) > 3000, 20)
    e1.stop_event.set()                                       # abrupt: no flush, no cleanup
    e1.reader.join(5)
    time.sleep(1.0)                                           # < 10 sim-s buffer at 3x
    e2 = Engine(d, NoopBackend(), "127.0.0.1", gport)
    try:
        e2.start()
        assert wait_until(lambda: e2.store.session_ended(e2.store.latest_session() or ""), 40)
        sid = e2.store.latest_session()
        rep = check(device_lines(e2.store, sid), "signal", 7, 60 * 400)
        assert rep["pass"], rep
        ws = [w["window"] for w in e2.store.windows(sid)]
        assert ws == sorted(set(ws)) and ws == list(range(60))   # every window exactly once
    finally:
        e2.stop()


def test_gateway_restart_is_recorded_and_new_session_is_complete(tmp_path, start_gateway):
    """Restart of the gateway => new session id, indexes from 0; old unread data is
    reported lost with an unknown end, the new session is captured completely."""
    from beatkeeper.cloud import NoopBackend
    gw, gport = start_gateway(mode="signal", duration=20, speed=4, restarts=[10.0])
    eng = Engine(tmp_path / "dev", NoopBackend(), "127.0.0.1", gport)
    try:
        eng.start()
        assert wait_until(lambda: len({s for s, in eng.store._conn.execute(
            "SELECT session_id FROM sessions")}) == 2 and
            eng.store.session_ended(eng.store.latest_session()), 40)
        new = eng.store.latest_session()
        old = [s for s, in eng.store._conn.execute("SELECT session_id FROM sessions") if s != new][0]
        assert any(b is None and why == "gateway_restart" for _, _, b, why in eng.store.losses(old))
        rep = check(device_lines(eng.store, new), "signal", 7, 20 * 400)
        assert rep["pass"], rep
        assert check(device_lines(eng.store, new), "signal", 7, 20 * 400)["flagged_lost"] == {1: 0, 2: 0}
        # the old session: everything it did store is correct, the rest is flagged
        assert check(device_lines(eng.store, old), "signal", 7, 20 * 400)["pass"]
    finally:
        eng.stop()
