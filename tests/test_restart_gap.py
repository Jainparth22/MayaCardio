"""The gateway changes under the pipeline's feet while the pipeline is NOT running."""
import json
import socket
import threading
import time
import zlib

from beatkeeper.cloud import NoopBackend
from beatkeeper.engine import Engine
from beatkeeper.reader import Reader
from beatkeeper.verify import check, detect_encoding

from test_end_to_end import device_lines, wait_until


def run_then_stop(data_dir, port):
    eng = Engine(data_dir, NoopBackend(), "127.0.0.1", port)
    eng.start()
    assert wait_until(lambda: (eng.store.max_index(eng.store.latest_session() or "", 2) or 0) > 2000, 20)
    eng.stop()
    eng.reader.join(5)
    sid = eng.store.latest_session()
    return sid, {s: eng.store.max_index(sid, s) for s in (1, 2)}


def test_gateway_restart_while_pipeline_down_records_old_session_loss(tmp_path, start_gateway):
    gw, port = start_gateway(mode="signal", duration=30, speed=4)
    old, last = run_then_stop(tmp_path / "dev", port)
    gw._new_session(gw.sim_time())                          # gateway restarts, pipeline is down
    eng = Engine(tmp_path / "dev", NoopBackend(), "127.0.0.1", port)
    try:
        eng.start()
        assert wait_until(lambda: eng.store.latest_session() != old, 20)
        time.sleep(1.0)
        rows = [(s, a, b) for s, a, b, why in eng.store.losses(old) if why == "gateway_restart"]
        assert sorted(rows) == [(1, last[1] + 1, None), (2, last[2] + 1, None)], rows
    finally:
        eng.stop()


def test_new_session_first_seen_late_flags_its_missing_start(tmp_path, start_gateway):
    gw, port = start_gateway(mode="signal", duration=30, speed=4)
    old, _ = run_then_stop(tmp_path / "dev", port)
    gw._new_session(gw.sim_time())
    time.sleep(3.5)                                         # 14 sim-s: past the 10 s buffer
    eng = Engine(tmp_path / "dev", NoopBackend(), "127.0.0.1", port)
    try:
        eng.start()
        assert wait_until(lambda: eng.store.latest_session() != old
                          and eng.store.session_ended(eng.store.latest_session()), 40)
        rep = check(device_lines(eng.store, eng.store.latest_session()), "signal", 7, 30 * 400)
        assert rep["pass"], rep
        assert rep["flagged_lost"][1] > 0 and rep["flagged_lost"][2] > 0
    finally:
        eng.stop()


def test_unreported_missing_start_of_a_session_is_flagged():
    """A session whose data begins at 100, with nothing reported, is a gap of 0..99."""
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)

    def fake():
        c, _ = srv.accept()
        f = c.makefile("rwb")
        for raw in f:
            req = json.loads(raw)
            first = max(100, req.get("from_index", 0))
            data = json.dumps([[1, 2, 3]] * 5)
            f.write(('{"session_id": "late", "sensor": %d, "first_index": %d, "count": 5, "lost": [], '
                     '"session_end": false, "crc32": "%08x", "data": %s}\n'
                     % (req["sensor"], first, zlib.crc32(data.encode()), data)).encode())
            f.flush()

    threading.Thread(target=fake, daemon=True).start()
    losses = []
    stop = threading.Event()
    r = Reader(lambda *a: None, lambda *a: losses.append(a), lambda *a: None, lambda s, n: None,
               stop, port=srv.getsockname()[1])
    r.start()
    time.sleep(1.5)
    stop.set()
    r.join(3)
    for sensor in (1, 2):
        assert ("late", sensor, 0, 99, "unreported_gap") in losses, losses


def test_verifier_reads_powershell_encodings(tmp_path):
    line = '{"sensor": 1, "from_index": 0, "to_index": 9, "reason": "x"}\n'
    line += line.replace('"sensor": 1', '"sensor": 2')
    for enc in ("utf-8", "utf-8-sig", "utf-16"):
        p = tmp_path / enc
        p.write_text(line, encoding=enc)
        with open(p, encoding=detect_encoding(str(p))) as f:
            assert check(f, "signal", 7, 10)["pass"], enc
