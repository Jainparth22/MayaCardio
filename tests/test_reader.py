import json
import sqlite3
import socket
import threading
import time
import zlib

from beatkeeper.reader import Reader, crc_of, extract_data_text, subtract_ranges
from beatkeeper.simulator import sample


def test_crc_is_over_raw_text_not_a_reserialisation():
    raw = '{"a": 1, "data": [[1,  2], [3,4]], "crc32": "x"}'     # odd spacing on purpose
    assert extract_data_text(raw) == "[[1,  2], [3,4]]"
    assert crc_of(raw) == "%08x" % zlib.crc32(b"[[1,  2], [3,4]]")


def test_subtract_ranges():
    assert subtract_ranges(10, 19, []) == [(10, 19)]
    assert subtract_ranges(10, 19, [(10, 19)]) == []
    assert subtract_ranges(10, 19, [(12, 14)]) == [(10, 11), (15, 19)]
    assert subtract_ranges(10, 19, [(15, None)]) == [(10, 14)]


class Sink:
    def __init__(self):
        self.rows = {1: {}, 2: {}}
        self.losses = []
        self.ended = set()

    def batch(self, sid, sensor, first, rows):
        for k, r in enumerate(rows):
            self.rows[sensor][(sid, first + k)] = r

    def loss(self, sid, sensor, a, b, why):
        self.losses.append((sid, sensor, a, b, why))

    def end(self, sid, sensor):
        self.ended.add((sid, sensor))


def run_reader(port, sink, resume=lambda s, n: None, seconds=6.0, until=None):
    stop = threading.Event()
    r = Reader(sink.batch, sink.loss, sink.end, resume, stop, port=port)
    r.start()
    t0 = time.time()
    while time.time() - t0 < seconds and not (until and until()):
        time.sleep(0.05)
    stop.set()
    r.join(3)
    return r


def test_reads_everything_exactly_once_despite_corruption_and_duplicates(start_gateway):
    gw, port = start_gateway(duration=20, corrupt_rate=0.2, dup_rate=0.2, speed=8)
    sink = Sink()
    r = run_reader(port, sink, until=lambda: len(sink.ended) == 2, seconds=20)
    assert len(sink.ended) == 2
    for s in (1, 2):
        assert sorted(i for _, i in sink.rows[s]) == list(range(20 * 400))
    assert sink.losses == []
    assert r.stats["corrupt"] > 0 and r.stats["duplicates"] > 0      # the faults really fired
    assert sink.rows[1][(gw.session_id, 7345)] == sample("ramp", 7, 1, 7345)


def test_gateway_reported_loss_is_recorded(start_gateway):
    gw, port = start_gateway(duration=30, speed=8, dropouts=[(2, 3.0, 4.0)])
    sink = Sink()
    run_reader(port, sink, until=lambda: len(sink.ended) == 2, seconds=20)
    assert any(l[1] == 2 and l[4] == "sensor_dropout" for l in sink.losses)
    have = {i for _, i in sink.rows[2]}
    lost = {i for l in sink.losses if l[1] == 2 for i in range(l[2], l[3] + 1)}
    assert have | lost == set(range(30 * 400)) and not (have & lost)


def test_unreported_gap_is_never_silent():
    """A gateway that skips indices without saying so must still be caught."""
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]

    def fake():
        c, _ = srv.accept()
        f = c.makefile("rwb")
        n1 = 0
        for raw in f:
            req = json.loads(raw)
            if req["sensor"] == 1:
                first = [0, 5, 1000][min(n1, 2)]       # 0..4, 5..9, then jumps to 1000
                n1 += 1
            else:
                first = req.get("from_index", 0)
            data = json.dumps([[1, 2, 3]] * 5)
            crc = "%08x" % zlib.crc32(data.encode())
            f.write(('{"session_id": "g", "sensor": %d, "first_index": %d, "count": 5, '
                     '"lost": [], "session_end": false, "crc32": "%s", "data": %s}\n'
                     % (req["sensor"], first, crc, data)).encode())
            f.flush()

    threading.Thread(target=fake, daemon=True).start()
    sink = Sink()
    run_reader(port, sink, seconds=2.0)
    assert any(l[4] == "unreported_gap" and l[1] == 1 and (l[2], l[3]) == (10, 999)
               for l in sink.losses), sink.losses


def test_resumes_from_last_stored_index_after_restart(start_gateway):
    gw, port = start_gateway(duration=60, speed=10)
    time.sleep(1.0)                                    # ~4000 samples are now buffered
    sink = Sink()
    run_reader(port, sink, resume=lambda sid, s: 1500, seconds=1.0)
    assert sorted(i for _, i in sink.rows[1])[0] == 1501   # continued; did not skip to "now"


def test_storage_failure_never_skips_data(start_gateway):
    """Disk errors must mean 'read again', never 'skip': the cursor only moves after a durable save."""
    gw, port = start_gateway(duration=20, speed=2)
    sink = Sink()
    state = {"fail": 2}
    real = sink.batch

    def flaky(sid, sensor, first, rows):
        if state["fail"] > 0:
            state["fail"] -= 1
            raise sqlite3.OperationalError("database or disk is full")
        real(sid, sensor, first, rows)

    stop = threading.Event()
    r = Reader(flaky, sink.loss, sink.end, lambda s, n: None, stop, port=port)
    r.start()
    t0 = time.time()
    while time.time() - t0 < 30 and len(sink.ended) < 2:
        time.sleep(0.1)
    stop.set()
    r.join(3)
    assert r.stats["storage_errors"] == 2
    for s in (1, 2):
        assert sorted(i for _, i in sink.rows[s]) == list(range(20 * 400))
    assert sink.losses == []
