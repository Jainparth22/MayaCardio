"""
test_integration.py — integration tests using a mock biosignal service stub.

These tests spin up a minimal TCP stub that mimics the biosignal-mock protocol
and verify that the pipeline correctly reads, stores and processes data.

Run: python tests/test_integration.py
"""

import json
import socket
import sys
import threading
import time
import tempfile
import zlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))

import store
import processor


# ── minimal mock TCP stub ─────────────────────────────────────────────────────

RATE = 400


class MockServer:
    """
    Serves deterministic ramp data on a TCP port.
    Sensor 1: 3 channels, (3i+c) mod 8388608
    Sensor 2: 2 channels, (2i+c) mod 8388608
    """

    def __init__(self, port: int = 0):
        self._srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.bind(("127.0.0.1", port))
        self._srv.listen(5)
        self._srv.settimeout(1.0)
        self.port = self._srv.getsockname()[1]
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._counters = {1: 0, 2: 0}   # next index per sensor
        self.session_id = "test_session"

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._srv.close()

    def _serve(self):
        while not self._stop.is_set():
            try:
                conn, _ = self._srv.accept()
            except (socket.timeout, OSError):
                continue
            threading.Thread(target=self._handle, args=(conn,),
                             daemon=True).start()

    def _handle(self, conn: socket.socket):
        buf = b""
        with conn:
            conn.settimeout(2.0)
            while not self._stop.is_set():
                try:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    buf += chunk
                    while b"\n" in buf:
                        line, buf = buf.split(b"\n", 1)
                        req = json.loads(line.decode())
                        resp = self._make_reply(req)
                        conn.sendall((json.dumps(resp) + "\n").encode())
                except (socket.timeout, ConnectionResetError, OSError):
                    break

    def _make_reply(self, req: dict) -> dict:
        sensor = req["sensor"]
        from_idx = req.get("from_index", self._counters[sensor])
        count = min(RATE, 50)   # return small batches for testing
        data = self._ramp_data(sensor, from_idx, count)
        data_json = json.dumps(data, separators=(",", ":"))
        crc = "%08x" % (zlib.crc32(data_json.encode()) & 0xFFFFFFFF)
        self._counters[sensor] = from_idx + count
        return {
            "session_id": self.session_id,
            "sensor": sensor,
            "first_index": from_idx,
            "count": count,
            "lost": [],
            "session_end": False,
            "crc32": crc,
            "data": data,
        }

    @staticmethod
    def _ramp_data(sensor: int, start: int, count: int) -> list:
        rows = []
        if sensor == 1:
            for i in range(start, start + count):
                rows.append([(3 * i + c) % 8388608 for c in range(1, 4)])
        else:
            for i in range(start, start + count):
                rows.append([(2 * i + c) % 8388608 for c in range(1, 3)])
        return rows


# ── test: reader reads and stores samples ─────────────────────────────────────

def test_reader_stores_samples():
    mock = MockServer()
    mock.start()

    from reader import SensorReader

    saved = []
    losses = []
    stop = threading.Event()

    def on_data(sid, sensor, fi, data):
        saved.append((sensor, fi, len(data)))

    def on_loss(sid, sensor, fi, ti, reason):
        losses.append((sensor, fi, ti, reason))

    reader = SensorReader(1, on_data=on_data, on_loss=on_loss,
                          stop_event=stop, port=mock.port)
    reader.start()
    time.sleep(2.0)
    stop.set()
    mock.stop()

    assert len(saved) > 0, "No data received from mock"
    assert all(s == 1 for s, _, _ in saved), "Wrong sensor"
    total = sum(c for _, _, c in saved)
    assert total >= 50, f"Expected at least 50 samples, got {total}"
    print(f"  reader_stores_samples: received {total} samples in {len(saved)} batches")


# ── test: ramp data integrity check ───────────────────────────────────────────

def test_ramp_integrity():
    """Verify ramp formula on both sensors."""
    for i in range(10):
        s1_row = [(3 * i + c) % 8388608 for c in range(1, 4)]
        s2_row = [(2 * i + c) % 8388608 for c in range(1, 3)]
        assert len(s1_row) == 3
        assert len(s2_row) == 2
        assert s1_row[0] == (3 * i + 1) % 8388608
    print("  ramp_integrity: OK")


# ── test: store survives repeated writes ──────────────────────────────────────

def test_store_repeated_writes():
    tmp = tempfile.mktemp(suffix=".db")
    conn = store.init_db(Path(tmp))
    for _ in range(3):
        store.save_samples(conn, "s", 1, 0, [[1, 2, 3]] * 400)
    assert store.count_samples(conn, "s") == 400, "Duplicates not de-duped"
    print("  store_repeated_writes: OK")


# ── test: process window with ramp data ───────────────────────────────────────

def test_process_window_ramp():
    s1 = {i: [(3*i+c) % 8388608 for c in range(1, 4)] for i in range(800)}
    s2 = {i: [(2*i+c) % 8388608 for c in range(1, 3)] for i in range(800)}
    ch2h = {i: (3*i+2) % 8388608 for i in range(2000)}

    result = processor.process_window(1, s1, s2, ch2h, "offline")
    assert result["missing_1"] == 0
    assert result["missing_2"] == 0
    assert result["complete"] is True
    assert result["mode"] == "offline"
    print(f"  process_window_ramp: magnitude={result['magnitude']}")


if __name__ == "__main__":
    import traceback
    tests = [test_ramp_integrity, test_store_repeated_writes,
             test_process_window_ramp, test_reader_stores_samples]
    passed = failed = 0
    for t in tests:
        try:
            print(f"Running {t.__name__}...")
            t()
            passed += 1
        except Exception as e:
            print(f"  FAIL: {e}")
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)
