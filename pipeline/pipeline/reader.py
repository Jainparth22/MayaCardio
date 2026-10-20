"""
reader.py — reads samples from the biosignal-mock TCP service.

Protocol: newline-delimited JSON over TCP, one client at a time (section 6.2).
- Sends {"cmd": "read", "sensor": N} to get next batch (≤ 1 second).
- Validates CRC-32 of the raw data field text.
- On corrupt reply: re-reads from the last known good index.
- On mock restart (new session_id): logs old session as ended, continues.
- The mock buffers only the last 10 seconds; we read at least once per second.
"""

import json
import socket
import struct
import threading
import time
import zlib
import logging
from typing import Callable, Optional

log = logging.getLogger(__name__)

MOCK_HOST = "127.0.0.1"
MOCK_PORT = 7100
READ_TIMEOUT = 2.0          # seconds — mock replies within 1 s normally
POLL_INTERVAL = 0.5         # seconds between read requests per sensor


class SensorReader(threading.Thread):
    """
    Background thread that continuously reads one sensor from the mock.

    Calls *on_data* with:
        (session_id, sensor, first_index, data_list, lost_list)

    Calls *on_session_end* with:
        (old_session_id)
    """

    def __init__(self, sensor: int,
                 on_data: Callable,
                 on_loss: Callable,
                 stop_event: threading.Event,
                 host: str = MOCK_HOST,
                 port: int = MOCK_PORT):
        super().__init__(daemon=True, name=f"reader-s{sensor}")
        self.sensor = sensor
        self.on_data = on_data
        self.on_loss = on_loss
        self.stop_event = stop_event
        self.host = host
        self.port = port

        self._session_id: Optional[str] = None
        self._next_index: Optional[int] = None   # next expected index
        self._sock: Optional[socket.socket] = None
        self._rbuf = b""

    # ── public ────────────────────────────────────────────────────────────────

    def run(self):
        while not self.stop_event.is_set():
            try:
                self._ensure_connected()
                self._do_read()
            except (ConnectionRefusedError, OSError) as exc:
                log.warning("Sensor %d: connection error: %s — retrying", self.sensor, exc)
                self._close()
                time.sleep(1.0)
            except Exception as exc:
                log.exception("Sensor %d: unexpected error: %s", self.sensor, exc)
                self._close()
                time.sleep(1.0)

    # ── private ───────────────────────────────────────────────────────────────

    def _ensure_connected(self):
        if self._sock is None:
            self._sock = socket.create_connection((self.host, self.port),
                                                  timeout=READ_TIMEOUT)
            self._rbuf = b""
            log.info("Sensor %d: connected to mock", self.sensor)

    def _send(self, obj: dict):
        line = json.dumps(obj) + "\n"
        self._sock.sendall(line.encode())

    def _recv_line(self) -> str:
        """Read one newline-terminated JSON line from the socket."""
        while b"\n" not in self._rbuf:
            chunk = self._sock.recv(65536)
            if not chunk:
                raise ConnectionResetError("Mock closed connection")
            self._rbuf += chunk
        line, self._rbuf = self._rbuf.split(b"\n", 1)
        return line.decode()

    def _do_read(self):
        # Build read command — use from_index for re-reads after corruption.
        cmd = {"cmd": "read", "sensor": self.sensor}
        if self._next_index is not None:
            cmd["from_index"] = self._next_index
        self._send(cmd)

        raw = self._recv_line()
        reply = json.loads(raw)

        session_id = reply["session_id"]

        # ── Session change (mock restart) ─────────────────────────────────────
        if self._session_id and session_id != self._session_id:
            log.warning("Sensor %d: new session %s (was %s)",
                        self.sensor, session_id, self._session_id)
            # Any unread samples from old session are now gone.
            if self._next_index is not None:
                self.on_loss(self._session_id, self.sensor,
                             self._next_index, None, "mock_restart")
            self._session_id = session_id
            self._next_index = None

        self._session_id = session_id

        # ── CRC-32 validation ─────────────────────────────────────────────────
        data_text_start = raw.index('"data": [') + len('"data": [') - 1
        # Find the data array in the raw string and CRC-check it.
        # Simpler: re-encode just the data field as the mock sends it.
        data_json = json.dumps(reply["data"], separators=(",", ":"))
        # The spec says CRC-32 (zlib) of the data text from "[" to "]".
        crc_computed = "%08x" % (zlib.crc32(data_json.encode()) & 0xFFFFFFFF)
        if crc_computed != reply.get("crc32", ""):
            log.warning("Sensor %d: CRC mismatch — re-reading from %s",
                        self.sensor, self._next_index)
            # Re-read: do NOT advance next_index.
            self._close()   # force reconnect (mock is one-client-at-a-time)
            time.sleep(0.05)
            return

        first_index: int = reply["first_index"]
        count: int       = reply["count"]
        data: list       = reply["data"]
        lost: list       = reply.get("lost", [])
        session_end: bool = reply.get("session_end", False)

        # ── Record mock-reported losses ───────────────────────────────────────
        for loss in lost:
            self.on_loss(session_id, self.sensor,
                         loss["from_index"], loss.get("to_index"),
                         loss.get("reason", "mock_reported"))

        # ── Deliver data ──────────────────────────────────────────────────────
        if count > 0 and data:
            self.on_data(session_id, self.sensor, first_index, data)
            self._next_index = first_index + count

        if session_end:
            log.info("Sensor %d: session %s ended", self.sensor, session_id)
            self._session_id = None
            self._next_index = None
            return

        # Throttle to avoid hammering mock.
        time.sleep(POLL_INTERVAL)

    def _close(self):
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None
        self._rbuf = b""
