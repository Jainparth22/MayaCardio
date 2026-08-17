"""
reader.py - reads both sensors from the gateway over ONE TCP connection.

The sensor gateway serves one client at a time, so a single thread polls the
sensors in turn on a single socket. (Two threads / two sockets would starve
each other.)

Defences, each one answering a real failure mode:
  * CRC-32 is computed over the *raw text* of the `data` array exactly as sent
    (not a re-serialisation), and a bad reply is never used - it is re-read.
  * Every request after the first carries from_index, so a replayed/duplicated
    reply (first_index < requested) is recognised and discarded without
    desynchronising the request/response stream.
  * The read cursor only advances AFTER the samples are durably stored, so a
    disk error means "read it again", never "skip it".
  * After a restart the cursor resumes from the last stored index; if the
    gateway no longer has the data, the missing range is recorded as a loss.
  * Any index gap that the gateway did not itself report is recorded as an
    "unreported_gap" loss: loss is never silent.
"""

from __future__ import annotations

import json
import logging
import socket
import threading
import time
import zlib
from typing import Callable, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

READ_TIMEOUT = 3.0
POLL_INTERVAL = 0.25
FULL_BATCH = 400          # a full reply -> more is probably waiting, poll again at once


class CorruptReply(Exception):
    pass


def extract_data_text(raw: str) -> str:
    """The exact text of the "data" array, from its '[' to its matching ']'."""
    k = raw.index('"data"')
    start = raw.index("[", k)
    depth = 0
    for pos in range(start, len(raw)):
        c = raw[pos]
        if c == "[":
            depth += 1
        elif c == "]":
            depth -= 1
            if depth == 0:
                return raw[start:pos + 1]
    raise CorruptReply("unterminated data array")


def crc_of(raw: str) -> str:
    return "%08x" % (zlib.crc32(extract_data_text(raw).encode()) & 0xFFFFFFFF)


def subtract_ranges(lo: int, hi: int, covered: List[Tuple[int, Optional[int]]]
                    ) -> List[Tuple[int, int]]:
    """Parts of [lo, hi] not covered by any reported lost range."""
    gaps = [(lo, hi)]
    for a, b in covered:
        b = hi if b is None else b
        nxt = []
        for g0, g1 in gaps:
            if b < g0 or a > g1:
                nxt.append((g0, g1))
                continue
            if a > g0:
                nxt.append((g0, a - 1))
            if b < g1:
                nxt.append((b + 1, g1))
        gaps = nxt
    return gaps


class _SensorState:
    def __init__(self):
        self.session: Optional[str] = None
        self.next_index: Optional[int] = None   # next index we want
        self.resumed = False                    # DB history consulted for this session?
        self.ended = False


class Reader(threading.Thread):
    """
    Callbacks (all run on the reader thread):
      on_batch(session, sensor, first_index, rows)   must persist or raise
      on_loss(session, sensor, from_index, to_index|None, reason)  must persist or raise
      on_session_end(session, sensor)
      resume_index(session, sensor) -> Optional[int]   last stored index (or None)
    """

    def __init__(self, on_batch: Callable, on_loss: Callable,
                 on_session_end: Callable, resume_index: Callable,
                 stop_event: threading.Event,
                 host: str = "127.0.0.1", port: int = 7100):
        super().__init__(daemon=True, name="reader")
        self.on_batch, self.on_loss = on_batch, on_loss
        self.on_session_end, self.resume_index = on_session_end, resume_index
        self.stop_event = stop_event
        self.host, self.port = host, port
        self._sock: Optional[socket.socket] = None
        self._buf = b""
        self._state: Dict[int, _SensorState] = {1: _SensorState(), 2: _SensorState()}
        self._pending_losses: List[tuple] = []     # losses we could not persist yet
        # Session left unfinished by an earlier run of the pipeline (set before start()).
        self.previous_session: Optional[str] = None
        self.stats = {"corrupt": 0, "duplicates": 0, "reconnects": 0,
                      "gaps": 0, "storage_errors": 0}

    # -- main loop --------------------------------------------------------------

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                self._connect()
                self._flush_losses()
                busy = False
                for sensor in (1, 2):
                    if self._state[sensor].ended:
                        continue
                    if self._poll(sensor) >= FULL_BATCH:
                        busy = True
                if not busy:
                    self.stop_event.wait(POLL_INTERVAL)
            except (OSError, ConnectionError, socket.timeout) as exc:
                log.warning("gateway connection problem: %s - reconnecting", exc)
                self._disconnect()
                self.stats["reconnects"] += 1
                self.stop_event.wait(1.0)
            except Exception as exc:  # storage failure etc: never advance, retry
                log.error("read cycle failed (%s: %s) - will retry", type(exc).__name__, exc)
                self.stats["storage_errors"] += 1
                self.stop_event.wait(1.0)
            self._maybe_all_ended()
        self._disconnect()        # free the gateway's single client slot for whoever comes next

    def _maybe_all_ended(self) -> None:
        if all(s.ended for s in self._state.values()):
            # session over on both sensors; keep polling slowly so a restarted
            # gateway (new session) is picked up.
            for s in self._state.values():
                s.ended = False
            self.stop_event.wait(1.0)

    # -- socket -----------------------------------------------------------------

    def _connect(self) -> None:
        if self._sock is None:
            self._sock = socket.create_connection((self.host, self.port), timeout=READ_TIMEOUT)
            self._sock.settimeout(READ_TIMEOUT)
            self._buf = b""
            log.info("connected to gateway %s:%d", self.host, self.port)

    def _disconnect(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
        self._sock, self._buf = None, b""

    def _recv_line(self) -> str:
        while b"\n" not in self._buf:
            chunk = self._sock.recv(65536)
            if not chunk:
                raise ConnectionResetError("gateway closed the connection")
            self._buf += chunk
        line, self._buf = self._buf.split(b"\n", 1)
        return line.decode()

    # -- one poll ---------------------------------------------------------------

    def _request(self, sensor: int, from_index: Optional[int]) -> Tuple[str, dict]:
        cmd = {"cmd": "read", "sensor": sensor}
        if from_index is not None:
            cmd["from_index"] = from_index
        self._sock.sendall((json.dumps(cmd) + "\n").encode())
        for _ in range(8):                       # skip stale duplicates
            raw = self._recv_line()
            reply = json.loads(raw)
            if reply.get("sensor") != sensor:
                self.stats["duplicates"] += 1
                continue
            if from_index is not None and reply["session_id"] == self._state[sensor].session \
                    and reply["first_index"] < from_index and reply["count"] > 0 \
                    and reply["first_index"] + reply["count"] <= from_index:
                self.stats["duplicates"] += 1    # replay of an older reply
                continue
            return raw, reply
        raise ConnectionError("could not resynchronise with gateway")

    def _poll(self, sensor: int) -> int:
        st = self._state[sensor]
        raw, reply = self._request(sensor, st.next_index)

        try:
            if crc_of(raw) != reply.get("crc32", ""):
                raise CorruptReply("crc mismatch")
        except (CorruptReply, ValueError) as exc:
            self.stats["corrupt"] += 1
            log.warning("sensor %d: corrupt reply (%s) - re-reading from %s",
                        sensor, exc, st.next_index)
            time.sleep(0.02)
            return FULL_BATCH                     # cursor unchanged => re-request right away

        session = reply["session_id"]
        if self.previous_session is not None:
            self._account_for_previous(session)
        if st.session != session:
            if st.session is not None and st.next_index is not None and not st.ended:
                self._loss(st.session, sensor, st.next_index, None, "gateway_restart")
            # A new session always starts at index 0. Ask for 0 explicitly rather
            # than "whatever you have next": anything later than 0 that comes
            # back is then caught by gap detection instead of silently skipped.
            st.session, st.next_index, st.resumed, st.ended = session, 0, False, False

        first, count, rows = reply["first_index"], reply["count"], reply["data"]
        reported: List[Tuple[int, Optional[int]]] = [
            (l["from_index"], l.get("to_index")) for l in reply.get("lost", [])]

        # resume after a restart: continue from what is already on disk
        if not st.resumed:
            st.resumed = True
            last = self.resume_index(session, sensor)
            if last is not None:
                st.next_index = last + 1
                if first > st.next_index:
                    return 0                       # re-request from the saved cursor
        # Never None here: a session's cursor starts at 0 (above), BEFORE anything
        # is persisted, so a failed first save is retried rather than skipped.
        expected = st.next_index

        for l in reply.get("lost", []):
            self._loss(session, sensor, l["from_index"], l.get("to_index"),
                       l.get("reason", "gateway_reported"))

        if expected is not None and first > expected and count > 0:
            for a, b in subtract_ranges(expected, first - 1, reported):
                self.stats["gaps"] += 1
                self._loss(session, sensor, a, b, "unreported_gap")

        if count > 0:
            if expected is not None and first + count <= expected:
                self.stats["duplicates"] += 1      # already have all of it
            else:
                self.on_batch(session, sensor, first, rows)   # raises => cursor not advanced
                st.next_index = max(expected or 0, first + count)
        elif reported:
            tops = [b + 1 for _, b in reported if b is not None]
            if tops:
                st.next_index = max([st.next_index or 0] + tops)

        if reply.get("session_end"):
            log.info("sensor %d: session %s ended", sensor, session)
            st.ended = True
            self.on_session_end(session, sensor)
        return count

    # -- losses (never dropped, even if the disk is momentarily failing) --------

    def _account_for_previous(self, current: str) -> None:
        """First contact after a (re)start. If the gateway restarted while we were
        down, whatever it had not yet given us from the old session is gone: say so,
        for BOTH sensors at once, before any data of the new session is stored."""
        old = self.previous_session
        rows = []
        if old != current:
            for sensor in (1, 2):
                last = self.resume_index(old, sensor)
                first_unread = 0 if last is None else last + 1
                log.warning("LOSS sensor %d %d..None (gateway_restart while stopped)",
                            sensor, first_unread)
                rows.append((old, sensor, first_unread, None, "gateway_restart"))
        self.previous_session = None
        self._pending_losses.extend(rows)
        self._flush_losses()

    def _loss(self, session, sensor, a, b, reason) -> None:
        log.warning("LOSS sensor %d %s..%s (%s)", sensor, a, b, reason)
        self._pending_losses.append((session, sensor, a, b, reason))
        self._flush_losses()

    def _flush_losses(self) -> None:
        while self._pending_losses:
            self.on_loss(*self._pending_losses[0])      # raises => stays queued
            self._pending_losses.pop(0)
