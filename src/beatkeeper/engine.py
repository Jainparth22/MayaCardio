"""
engine.py - wires reader -> store -> processor -> syncer, and owns the mode.

Invariant that everything else depends on:
    a sample is written to disk BEFORE anything else happens to it.

Processing placement
    online  : the CLOUD computes window results (same SessionProcessor class).
              The device still computes them locally as a shadow, but does not
              upload them - the cloud's result is the authoritative one.
    offline : the DEVICE's result is the real one; it is uploaded on reconnect.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Optional

from . import cloud as cloud_mod
from .network import NetworkMonitor
from .processor import SessionProcessor, WINDOW_SIZE
from .reader import Reader
from .store import Store
from .syncer import Syncer

log = logging.getLogger(__name__)
REPLAY_WINDOWS = 6      # warm-up windows replayed after a restart


class Engine:
    def __init__(self, data_dir: Path, backend=None,
                 gateway_host: str = "127.0.0.1", gateway_port: int = 7100):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.store = Store(self.data_dir / "beatkeeper.db")
        self.backend = backend or cloud_mod.get_backend()
        self.stop_event = threading.Event()

        self._mode = "offline" if not self.backend.enabled else "online"
        self._mode_lock = threading.Lock()
        self._session: Optional[str] = None
        self._proc: Optional[SessionProcessor] = None
        self._ended = {1: False, 2: False}

        self.syncer = Syncer(self.store, self.backend, self.stop_event)
        self.syncer.set_mode(self._mode)
        self.monitor = NetworkMonitor(self.backend.health, self._set_mode,
                                      self.stop_event, initial_mode=self._mode)
        self.reader = Reader(self._on_batch, self._on_loss, self._on_session_end,
                             self._resume_index, self.stop_event,
                             gateway_host, gateway_port)
        self._hb = threading.Thread(target=self._heartbeat, daemon=True, name="heartbeat")

    # -- lifecycle --------------------------------------------------------------

    def start(self) -> None:
        self._recover()
        latest = self.store.latest_session()
        if latest and not self.store.session_ended(latest):
            self.reader.previous_session = latest      # may turn out to be gone
        self.syncer.start()
        self.monitor.start()
        self._hb.start()
        self.reader.start()
        log.info("beatkeeper running (mode=%s)", self.mode)

    def stop(self) -> None:
        self.stop_event.set()
        self._write_status(stopped=True)

    @property
    def mode(self) -> str:
        with self._mode_lock:
            return self._mode

    def _set_mode(self, mode: str) -> None:
        with self._mode_lock:
            old, self._mode = self._mode, mode
        if old != mode:
            log.info("MODE %s -> %s", old, mode)
        self.syncer.set_mode(mode)
        self._write_status()

    # -- crash recovery ---------------------------------------------------------

    def _recover(self) -> None:
        """Rebuild processing state for the latest session from what is on disk."""
        sid = self.store.latest_session()
        if not sid or self.store.session_ended(sid):
            return
        last = self.store.last_window(sid)
        emit_from = 0 if last is None else last + 1
        self._begin_session(sid, emit_from)
        first = max(0, emit_from - REPLAY_WINDOWS)
        lo = WINDOW_SIZE * first
        for sensor in (1, 2):
            hi = (self.store.max_index(sid, sensor) or -1) + 1
            for idx, vals in self.store.read_samples(sid, sensor, lo, hi):
                self._proc.add_sample(sensor, idx, vals)
        for sensor, a, b, _ in self.store.losses(sid):
            self._proc.add_loss(sensor, a, b)
        self._drain()
        log.info("recovered session %s, resuming at window %d", sid, emit_from)

    def _begin_session(self, sid: str, emit_from: int = 0) -> None:
        first = max(0, emit_from - REPLAY_WINDOWS)
        self._session = sid
        self._proc = SessionProcessor(sid, first_window=first, emit_from=emit_from)
        self._ended = {1: False, 2: False}

    # -- reader callbacks (reader thread) ---------------------------------------

    def _ensure_session(self, sid: str) -> None:
        if self._session != sid:
            log.info("new session %s", sid)
            self._begin_session(sid)
        self.store.touch_session(sid, time.time())

    def _on_batch(self, sid: str, sensor: int, first: int, rows) -> None:
        self._ensure_session(sid)
        self.store.save_samples(sid, sensor, first, rows)      # 1. disk first
        self._proc.add_samples(sensor, first, rows)            # 2. then compute
        self._drain()

    def _on_loss(self, sid: str, sensor: int, a: int, b: Optional[int], reason: str) -> None:
        self._ensure_session(sid)
        self.store.save_loss(sid, sensor, a, b, reason)
        if self._proc and self._session == sid:
            self._proc.add_loss(sensor, a, b)
            self._drain()

    def _on_session_end(self, sid: str, sensor: int) -> None:
        if self._session != sid:
            return
        self._ended[sensor] = True
        if all(self._ended.values()):
            self.store.end_session(sid)
            self._proc.close()
            self._drain()

    def _resume_index(self, sid: str, sensor: int) -> Optional[int]:
        return self.store.max_index(sid, sensor)

    def _drain(self) -> None:
        mode = self.mode
        for res in self._proc.pop_ready(mode):
            # online windows are the cloud's job; mark them "synced" so only
            # genuinely offline results are ever uploaded.
            self.store.save_window(self._session, res["window"], mode, res,
                                   synced=(mode == "online"))

    # -- status -----------------------------------------------------------------

    def status(self) -> dict:
        sid = self._session or self.store.latest_session()
        return {"mode": self.mode, "session_id": sid, **self.store.counts(sid)}

    def _heartbeat(self) -> None:
        while not self.stop_event.wait(1.0):
            self._write_status()

    def _write_status(self, stopped: bool = False) -> None:
        try:
            # light-weight on purpose: no table scans while the writer is busy.
            # `beatkeeper status` counts rows itself over its own read connection.
            doc = {"mode": self.mode, "session_id": self._session,
                   "running": not stopped, "updated_at": time.time(),
                   "reader": self.reader.stats,
                   "uploaded_bytes": getattr(self.backend, "bytes_sent", 0)}
            tmp = self.data_dir / "status.json.tmp"
            tmp.write_text(json.dumps(doc))
            os.replace(tmp, self.data_dir / "status.json")     # atomic
        except Exception as exc:                               # status must never kill the pipeline
            log.debug("status write failed: %s", exc)
