"""
engine.py — the central pipeline coordinator.

Wires together: reader threads → store → processor → syncer → cloud.

Data flow:
  SensorReader  ──on_data──►  Engine._on_data  ──►  store.save_samples
                                                 ──►  _window_scheduler
  _window_scheduler  ──►  processor.process_window  ──►  store.save_window
  Syncer (background)  ──►  cloud (if online)

Mode switching (via NetworkMonitor callback):
  online  → offline: Syncer stops uploading; processor is told mode = offline.
  offline → online: Syncer resumes; pending samples/windows are uploaded.
"""

import json
import logging
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional

import store
import processor
import cloud as cloud_module
from reader import SensorReader
from network import NetworkMonitor
from syncer import Syncer

log = logging.getLogger(__name__)

WINDOW_SIZE = processor.WINDOW_SIZE   # 400 samples


class Engine:
    """Top-level pipeline object."""

    def __init__(self, db_path: Optional[Path] = None):
        self._conn = store.init_db(db_path) if db_path else store.init_db()
        self._backend = cloud_module.get_backend()
        self._stop = threading.Event()

        # Current session (set when first data arrives).
        self._session_id: Optional[str] = None
        self._session_lock = threading.Lock()

        # Mode: "online" or "offline".
        self._mode = "online"
        self._mode_lock = threading.Lock()
        self._data_dir = db_path.parent if db_path else (Path(__file__).parent.parent / "data")
        self._mode_file = self._data_dir / "pipeline.mode"
        self._write_mode_file("online")

        # Per-sensor sample buffers: sensor → {global_index: values}
        # We keep a rolling window of samples for heart-rate.
        self._s1: dict[int, list] = {}   # global_idx → [ch1,ch2,ch3]
        self._s2: dict[int, list] = {}   # global_idx → [ch1,ch2]
        self._s1_lock = threading.Lock()
        self._s2_lock = threading.Lock()

        # Next window to process for each sensor's contribution.
        self._next_window: int = 0
        self._window_lock = threading.Lock()

        # Syncer.
        self._syncer = Syncer(
            conn=self._conn,
            backend=self._backend,
            get_session=self._get_session,
            stop_event=self._stop,
        )

        # Network monitor.
        self._net_monitor = NetworkMonitor(
            on_change=self._on_mode_change,
            stop_event=self._stop,
        )

        # Reader threads (created in start()).
        self._readers: list[SensorReader] = []

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def start(self):
        log.info("Pipeline starting")
        self._syncer.start()
        self._net_monitor.start()
        for sensor in (1, 2):
            r = SensorReader(
                sensor=sensor,
                on_data=self._on_data,
                on_loss=self._on_loss,
                stop_event=self._stop,
            )
            r.start()
            self._readers.append(r)
        log.info("Pipeline running")

    def stop(self):
        log.info("Pipeline stopping")
        self._stop.set()
        self._write_mode_file("stopped")

    # ── callbacks ─────────────────────────────────────────────────────────────

    def _on_data(self, session_id: str, sensor: int,
                 first_index: int, data: list):
        # Update session.
        with self._session_lock:
            if self._session_id != session_id:
                log.info("Session: %s", session_id)
                self._session_id = session_id
                # Reset window counter on new session.
                with self._window_lock:
                    self._next_window = 0
                with self._s1_lock:
                    self._s1.clear()
                with self._s2_lock:
                    self._s2.clear()
            store.upsert_session(self._conn, session_id, time.time())

        # Persist to disk first (safety invariant).
        store.save_samples(self._conn, session_id, sensor, first_index, data)

        # Update in-memory buffer.
        if sensor == 1:
            with self._s1_lock:
                for i, row in enumerate(data):
                    self._s1[first_index + i] = row
                self._evict_old(self._s1, first_index + len(data))
        else:
            with self._s2_lock:
                for i, row in enumerate(data):
                    self._s2[first_index + i] = row
                self._evict_old(self._s2, first_index + len(data))

        # Try to process completed windows.
        self._try_process_windows(session_id)

    def _on_loss(self, session_id: str, sensor: int,
                 from_index: int, to_index: Optional[int], reason: str):
        if not session_id:
            return
        log.warning("Loss — sensor %d idx %d..%s: %s",
                    sensor, from_index, to_index, reason)
        store.save_loss(self._conn, session_id, sensor,
                        from_index, to_index, reason)

    def _on_mode_change(self, new_mode: str):
        with self._mode_lock:
            old = self._mode
            self._mode = new_mode
        self._write_mode_file(new_mode)
        log.info("Mode: %s → %s", old, new_mode)
        self._syncer.set_mode(new_mode)

    # ── window processing ─────────────────────────────────────────────────────

    def _try_process_windows(self, session_id: str):
        """Process all complete 1-second windows available in the buffers."""
        while True:
            with self._window_lock:
                w = self._next_window

            w_start = WINDOW_SIZE * w
            w_end   = w_start + WINDOW_SIZE

            # Check both sensors have (at least some) data past the window end.
            with self._s1_lock:
                s1_max = max(self._s1.keys()) if self._s1 else -1
            with self._s2_lock:
                s2_max = max(self._s2.keys()) if self._s2 else -1

            if s1_max < w_end - 1 and s2_max < w_end - 1:
                break   # Not enough data yet.

            with self._mode_lock:
                mode = self._mode

            with self._s1_lock:
                s1_snap = dict(self._s1)
            with self._s2_lock:
                s2_snap = dict(self._s2)

            # Build ch2 history for heart rate (need 2000 samples back).
            ch2_history = {idx: vals[1] for idx, vals in s1_snap.items()}

            result = processor.process_window(w, s1_snap, s2_snap,
                                              ch2_history, mode)
            store.save_window(self._conn, session_id, w, result, mode)
            log.debug("Window %d processed (%s) hr=%s",
                      w, mode, result.get("heart_rate"))

            with self._window_lock:
                self._next_window += 1

    # ── helpers ───────────────────────────────────────────────────────────────

    def _write_mode_file(self, mode_str: str):
        try:
            self._mode_file.parent.mkdir(parents=True, exist_ok=True)
            self._mode_file.write_text(mode_str)
        except Exception:
            pass

    def _get_session(self) -> Optional[str]:
        with self._session_lock:
            return self._session_id

    @staticmethod
    def _evict_old(buf: dict, latest_index: int, keep: int = 5000):
        """Keep only the last *keep* samples to bound memory use."""
        if len(buf) > keep:
            cutoff = latest_index - keep
            stale = [k for k in buf if k < cutoff]
            for k in stale:
                del buf[k]

    # ── status ────────────────────────────────────────────────────────────────

    def status(self) -> dict:
        sid = self._get_session()
        saved = store.count_samples(self._conn, sid) if sid else 0
        in_cloud = store.count_synced_samples(self._conn, sid) if sid else 0
        backlog = store.count_unsynced_samples(self._conn, sid) if sid else 0
        with self._mode_lock:
            mode = self._mode
        return {
            "session_id": sid,
            "mode": mode,
            "samples_saved": saved,
            "samples_in_cloud": in_cloud,
            "backlog": backlog,
        }
