"""
syncer.py — background thread that uploads local data to the cloud.

Responsibilities:
1. In online mode: upload newly saved samples and window results within seconds.
2. After a network return (catch-up): upload every sample the cloud is missing.
3. After a switch to offline: stop uploading (samples still saved locally).

Upload is done in batches to stay within free-tier rate limits and to reduce
the number of HTTP round trips.  Each batch is committed to the local DB as
synced only after a successful cloud ACK — so a crash during upload
re-sends the batch rather than silently losing it (at-least-once delivery;
the cloud endpoint must be idempotent by primary key).
"""

import json
import logging
import threading
import time
from typing import Optional

import store
import cloud

log = logging.getLogger(__name__)

UPLOAD_INTERVAL = 1.0       # seconds between upload cycles in online mode
SAMPLE_BATCH   = 2000       # max samples per HTTP call
WINDOW_BATCH   = 200        # max windows per HTTP call


class Syncer(threading.Thread):
    """
    Uploads local data to the cloud.

    Call set_mode("online"/"offline") when network changes.
    """

    def __init__(self, conn, backend: cloud.NoopBackend,
                 get_session: callable,
                 stop_event: threading.Event):
        super().__init__(daemon=True, name="syncer")
        self._conn     = conn
        self._backend  = backend
        self._get_session = get_session
        self.stop_event = stop_event

        self._mode = "online"
        self._mode_lock = threading.Lock()
        self._mode_changed = threading.Event()

    # ── public ────────────────────────────────────────────────────────────────

    def set_mode(self, mode: str):
        with self._mode_lock:
            self._mode = mode
        self._mode_changed.set()

    @property
    def mode(self) -> str:
        with self._mode_lock:
            return self._mode

    def run(self):
        while not self.stop_event.is_set():
            if self.mode == "online":
                session_id = self._get_session()
                if session_id:
                    try:
                        self._upload_cycle(session_id)
                    except Exception as exc:
                        log.warning("Upload cycle error: %s", exc)
            self._mode_changed.wait(timeout=UPLOAD_INTERVAL)
            self._mode_changed.clear()

    # ── private ───────────────────────────────────────────────────────────────

    def _upload_cycle(self, session_id: str):
        """Upload unsynced samples, windows, and losses for *session_id*."""
        self._upload_samples(session_id)
        self._upload_windows(session_id)
        self._upload_losses(session_id)

    def _upload_samples(self, session_id: str):
        rows = store.get_unsynced_samples(self._conn, session_id, SAMPLE_BATCH)
        if not rows:
            return
        self._backend.upload_samples(session_id, rows)
        keys = [(s, idx) for s, idx, _ in rows]
        store.mark_samples_synced(self._conn, session_id, keys)
        log.debug("Synced %d samples for %s", len(rows), session_id)

    def _upload_windows(self, session_id: str):
        rows = store.get_unsynced_windows(self._conn, session_id)
        if not rows:
            return
        batch = rows[:WINDOW_BATCH]
        self._backend.upload_windows(session_id, batch)
        widxs = [wi for wi, _, _ in batch]
        store.mark_windows_synced(self._conn, session_id, widxs)
        log.debug("Synced %d windows for %s", len(batch), session_id)

    def _upload_losses(self, session_id: str):
        rows = store.get_unsynced_losses(self._conn, session_id)
        if not rows:
            return
        # rows: [(id, sensor, from_index, to_index, reason)]
        payload = [(s, fi, ti, r) for _, s, fi, ti, r in rows]
        self._backend.upload_losses(session_id, payload)
        loss_ids = [lid for lid, _, _, _, _ in rows]
        store.mark_losses_synced(self._conn, session_id, loss_ids)
        log.debug("Synced %d loss records for %s", len(rows), session_id)
