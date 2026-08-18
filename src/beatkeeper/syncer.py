"""
syncer.py - uploads local data to the cloud (at-least-once, idempotent).

Delivery contract
    A row is flagged `synced` only AFTER the cloud acknowledged it. A crash or
    network failure mid-upload just means the batch is sent again; the cloud
    stores by primary key, so a repeat changes nothing. (At-least-once delivery
    + idempotent writes = exactly-once *effect*.)

Catch-up speed
    After an outage the syncer does NOT sleep between batches: it drains the
    backlog flat-out in large gzip-compressed batches and only idles when
    there is nothing left. (An earlier design that slept 1 s between 2 000-row
    batches needed ~80 minutes to clear a 2-hour outage.)

Upload order per cycle
    1. ended sessions   2. losses   3. offline-computed windows   4. samples
    Windows go before samples so the cloud knows which windows the device has
    already computed offline and does not re-compute them as "online".
"""

from __future__ import annotations

import logging
import threading
from typing import Dict, List

log = logging.getLogger(__name__)

IDLE_INTERVAL = 0.5
SAMPLE_BATCH = 8000
WINDOW_BATCH = 500


class Syncer(threading.Thread):
    def __init__(self, store, backend, stop_event: threading.Event):
        super().__init__(daemon=True, name="syncer")
        self.store, self.backend, self.stop_event = store, backend, stop_event
        self._online = False
        self._wake = threading.Event()
        self.stats = {"batches": 0, "errors": 0}

    def set_mode(self, mode: str) -> None:
        self._online = mode == "online"
        self._wake.set()

    def run(self) -> None:
        while not self.stop_event.is_set():
            worked = False
            if self._online and self.backend.enabled:
                try:
                    worked = self.cycle()
                except Exception as exc:
                    self.stats["errors"] += 1
                    log.warning("upload failed (%s) - will retry", exc)
                    self.stop_event.wait(1.0)
                    continue
            if not worked:                 # idle only when nothing was uploaded
                self._wake.wait(IDLE_INTERVAL)
                self._wake.clear()

    def cycle(self) -> bool:
        """Upload one round. Returns True if anything was sent."""
        did = False
        ended = self.store.unsynced_ended_sessions()
        if ended:
            self.backend.upload_session_ends(ended)
            self.store.mark_session_end_synced(ended)
            did = True

        losses = self.store.unsynced_losses()
        if losses:
            self.backend.upload_losses([(s, sn, a, b, r) for _, s, sn, a, b, r in losses])
            self.store.mark_losses_synced([l[0] for l in losses])
            did = True

        while self._online:
            wins = self.store.unsynced_windows(WINDOW_BATCH)
            if not wins:
                break
            self.backend.upload_windows(wins)
            self.store.mark_windows_synced([(s, w, m) for s, w, m, _ in wins])
            did = True

        rows = self.store.unsynced_samples(SAMPLE_BATCH)
        if rows:
            self.backend.upload_samples([(s, sn, i, v) for _, s, sn, i, v in rows])
            self.store.mark_samples_synced([r[0] for r in rows])
            self.stats["batches"] += 1
            did = True
        return did
