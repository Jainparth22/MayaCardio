"""
network.py - decides online/offline with hysteresis.

The probe asks the *cloud itself* (GET /health), not a public DNS server:
"the internet works" is not the same as "my cloud accepts my data".

Timing (worst case, both < 10 s):
    online -> offline : FAIL_THRESHOLD x (PROBE_TIMEOUT + PROBE_INTERVAL) = 3 x 2 s = 6 s
    offline -> online : PASS_THRESHOLD x (RTT + PROBE_INTERVAL)           ~ 3 s
Hysteresis (needing 3 in a row) stops a flaky link from flapping the mode.
"""

from __future__ import annotations

import logging
import threading
from typing import Callable

log = logging.getLogger(__name__)

PROBE_INTERVAL = 1.0
PROBE_TIMEOUT = 1.0       # enforced by the probe function (HttpBackend.health)
FAIL_THRESHOLD = 3
PASS_THRESHOLD = 3


class NetworkMonitor(threading.Thread):
    def __init__(self, probe: Callable[[], bool], on_change: Callable[[str], None],
                 stop_event: threading.Event, initial_mode: str = "online",
                 interval: float = PROBE_INTERVAL,
                 fail_threshold: int = FAIL_THRESHOLD,
                 pass_threshold: int = PASS_THRESHOLD):
        super().__init__(daemon=True, name="net-monitor")
        self.probe, self.on_change, self.stop_event = probe, on_change, stop_event
        self.mode = initial_mode
        self.interval = interval
        self.fail_threshold, self.pass_threshold = fail_threshold, pass_threshold
        self._streak = 0

    def observe(self, reachable: bool) -> None:
        """Feed one probe result (separate from run() so it is unit-testable)."""
        wants = "online" if reachable else "offline"
        if wants == self.mode:
            self._streak = 0
            return
        self._streak += 1
        needed = self.fail_threshold if wants == "offline" else self.pass_threshold
        if self._streak >= needed:
            self.mode, self._streak = wants, 0
            log.info("network: switching to %s", wants)
            try:
                self.on_change(wants)
            except Exception:
                log.exception("mode-change callback failed")

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                ok = bool(self.probe())
            except Exception:
                ok = False
            self.observe(ok)
            self.stop_event.wait(self.interval)
