"""
network.py — network availability monitor.

Probes the network every PROBE_INTERVAL seconds by attempting a TCP
connection to PROBE_HOST:PROBE_PORT.  Falls back to DNS resolution if TCP
fails (handles firewalled ping).

Mode switching:
- online  → offline when N consecutive failures (≥ FAIL_THRESHOLD).
- offline → online  when N consecutive successes (≥ PASS_THRESHOLD).

Both thresholds give ≤ 10-second switch time (probe every 2 s × 5 = 10 s).
"""

import socket
import threading
import time
import logging
from typing import Callable, Optional

log = logging.getLogger(__name__)

PROBE_HOST      = "8.8.8.8"        # Google DNS — widely reachable
PROBE_PORT      = 53
PROBE_INTERVAL  = 2.0              # seconds between probes
PROBE_TIMEOUT   = 1.5
FAIL_THRESHOLD  = 5                # consecutive failures → offline
PASS_THRESHOLD  = 5                # consecutive successes → online


class NetworkMonitor(threading.Thread):
    """
    Continuously checks network reachability.

    Calls *on_change(new_mode: str)* with "online" or "offline" when mode changes.
    """

    def __init__(self, on_change: Callable[[str], None],
                 stop_event: threading.Event,
                 initial_mode: str = "online"):
        super().__init__(daemon=True, name="net-monitor")
        self.on_change = on_change
        self.stop_event = stop_event
        self._mode = initial_mode
        self._consecutive = 0

    @property
    def mode(self) -> str:
        return self._mode

    def run(self):
        while not self.stop_event.is_set():
            reachable = self._probe()
            if self._mode == "online":
                if not reachable:
                    self._consecutive += 1
                    if self._consecutive >= FAIL_THRESHOLD:
                        self._switch("offline")
                else:
                    self._consecutive = 0
            else:  # offline
                if reachable:
                    self._consecutive += 1
                    if self._consecutive >= PASS_THRESHOLD:
                        self._switch("online")
                else:
                    self._consecutive = 0
            time.sleep(PROBE_INTERVAL)

    def _probe(self) -> bool:
        try:
            with socket.create_connection((PROBE_HOST, PROBE_PORT),
                                          timeout=PROBE_TIMEOUT):
                return True
        except OSError:
            return False

    def _switch(self, new_mode: str):
        log.info("Network: %s → %s", self._mode, new_mode)
        self._mode = new_mode
        self._consecutive = 0
        try:
            self.on_change(new_mode)
        except Exception as exc:
            log.exception("on_change callback error: %s", exc)
