"""
cloud.py - the device's client for the BeatKeeper cloud service.

Configuration (environment):
    BEATKEEPER_CLOUD_URL   e.g. http://203.0.113.10:8080   (unset => local-only)
    BEATKEEPER_API_KEY     shared secret sent as X-Api-Key (optional but advised)

Large uploads are gzip-compressed: the payload is highly repetitive JSON and
compresses ~8-10x, which both speeds up catch-up and cuts mobile/metered data.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import urllib.request
from typing import Iterator, List, Optional

log = logging.getLogger(__name__)


class NoopBackend:
    """Local-only operation: nothing is uploaded, the device is permanently 'offline'."""
    enabled = False

    def health(self) -> bool: return False
    def upload_samples(self, rows): pass
    def upload_losses(self, rows): pass
    def upload_windows(self, rows): pass
    def upload_session_ends(self, ids): pass
    def stream(self, path: str) -> Iterator[str]: return iter(())


class HttpBackend:
    enabled = True

    def __init__(self, base_url: str, api_key: str = ""):
        self.base = base_url.rstrip("/")
        self.key = api_key
        self.bytes_sent = 0          # request-body bytes actually put on the wire (post-gzip)

    def _headers(self, extra: Optional[dict] = None) -> dict:
        h = {"Content-Type": "application/json"}
        if self.key:
            h["X-Api-Key"] = self.key
        if extra:
            h.update(extra)
        return h

    def _post(self, path: str, payload) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode()
        headers = self._headers()
        if len(body) > 1024:
            body = gzip.compress(body, compresslevel=3)
            headers["Content-Encoding"] = "gzip"
        req = urllib.request.Request(self.base + path, data=body, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=15) as resp:
            if resp.status != 200:
                raise RuntimeError(f"cloud returned {resp.status}")
            resp.read()
        self.bytes_sent += len(body)       # count only acknowledged uploads

    def health(self) -> bool:
        try:
            req = urllib.request.Request(self.base + "/health", headers=self._headers())
            with urllib.request.urlopen(req, timeout=1.0) as r:
                return r.status == 200
        except Exception:
            return False

    def upload_samples(self, rows: List[tuple]) -> None:
        self._post("/v1/samples", {"rows": rows})          # (session, sensor, idx, values_json)

    def upload_losses(self, rows: List[tuple]) -> None:
        self._post("/v1/losses", {"rows": rows})           # (session, sensor, from, to, reason)

    def upload_windows(self, rows: List[tuple]) -> None:
        self._post("/v1/windows", {"rows": rows})          # (session, window, mode, result_json)

    def upload_session_ends(self, ids: List[str]) -> None:
        self._post("/v1/session_end", {"sessions": ids})

    def stream(self, path: str) -> Iterator[str]:
        """Stream an NDJSON endpoint line by line (constant memory)."""
        req = urllib.request.Request(self.base + path, headers=self._headers())
        with urllib.request.urlopen(req, timeout=60) as resp:
            for line in resp:
                yield line.decode().rstrip("\n")


def get_backend():
    url = os.environ.get("BEATKEEPER_CLOUD_URL", "").strip()
    if not url:
        log.warning("BEATKEEPER_CLOUD_URL not set - running local-only (offline mode)")
        return NoopBackend()
    return HttpBackend(url, os.environ.get("BEATKEEPER_API_KEY", ""))
