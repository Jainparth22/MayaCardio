"""
cloud.py — pluggable cloud uploader.

Supported backends (set CLOUD_BACKEND env var):
  supabase  — Supabase REST API (SUPABASE_URL + SUPABASE_KEY)
  firebase  — Firebase Firestore REST (FIREBASE_PROJECT + FIREBASE_KEY)
  http      — Generic HTTP endpoint (CLOUD_HTTP_URL, any server you run)
  noop      — Disables cloud upload (useful for local testing)

All backends expose the same interface:
  upload_samples(session_id, rows)         → None (raises on error)
  upload_windows(session_id, rows)         → None
  upload_losses(session_id, rows)          → None
  get_sample_count(session_id)             → int
  get_all_windows(session_id)              → list[dict]

Each backend should be idempotent (upsert by primary key).
"""

import json
import logging
import os
import urllib.request
import urllib.error
from typing import Any

log = logging.getLogger(__name__)

_BACKEND = os.environ.get("CLOUD_BACKEND", "noop").lower()
_SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
_SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
_FIREBASE_PROJECT = os.environ.get("FIREBASE_PROJECT", "")
_FIREBASE_KEY = os.environ.get("FIREBASE_KEY", "")
_HTTP_URL = os.environ.get("CLOUD_HTTP_URL", "http://localhost:8080")


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _http_post(url: str, payload: Any, headers: dict = None) -> dict:
    body = json.dumps(payload).encode()
    h = {"Content-Type": "application/json"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, data=body, headers=h, method="POST")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def _http_get(url: str, headers: dict = None) -> Any:
    h = {}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h, method="GET")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


# ── Supabase backend ──────────────────────────────────────────────────────────

class SupabaseBackend:
    """
    Uses Supabase REST API (PostgREST).  Tables required (create via Supabase UI):

    samples(session_id text, sensor int, idx int, values_json text, PRIMARY KEY(session_id,sensor,idx))
    windows(session_id text, window_index int, result_json text, mode text, PRIMARY KEY(session_id,window_index))
    losses(session_id text, sensor int, from_index int, to_index int, reason text)
    """

    def __init__(self):
        assert _SUPABASE_URL, "SUPABASE_URL must be set"
        assert _SUPABASE_KEY, "SUPABASE_KEY must be set"
        self._headers = {
            "apikey": _SUPABASE_KEY,
            "Authorization": f"Bearer {_SUPABASE_KEY}",
            "Prefer": "resolution=ignore-duplicates",
        }

    def _url(self, table: str) -> str:
        return f"{_SUPABASE_URL}/rest/v1/{table}"

    def upload_samples(self, session_id: str, rows):
        payload = [
            {"session_id": session_id, "sensor": s, "idx": idx,
             "values_json": vj}
            for s, idx, vj in rows
        ]
        if not payload:
            return
        _http_post(self._url("samples"), payload, self._headers)

    def upload_windows(self, session_id: str, rows):
        payload = [
            {"session_id": session_id, "window_index": wi,
             "result_json": rj, "mode": mode}
            for wi, rj, mode in rows
        ]
        if not payload:
            return
        _http_post(self._url("windows"), payload, self._headers)

    def upload_losses(self, session_id: str, rows):
        payload = [
            {"session_id": session_id, "sensor": s, "from_index": fi,
             "to_index": ti, "reason": r}
            for s, fi, ti, r in rows
        ]
        if not payload:
            return
        _http_post(self._url("losses"), payload, self._headers)

    def get_sample_count(self, session_id: str) -> int:
        url = (f"{self._url('samples')}?"
               f"session_id=eq.{session_id}&select=count")
        h = dict(self._headers)
        h["Prefer"] = "count=exact"
        try:
            result = _http_get(url, h)
            return result[0]["count"] if result else 0
        except Exception:
            return 0

    def get_all_windows(self, session_id: str):
        url = (f"{self._url('windows')}?"
               f"session_id=eq.{session_id}&order=window_index")
        try:
            rows = _http_get(url, self._headers)
            return [json.loads(r["result_json"]) | {"mode": r["mode"]}
                    for r in rows]
        except Exception:
            return []

    def get_export(self, session_id: str) -> dict:
        samples_url = (f"{self._url('samples')}?"
                       f"session_id=eq.{session_id}&order=sensor,idx")
        losses_url = (f"{self._url('losses')}?"
                      f"session_id=eq.{session_id}&order=sensor,from_index")
        windows_url = (f"{self._url('windows')}?"
                       f"session_id=eq.{session_id}&order=window_index")
        try:
            s_rows = _http_get(samples_url, self._headers)
            samples = [{"sensor": r["sensor"], "index": r["idx"],
                        "values": json.loads(r["values_json"])} for r in s_rows]
        except Exception:
            samples = []
        try:
            l_rows = _http_get(losses_url, self._headers)
            losses = [{"sensor": r["sensor"], "from_index": r["from_index"],
                       "to_index": r.get("to_index"), "reason": r["reason"]} for r in l_rows]
        except Exception:
            losses = []
        try:
            w_rows = _http_get(windows_url, self._headers)
            windows = [json.loads(r["result_json"]) | {"mode": r["mode"]} for r in w_rows]
        except Exception:
            windows = []
        return {"samples": samples, "losses": losses, "windows": windows}


# ── Generic HTTP backend (pairs with cloud_server.py) ────────────────────────

class HttpBackend:
    """
    Works with the bundled cloud_server.py (a tiny Flask server).
    Also works with any compatible REST endpoint.
    """

    def _post(self, path: str, payload):
        try:
            _http_post(f"{_HTTP_URL}{path}", payload)
        except Exception as exc:
            raise RuntimeError(f"Cloud POST {path} failed: {exc}") from exc

    def _get(self, path: str):
        try:
            return _http_get(f"{_HTTP_URL}{path}")
        except Exception as exc:
            raise RuntimeError(f"Cloud GET {path} failed: {exc}") from exc

    def upload_samples(self, session_id: str, rows):
        if not rows:
            return
        self._post("/samples", {
            "session_id": session_id,
            "rows": [{"sensor": s, "idx": idx, "values_json": vj}
                     for s, idx, vj in rows]
        })

    def upload_windows(self, session_id: str, rows):
        if not rows:
            return
        self._post("/windows", {
            "session_id": session_id,
            "rows": [{"window_index": wi, "result_json": rj, "mode": mode}
                     for wi, rj, mode in rows]
        })

    def upload_losses(self, session_id: str, rows):
        if not rows:
            return
        self._post("/losses", {
            "session_id": session_id,
            "rows": [{"sensor": s, "from_index": fi, "to_index": ti, "reason": r}
                     for s, fi, ti, r in rows]
        })

    def get_sample_count(self, session_id: str) -> int:
        try:
            return self._get(f"/sample_count/{session_id}")["count"]
        except Exception:
            return 0

    def get_all_windows(self, session_id: str):
        try:
            return self._get(f"/windows/{session_id}")
        except Exception:
            return []

    def get_export(self, session_id: str) -> dict:
        try:
            return self._get(f"/export/{session_id}")
        except Exception:
            return {"samples": [], "losses": [], "windows": []}


# ── Noop backend ──────────────────────────────────────────────────────────────

class NoopBackend:
    def upload_samples(self, *a, **kw): pass
    def upload_windows(self, *a, **kw): pass
    def upload_losses(self, *a, **kw): pass
    def get_sample_count(self, session_id): return 0
    def get_all_windows(self, session_id): return []
    def get_export(self, session_id): return {"samples": [], "losses": [], "windows": []}


# ── factory ───────────────────────────────────────────────────────────────────

def get_backend():
    if _BACKEND == "supabase":
        return SupabaseBackend()
    elif _BACKEND == "http":
        return HttpBackend()
    elif _BACKEND == "noop":
        return NoopBackend()
    else:
        log.warning("Unknown CLOUD_BACKEND=%r — using noop", _BACKEND)
        return NoopBackend()
