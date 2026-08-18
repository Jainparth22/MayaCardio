"""
cloud_server.py - the BeatKeeper cloud service (stdlib only; runs on any free VM).

Responsibilities
  * ingest samples / losses / offline-computed windows (idempotent, fsync'd
    BEFORE the 200 OK is sent - the device deletes nothing, but it stops
    re-sending once it hears "ok", so "ok" must mean "durable")
  * ONLINE PROCESSING: runs the very same SessionProcessor as the device over
    the samples as they arrive and stores a result for every second of data
  * stream exports as NDJSON (constant memory, even for millions of samples)

Run:   BEATKEEPER_API_KEY=secret python -m beatkeeper.cloud_server --port 8080
"""

from __future__ import annotations

import argparse
import gzip
import hmac
import json
import logging
import os
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, Optional

from .processor import SessionProcessor, WINDOW_SIZE

log = logging.getLogger("beatkeeper.cloud")
REPLAY_WINDOWS = 6
MAX_BODY = 64 * 1024 * 1024

SCHEMA = """
CREATE TABLE IF NOT EXISTS samples (
    session_id TEXT NOT NULL, sensor INTEGER NOT NULL, idx INTEGER NOT NULL,
    values_json TEXT NOT NULL, PRIMARY KEY (session_id, sensor, idx));
CREATE TABLE IF NOT EXISTS losses (
    id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL, sensor INTEGER NOT NULL,
    from_index INTEGER NOT NULL, to_index INTEGER, reason TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS ux_losses
    ON losses(session_id, sensor, from_index, IFNULL(to_index,-1), reason);
CREATE TABLE IF NOT EXISTS windows (
    session_id TEXT NOT NULL, window_index INTEGER NOT NULL, mode TEXT NOT NULL,
    result_json TEXT NOT NULL, processed_at REAL NOT NULL,
    PRIMARY KEY (session_id, window_index, mode));
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY, first_seen REAL NOT NULL, ended INTEGER NOT NULL DEFAULT 0);
"""


class CloudState:
    def __init__(self, db_path: str):
        self.db = sqlite3.connect(db_path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")      # "ok" must mean durable
        with self.db:
            self.db.executescript(SCHEMA)
        self.lock = threading.RLock()
        self.procs: Dict[str, SessionProcessor] = {}

    # -- processors -------------------------------------------------------------

    def _proc(self, sid: str) -> SessionProcessor:
        p = self.procs.get(sid)
        if p is not None:
            return p
        # Resume at the FIRST window we do not have (not MAX+1): offline results
        # uploaded by the device can sit beyond a hole that still needs computing.
        have = {r[0] for r in self.db.execute(
            "SELECT window_index FROM windows WHERE session_id=?", (sid,))}
        emit_from = 0
        while emit_from in have:
            emit_from += 1
        first = max(0, emit_from - REPLAY_WINDOWS)
        p = SessionProcessor(sid, first_window=first, emit_from=emit_from)
        lo = WINDOW_SIZE * first                          # replay after a cloud restart
        for sensor, idx, vj in self.db.execute(
                "SELECT sensor, idx, values_json FROM samples WHERE session_id=? AND idx>=?",
                (sid, lo)):
            p.add_sample(sensor, idx, json.loads(vj))
        for sensor, a, b in self.db.execute(
                "SELECT sensor, from_index, to_index FROM losses WHERE session_id=?", (sid,)):
            p.add_loss(sensor, a, b)
        if self.db.execute("SELECT ended FROM sessions WHERE session_id=?",
                           (sid,)).fetchone() == (1,):
            p.close()
        self.procs[sid] = p
        return p

    def _drain(self, sid: str) -> None:
        now = time.time()
        for res in self._proc(sid).pop_ready("online"):
            w = res["window"]
            # the device already computed this window offline -> keep that one
            done = self.db.execute(
                "SELECT 1 FROM windows WHERE session_id=? AND window_index=?",
                (sid, w)).fetchone()
            if done:
                continue
            self.db.execute("INSERT OR REPLACE INTO windows VALUES (?,?,?,?,?)",
                            (sid, w, "online", json.dumps(res), now))

    def _see(self, sids) -> None:
        now = time.time()
        for sid in set(sids):
            self.db.execute("INSERT OR IGNORE INTO sessions(session_id, first_seen) VALUES (?,?)",
                            (sid, now))

    # -- ingest -----------------------------------------------------------------

    def ingest_samples(self, rows) -> None:
        with self.lock:
            sids = {r[0] for r in rows}
            with self.db:                                   # one durable transaction
                self._see(sids)
                self.db.executemany(
                    "INSERT OR IGNORE INTO samples(session_id,sensor,idx,values_json) "
                    "VALUES (?,?,?,?)", rows)
                for sid in sids:                            # feed processor, store windows
                    p = self._proc(sid)
                    for s, sn, idx, vj in rows:
                        if s == sid:
                            p.add_sample(sn, idx, json.loads(vj))
                    self._drain(sid)

    def ingest_losses(self, rows) -> None:
        with self.lock, self.db:
            self._see({r[0] for r in rows})
            self.db.executemany(
                "INSERT OR IGNORE INTO losses(session_id,sensor,from_index,to_index,reason) "
                "VALUES (?,?,?,?,?)", rows)
            for sid, sn, a, b, _ in rows:
                self._proc(sid).add_loss(sn, a, b)
            for sid in {r[0] for r in rows}:
                self._drain(sid)

    def ingest_windows(self, rows) -> None:
        now = time.time()
        with self.lock, self.db:
            self._see({r[0] for r in rows})
            self.db.executemany(
                "INSERT OR REPLACE INTO windows(session_id,window_index,mode,result_json,processed_at) "
                "VALUES (?,?,?,?,?)", [(s, w, m, rj, now) for s, w, m, rj in rows])

    def ingest_session_end(self, sids) -> None:
        with self.lock, self.db:
            for sid in sids:
                self._see([sid])
                self.db.execute("UPDATE sessions SET ended=1 WHERE session_id=?", (sid,))
                self._proc(sid).close()
                self._drain(sid)

    # -- queries ----------------------------------------------------------------

    def latency(self, sid: str) -> dict:
        t0 = self.db.execute("SELECT first_seen FROM sessions WHERE session_id=?",
                             (sid,)).fetchone()
        if not t0:
            return {"n": 0}
        lat = sorted(pa - (t0[0] + (w + 1)) for w, pa in self.db.execute(
            "SELECT window_index, processed_at FROM windows WHERE session_id=? AND mode='online'",
            (sid,)))
        if not lat:
            return {"n": 0}
        pick = lambda q: round(lat[min(len(lat) - 1, int(q * len(lat)))], 2)
        return {"n": len(lat), "p50_s": pick(.5), "p95_s": pick(.95), "max_s": round(lat[-1], 2),
                "note": "approx: processed_at - (first_seen + window_end); includes gateway pace"}


class Handler(BaseHTTPRequestHandler):
    state: CloudState
    api_key: str = ""
    server_version = "BeatKeeperCloud/1.0"

    def log_message(self, fmt, *args):                      # quiet
        log.debug(fmt, *args)

    # -- helpers ----------------------------------------------------------------

    def _auth(self) -> bool:
        if not self.api_key:
            return True
        return hmac.compare_digest(self.headers.get("X-Api-Key", ""), self.api_key)

    def _json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _ndjson(self, lines):
        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson")
        self.end_headers()                                  # HTTP/1.0: body ends at close
        for ln in lines:
            self.wfile.write((ln + "\n").encode())

    # -- routes -----------------------------------------------------------------

    def do_GET(self):
        if self.path == "/health":
            return self._json({"status": "ok"})
        if not self._auth():
            return self._json({"error": "unauthorized"}, 401)
        st = self.state
        parts = self.path.strip("/").split("/")
        if len(parts) == 3 and parts[0] == "v1":
            kind, sid = parts[1], parts[2]
            if kind == "export":
                return self._ndjson(self._export_lines(sid))
            if kind == "results":
                return self._ndjson(self._result_lines(sid))
            if kind == "latency":
                return self._json(st.latency(sid))
        if self.path == "/v1/sessions":
            with st.lock:
                rows = st.db.execute("SELECT session_id, ended FROM sessions "
                                     "ORDER BY first_seen DESC").fetchall()
            return self._json([{"session_id": s, "ended": bool(e)} for s, e in rows])
        self._json({"error": "not found"}, 404)

    def do_POST(self):
        if not self._auth():
            return self._json({"error": "unauthorized"}, 401)
        try:
            n = int(self.headers.get("Content-Length", 0))
            if n > MAX_BODY:
                return self._json({"error": "too large"}, 413)
            body = self.rfile.read(n)
            if self.headers.get("Content-Encoding") == "gzip":
                body = gzip.decompress(body)
            doc = json.loads(body)
            st = self.state
            if self.path == "/v1/samples":
                st.ingest_samples([tuple(r) for r in doc["rows"]])
            elif self.path == "/v1/losses":
                st.ingest_losses([tuple(r) for r in doc["rows"]])
            elif self.path == "/v1/windows":
                st.ingest_windows([tuple(r) for r in doc["rows"]])
            elif self.path == "/v1/session_end":
                st.ingest_session_end(doc["sessions"])
            else:
                return self._json({"error": "not found"}, 404)
            self._json({"ok": True})                         # only after the commit
        except Exception as exc:                             # bad input must not kill the server
            log.exception("request failed")
            with self.state.lock:
                self.state.procs.clear()   # in-memory state may be ahead of the rolled-back DB
            self._json({"error": str(exc)}, 400)

    # -- streaming exports ------------------------------------------------------

    def _export_lines(self, sid: str):
        st = self.state
        with st.lock:
            cur = st.db.execute("SELECT sensor, idx, values_json FROM samples WHERE session_id=? "
                                "ORDER BY sensor, idx", (sid,))
            batch = cur.fetchmany(10000)
        while batch:
            for s, i, v in batch:
                yield '{"sensor": %d, "index": %d, "values": %s}' % (s, i, v)
            with st.lock:
                batch = cur.fetchmany(10000)
        with st.lock:
            losses = st.db.execute("SELECT sensor, from_index, to_index, reason FROM losses "
                                   "WHERE session_id=? ORDER BY sensor, from_index", (sid,)).fetchall()
        for s, a, b, r in losses:
            yield json.dumps({"sensor": s, "from_index": a, "to_index": b, "reason": r})

    def _result_lines(self, sid: str):
        with self.state.lock:
            rows = self.state.db.execute(
                "SELECT result_json, mode FROM windows WHERE session_id=? "
                "ORDER BY window_index, mode", (sid,)).fetchall()
        for rj, mode in rows:
            yield json.dumps(json.loads(rj) | {"mode": mode})


def make_server(db_path: str, port: int, api_key: str = "", host: str = "0.0.0.0"):
    Handler.state = CloudState(db_path)
    Handler.api_key = api_key
    return ThreadingHTTPServer((host, port), Handler)


def main() -> None:
    ap = argparse.ArgumentParser(description="BeatKeeper cloud service")
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8080)))
    ap.add_argument("--db", default=os.environ.get("BEATKEEPER_CLOUD_DB", "cloud.db"))
    ap.add_argument("--host", default="0.0.0.0")
    args = ap.parse_args()
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    srv = make_server(args.db, args.port, os.environ.get("BEATKEEPER_API_KEY", ""), args.host)
    log.info("cloud listening on %s:%d (db=%s, auth=%s)", args.host, args.port, args.db,
             "on" if Handler.api_key else "OFF")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
