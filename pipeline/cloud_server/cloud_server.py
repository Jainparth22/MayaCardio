"""
cloud_server.py — tiny cloud-side Flask server.

Receives samples, windows and losses from the device pipeline,
stores them in a local SQLite database, and exposes query endpoints.

Run on any free-tier cloud VM (e.g., Oracle Always Free, AWS t2.micro):
  pip install flask
  python cloud_server.py

Set CLOUD_HTTP_URL=http://<your-server-ip>:8080 on the device before
starting the pipeline, and set CLOUD_BACKEND=http.

All writes are idempotent (INSERT OR IGNORE / INSERT OR REPLACE).
"""

import json
import os
import sqlite3
import threading
from pathlib import Path

try:
    from flask import Flask, request, jsonify
except ImportError:
    raise SystemExit("Flask is required: pip install flask")

app = Flask(__name__)

DB_FILE = Path(os.environ.get("CLOUD_DB", "cloud_data.db"))
_lock = threading.Lock()


# ── database ──────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(str(DB_FILE), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db():
    conn = get_db()
    with conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS samples (
                session_id  TEXT    NOT NULL,
                sensor      INTEGER NOT NULL,
                idx         INTEGER NOT NULL,
                values_json TEXT    NOT NULL,
                PRIMARY KEY (session_id, sensor, idx)
            );
            CREATE TABLE IF NOT EXISTS windows (
                session_id   TEXT    NOT NULL,
                window_index INTEGER NOT NULL,
                result_json  TEXT    NOT NULL,
                mode         TEXT    NOT NULL,
                PRIMARY KEY (session_id, window_index)
            );
            CREATE TABLE IF NOT EXISTS losses (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id  TEXT    NOT NULL,
                sensor      INTEGER NOT NULL,
                from_index  INTEGER NOT NULL,
                to_index    INTEGER,
                reason      TEXT    NOT NULL
            );
        """)
    return conn


_conn = init_db()


# ── endpoints ─────────────────────────────────────────────────────────────────

@app.post("/samples")
def recv_samples():
    body = request.get_json(force=True)
    session_id = body["session_id"]
    rows = body.get("rows", [])
    with _lock, _conn:
        _conn.executemany("""
            INSERT OR IGNORE INTO samples(session_id, sensor, idx, values_json)
            VALUES (?, ?, ?, ?)
        """, [(session_id, r["sensor"], r["idx"], r["values_json"])
              for r in rows])
    return jsonify({"ok": True, "count": len(rows)})


@app.post("/windows")
def recv_windows():
    body = request.get_json(force=True)
    session_id = body["session_id"]
    rows = body.get("rows", [])
    with _lock, _conn:
        _conn.executemany("""
            INSERT OR REPLACE INTO windows(session_id, window_index, result_json, mode)
            VALUES (?, ?, ?, ?)
        """, [(session_id, r["window_index"], r["result_json"], r["mode"])
              for r in rows])
    return jsonify({"ok": True, "count": len(rows)})


@app.post("/losses")
def recv_losses():
    body = request.get_json(force=True)
    session_id = body["session_id"]
    rows = body.get("rows", [])
    with _lock, _conn:
        _conn.executemany("""
            INSERT INTO losses(session_id, sensor, from_index, to_index, reason)
            VALUES (?, ?, ?, ?, ?)
        """, [(session_id, r["sensor"], r["from_index"],
               r.get("to_index"), r["reason"])
              for r in rows])
    return jsonify({"ok": True, "count": len(rows)})


@app.get("/sample_count/<session_id>")
def sample_count(session_id):
    row = _conn.execute(
        "SELECT COUNT(*) FROM samples WHERE session_id=?", (session_id,)
    ).fetchone()
    return jsonify({"count": row[0] if row else 0})


@app.get("/windows/<session_id>")
def get_windows(session_id):
    rows = _conn.execute("""
        SELECT window_index, result_json, mode
        FROM windows WHERE session_id=?
        ORDER BY window_index
    """, (session_id,)).fetchall()
    results = []
    for wi, rj, mode in rows:
        r = json.loads(rj)
        r["mode"] = mode
        results.append(r)
    return jsonify(results)


@app.get("/export/<session_id>")
def export_session(session_id):
    """Return all samples, losses and windows for a session (for judging)."""
    samples = _conn.execute("""
        SELECT sensor, idx, values_json FROM samples
        WHERE session_id=? ORDER BY sensor, idx
    """, (session_id,)).fetchall()
    losses = _conn.execute("""
        SELECT sensor, from_index, to_index, reason FROM losses
        WHERE session_id=? ORDER BY sensor, from_index
    """, (session_id,)).fetchall()
    windows = _conn.execute("""
        SELECT window_index, result_json, mode FROM windows
        WHERE session_id=? ORDER BY window_index
    """, (session_id,)).fetchall()
    return jsonify({
        "samples": [{"sensor": s, "index": i, "values": json.loads(vj)}
                    for s, i, vj in samples],
        "losses":  [{"sensor": s, "from_index": fi,
                     "to_index": ti, "reason": r}
                    for s, fi, ti, r in losses],
        "windows": [json.loads(rj) | {"mode": m}
                    for _, rj, m in windows],
    })


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
