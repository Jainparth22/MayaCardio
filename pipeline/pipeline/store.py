"""
store.py — durable local storage for the biosignal pipeline.

Design decisions:
- SQLite with WAL journal mode: writes are crash-safe (committed transactions
  survive power cuts after the WAL is fsynced).
- synchronous = FULL ensures the WAL is flushed to disk on every commit.
- Samples are stored as a flat table keyed by (session_id, sensor, index).
  The index is the sample number from the mock, so deduplication is trivial.
- Lost ranges are stored separately so they can be reported without ambiguity.
- Window results (1-second processed outputs) are stored for later retrieval.
"""

import json
import sqlite3
import threading
from pathlib import Path
from typing import Optional


_DB_FILE = Path(__file__).parent.parent / "data" / "pipeline.db"
_lock = threading.Lock()


def _get_connection(db_path: Path = _DB_FILE) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: Path = _DB_FILE) -> sqlite3.Connection:
    """Initialise (or open) the database and create tables if needed."""
    conn = _get_connection(db_path)
    with conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS samples (
                session_id  TEXT    NOT NULL,
                sensor      INTEGER NOT NULL,
                idx         INTEGER NOT NULL,
                values_json TEXT    NOT NULL,
                synced      INTEGER NOT NULL DEFAULT 0,   -- 0=not uploaded, 1=uploaded
                PRIMARY KEY (session_id, sensor, idx)
            );

            CREATE TABLE IF NOT EXISTS losses (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id  TEXT    NOT NULL,
                sensor      INTEGER NOT NULL,
                from_index  INTEGER NOT NULL,
                to_index    INTEGER,                       -- NULL = unknown end
                reason      TEXT    NOT NULL,
                synced      INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS windows (
                session_id      TEXT    NOT NULL,
                window_index    INTEGER NOT NULL,
                result_json     TEXT    NOT NULL,
                mode            TEXT    NOT NULL,          -- 'online' or 'offline'
                synced          INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (session_id, window_index)
            );

            CREATE TABLE IF NOT EXISTS sessions (
                session_id      TEXT PRIMARY KEY,
                started_at      REAL NOT NULL,
                last_seen_at    REAL NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_samples_unsynced
                ON samples(session_id, synced)
                WHERE synced = 0;

            CREATE INDEX IF NOT EXISTS idx_windows_unsynced
                ON windows(session_id, synced)
                WHERE synced = 0;

            CREATE INDEX IF NOT EXISTS idx_losses_unsynced
                ON losses(session_id, synced)
                WHERE synced = 0;
        """)
        try:
            conn.execute("ALTER TABLE losses ADD COLUMN synced INTEGER NOT NULL DEFAULT 0")
        except sqlite3.OperationalError:
            pass
    return conn


# ── session tracking ─────────────────────────────────────────────────────────

def upsert_session(conn: sqlite3.Connection, session_id: str, ts: float):
    with _lock, conn:
        conn.execute("""
            INSERT INTO sessions(session_id, started_at, last_seen_at)
            VALUES (?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET last_seen_at=excluded.last_seen_at
        """, (session_id, ts, ts))


# ── sample storage ────────────────────────────────────────────────────────────

def save_samples(conn: sqlite3.Connection, session_id: str, sensor: int,
                 first_index: int, data: list):
    """Persist a batch of samples. Duplicate inserts are silently ignored."""
    rows = [
        (session_id, sensor, first_index + i, json.dumps(row))
        for i, row in enumerate(data)
    ]
    with _lock, conn:
        conn.executemany("""
            INSERT OR IGNORE INTO samples(session_id, sensor, idx, values_json)
            VALUES (?, ?, ?, ?)
        """, rows)


def save_loss(conn: sqlite3.Connection, session_id: str, sensor: int,
              from_index: int, to_index: Optional[int], reason: str):
    with _lock, conn:
        conn.execute("""
            INSERT INTO losses(session_id, sensor, from_index, to_index, reason)
            VALUES (?, ?, ?, ?, ?)
        """, (session_id, sensor, from_index, to_index, reason))


# ── sync helpers ──────────────────────────────────────────────────────────────

def get_unsynced_samples(conn: sqlite3.Connection, session_id: str,
                         limit: int = 4000):
    """Return up to *limit* unsynced samples ordered by sensor, idx."""
    cur = conn.execute("""
        SELECT sensor, idx, values_json
        FROM samples
        WHERE session_id = ? AND synced = 0
        ORDER BY sensor, idx
        LIMIT ?
    """, (session_id, limit))
    return cur.fetchall()


def mark_samples_synced(conn: sqlite3.Connection, session_id: str,
                        keys: list):
    """Mark (sensor, idx) pairs as synced."""
    with _lock, conn:
        conn.executemany("""
            UPDATE samples SET synced=1
            WHERE session_id=? AND sensor=? AND idx=?
        """, [(session_id, s, i) for s, i in keys])


def get_unsynced_windows(conn: sqlite3.Connection, session_id: str):
    cur = conn.execute("""
        SELECT window_index, result_json, mode
        FROM windows
        WHERE session_id = ? AND synced = 0
        ORDER BY window_index
    """, (session_id,))
    return cur.fetchall()


def mark_windows_synced(conn: sqlite3.Connection, session_id: str,
                        window_indexes: list):
    with _lock, conn:
        conn.executemany("""
            UPDATE windows SET synced=1
            WHERE session_id=? AND window_index=?
        """, [(session_id, w) for w in window_indexes])


def get_unsynced_losses(conn: sqlite3.Connection, session_id: str):
    cur = conn.execute("""
        SELECT id, sensor, from_index, to_index, reason
        FROM losses
        WHERE session_id = ? AND synced = 0
        ORDER BY id
    """, (session_id,))
    return cur.fetchall()


def mark_losses_synced(conn: sqlite3.Connection, session_id: str,
                       loss_ids: list):
    with _lock, conn:
        conn.executemany("""
            UPDATE losses SET synced=1
            WHERE session_id=? AND id=?
        """, [(session_id, lid) for lid in loss_ids])


# ── window results ────────────────────────────────────────────────────────────

def save_window(conn: sqlite3.Connection, session_id: str, window_index: int,
                result: dict, mode: str):
    with _lock, conn:
        conn.execute("""
            INSERT OR REPLACE INTO windows(session_id, window_index, result_json, mode)
            VALUES (?, ?, ?, ?)
        """, (session_id, window_index, json.dumps(result), mode))


# ── status queries ────────────────────────────────────────────────────────────

def count_samples(conn: sqlite3.Connection, session_id: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM samples WHERE session_id=?", (session_id,)
    ).fetchone()
    return row[0] if row else 0


def count_synced_samples(conn: sqlite3.Connection, session_id: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM samples WHERE session_id=? AND synced=1",
        (session_id,)
    ).fetchone()
    return row[0] if row else 0


def count_unsynced_samples(conn: sqlite3.Connection, session_id: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM samples WHERE session_id=? AND synced=0",
        (session_id,)
    ).fetchone()
    return row[0] if row else 0


# ── export helpers ────────────────────────────────────────────────────────────

def get_all_samples(conn: sqlite3.Connection, session_id: str):
    cur = conn.execute("""
        SELECT sensor, idx, values_json
        FROM samples WHERE session_id=?
        ORDER BY sensor, idx
    """, (session_id,))
    return cur.fetchall()


def get_all_losses(conn: sqlite3.Connection, session_id: str):
    cur = conn.execute("""
        SELECT sensor, from_index, to_index, reason
        FROM losses WHERE session_id=?
        ORDER BY sensor, from_index
    """, (session_id,))
    return cur.fetchall()


def get_all_windows(conn: sqlite3.Connection, session_id: str):
    cur = conn.execute("""
        SELECT window_index, result_json, mode
        FROM windows WHERE session_id=?
        ORDER BY window_index
    """, (session_id,))
    return cur.fetchall()


def list_sessions(conn: sqlite3.Connection):
    cur = conn.execute("SELECT session_id, started_at, last_seen_at FROM sessions ORDER BY started_at DESC")
    return cur.fetchall()
