"""
store.py - durable local storage (the "never lose a heartbeat" part).

* SQLite in WAL mode with synchronous=FULL: a committed transaction has been
  fsync'd, so it survives process kills, reboots and power cuts.
* Samples are keyed by (session_id, sensor, idx): re-inserting is a no-op, so
  every "retry" elsewhere in the system is safe (idempotent writes).
* Losses are first-class data with a unique key, so a re-recorded loss never
  produces a duplicate row.
* Window results are keyed by (session, window, mode): one row per window and
  mode, as the product requires.
* `synced` flags + rowid-based bulk marking implement at-least-once upload.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence, Tuple

SCHEMA = """
CREATE TABLE IF NOT EXISTS samples (
    session_id  TEXT    NOT NULL,
    sensor      INTEGER NOT NULL,
    idx         INTEGER NOT NULL,
    values_json TEXT    NOT NULL,
    synced      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (session_id, sensor, idx)
);
CREATE INDEX IF NOT EXISTS ix_samples_unsynced ON samples(synced) WHERE synced = 0;

CREATE TABLE IF NOT EXISTS losses (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id  TEXT    NOT NULL,
    sensor      INTEGER NOT NULL,
    from_index  INTEGER NOT NULL,
    to_index    INTEGER,
    reason      TEXT    NOT NULL,
    synced      INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_losses
    ON losses(session_id, sensor, from_index, IFNULL(to_index, -1), reason);

CREATE TABLE IF NOT EXISTS windows (
    session_id   TEXT    NOT NULL,
    window_index INTEGER NOT NULL,
    mode         TEXT    NOT NULL,
    result_json  TEXT    NOT NULL,
    synced       INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (session_id, window_index, mode)
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id   TEXT PRIMARY KEY,
    started_at   REAL NOT NULL,
    last_seen_at REAL NOT NULL,
    ended        INTEGER NOT NULL DEFAULT 0,
    ended_synced INTEGER NOT NULL DEFAULT 0
);
"""


class Store:
    def __init__(self, path: os.PathLike | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        with self._conn:
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- sessions ---------------------------------------------------------------

    def touch_session(self, session_id: str, now: float) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO sessions(session_id, started_at, last_seen_at) VALUES (?,?,?) "
                "ON CONFLICT(session_id) DO UPDATE SET last_seen_at=excluded.last_seen_at",
                (session_id, now, now))

    def end_session(self, session_id: str) -> None:
        with self._lock, self._conn:
            self._conn.execute("UPDATE sessions SET ended=1 WHERE session_id=?", (session_id,))

    def latest_session(self) -> Optional[str]:
        with self._lock:
            row = self._conn.execute(
                # insertion order, NOT wall-clock: a system-clock change must never
                # make an older session look "latest"
                "SELECT session_id FROM sessions ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
        return row[0] if row else None

    def session_ended(self, session_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT ended FROM sessions WHERE session_id=?", (session_id,)).fetchone()
        return bool(row and row[0])

    def unsynced_ended_sessions(self) -> List[str]:
        with self._lock:
            return [r[0] for r in self._conn.execute(
                "SELECT session_id FROM sessions WHERE ended=1 AND ended_synced=0")]

    def mark_session_end_synced(self, session_ids: Iterable[str]) -> None:
        with self._lock, self._conn:
            self._conn.executemany(
                "UPDATE sessions SET ended_synced=1 WHERE session_id=?",
                [(s,) for s in session_ids])

    # -- samples ----------------------------------------------------------------

    def save_samples(self, session_id: str, sensor: int, first_index: int,
                     rows: Sequence[Sequence[int]]) -> None:
        """One atomic, fsync'd transaction. Raises on disk errors (caller must
        NOT advance its read cursor in that case)."""
        data = [(session_id, sensor, first_index + k,
                 json.dumps(r, separators=(",", ":"))) for k, r in enumerate(rows)]
        with self._lock, self._conn:
            self._conn.executemany(
                "INSERT OR IGNORE INTO samples(session_id,sensor,idx,values_json) "
                "VALUES (?,?,?,?)", data)

    def max_index(self, session_id: str, sensor: int) -> Optional[int]:
        with self._lock:
            row = self._conn.execute(
                "SELECT MAX(idx) FROM samples WHERE session_id=? AND sensor=?",
                (session_id, sensor)).fetchone()
        return row[0]

    def read_samples(self, session_id: str, sensor: int, lo: int, hi: int
                     ) -> List[Tuple[int, List[int]]]:
        """Samples with lo <= idx < hi, ordered."""
        with self._lock:
            cur = self._conn.execute(
                "SELECT idx, values_json FROM samples "
                "WHERE session_id=? AND sensor=? AND idx>=? AND idx<? ORDER BY idx",
                (session_id, sensor, lo, hi))
            return [(i, json.loads(v)) for i, v in cur]

    def unsynced_samples(self, limit: int) -> List[Tuple[int, str, int, int, str]]:
        """(rowid, session, sensor, idx, values_json), oldest first."""
        with self._lock:
            return self._conn.execute(
                "SELECT rowid, session_id, sensor, idx, values_json FROM samples "
                "WHERE synced=0 ORDER BY rowid LIMIT ?", (limit,)).fetchall()

    def mark_samples_synced(self, rowids: Sequence[int]) -> None:
        with self._lock, self._conn:
            self._conn.executemany("UPDATE samples SET synced=1 WHERE rowid=?",
                                   [(r,) for r in rowids])

    # -- losses -----------------------------------------------------------------

    def save_loss(self, session_id: str, sensor: int, from_index: int,
                  to_index: Optional[int], reason: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO losses(session_id,sensor,from_index,to_index,reason) "
                "VALUES (?,?,?,?,?)", (session_id, sensor, from_index, to_index, reason))

    def losses(self, session_id: str) -> List[Tuple[int, int, Optional[int], str]]:
        with self._lock:
            return self._conn.execute(
                "SELECT sensor, from_index, to_index, reason FROM losses "
                "WHERE session_id=? ORDER BY sensor, from_index", (session_id,)).fetchall()

    def unsynced_losses(self) -> List[Tuple[int, str, int, int, Optional[int], str]]:
        with self._lock:
            return self._conn.execute(
                "SELECT id, session_id, sensor, from_index, to_index, reason "
                "FROM losses WHERE synced=0 ORDER BY id").fetchall()

    def mark_losses_synced(self, ids: Sequence[int]) -> None:
        with self._lock, self._conn:
            self._conn.executemany("UPDATE losses SET synced=1 WHERE id=?", [(i,) for i in ids])

    # -- windows ----------------------------------------------------------------

    def save_window(self, session_id: str, window_index: int, mode: str,
                    result: dict, synced: bool) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO windows(session_id,window_index,mode,result_json,synced) "
                "VALUES (?,?,?,?,?)",
                (session_id, window_index, mode, json.dumps(result), int(synced)))

    def last_window(self, session_id: str) -> Optional[int]:
        with self._lock:
            row = self._conn.execute(
                "SELECT MAX(window_index) FROM windows WHERE session_id=?",
                (session_id,)).fetchone()
        return row[0]

    def windows(self, session_id: str) -> List[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT result_json, mode FROM windows WHERE session_id=? "
                "ORDER BY window_index, mode", (session_id,)).fetchall()
        return [json.loads(r) | {"mode": m} for r, m in rows]

    def unsynced_windows(self, limit: int) -> List[Tuple[str, int, str, str]]:
        with self._lock:
            return self._conn.execute(
                "SELECT session_id, window_index, mode, result_json FROM windows "
                "WHERE synced=0 ORDER BY session_id, window_index LIMIT ?", (limit,)).fetchall()

    def mark_windows_synced(self, keys: Sequence[Tuple[str, int, str]]) -> None:
        with self._lock, self._conn:
            self._conn.executemany(
                "UPDATE windows SET synced=1 WHERE session_id=? AND window_index=? AND mode=?",
                keys)

    # -- counts (status) --------------------------------------------------------

    def counts(self, session_id: Optional[str]) -> dict:
        with self._lock:
            q = self._conn.execute
            total = q("SELECT COUNT(*) FROM samples" + (" WHERE session_id=?" if session_id else ""),
                      (session_id,) if session_id else ()).fetchone()[0]
            backlog_all = q("SELECT COUNT(*) FROM samples WHERE synced=0").fetchone()[0]
            if session_id:
                backlog = q("SELECT COUNT(*) FROM samples WHERE session_id=? AND synced=0",
                            (session_id,)).fetchone()[0]
            else:
                backlog = backlog_all
        return {"samples_saved": total, "samples_in_cloud": total - backlog,
                "backlog": backlog, "backlog_all_sessions": backlog_all}

    # -- export -----------------------------------------------------------------

    def iter_samples(self, session_id: str) -> Iterator[Tuple[int, int, str]]:
        """Stream samples (sensor, idx, values_json) without loading them all."""
        with self._lock:
            cur = self._conn.execute(
                "SELECT sensor, idx, values_json FROM samples WHERE session_id=? "
                "ORDER BY sensor, idx", (session_id,))
            rows = cur.fetchmany(10000)
        while rows:
            yield from rows
            with self._lock:
                rows = cur.fetchmany(10000)
