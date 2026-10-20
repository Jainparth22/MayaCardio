"""
test_store.py — tests for the local SQLite store.

Verifies:
- Sample insert and idempotency (INSERT OR IGNORE).
- Loss recording.
- Sync flag lifecycle.
- Window storage and retrieval.
- Session upsert.
"""

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))

import store as S


def _fresh_db():
    tmp = tempfile.mktemp(suffix=".db")
    return S.init_db(Path(tmp))


# ── samples ───────────────────────────────────────────────────────────────────

def test_save_samples_basic():
    conn = _fresh_db()
    S.save_samples(conn, "sess1", 1, 0, [[1, 2, 3], [4, 5, 6]])
    assert S.count_samples(conn, "sess1") == 2


def test_save_samples_idempotent():
    conn = _fresh_db()
    S.save_samples(conn, "sess1", 1, 0, [[1, 2, 3]])
    S.save_samples(conn, "sess1", 1, 0, [[1, 2, 3]])   # duplicate
    assert S.count_samples(conn, "sess1") == 1


def test_save_samples_two_sensors():
    conn = _fresh_db()
    S.save_samples(conn, "s", 1, 0, [[1, 2, 3]])
    S.save_samples(conn, "s", 2, 0, [[4, 5]])
    assert S.count_samples(conn, "s") == 2


# ── losses ────────────────────────────────────────────────────────────────────

def test_save_and_retrieve_loss():
    conn = _fresh_db()
    S.save_loss(conn, "s", 1, 100, 199, "buffer_overflow")
    losses = S.get_all_losses(conn, "s")
    assert len(losses) == 1
    sensor, fi, ti, reason = losses[0]
    assert sensor == 1
    assert fi == 100
    assert ti == 199
    assert reason == "buffer_overflow"


def test_save_loss_null_to_index():
    conn = _fresh_db()
    S.save_loss(conn, "s", 2, 500, None, "mock_restart")
    losses = S.get_all_losses(conn, "s")
    assert losses[0][2] is None


# ── sync flags ────────────────────────────────────────────────────────────────

def test_sync_lifecycle():
    conn = _fresh_db()
    S.save_samples(conn, "s", 1, 0, [[1, 2, 3], [4, 5, 6]])
    assert S.count_unsynced_samples(conn, "s") == 2
    rows = S.get_unsynced_samples(conn, "s")
    keys = [(r[0], r[1]) for r in rows]
    S.mark_samples_synced(conn, "s", keys)
    assert S.count_synced_samples(conn, "s") == 2
    assert S.count_unsynced_samples(conn, "s") == 0


# ── windows ───────────────────────────────────────────────────────────────────

def test_save_and_retrieve_window():
    conn = _fresh_db()
    result = {"window": 0, "heart_rate": 60.0, "mode": "online"}
    S.save_window(conn, "s", 0, result, "online")
    windows = S.get_all_windows(conn, "s")
    assert len(windows) == 1
    wi, rj, mode = windows[0]
    assert wi == 0
    assert json.loads(rj)["heart_rate"] == 60.0
    assert mode == "online"


def test_window_upsert():
    conn = _fresh_db()
    S.save_window(conn, "s", 0, {"heart_rate": 60.0}, "online")
    S.save_window(conn, "s", 0, {"heart_rate": 72.0}, "offline")  # overwrite
    windows = S.get_all_windows(conn, "s")
    assert len(windows) == 1
    assert json.loads(windows[0][1])["heart_rate"] == 72.0


def test_window_sync_lifecycle():
    conn = _fresh_db()
    S.save_window(conn, "s", 0, {}, "online")
    S.save_window(conn, "s", 1, {}, "online")
    rows = S.get_unsynced_windows(conn, "s")
    assert len(rows) == 2
    S.mark_windows_synced(conn, "s", [0, 1])
    assert len(S.get_unsynced_windows(conn, "s")) == 0


# ── sessions ──────────────────────────────────────────────────────────────────

def test_upsert_session():
    conn = _fresh_db()
    S.upsert_session(conn, "abc123", 1000.0)
    S.upsert_session(conn, "abc123", 2000.0)   # update last_seen_at
    sessions = S.list_sessions(conn)
    assert len(sessions) == 1
    assert sessions[0][2] == 2000.0   # last_seen_at updated


if __name__ == "__main__":
    import traceback
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"  FAIL  {t.__name__}: {e}")
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)
