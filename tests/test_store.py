import json

from beatkeeper.store import Store


def test_samples_are_idempotent(tmp_path):
    s = Store(tmp_path / "a.db")
    rows = [[1, 2, 3], [4, 5, 6]]
    s.save_samples("S", 1, 100, rows)
    s.save_samples("S", 1, 100, rows)                       # replay
    s.save_samples("S", 1, 101, [[4, 5, 6], [7, 8, 9]])     # overlapping replay
    assert [r[1] for r in s.iter_samples("S")] == [100, 101, 102]
    assert s.counts("S")["samples_saved"] == 3


def test_values_unchanged_roundtrip(tmp_path):
    s = Store(tmp_path / "a.db")
    s.save_samples("S", 2, 0, [[-8388608, 8388607]])
    (_, _, vj), = list(s.iter_samples("S"))
    assert json.loads(vj) == [-8388608, 8388607]


def test_data_survives_reopen_without_graceful_shutdown(tmp_path):
    p = tmp_path / "a.db"
    s = Store(p)
    s.save_samples("S", 1, 0, [[1, 2, 3]] * 50)
    s._conn.close()
    s2 = Store(p)
    assert s2.counts("S")["samples_saved"] == 50
    assert s2.max_index("S", 1) == 49


def test_losses_are_idempotent_including_open_ended(tmp_path):
    s = Store(tmp_path / "a.db")
    for _ in range(3):
        s.save_loss("S", 1, 10, 20, "buffer_overflow")
        s.save_loss("S", 2, 50, None, "gateway_restart")    # NULL to_index
    assert len(s.losses("S")) == 2


def test_sync_flags_and_backlog_span_all_sessions(tmp_path):
    s = Store(tmp_path / "a.db")
    s.save_samples("S", 1, 0, [[1, 2, 3]] * 10)
    s.save_samples("T", 2, 0, [[1, 2]] * 5)
    rows = s.unsynced_samples(100)
    assert len(rows) == 15 and {r[1] for r in rows} == {"S", "T"}
    s.mark_samples_synced([r[0] for r in rows[:10]])
    assert s.counts(None)["backlog"] == 5
    assert len(s.unsynced_samples(100)) == 5


def test_window_row_per_mode(tmp_path):
    s = Store(tmp_path / "a.db")
    s.save_window("S", 3, "offline", {"window": 3}, synced=False)
    s.save_window("S", 3, "online", {"window": 3}, synced=True)
    assert [w["mode"] for w in s.windows("S")] == ["offline", "online"]
    assert len(s.unsynced_windows(10)) == 1
