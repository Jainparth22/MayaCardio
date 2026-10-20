# Interview Prep — Part 1: System Architecture & Design Decisions

> Every answer here is grounded directly in the code you built.
> Expect these questions in the 60-min virtual interview.

---

## 1. "Walk me through the architecture of your pipeline from scratch."

**Answer:**

The pipeline is a single Python process with **6 cooperating threads** sharing one SQLite database. Here's the full picture:

```
biosignal-mock (TCP :7100)
       │
       ├── reader-s1 (Thread) ──────────────────────────────┐
       └── reader-s2 (Thread) ──────────────────────────────┤
                                                            ▼
                                                    Engine._on_data()
                                                            │
                                          ┌─────────────────┼─────────────────┐
                                          ▼                 ▼                 ▼
                                   store.save_samples   in-memory       _try_process_
                                   (SQLite WAL, fsync)  rolling buf     windows()
                                                                             │
                                                                    processor.process_window()
                                                                             │
                                                                    store.save_window()
                                                                             │
                                                              ┌──────────────┘
                                                              ▼
                                                      Syncer (Thread)
                                               (uploads samples, windows, losses)
                                                              │
                                                      CloudBackend
                                                  (HTTP / Supabase / noop)

NetworkMonitor (Thread)  ──probes 8.8.8.8:53──► on_mode_change() ──► Syncer.set_mode()
                                                                  └──► pipeline.mode (IPC for CLI)
```

**The invariant that never breaks:**
> Data is written to disk **before** it enters any in-memory buffer or is processed.
> Processing is an optimization. Disk is the ground truth.

**Thread roster:**
| Thread | Name | Job |
|---|---|---|
| main | (none) | Starts everything, handles signals |
| reader-s1 | `reader.py` | Reads sensor 1 from mock TCP |
| reader-s2 | `reader.py` | Reads sensor 2 from mock TCP |
| net-monitor | `network.py` | Probes 8.8.8.8:53 every 2s, triggers online/offline mode switch |
| syncer | `syncer.py` | Uploads unsynced samples, windows, and loss records to cloud |
| (engine callbacks) | inline | Window processing triggered by reader callbacks |

---

## 2. "Why Python? The JD also lists C/C++ and Java."

**Answer:**

I chose Python for these specific reasons:

1. **Stdlib-only on the device** — no `pip install` on a fresh Pi. SQLite (`sqlite3`), TCP sockets (`socket`), CRC-32 (`zlib`), threading (`threading`) — all stdlib. Zero dependency issues.
2. **Raspberry Pi-class resource constraint** — the bottleneck is I/O (network + disk), not CPU. At 400 Hz × 2 sensors = 800 samples/s, Python handles it comfortably within the 4-core/2 GB limit.
3. **Rapid correctness** — the spec has precise processing rules (heart rate formula, CRC-32 validation, session change detection). Python's readability made it easy to verify the implementation matches the spec line-by-line.
4. **GIL doesn't hurt here** — our threads are all I/O-bound (TCP reads, SQLite writes, HTTP calls). The GIL releases on every blocking I/O call, so threads run truly concurrently for our workload.

**When would I choose C instead?**
If we had to process at 40,000 Hz (100× faster), or if the Pi had only 256 MB RAM. At that point, Python's JSON parsing overhead would matter. But at 400 Hz, a Python dict lookup takes < 1 µs, so it's irrelevant.

---

## 3. "Why one process instead of multiple microservices?"

**Answer:**

On a Raspberry Pi-class device, process isolation creates more problems than it solves:

- **IPC cost**: Crossing process boundaries (pipes, sockets) adds latency and failure modes. A crash in one process doesn't guarantee the other is in a consistent state.
- **Shared SQLite**: SQLite supports one writer at a time. Multiple processes would need IPC or a separate DB daemon (like PostgreSQL), which is overkill for this resource class.
- **Simpler failure model**: When the single process dies, the state is only in the WAL-journaled SQLite file. Recovery on restart is: open the DB, read the last session, resume from the highest saved index.

**The tradeoff I accepted**: A bug in any thread can crash the whole process. I mitigated this with:
1. Daemon threads (auto-killed when main exits cleanly via SIGTERM)
2. Per-reader exception handling that retries, not crashes
3. WAL + FULL sync so even an unexpected crash (SIGKILL) leaves a consistent DB

---

## 4. "How does data flow from the mock service to the cloud, step by step?"

**Full data flow — every hop:**

```
Step 1: Mock → SensorReader (reader.py)
  - TCP connection to 127.0.0.1:7100
  - Send: {"cmd": "read", "sensor": 1}
  - Receive: {"session_id":…, "first_index":…, "count":…, "data":[[…],…], "crc32":…}
  - Validate CRC-32 (zlib) of the data field text
  - If corrupt: do NOT advance next_index, reconnect, retry

Step 2: SensorReader → Engine (callback)
  - on_data(session_id, sensor=1, first_index=0, data=[[720,1200,1560],…])
  - Called inline in the reader thread

Step 3: Engine → SQLite (store.py)
  - INSERT OR IGNORE INTO samples (session_id, sensor, idx, values_json)
  - WAL fsynced before function returns
  - Safety invariant satisfied

Step 4: Engine → in-memory buffer (engine.py)
  - self._s1[first_index + i] = row  (for each row in data)
  - Evict samples older than last 3000 to bound memory

Step 5: Engine → Processor → SQLite (engine.py + processor.py)
  - _try_process_windows() checks: is max index in buffer ≥ window_end?
  - If yes: take snapshot of both sensor buffers
  - processor.process_window(w, s1_snap, s2_snap, ch2_history, mode)
  - store.save_window(conn, session_id, w, result, mode)

Step 6: Syncer → Cloud (syncer.py + cloud.py)
  - Every 1 second (UPLOAD_INTERVAL): query unsynced samples + windows
  - HTTP POST to cloud backend (batch of ≤ 2000 samples)
  - On HTTP 200: UPDATE samples SET synced=1
  - If HTTP fails: leave synced=0, retry next cycle
```

---

## 5. "What happens at each layer when the power is cut?"

**Layer-by-layer analysis:**

| Layer | What happens | Recovery |
|---|---|---|
| In-memory buffers (self._s1, self._s2) | Lost entirely | Re-read from DB on restart |
| Unsynced sample in current SQLite transaction | Lost if not yet fsynced (< 1s worth) | Listed as gap when compared to mock ground truth |
| Committed SQLite samples | Survive (WAL + FULL sync = fsync before commit returns) | Resume from last saved idx |
| Window results not yet computed | Recomputed from saved samples on restart | No loss |
| Samples in Syncer's current batch (uploaded but not yet marked synced) | Re-uploaded on next cycle (at-least-once delivery) | Cloud does INSERT OR IGNORE — no duplicate |
| Samples already marked synced | In cloud already | No action needed |

**Maximum data loss = samples in the current uncommitted write batch**
= at most 1 read-interval worth of data = ≤ 400 samples (1 second) per sensor.

---

## 6. "Why SQLite? Why not a file-per-sample or append-only log?"

**Comparison:**

| Option | Pros | Cons |
|---|---|---|
| **SQLite (chosen)** | Atomic transactions, indexed lookups, dedup by PK, partial sync tracking (synced flag), SQL queries for export | Slightly more overhead than raw files |
| Append-only log (e.g., one line per sample) | Fastest sequential writes | No dedup, no easy random access, hard to mark sync state, no index |
| One file per sample | Trivially crash-safe if using rename() | 400 files/sec × 3600s = 1.44M files — filesystem namespace explosion, very slow directory reads |
| RocksDB / LevelDB | Fast KV store, crash-safe | Not in stdlib, adds a dependency, overkill for 400 Hz |

**Why SQLite wins here:**
- The WAL journal mode with `synchronous=FULL` gives exactly the same durability as `fsync()` after every write.
- `INSERT OR IGNORE` with a composite primary key (session_id, sensor, idx) gives **free deduplication** — if a re-read delivers the same samples, they're silently ignored.
- The `synced` flag column turns the samples table into a **queue** with O(1) unsynced-count queries (backed by a partial index).

---

## 7. "What is WAL mode? How does it make writes crash-safe?"

**Explanation (go deep):**

SQLite has three journal modes. Default is DELETE (rollback journal).

**WAL (Write-Ahead Log):**
1. When you `BEGIN; INSERT; COMMIT`, SQLite writes the new pages to a *separate* `.db-wal` file, not the main DB file.
2. On commit, it fsyncs the WAL file.
3. Readers can still read the main DB concurrently (from the old checkpoint).
4. A background checkpointer eventually copies WAL → main DB.

**Why this is crash-safe:**
- The WAL fsync happens *before* COMMIT returns.
- If power is cut after the WAL fsync but before the checkpoint: on restart, SQLite replays the WAL → data is not lost.
- If power is cut during the WAL write (mid-fsync): the partial WAL page is discarded (CRC check fails) → transaction is as if it never happened. No corruption.

**`synchronous=FULL` role:**
- Without it, SQLite may use the OS page cache and defer fsync. With `FULL`, every commit flushes the WAL to physical disk.
- Without FULL, a power cut could lose up to the OS's write-back window (potentially seconds).
- With FULL, loss is bounded to the time between the last fsync and the cut (< 1 ms for a single INSERT).

---

## 8. "What are the threading locks for? Could you have a race condition?"

**Locks used and why:**

| Lock | Protects | Risk if absent |
|---|---|---|
| `_lock` in store.py | SQLite connection object | `sqlite3.Connection` is not thread-safe by default (`check_same_thread=False` enables sharing but you must serialize access) |
| `_session_lock` in engine.py | `self._session_id` | Reader-s1 and reader-s2 both call `_on_data` concurrently; both could try to update session_id at the same time |
| `_s1_lock`, `_s2_lock` | In-memory sample dicts | Reader threads write; engine reads for window processing; classic TOCTOU race |
| `_window_lock` | `self._next_window` counter | Both readers trigger `_try_process_windows()`; could double-process window w |
| `_mode_lock` | `self._mode` string | NetworkMonitor writes; Syncer and processor read |

**Potential race that's guarded:**
```
Thread A (reader-s1): calls _try_process_windows(w=5)
Thread B (reader-s2): calls _try_process_windows(w=5) concurrently

Without _window_lock:
  Both read self._next_window = 5
  Both process window 5 (duplicate computation)
  Both call save_window(w=5, ...) — INSERT OR REPLACE means the second
  write silently overwrites the first. Not a correctness bug, but wasteful.

With _window_lock:
  A reads and increments to 6 while holding the lock.
  B then reads 6, skips window 5 entirely.
```

---

## 9. "How does the system guarantee no duplicate samples in the cloud?"

**Multi-layer guarantee:**

1. **Device DB**: `INSERT OR IGNORE` with PK `(session_id, sensor, idx)`. A sample received twice from the mock (fault injection: mock sends same reply twice) is stored exactly once.

2. **Sync flag**: `synced=1` means "cloud has ACKed this". On re-upload after crash, the same batch is POSTed again. Cloud uses `INSERT OR IGNORE` → no duplicate row.

3. **Cloud server**: `INSERT OR IGNORE INTO samples (session_id, sensor, idx, …)` — same PK constraint.

4. **At-least-once delivery is safe** because of idempotent inserts. We deliberately chose at-least-once (mark synced only *after* ACK) over at-most-once (mark synced before upload — would lose data on crash) because idempotency at the cloud level makes it safe.

---

## 10. "If you had to redesign this for 100,000 samples/sec, what would you change?"

**Answer (shows you can reason about tradeoffs):**

At 100K samples/sec = 100× our current load:

| Problem | Current solution | New solution |
|---|---|---|
| SQLite write throughput | FULL sync = 1 fsync/commit | Write to an append-only binary log, batch-commit to SQLite every 50 ms |
| Python JSON parsing | re-encode data for CRC check | Use `orjson` (10× faster) or binary protocol |
| Single-threaded SQLite writer | `_lock` serializes all writes | WAL already allows one writer; use connection pool with write queue |
| In-memory dict (3000 samples) | Fine at 800 Hz | Circular buffer (numpy array) for O(1) insert/evict at 100K Hz |
| CRC validation | Per-reply, Python `zlib` | Offload to Rust extension or use hardware CRC |
| Cloud upload | HTTP REST, 2000 samples/batch | WebSocket streaming or gRPC for persistent connection |
