# Interview Prep — Part 4: Tradeoffs & "Why Not X?" Questions

> These are the hardest interview questions. For every choice, know
> what you gave up and why that was acceptable.

---

## Tradeoff 1: Threading vs asyncio

**What we chose:** `threading.Thread` for each component.

**Why not asyncio?**

| Aspect | threading | asyncio |
|---|---|---|
| SQLite writes | Blocking — threads handle it naturally | Need `asyncio.to_thread()` wrapper for every DB call |
| TCP reads | Blocking `socket.recv()` — fine in threads | Need `asyncio.StreamReader` / `asyncio.open_connection()` |
| Learning curve | Every Python developer knows threading | asyncio requires understanding event loops, coroutines, `await` |
| Debugging | Thread dumps, `threading.enumerate()` | Harder — need `asyncio.all_tasks()` |
| Performance at 400 Hz | GIL releases on every I/O call — threads effectively parallel | asyncio is genuinely single-threaded (cooperative), same performance |
| Deadlock risk | Yes — need to avoid lock ordering bugs | Lower — no shared state by default |

**Verdict:** For I/O-bound workloads at this scale, threading is simpler, more debuggable, and performs identically to asyncio. asyncio would shine at 10,000+ concurrent connections; we have 2.

**Admission:** If I had a UI or many more concurrent sensors, asyncio would be worth the complexity.

---

## Tradeoff 2: WAL vs Other SQLite Journal Modes

**Options:**

| Mode | Write speed | Durability | Concurrent reads |
|---|---|---|---|
| **DELETE** (default) | Medium | Good (with FULL sync) | ❌ Readers blocked during writes |
| **WAL** (chosen) | Faster (no main DB rewrite on every commit) | Same as DELETE with FULL | ✅ Readers never blocked |
| **MEMORY** | Very fast (no disk) | ❌ Lost on any crash | ✅ |
| **OFF** | Fastest | ❌ Corruption on crash | ✅ |

**Why WAL + FULL over DELETE + FULL?**
- WAL allows simultaneous readers (status queries) while the writer (reader thread) is inserting. In DELETE mode, a `COUNT(*)` query would block on the write lock.
- WAL has slightly lower commit overhead because it appends to the WAL file rather than modifying the main DB pages in place.

**Why not MEMORY journal?**
Obvious: no durability. A power cut loses everything. Exactly what we're trying to prevent.

---

## Tradeoff 3: synchronous=FULL vs NORMAL

| Setting | Behaviour | Risk |
|---|---|---|
| **OFF** | Never fsyncs. Uses OS cache. | Power cut → full DB corruption possible |
| **NORMAL** | Fsyncs on checkpoint, not every commit | Power cut between commits → lose last few seconds |
| **FULL** (chosen) | Fsyncs WAL on every commit | Power cut → lose at most current uncommitted batch |
| **EXTRA** | Fsyncs WAL + main DB after checkpoint | Slowest; rarely needed |

**Write performance cost of FULL:**
Each `fdatasync()` on an SD card takes ~5-20ms. At one commit per reader poll (0.5s), that's one fsync per 500ms = negligible overhead.

**At higher frequencies:** If we were committing 100 times per second, FULL sync would add 100 × 20ms = 2 seconds of overhead per second — impossible. We'd switch to batching commits (commit every 50ms, lose up to 50ms of data on crash).

---

## Tradeoff 4: Polling vs Interrupt-driven Data Reading

**What we do:** Poll the mock every 0.5 seconds per sensor (POLL_INTERVAL).

**Why not use the mock's push model?** The mock doesn't have one. The protocol is request-response: we send `{"cmd": "read"}`, mock replies. We have no choice but to poll.

**Why 0.5s interval?**
- Mock buffers 10 seconds. At 0.5s polling, we have a 20× safety margin.
- Polling more frequently (e.g., every 0.1s) would increase mock connection overhead and unnecessary calls when there's no new data.
- Polling less frequently (e.g., every 2s) would give only a 5× safety margin and increase latency for window processing.

**If the mock had a push model**, we'd use a callback architecture:
- Mock pushes data to a TCP connection we hold open.
- Reader thread just reads continuously.
- No polling overhead, minimal latency.

---

## Tradeoff 5: JSON vs Binary Protocol

**Current:** All data is JSON (text).

**JSON costs:**
- A sensor 1 sample with 3 channels: `[720, 1200, 1560]` = ~18 bytes as ASCII vs 12 bytes as 3 × int32 binary. ~50% overhead.
- JSON parsing in Python: slower than `struct.unpack` for binary.
- At 800 samples/s × 5 values × 5 bytes average = ~20 KB/s — trivial.

**Why JSON is fine here:**
- The mock protocol is JSON (spec says "Newline-delimited JSON over TCP"). We cannot change the protocol.
- For the cloud upload, JSON is human-readable (easier to debug), compatible with any HTTP endpoint, and at our data rate the overhead is irrelevant.

**When binary would matter:** If we were streaming at 40 KHz, JSON overhead would become 400 KB/s extra bandwidth and 10% of CPU in parsing. Switch to Protocol Buffers or CBOR.

---

## Tradeoff 6: In-memory Buffer vs Read from DB for Processing

**Current:** Samples are buffered in `self._s1` and `self._s2` (dicts) for window processing.

**Alternative:** Read from SQLite for every window computation.

| Approach | Read latency | Memory use | Complexity |
|---|---|---|---|
| **In-memory dict (chosen)** | O(1) dict lookup | ~300 KB bounded | Medium (eviction, locks) |
| Read from SQLite | ~10ms per query | Minimal | Simpler, no eviction |

**Why in-memory?**
- Window processing is triggered on every new batch (up to twice per second). Reading 2000 rows from SQLite per window computation would add 10ms × 2/s = 20ms/s I/O load.
- More importantly, SQLite has one writer at a time. Processing queries would contend with the write lock.
- The in-memory dict gives immediate access with zero I/O.

**The eviction logic (keep=3000)** bounds memory and ensures we don't grow unboundedly, while keeping enough history for heart rate (needs 2000 samples).

---

## Tradeoff 7: at-least-once vs at-most-once Upload

| Delivery guarantee | How | Risk |
|---|---|---|
| **at-most-once** | Mark synced before upload | A crash after marking but before upload → data never reaches cloud |
| **at-least-once (chosen)** | Mark synced only after cloud ACK | A crash after upload but before marking → data uploaded twice |
| exactly-once | Distributed transaction or 2-phase commit | Very complex, not needed when cloud endpoint is idempotent |

**Why at-least-once is safe here:**
The cloud endpoint uses `INSERT OR IGNORE` with `(session_id, sensor, idx)` as the primary key. Uploading the same sample twice produces no duplicate row. So at-least-once + idempotent cloud = effectively exactly-once delivery.

**Why not at-most-once?**
If we marked samples synced before the HTTP call and then the process crashed, those samples would never be uploaded. The cloud would have a gap. This violates criterion 6 (complete cloud copy).

---

## Tradeoff 8: One Cloud Backend vs Specific Cloud (Supabase/Firebase)

**What we did:** Pluggable backend via `CLOUD_BACKEND` environment variable.

**Why:**
- The spec says "free cloud service" — no single free tier is universally available in India (GCP, AWS, Azure all have credit card requirements).
- Supabase and Firebase both have truly free tiers with generous quotas.
- A self-hosted Flask server on Oracle Always Free ARM64 is completely free forever.
- By abstracting the backend, we can switch without changing any other code.

**Tradeoff:** More code to maintain (three backend implementations). Worth it for flexibility.

---

## Tradeoff 9: Consecutive probe threshold vs Exponential backoff

**What we do:** Simple consecutive count (5 × 2s = 10s).

**Alternative:** Exponential backoff after first failure.

| Approach | Pros | Cons |
|---|---|---|
| **Consecutive count (chosen)** | Simple, predictable switch time | Doesn't adapt to brief blips vs long outages differently |
| Exponential backoff | Adapts to transient vs sustained outages | Unpredictable switch time; might violate the 10s requirement |
| Sliding window average | Smooth; tolerates occasional failures | More complex; may never cross threshold on intermittent 50% loss |

**Why consecutive count:** The spec says "within 10 seconds". A predictable, constant threshold guarantees this. Exponential backoff could theoretically delay the switch beyond 10s.

---

## Tradeoff 10: Stateless vs Stateful Processor

**What we do:** `process_window()` is a pure function — takes dicts of samples and returns a dict. No internal state.

**Tradeoff:** The heart rate computation needs the ch2 history (2000 samples look-back). We pass it as `ch2_history` — built from the in-memory buffer in `engine.py`.

**Why stateless is better:**
- Testable: `test_processor.py` calls `process_window()` directly with synthetic data. No need to mock an engine object.
- Reusable: `process_window()` can be called for offline reprocessing from the DB (re-run all windows with corrected logic) without any engine dependency.
- No hidden state bugs: if the engine resets for a new session, the processor doesn't need to reset anything.

---

## Key Numbers to Know Cold

| Parameter | Value | Why |
|---|---|---|
| Sample rate | 400 Hz | Spec |
| Window size | 400 samples = 1 second | Spec |
| Heart rate look-back | 2000 samples = 5 seconds | Spec |
| Peak refractory period | 100 samples = 0.25s | Spec (min beat interval = 240 bpm) |
| Adaptive threshold | > max_800 / 2 | Spec |
| Network probe interval | 2 seconds | Design: 5 probes × 2s = 10s |
| Mode switch threshold | 5 consecutive | Design |
| Upload batch size | 2000 samples | Design |
| Upload interval | 1 second | Design |
| In-memory buffer size | 3000 samples per sensor | Design: 2000 + 1000 margin |
| Max power-cut loss | < 1 second (< 400 samples/sensor) | WAL + FULL sync |
| Mock buffer size | 10 seconds | Spec |
| Poll interval | 0.5 seconds | Design: 20× safety margin |
| Catch-up budget | 15 minutes for 2h outage | Spec |
| Online processing latency | < 5 seconds | Spec |
