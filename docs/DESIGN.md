# BeatKeeper — design

## 1. Problem and requirements

Two sensors (sensor 1: 3 channels, sensor 2: 2 channels), each 400 samples/s,
signed 24-bit values, delivered by a gateway that keeps only the last 10 s of
each sensor. The device runs for hours, unattended, on a Raspberry Pi-class
board (≤ 4 cores, ≤ 2 GB), over a network that fails without warning.

| # | Requirement |
|---|---|
| R1 | Every received sample is on disk exactly once, unchanged |
| R2 | Every sample not on disk is listed as lost, with a reason; no invented values |
| R3 | Survive process crash, reboot and power cut; state the maximum loss |
| R4 | Online: a result for each second of data in the cloud within 5 s |
| R5 | Switch online ⇄ offline within 10 s of a network change, without loss or duplication |
| R6 | After an outage (up to 2 h) the cloud copy is complete within 15 min |
| R7 | Window results are identical whichever side computed them |
| R8 | Fit small-device limits; report CPU, memory, disk and network use |

## 2. Architecture

```
                         ┌──────────────── device (Raspberry Pi) ────────────────┐
 sensor gateway ◄─TCP──► │ Reader ─► Engine ─► Store (SQLite WAL, fsync)          │
 one client at a time    │            │  └─► SessionProcessor (shadow / offline)   │
                         │            ▼                                            │
                         │      mode (online|offline) ◄── NetworkMonitor          │
                         │            │                       (probes cloud)       │
                         │            ▼                                            │
                         │          Syncer ── gzip HTTP, batches of 8 000 ─────────┼──► Cloud service
                         └─────────────────────────────────────────────────────────┘     ingest (fsync'd)
                                                                                         SessionProcessor
                                                                                         = online results
```

**Threads on the device:** reader (both sensors on one socket), syncer,
network monitor, heartbeat. Processing runs on the reader thread, so
processing state needs no locks.

**Data flow, in order, for every batch:** `Reader → Store.save_samples (commit + fsync) →
SessionProcessor.add_samples → windows → Store.save_window`. Upload is a separate
concern: the Syncer reads *from the database*, never from memory.

### Processing placement

| Mode | Who computes the authoritative result | The other side |
|---|---|---|
| online | **cloud** (as samples arrive) | device computes a *shadow* result locally, stored with `synced=1` so it is never uploaded |
| offline | **device** | uploaded after reconnection (windows are uploaded *before* samples, so the cloud knows not to recompute them) |

Both sides run the same `SessionProcessor` class. An integration test asserts
that every cloud-computed result equals the device's shadow result.

### Windowing by watermark

For each sensor the processor keeps a *resolved watermark*: the first index that
is neither present nor known-lost. Window *w* is emitted only when both
watermarks are past its end (sensor 1 needs one sample further: the peak rule
looks at the sample after). So a window is computed from "everything that will
ever exist for it" — never from data still in flight. At session end, the
missing "next sample" is treated as absent so the final window is emitted too.

## 3. Guarantees, and what each rests on

| Guarantee | Mechanism |
|---|---|
| No loss of committed data | WAL + `synchronous=FULL`: commit returns only after fsync |
| Exactly once on disk | primary key `(session, sensor, idx)` + `INSERT OR IGNORE` |
| Values unchanged | CRC-32 over the *raw text* of each reply; bad replies re-read, never used |
| No silent gaps | gateway-reported `lost` ranges stored; any index jump the gateway did *not* report is detected by arithmetic and stored as `unreported_gap` |
| Read cursor never skips | cursor advances only after the batch is durably stored; a storage error means "read again" |
| Restart of the gateway while stopped | the unfinished session found on disk is marked lost from the last stored index (end unknown). Sessions that started *and* ended entirely while the pipeline was off cannot be known |
| Restart without a gap | on startup the cursor resumes from the last stored index and re-requests it with `from_index` (gateway buffer is 10 s) |
| Duplicates harmless | replayed replies recognised by `first_index`; storage is idempotent anyway |
| Complete cloud copy | `synced` flag set only after the cloud's 200; cloud writes are idempotent and fsync'd before the 200 |
| Mode switch ≤ 10 s | 3 consecutive failed probes × (1 s timeout + 1 s interval) = 6 s; 3 successes ≈ 3 s |
| No loss across a switch | the mode only decides *who computes* and *whether to upload*; capture and disk writes are unaffected |

### Maximum data loss on a power cut

* **Committed data: none.** A transaction that returned has been fsync'd.
* **In flight: at most one batch** — the samples read from the gateway but not yet committed. A reply holds ≤ 1 s of one sensor (the reader polls every 0.25 s, so typically ≈ 0.25 s ≈ 100 samples per sensor).
* **In practice often zero**, because on restart the reader resumes from the last *stored* index and re-requests the missing samples from the gateway, which still holds them for 10 s. If the restart is later than 10 s, the overflow is recorded as a `buffer_overflow` loss.
* Data produced while the device is *off* is not capturable by any design; the guarantee is about data the device received.
* The bound assumes the storage honours `fsync`. Some consumer SD cards acknowledge writes they have not made durable; use an endurance-rated card or SSD for deployments.

### What is *not* guaranteed

| Situation | Behaviour |
|---|---|
| Reader blocked > 10 s (overload, long stall) | gateway drops data; **recorded as a loss**, cannot be recovered |
| Disk full | writes fail; the reader stops advancing its cursor and retries (nothing is skipped); if free space returns within the gateway's 10 s buffer nothing is lost, otherwise the overflow is recorded |
| Cloud down for hours | device keeps working; catch-up time grows with outage length (≈ 1.3 min per 2 h on loopback) |
| Clock changes | all control-flow timing uses monotonic intervals or counters; wall-clock time appears only in log/metadata fields |
| Window results during the ≈ 6 s before an outage is detected | computed late, in the cloud, after reconnection (still correct, but not within 5 s) |

## 4. Failure matrix

| Failure | Detected by | Effect | Recovery |
|---|---|---|---|
| Corrupt reply | CRC mismatch | discarded | immediate re-read from same index |
| Duplicate reply | `first_index` ≤ requested | discarded | none needed |
| Gateway stops answering | socket timeout (3 s) | reconnect loop | resume from stored cursor; overflow → loss |
| Sensor dropout | `lost` in reply | stored as loss | windows show `missing_N`, stats over available samples |
| Gateway restart (new session) | session id changes | open-ended loss for the old session; new session starts | automatic |
| Gap not reported by gateway | index arithmetic | stored as `unreported_gap` | none (it is the record) |
| Network down | health probe ×3 | switch to offline, compute locally | on return: upload losses → offline windows → samples |
| Cloud restarts | probe / HTTP errors | offline mode | processor state rebuilt from the cloud's database |
| Upload interrupted mid-batch | no 200 | rows stay unsynced | batch re-sent; idempotent |
| Pipeline killed | — | state on disk | `_recover()` replays recent samples to rebuild processor state, continues at the next window |

## 5. Trade-offs I made

* **SQLite over a custom log.** Slower than an append-only file but gives transactions, crash recovery and queries for free; 800 inserts/s is far below its limits.
* **Rows over packed blocks.** One row per sample is simple and makes "exactly once, unchanged" trivially checkable, at ≈ 71 B/sample. Packed per-second blocks would be ≈ 3× smaller; I chose auditability first.
* **At-least-once + idempotent writes** instead of attempting exactly-once delivery, which is not achievable over an unreliable network.
* **Shared `SessionProcessor`, not two implementations.** Removes a whole class of "device and cloud disagree" bugs.
* **Stdlib only.** Fewer things to install on a fresh board, nothing to patch, easier to audit; costs a little ergonomics (e.g. `http.server` instead of Flask).
* **HTTP + API key, not MQTT/gRPC.** Simplest thing that works through hospital firewalls; TLS termination is deployment-time.

## 6. Hardening log — what the first version got wrong

A first working version (written against a take-home brief) passed its own unit
tests but had real defects. Finding them was the main exercise in building this
version. Each is fixed and, where practical, covered by a regression test.

| # | Defect in v0 | Why it mattered | Fix |
|---|---|---|---|
| 1 | Two reader threads, two TCP connections to a gateway that serves **one client at a time** | one sensor could starve the other | single reader, one socket, both sensors in turn |
| 2 | CRC computed over a *re-serialised* copy of the data, plus an unused `raw.index(...)` that could throw | a correct reply could be rejected, or the thread could crash on a format difference | CRC over the exact raw text of the `data` array |
| 3 | "Gap detection" described in the design note but not implemented | silent loss possible | index arithmetic → `unreported_gap` losses |
| 4 | After restart the reader began from the gateway's *current* position | data still in the 10 s buffer was abandoned | resume from last stored index via `from_index` |
| 5 | Window processed when *either* sensor had data (`and` instead of `or` in the wait condition) and never recomputed | windows reported data as missing that was merely late | per-sensor resolved watermarks; emit only when both are resolved |
| 6 | Windows keyed by `(session, window)` | "one row per window **and mode**" impossible | key includes mode |
| 7 | Loss uploads were non-idempotent | retries produced duplicate loss rows in the cloud | unique index (incl. open-ended losses) + `INSERT OR IGNORE` |
| 8 | Syncer slept 1 s between 2 000-row batches | 2 h outage ⇒ ≈ 80 min catch-up vs the 15 min requirement | drain flat-out, 8 000-row gzip batches (≈ 1.3 min projected) |
| 9 | Probe interval 2 s × 5 failures, 1.5 s timeout | worst case ≈ 17 s vs the 10 s requirement | 1 s interval × 3 failures, 1 s timeout (≈ 6 s) |
| 10 | Probe target was `8.8.8.8:53` | "internet works" ≠ "my cloud accepts data"; behind a captive portal it lies | probe the cloud's `/health` |
| 11 | "Online processing" actually ran on the device | not what the product claims | `SessionProcessor` runs inside the cloud service |
| 12 | Peak detection rescanned ≈ 2 000 × 800 samples every second | CPU waste on a small board | streaming monotonic-deque detector, O(1)/sample, proven equal to the literal rule |
| 13 | Only the *current* session was ever synced | after a gateway restart, the old session's data was never uploaded | sync everything unsynced, across sessions |
| 14 | `os.kill(pid, 0)` used to test "is it running" | on Windows that **terminates** the process | heartbeat file with a freshness window |
| 15 | Cloud resumed processing at `MAX(window)+1` after a restart | windows preceding already-uploaded offline results were skipped forever (found by the chaos test) | resume at the *first missing* window |
| 16 | Reader never closed its socket on shutdown | a one-client gateway stayed occupied, starving the restarted process (found by the crash test) | close on stop |
| 17 | If the *first* batch of a session failed to save, the retry had no cursor, so the gateway continued from its own position | that batch was skipped with no record (found by the disk-error test; my first fix for it introduced item 18, which the restart test then caught) | claim the cursor at the first index seen, before persisting |
| 18 | After a gateway restart the reader asked the new session for "whatever is next" and adopted a stale position | the first ~4,000 samples of the new session were skipped, unflagged (found by the whole-system restart test) | a new session is explicitly requested from index 0; any later start is caught by gap detection |
| 19 | Resource "figures" in the notes were estimates presented as results | unverifiable claims | removed; only measured numbers are published, with their environment |
| 20 | If the gateway restarted while the pipeline was **stopped**, nothing recorded the old session's unread tail | a silent gap, contradicting the main guarantee (found in a later code review, then reproduced by a test) | on first contact, an unfinished session that is gone gets an open-ended `gateway_restart` loss for both sensors |

## 7. Roadmap

* Run and publish the Raspberry Pi measurements (CPU, RSS, SD write volume) and a real power-pull test.
* Cut write amplification (measured ≈ 9× stored data ≈ 2 GB/h): one commit per second, packed per-second binary sample blocks, and a `synced` high-water mark instead of per-row flags.
* TLS and per-device keys; a small web dashboard over `/v1/results`.
* Shadow-vs-cloud divergence alarm (the data to compare is already stored).
* Disk-pressure policy: alert and prune only *synced* data, never unsynced.
