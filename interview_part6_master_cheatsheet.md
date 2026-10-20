# Interview Prep — Part 6: Master Cheatsheet (Morning-of-Interview Review)

> Read this the morning of the interview. Know every line cold.

---

## THE FULL SYSTEM IN ONE BREATH

> Practice saying this out loud in under 90 seconds.

"The pipeline is a single Python process with six threads. Two reader threads — one per sensor — connect to the biosignal-mock over TCP and pull newline-delimited JSON at 400 samples per second per sensor. Before anything else, every batch is written to a local SQLite database using WAL mode and `synchronous=FULL` — that's what makes data survive power cuts. The data then goes into an in-memory rolling buffer per sensor. A window-processing step fires whenever a new second of data is complete, computing heart rate, magnitude, and per-channel stats using the exact rules in section 3 of the spec.

A separate network-monitor thread probes 8.8.8.8:53 every 2 seconds. After 5 consecutive failures — so within 10 seconds — it switches the pipeline to offline mode. In offline mode, data keeps saving to disk and processing still runs, but the cloud upload stops. When the network comes back — again within 10 seconds — a syncer thread resumes uploading all unsynced samples in batches to the cloud, using an idempotent upsert so there are no duplicates even if we crash mid-upload.

The CLI exposes `pipeline status`, `pipeline export`, and `pipeline results` commands. The cloud side is a small Flask server running on any free-tier VM that stores and serves the data."

---

## EVERY KEY NUMBER

| What | Value | Why |
|---|---|---|
| Sample rate | **400 Hz** | Spec |
| Channels per sensor | **S1: 3, S2: 2** | Spec |
| Window size | **400 samples = 1 second** | Spec |
| Heart rate look-back | **2000 samples = 5 seconds** | Spec |
| Peak refractory | **100 samples = 0.25 s** | Spec (max HR = 240 bpm) |
| Adaptive threshold | **> max_800 / 2** | Spec |
| Mock ring buffer | **10 seconds** | Spec |
| Max power-cut loss | **< 0.25 seconds** | WAL + atomic batch commits |
| Network probe interval | **2 seconds** | Design |
| Mode switch threshold | **2 consecutive probes (4s)** | Design → ≤ 10 s max |
| Upload batch size | **2000 samples** | Design |
| Upload interval | **1 second** | Design |
| In-memory buffer cap | **5000 samples per sensor** | Design (2000 lookback + 800 context + margin) |
| Online processing SLA | **< 5 seconds** | Spec |
| Catch-up budget | **15 min for 2h outage** | Spec |
| Probe target | **8.8.8.8:53 (TCP)** | DNS, rarely firewalled |
| SQLite journal mode | **WAL** | Concurrent reads, crash-safe |
| SQLite sync mode | **NORMAL / FULL** | Durability on transaction checkpoints |

---

## ALL 8 CRITERIA → CODE

| # | Criterion | File + mechanism |
|---|---|---|
| 1 | Every sample on disk exactly once | `store.py` → `INSERT OR IGNORE` PK `(session_id, sensor, idx)` |
| 2 | Lost samples listed with reason | `store.py` `losses` table + `reader.py` `on_loss()` callback |
| 3 | Survives crash/reboot/power cut | SQLite WAL; bound < 0.25s verified in `failure_test_report.md` |
| 4 | Online: cloud result within 5s | `syncer.py` uploads windows every 1s cycle |
| 5 | Mode switch ≤ 10s, no loss/dup | `network.py` hysteresis (4s); disk-first write order |
| 6 | Cloud complete within 15 min | `syncer.py` pushes samples AND losses; `pipeline export --from cloud` matches ground truth |
| 7 | Results follow section 3 exactly | `processor.py` + 30 unit/integration tests |
| 8 | Runs in ≤ 4 cores / 2 GB | Stdlib-only; rolling 5000-sample buffer; ~45 MB peak |

---

## THE 10 HARDEST QUESTIONS — CRISP ANSWERS

**Q1: How does WAL mode make writes crash-safe?**
New data goes to a separate `.db-wal` file. SQLite fsyncs the WAL before returning from COMMIT. If power is cut: on restart, SQLite replays the WAL automatically. Partial WAL pages (cut mid-write) are detected by CRC and discarded — transaction is as if it never started. No corruption.

**Q2: Why `INSERT OR IGNORE` not `INSERT OR REPLACE` for samples?**
`OR REPLACE` deletes the existing row and inserts a new one. For samples, we trust the first write. If the mock delivers the same batch twice (judge fault injection), `OR IGNORE` keeps the original without touching it. `OR REPLACE` would delete + reinsert, which resets the `synced` flag to 0 on an already-synced sample → it would be uploaded again unnecessarily.

**Q3: What stops two threads from processing the same window twice?**
`_window_lock` protects `self._next_window`. When reader-s1 and reader-s2 both trigger `_try_process_windows()` concurrently, the lock ensures only one increments `_next_window` to 6 — the other then reads 6 and skips window 5.

**Q4: Why does the CRC check re-encode the data field?**
The spec says CRC-32 of "the data text as sent" — meaning the exact bytes the mock used for the JSON array. We can't use Python's `json.loads` output for this because Python may add spaces or reorder keys. We re-encode with `json.dumps(reply["data"], separators=(",", ":"))` — compact JSON, no spaces — matching what the mock used when it computed the CRC.

**Q5: Prove no sample is lost during a mode switch.**
The mode flag controls only the Syncer's upload behaviour. The disk-write path in `_on_data()` runs before the mode flag is checked. Even if NetworkMonitor switches mode during a `_on_data()` call, the sample is already committed to SQLite before the switch takes effect. The Syncer will include it in the next online upload cycle.

**Q6: What is "at-least-once delivery" and why is it safe here?**
We mark a sample as synced only AFTER the cloud returns HTTP 200. If we crash between upload and the local mark, the sample gets re-uploaded on restart. The cloud's `INSERT OR IGNORE` ensures no duplicate row is created. Net result: every sample lands in the cloud exactly once, even with crashes.

**Q7: How does the pipeline handle a mock that sends the same reply twice?**
The reader gets the same JSON line twice. The second `INSERT OR IGNORE` for the same `(session_id, sensor, idx)` is silently skipped. `_next_index` is already past those samples (set after the first delivery), so the second delivery is harmless.

**Q8: Why can't you just use ping to check network?**
ICMP ping is blocked by most hospital firewall policies for security. A TCP connect to a well-known address (8.8.8.8:53, Google DNS) works through nearly all firewalls because DNS traffic is required for normal network operation. Also, TCP connect confirms bidirectional connectivity, not just one-way ICMP.

**Q9: What does `to_index: null` mean in a loss record?**
When the mock restarts, we know data from `_next_index` onward is lost — but we don't know how many more samples the old session would have produced (because the mock never told us before restarting). `null` means "from `from_index` to an unknown end". The spec explicitly requires this rather than inventing an end index.

**Q10: Why does heart rate use mean gap, not mean peak rate?**
Mean gap (mean of inter-beat intervals) and mean peak rate (60 / count of peaks over 5 seconds) give similar results but mean gap is more robust: it naturally weights longer gaps more (a missed beat adds a long gap), which is the correct behaviour for a heart rate estimator. The spec defines it as "60 ÷ mean gap between consecutive peaks in seconds", so we follow the spec exactly.

---

## COMPONENT ONE-LINERS

| Component | One-line job | One-line tradeoff |
|---|---|---|
| `reader.py` | Pulls JSON from mock TCP; validates CRC | Polling (no push) — 0.5s interval gives 20× mock buffer margin |
| `store.py` | WAL SQLite; idempotent inserts; sync flag queue | fsync on every commit = slow writes but crash-safe |
| `processor.py` | Pure-function windowed stats + heart rate | Stateless = testable + reusable; history passed as arg |
| `network.py` | Probes 8.8.8.8:53 every 2s; 5-probe hysteresis | TCP over ICMP — works through firewalls |
| `syncer.py` | Uploads unsynced rows; marks only after ACK | at-least-once + idempotent cloud = no loss, no duplicates |
| `engine.py` | Wires all threads; disk-first invariant | Inline window processing in reader callback — simple but not parallelised |
| `cloud.py` | Strategy pattern; HTTP/Supabase/noop | stdlib urllib — no pip install; no connection pooling |
| `cli.py` | run/status/export/results subcommands | Status reads from DB, not live engine — may lag by one cycle |
| `cloud_server.py` | Flask receiver; idempotent upserts; export endpoint | SQLite server-side — simple but single-writer |

---

## WHAT YOU BUILT — TRACEABILITY

```
Assessment criterion          → What you built
─────────────────────────────────────────────────────────────────────────────
"never loses data"            → store.save_samples() BEFORE any other step
                                 WAL + synchronous=FULL (< 1s max loss)

"processes data live"         → Syncer uploads windows within 1s of commit
                                 Cloud stores results with timestamp

"keeps working offline"       → NetworkMonitor switches in ≤ 10s
                                 Processor runs in offline mode, tags results

"switch automatically"        → NetworkMonitor hysteresis (5 probes × 2s)
                                 Syncer.set_mode() triggered immediately

"catch up"                    → Syncer reads WHERE synced=0, batch uploads
                                 Idempotent cloud → no duplicates after outage

"handle loss honestly"        → losses table + on_loss() for every gap
                                 to_index=null when end unknown (mock restart)
                                 NEVER invent values (insert corrupt = rejected by CRC)

"pipeline status"             → cli.py cmd_status() → JSON line
"pipeline export"             → cli.py cmd_export() → samples + losses JSON lines
"pipeline results"            → cli.py cmd_results() → window results JSON lines
```

---

## PRE-INTERVIEW CONFIDENCE CHECKLIST

- [ ] I can draw the architecture on a whiteboard in 2 minutes
- [ ] I can explain WAL + FULL sync without looking it up
- [ ] I can trace a sample from TCP wire to cloud DB, step by step
- [ ] I can explain why the CRC re-encoding uses `separators=(",", ":")`
- [ ] I know all 4 peak detection conditions and why each exists
- [ ] I know what `to_index: null` means and when it's used
- [ ] I know the difference between `INSERT OR IGNORE` and `INSERT OR REPLACE`
- [ ] I can explain the threading lock strategy without looking at the code
- [ ] I can explain at-least-once delivery and why it's safe here
- [ ] I can give honest answers about what's NOT guaranteed (disk full, catch-up time)
- [ ] I know all 8 scoring criteria and which file/function covers each
- [ ] I can say "I don't know yet, but here's how I'd find out" without flinching

---

## DISCLOSURE (say this proactively)

> "I used Antigravity (an AI coding assistant by Google DeepMind) to generate and
> iterate on the code. I verified every design decision against the spec, ran all
> 30 tests, and I'm ready to explain, modify, or debug any part of the code in
> the interview."

This shows: intellectual honesty, compliance with rules, and confidence that you understand what you built.
