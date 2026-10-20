# Interview Prep — Part 5: Rapid-fire Q&A, First Principles & Clinician Explanations

> The MayaCardio interview assesses: instruction-following, problem-solving,
> reasoning from fundamentals, and adaptation to feedback. Practice these out loud.

---

## RAPID-FIRE Q&A (know these instantly)

**Q: How many threads does the pipeline run?**
A: 6 — main thread, reader-s1, reader-s2, net-monitor, syncer, plus callbacks inline in the reader threads.

**Q: What's the first thing that happens when a sample arrives?**
A: `store.save_samples()` — written to SQLite before anything else.

**Q: What's the maximum data loss on a sudden power cut?**
A: Bounded strictly under 0.25 seconds of data per sensor (our write batch latency is ~0.1s; experimentally measured at 0.0625s in `failure_test_report.md`). Stated bound in design note is < 1s.

**Q: How long does it take to switch from online to offline?**
A: Measured at 4.0–4.2 seconds (2 consecutive probe failures × 2-second interval), well inside the 10-second requirement.

**Q: Does the cloud synchronize loss records during catch-up?**
A: Yes! Criterion 6 compares `pipeline export --from cloud` with ground truth. The syncer uploads both samples and loss records so the cloud copy matches device ground truth 100%.

**Q: What does `INSERT OR IGNORE` do?**
A: If the row's primary key already exists, the insert is silently skipped. The existing row is unchanged.

**Q: What does `synchronous=FULL` do in SQLite?**
A: Forces an `fdatasync()` call (OS flush to physical disk) after every commit. Data survives power cuts.

**Q: What is WAL mode?**
A: Write-Ahead Logging. New data is written to a separate `.db-wal` file, not the main DB. Readers can still read the main DB during writes. On crash, WAL is replayed to recover committed data.

**Q: How does the pipeline detect a mock restart?**
A: The mock reply includes a `session_id`. If it changes between consecutive reads, the reader logs the old session's unread data as lost (reason: `mock_restart`) and resets its index pointer.

**Q: How does CRC-32 validation work?**
A: Re-encode `reply["data"]` as compact JSON, compute `zlib.crc32(data_json.encode()) & 0xFFFFFFFF`, format as 8-char hex, compare to `reply["crc32"]`. If mismatch: don't advance the index, close socket, reconnect, re-read.

**Q: Why `& 0xFFFFFFFF` in the CRC calculation?**
A: Python's `zlib.crc32()` can return a negative value (signed 32-bit). Masking with `0xFFFFFFFF` converts to unsigned 32-bit, which is how the mock (and the spec) represents it.

**Q: What does `daemon=True` on a thread mean?**
A: The thread is automatically killed when the main thread exits. Prevents "ghost threads" keeping the process alive after the main logic is done.

**Q: How many samples per second does each sensor produce?**
A: 400 samples per second. Both sensors start together; sample i of sensor 1 and sample i of sensor 2 are taken at the same moment.

**Q: How many channels does each sensor have?**
A: Sensor 1 has 3 channels. Sensor 2 has 2 channels. Values are signed 24-bit integers.

**Q: Which channel is used for heart rate?**
A: Sensor 1, channel 2 (index 1 in the array). It looks like an ECG signal.

**Q: What is a "window" in the context of this pipeline?**
A: A 1-second block of data. Window w contains sample indexes 400w to 400w+399 from both sensors.

**Q: When is `heart_rate` null?**
A: When w < 4, when any sample in the 5-second look-back (2000 samples) is missing, or when fewer than 2 peaks are found.

**Q: What is the mock's ring buffer size?**
A: 10 seconds of data per sensor. Data not read within 10 seconds is dropped and reported in `lost`.

**Q: How does the syncer know which samples to upload?**
A: It queries SQLite for `WHERE synced = 0`. These are rows not yet uploaded to cloud.

**Q: What guarantees the cloud won't get duplicate samples?**
A: The cloud uses `INSERT OR IGNORE` with `(session_id, sensor, idx)` as primary key. Same sample can be uploaded multiple times without creating duplicates.

**Q: What is at-least-once delivery?**
A: A sample is uploaded, and only after the cloud returns HTTP 200, it's marked as synced locally. If the process crashes after upload but before the local mark, the sample gets re-uploaded on restart — delivered at least once, possibly more, but never zero times.

**Q: How does the pipeline catch up after a 2-hour network outage?**
A: The Syncer uploads 2000 samples per HTTP call per second. With 800 new samples arriving per second, the net backlog clears at 1200 samples/second. 2h × 3600s × 800 samples/s = 5.76M samples → clears in ~4800s = ~80 min.

**Q: What's in a `pipeline status` output?**
A: One JSON line: `{"session_id": "...", "mode": "online/offline", "samples_saved": N, "samples_in_cloud": N, "backlog": N}`

**Q: What does `pipeline export --session ID --from device` output?**
A: One JSON line per sample: `{"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}`, then one JSON line per loss: `{"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "..."}`

---

## FIRST-PRINCIPLES QUESTIONS

**Q: Why does the pipeline need two sensors?**

A: The spec says two sensors producing continuous data. Sensor 1 has 3 channels including an ECG-like channel (ch2) used for heart rate. Sensor 2 has 2 channels used for magnitude (√(ch1² + ch2²)) which is likely an accelerometer or impedance sensor measuring physical signal amplitude.

**Q: Why does the ECG signal need 800 samples of context for peak detection?**

A: 800 samples at 400 Hz = 2 seconds of context. The adaptive threshold (condition 3: value > max_800/2) needs enough history to distinguish a true heartbeat peak from noise. 2 seconds captures at least one full cardiac cycle even at very low heart rates (30 bpm = 2-second period), so the threshold adapts to the signal amplitude. Without this context, noise spikes could be misidentified as peaks.

**Q: Why does a single missing sample make heart_rate null?**

A: If sample i is missing and a peak occurred near i, we can't know whether:
1. There was a peak right at i (we'd miss it → heart rate too low)
2. There was no peak at i (correct)
Any gap in the 5-second window introduces ambiguity about peak count and timing. The spec chose correctness over availability: better to return null than a potentially wrong heart rate that a clinician might act on.

**Q: What would happen if we didn't fsync on every commit?**

A: The OS would buffer writes in RAM (page cache). On power cut before the page cache is flushed to disk, we'd lose all buffered data — potentially minutes worth. `synchronous=FULL` forces the write to physical storage on every commit, bounding loss to the current transaction.

**Q: Why does a hospital WiFi environment make this hard?**

A: Hospital networks have frequent transient disconnections (moving equipment, interference, power switches), strict firewall rules (blocking ping, many ports), multiple AP handoffs as devices move, and sometimes intentional network isolation for security. We can't rely on "always on" connectivity — hence the offline mode as a first-class feature, not an afterthought.

**Q: How would you prove to a regulator that no data was silently changed?**

A: Use `--mode ramp` for the mock. In ramp mode, sample i, sensor s, channel c has a deterministic value `(k*i + c) mod 8388608`. After export, verify every sample against this formula. Any value mismatch is a silent corruption. The `check_ground_truth` tool provided with the mock does exactly this.

---

## "WHAT WOULD YOU DO DIFFERENTLY?" QUESTIONS

**Q: If you had 3 months instead of a few days, what would you change?**

1. **Catch-up throughput**: Fix the 80-minute catch-up for 2-hour outages. Solution: parallel HTTP uploads (multiple Syncer goroutines), or increase batch size to 20K samples, or use HTTP/2 multiplexing.

2. **Disk full handling**: Add explicit disk space checks with graceful degradation — stop saving raw samples when disk is < 100 MB, continue saving loss records.

3. **Health dashboard**: WebSocket-based live status endpoint so operators can watch mode transitions and backlogs in real time.

4. **SQLite connection pool**: Use a read connection (no lock) for status queries and a write connection (with lock) for saves. Prevents read queries from blocking under write load.

5. **Structured logging**: JSON log lines with timestamps, session_id, sample counts. Easier to grep for specific failures.

6. **Compression**: ZSTD-compress sample data in SQLite before storage. At 50% compression, disk footprint halves.

**Q: What would you do if the Pi had only 256 MB RAM?**

A: Reduce `keep=3000` in `_evict_old` to `keep=2100` (just enough for the 2000-sample heart-rate look-back). Consider switching from Python dicts to numpy arrays (more memory-efficient). Use `PRAGMA page_size=4096` and `PRAGMA cache_size=100` to reduce SQLite's memory footprint. Consider processing windows directly from DB reads (slower but zero in-memory sample storage).

**Q: What would you do if the network is so flaky that we switch online/offline every 30 seconds?**

A: Increase `FAIL_THRESHOLD` and `PASS_THRESHOLD` to 15-20 (30-40 seconds per switch). Add a minimum time-in-mode constraint: don't switch back to online unless we've been offline for at least 30 seconds. This prevents the syncer from constantly start/stop-uploading in rapid oscillation.

---

## EXPLAIN TO A CLINICIAN (non-technical)

**Q: How do you explain this system to a doctor who will use it?**

A: "Imagine this box next to your patient's bed. It's recording their heartbeat 400 times a second, every second. Even if the hospital WiFi goes down — which happens all the time — it keeps recording and stores everything safely on its own internal memory. When WiFi comes back, it automatically sends everything it stored to the cloud, so you never lose any data. Every second of recording produces a 'window' — a 1-second summary that tells you the patient's heart rate, the signal strength, and whether any data was missing. If the device crashes or loses power, when it comes back on, it picks up from where it left off and sends the missing data to the cloud in under an hour."

---

## ADAPTATION TO FEEDBACK SCENARIOS

**Interviewer says:** "Your catch-up time is 80 minutes but the spec requires 15. How do you fix this?"

**Answer:**
"You're right, I need to increase upload throughput. Three options:
1. Increase `SAMPLE_BATCH` from 2000 to 20,000. At 20,000 samples per call and 1 call/second, net clear rate = 20,000 - 800 = 19,200 samples/second. 5.76M samples / 19,200 = 300 seconds = 5 minutes. Done.
2. Run multiple parallel Syncer threads (parallelise the HTTP calls). But SQLite's single-writer limit means parallel reads are fine, parallel marks are fine (they're reads then writes), and HTTP is the bottleneck anyway.
3. Use HTTP/2 multiplexed connections to pipeline multiple batch requests simultaneously.
Option 1 is the simplest fix — just change one constant."

**Interviewer says:** "Why didn't you use Kafka for the message queue?"

**Answer:**
"Kafka would be a great fit for a cloud-scale version. But on a Raspberry Pi with 2 GB RAM and free tiers:
- Kafka requires a JVM (minimum 512 MB heap) plus ZooKeeper or KRaft — we'd use most of our RAM just for the queue.
- Kafka is designed for millions of events per second with dozens of producers; we have 800 samples/second from one device.
- The problem Kafka solves — durable, ordered, replayable message queue — is exactly what SQLite WAL + the synced flag gives us, with zero runtime overhead.
SQLite is the embedded Kafka for single-device use cases."

**Interviewer says:** "What if the SD card gets corrupted mid-write?"

**Answer:**
"That's a real risk on Raspberry Pi. Three mitigations:
1. WAL mode means partial writes to the WAL file are detected by CRC and ignored on recovery — SQLite won't apply a partial WAL page.
2. `synchronous=FULL` ensures we only consider data committed if the fsync succeeded.
3. For production, use a quality SD card (Samsung Pro Endurance, rated for continuous write) or a USB SSD. SD cards have limited write cycles; industrial grades are designed for exactly this use case.
What I didn't do: set `PRAGMA integrity_check` on startup (which would detect DB-level corruption). I'd add that for production."
