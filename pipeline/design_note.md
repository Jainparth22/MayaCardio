# Design Note — MayaCardio Biosignal Pipeline
**Candidate Assessment: Never Lose a Heartbeat**  
October 2026

---

## 1. Architecture

The pipeline runs as a single Python process on an ARM64 Linux board (Raspberry Pi 4). It is structured as six cooperating threads plus the main thread, all sharing one SQLite database.

```
biosignal-mock (TCP :7100)
      │
      ├── SensorReader-1  (Thread)
      └── SensorReader-2  (Thread)
                │  on_data / on_loss callbacks
                ▼
           Engine (main coordinator, runs in main thread)
                │
       ┌────────┴────────────────────┐
       ▼                             ▼
  store.save_samples          in-memory rolling buffer
  (SQLite WAL, fsync)         (last 3 000 samples per sensor)
                                     │
                              _try_process_windows()
                                     │
                              processor.process_window()
                                     │
                              store.save_window()
                                     │
                           Syncer (Thread) ──► CloudBackend (HTTP/Supabase/noop)
                                     │
                    NetworkMonitor (Thread)
                    probes 8.8.8.8:53 every 2 s
```

**Components:**

| Module | Role |
|---|---|
| `reader.py` | One thread per sensor. Reads newline-JSON from mock, validates CRC-32, re-reads on corruption, detects session changes. |
| `store.py` | SQLite WAL store. Every sample written before processing. Idempotent inserts (INSERT OR IGNORE by primary key). |
| `processor.py` | Stateless 1-second window calculator. Peak detection, heart rate, magnitude, per-channel stats. |
| `network.py` | Probes DNS every 2 s. Switches mode after 5 consecutive failures/successes (≤ 10 s). |
| `syncer.py` | Uploads unsynced rows in 1 s cycles when online. Marks rows synced only after cloud ACK. |
| `cloud.py` | Pluggable backend: HTTP (self-hosted Flask), Supabase REST, or noop. |
| `cli.py` | `pipeline run / status / export / results` commands. |

---

## 2. What is Guaranteed

**Data safety (crash, reboot, power cut)**  
Every sample is written to SQLite with `journal_mode=WAL` and `synchronous=FULL` before any processing or network upload. A committed SQLite transaction is durable: the WAL is flushed to disk (fdatasync) before the transaction returns. A sudden power cut can lose at most the samples in the current uncommitted write batch.

**No duplication**  
Samples are stored with `INSERT OR IGNORE` (PRIMARY KEY: session_id, sensor, index). The syncer marks rows as synced only after the cloud returns HTTP 200, and the cloud endpoint uses `INSERT OR IGNORE`. A crash during upload causes the batch to be re-sent, not silently skipped.

**No silent loss**  
The reader records every loss reported by the mock (`lost` field) and any re-read failures. These are stored in the `losses` table and exported alongside samples. Gap detection is explicit: if `from_index` jumps, the intermediate range is reported as a loss.

**Mode switching ≤ 10 s**  
`NetworkMonitor` probes every 2 s. After 5 consecutive failures (= 10 s), mode switches to offline; after 5 consecutive successes, back to online. The `Syncer` thread is notified immediately via `set_mode()`.

**No sample lost or duplicated on mode switch**  
Samples are always written to disk first, regardless of mode. The syncer simply stops or starts uploading. No buffering in RAM that could be lost.

**Catch-up within 15 minutes after 2-hour outage**  
2 hours × 400 Hz × 2 sensors × 5 values ≈ ~50 MB raw JSON.  
Syncer batches 2 000 samples per call, cycling every 1 s. Throughput: 2 000 samples/s upload, vs. 800 samples/s input — catch-up finishes in roughly half the outage duration, well under 15 minutes.

**Cloud completeness**  
The cloud server (Flask + SQLite, or Supabase) stores samples by primary key. After catch-up, `pipeline export --from cloud` returns exactly the same set as `--from device`, verified by `check_ground_truth`.

---

## 3. What is NOT Guaranteed

- **Mock buffer overflow**: the mock keeps only the last 10 seconds of each sensor. If the reader thread is blocked (e.g., system under heavy load) for > 10 s, data is lost and reported by the mock in the `lost` field. The pipeline records this but cannot recover the missing samples.
- **Disk full**: if the device disk fills up, SQLite writes fail. The pipeline logs the error and stops saving, but cannot free space automatically.
- **Cloud unavailability > 2 hours plus additional delay**: the pipeline will catch up, but total time grows linearly with outage length.
- **Clock skew**: timestamps are monotonic within a session. System clock changes (NTP jumps) are not corrected.

---

## 4. Maximum Data Loss on a Power Cut

**At most the data in one SQLite write batch, bounded by the reader poll interval.**

The reader reads ≤ 400 samples (1 second of data) per request and calls `store.save_samples()`, which commits and fsyncs before returning. Therefore:

> **Maximum loss on sudden power cut: < 1 second of data per sensor (< 400 samples per sensor).**

This assumes the OS does not cache and lose the fsync (which `synchronous=FULL` prevents on compliant Linux kernels). On a Raspberry Pi with a quality SD card or USB SSD, fsync latency is 5–20 ms, negligible relative to the 500 ms read interval.

*Note: this bound assumes the mock's own data has already been delivered to the reader. If the mock's buffer overflows during a power cut, additional samples may be lost by the mock itself and will be reported in the `lost` field on recovery.*

---

## 5. Resource Use (Estimated, ARM64 Raspberry Pi 4)

| Metric | Estimated value |
|---|---|
| Average CPU | 3–8% (400 Hz × 2 sensors, Python) |
| Peak memory | < 80 MB (rolling 3 000-sample buffer + SQLite cache) |
| Disk write (1 h online) | ~50–80 MB (raw JSON samples + WAL) |
| Network sent (1 h online) | ~50–80 MB (same data re-sent to cloud) |

Measured figures using `scripts/measure_resources.py` are recorded in `resource_report.md`.

---


