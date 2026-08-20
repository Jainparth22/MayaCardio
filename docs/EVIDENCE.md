# Evidence

Every number here was produced by a command in this repository. Each entry says
**where it was measured** and **what it does not show**. Nothing is estimated.

Environment for all results below: Windows 11 development PC, Python 3.14,
loopback networking (device, cloud and gateway on one machine, as separate
processes unless noted). Section 7 was measured in a Linux container limited to
the Raspberry Pi 4 envelope (4 CPUs, 2 GB). **No physical Raspberry Pi has been
used** — see "Still to do".

## 1. Automated tests

`python -m pytest tests -q` → **40 passed** (~115 s).

| File | Covers |
|---|---|
| `test_processor.py` | heart rate follows the 60/90/72 bpm schedule; streaming detector equals a literal implementation of the rule (incl. across data gaps); missing/lost handling; windows wait for both sensors; duplicates ignored; crash-replay equals uninterrupted run; arrival-order independence |
| `test_store.py` | idempotent inserts; values round-trip unchanged; survives reopen without graceful shutdown; idempotent losses (incl. open-ended); sync flags across sessions; one row per window and mode |
| `test_reader.py` | CRC over raw text; gap arithmetic; exactly-once under 20 % corrupt + 20 % duplicate replies; gateway-reported losses; **unreported gap is never silent**; resume from stored cursor; **storage failure never skips data** |
| `test_network.py` | hysteresis (no flapping); recovery; worst-case switch time < 10 s |
| `test_cloud.py` | online windows as data arrives; idempotent ingest; device offline results kept; **cloud restart fills the hole before offline results**; final window on session end |
| `test_restart_gap.py` | gateway restart while the pipeline is stopped; late-seen and unreported missing start of a session; verifier reads PowerShell-encoded files |
| `test_end_to_end.py` | whole system: complete + identical cloud copy, cloud results equal device shadow results; outage → offline results → catch-up; crash + restart ⇒ no loss, every window exactly once; **gateway restart** ⇒ old session's loss recorded, new session complete |

## 2. Chaos test — real processes, hard kills

`python scripts/chaos_test.py` (simulator at 2× speed, 160 simulated seconds,
three processes + the harness).

Timeline injected (wall seconds): cloud killed at 22.5 → restarted at 47.5;
pipeline hard-killed at 55.0 → restarted at 56.5; hard-killed again at 64.0 →
restarted at 71.0. Gateway faults throughout: 3 % corrupt replies, 3 %
duplicate replies, a 12 s stall, an 8 s dropout of sensor 2.

Result of the final run on the final code (`VERDICT: PASS`; counts vary slightly
run to run with scheduling jitter, the pass criteria do not):

| Check | Result |
|---|---|
| Wrong values (every stored sample recomputed from ground truth) | **0** of 118,656 |
| Duplicate samples | **0** |
| Silent loss (index neither stored nor listed as lost) | **0** |
| Lost samples, all listed with a reason | 9,344 (`buffer_overflow` 6,144; `sensor_dropout` 3,200) |
| Cloud export vs device export | **identical** |
| Window results | 160 / 160 (118 `online`, 42 `offline`) |
| Final status | mode online, backlog 0 |

Reading the numbers: the `buffer_overflow` losses come from the 12 s stall and
from the second kill, where the restart was deliberately slower than the
gateway's 10 s buffer. The first kill (restart after 1.5 s) lost nothing. The
point of the test is not that nothing is lost — some loss is physically
unavoidable — but that **every loss is accounted for**.

Per-event detail from the pipeline log of that run: after kill #1 the engine
logged `recovered session … resuming at window 109` and **no loss record
follows it**; after kill #2 it logged `resuming at window 127` followed by two
`buffer_overflow` losses (sensor 1: 1,985 samples, sensor 2: 2,186 ≈ 5 s of
data = the 14 s restart gap minus the gateway's 10 s buffer). The 12 s stall
produced two further `buffer_overflow` records (986 and 987 samples).

An earlier run of this test found a real cloud bug (item 15 in the hardening
log): windows 44–54 were missing from the cloud copy while present on the device. A
whole-system restart test then found a further reader bug (item 18).

**What it does not show:** a hard process kill (`TerminateProcess` / `SIGKILL`)
exercises the software durability path but not the hardware one; it is not a
power cut.

The chaos test was re-run after the fix for hardening item 20 and passed again
(0 wrong, 0 duplicates, audit pass, cloud identical, 160 windows). The table above
shows the earlier run's counts.

## 3. Online latency

`python scripts/measure_latency.py --seconds 40` — simulator at real time (1×);
reference clock = the simulator's start; result time = `processed_at` stored by
the cloud.

| | median | p95 | worst |
|---|---|---|---|
| all 40 windows | 0.22 s | 0.50 s | 0.60 s |
| steady state (window ≥ 10) | 0.18 s | 0.40 s | 0.60 s |

Requirement: ≤ 5 s. **Not shown:** real network round-trip time and a real
WAN; loopback removes it. The pipeline's own contribution is bounded by the
0.25 s poll interval plus one HTTP round trip.

## 4. Catch-up after an outage

`python scripts/bench_catchup.py --seconds 600` — 480,000 samples (10 min of
both sensors) drained by the real syncer into the real cloud service over
loopback with gzip, fsync'd cloud database, and online processing running.

| | |
|---|---|
| Throughput | ≈ 73,000 samples/s (6.6 s for 480,000) |
| Incoming rate while online | 800 samples/s |
| Projected, 2-hour outage (5.76 M samples) | ≈ 1.3 min |
| Requirement | ≤ 15 min |

The first version (1 s sleep between 2,000-row batches) drains at ≤ 2,000
samples/s → ≈ 80 min for the same backlog. **Not shown:** a Raspberry Pi's CPU
and a real uplink. Treat the Pi/WAN number as bandwidth-bound: at the
measured upload size (below) 5.76 M samples ≈ 130 MB, so even a 10 Mbit/s
uplink needs ≈ 2 min.

## 5. Storage and network volume

From a 60 s real-time run (48,000 samples), extrapolated linearly to one hour —
**extrapolation, not a 1-hour measurement**:

| | 60 s run | per hour (extrapolated) |
|---|---|---|
| Bytes uploaded (after gzip, counted in the client) | 364 KB | ≈ 22 MB |
| Device database at rest (after WAL checkpoint) | 3.4 MB (71 B/sample) | ≈ 103 MB |

The directory size observed during the run (6.8 MB, ≈ 410 MB/h) includes an
uncheckpointed write-ahead log, so it overstates data at rest; the **physical
write volume** (WAL + checkpoint + sync-flag updates) is what wears an SD card
and can only be measured on the target device with `scripts/measure_resources.py`.

## 6. Mode-switch time

By construction: `FAIL_THRESHOLD × (PROBE_TIMEOUT + PROBE_INTERVAL)` = 3 × 2 s = 6 s
(plus < 1 s probe alignment); `PASS_THRESHOLD` successes ≈ 3 s. A unit test
asserts both are < 10 s; two integration tests observe the real switch
(offline within 10 s of the cloud being unreachable; online again after it
returns). A stopwatch measurement against a real network cut is still to do.

## 7. Resource use in a Pi-class container

`sh scripts/measure_in_container.sh 600` — a Linux container (python:3.12-slim) limited
to **4 CPUs and 2 GB RAM** (the Raspberry Pi 4 envelope; confirmed from the container's
cgroup: `cpu.max 400000 100000`, `memory.max 2147483648`), on a Windows 11 PC. Simulator,
cloud service and pipeline all ran inside it; only the **pipeline process** was measured,
over 580 s, in online mode, at the real 400 Hz rate. This is **not** a Raspberry Pi: CPU
percentages are relative to this PC's cores and will be higher on a Pi's slower cores.

| Metric (pipeline process) | 580 s run | Per hour (extrapolated) |
|---|---|---|
| Average CPU | **1.59 %** of one core (never above 2.0 % in any 10 s interval) | — |
| Peak memory (RSS) | **32.5 MB** | — |
| Bytes uploaded (gzip, counted in client) | 3.8 MB | ≈ 24 MB |
| Database size at end | 33.9 MB (480,000 samples) | ≈ 210 MB (grows with sessions length) |
| **Data physically written to disk** | **315.9 MB** | **≈ 1.96 GB** |

Correctness in the same run: 480,000 samples stored (240,000 per sensor), **0 wrong values,
0 duplicates, 0 silent losses**, backlog 0, and the cloud export had the same 480,000 lines
as the device export.

**The disk-write figure is the main weakness this run exposed.** The process wrote ≈ 9× more
bytes than the data it stored. Causes: `synchronous=FULL` flushes whole 4 KB pages on every
small commit, the write-ahead log is written and later checkpointed (data written twice), and
every row is updated again when it is marked `synced`. On an SD card ≈ 2 GB/hour matters for
wear. Planned fix (roadmap): commit once per second, packed per-second sample blocks, and a
`synced` high-water mark instead of per-row flags. Not yet done.

## Still to do (not yet evidence)

1. Physical Raspberry Pi 4 (or a free ARM64 cloud VM) run: 1 h online with `scripts/measure_resources.py`. The 4-CPU/2 GB container run above is the stand-in.
2. Real **power-pull** test on the Pi (the hardware path), then `beatkeeper.verify` on the export.
3. Real network cut with a stopwatch (`iptables` / unplug) and a 2-hour outage on a real link.
4. Cloud deployed on a free VM, tested over the public internet behind TLS.
