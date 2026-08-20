# BeatKeeper — never lose a heartbeat

**A crash-safe, offline-first pipeline for continuous biosignal capture.**
It records two sensors at 400 Hz on a small Linux device, keeps every sample
safe through crashes and power cuts, processes data in the cloud when the
network is up and on the device when it isn't, and *proves* afterwards that
nothing was lost or silently altered.

Zero dependencies — Python standard library only, on the device and in the cloud.

```
 sensor gateway ──► Reader ──► SQLite (WAL, fsync) ──► SessionProcessor ──► results
 (2 sensors,        CRC check,    every sample lands      (same code runs on       │
  400 Hz,           gap check,    on disk FIRST           device AND in cloud)     │
  faulty on         resume                │                                        │
  purpose)                                ▼                                        ▼
                              NetworkMonitor ─► mode ─► Syncer ══ gzip HTTP ══► Cloud service
                              (probes the cloud,      (idempotent,              (stores + computes
                               hysteresis, <10 s)      drains backlog fast)      online results)
```

## Why this is hard (and why I built it)

A bedside device in a hospital loses WiFi without warning, gets unplugged, and
nobody is watching it. A gap in a heart recording can matter clinically, and a
*silent* gap is worse than a visible one. So the system has to be able to make
three promises and keep them:

1. **Every sample the device received is on disk, exactly once, unchanged.**
2. **Every sample that is *not* on disk is listed as lost, with a reason.** Never a silent gap, never an invented value.
3. **After any outage the cloud ends up with a complete copy**, and results are identical whether they were computed on the device or in the cloud.

## Proof, not claims

The repository ships its own adversary. A built-in sensor simulator injects
corrupt replies, duplicate replies, stalls, sensor dropouts and restarts; a
chaos test hard-kills processes and cuts the network; a verifier then audits
the result against ground truth.

Latest chaos run (160 s session, two sensors, real separate processes):

| Event injected | Outcome |
|---|---|
| Cloud killed for ~25 s of wall time (network cut) | device switched to offline processing; 42 windows computed on-device |
| Cloud restored | backlog drained, cloud copy completed automatically |
| Pipeline hard-killed, restarted 1.5 s later | resumed from the last stored sample — **0 samples lost** |
| Pipeline hard-killed, restarted 7 s later (too late for the gateway's buffer) | the overflow was **detected and recorded as a loss with a reason** |
| 3 % corrupt replies, 3 % duplicate replies, a 12 s stall, an 8 s sensor dropout | all absorbed or recorded |

Audit of the device export against ground truth: **0 wrong values, 0 duplicates, 0 silent losses**;
9,344 lost samples, every one listed with a reason; cloud export **identical** to the device export;
160 / 160 window results present (118 computed online in the cloud, 42 offline on the device).

Other measurements (details and caveats in [docs/EVIDENCE.md](docs/EVIDENCE.md)):

| | Result |
|---|---|
| Online result latency (data produced → result in cloud) | median 0.22 s, worst 0.60 s (loopback; requirement 5 s) |
| Catch-up after an outage | ~73,000 samples/s → a 2-hour outage (5.76 M samples) in ≈ 1.3 min (loopback; requirement 15 min) |
| Mode switch | bounded by construction at ≈ 6–7 s to offline and ≈ 3 s back online (requirement 10 s); asserted in tests |
| Resources (4-CPU / 2 GB container, 10 min, online) | 1.6 % avg CPU · 32.5 MB peak RAM · 24 MB/h upload · 480,000 samples, 0 silent losses |
| Tests | 40 automated tests, including whole-system runs |

## Quick start

Requires Python 3.10+. No installation of third-party packages.

```bash
git clone <this-repo> && cd beatkeeper
pip install -e .                   # optional: gives you the `beatkeeper` commands
export PYTHONPATH=src              # (PowerShell: $env:PYTHONPATH="src") if you skip the install

# terminal 1 — a faulty two-sensor gateway
python -m beatkeeper.simulator --duration 600 --corrupt-rate 0.02 --dup-rate 0.02

# terminal 2 — the cloud service (any free VM; stdlib only)
BEATKEEPER_API_KEY=secret python -m beatkeeper.cloud_server --port 8080

# terminal 3 — the device pipeline
export BEATKEEPER_CLOUD_URL=http://127.0.0.1:8080 BEATKEEPER_API_KEY=secret
python -m beatkeeper run

# terminal 4 — inspect, then break things (kill the cloud, kill -9 the pipeline…)
python -m beatkeeper status
python -m beatkeeper export  --session latest --from device > device.ndjson
python -m beatkeeper export  --session latest --from cloud  > cloud.ndjson
python -m beatkeeper results --session latest --from cloud | head -3
python -m beatkeeper.verify --export device.ndjson --samples 240000    # (600 s × 400 Hz)
```

**Windows PowerShell** (four separate windows, each starting in the project folder):

```powershell
# 1 simulator
$env:PYTHONPATH="src"; python -m beatkeeper.simulator --duration 600 --corrupt-rate 0.02
# 2 cloud
$env:PYTHONPATH="src"; $env:BEATKEEPER_API_KEY="secret"; python -m beatkeeper.cloud_server --port 8080 --host 127.0.0.1
# 3 device pipeline
$env:PYTHONPATH="src"; $env:BEATKEEPER_API_KEY="secret"; $env:BEATKEEPER_CLOUD_URL="http://127.0.0.1:8080"; python -m beatkeeper run
# 4 you
$env:PYTHONPATH="src"; python -m beatkeeper status
```

Without `BEATKEEPER_CLOUD_URL` it runs local-only: data is still captured and
processed (permanently in offline mode).

### Commands

| Command | Output |
|---|---|
| `beatkeeper run [--gateway HOST:PORT]` | start collecting (foreground; Ctrl-C to stop) |
| `beatkeeper status` | one JSON line: mode, samples saved, samples in cloud, backlog |
| `beatkeeper export --session ID\|latest --from device\|cloud` | one JSON line per sample, then one per loss |
| `beatkeeper results --session ID\|latest [--from cloud]` | one JSON line per window and mode |
| `python -m beatkeeper.verify …` | audit an export against simulator ground truth |

Export format: `{"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}` per sample, then
`{"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "buffer_overflow"}` per loss.

### Run the tests and the chaos test

```bash
pip install pytest
python -m pytest tests -q             # 40 tests, ~115 s
python scripts/chaos_test.py          # real processes, hard kills, ~100 s
python scripts/bench_catchup.py       # catch-up throughput
python scripts/measure_latency.py     # online latency, real time
```

### Deploy the cloud service

Docker Compose with automatic HTTPS: [docs/DEPLOY.md](docs/DEPLOY.md).

## Design decisions worth knowing

* **Disk first.** A sample is committed (fsync'd) before anything else touches it. SQLite in WAL mode with `synchronous=FULL`.
* **Idempotency everywhere.** Primary keys on `(session, sensor, index)` make every retry harmless. Retries + idempotent writes give exactly-once *effect* without exactly-once delivery.
* **One processing core, two homes.** `SessionProcessor` runs on the device (offline) and inside the cloud service (online). "Same logic online and offline" is true by construction — and a test checks cloud results equal the device's shadow results.
* **Watermarks, not timers.** A window is emitted only when both sensors are *resolved* past its end (each sample present or explicitly lost), so a window is never computed from data that simply hasn't arrived yet.
* **Loss is data.** Losses are stored, synced and exported like samples; unreported gaps are detected by index arithmetic.
* **Probe the thing you depend on.** The network monitor asks the *cloud* if it is reachable, not a public DNS server, and uses hysteresis so a flaky link doesn't flap the mode.
* **Streaming signal processing.** Peak detection uses a monotonic-deque sliding maximum (O(1) per sample) instead of rescanning 2,000 × 800 samples per second. A test proves it matches a literal implementation of the rule, including across data gaps.

Full architecture, guarantees and the hardening log: [docs/DESIGN.md](docs/DESIGN.md).

## Honest limitations

* A power cut can lose the in-flight batch (≤ one read, ≈ 0.25–1 s) — *unless* the gateway still holds it (10 s buffer), in which case restart recovers it automatically. Data produced while the device is off cannot be captured by anyone.
* `fsync` guarantees depend on the storage hardware honouring it; some SD cards lie. A true power-pull test on a Raspberry Pi has **not yet been run** (the chaos test uses hard process kills, which exercise the software path, not the hardware).
* I have no Raspberry Pi. Resource figures were measured in a Linux container limited to Pi 4 resources (4 CPUs, 2 GB) on a PC: **1.6 % average CPU, 32.5 MB peak memory, ≈ 24 MB/hour uploaded**. CPU is relative to the PC's cores, so expect higher on a Pi.
* **Disk write volume is high**: ≈ 9× the stored data (≈ 2 GB/hour) because of per-commit fsync, the write-ahead log and per-row sync flags. Fine for an SSD, a wear concern for an SD card; reducing it is the top roadmap item.
* The cloud is a single-node service with a shared API key. It speaks plain HTTP itself; the [deployment setup](docs/DEPLOY.md) puts HTTPS in front of it, but it has no user accounts or rate limiting. No patient data is involved; everything is synthetic.
* Storage is ≈ 71 B/sample (≈ 103 MB/hour at rest). Packing samples into binary blocks would cut that ~3× at the cost of simplicity; deliberately not done yet.

## Project layout

```
src/beatkeeper/
  processor.py      streaming window + heart-rate core (shared device/cloud)
  store.py          durable SQLite store (WAL, idempotent, loss-aware)
  reader.py         gateway client: CRC, duplicates, gaps, resume
  engine.py         wiring, mode, crash recovery, status heartbeat
  network.py        online/offline decision with hysteresis
  syncer.py         at-least-once upload, fast catch-up
  cloud.py          device-side cloud client (gzip, API key)
  cloud_server.py   cloud service: ingest, online processing, NDJSON export
  simulator.py      faulty two-sensor gateway + deterministic ground truth
  verify.py         ground-truth auditor
  cli.py            `beatkeeper` command
tests/              unit, integration and whole-system tests
scripts/            chaos test, catch-up benchmark, latency, resource measurement
docs/               DESIGN · EVIDENCE · DEPLOY
Dockerfile          cloud service image (compose.yaml + Caddyfile add HTTPS)
```

## How this was built

Built iteratively with an AI assistant helping with formatting, review and
final presentation. Every number in this repository comes from running the code, and every
bug found along the way, including the ones the chaos test surfaced, is recorded
in the hardening log in [docs/DESIGN.md](docs/DESIGN.md).

MIT licensed.
