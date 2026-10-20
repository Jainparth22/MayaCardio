# MayaCardio Biosignal Pipeline

Never Lose a Heartbeat — device-side data pipeline for Raspberry Pi + cloud.

---

## Quick start (fresh Raspberry Pi OS)

```bash
# 1. Clone the repository
git clone <repo-url> pipeline && cd pipeline

# 2. Python 3.10+ is included in Raspberry Pi OS Bullseye/Bookworm
python3 --version   # should print 3.10+

# 3. (Optional) create a venv
python3 -m venv .venv && source .venv/bin/activate

# 4. No extra packages needed for the device pipeline (stdlib only)
#    For the cloud server or tests:
pip install flask pytest          # only on the cloud VM / dev machine

# 5. Download the mock service (ARM64 binary provided by MayaCardio)
chmod +x biosignal-mock
./biosignal-mock --mode signal --seed 7 --duration 3600 --port 7100 &

# 6. Configure cloud backend (choose one)
export CLOUD_BACKEND=http
export CLOUD_HTTP_URL=http://<your-cloud-vm-ip>:8080
# or: export CLOUD_BACKEND=supabase
#     export SUPABASE_URL=https://<project>.supabase.co
#     export SUPABASE_KEY=<anon-key>
# or: export CLOUD_BACKEND=noop   # local only, no upload

# 7. Start the pipeline
python3 pipeline/cli.py run
```

---

## Commands

```bash
# Status (one JSON line)
python3 pipeline/cli.py status

# Export from device
python3 pipeline/cli.py export --session <SESSION_ID> --from device

# Export from cloud
python3 pipeline/cli.py export --session <SESSION_ID> --from cloud

# Window results
python3 pipeline/cli.py results --session <SESSION_ID>
```

If installed as a package (`pip install -e .`), use `pipeline` directly:

```bash
pipeline status
pipeline export --session 5f0c2a9e --from device
pipeline results --session 5f0c2a9e
```

---

## Cloud server (self-hosted)

Run on any free-tier Linux VM (Oracle Always Free ARM64 is ideal):

```bash
pip install flask
CLOUD_DB=cloud_data.db python3 cloud_server/cloud_server.py
```

The server listens on port 8080 by default (`PORT` env var to change).

### Supabase (alternative)

Create three tables in Supabase with the SQL below, then set
`CLOUD_BACKEND=supabase`, `SUPABASE_URL`, and `SUPABASE_KEY`.

```sql
create table samples (
  session_id text, sensor int, idx int,
  values_json text,
  primary key (session_id, sensor, idx)
);
create table windows (
  session_id text, window_index int,
  result_json text, mode text,
  primary key (session_id, window_index)
);
create table losses (
  id serial primary key,
  session_id text, sensor int,
  from_index int, to_index int, reason text
);
```

---

## Running tests

```bash
# Unit tests (no mock service needed)
python3 tests/test_processor.py
python3 tests/test_store.py

# Integration tests (spins up its own TCP stub)
python3 tests/test_integration.py

# Or with pytest
pytest tests/
```

---

## Resource measurement (criterion 8)

```bash
# Run while the pipeline is active; redirects CSV to a file
python3 scripts/measure_resources.py --duration 3600 --interval 10 \
  --iface eth0 --dev mmcblk0 > resource_log.csv 2>resource_summary.txt
cat resource_summary.txt
```

---

## Failure tests (criteria 3 & 5)

```bash
# Start pipeline first
python3 pipeline/cli.py run &
sleep 30   # let it collect some samples

# Run tests (power-cut test requires sudo for SIGKILL)
sudo python3 scripts/run_failure_tests.py
```

Network-cut test requires root (uses `iptables`). On a device where you
cannot use iptables, simulate a network cut by unplugging the Ethernet cable
or disabling WiFi:

```bash
sudo ip link set eth0 down
sleep 15
sudo ip link set eth0 up
```

---

## Architecture overview

```
biosignal-mock (TCP :7100)
      │
      ├── SensorReader(1) ──┐
      └── SensorReader(2) ──┤
                            ▼
                        Engine._on_data
                            │
                     ┌──────┴──────────────────┐
                     ▼                         ▼
              store.save_samples        in-memory buffer
              (SQLite WAL)             (rolling 3000 samples)
                                             │
                                    _try_process_windows
                                             │
                                    processor.process_window
                                             │
                                    store.save_window
                                             │
                                          Syncer
                                    (if online: upload)
                                             │
                                      CloudBackend
```

**Mode switching:**  
`NetworkMonitor` probes `8.8.8.8:53` every 2 s.  
- 5 consecutive failures → offline (≤ 10 s).  
- 5 consecutive successes → online (≤ 10 s).  
- `Syncer.set_mode()` stops/starts uploads immediately.

**Data safety:**  
Every sample is written to SQLite **before** any processing or upload.  
SQLite WAL + `synchronous=FULL` ensures committed data survives power cuts  
(at most the last unflushed transaction, typically < 0.1 s at 400 Hz).

---

## File layout

```
pipeline/
├── pipeline/               Device-side package
│   ├── cli.py              `pipeline` command entry point
│   ├── engine.py           Central coordinator
│   ├── reader.py           Mock-service TCP reader threads
│   ├── store.py            SQLite local storage (WAL)
│   ├── processor.py        Windowed signal processing + heart rate
│   ├── network.py          Network availability monitor
│   ├── syncer.py           Cloud upload thread
│   └── cloud.py            Pluggable cloud backends
├── cloud_server/
│   └── cloud_server.py     Self-hosted Flask server (cloud side)
├── tests/
│   ├── test_processor.py   Unit tests: processing logic
│   ├── test_store.py       Unit tests: SQLite store
│   └── test_integration.py Integration tests: TCP stub + reader
├── scripts/
│   ├── measure_resources.py  CPU/mem/disk/net measurement
│   └── run_failure_tests.py  Process-kill / power-cut / network-cut tests
├── data/                   Created at runtime (DB, PID file)
├── pyproject.toml
└── requirements.txt
```

---

## Known limitations

- One reader thread per sensor: the mock allows one client at a time, so
  both readers share the port sequentially. Each re-reads from its last
  known good index after a corrupt reply or reconnect.
- Resource figures are environment-specific; see `resource_report.md`.
- The network monitor uses DNS (port 53) to detect connectivity; some
  restrictive networks may block this.

---


