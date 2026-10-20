# Interview Prep — Part 3: Component Deep Dives (Code-Level)

> Go through each module. Know every function, every line's purpose, every tradeoff.

---

## MODULE 1: `reader.py` — The TCP Reader

### What it does
One `SensorReader` thread per sensor. Continuously pulls data from the mock via newline-delimited JSON over TCP. Validates, delivers, handles faults.

### Key design decisions

**1. Why daemon threads?**
```python
super().__init__(daemon=True, name=f"reader-s{sensor}")
```
Daemon threads are automatically killed when the main thread exits. This means if the pipeline receives SIGTERM and calls `sys.exit(0)`, we don't need to explicitly join these threads — they die with the process. For long-running IoT processes, this prevents hang-on-exit.

**2. The `_next_index` pointer**
```python
self._next_index: Optional[int] = None
```
This is the state that makes re-reads possible. After a successful read, we advance it: `self._next_index = first_index + count`. If a CRC failure occurs, we do NOT advance it. On the next request:
```python
cmd = {"cmd": "read", "sensor": self.sensor}
if self._next_index is not None:
    cmd["from_index"] = self._next_index
```
This requests the mock to replay from the last known good index.

**3. The receive buffer `self._rbuf`**
```python
self._rbuf = b""
```
TCP is a stream protocol — there's no guarantee one `recv()` call delivers exactly one JSON line. The buffer accumulates bytes until a `\n` is found:
```python
def _recv_line(self) -> str:
    while b"\n" not in self._rbuf:
        chunk = self._sock.recv(65536)
        if not chunk:
            raise ConnectionResetError("Mock closed connection")
        self._rbuf += chunk
    line, self._rbuf = self._rbuf.split(b"\n", 1)
    return line.decode()
```
After a `split(b"\n", 1)`, any leftover bytes stay in `_rbuf` for the next call. This is a classic streaming parser pattern.

**4. Why reconnect on CRC failure instead of just re-sending?**
The mock is "one client at a time". If CRC fails mid-connection, the mock's send buffer may be in an inconsistent state (it may have already moved its internal pointer). The safest recovery is: close the TCP connection, reopen it, and send a fresh `from_index` request. This forces the mock to start a clean request-response cycle.

**5. Connection error vs unexpected error**
```python
except (ConnectionRefusedError, OSError) as exc:
    log.warning(...)   # expected — network hiccup
    self._close()
    time.sleep(1.0)
except Exception as exc:
    log.exception(...)   # unexpected — log full traceback
    self._close()
    time.sleep(1.0)
```
`ConnectionRefusedError` and `OSError` are normal network events — the mock may not be running yet, or temporarily unavailable. We log at WARNING. Any other exception (bug in our code) is logged at EXCEPTION (includes traceback) so we can debug it.

---

## MODULE 2: `store.py` — SQLite Local Storage

### Schema design

```sql
samples (session_id, sensor, idx, values_json, synced)
-- PK: (session_id, sensor, idx)
-- synced: 0=unsynced, 1=uploaded to cloud

losses (id, session_id, sensor, from_index, to_index, reason, synced)
-- PK: id
-- synced: 0=unsynced, 1=uploaded to cloud
```

**Why store values as JSON text, not as separate columns?**

Sensor 1 has 3 channels, sensor 2 has 2 channels. We could have columns `ch1, ch2, ch3` but:
- We'd need NULL for ch3 for sensor 2 rows.
- The format `[[720, 1200, 1560], ...]` is already what the export needs.
- JSON text is schema-flexible: if a future sensor has 4 channels, we change nothing.
- Downside: can't query individual channel values with SQL. But we never need to — processing happens in Python, not SQL.

**Why partial indexes on `synced`?**
```sql
CREATE INDEX IF NOT EXISTS idx_samples_unsynced
    ON samples(session_id, synced)
    WHERE synced = 0;

CREATE INDEX IF NOT EXISTS idx_losses_unsynced
    ON losses(session_id, synced)
    WHERE synced = 0;
```
A *partial index* only indexes rows where `synced = 0`. Since synced rows are the vast majority (after catch-up), a full index on `synced` would waste space on rows we never query by that condition. The partial index is small, fast, and exactly covers the `WHERE synced = 0` query pattern for both samples and loss records.

**Why `INSERT OR IGNORE` not `INSERT OR REPLACE`?**

For samples: `INSERT OR IGNORE` — if the row already exists (from a re-delivery by the mock, or a duplicate batch from a crashed upload cycle), we silently keep the original. We trust the first write.

For windows: `INSERT OR REPLACE` — window results can be re-processed (e.g., if we fix a bug and re-run). We want the latest result to overwrite.

**Thread safety:**
```python
_lock = threading.Lock()

def save_samples(conn, ...):
    with _lock, conn:   # acquires both _lock and conn's internal transaction
        conn.executemany(...)
```
`with conn:` in SQLite Python begins a transaction and auto-commits on success, rolls back on exception. Combined with the module-level `_lock`, this ensures only one thread writes at a time.

### The sync lifecycle (Samples & Losses)

```
New sample or loss arrives:
  INSERT OR IGNORE (synced=0)          ← saved locally, unsynced

Syncer reads batch:
  SELECT WHERE synced=0 LIMIT 2000    ← read unsynced batch

Syncer uploads to cloud:
  HTTP POST → cloud ACK               ← uploaded (samples, windows, losses)

Syncer marks synced:
  UPDATE SET synced=1                 ← marked locally

If crash between upload and mark:
  synced stays 0                      ← re-uploaded on restart (at-least-once)
  cloud does INSERT OR IGNORE         ← no duplicate
```
> **Critical Viva Point**: Why sync losses? Criterion 6 compares `pipeline export --from cloud` with ground truth. If losses were only kept on the device, the cloud copy would lack loss records, failing the comparison. Both samples and loss records must be synchronized!

---

## MODULE 3: `processor.py` — Signal Processing

### Window definition
Window w = sample indexes 400w to 400w+399 of each sensor.

```python
RATE = 400          # samples per second
WINDOW_SIZE = 400   # samples per 1-second window
# So window w covers time [w, w+1) seconds from session start
```

### Peak detection — all 4 conditions explained

```python
def find_peaks(samples: list) -> list:
    for i, val in enumerate(samples):
        # Condition 1: context requirement
        if i < 800 or i + 1 >= len(samples):
            continue
```
**Condition 1:** We need 800 samples before (for Condition 3) and 1 sample after (for Condition 2). If either is unavailable, we can't evaluate this candidate.

```python
        # Condition 2: local maximum
        if not (val > samples[i - 1] and val > samples[i + 1]):
            continue
```
**Condition 2:** Strict local maximum — value must be larger than both immediate neighbours. This eliminates plateaus and ascending runs.

```python
        # Condition 3: significance threshold
        window_800 = samples[i - 800: i]
        max_800 = max(window_800)
        if val <= max_800 / 2:
            continue
```
**Condition 3:** The value must exceed half the maximum of the preceding 2 seconds (800 samples at 400 Hz). This filters out small noise peaks — only "significant" peaks count. This is an adaptive threshold, not a fixed one, so it works at different signal amplitudes.

```python
        # Condition 4: refractory period
        if prev_peak_local >= 0 and (i - prev_peak_local) < 100:
            continue
```
**Condition 4:** At least 100 samples (0.25 seconds) after the previous peak. This enforces a physiological refractory period — a human heart cannot beat faster than 240 bpm (0.25s interval). Prevents double-counting a single QRS complex.

### Heart rate formula & Historical Context

```python
def compute_heart_rate(window_w: int, ch2_samples: dict) -> Optional[float]:
    if window_w < 4:
        return None

    start = RATE * (window_w - 4)   # look-back 4 windows = 4 seconds
    end   = RATE * (window_w + 1)   # include current window = 1 second
    # Total window range to evaluate peaks: 5 seconds = 2000 samples

    # Check all 2000 samples exist in the 5-second lookback.
    for idx in range(start, end):
        if idx not in ch2_samples:
            return None

    # Scan for peaks starting from prior history to establish refractory state
    scan_start = max(800, start - 800)
    last_peak = -999999
    peaks_in_window = []

    for i in range(scan_start, end):
        # 1. 800 samples before & 1 after available
        # 2. Strict local maximum
        # 3. > max_800 / 2
        # 4. Refractory: i - last_peak >= 100
        ...
        if start <= i < end:
            peaks_in_window.append(i)
```

**Why scan starting from `max(800, start - 800)`?**
If you slice only the 2000-sample window (`[start:end]`), any sample with index `< 800` inside that slice would be skipped because of Condition 1! That would throw away peaks in the first 2 seconds ($400(w-4)$ to $400(w-4)+799$) of the 5-second window, even when those samples had valid 800-sample history. By scanning from `start - 800`, we ensure:
1. Every candidate peak $i \in [start, end-1]$ is tested against its true prior 800 samples.
2. Refractory state (`i - last_peak >= 100`) is tracked across the boundary.
3. All peaks in the 5-second window are captured, matching the judging reference implementation.

**Why 5 seconds (2000 samples) look-back?**
At 60 bpm, one beat every 1 second → only 1 beat in a 1-second window → need at least 2 peaks to compute a gap → need at least 2 seconds. But at 60 bpm, 2 seconds = 2 peaks = 1 gap = unstable estimate. 5 seconds = 5 peaks = 4 gaps = much more stable average. The spec chose 5 seconds for robustness.

**Why return null if ANY sample in the 5-second window is missing?**
A missing sample means we don't know if a peak occurred there. A peak near a missing region could be counted as two partial peaks, inflating the heart rate. Rather than guess, we return null. This is "honest" processing — the spec explicitly requires null on any missing sample.

**Heart rate calculation:**
```python
peak_times = [p / RATE for p in peaks_in_window]
# peak_times in seconds since session start
gaps = [peak_times[j+1] - peak_times[j] for j in range(len(peak_times)-1)]
mean_gap = sum(gaps) / len(gaps)
heart_rate = round(60.0 / mean_gap, 1)   # bpm
```
Mean gap in seconds → beats per minute = 60 / mean_gap. 1 decimal place rounding.

### Magnitude formula

```python
# Over available sensor 2 samples only
mag_vals.append(math.sqrt(c1 ** 2 + c2 ** 2))
magnitude = sum(mag_vals) / len(mag_vals) if mag_vals else None
```
This is Euclidean magnitude: √(ch1² + ch2²) for each sample, then average. The spec says "over the available sensor 2 samples" — so we don't require completeness, we use what we have.

---

## MODULE 4: `network.py` — Network Monitor

### Probe choice rationale

```python
PROBE_HOST = "8.8.8.8"   # Google Public DNS
PROBE_PORT = 53           # DNS
```

**Why DNS (port 53)?**
- TCP port 53 is almost never firewalled on hospital networks (DNS is required for normal operation).
- ICMP ping would be cleaner but is often blocked by hospital network security policies.
- HTTP to an external URL would work but adds DNS resolution overhead (ironic — checking DNS by doing DNS).

**Why not probe the cloud server directly?**
- If we probe our cloud server, we're testing both network AND cloud availability. If the cloud is down but network is up, we'd falsely switch to offline and stop uploading.
- Probing a third-party reliable endpoint (8.8.8.8) purely tests connectivity.

### Hysteresis explained

Without hysteresis: one failed probe → switch offline → one successful probe → switch online → repeat. This "chattering" would cause the mode to flip every 2 seconds on a flaky connection, making the system useless.

With 5-probe hysteresis:
- A single packet loss doesn't trigger a switch.
- A genuine outage takes 10 seconds to detect (acceptable per spec).
- Recovery also takes 10 seconds after the network is truly stable.

---

## MODULE 5: `syncer.py` — Cloud Upload Thread

### The `_mode_changed` Event

```python
self._mode_changed = threading.Event()

def set_mode(self, mode: str):
    with self._mode_lock:
        self._mode = mode
    self._mode_changed.set()   # wake the syncer immediately

def run(self):
    while not self.stop_event.is_set():
        if self.mode == "online":
            self._upload_cycle(session_id)
        self._mode_changed.wait(timeout=UPLOAD_INTERVAL)
        self._mode_changed.clear()
```

**Why `Event.wait(timeout=1.0)` instead of `time.sleep(1.0)`?**
- `time.sleep(1.0)` cannot be interrupted. If mode changes to online, we'd wait up to 1 second before starting uploads.
- `Event.wait(timeout=1.0)` either wakes immediately (when `set_mode` is called) or after 1 second. This means mode transitions take effect within milliseconds, not 1 second.

### Triple upload cycle (Samples, Windows, Losses)
```python
def _upload_cycle(self, session_id: str):
    self._upload_samples(session_id)
    self._upload_windows(session_id)
    self._upload_losses(session_id)
```
- **Samples**: Pushed in batches of 2000 (`SAMPLE_BATCH`).
- **Windows**: Pushed in batches of 200 (`WINDOW_BATCH`).
- **Losses**: Unsynced loss records are pulled via `store.get_unsynced_losses()` and posted to the cloud. Upon 200 ACK, marked locally as `synced=1`. This guarantees that the cloud has full loss visibility for Criterion 6 ground truth comparison!

---

## MODULE 6: `engine.py` — The Coordinator

### Why `_evict_old` keeps 5000 samples

```python
@staticmethod
def _evict_old(buf: dict, latest_index: int, keep: int = 5000):
    if len(buf) > keep:
        cutoff = latest_index - keep
        stale = [k for k in buf if k < cutoff]
        for k in stale:
            del buf[k]
```

**Without eviction:** At 400 samples/second, after 1 hour we'd have 1,440,000 entries in `self._s1`. Each entry is a list of 3 integers ≈ 100 bytes in Python. Total: ~144 MB per sensor. Two sensors = ~288 MB.
**With eviction at 5000 samples:** Memory stays bounded under ~500 KB per sensor regardless of session length. 5000 is chosen to cover:
1. 2000 samples for the 5-second window lookback ($400(w-4)$ to $400(w+1)-1$).
2. 800 prior samples context needed for adaptive threshold peak detection ($i-800$).
3. 2200 samples safety buffer for delayed or re-read packets.

### Inter-Process Mode Communication (`pipeline.mode`)
```python
def _write_mode_file(self, mode_str: str):
    self._mode_file.write_text(mode_str)
```
Whenever the engine starts, switches mode (`online` ↔ `offline`), or terminates, it atomically updates `data/pipeline.mode`. This allows external inspection tools like `pipeline status` to report the true real-time engine state without requiring heavy inter-process RPC.

### `_try_process_windows` — the processing trigger

```python
def _try_process_windows(self, session_id: str):
    while True:
        with self._window_lock:
            w = self._next_window

        w_start = WINDOW_SIZE * w
        w_end   = w_start + WINDOW_SIZE

        with self._s1_lock:
            s1_max = max(self._s1.keys()) if self._s1 else -1
        with self._s2_lock:
            s2_max = max(self._s2.keys()) if self._s2 else -1

        if s1_max < w_end - 1 and s2_max < w_end - 1:
            break   # not enough data
```

**Why check BOTH sensors?** The condition `s1_max < w_end - 1 AND s2_max < w_end - 1` means: stop if BOTH sensors haven't received data past the window end yet. We process with whatever is available from each sensor (missing samples show up as `missing_N > 0`).

**The snapshot pattern:**
```python
with self._s1_lock:
    s1_snap = dict(self._s1)
```
We take a shallow copy of the dict under the lock, then release. Processing runs without holding the lock. This ensures signal processing never blocks the high-throughput TCP reader threads.

---

## MODULE 7: `cloud.py` — The Backend Abstraction

### Why an abstraction?

```python
def get_backend():
    if _BACKEND == "supabase":
        return SupabaseBackend()
    elif _BACKEND == "http":
        return HttpBackend()
    elif _BACKEND == "noop":
        return NoopBackend()
```

By abstracting the backend behind a common interface (`upload_samples`, `upload_windows`, `upload_losses`, `get_sample_count`, `get_all_windows`, `get_export`), the rest of the code doesn't need to know which cloud is configured.

**Export Support:**
All backends implement `get_export(session_id) -> dict`, retrieving `{"samples": [...], "losses": [...], "windows": [...]}` so that `pipeline export --from cloud` outputs data identical to the device export.

---

## MODULE 8: `cli.py` — The Command-Line Interface

The assessment requires one unified binary `pipeline` exposing 3 primary commands:

### 1. `pipeline status`
* Reads `data/pipeline.pid` to check if the process is alive.
* Reads `data/pipeline.mode` for live state (`online`, `offline`, `stopped`).
* Queries local SQLite database for `samples_saved`, `samples_in_cloud`, and `backlog`.
* Outputs exactly one JSON line:
```json
{"mode": "online", "samples_saved": 120000, "samples_in_cloud": 120000, "backlog": 0, "running": true, "session_id": "5f0c2a9e"}
```

### 2. `pipeline export --session ID --from device|cloud`
* Streams newline-delimited JSON Lines (JSONL).
* Format: One line per sample `{"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}`, followed by one line per loss `{"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "..."}`.
* Both `--from device` and `--from cloud` yield this exact format, allowing direct verification by `check_ground_truth`.

### 3. `pipeline results --session ID`
* Outputs one JSON line per processed 1-second window containing `window`, `missing_1`, `missing_2`, `complete`, channel stats (`mean`, `min`, `max`), vector `magnitude`, `heart_rate`, and `mode`.
