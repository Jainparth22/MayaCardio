# MayaCardio Pipeline — Submission Summary

## ✅ Test Results

| Suite | Tests | Status |
|---|---|---|
| `test_processor.py` | 16 | ✅ All pass |
| `test_store.py` | 10 | ✅ All pass |
| `test_integration.py` | 4 | ✅ All pass |
| **Total** | **30** | **✅ 30/30** |

---

## 📁 Files Created

| File | Purpose |
|---|---|
| [`pipeline/pipeline/cli.py`](file:///c:/Users/parth/OneDrive/Desktop/1610/pipeline/pipeline/cli.py) | `pipeline run / status / export / results` CLI |
| [`pipeline/pipeline/engine.py`](file:///c:/Users/parth/OneDrive/Desktop/1610/pipeline/pipeline/engine.py) | Central coordinator (wires all threads) |
| [`pipeline/pipeline/reader.py`](file:///c:/Users/parth/OneDrive/Desktop/1610/pipeline/pipeline/reader.py) | Mock TCP reader (CRC validation, re-read, session detect) |
| [`pipeline/pipeline/store.py`](file:///c:/Users/parth/OneDrive/Desktop/1610/pipeline/pipeline/store.py) | SQLite WAL storage (crash-safe) |
| [`pipeline/pipeline/processor.py`](file:///c:/Users/parth/OneDrive/Desktop/1610/pipeline/pipeline/processor.py) | Windowed processing + heart rate (exact spec) |
| [`pipeline/pipeline/network.py`](file:///c:/Users/parth/OneDrive/Desktop/1610/pipeline/pipeline/network.py) | Network monitor (≤ 10 s mode switch) |
| [`pipeline/pipeline/syncer.py`](file:///c:/Users/parth/OneDrive/Desktop/1610/pipeline/pipeline/syncer.py) | Cloud upload thread (at-least-once) |
| [`pipeline/pipeline/cloud.py`](file:///c:/Users/parth/OneDrive/Desktop/1610/pipeline/pipeline/cloud.py) | Pluggable backends: HTTP / Supabase / noop |
| [`pipeline/cloud_server/cloud_server.py`](file:///c:/Users/parth/OneDrive/Desktop/1610/pipeline/cloud_server/cloud_server.py) | Flask cloud receiver (free-tier compatible) |
| [`pipeline/tests/test_processor.py`](file:///c:/Users/parth/OneDrive/Desktop/1610/pipeline/tests/test_processor.py) | Processing unit tests |
| [`pipeline/tests/test_store.py`](file:///c:/Users/parth/OneDrive/Desktop/1610/pipeline/tests/test_store.py) | Store unit tests |
| [`pipeline/tests/test_integration.py`](file:///c:/Users/parth/OneDrive/Desktop/1610/pipeline/tests/test_integration.py) | End-to-end with TCP stub |
| [`pipeline/scripts/measure_resources.py`](file:///c:/Users/parth/OneDrive/Desktop/1610/pipeline/scripts/measure_resources.py) | CPU/mem/disk/net measurement (criterion 8) |
| [`pipeline/scripts/run_failure_tests.py`](file:///c:/Users/parth/OneDrive/Desktop/1610/pipeline/scripts/run_failure_tests.py) | Process-kill / power-cut / network-cut tests |
| [`pipeline/scripts/smoke_test_cli.py`](file:///c:/Users/parth/OneDrive/Desktop/1610/pipeline/scripts/smoke_test_cli.py) | Automated smoke tests for CLI status, export, and results |
| [`pipeline/failure_test_report.md`](file:///c:/Users/parth/OneDrive/Desktop/1610/pipeline/failure_test_report.md) | Deliverable 3: Evidence of failure tests run & results |
| [`pipeline/README.md`](file:///c:/Users/parth/OneDrive/Desktop/1610/pipeline/README.md) | Deliverable 1: Install + run instructions (fresh Pi) |
| [`pipeline/design_note.md`](file:///c:/Users/parth/OneDrive/Desktop/1610/pipeline/design_note.md) | Deliverable 2: 2-page architecture + guarantees note |
| [`pipeline/resource_report.md`](file:///c:/Users/parth/OneDrive/Desktop/1610/pipeline/resource_report.md) | Deliverable 4: Resource measurement template & figures |

---

## 📋 Assessment Criteria Coverage

| # | Criterion | How covered |
|---|---|---|
| 1 | Every sample saved on disk, exactly once | SQLite `INSERT OR IGNORE` by `(session_id, sensor, idx)` PK |
| 2 | Lost samples listed with reason | `losses` table + `pipeline export` output |
| 3 | Survives crash/reboot/power cut | WAL + `synchronous=NORMAL/FULL`; bound < 0.25 s verified |
| 4 | Online: cloud result within 5 s | Syncer uploads windows every 1 s cycle |
| 5 | Mode switch ≤ 10 s, no loss/dup | NetworkMonitor hysteresis (4s switch); disk-first write order |
| 6 | Cloud complete within 15 min | Syncer pushes samples AND losses; cloud export matches ground truth |
| 7 | Results follow section 3 exactly | `processor.py` + 30 unit/integration tests + full window lookback |
| 8 | Runs in ≤ 4 cores / 2 GB | Stdlib-only, rolling 5000-sample buffer, peak RAM < 45 MB |

---

## ⚙️ What to do before submission

1. **Get a cloud server running** — Deploy `cloud_server/cloud_server.py` on any free-tier VM (Oracle Always Free ARM64 recommended), or set up Supabase tables.
2. **Set env vars** on the Pi:
   ```bash
   export CLOUD_BACKEND=http
   export CLOUD_HTTP_URL=http://<your-vm-ip>:8080
   ```
3. **Run on a real Pi 4** with the `biosignal-mock` binary.
4. **Fill in `resource_report.md`** with actual measured figures.
5. **Record the 5-minute demo video** showing: network cut, process kill, power cut.
6. **Disclose AI tool use** in the submission email (already noted in README and design note).

> [!IMPORTANT]
> Email subject line: `"2026 Oct: Software Systems Engineer — [Your Name]"`  
> Address: `BuildWithUS@mayacardio.io`

