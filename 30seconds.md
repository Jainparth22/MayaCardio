Core System Viva Blueprint
1. The 90-Second System Architecture Pitch
"Our pipeline is an edge-native, decoupled multi-threaded Python system designed for zero-loss telemetry under intermittent connectivity and unannounced power failures on ARM64 hardware.

Two dedicated SensorReader threads pull 400 Hz samples over TCP from the mock service, verify packet integrity with CRC-32, and immediately persist raw data to a local SQLite database configured with Write-Ahead Logging (WAL) and SYNCHRONOUS=NORMAL. This guarantees that committed samples survive process termination and sudden power pull with an upper bound of data loss under 0.25 seconds (our maximum batch write latency).

A decoupled NetworkMonitor probe pings the cloud endpoint every 2 seconds via monotonic HTTP HEAD requests, driving state transitions (ONLINE ↔ OFFLINE) with debounce hysteresis well within the 10-second requirement.

In Online mode, an asynchronous Syncer thread pushes batches to the cloud, where 1-second sliding windows are processed within 5 seconds. In Offline mode, an on-device Processor computes the exact same window metrics locally. Upon network recovery, an idempotent sync_backlog loop transmits stored samples and marks them synchronized, achieving full parity within minutes without silent data loss or invented values."

               [ biosignal-mock (TCP :7100) ]
                       |            |
                 Sensor 1 (400Hz)  Sensor 2 (400Hz)
                       v            v
             +----------------------------------+
             |       SensorReader Threads       | (CRC-32 verified)
             +----------------------------------+
                              |
                     Batched atomic inserts
                              v
             +----------------------------------+
             |    Local SQLite (WAL mode)       |
             |   - samples (PRIMARY KEY)        |
             |   - loss_records                 |
             |   - processed_windows            |
             +----------------------------------+
                    |                       |
       [Online: network up]        [Offline: network down]
                    v                       v
      +------------------------+  +------------------------+
      |      Syncer Thread     |  | Local Engine Processor |
      | (HTTP chunked batches) |  | (Identical math logic) |
      +------------------------+  +------------------------+
                    |                       |
                    v                       v
        [ Cloud REST Server ]    [ Saved in Local SQLite ]
        (Flask / Supabase)       (Synced when net returns)
2. Component Functioning & Invariants


reader.py
 (SensorReader):

Invariant: Never advances the requested index until samples are verified and safely staged.
CRC-32 Verification: Before decoding payload JSON, computes zlib.crc32(raw_slice) on the exact text between [ and ]. If corrupt, drops the batch and re-requests from the same from_index.
Loss Accounting: If the mock's lost field contains gaps (e.g., buffer overflows during reader backpressure), records them directly in loss_records without fabricating substitute samples.
Session Restarts: Detects mock reboots via session_id change; closes previous session loss records (to_index=None) and resets the tracking sequence index to 0.


store.py
 (SQLite Persistence Layer):

Schema Design: samples (session_id, sensor, sample_index, ...) has a composite primary key. Re-inserting existing samples uses INSERT OR IGNORE, guaranteeing complete idempotency under network or thread replay.
WAL Optimization: PRAGMA journal_mode=WAL separates readers from writers. PRAGMA synchronous=NORMAL ensures filesystem sync on checkpointing while maintaining sub-millisecond write transactions.


processor.py
 (Windowing & Telemetry Math):

Stateless Pure Functions: Evaluates 1-second chunks ($400$ samples/sec).
Signal Analysis: Calculates mean, min, max per channel, plus sensor 2 vector magnitude $\frac{1}{N}\sum\sqrt{ch_1^2 + ch_2^2}$.
Peak Detection: Strict 4-point criteria for ECG R-peaks evaluated using full historical context ($\max(800, \text{start}-800)$):
- Sample $i$ has 800 prior samples and 1 posterior sample available.
- $x[i] > x[i-1]$ and $x[i] > x[i+1]$.
- $x[i] > 0.5 \times \max(x[i-800 : i])$.
- Refractory blanking: $i - \text{last_peak} \ge 100$ samples ($250\text{ ms}$).
Heart Rate: Requires a full 5-second window ($w \ge 4$), zero missing samples, and $\ge 2$ peaks: $60 / \text{mean_gap_seconds}$.


network.py
 (NetworkMonitor):

Probing: Lightweight HEAD request with a 1.5s socket timeout.
Hysteresis: Requires 2 consecutive failures to trigger OFFLINE, and 2 consecutive successes to trigger ONLINE, preventing rapid oscillation on noisy hospital networks.
Clock Immunity: Uses time.monotonic() for intervals so NTP sync or time adjustments never distort mode transitions.


syncer.py
 (CloudSyncer):

Pushes samples (2000/batch), windows (200/batch), and loss records.
Marks synced=1 locally only upon receiving HTTP 200/201 confirmation.
If a packet is lost in transit, the batch remains unflagged and is resent in the subsequent cycle.
Both samples and loss records are synchronized so `pipeline export --from cloud` matches the device ground truth 100%.
3. Critical Tradeoffs Defended
Decision	Why We Chose It	Why We Rejected The Alternative
threading vs asyncio	Direct blocking TCP socket reads with simple timeout semantics; zero event-loop starvation risk from compute-heavy peak detection.	Python asyncio on ARM64 can suffer latency spikes if CPU-bound signal processing runs on the main thread without explicit executor offloading.
SQLite WAL vs DuckDB / Redis	Zero external daemon dependencies, atomic ACID commits, ubiquitous support, minimal RAM (<25 MB), robust crash recovery via write-ahead log.	Redis requires an in-memory daemon that loses un-fsynced data on power cuts; DuckDB has higher memory overhead and is optimized for OLAP analytics rather than continuous 400 Hz single-row appends.
At-Least-Once Delivery	Network drops can truncate HTTP responses after server commit. Resending with server-side primary key deduplication (ON CONFLICT DO NOTHING) guarantees data integrity.	Exactly-once over unreliable TCP requires heavy distributed two-phase commit protocols (2PC) that stall edge devices when the cloud is offline.
Raw JSON Lines vs Protocol Buffers	Assessment CLI explicitly mandates JSON line output for exports and results; human-readable debugging during viva demos.	Protobuf adds a binary compilation step and requires non-standard runtime wheels on minimal ARM64 targets without performance necessity at 400 Hz.
4. Handling All 7 Judge-Injected Faults
Process Kill (kill -9):
Mechanism: OS immediately reclaims process memory without executing finally blocks.
Mitigation: Because transactions commit every 100–400 samples with WAL, uncommitted memory buffers are bounded to $<0.25\text{s}$. Upon restart, SQLite WAL recovers cleanly with zero database corruption.
Sudden Power Pull:
Mechanism: Device loses power mid-write.
Mitigation: SQLite atomic commits ensure transactions either fully exist or are rolled back cleanly. Upper bound of lost samples is $<0.25\text{s}$, reported truthfully in the design note.
Corrupt Packet (CRC-32 Mismatch):
Mechanism: Mock injects inverted bits into data payload.
Mitigation: 

reader.py
 detects crc32_calc != crc32_header, discards the frame, and issues a re-read command {"cmd": "read", "sensor": s, "from_index": last_idx}.
2-Hour Network Blackout:
Mechanism: Physical Ethernet or WiFi link severed.
Mitigation: Network monitor shifts to OFFLINE within 4 seconds. The device processes 1-second windows locally and writes metrics to processed_windows (mode='offline'). Samples accumulate with synced=0. When connectivity restores, syncer.py drains the backlog in 400-sample chunks without dropping live readings.
Mock Service Restart / Session Reset:
Mechanism: Mock resets internal counter to 0 with a new session_id.
Mitigation: Reader detects new_session_id != current_session_id. It logs a record loss_records(to_index=None, reason="mock_restart") for the old session, registers the new session in sessions table, and resets the index tracking to 0.
Mock 10-Second Buffer Overflow:
Mechanism: Reader stalled or paused while mock buffer fills past 10 seconds.
Mitigation: Mock returns dropped ranges in the lost array. Reader immediately logs these ranges to loss_records with reason "buffer_overflow" without fabricating dummy data.
System Clock Jump (NTP Step):
Mechanism: Judges jump the Linux system clock backwards or forwards.
Mitigation: All timeouts, window boundaries, and network probe loops utilize time.monotonic() instead of time.time(), remaining completely immune to wall-clock shifts.
Verification and Running Commands
All verification commands can be run directly from the workspace terminal:

bash
# 1. Run all unit and integration test suites
python tests\test_processor.py
python tests\test_store.py
python tests\test_integration.py
# 2. Check CLI status command
python pipeline\cli.py status
# 3. Test export formatting
python pipeline\cli.py export --session test_session --from device
# 4. View window processing results
python pipeline\cli.py results --session test_session
All interview prep artifacts are saved and ready to review. If you need any mock interview practice questions or additional system scenarios explored, let me know!