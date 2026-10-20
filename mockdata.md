In our pipeline, handling the mock service (biosignal-mock) and its synthetic data is handled directly by 

reader.py
 alongside the SQLite idempotency layer in 

store.py
.

1. The Two Mock Data Modes Handled
The mock service runs on port 7100 and generates two distinct data modes:

[ biosignal-mock ]
       │
       ├── Mode 1: --mode signal  (Realistic Clinical ECG)
       │     • Sensor 1, ch2: ECG with sharp R-peaks (60 bpm → 90 bpm → 72 bpm cycling)
       │     • Sensor 1, ch1 & ch3: Scaled ECG (ch2 × 0.6 and ch2 × 1.3)
       │     • Sensor 2: Baseline hemodynamic signals (ch1 ≈ 400,000, ch2 ≈ 150,000)
       │
       └── Mode 2: --mode ramp    (Deterministic Ground Truth)
             • Sensor 1 channel c at sample i = (3i + c) mod 8,388,608
             • Sensor 2 channel c at sample i = (2i + c) mod 8,388,608
             • Used by judges' `check_ground_truth` to verify zero sample corruption/loss
Our test suite explicitly validates against this math in 

tests/test_processor.py
 (test_ramp_sensor1_formula and test_ramp_sensor2_formula).

2. Transport & Streaming Protocol
Transport: TCP socket to 127.0.0.1:7100.
Threading: One independent SensorReader daemon thread per sensor (reader-s1 and reader-s2).
TCP Stream Boundary Protection: TCP does not preserve message boundaries. One recv() call may return partial JSON or multiple lines. We implemented an internal stream accumulator in 

reader.py
:
python
def _recv_line(self) -> str:
    while b"\n" not in self._rbuf:
        chunk = self._sock.recv(65536)
        if not chunk:
            raise ConnectionResetError("Mock closed connection")
        self._rbuf += chunk
    line, self._rbuf = self._rbuf.split(b"\n", 1)
    return line.decode()
3. Handling the 6 Mock Faults Injected by Judges
The assessment specification outlines 6 faults that the mock will inject during automated judging. Here is how our code handles each:

                  Mock Sends Packet
                          │
            [ Step 1: CRC-32 Verification ]
             /                             \
        CRC Valid                       CRC Mismatch
           │                                 │
  [ Step 2: Session Check ]          Drop batch, do not advance index;
     /                  \            Close socket, reconnect;
Same Session        New Session      Re-request: {"cmd": "read", "from_index": N}
     │                   │
Process Batch      Record old session loss (to_index=None);
                   Reset sequence to index 0;
                         │
           [ Step 3: Check "lost" Array ]
                         │
                   Record reported gaps to SQLite losses;
                   (Never invent dummy values)
                         │
           [ Step 4: Write to SQLite WAL ]
                   (INSERT OR IGNORE deduplicates re-sent frames)
Fault 1: Mock Injects Corrupted Packets (Wrong CRC-32)
Spec Rule: "CRC-32 (zlib) of the data text exactly as sent, from [ to the matching ]. A reply with a wrong value is corrupt and must not be used."
Our Handling (

reader.py:L123-137
):
python
data_json = json.dumps(reply["data"], separators=(",", ":"))
crc_computed = "%08x" % (zlib.crc32(data_json.encode()) & 0xFFFFFFFF)
if crc_computed != reply.get("crc32", ""):
    log.warning("Sensor %d: CRC mismatch — re-reading from %s", self.sensor, self._next_index)
    self._close()       # Close socket to reset mock TCP stream
    time.sleep(0.05)
    return              # Do NOT advance self._next_index!
On the next loop iteration, the reader sends: {"cmd": "read", "sensor": 1, "from_index": last_good_index} This forces the mock to replay the uncorrupted batch from the exact index needed.
Fault 2: Mock Reboots / Session Change
Spec Rule: "session_id changes when the mock restarts; indexes then start again from 0. Unread data of the old session is then lost and must be listed, with to_index: null."
Our Handling (

reader.py:L111-120
):
python
if self._session_id and session_id != self._session_id:
    log.warning("Sensor %d: new session %s (was %s)", self.sensor, session_id, self._session_id)
    if self._next_index is not None:
        self.on_loss(self._session_id, self.sensor, self._next_index, None, "mock_restart")
    self._session_id = session_id
    self._next_index = None  # Reset pointer to start from 0 for new session
Fault 3: Mock Buffer Overflow (10-Second Circular Ring Buffer)
Spec Rule: "The mock keeps only the last 10 seconds of each sensor. Data not read in time is lost and reported in lost."
Our Handling (

reader.py:L144-149
): The mock provides dropped ranges in the lost array (e.g. [{"from_index": 4000, "to_index": 4399, "reason": "buffer_overflow"}]).
python
for loss in lost:
    self.on_loss(session_id, self.sensor, loss["from_index"], loss.get("to_index"), loss.get("reason", "mock_reported"))
This immediately commits the loss range to SQLite losses. Crucially, we never fabricate or interpolate missing samples. The window processor accurately marks these windows as complete = false and outputs the exact missing_1 and missing_2 counts.
Fault 4: Mock Sends Duplicate Reply
Spec Rule: Mock may retransmit the same frame twice.
Our Handling (

store.py:L104-107
): The SQLite table uses composite primary key (session_id, sensor, idx) with INSERT OR IGNORE:
sql
INSERT OR IGNORE INTO samples(session_id, sensor, idx, values_json) VALUES (?, ?, ?, ?)
Duplicate packets are silently discarded at the database storage engine level with zero memory or database bloat.
Fault 5: Mock Stops Responding / Hangs
Spec Rule: Mock may stop answering temporarily.
Our Handling: Sockets are configured with READ_TIMEOUT = 2.0s. If the mock stops sending, socket timeout throws socket.timeout or ConnectionResetError, triggering a clean reconnect attempt after a 1-second backoff without crashing the thread or blocking the rest of the pipeline.
Fault 6: Mock Stops One Sensor Temporarily
Spec Rule: Mock may deliver sensor 1 while sensor 2 is paused.
Our Handling: The readers run on two completely independent threads. If sensor 2 stops sending, reader-s1 continues streaming at full 400 Hz. The window engine (

engine.py
) detects available samples and computes window metrics with missing_2 = 400, complete = false, without stalling sensor 1 processing.
4. How to Explain This in the Interview
"We treat the mock as an untrusted external TCP hardware emulator. We read one-client-at-a-time with two daemon threads, verify every packet's CRC-32 against raw bytes before parsing, and re-request via from_index if corrupted. If the mock overflows its 10-second ring buffer or reboots with a new session_id, we log the exact missing ranges to our losses table with to_index: null without inventing dummy values. Finally, composite primary keys in SQLite ensure duplicate packets from the mock are deduplicated."

10:57 AM
where do we getting the mock data 
10:58 AM
We get the mock data from two sources, depending on whether you are running the live pipeline on the Raspberry Pi or running automated tests:

1. In Live Running & Judging: The biosignal-mock Binary
In the real deployment on the Raspberry Pi (and during evaluation by the judges), the data comes from a pre-compiled binary executable provided by MayaCardio named biosignal-mock (Section 6 of the assessment).

How it works:
You launch the mock binary as a background process on the device:
bash
chmod +x biosignal-mock
./biosignal-mock --mode signal --seed 7 --duration 3600 --port 7100 &
The mock starts a local TCP server on port 7100 (127.0.0.1:7100).
Our pipeline (

reader.py
) connects to 127.0.0.1:7100 using standard TCP sockets:
python
MOCK_HOST = "127.0.0.1"
MOCK_PORT = 7100
Our SensorReader threads send a newline-terminated JSON request:
json
{"cmd": "read", "sensor": 1}
The mock replies over TCP with up to 1 second of data ($400$ samples) in newline-delimited JSON:
json
{
  "session_id": "5f0c2a9e",
  "sensor": 1,
  "first_index": 120000,
  "count": 3,
  "lost": [],
  "session_end": false,
  "crc32": "f9a5bf62",
  "data": [[720, 1200, 1560], [708, 1180, 1534], [726, 1210, 1573]]
}
2. In Local Development & Unit Testing: Embedded MockServer Stub
To develop and test without having to run MayaCardio's closed binary, we built an in-process TCP server stub in 

tests/test_integration.py
:

python
class MockServer:
    """
    Serves deterministic ramp data on a TCP port.
    Sensor 1: 3 channels, (3i+c) mod 8388608
    Sensor 2: 2 channels, (2i+c) mod 8388608
    """
It binds dynamically to an available localhost port.
It generates the exact synthetic data formulas specified in Section 6.1:
Sensor 1: $\text{Channel } c \text{ at sample } i = (3i + c) \pmod{8{,}388{,}608}$
Sensor 2: $\text{Channel } c \text{ at sample } i = (2i + c) \pmod{8{,}388{,}608}$
It computes the real zlib CRC-32 over data and sends compliant newline-delimited JSON.
This allows our test suite (python tests\test_integration.py) to run and verify the entire TCP read-and-store lifecycle in any environment.
Summary Table
Environment	Where Mock Runs	How Pipeline Connects	Data Generation
Judging / Raspberry Pi	Standalone Linux executable (./biosignal-mock)	TCP 127.0.0.1:7100 via 

reader.py
Real synthetic ECG (--mode signal) or ramp arithmetic (--mode ramp)
CI / Integration Tests	In-memory Python thread (MockServer in 

test_integration.py
)	TCP 127.0.0.1:<dynamic_port>	Deterministic modulo arithmetic matching Section 6.1 ramp mode
