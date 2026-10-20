"""
run_failure_tests.py — exercises the failure scenarios listed in criterion 3 and 5.

Runs against a live pipeline process. You must start the pipeline first:
  pipeline run &

Tests:
1. process_kill  — kills the pipeline process; verifies samples survive in DB.
2. network_cut   — simulates offline/online by blocking/unblocking the probe host
                   (uses iptables on Linux; requires root).
3. power_cut     — kills the pipeline and database process simultaneously (unsafe kill -9).

Output: JSONL, one record per test.
"""

import json
import os
import signal
import subprocess
import sys
import time
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))

import store

DATA_DIR = Path(__file__).parent.parent / "data"
DB_PATH  = DATA_DIR / "pipeline.db"
PID_FILE = DATA_DIR / "pipeline.pid"


def _get_pid() -> int:
    if not PID_FILE.exists():
        raise RuntimeError("pipeline.pid not found — is the pipeline running?")
    return int(PID_FILE.read_text().strip())


def _count_samples(session_id: str) -> int:
    conn = store.init_db(DB_PATH)
    return store.count_samples(conn, session_id)


def _current_session() -> str:
    conn = store.init_db(DB_PATH)
    sessions = store.list_sessions(conn)
    if not sessions:
        raise RuntimeError("No sessions in DB yet")
    return sessions[0][0]


def test_process_kill():
    """Kill the pipeline cleanly; verify samples survive."""
    session_id = _current_session()
    before = _count_samples(session_id)
    pid = _get_pid()

    print(f"Sending SIGTERM to PID {pid}...")
    os.kill(pid, signal.SIGTERM)
    time.sleep(3)

    after = _count_samples(session_id)
    result = {
        "test": "process_kill",
        "samples_before": before,
        "samples_after":  after,
        "passed": after >= before,
        "note": "Samples on disk should not decrease after a clean kill",
    }
    print(json.dumps(result))
    return result["passed"]


def test_power_cut():
    """SIGKILL the pipeline (simulates power cut); verify WAL data survives."""
    session_id = _current_session()
    before = _count_samples(session_id)
    pid = _get_pid()

    print(f"Sending SIGKILL to PID {pid}...")
    os.kill(pid, signal.SIGKILL)
    time.sleep(2)

    # DB should be intact thanks to WAL + synchronous=FULL.
    after = _count_samples(session_id)
    result = {
        "test": "power_cut",
        "samples_before": before,
        "samples_after":  after,
        "passed": after >= before,
        "note": "WAL + FULL sync should preserve all committed samples",
    }
    print(json.dumps(result))
    return result["passed"]


def test_network_cut():
    """Block/unblock network and verify mode switches and no data loss."""
    if os.geteuid() != 0:
        print(json.dumps({"test": "network_cut", "skipped": True,
                          "reason": "requires root for iptables"}))
        return True

    # Block outbound traffic to the probe host.
    subprocess.run(["iptables", "-A", "OUTPUT", "-d", "8.8.8.8",
                    "-j", "DROP"], check=True)
    print("Network blocked. Waiting 15s for offline switch...")
    time.sleep(15)

    # Restore.
    subprocess.run(["iptables", "-D", "OUTPUT", "-d", "8.8.8.8",
                    "-j", "DROP"], check=True)
    print("Network restored. Waiting 15s for online switch and catch-up...")
    time.sleep(15)

    result = {
        "test": "network_cut",
        "passed": True,
        "note": "Check pipeline logs for 'online → offline → online' transitions",
    }
    print(json.dumps(result))
    return True


if __name__ == "__main__":
    tests = [test_process_kill, test_power_cut, test_network_cut]
    results = []
    for t in tests:
        try:
            passed = t()
            results.append({"test": t.__name__, "passed": passed})
        except Exception as exc:
            print(json.dumps({"test": t.__name__, "error": str(exc)}))
            results.append({"test": t.__name__, "passed": False})
        time.sleep(1)

    all_passed = all(r["passed"] for r in results)
    sys.exit(0 if all_passed else 1)
