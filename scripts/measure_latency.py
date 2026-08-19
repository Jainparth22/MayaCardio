"""
measure_latency.py - real-time online latency: how long after a second of data
has been *produced* does its result exist in the cloud?

Runs simulator (1x real time) + cloud + pipeline as separate processes. The
simulator's clock is the reference: window w completes at sim-time (w+1) s, so

    latency(w) = cloud.processed_at(w) - (session_start + (w + 1))

`session_start` is the moment the simulator began listening (its clock origin).

    python scripts/measure_latency.py --seconds 40
"""

import argparse
import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"


def spawn(args, env, log):
    return subprocess.Popen([sys.executable, "-u", "-m", *args], env=env, cwd=str(ROOT),
                            stdout=open(log, "ab"), stderr=subprocess.STDOUT)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=40)
    a = ap.parse_args()
    work = Path(tempfile.mkdtemp(prefix="bk-lat-"))
    env = dict(os.environ, PYTHONPATH=str(SRC), BEATKEEPER_DATA=str(work / "dev"),
               BEATKEEPER_CLOUD_URL="http://127.0.0.1:18081", BEATKEEPER_API_KEY="k")
    sim = spawn(["beatkeeper.simulator", "--duration", str(a.seconds), "--port", "17101"],
                env, work / "sim.log")
    t0 = None
    while t0 is None:                       # gateway clock starts when it begins listening
        try:
            socket.create_connection(("127.0.0.1", 17101), timeout=0.2).close()
            t0 = time.time()
        except OSError:
            time.sleep(0.01)
    procs = [sim, spawn(["beatkeeper.cloud_server", "--port", "18081", "--db", str(work / "c.db"),
                         "--host", "127.0.0.1"], env, work / "cloud.log")]
    procs.append(spawn(["beatkeeper", "run", "--gateway", "127.0.0.1:17101"], env, work / "pipe.log"))
    try:
        time.sleep(a.seconds + 6)
        db = sqlite3.connect(f"file:{work / 'c.db'}?mode=ro", uri=True)
        lat = sorted(pa - (t0 + w + 1) for w, pa in db.execute(
            "SELECT window_index, processed_at FROM windows WHERE mode='online'"))
        steady = sorted(pa - (t0 + w + 1) for w, pa in db.execute(
            "SELECT window_index, processed_at FROM windows WHERE mode='online' AND window_index>=10"))
        pct = lambda xs, q: round(xs[min(len(xs) - 1, int(q * len(xs)))], 2)
        print(json.dumps({
            "windows": len(lat),
            "all": {"p50_s": pct(lat, .5), "p95_s": pct(lat, .95), "max_s": round(lat[-1], 2)},
            "steady_state_after_10s": {"p50_s": pct(steady, .5), "p95_s": pct(steady, .95),
                                       "max_s": round(steady[-1], 2)},
            "requirement_s": 5}, indent=2))
    finally:
        for p in procs:
            p.kill()


if __name__ == "__main__":
    main()
