"""
chaos_test.py - end-to-end failure test with REAL processes and hard kills.

Starts the simulator (with injected faults), the cloud service and the
pipeline as separate OS processes, then inflicts, on a schedule:

    network cut      cloud service killed, later restarted (same database)
    process kill #1  pipeline hard-killed, restarted quickly   -> recovers everything
    process kill #2  pipeline hard-killed, restarted too late  -> loss must be reported
    gateway faults   corrupt replies, duplicate replies, a stall, a sensor dropout

and finally audits the result against ground truth (beatkeeper.verify):
no wrong values, no duplicates, NO SILENT LOSS, cloud copy == device copy.

    python scripts/chaos_test.py            # ~100 s
    python scripts/chaos_test.py --speed 1  # real time, 4 min

(A hard kill = SIGKILL on Linux / TerminateProcess on Windows: the process gets
no chance to flush or clean up, which is what a power cut looks like to the
software. A true power-pull test needs hardware; see docs/EVIDENCE.md.)
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))
from beatkeeper.verify import check  # noqa: E402


def spawn(args, env, log):
    return subprocess.Popen([sys.executable, "-u", "-m", *args], env=env, cwd=str(ROOT),
                            stdout=open(log, "ab"), stderr=subprocess.STDOUT)


def get(url):
    with urllib.request.urlopen(url, timeout=5) as r:
        return r.read().decode()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--speed", type=float, default=2.0, help="simulator time acceleration")
    ap.add_argument("--duration", type=float, default=160, help="simulated session seconds")
    ap.add_argument("--keep", action="store_true", help="keep the work directory")
    a = ap.parse_args()

    work = Path(tempfile.mkdtemp(prefix="beatkeeper-chaos-"))
    logs = work / "logs"
    logs.mkdir()
    port_gw, port_cloud = 17100, 18080
    env = dict(os.environ, PYTHONPATH=str(SRC), BEATKEEPER_DATA=str(work / "device"),
               BEATKEEPER_CLOUD_URL=f"http://127.0.0.1:{port_cloud}",
               BEATKEEPER_API_KEY="chaos", LOG_LEVEL="INFO", PYTHONUNBUFFERED="1")
    S = a.speed                      # wall seconds = sim seconds / S
    w = lambda sim: sim / S

    print(f"[chaos] workdir {work}")
    sim = spawn(["beatkeeper.simulator", "--mode", "signal", "--seed", "7",
                 "--duration", str(a.duration), "--port", str(port_gw), "--speed", str(S),
                 "--corrupt-rate", "0.03", "--dup-rate", "0.03",
                 "--stall", "20:12", "--dropout", f"2:100:8"], env, logs / "sim.log")
    cloud_cmd = ["beatkeeper.cloud_server", "--port", str(port_cloud), "--db", str(work / "cloud.db"),
                 "--host", "127.0.0.1"]
    cloud = spawn(cloud_cmd, env, logs / "cloud.log")
    pipe_cmd = ["beatkeeper", "run", "--gateway", f"127.0.0.1:{port_gw}"]
    pipe = spawn(pipe_cmd, env, logs / "pipeline.log")
    t0 = time.time()
    events = []

    def at(sim_t, label):
        while time.time() - t0 < w(sim_t):
            time.sleep(0.05)
        events.append((round(time.time() - t0, 1), label))
        print(f"[chaos] t={events[-1][0]:>5}s  {label}", flush=True)

    try:
        at(45, "NETWORK CUT: killing cloud service")
        cloud.kill(); cloud.wait()
        at(95, "NETWORK BACK: restarting cloud service")
        cloud = spawn(cloud_cmd, env, logs / "cloud.log")
        at(110, "PROCESS KILL #1 (hard): pipeline")
        pipe.kill(); pipe.wait()
        at(113, "restart pipeline (fast: data still in gateway buffer)")
        pipe = spawn(pipe_cmd, env, logs / "pipeline.log")
        at(128, "PROCESS KILL #2 (hard): pipeline")
        pipe.kill(); pipe.wait()
        at(142, "restart pipeline (slow: gateway buffer overflowed meanwhile)")
        pipe = spawn(pipe_cmd, env, logs / "pipeline.log")

        # wait for completion: session ended and backlog empty
        deadline = time.time() + w(a.duration) + 90
        status = {}
        while time.time() < deadline:
            time.sleep(1.0)
            out = subprocess.run([sys.executable, "-m", "beatkeeper", "status"], env=env,
                                 cwd=str(ROOT), capture_output=True, text=True).stdout.strip()
            try:
                status = json.loads(out.splitlines()[-1])
            except (ValueError, IndexError):
                continue
            if (time.time() - t0 > w(a.duration) + 3 and status.get("backlog") == 0
                    and status.get("samples_saved", 0) > 0 and status["mode"] == "online"):
                time.sleep(3)
                break
        print("[chaos] final status:", json.dumps(status))

        sid = status["session_id"]
        n = int(a.duration * 400)
        dev = subprocess.run([sys.executable, "-m", "beatkeeper", "export", "--session", sid,
                              "--from", "device"], env=env, cwd=str(ROOT),
                             capture_output=True, text=True).stdout.splitlines()
        cld = subprocess.run([sys.executable, "-m", "beatkeeper", "export", "--session", sid,
                              "--from", "cloud"], env=env, cwd=str(ROOT),
                             capture_output=True, text=True).stdout.splitlines()
        res = subprocess.run([sys.executable, "-m", "beatkeeper", "results", "--session", sid,
                              "--from", "cloud"], env=env, cwd=str(ROOT),
                             capture_output=True, text=True).stdout.splitlines()
        lat = json.loads(get(f"http://127.0.0.1:{port_cloud}/v1/latency/{sid}")) \
            if False else None

        rep = check(dev, "signal", 7, n)
        norm = lambda ls: sorted(json.dumps(json.loads(x), sort_keys=True) for x in ls if x.strip())
        same = norm(dev) == norm(cld)
        windows = [json.loads(x) for x in res]
        modes = {}
        for x in windows:
            modes[x["mode"]] = modes.get(x["mode"], 0) + 1
        reasons = rep["loss_reasons"]

        print("\n================ RESULT ================")
        print(json.dumps({"ground_truth_audit": rep, "cloud_equals_device": same,
                          "windows": len(windows), "windows_by_mode": modes}, indent=2))
        ok = rep["pass"] and same and len(windows) >= int(a.duration) - 1
        # sanity: the faults must actually have been exercised and accounted for
        exercised = bool(reasons.get("buffer_overflow")) and bool(reasons.get("sensor_dropout"))
        print(f"\nfaults exercised and reported as losses: {exercised} {reasons}")
        print("VERDICT:", "PASS" if ok and exercised else "FAIL")
        (work / "chaos_report.json").write_text(json.dumps(
            {"events": events, "audit": rep, "cloud_equals_device": same,
             "windows_by_mode": modes}, indent=2))
        print(f"report: {work / 'chaos_report.json'}   logs: {logs}")
        return 0 if ok and exercised else 1
    finally:
        for p in (pipe, cloud, sim):
            try:
                p.kill()
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())
