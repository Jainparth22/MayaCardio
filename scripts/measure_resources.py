"""
measure_resources.py - CPU, memory, disk and network use of the running pipeline.

Linux only (reads /proc). Measures the pipeline PROCESS, not the whole machine,
so the numbers are attributable to BeatKeeper.

    beatkeeper run &                                   # pipeline already running
    python scripts/measure_resources.py --pid $! --data ./data --duration 3600 --interval 10

Reports:
  avg_cpu_percent      process CPU time / wall time (100% = one full core)
  peak_rss_mb          peak resident memory (VmHWM)
  disk_written_mb      process write_bytes (what reached the block layer)
  db_size_mb           size of the SQLite files after the run
  uploaded_mb          bytes the pipeline put on the wire (counted in the client,
                       after gzip, from data/status.json)
"""

import argparse
import json
import os
import time
from pathlib import Path

CLK = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100


def cpu_ticks(pid):
    f = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return int(f[11]) + int(f[12])                 # utime + stime


def vm_hwm_mb(pid):
    for ln in Path(f"/proc/{pid}/status").read_text().splitlines():
        if ln.startswith("VmHWM"):
            return int(ln.split()[1]) / 1024
    return 0.0


def write_bytes(pid):
    for ln in Path(f"/proc/{pid}/io").read_text().splitlines():
        if ln.startswith("write_bytes"):
            return int(ln.split()[1])
    return 0


def uploaded(data):
    try:
        return json.loads((Path(data) / "status.json").read_text()).get("uploaded_bytes", 0)
    except (OSError, ValueError):
        return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pid", type=int, required=True)
    ap.add_argument("--data", default="data")
    ap.add_argument("--duration", type=int, default=3600)
    ap.add_argument("--interval", type=int, default=10)
    a = ap.parse_args()

    t0, c0, w0, u0 = time.time(), cpu_ticks(a.pid), write_bytes(a.pid), uploaded(a.data)
    print("t_s,cpu_percent,rss_peak_mb,disk_written_mb,uploaded_mb")
    last_t, last_c = t0, c0
    while time.time() - t0 < a.duration:
        time.sleep(a.interval)
        now, c = time.time(), cpu_ticks(a.pid)
        print(f"{now - t0:.0f},{100 * (c - last_c) / CLK / (now - last_t):.1f},"
              f"{vm_hwm_mb(a.pid):.1f},{(write_bytes(a.pid) - w0) / 1e6:.1f},"
              f"{(uploaded(a.data) - u0) / 1e6:.1f}", flush=True)
        last_t, last_c = now, c
    el = time.time() - t0
    db = sum(p.stat().st_size for p in Path(a.data).glob("beatkeeper.db*"))
    print(json.dumps({
        "duration_s": round(el), "avg_cpu_percent": round(100 * (cpu_ticks(a.pid) - c0) / CLK / el, 2),
        "peak_rss_mb": round(vm_hwm_mb(a.pid), 1),
        "disk_written_mb": round((write_bytes(a.pid) - w0) / 1e6, 1),
        "db_size_mb": round(db / 1e6, 1),
        "uploaded_mb": round((uploaded(a.data) - u0) / 1e6, 1)}), flush=True)


if __name__ == "__main__":
    main()
