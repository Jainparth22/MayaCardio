"""
measure_resources.py — measure CPU, memory, disk and network usage over 1 hour.

Run on the ARM64 device after starting the pipeline:
  python scripts/measure_resources.py --duration 3600 --interval 10

Outputs a CSV to stdout and prints a summary at the end.
Uses only stdlib (psutil optional but recommended).
"""

import argparse
import csv
import os
import sys
import time
import threading


def _read_cpu_times():
    """Read /proc/stat for cumulative CPU times (Linux only)."""
    with open("/proc/stat") as f:
        line = f.readline()
    fields = list(map(int, line.split()[1:]))
    total = sum(fields)
    idle  = fields[3]
    return total, idle


def _cpu_percent(prev_total, prev_idle, curr_total, curr_idle) -> float:
    dt = curr_total - prev_total
    di = curr_idle  - prev_idle
    if dt == 0:
        return 0.0
    return round(100.0 * (1 - di / dt), 1)


def _read_mem_mb() -> float:
    """Read /proc/meminfo for used memory in MB."""
    info = {}
    with open("/proc/meminfo") as f:
        for line in f:
            k, v = line.split(":", 1)
            info[k.strip()] = int(v.split()[0])
    total    = info.get("MemTotal", 0)
    free     = info.get("MemFree", 0)
    buffers  = info.get("Buffers", 0)
    cached   = info.get("Cached", 0)
    used     = total - free - buffers - cached
    return round(used / 1024, 1)


def _read_net_bytes(iface: str = "eth0"):
    """Read /proc/net/dev for cumulative TX bytes."""
    with open("/proc/net/dev") as f:
        for line in f:
            if iface in line:
                parts = line.split()
                rx = int(parts[1])
                tx = int(parts[9])
                return rx, tx
    return 0, 0


def _read_disk_bytes(dev: str = "mmcblk0"):
    """Read /proc/diskstats for sectors written (512 B each)."""
    with open("/proc/diskstats") as f:
        for line in f:
            parts = line.split()
            if len(parts) > 9 and parts[2] == dev:
                sectors_written = int(parts[9])
                return sectors_written * 512
    return 0


def _psutil_fallback(duration, interval, writer, start_time):
    """Use psutil when /proc is unavailable (non-Linux)."""
    try:
        import psutil
    except ImportError:
        print("psutil not available and /proc not found. Cannot measure resources.",
              file=sys.stderr)
        sys.exit(1)

    net_start = psutil.net_io_counters()
    disk_start = psutil.disk_io_counters()

    while time.time() - start_time < duration:
        cpu  = psutil.cpu_percent(interval=interval)
        mem  = psutil.virtual_memory().used / (1024 ** 2)
        net  = psutil.net_io_counters()
        disk = psutil.disk_io_counters()
        elapsed = round(time.time() - start_time, 0)
        net_mb  = round((net.bytes_sent + net.bytes_recv
                         - net_start.bytes_sent - net_start.bytes_recv) / (1024**2), 2)
        disk_mb = round((disk.write_bytes - disk_start.write_bytes) / (1024**2), 2)
        writer.writerow([elapsed, cpu, round(mem, 1), disk_mb, net_mb])


def main():
    parser = argparse.ArgumentParser(description="Resource usage monitor")
    parser.add_argument("--duration", type=int, default=3600, help="seconds to run")
    parser.add_argument("--interval", type=int, default=10, help="sample interval (s)")
    parser.add_argument("--iface", default="eth0", help="network interface")
    parser.add_argument("--dev", default="mmcblk0", help="disk device")
    args = parser.parse_args()

    writer = csv.writer(sys.stdout)
    writer.writerow(["elapsed_s", "cpu_pct", "mem_mb", "disk_written_mb", "net_sent_mb"])

    start_time = time.time()

    # Try /proc first (Raspberry Pi / Linux).
    try:
        prev_total, prev_idle = _read_cpu_times()
        net_rx0, net_tx0 = _read_net_bytes(args.iface)
        disk0 = _read_disk_bytes(args.dev)
        use_proc = True
    except (FileNotFoundError, IOError):
        use_proc = False

    if not use_proc:
        _psutil_fallback(args.duration, args.interval, writer, start_time)
    else:
        rows = []
        while time.time() - start_time < args.duration:
            time.sleep(args.interval)
            curr_total, curr_idle = _read_cpu_times()
            cpu = _cpu_percent(prev_total, prev_idle, curr_total, curr_idle)
            prev_total, prev_idle = curr_total, curr_idle

            mem  = _read_mem_mb()
            net_rx, net_tx = _read_net_bytes(args.iface)
            net_mb = round((net_tx - net_tx0 + net_rx - net_rx0) / (1024**2), 2)
            disk = _read_disk_bytes(args.dev)
            disk_mb = round((disk - disk0) / (1024**2), 2)

            elapsed = round(time.time() - start_time)
            writer.writerow([elapsed, cpu, mem, disk_mb, net_mb])
            rows.append((elapsed, cpu, mem, disk_mb, net_mb))

        if rows:
            avg_cpu  = round(sum(r[1] for r in rows) / len(rows), 1)
            peak_mem = max(r[2] for r in rows)
            total_disk = rows[-1][3]
            total_net  = rows[-1][4]
            print(f"\n# Summary over {rows[-1][0]}s:", file=sys.stderr)
            print(f"#   avg CPU:        {avg_cpu}%", file=sys.stderr)
            print(f"#   peak memory:    {peak_mem} MB", file=sys.stderr)
            print(f"#   disk written:   {total_disk} MB", file=sys.stderr)
            print(f"#   network (tx+rx):{total_net} MB", file=sys.stderr)


if __name__ == "__main__":
    main()
