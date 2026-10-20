# Resource Usage Report

**Environment**: Raspberry Pi 4 Model B (4 GB RAM), ARM64, Raspberry Pi OS Bookworm (64-bit)  
**Duration**: 1 hour in online mode  
**Pipeline version**: 1.0.0  
**Mock command**: `biosignal-mock --mode signal --seed 7 --duration 3600 --port 7100`  
**Measurement tool**: `scripts/measure_resources.py --duration 3600 --interval 10 --iface eth0 --dev mmcblk0`

---

## Summary (to be filled in after running on the Pi)

| Metric | Value |
|---|---|
| Average CPU usage | ___ % |
| Peak memory usage | ___ MB |
| Total disk written (1 h) | ___ MB |
| Total network sent (1 h) | ___ MB |

---

## How to reproduce

```bash
# 1. Start the mock
./biosignal-mock --mode signal --seed 7 --duration 3600 --port 7100 &

# 2. Start the pipeline
export CLOUD_BACKEND=http
export CLOUD_HTTP_URL=http://<cloud-vm>:8080
python3 pipeline/cli.py run &

# 3. Measure resources for 1 hour
python3 scripts/measure_resources.py \
  --duration 3600 \
  --interval 10 \
  --iface eth0 \
  --dev mmcblk0 \
  > resource_log.csv 2> resource_summary.txt

cat resource_summary.txt
```

---

## Raw CSV (attach after measurement)

The file `resource_log.csv` produced during the run contains columns:

```
elapsed_s, cpu_pct, mem_mb, disk_written_mb, net_sent_mb
```

Attach the CSV alongside this report when submitting.

---

## Notes

- CPU is measured via `/proc/stat` (user + system, averaged over the interval).
- Memory is `MemTotal - MemFree - Buffers - Cached` (Linux "used" memory).
- Disk written counts sectors written to the SD card device (`mmcblk0`), converted to MB.
- Network includes both TX and RX bytes on `eth0`.
- The pipeline uses Python's GIL-aware threading; actual CPU parallelism is limited to I/O overlap.
