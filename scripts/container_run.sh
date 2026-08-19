#!/bin/sh
# Runs simulator + cloud + pipeline inside ONE container and measures the pipeline process.
# Invoked by scripts/measure_in_container.sh (see docs/EVIDENCE.md). Linux only.
set -u
SECONDS_TOTAL="${1:-600}"
export PYTHONPATH=/app/src BEATKEEPER_DATA=/tmp/dev BEATKEEPER_CLOUD_URL=http://127.0.0.1:8080 \
       BEATKEEPER_API_KEY=k PYTHONUNBUFFERED=1
mkdir -p /out
python -m beatkeeper.simulator --duration "$SECONDS_TOTAL" --port 7100 > /out/sim.log 2>&1 &
python -m beatkeeper.cloud_server --port 8080 --db /tmp/cloud.db --host 127.0.0.1 > /out/cloud.log 2>&1 &
sleep 1.5
python -m beatkeeper run > /out/pipeline.log 2>&1 &
PIPE=$!
sleep 1.5
echo "pipeline pid $PIPE; cgroup cpu.max: $(cat /sys/fs/cgroup/cpu.max 2>/dev/null) memory.max: $(cat /sys/fs/cgroup/memory.max 2>/dev/null)" > /out/env.txt
python /app/scripts/measure_resources.py --pid "$PIPE" --data /tmp/dev \
    --duration $((SECONDS_TOTAL - 20)) --interval 10 > /out/resources.csv
sleep 20
python -m beatkeeper status > /out/status.json
python -m beatkeeper export --session latest --from device > /out/device.ndjson
python -m beatkeeper export --session latest --from cloud  > /out/cloud.ndjson
python -m beatkeeper.verify --export /out/device.ndjson --samples $((SECONDS_TOTAL * 400)) > /out/verify.json
echo "device lines: $(wc -l < /out/device.ndjson)  cloud lines: $(wc -l < /out/cloud.ndjson)" >> /out/env.txt
du -sk /tmp/dev | cut -f1 | sed 's/^/device dir KB: /' >> /out/env.txt
