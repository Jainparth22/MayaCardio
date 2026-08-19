#!/bin/sh
# Measure BeatKeeper inside a Linux container limited to Raspberry-Pi-4-class resources
# (4 CPUs, 2 GB RAM). Needs Docker. Run from the repository root:
#
#     sh scripts/measure_in_container.sh 600        # 600 s run -> results in ./evidence_run
#
# Windows (Git Bash): prefix with MSYS_NO_PATHCONV=1
set -e
SECS="${1:-600}"
OUT="$(pwd)/evidence_run"
mkdir -p "$OUT"
docker run --rm --cpus=4 --memory=2g \
    -v "$(pwd):/app:ro" -v "$OUT:/out" \
    python:3.12-slim sh /app/scripts/container_run.sh "$SECS"
echo "results: $OUT/resources.csv  $OUT/verify.json  $OUT/env.txt"
