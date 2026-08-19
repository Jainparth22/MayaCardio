"""
bench_catchup.py - how long does catch-up take after an outage?

Builds a synthetic backlog (two sensors at 400 Hz, signal mode) in a real
device store, then drains it through the real Syncer into the real cloud
service (loopback HTTP, gzip, fsync'd cloud DB) and reports throughput and the
projected time for a 2-hour outage (5.76 M samples).

    python scripts/bench_catchup.py --seconds 900
"""

import argparse
import json
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from beatkeeper import cloud_server                    # noqa: E402
from beatkeeper.cloud import HttpBackend               # noqa: E402
from beatkeeper.simulator import sample                # noqa: E402
from beatkeeper.store import Store                     # noqa: E402
from beatkeeper.syncer import Syncer                   # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=600, help="seconds of backlog to build")
    a = ap.parse_args()
    tmp = Path(tempfile.mkdtemp(prefix="bk-bench-"))
    store = Store(tmp / "dev.db")
    n = a.seconds * 400
    t = time.time()
    for lo in range(0, n, 4000):
        for sensor in (1, 2):
            store.save_samples("BENCH", sensor, lo,
                               [sample("signal", 7, sensor, i) for i in range(lo, min(lo + 4000, n))])
    store.touch_session("BENCH", time.time())
    rows = 2 * n
    print(f"built backlog: {rows:,} samples ({a.seconds}s of data) in {time.time() - t:.1f}s")

    srv = cloud_server.make_server(str(tmp / "cloud.db"), 18999, host="127.0.0.1")
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    sy = Syncer(store, HttpBackend("http://127.0.0.1:18999"), threading.Event())
    t = time.time()
    while store.counts(None)["backlog"] > 0:
        sy.cycle()
    dt = time.time() - t
    rate = rows / dt
    two_hours = 2 * 3600 * 400 * 2
    print(json.dumps({
        "samples_uploaded": rows, "seconds": round(dt, 1),
        "samples_per_second": round(rate),
        "input_rate_samples_per_second": 800,
        "projected_2h_outage_catchup_minutes": round(two_hours / rate / 60, 1),
        "requirement_minutes": 15,
        "cloud_rows": srv.RequestHandlerClass.state.db.execute(
            "SELECT COUNT(*) FROM samples").fetchone()[0],
        "online_windows_computed_in_cloud": srv.RequestHandlerClass.state.db.execute(
            "SELECT COUNT(*) FROM windows").fetchone()[0],
    }, indent=2))
    srv.shutdown()


if __name__ == "__main__":
    main()
