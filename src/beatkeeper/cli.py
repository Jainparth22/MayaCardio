"""
beatkeeper - command line interface.

    beatkeeper run [--gateway HOST:PORT]    start collecting (foreground)
    beatkeeper status                       one JSON line
    beatkeeper export  --session ID --from device|cloud     NDJSON samples, then losses
    beatkeeper results --session ID [--from device|cloud]   NDJSON, one line per window+mode

`--session latest` selects the most recent session.
Data lives in $BEATKEEPER_DATA (default ./data).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path

from . import cloud as cloud_mod
from .store import Store

STALE_AFTER_S = 5.0      # heartbeat older than this => pipeline is not running


def data_dir() -> Path:
    return Path(os.environ.get("BEATKEEPER_DATA", "data"))


def _read_status_file() -> dict:
    try:
        return json.loads((data_dir() / "status.json").read_text())
    except (OSError, ValueError):
        return {}


def cmd_run(args) -> None:
    from .engine import Engine
    host, _, port = args.gateway.partition(":")
    engine = Engine(data_dir(), gateway_host=host or "127.0.0.1", gateway_port=int(port or 7100))
    engine.start()

    def _bye(sig, _frm):
        logging.getLogger("beatkeeper").info("signal %s - shutting down", sig)
        engine.stop()

    signal.signal(signal.SIGINT, _bye)
    signal.signal(signal.SIGTERM, _bye)
    while not engine.stop_event.is_set():
        time.sleep(0.5)


def cmd_status(_args) -> None:
    hb = _read_status_file()
    running = bool(hb.get("running")) and (time.time() - hb.get("updated_at", 0)) < STALE_AFTER_S
    store = Store(data_dir() / "beatkeeper.db")
    sid = hb.get("session_id") or store.latest_session()
    out = {"mode": hb.get("mode") if running else "stopped", "running": running,
           "session_id": sid, **store.counts(sid)}
    print(json.dumps(out))


def _resolve_session(store: Store, value: str) -> str:
    return (store.latest_session() or "") if value == "latest" else value


def cmd_export(args) -> None:
    if args.frm == "device":
        store = Store(data_dir() / "beatkeeper.db")
        sid = _resolve_session(store, args.session)
        out = sys.stdout
        for sensor, idx, vj in store.iter_samples(sid):
            out.write('{"sensor": %d, "index": %d, "values": %s}\n' % (sensor, idx, vj))
        for sensor, a, b, reason in store.losses(sid):
            out.write(json.dumps({"sensor": sensor, "from_index": a, "to_index": b,
                                  "reason": reason}) + "\n")
    else:
        sid = args.session
        if sid == "latest":
            sid = Store(data_dir() / "beatkeeper.db").latest_session() or ""
        for line in cloud_mod.get_backend().stream(f"/v1/export/{sid}"):
            print(line)


def cmd_results(args) -> None:
    if args.frm == "device":
        store = Store(data_dir() / "beatkeeper.db")
        for w in store.windows(_resolve_session(store, args.session)):
            print(json.dumps(w))
    else:
        sid = args.session
        if sid == "latest":
            sid = Store(data_dir() / "beatkeeper.db").latest_session() or ""
        for line in cloud_mod.get_backend().stream(f"/v1/results/{sid}"):
            print(line)


def main(argv=None) -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    ap = argparse.ArgumentParser(prog="beatkeeper",
                                 description="Never lose a heartbeat: durable biosignal pipeline")
    sub = ap.add_subparsers(dest="command", required=True)
    r = sub.add_parser("run", help="start the pipeline (foreground)")
    r.add_argument("--gateway", default="127.0.0.1:7100", metavar="HOST:PORT")
    sub.add_parser("status", help="print status as one JSON line")
    e = sub.add_parser("export", help="export samples + losses as JSON lines")
    e.add_argument("--session", required=True, metavar="ID|latest")
    e.add_argument("--from", dest="frm", required=True, choices=["device", "cloud"])
    s = sub.add_parser("results", help="print per-window results as JSON lines")
    s.add_argument("--session", required=True, metavar="ID|latest")
    s.add_argument("--from", dest="frm", default="device", choices=["device", "cloud"])
    args = ap.parse_args(argv)
    {"run": cmd_run, "status": cmd_status, "export": cmd_export, "results": cmd_results}[
        args.command](args)


if __name__ == "__main__":
    main()
