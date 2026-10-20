"""
cli.py — the `pipeline` command-line interface.

Commands:
  pipeline run              Start the pipeline (foreground).
  pipeline status           Print current status as one JSON line.
  pipeline export --session ID --from device|cloud
                            Print data as JSON lines (samples + losses).
  pipeline results --session ID
                            Print one JSON line per processed window.

The pipeline process writes its PID to data/pipeline.pid so the status
command can detect whether it is running.
"""

import argparse
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path

# Add package root to path when invoked directly.
sys.path.insert(0, str(Path(__file__).parent))

import store
import cloud as cloud_module

DATA_DIR = Path(__file__).parent.parent / "data"
PID_FILE = DATA_DIR / "pipeline.pid"
DB_PATH  = DATA_DIR / "pipeline.db"

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)
log = logging.getLogger("pipeline")


# ── helpers ───────────────────────────────────────────────────────────────────

def _write_pid():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(str(os.getpid()))


def _rm_pid():
    try:
        PID_FILE.unlink()
    except FileNotFoundError:
        pass


def _is_running() -> bool:
    if not PID_FILE.exists():
        return False
    try:
        pid = int(PID_FILE.read_text())
        os.kill(pid, 0)
        return True
    except (ValueError, ProcessLookupError, PermissionError):
        return False


# ── sub-commands ──────────────────────────────────────────────────────────────

def cmd_run(_args):
    """Start the pipeline in the foreground."""
    from engine import Engine

    _write_pid()
    engine = Engine(db_path=DB_PATH)
    engine.start()

    def _shutdown(sig, frame):
        log.info("Signal %d received — shutting down", sig)
        engine.stop()
        _rm_pid()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        while True:
            time.sleep(1)
    finally:
        _rm_pid()


MODE_FILE = DATA_DIR / "pipeline.mode"


def cmd_status(_args):
    """Print pipeline status as one JSON line."""
    conn = store.init_db(DB_PATH)
    sessions = store.list_sessions(conn)
    running = _is_running()

    mode = "stopped"
    if running:
        if MODE_FILE.exists():
            try:
                mode = MODE_FILE.read_text().strip()
            except Exception:
                mode = "online"
        else:
            mode = "online"

    if not sessions:
        print(json.dumps({
            "mode": mode,
            "samples_saved": 0,
            "samples_in_cloud": 0,
            "backlog": 0,
            "running": running,
            "session_id": None,
        }))
        return

    sid = sessions[0][0]   # most recent session
    saved    = store.count_samples(conn, sid)
    in_cloud = store.count_synced_samples(conn, sid)
    backlog  = store.count_unsynced_samples(conn, sid)

    print(json.dumps({
        "mode": mode,
        "samples_saved": saved,
        "samples_in_cloud": in_cloud,
        "backlog": backlog,
        "running": running,
        "session_id": sid,
    }))


def cmd_export(args):
    """Export data as JSON lines (one per sample, then one per loss)."""
    conn = store.init_db(DB_PATH)
    session_id = args.session

    if args.frm == "device":
        # Samples from local DB.
        for sensor, idx, vj in store.get_all_samples(conn, session_id):
            print(json.dumps({
                "sensor": sensor,
                "index":  idx,
                "values": json.loads(vj),
            }))
        # Losses from local DB.
        for sensor, fi, ti, reason in store.get_all_losses(conn, session_id):
            print(json.dumps({
                "sensor":     sensor,
                "from_index": fi,
                "to_index":   ti,
                "reason":     reason,
            }))
    else:  # cloud
        backend = cloud_module.get_backend()
        exp = backend.get_export(session_id)
        # Samples from cloud.
        for s in exp.get("samples", []):
            print(json.dumps({
                "sensor": s["sensor"],
                "index":  s["index"],
                "values": s["values"],
            }))
        # Losses from cloud.
        for l in exp.get("losses", []):
            print(json.dumps({
                "sensor":     l["sensor"],
                "from_index": l["from_index"],
                "to_index":   l.get("to_index"),
                "reason":     l["reason"],
            }))


def cmd_results(args):
    """Print one JSON line per processed window."""
    conn = store.init_db(DB_PATH)
    for wi, rj, mode in store.get_all_windows(conn, args.session):
        result = json.loads(rj)
        result["mode"] = mode
        print(json.dumps(result))


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(prog="pipeline",
                                     description="MayaCardio biosignal pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("run", help="Start the pipeline (foreground)")

    sub.add_parser("status", help="Print current status as one JSON line")

    exp = sub.add_parser("export", help="Export session data as JSON lines")
    exp.add_argument("--session", required=True, metavar="ID")
    exp.add_argument("--from",    required=True, dest="frm",
                     choices=["device", "cloud"])

    res = sub.add_parser("results", help="Print processed window results")
    res.add_argument("--session", required=True, metavar="ID")

    args = parser.parse_args()

    dispatch = {
        "run":     cmd_run,
        "status":  cmd_status,
        "export":  cmd_export,
        "results": cmd_results,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
