"""
verify.py - ground-truth checker for an export.

Given an NDJSON export (samples then losses) for one session and the
simulator's parameters, decide whether the "never lose data" promise held:

  wrong_values   a stored sample differs from the truth            -> must be 0
  duplicates     an index stored more than once                    -> must be 0
  silent_loss    an index neither stored nor listed as lost        -> must be 0
  flagged_lost   indices honestly listed as lost (informational)
  lost_but_present  listed lost yet also stored (harmless, reported)

    python -m beatkeeper.verify --export out.ndjson --mode signal --seed 7 --samples 96000
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Dict, Iterable, List, Optional, Set, Tuple

from .simulator import sample


def check(lines: Iterable[str], mode: str, seed: int, samples_per_sensor: int,
          value_stride: int = 1) -> dict:
    stored: Dict[int, Set[int]] = {1: set(), 2: set()}
    dup = {1: 0, 2: 0}
    wrong = 0
    checked = 0
    losses: List[Tuple[int, int, Optional[int], str]] = []
    for ln in lines:
        if not ln.strip():
            continue
        d = json.loads(ln)
        if "values" in d:
            s, i = d["sensor"], d["index"]
            if i in stored[s]:
                dup[s] += 1
            stored[s].add(i)
            if i % value_stride == 0:
                checked += 1
                if d["values"] != sample(mode, seed, s, i):
                    wrong += 1
        else:
            losses.append((d["sensor"], d["from_index"], d["to_index"], d["reason"]))

    lost: Dict[int, Set[int]] = {1: set(), 2: set()}
    reasons: Dict[str, int] = {}
    for s, a, b, why in losses:
        hi = samples_per_sensor - 1 if b is None else min(b, samples_per_sensor - 1)
        n = max(0, hi - a + 1)
        reasons[why] = reasons.get(why, 0) + n
        lost[s].update(range(a, hi + 1))

    silent = {s: [] for s in (1, 2)}
    for s in (1, 2):
        for i in range(samples_per_sensor):
            if i not in stored[s] and i not in lost[s]:
                silent[s].append(i)

    def ranges(xs):
        out, start, prev = [], None, None
        for x in xs:
            if start is None:
                start = prev = x
            elif x == prev + 1:
                prev = x
            else:
                out.append((start, prev)); start = prev = x
        if start is not None:
            out.append((start, prev))
        return out

    report = {
        "expected_per_sensor": samples_per_sensor,
        "stored": {s: len(stored[s]) for s in (1, 2)},
        "flagged_lost": {s: len(lost[s] - stored[s]) for s in (1, 2)},
        "loss_reasons": reasons,
        "wrong_values": wrong,
        "values_checked": checked,
        "duplicates": dup[1] + dup[2],
        "silent_loss": {s: len(silent[s]) for s in (1, 2)},
        "silent_loss_ranges": {s: ranges(silent[s])[:10] for s in (1, 2)},
        "lost_but_present": sum(len(lost[s] & stored[s]) for s in (1, 2)),
    }
    report["pass"] = (wrong == 0 and report["duplicates"] == 0
                      and not any(silent[s] for s in (1, 2)))
    return report


def detect_encoding(path: str) -> str:
    """Windows PowerShell's `>` writes UTF-16 (or UTF-8 with a BOM), not plain UTF-8."""
    with open(path, "rb") as f:
        head = f.read(2)
    return "utf-16" if head in (b"\xff\xfe", b"\xfe\xff") else "utf-8-sig"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--export", required=True, help="NDJSON file, or - for stdin")
    ap.add_argument("--mode", default="signal", choices=["signal", "ramp"])
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--samples", type=int, required=True, help="samples per sensor in the session")
    ap.add_argument("--stride", type=int, default=1, help="value-check every Nth sample")
    a = ap.parse_args()
    f = sys.stdin if a.export == "-" else open(a.export, encoding=detect_encoding(a.export))
    rep = check(f, a.mode, a.seed, a.samples, a.stride)
    print(json.dumps(rep, indent=2))
    sys.exit(0 if rep["pass"] else 1)


if __name__ == "__main__":
    main()
