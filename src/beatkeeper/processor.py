"""
processor.py - the signal-processing core, shared by the device AND the cloud.

One implementation, two deployment sites: offline mode runs it on the device,
online mode runs it in the cloud server. Because both use this exact class,
"same logic online and offline" is true by construction, not by discipline.

Streaming design
----------------
Samples arrive in order, possibly with holes (lost ranges), possibly in odd
interleavings of the two sensors (cloud catch-up). For each sensor we track a
*resolved watermark*: the first index that is neither present nor known-lost.
A window is emitted only once both sensors are resolved past its end, so a
window is never computed from "data that simply has not arrived yet".

Heart-rate peaks are found by a streaming detector (monotonic-deque sliding
maximum), O(1) amortised per sample, instead of re-scanning 2 000 x 800
samples every second.

Rules implemented (windows of 400 samples, 400 Hz):
  * per window: missing_1/2, complete, mean/min/max per channel, magnitude
  * peak i: 800 samples before and 1 after present; larger than both
    neighbours; larger than half the max of the 800 before; >= 100 samples
    after the previous peak
  * heart_rate(w) = 60 / mean gap of peaks in samples 400(w-4)..400(w+1)-1;
    None if w < 4, any of those 2000 samples missing, or < 2 peaks
"""

from __future__ import annotations

import math
from collections import deque
from typing import Dict, List, Optional, Tuple

RATE = 400
WINDOW_SIZE = 400
HR_HISTORY = 800          # samples that must precede a peak
MIN_PEAK_GAP = 100        # refractory period, in samples
HR_SPAN_WINDOWS = 5       # window w looks at windows w-4 .. w


def _r1(v: Optional[float]) -> Optional[float]:
    return None if v is None else round(float(v), 1)


def _stats(values: List[int]) -> dict:
    if not values:
        return {"mean": None, "min": None, "max": None}
    return {"mean": _r1(sum(values) / len(values)),
            "min": min(values), "max": max(values)}


class PeakDetector:
    """Streaming implementation of the 4-condition peak rule."""

    def __init__(self, start_index: int = 0):
        self.pos = start_index            # next index to evaluate
        self._run = 0                     # consecutive available samples before pos
        self._prev: Optional[int] = None  # value at pos-1 (None if missing)
        self._dq: deque = deque()         # (index, value), decreasing values
        self._last_peak: Optional[int] = None
        self.peaks: List[int] = []

    def step(self, v: Optional[int], nxt: Optional[int]) -> None:
        """Evaluate index `pos` (value v) given the value after it (nxt)."""
        i = self.pos
        if (v is not None and nxt is not None
                and self._run >= HR_HISTORY
                and self._prev is not None
                and v > self._prev and v > nxt
                and v > self._dq[0][1] / 2
                and (self._last_peak is None
                     or i - self._last_peak >= MIN_PEAK_GAP)):
            self.peaks.append(i)
            self._last_peak = i
        # advance state past i
        if v is None:
            self._run = 0
            self._prev = None
            self._dq.clear()
        else:
            self._run += 1
            self._prev = v
            while self._dq and self._dq[-1][1] <= v:
                self._dq.pop()
            self._dq.append((i, v))
            while self._dq[0][0] < i - HR_HISTORY + 1:
                self._dq.popleft()
        self.pos = i + 1

    def prune(self, before_index: int) -> None:
        while self.peaks and self.peaks[0] < before_index:
            self.peaks.pop(0)


class SessionProcessor:
    """
    Turns a stream of samples + loss ranges for ONE session into window results.

    first_window / emit_from support crash recovery: replay stored samples from
    a few windows before `emit_from` to warm the detector, but only emit
    results for windows >= emit_from.
    """

    def __init__(self, session_id: str, first_window: int = 0,
                 emit_from: Optional[int] = None):
        self.session_id = session_id
        self.emit_from = first_window if emit_from is None else emit_from
        self.next_window = first_window
        self._samples: Dict[int, Dict[int, tuple]] = {1: {}, 2: {}}
        self._lost: Dict[int, List[Tuple[int, int]]] = {1: [], 2: []}
        base = WINDOW_SIZE * first_window
        self._resolved = {1: base, 2: base}
        self._detector = PeakDetector(base)
        self._missing1: Dict[int, int] = {}      # window -> missing sensor-1 count
        self.closed = False

    # -- input ----------------------------------------------------------------

    def add_sample(self, sensor: int, index: int, values) -> None:
        if index < self._resolved[sensor]:
            return                                    # duplicate / already resolved
        self._samples[sensor][index] = tuple(values)

    def add_samples(self, sensor: int, first_index: int, rows) -> None:
        for k, row in enumerate(rows):
            self.add_sample(sensor, first_index + k, row)

    def add_loss(self, sensor: int, from_index: int, to_index: Optional[int]) -> None:
        if to_index is None:
            return                                    # open-ended: session is over
        self._lost[sensor].append((from_index, to_index))

    def close(self) -> None:
        """No more data will ever arrive: sample 'after the end' counts as missing."""
        self.closed = True

    # -- watermark --------------------------------------------------------------

    def _advance(self, sensor: int) -> int:
        ru = self._resolved[sensor]
        present = self._samples[sensor]
        lost = self._lost[sensor]
        while True:
            if ru in present:
                ru += 1
                continue
            for a, b in lost:
                if a <= ru <= b:
                    ru = b + 1
                    break
            else:
                break
        self._resolved[sensor] = ru
        return ru

    # -- output -----------------------------------------------------------------

    def pop_ready(self, mode: str = "offline") -> List[dict]:
        """Return results for every window that can now be finalised."""
        out: List[dict] = []
        while True:
            w = self.next_window
            end = WINDOW_SIZE * (w + 1)
            ru1, ru2 = self._advance(1), self._advance(2)
            need1 = end if self.closed else end + 1   # +1: peak needs the next sample
            if ru1 < need1 or ru2 < end:
                break
            res = self._finalise(w)
            self.next_window += 1
            if w >= self.emit_from:
                res["mode"] = mode
                out.append(res)
        return out

    def _finalise(self, w: int) -> dict:
        lo, hi = WINDOW_SIZE * w, WINDOW_SIZE * (w + 1)
        s1, s2 = self._samples[1], self._samples[2]

        # feed the peak detector over this window
        det = self._detector
        while det.pos < hi:
            i = det.pos
            r = s1.get(i)
            n = s1.get(i + 1)
            det.step(None if r is None else r[1], None if n is None else n[1])

        rows1 = [s1[i] for i in range(lo, hi) if i in s1]
        rows2 = [s2[i] for i in range(lo, hi) if i in s2]
        miss1, miss2 = WINDOW_SIZE - len(rows1), WINDOW_SIZE - len(rows2)
        self._missing1[w] = miss1

        mags = [math.sqrt(r[0] ** 2 + r[1] ** 2) for r in rows2]
        result = {
            "window": w,
            "missing_1": miss1,
            "missing_2": miss2,
            "complete": miss1 == 0 and miss2 == 0,
            "sensor_1": {f"ch{c + 1}": _stats([r[c] for r in rows1]) for c in range(3)},
            "sensor_2": {f"ch{c + 1}": _stats([r[c] for r in rows2]) for c in range(2)},
            "magnitude": _r1(sum(mags) / len(mags)) if mags else None,
            "heart_rate": self._heart_rate(w),
        }

        # bounded memory: drop everything this window no longer needs
        for i in range(lo, hi):
            s1.pop(i, None)
            s2.pop(i, None)
        det.prune(WINDOW_SIZE * (w - HR_SPAN_WINDOWS + 2))
        self._missing1.pop(w - HR_SPAN_WINDOWS, None)
        self._lost[1] = [x for x in self._lost[1] if x[1] >= lo]
        self._lost[2] = [x for x in self._lost[2] if x[1] >= lo]
        return result

    def _heart_rate(self, w: int) -> Optional[float]:
        if w < HR_SPAN_WINDOWS - 1:
            return None
        span = range(w - HR_SPAN_WINDOWS + 1, w + 1)
        if any(self._missing1.get(k, 1) != 0 for k in span):
            return None
        lo, hi = WINDOW_SIZE * (w - HR_SPAN_WINDOWS + 1), WINDOW_SIZE * (w + 1)
        peaks = [p for p in self._detector.peaks if lo <= p < hi]
        if len(peaks) < 2:
            return None
        mean_gap_s = (peaks[-1] - peaks[0]) / (len(peaks) - 1) / RATE
        return _r1(60.0 / mean_gap_s)
