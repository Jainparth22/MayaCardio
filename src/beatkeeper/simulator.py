"""
simulator.py - a two-sensor biosignal gateway with built-in fault injection.

Why it exists: you cannot claim "never loses data" without a way to make
things go wrong on demand and compare against ground truth. This simulator
speaks the gateway protocol and is *deterministic*: sample(mode, seed, sensor,
index) is a pure function, so a checker can recompute the truth for any index.

Protocol (newline-delimited JSON over TCP, ONE client at a time)
    -> {"cmd": "read", "sensor": 1[, "from_index": N]}
    <- {"session_id", "sensor", "first_index", "count", "lost": [...],
        "session_end", "crc32", "data": [[...], ...]}
    crc32 = zlib CRC-32 of the data text exactly as sent. At most 1 s of data
    per reply. Only the last 10 s of each sensor is buffered; older unread data
    is reported in `lost` (reason: buffer_overflow).

Signals (mode=signal)
    sensor 1: ch2 = ECG-like (one sharp R-peak per beat); ch1 = 0.6*ch2; ch3 = 1.3*ch2
              heart rate 60 bpm (min 0-2), 90 bpm (min 2-4), 72 bpm (min 4-6), repeating
    sensor 2: ch1 ~ 400 000, ch2 ~ 150 000, slowly varying
Mode ramp: sensor 1 ch c = (3i + c) mod 2^23, sensor 2 ch c = (2i + c) mod 2^23

Faults (times are simulated seconds): --corrupt-rate, --dup-rate,
    --stall T:D, --dropout SENSOR:T:D, --restart T
"""

from __future__ import annotations

import argparse
import json
import math
import random
import socket
import time
import zlib
from typing import List, Optional, Tuple

RATE = 400
BUFFER_SECONDS = 10
MAX_PER_REPLY = RATE
MOD24 = 8_388_608
CYCLE = 6 * 60 * RATE
BLOCKS = [(0, 60), (48_000, 90), (96_000, 72)]       # (start sample in cycle, bpm)


# -- ground truth (pure functions) ------------------------------------------------

def _noise(seed: int, sensor: int, i: int) -> int:
    x = (i * 0x9E3779B1 + seed * 0x85EBCA6B + sensor * 0xC2B2AE35) & 0xFFFFFFFF
    x ^= x >> 15
    x = (x * 0x2C1B3C6D) & 0xFFFFFFFF
    x ^= x >> 12
    return (x % 31) - 15


def _clamp(v: int) -> int:
    return max(-MOD24 // 2, min(MOD24 // 2 - 1, v))


def bpm_at(i: int) -> int:
    c = i % CYCLE
    bpm = 60
    for start, b in BLOCKS:
        if c >= start:
            bpm = b
    return bpm


def _ecg(seed: int, i: int) -> int:
    c = i % CYCLE
    start, bpm = max(((s, b) for s, b in BLOCKS if s <= c), key=lambda t: t[0])
    spacing = RATE * 60 / bpm
    k = round((c - start) / spacing)
    d = (c - start) - round(k * spacing)
    v = 1000 + _noise(seed, 1, i)
    if abs(d) <= 20:
        v += round(3000 * math.exp(-((d / 4.0) ** 2)))        # sharp R peak
    if 40 <= d <= 160:
        v += round(350 * math.exp(-(((d - 90) / 25.0) ** 2)))  # T wave
    return v


def sample(mode: str, seed: int, sensor: int, i: int) -> List[int]:
    if mode == "ramp":
        n = 3 if sensor == 1 else 2
        m = 3 if sensor == 1 else 2
        return [(m * i + c) % MOD24 for c in range(1, n + 1)]
    if sensor == 1:
        ch2 = _ecg(seed, i)
        return [_clamp(int(ch2 * 0.6)), _clamp(ch2), _clamp(int(ch2 * 1.3))]
    return [_clamp(400_000 + round(8000 * math.sin(2 * math.pi * i / 16000)) + _noise(seed, 2, i) * 10),
            _clamp(150_000 + round(3000 * math.sin(2 * math.pi * i / 24000 + 1)) + _noise(seed, 3, i))]


# -- gateway ------------------------------------------------------------------------

class Gateway:
    def __init__(self, mode, seed, duration, speed, corrupt_rate=0.0, dup_rate=0.0,
                 stalls=(), dropouts=(), restarts=()):
        self.mode, self.seed, self.duration, self.speed = mode, seed, duration, speed
        self.corrupt_rate, self.dup_rate = corrupt_rate, dup_rate
        self.stalls = sorted(stalls)                      # (t, dur)
        self.dropouts = list(dropouts)                    # (sensor, t, dur)
        self.restarts = sorted(restarts)
        self.rng = random.Random(seed)
        self._wall0 = time.monotonic()
        self.session_n = 0
        self._new_session(0.0)

    def sim_time(self) -> float:
        return (time.monotonic() - self._wall0) * self.speed

    def _new_session(self, at: float) -> None:
        self.session_n += 1
        self.session_id = "%04x%04x" % (self.seed & 0xFFFF, self.session_n)
        self.t0 = at
        self.cursor = {1: 0, 2: 0}
        self.total = int(self.duration * RATE)
        self.drops = [(s, int(t * RATE), int((t + d) * RATE) - 1)
                      for s, t, d in self.dropouts] if self.session_n == 1 else []

    def _maybe_restart(self) -> None:
        now = self.sim_time()
        while self.restarts and now >= self.restarts[0]:
            self.restarts.pop(0)
            self._new_session(now)

    def stall_until(self) -> Optional[float]:
        now = self.sim_time()
        for t, d in self.stalls:
            if t <= now < t + d:
                return (t + d - now) / self.speed         # wall seconds to wait
        return None

    def read(self, sensor: int, from_index: Optional[int]) -> List[str]:
        """Return the reply line(s) to send (2 lines if the duplicate fault fires)."""
        self._maybe_restart()
        avail = min(int((self.sim_time() - self.t0) * RATE), self.total)
        oldest = max(0, avail - BUFFER_SECONDS * RATE)
        cur = self.cursor[sensor] if from_index is None else max(0, from_index)
        cur = min(cur, avail)                             # cannot read the future
        lost: List[dict] = []
        if cur < oldest:
            lost.append({"from_index": cur, "to_index": oldest - 1, "reason": "buffer_overflow"})
            cur = oldest
        limit = avail
        for s, a, b in self.drops:
            if s != sensor or b < cur:
                continue
            if cur >= a:
                if avail <= b:                            # outage still in progress
                    limit = cur
                else:
                    lost.append({"from_index": cur, "to_index": b, "reason": "sensor_dropout"})
                    cur = b + 1
            else:
                limit = min(limit, a)
        count = max(0, min(MAX_PER_REPLY, limit - cur))
        data = [sample(self.mode, self.seed, sensor, cur + k) for k in range(count)]
        data_text = json.dumps(data)
        crc = "%08x" % (zlib.crc32(data_text.encode()) & 0xFFFFFFFF)
        if self.rng.random() < self.corrupt_rate:
            crc = "%08x" % (int(crc, 16) ^ 0xDEADBEEF)
        self.cursor[sensor] = cur + count
        ended = cur + count >= self.total and avail >= self.total
        line = ('{"session_id": "%s", "sensor": %d, "first_index": %d, "count": %d, '
                '"lost": %s, "session_end": %s, "crc32": "%s", "data": %s}'
                % (self.session_id, sensor, cur, count, json.dumps(lost),
                   "true" if ended else "false", crc, data_text))
        lines = [line]
        if self.rng.random() < self.dup_rate:
            lines.append(line)
        return lines


def serve(gw: Gateway, port: int, host: str = "127.0.0.1") -> None:
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(4)
    print(f"[sim] listening on {host}:{port} mode={gw.mode} seed={gw.seed} "
          f"duration={gw.duration}s speed={gw.speed}x", flush=True)
    while True:
        conn, _ = srv.accept()                            # one client at a time
        try:
            f = conn.makefile("rwb")
            for raw in f:
                req = json.loads(raw)
                wait = gw.stall_until()
                if wait:
                    time.sleep(wait)                      # "stop answering for a while"
                for line in gw.read(req["sensor"], req.get("from_index")):
                    f.write(line.encode() + b"\n")
                f.flush()
        except (OSError, ValueError):
            pass
        finally:
            conn.close()


def _pair(s: str) -> Tuple[float, ...]:
    return tuple(float(x) for x in s.split(":"))


def main() -> None:
    ap = argparse.ArgumentParser(description="BeatKeeper sensor-gateway simulator")
    ap.add_argument("--mode", choices=["signal", "ramp"], default="signal")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--duration", type=float, default=3600, help="session length, sim seconds")
    ap.add_argument("--port", type=int, default=7100)
    ap.add_argument("--speed", type=float, default=1.0, help="time acceleration (tests)")
    ap.add_argument("--corrupt-rate", type=float, default=0.0)
    ap.add_argument("--dup-rate", type=float, default=0.0)
    ap.add_argument("--stall", action="append", default=[], metavar="T:D")
    ap.add_argument("--dropout", action="append", default=[], metavar="SENSOR:T:D")
    ap.add_argument("--restart", action="append", default=[], type=float, metavar="T")
    a = ap.parse_args()
    gw = Gateway(a.mode, a.seed, a.duration, a.speed, a.corrupt_rate, a.dup_rate,
                 [_pair(s) for s in a.stall],
                 [(int(p[0]), p[1], p[2]) for p in map(_pair, a.dropout)], a.restart)
    serve(gw, a.port)


if __name__ == "__main__":
    main()
