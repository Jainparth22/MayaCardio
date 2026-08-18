import threading

from beatkeeper import network as n
from beatkeeper.network import NetworkMonitor


def make(seen):
    return NetworkMonitor(lambda: True, seen.append, threading.Event(), "online")


def test_needs_consecutive_failures_to_go_offline():
    seen = []
    m = make(seen)
    for ok in [False, False, True, False, False, True]:
        m.observe(ok)
    assert seen == []                       # a flaky link does not flap the mode
    for _ in range(3):
        m.observe(False)
    assert seen == ["offline"] and m.mode == "offline"


def test_recovers_after_consecutive_successes():
    seen = []
    m = make(seen)
    for _ in range(3):
        m.observe(False)
    for ok in [True, True, False, True, True]:
        m.observe(ok)
    assert seen == ["offline"]
    for _ in range(3):
        m.observe(True)
    assert seen == ["offline", "online"]


def test_worst_case_switch_time_is_under_10_seconds():
    assert n.FAIL_THRESHOLD * (n.PROBE_TIMEOUT + n.PROBE_INTERVAL) < 10
    assert n.PASS_THRESHOLD * (n.PROBE_TIMEOUT + n.PROBE_INTERVAL) < 10
