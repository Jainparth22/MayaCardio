import socket
import threading
import time

import pytest

from beatkeeper import simulator


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def start_gateway():
    """Start an in-process simulator gateway; returns (gateway, port)."""
    def _start(**kw):
        params = dict(mode="ramp", seed=7, duration=30, speed=20.0)
        params.update(kw)
        gw = simulator.Gateway(**params)
        port = free_port()
        threading.Thread(target=simulator.serve, args=(gw, port), daemon=True).start()
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                break
            except OSError:
                time.sleep(0.05)
        return gw, port
    return _start
