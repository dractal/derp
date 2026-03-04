"""Shared fixtures for payments tests."""

from __future__ import annotations

import shutil
import socket
import subprocess
import time
from collections.abc import Generator

import pytest

STRIPE_MOCK_PORT = 12111


def _port_is_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) == 0


@pytest.fixture(scope="session")
def stripe_mock() -> Generator[None, None, None]:
    """Ensure stripe-mock is running on localhost:12111."""
    if _port_is_open(STRIPE_MOCK_PORT):
        yield
        return

    if shutil.which("stripe-mock") is None:
        pytest.skip("stripe-mock is not installed")

    proc = subprocess.Popen(
        ["stripe-mock", "-http-port", str(STRIPE_MOCK_PORT)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(50):
        if _port_is_open(STRIPE_MOCK_PORT):
            break
        time.sleep(0.1)
    else:
        proc.kill()
        pytest.fail("stripe-mock did not start within 5 seconds")

    yield

    proc.terminate()
    proc.wait(timeout=5)
