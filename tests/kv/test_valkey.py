from __future__ import annotations

import shutil
import socket
import subprocess
import time
from collections.abc import Iterator

import pytest

import derp.kv.valkey as valkey_mod


def _wait_for_port(host: str, port: int, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.2)
            if sock.connect_ex((host, port)) == 0:
                return
        time.sleep(0.05)
    raise RuntimeError(f"Valkey did not start on {host}:{port}")


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="module")
def valkey_server() -> Iterator[tuple[str, int]]:
    if shutil.which("valkey-server") is None:
        pytest.skip("valkey-server binary not found on PATH")

    host = "127.0.0.1"
    port = _pick_free_port()
    process = subprocess.Popen(
        [
            "valkey-server",
            "--bind",
            host,
            "--port",
            str(port),
            "--save",
            "",
            "--appendonly",
            "no",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_port(host, port)
        yield host, port
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()


@pytest.mark.asyncio
async def test_valkey_store_roundtrip(valkey_server: tuple[str, int]) -> None:
    host, port = valkey_server
    config = valkey_mod.ValkeyConfig(host=host, port=port)
    store = valkey_mod.ValkeyStore(config)
    await store.connect()

    await store.set(b"a", b"1")
    assert await store.get(b"a") == b"1"

    await store.mset([(b"b", b"2"), (b"c", b"3")], ttl=10)
    assert await store.mget([b"a", b"b", b"c"]) == [b"1", b"2", b"3"]

    assert await store.exists(b"b") is True
    assert await store.delete_many([b"a", b"b"]) == 2
    assert await store.get(b"a") is None

    await store.set(b"user:1", b"1")
    await store.set(b"user:2", b"2")
    keys = [key async for key in store.scan(prefix=b"user:")]
    assert set(keys) == {b"user:1", b"user:2"}

    await store.disconnect()
