"""Shared fixtures for Derp client tests."""

from __future__ import annotations

import shutil
import socket
import subprocess
import time
from collections.abc import Generator, Iterator
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
import requests
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from derp.auth.models import AuthSession, AuthUser
from derp.orm import (
    UUID,
    Field,
    Index,
    Integer,
    Serial,
    TimestampTZ,
    Varchar,
)
from derp.orm.table import Table


class UserAssetAccessLog(Table, table="user_asset_access_logs"):
    id: Serial = Field(primary=True)
    user_id: UUID = Field(
        foreign_key=AuthUser.id,
        on_delete="cascade",
    )
    session_id: UUID = Field(
        foreign_key=AuthSession.id,
        on_delete="cascade",
    )
    object_key: Varchar[512] = Field()
    object_size: Integer = Field()
    created_at: TimestampTZ = Field(default="now()")

    @classmethod
    def indexes(cls) -> list[Index]:
        return [Index(cls.user_id), Index(cls.session_id)]


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_port(host: str, port: int, timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.2)
            if sock.connect_ex((host, port)) == 0:
                return
        time.sleep(0.05)
    raise RuntimeError(f"Service did not start on {host}:{port}")


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


@pytest.fixture(scope="module")
def moto_server() -> Iterator[dict[str, str]]:
    """Start a moto server for S3-compatible storage tests."""
    server = ThreadedMotoServer(port=0, verbose=False)
    server.start()
    endpoint = f"http://localhost:{server._server.server_address[1]}"  # ty:ignore[possibly-missing-attribute]
    try:
        yield {
            "endpoint_url": endpoint,
            "access_key_id": "testing",
            "secret_access_key": "testing",
        }
    finally:
        server.stop()


@pytest.fixture(autouse=True)
def _reset_moto(moto_server: dict[str, str]) -> Iterator[None]:
    """Reset moto state before each test."""
    requests.post(f"{moto_server['endpoint_url']}/moto-api/reset")
    yield


@pytest.fixture
def mock_smtp() -> Generator[AsyncMock, None, None]:
    mock_send = AsyncMock()
    with patch("aiosmtplib.send", mock_send):
        yield mock_send


@pytest.fixture
def client_schema_path() -> str:
    return str(Path(__file__))
