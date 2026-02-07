"""Shared fixtures for Derp client tests."""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
import uuid
from collections.abc import Generator, Iterator
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from derp.auth.models import AuthMagicLink, AuthRefreshToken, AuthSession, BaseUser
from derp.orm import Table
from derp.orm.fields import (
    UUID,
    Field,
    ForeignKey,
    ForeignKeyAction,
    Integer,
    Serial,
    Timestamp,
    Varchar,
)

_AuthMagicLink = AuthMagicLink
_AuthRefreshToken = AuthRefreshToken
_AuthSession = AuthSession


class User(BaseUser, table="users"):
    display_name: str | None = Field(Varchar(255), nullable=True)


class UserAssetAccessLog(Table, table="user_asset_access_logs"):
    id: int = Field(Serial(), primary_key=True)
    user_id: uuid.UUID = Field(
        UUID(),
        foreign_key=ForeignKey(User, on_delete=ForeignKeyAction.CASCADE),
        index=True,
    )
    session_id: uuid.UUID = Field(
        UUID(),
        foreign_key=ForeignKey(AuthSession, on_delete=ForeignKeyAction.CASCADE),
        index=True,
    )
    object_key: str = Field(Varchar(512))
    object_size: int = Field(Integer())
    created_at: datetime = Field(Timestamp(with_timezone=True), default="now()")


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
def minio_server(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, str]]:
    if shutil.which("minio") is None:
        pytest.skip("minio binary not found on PATH")

    host = "127.0.0.1"
    api_port = _pick_free_port()
    console_port = _pick_free_port()
    access_key = "minioadmin"
    secret_key = "minioadmin"
    data_dir = tmp_path_factory.mktemp("minio-client-data")

    env = {
        **os.environ,
        "MINIO_ROOT_USER": access_key,
        "MINIO_ROOT_PASSWORD": secret_key,
    }

    process = subprocess.Popen(
        [
            "minio",
            "server",
            str(data_dir),
            "--address",
            f"{host}:{api_port}",
            "--console-address",
            f"{host}:{console_port}",
            "--quiet",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
    )
    try:
        _wait_for_port(host, api_port)
        yield {
            "endpoint_url": f"http://{host}:{api_port}",
            "access_key_id": access_key,
            "secret_access_key": secret_key,
        }
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()


@pytest.fixture
def mock_smtp() -> Generator[AsyncMock, None, None]:
    mock_send = AsyncMock()
    with patch("aiosmtplib.send", mock_send):
        yield mock_send


@pytest.fixture
def client_schema_path() -> str:
    return str(Path(__file__))
