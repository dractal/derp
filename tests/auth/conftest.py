"""Pytest fixtures for auth tests."""

from __future__ import annotations

import shutil
import socket
import subprocess
import time
from collections.abc import AsyncGenerator, Generator, Iterator
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from derp.auth import AuthConfig, EmailConfig, JWTConfig
from derp.auth.models import AuthSession, BaseUser
from derp.config import DerpConfig, ValkeyConfig
from derp.derp_client import DerpClient
from derp.kv.valkey import ValkeyClient
from derp.orm import DatabaseConfig, DatabaseEngine
from derp.orm.fields import JSONB, Field


class User(BaseUser, table="users"):
    user_metadata: dict[str, Any] | None = Field(JSONB(), nullable=True)


class AuthSession(AuthSession, table="auth_sessions"):
    pass


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


@pytest.fixture
async def kv_client(
    valkey_server: tuple[str, int],
) -> AsyncGenerator[ValkeyClient, None]:
    host, port = valkey_server
    client = ValkeyClient(ValkeyConfig(host=host, port=port))
    await client.connect()
    yield client
    await client.disconnect()


@pytest.fixture
def jwt_config() -> JWTConfig:
    """JWT config with environment variable set."""
    return JWTConfig(
        secret="test-secret-key-for-jwt-testing-purposes-12345",
        algorithm="HS256",
        access_token_expire_minutes=15,
        refresh_token_expire_days=7,
    )


@pytest.fixture
def email_config() -> EmailConfig:
    """Create an SMTP config for testing."""
    return EmailConfig(
        site_name="Test Site",
        site_url="http://localhost:3000",
        from_email="test@example.com",
        smtp_host="localhost",
        smtp_port=587,
        smtp_user="test@example.com",
        smtp_password="test-password",
        use_tls=True,
        start_tls=False,
    )


@pytest.fixture
def auth_config(jwt_config: JWTConfig) -> AuthConfig[User]:
    """Create an auth config for testing."""
    return AuthConfig(jwt=jwt_config, enable_magic_link=True)


@pytest.fixture
async def derp(
    clean_database: str,
    auth_config: AuthConfig,
    email_config: EmailConfig,
    kv_client: ValkeyClient,
) -> AsyncGenerator[DerpClient[User], None]:
    """Create a Derp client for testing."""
    derp = DerpClient(
        config=DerpConfig(
            database=DatabaseConfig(
                db_url=clean_database,
                replica_url=clean_database,
                schema_path=str(Path(__file__)),
            ),
            email=email_config,
            auth=auth_config,
        ),
    )
    await derp.connect()
    await _create_auth_tables(derp.db)
    derp.auth.set_kv(kv_client)
    yield derp
    await derp.disconnect()


async def _create_auth_tables(db: DatabaseEngine) -> None:
    """Create auth tables for testing."""
    # Create tables in order respecting foreign keys
    await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            email VARCHAR(255) UNIQUE NOT NULL,
            email_confirmed_at TIMESTAMP WITH TIME ZONE,
            encrypted_password TEXT,
            provider VARCHAR(50) NOT NULL DEFAULT 'email',
            provider_id VARCHAR(255),
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            is_superuser BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            last_sign_in_at TIMESTAMP WITH TIME ZONE,
            user_metadata JSONB
        )
    """)

    await db.execute("""
        CREATE TABLE IF NOT EXISTS auth_sessions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            session_id UUID NOT NULL DEFAULT gen_random_uuid(),
            token VARCHAR(255) UNIQUE NOT NULL,
            revoked BOOLEAN NOT NULL DEFAULT FALSE,
            user_agent TEXT,
            ip_address VARCHAR(45),
            not_after TIMESTAMP WITH TIME ZONE NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        )
    """)


async def get_confirmation_token(
    kv: ValkeyClient, prefix: str, user_id: str
) -> str | None:
    """Scan KV for a confirmation token belonging to the given user_id."""
    key_prefix = f"{prefix}:confirmation:".encode()
    async for key in kv.scan(prefix=key_prefix):
        value = await kv.get(key)
        if value is not None and value.decode() == user_id:
            # Strip the prefix to get just the token
            return key.decode().removeprefix(f"{prefix}:confirmation:")
    return None


@pytest.fixture
def mock_smtp() -> Generator[AsyncMock, None, None]:
    """Mock SMTP client for testing."""
    mock_send = AsyncMock()
    with patch("aiosmtplib.send", mock_send):
        yield mock_send
