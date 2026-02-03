"""Pytest fixtures for auth tests."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Generator
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from derp.auth import AuthConfig, EmailConfig, JWTConfig
from derp.auth.models import BaseUser
from derp.config import DerpConfig
from derp.derp_client import DerpClient
from derp.orm import DatabaseConfig, DatabaseEngine
from derp.orm.fields import JSONB, Field


class User(BaseUser, table="users"):
    user_metadata: dict[str, Any] | None = Field(JSONB(), nullable=True)


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
        confirm_email_url="{site_url}/auth/confirm",
        recovery_url="{site_url}/auth/recovery",
        magic_link_url="{site_url}/auth/magic-link",
        enable_signup=True,
        enable_confirmation=True,
        enable_magic_link=True,
        from_email="test@example.com",
        smtp_host="localhost",
        smtp_port=587,
        smtp_user="test@example.com",
        smtp_password="test-password",
        use_tls=True,
        start_tls=False,
    )


@pytest.fixture
def auth_config(jwt_config: JWTConfig, email_config: EmailConfig) -> AuthConfig[User]:
    """Create an auth config for testing."""
    return AuthConfig(email=email_config, jwt=jwt_config, user_table_name="users")


@pytest.fixture
async def derp(
    clean_database: str, auth_config: AuthConfig
) -> AsyncGenerator[DerpClient[User], None]:
    """Create a Derp client for testing."""
    derp = DerpClient(
        config=DerpConfig(
            database=DatabaseConfig(
                db_url=clean_database,
                replica_url=clean_database,
                schema_path=str(Path(__file__)),
            ),
            auth=auth_config,
        ),
    )
    await derp.connect()
    await _create_auth_tables(derp.db)
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
            recovery_token VARCHAR(255),
            recovery_sent_at TIMESTAMP WITH TIME ZONE,
            confirmation_token VARCHAR(255),
            confirmation_sent_at TIMESTAMP WITH TIME ZONE,
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
            user_agent TEXT,
            ip_address VARCHAR(45),
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            not_after TIMESTAMP WITH TIME ZONE NOT NULL
        )
    """)

    await db.execute("""
        CREATE TABLE IF NOT EXISTS auth_refresh_tokens (
            id SERIAL PRIMARY KEY,
            session_id UUID NOT NULL REFERENCES auth_sessions(id) ON DELETE CASCADE,
            token VARCHAR(255) UNIQUE NOT NULL,
            revoked BOOLEAN NOT NULL DEFAULT FALSE,
            parent VARCHAR(255),
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        )
    """)

    await db.execute("""
        CREATE TABLE IF NOT EXISTS auth_magic_links (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            email VARCHAR(255) NOT NULL,
            token VARCHAR(255) UNIQUE NOT NULL,
            used BOOLEAN NOT NULL DEFAULT FALSE,
            expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        )
    """)


@pytest.fixture
def mock_smtp() -> Generator[AsyncMock, None, None]:
    """Mock SMTP client for testing."""
    mock_send = AsyncMock()
    with patch("aiosmtplib.send", mock_send):
        yield mock_send
