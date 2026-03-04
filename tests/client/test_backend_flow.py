"""Backend-style DerpClient integration tests."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any
from unittest.mock import AsyncMock

import pytest

from derp.auth import AuthConfig, EmailConfig, JWTConfig
from derp.auth.jwt import decode_token
from derp.config import DatabaseConfig, DerpConfig, StorageConfig
from derp.derp_client import DerpClient
from derp.orm import DatabaseEngine


async def _create_bucket_with_retry(
    client: DerpClient[Any], bucket: str, retries: int = 30
) -> None:
    for attempt in range(retries):
        try:
            await client.storage.client.create_bucket(Bucket=bucket)
            return
        except Exception:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(0.1)


async def _delete_bucket_with_objects(client: DerpClient[Any], bucket: str) -> None:
    keys = await client.storage.list_files(bucket=bucket)
    for key in keys:
        await client.storage.delete_file(bucket=bucket, key=key)
    await client.storage.client.delete_bucket(Bucket=bucket)


async def _create_backend_tables(db: DatabaseEngine) -> None:
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
            role VARCHAR(50) NOT NULL DEFAULT 'default',
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            last_sign_in_at TIMESTAMP WITH TIME ZONE,
            display_name VARCHAR(255)
        )
    """)

    await db.execute("""
        CREATE TABLE IF NOT EXISTS auth_sessions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            session_id UUID NOT NULL DEFAULT gen_random_uuid(),
            token VARCHAR(255) UNIQUE NOT NULL,
            role VARCHAR(50) NOT NULL DEFAULT 'default',
            revoked BOOLEAN NOT NULL DEFAULT FALSE,
            user_agent TEXT,
            ip_address VARCHAR(45),
            not_after TIMESTAMP WITH TIME ZONE NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        )
    """)

    await db.execute("""
        CREATE TABLE IF NOT EXISTS user_asset_access_logs (
            id SERIAL PRIMARY KEY,
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            session_id UUID NOT NULL REFERENCES auth_sessions(id) ON DELETE CASCADE,
            object_key VARCHAR(512) NOT NULL,
            object_size INTEGER NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        )
    """)


async def _fetch_user_asset_and_log_access(
    *,
    derp: DerpClient[Any],
    access_token: str,
    bucket: str,
    key: str,
) -> dict[str, Any]:
    payload = decode_token(derp.auth._config.jwt, access_token)
    session = await derp.auth.validate_session(access_token)
    if session is None:
        raise ValueError("Session is not active.")

    user = await derp.auth.get_user(payload.sub)
    if user is None:
        raise ValueError("User not found.")

    content = await derp.storage.fetch_file(bucket=bucket, key=key)
    [inserted] = await derp.db.execute(
        """
        INSERT INTO user_asset_access_logs
            (user_id, session_id, object_key, object_size)
        VALUES ($1, $2, $3, $4)
        RETURNING
            id,
            user_id::text AS user_id,
            session_id::text AS session_id,
            object_key,
            object_size
        """,
        [str(user.id), str(session.id), key, len(content)],
    )

    return {
        "log_id": inserted["id"],
        "user_id": inserted["user_id"],
        "session_id": inserted["session_id"],
        "object_key": inserted["object_key"],
        "object_size": inserted["object_size"],
        "preview": content.decode("utf-8"),
    }


def _email_config() -> EmailConfig:
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


def _auth_config() -> AuthConfig:
    return AuthConfig(
        jwt=JWTConfig(
            secret="test-secret-key-for-jwt-testing-purposes-12345",
            algorithm="HS256",
            access_token_expire_minutes=15,
            refresh_token_expire_days=7,
        ),
        enable_signup=True,
        enable_confirmation=False,
        enable_magic_link=True,
    )


@pytest.mark.asyncio
async def test_backend_handler_auth_storage_db_chain(
    clean_database: str,
    minio_server: dict[str, str],
    client_schema_path: str,
    mock_smtp: AsyncMock,
) -> None:
    derp = DerpClient(
        DerpConfig(
            database=DatabaseConfig(
                db_url=clean_database,
                replica_url=clean_database,
                schema_path=client_schema_path,
            ),
            email=_email_config(),
            auth=_auth_config(),
            storage=StorageConfig(
                endpoint_url=minio_server["endpoint_url"],
                access_key_id=minio_server["access_key_id"],
                secret_access_key=minio_server["secret_access_key"],
                use_ssl=False,
                region="us-east-1",
            ),
        )
    )

    bucket = f"assets-{uuid.uuid4().hex}"
    key = ""
    content = b"backend-asset"
    bucket_created = False

    await derp.connect()
    try:
        await _create_backend_tables(derp.db)

        user, _ = await derp.auth.sign_up(
            email="backend@example.com",
            password="password123",
        )

        _, tokens = await derp.auth.sign_in_with_password(
            email="backend@example.com",
            password="password123",
        )

        key = f"users/{user.id}/profile.txt"

        await _create_bucket_with_retry(derp, bucket)
        bucket_created = True
        await derp.storage.upload_file(
            bucket=bucket,
            key=key,
            data=content,
            content_type="text/plain",
        )

        result = await _fetch_user_asset_and_log_access(
            derp=derp,
            access_token=tokens.access_token,
            bucket=bucket,
            key=key,
        )

        assert result["user_id"] == str(user.id)
        assert result["object_key"] == key
        assert result["object_size"] == len(content)
        assert result["preview"] == content.decode("utf-8")

        logs = await derp.db.execute(
            """
            SELECT
                id,
                user_id::text AS user_id,
                session_id::text AS session_id,
                object_key,
                object_size
            FROM user_asset_access_logs
            WHERE id = $1
            """,
            [result["log_id"]],
        )
        assert len(logs) == 1
        [log] = logs

        assert log["user_id"] == str(user.id)
        assert log["session_id"] == result["session_id"]
        assert log["object_key"] == key
        assert log["object_size"] == len(content)
    finally:
        try:
            if bucket_created:
                await _delete_bucket_with_objects(derp, bucket)
        finally:
            await derp.disconnect()
