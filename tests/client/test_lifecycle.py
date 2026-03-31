"""Tests for DerpClient lifecycle and service access behavior."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from derp.ai import AIClient
from derp.config import (
    AIConfig,
    DatabaseConfig,
    DerpConfig,
    KVConfig,
    PaymentsConfig,
    StorageConfig,
    ValkeyConfig,
)
from derp.derp_client import DerpClient


def _config(
    *,
    db_url: str,
    schema_path: str,
    replica_url: str | None = None,
    storage: StorageConfig | None = None,
    kv: KVConfig | None = None,
    payments: PaymentsConfig | None = None,
    ai: AIConfig | None = None,
) -> DerpConfig:
    return DerpConfig(
        database=DatabaseConfig(
            db_url=db_url,
            replica_url=replica_url,
            schema_path=schema_path,
        ),
        storage=storage,
        kv=kv,
        payments=payments,
        ai=ai,
    )


def test_properties_require_active_session(client_schema_path: str) -> None:
    client = DerpClient(
        _config(db_url="postgresql://unused", schema_path=client_schema_path)
    )

    for accessor in (
        lambda c: c.db,
        lambda c: c.storage,
        lambda c: c.auth,
        lambda c: c.kv,
        lambda c: c.payments,
    ):
        with pytest.raises(ValueError, match="Not in a session"):
            accessor(client)


@pytest.mark.asyncio
async def test_connect_enables_access_disconnect_disables_access(
    client_schema_path: str,
) -> None:
    mock_db = MagicMock()
    mock_db.connect = AsyncMock()
    mock_db.disconnect = AsyncMock()

    with patch("derp.derp_client.DatabaseEngine", return_value=mock_db):
        client = DerpClient(
            _config(db_url="postgresql://unused", schema_path=client_schema_path)
        )
        await client.connect()
        assert client.db is mock_db

        await client.disconnect()
        with pytest.raises(ValueError, match="Not in a session"):
            _ = client.db

    mock_db.connect.assert_awaited_once()
    mock_db.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_optional_services_require_config(client_schema_path: str) -> None:
    mock_db = MagicMock()
    mock_db.connect = AsyncMock()
    mock_db.disconnect = AsyncMock()

    with patch("derp.derp_client.DatabaseEngine", return_value=mock_db):
        client = DerpClient(
            _config(db_url="postgresql://unused", schema_path=client_schema_path)
        )
        await client.connect()

        with pytest.raises(ValueError, match="`StorageConfig` was not passed"):
            _ = client.storage
        with pytest.raises(ValueError, match="`AuthConfig` was not passed"):
            _ = client.auth
        with pytest.raises(ValueError, match="`KVConfig` was not passed"):
            _ = client.kv
        with pytest.raises(ValueError, match="`PaymentsConfig` was not passed"):
            _ = client.payments

        await client.disconnect()


@pytest.mark.asyncio
async def test_payments_service_available_in_session(client_schema_path: str) -> None:
    mock_db = MagicMock()
    mock_db.connect = AsyncMock()
    mock_db.disconnect = AsyncMock()
    mock_payments = MagicMock()
    mock_payments.connect = AsyncMock()
    mock_payments.disconnect = AsyncMock()

    with (
        patch("derp.derp_client.DatabaseEngine", return_value=mock_db),
        patch("derp.derp_client.PaymentsClient", return_value=mock_payments),
    ):
        client = DerpClient(
            _config(
                db_url="postgresql://unused",
                schema_path=client_schema_path,
                payments=PaymentsConfig(api_key="sk_test_123"),
            )
        )
        await client.connect()
        assert client.payments is mock_payments
        await client.disconnect()

    mock_payments.connect.assert_awaited_once()
    mock_payments.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_context_manager_scopes_access(client_schema_path: str) -> None:
    mock_db = MagicMock()
    mock_db.connect = AsyncMock()
    mock_db.disconnect = AsyncMock()

    with patch("derp.derp_client.DatabaseEngine", return_value=mock_db):
        client = DerpClient(
            _config(db_url="postgresql://unused", schema_path=client_schema_path)
        )
        async with client as entered:
            assert entered is client
            assert client.db is mock_db

        with pytest.raises(ValueError, match="Not in a session"):
            _ = client.db


@pytest.mark.asyncio
async def test_db_with_replica_executes_queries(
    clean_database: str, client_schema_path: str
) -> None:
    client = DerpClient(
        _config(
            db_url=clean_database,
            schema_path=client_schema_path,
            replica_url=clean_database,
        )
    )

    with pytest.raises(ValueError, match="Not in a session"):
        _ = client.db

    await client.connect()
    try:
        db_rows = await client.db.execute("SELECT 1 AS n")
        assert db_rows[0]["n"] == 1
    finally:
        await client.disconnect()

    with pytest.raises(ValueError, match="Not in a session"):
        _ = client.db


@pytest.mark.asyncio
async def test_kv_service_available_in_session(
    clean_database: str,
    valkey_server: tuple[str, int],
    client_schema_path: str,
) -> None:
    host, port = valkey_server
    client = DerpClient(
        _config(
            db_url=clean_database,
            schema_path=client_schema_path,
            kv=KVConfig(valkey=ValkeyConfig(addresses=[(host, port)])),
        )
    )

    await client.connect()
    key = f"hello:{uuid.uuid4().hex}".encode()
    value = b"world"

    try:
        await client.kv.set(key, value)
        assert await client.kv.get(key) == value
        assert await client.kv.exists(key) is True
        assert await client.kv.delete(key) is True
        assert await client.kv.exists(key) is False
    finally:
        try:
            await client.kv.delete(key)
        finally:
            await client.disconnect()


@pytest.mark.asyncio
async def test_storage_service_available_in_session(
    clean_database: str,
    moto_server: dict[str, str],
    client_schema_path: str,
) -> None:
    client = DerpClient(
        _config(
            db_url=clean_database,
            schema_path=client_schema_path,
            storage=StorageConfig(
                endpoint_url=moto_server["endpoint_url"],
                access_key_id=moto_server["access_key_id"],
                secret_access_key=moto_server["secret_access_key"],
                use_ssl=False,
                region="us-east-1",
            ),
        )
    )

    await client.connect()
    bucket = f"bucket-{uuid.uuid4().hex}"
    key = f"files/{uuid.uuid4().hex}.txt"
    content = b"derp-storage-content"

    try:
        await client.storage._client.create_bucket(Bucket=bucket)  # ty:ignore[possibly-missing-attribute]
        await client.storage.upload_file(
            bucket=bucket,
            key=key,
            data=content,
            content_type="text/plain",
        )
        fetched = await client.storage.fetch_file(bucket=bucket, key=key)
        assert fetched == content
    finally:
        await client.disconnect()


@pytest.mark.asyncio
async def test_ai_property_without_config_raises(client_schema_path: str) -> None:
    mock_db = MagicMock()
    mock_db.connect = AsyncMock()
    mock_db.disconnect = AsyncMock()
    with patch("derp.derp_client.DatabaseEngine", return_value=mock_db):
        client = DerpClient(
            _config(db_url="postgresql://unused", schema_path=client_schema_path)
        )
        await client.connect()
        with pytest.raises(ValueError, match="`AIConfig` was not passed"):
            _ = client.ai
        await client.disconnect()


@pytest.mark.asyncio
async def test_ai_property_returns_ai_client(client_schema_path: str) -> None:
    mock_db = MagicMock()
    mock_db.connect = AsyncMock()
    mock_db.disconnect = AsyncMock()
    with patch("derp.derp_client.DatabaseEngine", return_value=mock_db):
        client = DerpClient(
            _config(
                db_url="postgresql://unused",
                schema_path=client_schema_path,
                ai=AIConfig(api_key="sk-test-123"),
            )
        )
        await client.connect()
        assert isinstance(client.ai, AIClient)
        await client.disconnect()


@pytest.mark.asyncio
async def test_ai_client_uses_custom_base_url(client_schema_path: str) -> None:
    mock_db = MagicMock()
    mock_db.connect = AsyncMock()
    mock_db.disconnect = AsyncMock()
    with patch("derp.derp_client.DatabaseEngine", return_value=mock_db):
        client = DerpClient(
            _config(
                db_url="postgresql://unused",
                schema_path=client_schema_path,
                ai=AIConfig(
                    api_key="sk-test-123",
                    base_url="https://api.openrouter.ai/v1",
                ),
            )
        )
        await client.connect()
        assert str(client.ai._openai_client.base_url) == "https://api.openrouter.ai/v1/"
        await client.disconnect()


def test_ai_requires_session(client_schema_path: str) -> None:
    mock_db = MagicMock()
    with patch("derp.derp_client.DatabaseEngine", return_value=mock_db):
        client = DerpClient(
            _config(
                db_url="postgresql://unused",
                schema_path=client_schema_path,
                ai=AIConfig(api_key="sk-test-123"),
            )
        )
        with pytest.raises(ValueError, match="Not in a session"):
            _ = client.ai
