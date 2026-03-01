"""Unit tests for Valkey cluster mode using mocks."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from derp.config import ValkeyConfig, ValkeyMode


@pytest.mark.asyncio
async def test_cluster_mode_creates_cluster_client() -> None:
    config = ValkeyConfig(
        addresses=[("node1", 7000), ("node2", 7001), ("node3", 7002)],
        mode=ValkeyMode.CLUSTER,
    )

    with (
        patch("glide.GlideClusterClient") as mock_cluster_cls,
        patch("glide.GlideClusterClientConfiguration"),
        patch("glide.NodeAddress"),
        patch("glide.ServerCredentials"),
    ):
        mock_client = AsyncMock()
        mock_cluster_cls.create = AsyncMock(return_value=mock_client)

        from derp.kv.valkey import ValkeyClient

        store = ValkeyClient(config)
        assert store._is_cluster is True

        await store.connect()
        mock_cluster_cls.create.assert_called_once()


@pytest.mark.asyncio
async def test_standalone_mode_creates_standalone_client() -> None:
    config = ValkeyConfig(
        addresses=[("localhost", 6379)],
        mode=ValkeyMode.STANDALONE,
    )

    with (
        patch("glide.GlideClient") as mock_standalone_cls,
        patch("glide.GlideClientConfiguration"),
        patch("glide.NodeAddress"),
        patch("glide.ServerCredentials"),
    ):
        mock_client = AsyncMock()
        mock_standalone_cls.create = AsyncMock(return_value=mock_client)

        from derp.kv.valkey import ValkeyClient

        store = ValkeyClient(config)
        assert store._is_cluster is False

        await store.connect()
        mock_standalone_cls.create.assert_called_once()


@pytest.mark.asyncio
async def test_cluster_scan_uses_cluster_cursor() -> None:
    config = ValkeyConfig(
        addresses=[("node1", 7000)],
        mode=ValkeyMode.CLUSTER,
    )

    mock_cursor_cls = MagicMock()
    mock_cursor_instance = MagicMock()
    # First call: not finished, second call: finished
    mock_cursor_instance.is_finished.side_effect = [False, True]
    mock_cursor_cls.return_value = mock_cursor_instance

    mock_client = AsyncMock()
    new_cursor = MagicMock()
    new_cursor.is_finished.return_value = True
    mock_client.scan = AsyncMock(return_value=[new_cursor, [b"key1", b"key2"]])

    with (
        patch("glide.GlideClusterClient") as mock_cluster_cls,
        patch("glide.GlideClusterClientConfiguration"),
        patch("glide.ClusterScanCursor", mock_cursor_cls),
        patch("glide.NodeAddress"),
        patch("glide.ServerCredentials"),
    ):
        mock_cluster_cls.create = AsyncMock(return_value=mock_client)

        from derp.kv.valkey import ValkeyClient

        store = ValkeyClient(config)
        await store.connect()

        keys = [key async for key in store.scan()]
        assert keys == [b"key1", b"key2"]
        mock_client.scan.assert_called_once()


@pytest.mark.asyncio
async def test_cluster_addresses_built_from_config() -> None:
    config = ValkeyConfig(
        addresses=[("seed1", 7000), ("seed2", 7001)],
        mode=ValkeyMode.CLUSTER,
    )

    captured_addresses: list = []

    def capture_config(addresses, **kwargs):
        captured_addresses.extend(addresses)
        return MagicMock()

    with (
        patch("glide.GlideClusterClient"),
        patch(
            "glide.GlideClusterClientConfiguration", side_effect=capture_config
        ),
        patch("glide.NodeAddress", side_effect=lambda host, port: (host, port)),
        patch("glide.ServerCredentials"),
    ):
        from derp.kv.valkey import ValkeyClient

        ValkeyClient(config)
        # Should have 2 addresses: seed1:7000 and seed2:7001
        assert len(captured_addresses) == 2
        assert captured_addresses[0] == ("seed1", 7000)
        assert captured_addresses[1] == ("seed2", 7001)
