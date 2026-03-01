"""Tests for ReplicaRouter read routing with lag awareness."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from derp.config import DatabaseConfig
from derp.orm.router import ReplicaRouter


def _make_config(**overrides: object) -> DatabaseConfig:
    defaults = {
        "db_url": "postgresql://primary",
        "replica_url": "postgresql://replica",
        "schema_path": "schema.py",
        "replica_max_lag_bytes": 1_048_576,
        "replica_write_fence_seconds": 2.0,
        "replica_lag_check_interval_seconds": 5.0,
    }
    defaults.update(overrides)
    return DatabaseConfig(**defaults)


def _make_router(
    config: DatabaseConfig | None = None,
) -> tuple[ReplicaRouter, MagicMock, MagicMock]:
    primary = MagicMock()
    replica = MagicMock()
    cfg = config or _make_config()
    router = ReplicaRouter(primary, replica, cfg)
    return router, primary, replica


def test_should_use_replica_by_default() -> None:
    router, _, _ = _make_router()
    assert router.should_use_replica() is True


def test_should_use_primary_after_write() -> None:
    router, _, _ = _make_router()
    router.record_write()
    assert router.should_use_replica() is False


def test_should_use_replica_after_fence_expires() -> None:
    router, _, _ = _make_router(_make_config(replica_write_fence_seconds=0.5))
    router.record_write()
    assert router.should_use_replica() is False

    with patch("derp.orm.router.time") as mock_time:
        # Simulate time passing beyond fence
        mock_time.monotonic.return_value = router._last_write_time + 1.0
        assert router.should_use_replica() is True


def test_should_use_primary_when_lag_high() -> None:
    router, _, _ = _make_router(_make_config(replica_max_lag_bytes=1000))
    router._current_lag_bytes = 2000
    assert router.should_use_replica() is False


def test_should_use_primary_when_replica_unavailable() -> None:
    router, _, _ = _make_router()
    router._replica_available = False
    assert router.should_use_replica() is False


def test_get_read_pool_returns_replica_when_safe() -> None:
    router, primary, replica = _make_router()
    assert router.get_read_pool() is replica


def test_get_read_pool_returns_primary_after_write() -> None:
    router, primary, _ = _make_router()
    router.record_write()
    assert router.get_read_pool() is primary


def test_primary_property() -> None:
    router, primary, _ = _make_router()
    assert router.primary is primary


@pytest.mark.asyncio
async def test_check_lag_marks_replica_unavailable_on_error() -> None:
    router, primary, _ = _make_router()

    primary.acquire.return_value.__aenter__ = AsyncMock(side_effect=Exception("conn failed"))
    primary.acquire.return_value.__aexit__ = AsyncMock()

    await router._check_lag()
    assert router._replica_available is False


@pytest.mark.asyncio
async def test_check_lag_computes_byte_difference() -> None:
    router, primary, replica = _make_router()

    # Mock primary connection
    primary_conn = AsyncMock()
    primary_conn.fetchrow = AsyncMock(side_effect=[
        MagicMock(__getitem__=lambda self, i: "0/1000"),  # pg_current_wal_lsn
        MagicMock(__getitem__=lambda self, i: 500),  # pg_wal_lsn_diff
    ])
    primary.acquire.return_value.__aenter__ = AsyncMock(return_value=primary_conn)
    primary.acquire.return_value.__aexit__ = AsyncMock()

    # Mock replica connection
    replica_conn = AsyncMock()
    replica_conn.fetchrow = AsyncMock(
        return_value=MagicMock(__getitem__=lambda self, i: "0/800")
    )
    replica.acquire.return_value.__aenter__ = AsyncMock(return_value=replica_conn)
    replica.acquire.return_value.__aexit__ = AsyncMock()

    await router._check_lag()
    assert router._replica_available is True
    assert router._current_lag_bytes == 500


@pytest.mark.asyncio
async def test_start_and_stop() -> None:
    router, _, _ = _make_router()

    with patch.object(router, "_lag_check_loop", new_callable=AsyncMock) as mock_loop:
        await router.start()
        assert router._check_task is not None

        await router.stop()
        assert router._check_task is None
