"""Read replica routing with WAL lag awareness."""

from __future__ import annotations

import asyncio
import logging
import time

import asyncpg

from derp.config import DatabaseConfig

logger = logging.getLogger(__name__)


class ReplicaRouter:
    """Routes read queries to a replica when safe.

    Monitors WAL replication lag and applies a write fence to ensure
    read-after-write consistency.
    """

    def __init__(
        self,
        primary: asyncpg.Pool,
        replica: asyncpg.Pool,
        config: DatabaseConfig,
    ):
        self._primary = primary
        self._replica = replica
        self._max_lag_bytes = config.replica_max_lag_bytes
        self._write_fence_seconds = config.replica_write_fence_seconds
        self._check_interval = config.replica_lag_check_interval_seconds

        self._last_write_time: float = 0.0
        self._replica_available: bool = True
        self._current_lag_bytes: int = 0
        self._check_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start the background lag-check task."""
        if self._check_task is not None:
            return
        self._check_task = asyncio.create_task(self._lag_check_loop())

    async def stop(self) -> None:
        """Stop the background lag-check task."""
        if self._check_task is not None:
            self._check_task.cancel()
            try:
                await self._check_task
            except asyncio.CancelledError:
                pass
            self._check_task = None

    def record_write(self) -> None:
        """Record that a write just happened on the primary."""
        self._last_write_time = time.monotonic()

    def should_use_replica(self) -> bool:
        """Determine if reads should go to the replica."""
        if not self._replica_available:
            return False
        if self._current_lag_bytes > self._max_lag_bytes:
            return False
        elapsed = time.monotonic() - self._last_write_time
        if elapsed < self._write_fence_seconds:
            return False
        return True

    def get_read_pool(self) -> asyncpg.Pool:
        """Return the appropriate pool for read queries."""
        if self.should_use_replica():
            return self._replica
        return self._primary

    @property
    def primary(self) -> asyncpg.Pool:
        """The primary pool (always used for writes)."""
        return self._primary

    async def _check_lag(self) -> None:
        """Query WAL positions and compute replication lag in bytes."""
        try:
            async with self._primary.acquire() as p_conn:
                p_row = await p_conn.fetchrow("SELECT pg_current_wal_lsn()")
                if p_row is None:
                    self._replica_available = False
                    return
                primary_lsn = p_row[0]

                async with self._replica.acquire() as r_conn:
                    r_row = await r_conn.fetchrow("SELECT pg_last_wal_replay_lsn()")
                if r_row is None:
                    self._replica_available = False
                    return
                replica_lsn = r_row[0]

                diff_row = await p_conn.fetchrow(
                    "SELECT pg_wal_lsn_diff($1, $2)", primary_lsn, replica_lsn
                )
            if diff_row is None:
                self._replica_available = False
                return

            self._current_lag_bytes = max(0, int(diff_row[0]))
            self._replica_available = True
        except Exception:
            logger.warning(
                "Replica lag check failed, marking replica unavailable", exc_info=True
            )
            self._replica_available = False

    async def _lag_check_loop(self) -> None:
        """Periodically check replication lag."""
        while True:
            await self._check_lag()
            await asyncio.sleep(self._check_interval)
