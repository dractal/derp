"""Base interface for KV clients."""

from __future__ import annotations

import abc
import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence


class KVClient(abc.ABC):
    """Byte-level async KV client."""

    supports_ttl: bool
    supports_scan: bool
    supports_batch: bool

    @abc.abstractmethod
    async def connect(self) -> None:
        """Connect to the store."""

    @abc.abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the store."""

    @abc.abstractmethod
    async def get(self, key: bytes) -> bytes | None:
        """Fetch a value by key."""

    @abc.abstractmethod
    async def set(self, key: bytes, value: bytes, *, ttl: float | None = None) -> None:
        """Set a value by key."""

    @abc.abstractmethod
    async def delete(self, key: bytes) -> bool:
        """Delete a key."""

    @abc.abstractmethod
    async def exists(self, key: bytes) -> bool:
        """Check if a key exists."""

    @abc.abstractmethod
    async def mget(self, keys: Sequence[bytes]) -> Sequence[bytes | None]:
        """Fetch multiple keys."""

    @abc.abstractmethod
    async def mset(
        self, items: Sequence[tuple[bytes, bytes]], *, ttl: float | None = None
    ) -> None:
        """Set multiple key/value pairs."""

    @abc.abstractmethod
    async def delete_many(self, keys: Sequence[bytes]) -> int:
        """Delete multiple keys."""

    @abc.abstractmethod
    async def ttl(self, key: bytes) -> float | None:
        """Return remaining TTL in seconds, or None."""

    @abc.abstractmethod
    async def expire(self, key: bytes, ttl: float) -> bool:
        """Set TTL for a key. Returns False if missing."""

    @abc.abstractmethod
    async def set_nx(
        self, key: bytes, value: bytes, *, ttl: float | None = None
    ) -> bool:
        """Set a value only if the key does not exist. Returns True if set."""

    @abc.abstractmethod
    async def scan(
        self, *, prefix: bytes | None = None, limit: int | None = None
    ) -> AsyncIterator[bytes]:
        """Iterate keys with optional prefix and limit."""
        yield b""  # pragma: no cover
        raise NotImplementedError  # pragma: no cover

    async def guarded_get(
        self,
        cache_key: bytes,
        *,
        compute: Callable[[], Awaitable[bytes]],
        ttl: float,
        lock_ttl: float = 2.0,
        retry_delay: float = 0.05,
    ) -> bytes:
        """Fetch from cache with stampede protection.

        On a cache miss, only one caller acquires a lock and computes the
        value. Other callers wait for the cache to be populated. If retries
        are exhausted, the caller falls through and computes directly.

        The wait budget is derived from ``lock_ttl`` so waiters keep
        retrying for the full duration the lock could be held.

        Args:
            cache_key: The cache key to read/write.
            compute: Async callable that produces the value on cache miss.
            ttl: TTL in seconds for the cached value.
            lock_ttl: TTL in seconds for the lock key.
            retry_delay: Seconds to sleep between retry attempts.

        Returns:
            The cached or freshly computed value.
        """
        cached = await self.get(cache_key)
        if cached is not None:
            return cached

        lock_key = cache_key + b":lock"
        acquired = await self.set_nx(lock_key, b"1", ttl=lock_ttl)

        if acquired:
            try:
                cached = await self.get(cache_key)
                if cached is not None:
                    return cached

                value = await compute()
                await self.set(cache_key, value, ttl=ttl)
                return value
            finally:
                await self.delete(lock_key)

        max_retries = round(lock_ttl / retry_delay)
        for _ in range(max_retries):
            await asyncio.sleep(retry_delay)
            cached = await self.get(cache_key)
            if cached is not None:
                return cached

        return await compute()
