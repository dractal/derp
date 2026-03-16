"""Base interface for KV clients."""

from __future__ import annotations

import abc
import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Any


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

    async def idempotent_execute(
        self,
        *,
        key: str,
        compute: Callable[[], Awaitable[Any]],
        status_code: int = 200,
        ttl: float = 86400,
        key_prefix: str = "derp:idempotency",
    ) -> tuple[Any, int, bool]:
        """Execute idempotently: run ``compute`` once per key.

        On the first call for a given key, ``compute`` is invoked and
        the result is cached. Subsequent calls return the cached result
        without re-invoking ``compute``. Uses :meth:`guarded_get` for
        stampede protection.

        Args:
            key: Idempotency key (typically from a client header).
            compute: Async callable producing a JSON-serializable result.
            status_code: HTTP status code to cache alongside the body.
            ttl: Cache TTL in seconds (default 24h).
            key_prefix: KV key prefix.

        Returns:
            ``(body, status_code, is_replay)`` — *body* is the
            deserialized result, *status_code* is the cached status,
            and *is_replay* is ``True`` when the cached value was used.
        """
        cache_key = f"{key_prefix}:{key}".encode()
        was_computed = False

        async def _compute() -> bytes:
            nonlocal was_computed
            was_computed = True
            result = await compute()
            payload = json.dumps(
                {"status_code": status_code, "body": result},
                default=str,
            )
            return payload.encode()

        raw = await self.guarded_get(cache_key, compute=_compute, ttl=ttl)
        parsed = json.loads(raw)
        return parsed["body"], parsed["status_code"], not was_computed

    async def already_processed(
        self,
        *,
        event_id: str,
        ttl: float = 86400,
        key_prefix: str = "derp:webhook",
    ) -> bool:
        """Check if an event has already been processed.

        Uses :meth:`set_nx` to atomically mark the event. Returns
        ``True`` if the event was already seen, ``False`` on first call.

        Args:
            event_id: Unique event identifier (e.g. Stripe event ID).
            ttl: How long to remember the event (default 24h).
            key_prefix: KV key prefix.
        """
        cache_key = f"{key_prefix}:{event_id}".encode()
        acquired = await self.set_nx(cache_key, b"1", ttl=ttl)
        return not acquired
