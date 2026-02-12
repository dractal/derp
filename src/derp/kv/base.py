"""Base interface for KV clients."""

from __future__ import annotations

import abc
from collections.abc import AsyncIterator, Sequence


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
    async def scan(
        self, *, prefix: bytes | None = None, limit: int | None = None
    ) -> AsyncIterator[bytes]:
        """Iterate keys with optional prefix and limit."""
        yield b""  # pragma: no cover
        raise NotImplementedError  # pragma: no cover

