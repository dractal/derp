"""Base interfaces for KV stores."""

from __future__ import annotations

import abc
from collections.abc import AsyncIterator, Sequence
from typing import Any, get_type_hints

from derp.kv.errors import KVError


class KVStore(abc.ABC):
    """Byte-level async KV store protocol."""

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


class KVMeta(type):
    """Metaclass enforcing key/value-only annotations."""

    def __new__(mcls, name: str, bases: tuple[type, ...], namespace: dict[str, Any]):
        cls = super().__new__(mcls, name, bases, namespace)
        if name == "KVBase":
            return cls

        hints = get_type_hints(cls, include_extras=True)
        if not hints:
            raise TypeError("KV store must define 'key' and 'value' annotations.")

        allowed = {"key", "value"}
        extra = set(hints.keys()) - allowed
        missing = allowed - set(hints.keys())
        if extra:
            raise TypeError(
                "KV store may only define 'key' and 'value' annotations; "
                f"got extra: {sorted(extra)}"
            )
        if missing:
            raise TypeError(
                "KV store must define both 'key' and 'value' annotations; "
                f"missing: {sorted(missing)}"
            )

        cls._key_type: type[Any] = hints["key"]
        cls._value_type: type[Any] = hints["value"]
        return cls


class KVBase[K, V](metaclass=KVMeta):
    """Base class for typed KV stores."""

    _key_type: type[Any]
    _value_type: type[Any]

    @classmethod
    def key_type(cls) -> type[Any]:
        """Return the key type for this KV store."""
        if not hasattr(cls, "_key_type"):
            raise KVError("KV store types not resolved.")
        return cls._key_type

    @classmethod
    def value_type(cls) -> type[Any]:
        """Return the value type for this KV store."""
        if not hasattr(cls, "_value_type"):
            raise KVError("KV store types not resolved.")
        return cls._value_type
