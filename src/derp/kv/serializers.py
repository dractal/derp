"""Serializer registry for KV stores."""

from __future__ import annotations

import abc


class KVSerializer[T](abc.ABC):
    """Serializer for KV values."""

    @abc.abstractmethod
    def encode(self, value: T) -> bytes:
        """Encode a value to bytes."""

    @abc.abstractmethod
    def decode(self, data: bytes) -> T:
        """Decode bytes into a value."""


_serializers: dict[type[object], KVSerializer[object]] = {}


def register_serializer[T](t: type[T], serializer: KVSerializer[T]) -> None:
    """Register a serializer for a type.

    Args:
        t: Type to register.
        serializer: Serializer for the type.

    Raises:
        ValueError: If a serializer is already registered for the type.
    """
    if t in _serializers:
        raise ValueError(f"Serializer already registered for type: {t}.")
    _serializers[t] = serializer  # type: ignore[assignment]


def get_serializer[T](t: type[T]) -> KVSerializer[T]:
    """Fetch a serializer for a type.

    Raises:
        KVError: If no serializer is registered.
    """
    serializer = _serializers.get(t)
    if serializer is None:
        raise ValueError(f"No serializer registered for type: {t.__name__}")
    return serializer  # type: ignore[return-value]
