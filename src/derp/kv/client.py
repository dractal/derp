"""Typed KV client adapter."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence

from derp.config import KVConfig
from derp.kv.base import KVBase, KVStore
from derp.kv.serializers import KVSerializer, get_serializer
from derp.kv.valkey import ValkeyStore


class KVClients:
    """Container for multiple KV clients."""

    def __init__(self, config: KVConfig, schema_path: str):
        if config.valkey is None:
            raise ValueError("KVConfig must include a Valkey configuration.")
        self._config = config
        self._schema_path = schema_path
        self._store: KVStore = ValkeyStore(config.valkey)
        self._clients: dict[type[KVBase], KVClient] = {}

    async def connect(self) -> None:
        await self._store.connect()

    async def disconnect(self) -> None:
        await self._store.disconnect()

    @property
    def store(self) -> KVStore:
        """Return the underlying byte-level store."""
        return self._store

    def from_[K, V](self, kv_cls: type[KVBase[K, V]]) -> KVClient[K, V]:
        if kv_cls not in self._clients:
            self._clients[kv_cls] = KVClient(kv_cls, self._store)
        return self._clients[kv_cls]


class KVClient[K, V]:
    """Typed KV client that uses serializers for K and V."""

    def __init__(self, kv_cls: type[KVBase[K, V]], store: KVStore):
        self._kv_cls: type[KVBase[K, V]] = kv_cls
        self._store: KVStore = store
        self._key_serializer: KVSerializer[K] = get_serializer(self._kv_cls.key_type())
        self._value_serializer: KVSerializer[V] = get_serializer(
            self._kv_cls.value_type()
        )

    async def connect(self) -> None:
        await self._store.connect()

    async def disconnect(self) -> None:
        await self._store.disconnect()

    @property
    def store(self) -> KVStore:
        """Return the underlying byte-level store."""
        return self._store

    def _encode_key(self, key: K) -> bytes:
        return self._key_serializer.encode(key)

    def _decode_key(self, data: bytes) -> K:
        return self._key_serializer.decode(data)

    def _encode_value(self, value: V) -> bytes:
        return self._value_serializer.encode(value)

    def _decode_value(self, data: bytes) -> V:
        return self._value_serializer.decode(data)

    async def get(self, key: K) -> V | None:
        data = await self._store.get(self._encode_key(key))
        if data is None:
            return None
        return self._decode_value(data)

    async def set(self, key: K, value: V, *, ttl: float | None = None) -> None:
        await self._store.set(self._encode_key(key), self._encode_value(value), ttl=ttl)

    async def delete(self, key: K) -> bool:
        return await self._store.delete(self._encode_key(key))

    async def exists(self, key: K) -> bool:
        return await self._store.exists(self._encode_key(key))

    async def mget(self, keys: Sequence[K]) -> Sequence[V | None]:
        encoded: list[bytes] = [self._encode_key(key) for key in keys]
        results: Sequence[bytes | None] = await self._store.mget(encoded)
        decoded: list[V | None] = []
        for item in results:
            decoded.append(self._decode_value(item) if item is not None else None)
        return decoded

    async def mset(
        self, items: Sequence[tuple[K, V]], *, ttl: float | None = None
    ) -> None:
        encoded: list[tuple[bytes, bytes]] = [
            (self._encode_key(k), self._encode_value(v)) for k, v in items
        ]
        await self._store.mset(encoded, ttl=ttl)

    async def delete_many(self, keys: Sequence[K]) -> int:
        encoded = [self._encode_key(key) for key in keys]
        return await self._store.delete_many(encoded)

    async def ttl(self, key: K) -> float | None:
        return await self._store.ttl(self._encode_key(key))

    async def expire(self, key: K, ttl: float) -> bool:
        return await self._store.expire(self._encode_key(key), ttl)

    async def scan(
        self, *, prefix: K | None = None, limit: int | None = None
    ) -> AsyncIterator[K]:
        prefix_bytes = self._encode_key(prefix) if prefix is not None else None
        async for key in self._store.scan(prefix=prefix_bytes, limit=limit):
            yield self._decode_key(key)
