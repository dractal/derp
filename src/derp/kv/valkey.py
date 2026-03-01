"""Valkey-backed KV client using Valkey GLIDE."""

from __future__ import annotations

import asyncio
import math
from collections.abc import AsyncIterator, Sequence

from etils import epy

from derp.config import ValkeyConfig, ValkeyMode
from derp.kv.base import KVClient

with epy.lazy_imports():
    import glide


class ValkeyClient(KVClient):
    """Byte-level KV client backed by Valkey GLIDE."""

    supports_ttl = True
    supports_scan = True
    supports_batch = True

    def __init__(self, config: ValkeyConfig):
        addresses = [
            glide.NodeAddress(host=host, port=port) for host, port in config.addresses
        ]

        credentials: glide.ServerCredentials | None = (
            glide.ServerCredentials(username=config.username, password=config.password)
            if config.password is not None
            else None
        )

        self._is_cluster = config.mode == ValkeyMode.CLUSTER
        self._config: ValkeyConfig = config

        if self._is_cluster:
            self._glide_config: (
                glide.GlideClientConfiguration | glide.GlideClusterClientConfiguration
            ) = glide.GlideClusterClientConfiguration(
                addresses,
                credentials=credentials,
                use_tls=config.use_tls,
            )
        else:
            self._glide_config = glide.GlideClientConfiguration(
                addresses,
                credentials=credentials,
                use_tls=config.use_tls,
            )

        self._client: glide.GlideClient | glide.GlideClusterClient | None = None

    async def connect(self) -> None:
        if self._client is not None:
            return
        if self._is_cluster:
            self._client = await glide.GlideClusterClient.create(self._glide_config)
        else:
            self._client = await glide.GlideClient.create(self._glide_config)

    async def disconnect(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None

    @property
    def client(self) -> glide.GlideClient | glide.GlideClusterClient:
        if self._client is None:
            raise RuntimeError("Valkey client not connected. Call connect() first.")
        return self._client

    async def get(self, key: bytes) -> bytes | None:
        return await self.client.get(key)

    async def set(self, key: bytes, value: bytes, *, ttl: float | None = None) -> None:
        expiry = (
            glide.ExpirySet(glide.ExpiryType.SEC, math.ceil(ttl))
            if ttl is not None
            else None
        )
        await self.client.set(key, value, expiry=expiry)

    async def set_nx(
        self, key: bytes, value: bytes, *, ttl: float | None = None
    ) -> bool:
        expiry = (
            glide.ExpirySet(glide.ExpiryType.SEC, math.ceil(ttl))
            if ttl is not None
            else None
        )
        result = await self.client.set(
            key,
            value,
            conditional_set=glide.ConditionalChange.ONLY_IF_DOES_NOT_EXIST,
            expiry=expiry,
        )
        return result is not None

    async def delete(self, key: bytes) -> bool:
        return (await self.client.delete([key])) > 0

    async def exists(self, key: bytes) -> bool:
        return (await self.client.exists([key])) > 0

    async def mget(self, keys: Sequence[bytes]) -> Sequence[bytes | None]:
        if not keys:
            return []
        return await self.client.mget(list(keys))

    async def mset(
        self, items: Sequence[tuple[bytes, bytes]], *, ttl: float | None = None
    ) -> None:
        if not items:
            return
        mapping = {key: value for key, value in items}
        await self.client.mset(mapping)
        if ttl is not None:
            ttl_seconds = math.ceil(ttl)
            await asyncio.gather(
                *(self.client.expire(key, ttl_seconds) for key, _ in items)
            )

    async def delete_many(self, keys: Sequence[bytes]) -> int:
        if not keys:
            return 0
        return int(await self.client.delete(list(keys)))

    async def ttl(self, key: bytes) -> float | None:
        ttl = await self.client.ttl(key)
        if ttl is None or ttl < 0:
            return None
        return float(ttl)

    async def expire(self, key: bytes, ttl: float) -> bool:
        return bool(await self.client.expire(key, math.ceil(ttl)))

    async def scan(
        self, *, prefix: bytes | None = None, limit: int | None = None
    ) -> AsyncIterator[bytes]:
        match = prefix + b"*" if prefix else None
        count = 0

        if self._is_cluster:
            cluster_client: glide.GlideClusterClient = self.client  # type: ignore[assignment]
            cursor = glide.ClusterScanCursor()
            while not cursor.is_finished():
                result = await cluster_client.scan(cursor, match=match)
                cursor = result[0]
                keys: list[bytes] = result[1]  # type: ignore[assignment]
                for key in keys:
                    yield key
                    count += 1
                    if limit is not None and count >= limit:
                        return
        else:
            standalone_client: glide.GlideClient = self.client  # type: ignore[assignment]
            raw_cursor: bytes = b"0"
            while True:
                sa_result = await standalone_client.scan(raw_cursor, match=match)
                raw_cursor = sa_result[0]  # type: ignore[assignment]
                sa_keys: list[bytes] = sa_result[1]  # type: ignore[assignment]
                for key in sa_keys:
                    yield key
                    count += 1
                    if limit is not None and count >= limit:
                        return
                if raw_cursor == b"0":
                    break
