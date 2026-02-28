"""Valkey-backed KV client using Valkey GLIDE."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence

from etils import epy

from derp.config import ValkeyConfig
from derp.kv.base import KVClient

with epy.lazy_imports():
    import glide


class ValkeyClient(KVClient):
    """Byte-level KV client backed by Valkey GLIDE."""

    supports_ttl = True
    supports_scan = True
    supports_batch = True

    def __init__(self, config: ValkeyConfig):
        addresses: list[glide.NodeAddress] = [
            glide.NodeAddress(host=config.host, port=config.port)
        ]
        credentials = (
            glide.ServerCredentials(username=config.username, password=config.password)
            if config.password is not None and config.username is not None
            else None
        )
        glide_config = glide.GlideClientConfiguration(
            addresses,
            credentials=credentials,
            use_tls=config.use_tls,
        )
        self._config: ValkeyConfig = config
        self._glide_config: glide.GlideClientConfiguration = glide_config
        self._client: glide.GlideClient | None = None

    async def connect(self) -> None:
        if self._client is not None:
            return
        self._client = await glide.GlideClient.create(self._glide_config)

    async def disconnect(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None

    @property
    def client(self) -> glide.GlideClient:
        if self._client is None:
            raise RuntimeError("Valkey client not connected. Call connect() first.")
        return self._client

    async def get(self, key: bytes) -> bytes | None:
        return await self.client.get(key)

    async def set(self, key: bytes, value: bytes, *, ttl: float | None = None) -> None:
        expiry = (
            glide.ExpirySet(glide.ExpiryType.SEC, int(ttl)) if ttl is not None else None
        )
        await self.client.set(key, value, expiry=expiry)

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
            ttl_seconds = int(ttl)
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
        return bool(await self.client.expire(key, int(ttl)))

    async def scan(
        self, *, prefix: bytes | None = None, limit: int | None = None
    ) -> AsyncIterator[bytes]:
        cursor: bytes = b"0"
        count = 0
        match = prefix + b"*" if prefix else None
        while True:
            result: tuple[bytes, list[bytes]] = await self.client.scan(  # type: ignore[assignment]
                cursor, match=match
            )
            cursor, keys = result
            for key in keys:
                yield key
                count += 1
                if limit is not None and count >= limit:
                    return
            if cursor == b"0":
                break
