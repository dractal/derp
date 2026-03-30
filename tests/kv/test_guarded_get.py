"""Tests for KVClient.guarded_get stampede protection."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence

import pytest

from derp.kv.base import KVClient


class InMemoryKV(KVClient):
    """Minimal in-memory KV client for testing."""

    supports_ttl = False
    supports_scan = False
    supports_batch = False

    def __init__(self) -> None:
        self._store: dict[bytes, bytes] = {}

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass

    async def get(self, key: bytes) -> bytes | None:
        return self._store.get(key)

    async def set(self, key: bytes, value: bytes, *, ttl: float | None = None) -> None:
        self._store[key] = value

    async def delete(self, key: bytes) -> bool:
        return self._store.pop(key, None) is not None

    async def exists(self, key: bytes) -> bool:
        return key in self._store

    async def mget(self, keys: Sequence[bytes]) -> Sequence[bytes | None]:
        return [self._store.get(k) for k in keys]

    async def mset(
        self, items: Sequence[tuple[bytes, bytes]], *, ttl: float | None = None
    ) -> None:
        for k, v in items:
            self._store[k] = v

    async def delete_many(self, keys: Sequence[bytes]) -> int:
        count = 0
        for k in keys:
            if k in self._store:
                del self._store[k]
                count += 1
        return count

    async def ttl(self, key: bytes) -> float | None:
        return None

    async def expire(self, key: bytes, ttl: float) -> bool:
        return key in self._store

    async def set_nx(
        self, key: bytes, value: bytes, *, ttl: float | None = None
    ) -> bool:
        if key in self._store:
            return False
        self._store[key] = value
        return True

    async def incr(self, key: bytes) -> int:
        raw = self._store.get(key, b"0")
        value = int(raw) + 1
        self._store[key] = str(value).encode()
        return value

    async def scan(
        self, *, prefix: bytes | None = None, limit: int | None = None
    ) -> AsyncIterator[bytes]:
        count = 0
        for key in list(self._store):
            if prefix and not key.startswith(prefix):
                continue
            yield key
            count += 1
            if limit is not None and count >= limit:
                return


@pytest.mark.asyncio
async def test_cache_hit_skips_compute() -> None:
    kv = InMemoryKV()
    await kv.set(b"key", b"cached_value")
    call_count = 0

    async def compute() -> bytes:
        nonlocal call_count
        call_count += 1
        return b"computed"

    result = await kv.guarded_get(b"key", compute=compute, ttl=60)
    assert result == b"cached_value"
    assert call_count == 0


@pytest.mark.asyncio
async def test_cache_miss_computes_and_caches() -> None:
    kv = InMemoryKV()

    async def compute() -> bytes:
        return b"fresh_value"

    result = await kv.guarded_get(b"missing", compute=compute, ttl=60)
    assert result == b"fresh_value"
    assert await kv.get(b"missing") == b"fresh_value"


@pytest.mark.asyncio
async def test_concurrent_calls_compute_once() -> None:
    kv = InMemoryKV()
    call_count = 0

    async def compute() -> bytes:
        nonlocal call_count
        call_count += 1
        # Simulate slow computation
        await asyncio.sleep(0.1)
        return b"result"

    tasks = [
        asyncio.create_task(
            kv.guarded_get(b"hot_key", compute=compute, ttl=60, retry_delay=0.02)
        )
        for _ in range(10)
    ]
    results = await asyncio.gather(*tasks)

    assert all(r == b"result" for r in results)
    # Only one task should have called compute (the lock winner).
    # Others either got the cached value or fell through.
    # At most 1 lock holder + possibly 1 fallthrough, but typically just 1.
    assert call_count <= 2


@pytest.mark.asyncio
async def test_lock_released_on_compute_error() -> None:
    kv = InMemoryKV()
    call_count = 0

    async def failing_compute() -> bytes:
        nonlocal call_count
        call_count += 1
        raise RuntimeError("compute failed")

    with pytest.raises(RuntimeError, match="compute failed"):
        await kv.guarded_get(b"err_key", compute=failing_compute, ttl=60)

    # Lock should be released
    assert await kv.get(b"err_key:lock") is None
    assert call_count == 1

    # A subsequent call should be able to acquire the lock
    async def ok_compute() -> bytes:
        return b"ok"

    result = await kv.guarded_get(b"err_key", compute=ok_compute, ttl=60)
    assert result == b"ok"


@pytest.mark.asyncio
async def test_fallthrough_on_permanent_lock() -> None:
    """If set_nx always returns False, compute is called directly."""
    kv = InMemoryKV()
    # Pre-set the lock so set_nx always fails
    await kv.set(b"stuck:lock", b"1")

    call_count = 0

    async def compute() -> bytes:
        nonlocal call_count
        call_count += 1
        return b"fallthrough"

    result = await kv.guarded_get(
        b"stuck",
        compute=compute,
        ttl=60,
        lock_ttl=0.05,
        retry_delay=0.01,
    )
    assert result == b"fallthrough"
    assert call_count == 1


# ── rate_limit ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_rate_limit_allows_under_limit() -> None:
    kv = InMemoryKV()
    result = await kv.rate_limit("user:1", limit=5, window=60)
    assert result.allowed is True
    assert result.count == 1
    assert result.remaining == 4
    assert result.retry_after is None


@pytest.mark.asyncio
async def test_rate_limit_allows_up_to_limit() -> None:
    kv = InMemoryKV()
    for _ in range(5):
        result = await kv.rate_limit("user:2", limit=5, window=60)
    assert result.allowed is True
    assert result.count == 5
    assert result.remaining == 0


@pytest.mark.asyncio
async def test_rate_limit_denies_over_limit() -> None:
    kv = InMemoryKV()
    for _ in range(5):
        await kv.rate_limit("user:3", limit=5, window=60)

    result = await kv.rate_limit("user:3", limit=5, window=60)
    assert result.allowed is False
    assert result.count == 6
    assert result.remaining == 0
    assert result.retry_after is not None


@pytest.mark.asyncio
async def test_rate_limit_separate_keys() -> None:
    kv = InMemoryKV()
    for _ in range(5):
        await kv.rate_limit("user:a", limit=5, window=60)

    result = await kv.rate_limit("user:b", limit=5, window=60)
    assert result.allowed is True
    assert result.count == 1


@pytest.mark.asyncio
async def test_rate_limit_result_fields() -> None:
    kv = InMemoryKV()
    result = await kv.rate_limit("user:4", limit=10, window=60)
    assert result.limit == 10
    assert result.count == 1
    assert result.remaining == 9
    assert result.allowed is True
