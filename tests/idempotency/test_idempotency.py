"""Tests for KVClient idempotency and webhook deduplication methods."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence

import pytest

from derp.kv.base import KVClient


class InMemoryKV(KVClient):
    """Minimal in-memory KV for testing."""

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
        self,
        items: Sequence[tuple[bytes, bytes]],
        *,
        ttl: float | None = None,
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


# =============================================================================
# idempotent_execute
# =============================================================================


class TestIdempotentExecute:
    @pytest.mark.asyncio
    async def test_first_call_executes_compute(self):
        kv = InMemoryKV()
        call_count = 0

        async def compute():
            nonlocal call_count
            call_count += 1
            return {"id": 1, "n": call_count}

        body, status, is_replay = await kv.idempotent_execute(
            key="abc", compute=compute, status_code=201
        )
        assert body == {"id": 1, "n": 1}
        assert status == 201
        assert is_replay is False
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_repeat_call_returns_cached(self):
        kv = InMemoryKV()
        call_count = 0

        async def compute():
            nonlocal call_count
            call_count += 1
            return {"id": 1, "n": call_count}

        await kv.idempotent_execute(key="abc", compute=compute, status_code=201)
        body, status, is_replay = await kv.idempotent_execute(
            key="abc", compute=compute, status_code=201
        )
        assert body == {"id": 1, "n": 1}
        assert status == 201
        assert is_replay is True
        assert call_count == 1  # compute only called once

    @pytest.mark.asyncio
    async def test_different_keys_execute_independently(self):
        kv = InMemoryKV()
        call_count = 0

        async def compute():
            nonlocal call_count
            call_count += 1
            return {"n": call_count}

        b1, _, _ = await kv.idempotent_execute(key="a", compute=compute)
        b2, _, _ = await kv.idempotent_execute(key="b", compute=compute)
        assert b1["n"] == 1
        assert b2["n"] == 2
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_status_code_preserved(self):
        kv = InMemoryKV()

        async def compute():
            return {"ok": True}

        _, status1, _ = await kv.idempotent_execute(
            key="x", compute=compute, status_code=201
        )
        _, status2, _ = await kv.idempotent_execute(
            key="x", compute=compute, status_code=201
        )
        assert status1 == 201
        assert status2 == 201

    @pytest.mark.asyncio
    async def test_custom_prefix(self):
        kv = InMemoryKV()

        async def compute():
            return {}

        await kv.idempotent_execute(key="k1", compute=compute, key_prefix="myapp:idem")
        assert b"myapp:idem:k1" in kv._store


# =============================================================================
# already_processed
# =============================================================================


class TestAlreadyProcessed:
    @pytest.mark.asyncio
    async def test_first_call_returns_false(self):
        kv = InMemoryKV()
        assert await kv.already_processed(event_id="evt_1") is False

    @pytest.mark.asyncio
    async def test_second_call_returns_true(self):
        kv = InMemoryKV()
        await kv.already_processed(event_id="evt_1")
        assert await kv.already_processed(event_id="evt_1") is True

    @pytest.mark.asyncio
    async def test_different_events_independent(self):
        kv = InMemoryKV()
        assert await kv.already_processed(event_id="evt_1") is False
        assert await kv.already_processed(event_id="evt_2") is False

    @pytest.mark.asyncio
    async def test_custom_prefix(self):
        kv = InMemoryKV()
        await kv.already_processed(event_id="evt_1", key_prefix="stripe")
        assert b"stripe:evt_1" in kv._store
