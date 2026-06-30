"""Tests for the async, TTL-cached JWKS resolver."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, cast

import httpx
import jwt as pyjwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm

from derp.auth.aiojwks import AsyncJWKS

URL = "https://example.test/jwks"

_PRIV = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_PRIV2 = rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _jwk(private_key: rsa.RSAPrivateKey, kid: str) -> dict[str, Any]:
    raw = RSAAlgorithm.to_jwk(private_key.public_key())
    data = json.loads(raw) if isinstance(raw, str) else dict(raw)
    data.update(kid=kid, use="sig", alg="RS256")
    return data


def _jwks_doc(*pairs: tuple[rsa.RSAPrivateKey, str]) -> dict[str, Any]:
    return {"keys": [_jwk(pk, kid) for pk, kid in pairs]}


def _token(private_key: rsa.RSAPrivateKey, *, kid: str | None = "k1") -> str:
    headers = {"kid": kid} if kid is not None else {}
    return pyjwt.encode(
        {"sub": "u1", "exp": int(time.time()) + 3600},
        private_key,
        algorithm="RS256",
        headers=headers,
    )


class _Resp:
    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._data


class FakeHTTP:
    """Records fetches and returns a JWKS doc (fixed, or one per call)."""

    def __init__(self, payloads: dict[str, Any] | list[dict[str, Any]]) -> None:
        self._payloads = payloads
        self.calls = 0

    async def get(self, url: str) -> _Resp:
        await asyncio.sleep(0)  # yield so concurrent callers interleave
        i = self.calls
        self.calls += 1
        if isinstance(self._payloads, list):
            return _Resp(self._payloads[min(i, len(self._payloads) - 1)])
        return _Resp(self._payloads)


def _make(http: Any, *, ttl: float = 3600.0) -> AsyncJWKS:
    return AsyncJWKS(URL, cast(httpx.AsyncClient, http), ttl=ttl)


async def test_resolves_key_that_verifies_the_token():
    jwks = _make(FakeHTTP(_jwks_doc((_PRIV, "k1"))))
    key = await jwks.get_signing_key(_token(_PRIV, kid="k1"))
    decoded = pyjwt.decode(_token(_PRIV, kid="k1"), key, algorithms=["RS256"])
    assert decoded["sub"] == "u1"


async def test_caches_within_ttl():
    http = FakeHTTP(_jwks_doc((_PRIV, "k1")))
    jwks = _make(http)
    await jwks.get_signing_key(_token(_PRIV))
    await jwks.get_signing_key(_token(_PRIV))
    assert http.calls == 1


async def test_refetches_after_ttl():
    http = FakeHTTP(_jwks_doc((_PRIV, "k1")))
    jwks = _make(http, ttl=0.0)
    await jwks.get_signing_key(_token(_PRIV))
    await jwks.get_signing_key(_token(_PRIV))
    assert http.calls == 2


async def test_concurrent_cold_cache_fetches_once():
    # The lock + double-check must collapse a thundering herd into one fetch.
    http = FakeHTTP(_jwks_doc((_PRIV, "k1")))
    jwks = _make(http)
    token = _token(_PRIV)
    await asyncio.gather(*(jwks.get_signing_key(token) for _ in range(20)))
    assert http.calls == 1


async def test_unknown_kid_triggers_one_refetch_then_raises():
    http = FakeHTTP(_jwks_doc((_PRIV, "k1")))
    jwks = _make(http)
    with pytest.raises(pyjwt.exceptions.PyJWKClientError):
        await jwks.get_signing_key(_token(_PRIV2, kid="rotated"))
    assert http.calls == 2  # initial + one forced rotation refetch


async def test_rotation_picks_up_new_key():
    # First fetch only has k1; after "rotation" the endpoint serves k2.
    http = FakeHTTP([_jwks_doc((_PRIV, "k1")), _jwks_doc((_PRIV2, "k2"))])
    jwks = _make(http)
    key = await jwks.get_signing_key(_token(_PRIV2, kid="k2"))
    decoded = pyjwt.decode(_token(_PRIV2, kid="k2"), key, algorithms=["RS256"])
    assert decoded["sub"] == "u1"
    assert http.calls == 2


async def test_fetch_failure_raises_pyjwkclienterror():
    class FailHTTP:
        async def get(self, url: str) -> _Resp:
            raise httpx.ConnectError("boom")

    jwks = _make(FailHTTP())
    with pytest.raises(pyjwt.exceptions.PyJWKClientError):
        await jwks.get_signing_key(_token(_PRIV))


async def test_token_without_kid_raises():
    jwks = _make(FakeHTTP(_jwks_doc((_PRIV, "k1"))))
    with pytest.raises(pyjwt.exceptions.PyJWKClientError, match="kid"):
        await jwks.get_signing_key(_token(_PRIV, kid=None))


async def test_malformed_token_raises():
    jwks = _make(FakeHTTP(_jwks_doc((_PRIV, "k1"))))
    with pytest.raises(pyjwt.exceptions.PyJWKClientError):
        await jwks.get_signing_key("not-a-jwt")
