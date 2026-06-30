"""Async, TTL-cached JWKS resolver.

PyJWT's :class:`jwt.PyJWKClient` is synchronous — it fetches the key set with a
blocking ``urllib`` call, which stalls the asyncio event loop on every
cache-miss refetch. :class:`AsyncJWKS` is a small async replacement built on an
``httpx.AsyncClient``: it fetches the JWKS asynchronously, caches it with a TTL,
and serializes concurrent refreshes with a lock so a cold cache does not trigger
a thundering herd of fetches. The signature crypto itself is reused from PyJWT
(``PyJWKSet`` / :func:`jwt.decode`).
"""

from __future__ import annotations

import asyncio
import time

import httpx
import jwt as pyjwt


class AsyncJWKS:
    """Resolve the signing key for a JWT from a remote JWKS, asynchronously.

    *ttl* bounds how long a fetched key set is reused before a refetch; the
    default is an hour (Google serves a multi-hour ``Cache-Control`` max-age, so
    a short TTL only adds needless refetches).
    """

    def __init__(
        self, url: str, http: httpx.AsyncClient, *, ttl: float = 3600.0
    ) -> None:
        self._url = url
        self._http = http
        self._ttl = ttl
        self._jwks: pyjwt.PyJWKSet | None = None
        self._fetched_at = 0.0
        self._lock = asyncio.Lock()

    async def get_signing_key(self, token: str):
        """Return the cryptographic key that signed *token*, matched by ``kid``.

        Raises :class:`jwt.exceptions.PyJWKClientError` when the token header is
        malformed or has no ``kid``, when no matching key exists (even after a
        rotation refresh), or when the JWKS cannot be fetched.
        """
        try:
            header = pyjwt.get_unverified_header(token)
        except pyjwt.exceptions.PyJWTError as e:
            raise pyjwt.exceptions.PyJWKClientError("token header is malformed") from e
        kid = header.get("kid")
        if not kid:
            raise pyjwt.exceptions.PyJWKClientError("token has no 'kid' header")

        key = self._match(await self._get_jwks(), kid)
        if key is None:
            # Unknown kid — Google may have rotated its keys; refetch once.
            key = self._match(await self._get_jwks(force=True), kid)
        if key is None:
            raise pyjwt.exceptions.PyJWKClientError(f"no signing key for kid {kid!r}")
        return key

    def _match(self, jwks: pyjwt.PyJWKSet, kid: str):
        return next((k.key for k in jwks.keys if k.key_id == kid), None)

    async def _get_jwks(self, *, force: bool = False) -> pyjwt.PyJWKSet:
        cached = self._jwks
        if not force and cached is not None and self._is_fresh():
            return cached
        async with self._lock:
            # Re-check after acquiring the lock: another coroutine may have
            # refreshed the cache while we were waiting.
            cached = self._jwks
            if not force and cached is not None and self._is_fresh():
                return cached
            jwks = await self._fetch()
            self._jwks = jwks
            self._fetched_at = time.time()
            return jwks

    def _is_fresh(self) -> bool:
        return time.time() - self._fetched_at < self._ttl

    async def _fetch(self) -> pyjwt.PyJWKSet:
        try:
            resp = await self._http.get(self._url)
            resp.raise_for_status()
            return pyjwt.PyJWKSet.from_dict(resp.json())
        except (
            httpx.HTTPError,
            ValueError,
            KeyError,
            pyjwt.exceptions.PyJWKError,
        ) as e:
            raise pyjwt.exceptions.PyJWKClientError(
                f"failed to fetch JWKS from {self._url}: {e}"
            ) from e
