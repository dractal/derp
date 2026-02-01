"""FastAPI middleware for JWT authentication."""

from __future__ import annotations

from collections.abc import Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from derp.auth.config import AuthConfig
from derp.auth.exceptions import InvalidTokenError, TokenExpiredError
from derp.auth.jwt import decode_token


class JWTMiddleware(BaseHTTPMiddleware):
    """Middleware that validates JWT tokens and attaches user info to request.

    This middleware is optional - you can use the dependencies directly instead.
    The middleware approach is useful if you want all requests to be authenticated
    by default.

    Usage:
        app.add_middleware(
            JWTMiddleware,
            auth_config=auth_config,
            exclude_paths=["/auth/signin", "/auth/signup", "/docs"],
        )
    """

    def __init__(
        self,
        app: Callable,
        auth_config: AuthConfig,
        exclude_paths: list[str] | None = None,
        exclude_prefixes: list[str] | None = None,
    ):
        super().__init__(app)
        self._config = auth_config
        self._exclude_paths = set(exclude_paths or [])
        self._exclude_prefixes = tuple(exclude_prefixes or [])

    def _should_skip(self, path: str) -> bool:
        """Check if the path should skip authentication."""
        if path in self._exclude_paths:
            return True
        if self._exclude_prefixes and path.startswith(self._exclude_prefixes):
            return True
        return False

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request and validate JWT if present."""
        # Skip excluded paths
        if self._should_skip(request.url.path):
            return await call_next(request)

        # Get token from header
        auth_header = request.headers.get("Authorization")
        if not auth_header:
            return await call_next(request)

        parts = auth_header.split()
        if len(parts) != 2 or parts[0].lower() != "bearer":
            return await call_next(request)

        token = parts[1]

        try:
            payload = decode_token(self._config.jwt, token)

            # Store token payload in request state
            request.state.token_payload = payload
            request.state.user_id = payload.sub
            request.state.session_id = payload.session_id

        except (TokenExpiredError, InvalidTokenError):
            # Token is invalid, but we don't block the request here
            # The dependency will handle unauthorized access
            pass

        return await call_next(request)
