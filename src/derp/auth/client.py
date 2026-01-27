"""OAuth2 client for authorization code flow."""

from __future__ import annotations

import secrets
from typing import Any
from urllib.parse import urlencode, urlparse, urlunparse

import httpx

from derp.auth.tokens import TokenResponse


class OAuth2Client:
    """OAuth2 client for implementing authorization code flow.

    Example:
        client = OAuth2Client(
            client_id="your-client-id",
            client_secret="your-client-secret",
            authorization_url="https://example.com/oauth/authorize",
            token_url="https://example.com/oauth/token",
            redirect_uri="https://yourapp.com/callback",
        )

        # Generate authorization URL
        auth_url, state = await client.get_authorization_url(
            scopes=["read", "write"]
        )

        # After user authorizes, exchange code for tokens
        tokens = await client.exchange_code("authorization-code", state)

        # Use access token for API requests
        async with httpx.AsyncClient() as http_client:
            response = await http_client.get(
                "https://api.example.com/user",
                headers={"Authorization": f"Bearer {tokens.access_token}"},
            )

        # Refresh tokens when expired
        new_tokens = await client.refresh_token(tokens.refresh_token)
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        authorization_url: str,
        token_url: str,
        redirect_uri: str,
        *,
        scopes: list[str] | None = None,
        state_length: int = 32,
    ):
        """Initialize OAuth2 client.

        Args:
            client_id: OAuth2 client ID
            client_secret: OAuth2 client secret
            authorization_url: Authorization endpoint URL
            token_url: Token endpoint URL
            redirect_uri: Redirect URI registered with the provider
            scopes: Default scopes to request
            state_length: Length of state parameter for CSRF protection
        """
        self._client_id = client_id
        self._client_secret = client_secret
        self._authorization_url = authorization_url
        self._token_url = token_url
        self._redirect_uri = redirect_uri
        self._default_scopes = scopes or []
        self._state_length = state_length
        self._http_client: httpx.AsyncClient | None = None

    async def _get_http_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient()
        return self._http_client

    async def close(self) -> None:
        """Close HTTP client."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def __aenter__(self) -> OAuth2Client:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: type[BaseException] | None,
    ) -> None:
        await self.close()

    def get_authorization_url(
        self,
        *,
        scopes: list[str] | None = None,
        state: str | None = None,
        extra_params: dict[str, str] | None = None,
    ) -> tuple[str, str]:
        """Generate authorization URL.

        Args:
            scopes: Scopes to request (defaults to client's default scopes)
            state: State parameter for CSRF protection (auto-generated if None)
            extra_params: Additional query parameters

        Returns:
            Tuple of (authorization_url, state)

        Example:
            auth_url, state = client.get_authorization_url(
                scopes=["read", "write"],
            )
        """
        if state is None:
            state = secrets.token_urlsafe(self._state_length)

        scopes_to_use = scopes or self._default_scopes
        scope_str = " ".join(scopes_to_use) if scopes_to_use else None

        params: dict[str, str] = {
            "response_type": "code",
            "client_id": self._client_id,
            "redirect_uri": self._redirect_uri,
            "state": state,
        }

        if scope_str:
            params["scope"] = scope_str

        if extra_params:
            params.update(extra_params)

        parsed = urlparse(self._authorization_url)
        query = urlencode(params)
        auth_url = urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                query,
                parsed.fragment,
            )
        )

        return auth_url, state

    async def exchange_code(
        self,
        code: str,
        state: str | None = None,
        *,
        expected_state: str | None = None,
    ) -> TokenResponse:
        """Exchange authorization code for access token.

        Args:
            code: Authorization code from callback
            state: State parameter from callback (for validation)
            expected_state: Expected state value (if None, state is not validated)

        Returns:
            TokenResponse with access and refresh tokens

        Raises:
            ValueError: If state validation fails
            httpx.HTTPStatusError: If token exchange fails

        Example:
            tokens = await client.exchange_code(
                code="authorization-code",
                state="state-from-callback",
                expected_state="state-from-authorization",
            )
        """
        if expected_state is not None and state != expected_state:
            raise ValueError("State parameter mismatch")

        http_client = await self._get_http_client()

        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self._redirect_uri,
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }

        response = await http_client.post(
            self._token_url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response.raise_for_status()

        token_data = response.json()
        return self._parse_token_response(token_data)

    async def refresh_token(self, refresh_token: str) -> TokenResponse:
        """Refresh access token using refresh token.

        Args:
            refresh_token: Refresh token from previous token response

        Returns:
            TokenResponse with new access and refresh tokens

        Raises:
            httpx.HTTPStatusError: If token refresh fails

        Example:
            new_tokens = await client.refresh_token(old_tokens.refresh_token)
        """
        http_client = await self._get_http_client()

        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }

        response = await http_client.post(
            self._token_url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response.raise_for_status()

        token_data = response.json()
        return self._parse_token_response(token_data)

    def _parse_token_response(self, data: dict[str, Any]) -> TokenResponse:
        """Parse token response from provider.

        Args:
            data: JSON response from token endpoint

        Returns:
            TokenResponse object
        """
        from datetime import UTC, datetime, timedelta

        expires_in = data.get("expires_in")
        expires_at = None
        if expires_in is not None:
            expires_at = datetime.now(UTC) + timedelta(seconds=int(expires_in))

        return TokenResponse(
            access_token=data["access_token"],
            token_type=data.get("token_type", "Bearer"),
            expires_in=expires_in,
            refresh_token=data.get("refresh_token"),
            scope=data.get("scope"),
            expires_at=expires_at,
        )

    async def revoke_token(
        self,
        token: str,
        *,
        token_type_hint: str | None = None,
        revocation_url: str | None = None,
    ) -> None:
        """Revoke an access or refresh token.

        Args:
            token: Token to revoke
            token_type_hint: Hint about token type ("access_token" or "refresh_token")
            revocation_url: Token revocation endpoint (if different from token_url)

        Raises:
            httpx.HTTPStatusError: If token revocation fails

        Example:
            await client.revoke_token(
                tokens.refresh_token, token_type_hint="refresh_token"
            )
        """
        http_client = await self._get_http_client()

        url = revocation_url or self._token_url.replace("/token", "/revoke")

        data: dict[str, str] = {
            "token": token,
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }

        if token_type_hint:
            data["token_type_hint"] = token_type_hint

        response = await http_client.post(
            url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response.raise_for_status()
