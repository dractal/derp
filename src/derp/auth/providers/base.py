"""Base OAuth provider interface."""

from __future__ import annotations

import abc
import dataclasses
from typing import Any


@dataclasses.dataclass
class OAuthUserInfo:
    """User information retrieved from OAuth provider."""

    id: str
    email: str
    email_verified: bool = False
    name: str | None = None
    picture: str | None = None
    raw_data: dict[str, Any] | None = None


@dataclasses.dataclass
class OAuthTokens:
    """Tokens received from OAuth provider."""

    access_token: str
    token_type: str = "bearer"
    refresh_token: str | None = None
    expires_in: int | None = None
    scope: str | None = None
    id_token: str | None = None


class BaseOAuthProvider[ConfigT](abc.ABC):
    """Abstract base class for OAuth providers."""

    provider_name: str

    def __init__(self, config: ConfigT):
        self._config = config

    @abc.abstractmethod
    def get_authorization_url(
        self,
        state: str,
        scopes: list[str] | None = None,
        redirect_uri: str | None = None,
    ) -> str:
        """Generate the OAuth authorization URL.

        Args:
            state: CSRF protection state token
            scopes: Optional list of scopes to request
            redirect_uri: Optional override for redirect URI

        Returns:
            Authorization URL to redirect the user to
        """

    @abc.abstractmethod
    async def exchange_code(
        self,
        code: str,
        redirect_uri: str | None = None,
    ) -> OAuthTokens:
        """Exchange authorization code for tokens.

        Args:
            code: Authorization code from callback
            redirect_uri: Redirect URI (must match authorization request)

        Returns:
            OAuthTokens with access token and optional refresh token
        """

    @abc.abstractmethod
    async def get_user_info(self, access_token: str) -> OAuthUserInfo:
        """Get user information from the provider.

        Args:
            access_token: Access token from token exchange

        Returns:
            OAuthUserInfo with user details
        """

    async def authenticate(
        self,
        code: str,
        redirect_uri: str | None = None,
    ) -> OAuthUserInfo:
        """Complete OAuth flow: exchange code and get user info.

        Args:
            code: Authorization code from callback
            redirect_uri: Redirect URI (must match authorization request)

        Returns:
            OAuthUserInfo with user details
        """
        tokens = await self.exchange_code(code, redirect_uri)
        return await self.get_user_info(tokens.access_token)
