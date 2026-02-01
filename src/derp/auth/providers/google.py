"""Google OAuth 2.0 provider implementation."""

from __future__ import annotations

from urllib.parse import urlencode

import httpx

from derp.auth.config import GoogleOAuthConfig
from derp.auth.exceptions import OAuthProviderError
from derp.auth.providers.base import BaseOAuthProvider, OAuthTokens, OAuthUserInfo


class GoogleProvider(BaseOAuthProvider[GoogleOAuthConfig]):
    """Google OAuth 2.0 provider."""

    provider_name = "google"

    # Google OAuth endpoints
    AUTHORIZATION_URL = "https://accounts.google.com/o/oauth2/v2/auth"
    TOKEN_URL = "https://oauth2.googleapis.com/token"
    USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"

    def get_authorization_url(
        self,
        state: str,
        scopes: list[str] | None = None,
        redirect_uri: str | None = None,
    ) -> str:
        """Generate Google OAuth authorization URL."""
        params = {
            "client_id": self._config.client_id,
            "redirect_uri": redirect_uri or self._config.redirect_uri,
            "response_type": "code",
            "scope": " ".join(scopes or self._config.scopes),
            "state": state,
            "access_type": "offline",  # Request refresh token
            "prompt": "consent",  # Force consent screen for refresh token
        }
        return f"{self.AUTHORIZATION_URL}?{urlencode(params)}"

    async def exchange_code(
        self,
        code: str,
        redirect_uri: str | None = None,
    ) -> OAuthTokens:
        """Exchange authorization code for Google tokens."""
        data = {
            "client_id": self._config.client_id,
            "client_secret": self._config.client_secret,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri or self._config.redirect_uri,
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.TOKEN_URL,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            if response.status_code != 200:
                error_data = response.json() if response.content else {}
                message = error_data.get("error_description", response.text)
                raise OAuthProviderError(f"Failed to exchange code: {message}")

            token_data = response.json()

        return OAuthTokens(
            access_token=token_data["access_token"],
            token_type=token_data.get("token_type", "bearer"),
            refresh_token=token_data.get("refresh_token"),
            expires_in=token_data.get("expires_in"),
            scope=token_data.get("scope"),
            id_token=token_data.get("id_token"),
        )

    async def get_user_info(self, access_token: str) -> OAuthUserInfo:
        """Get user info from Google."""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                self.USERINFO_URL,
                headers={"Authorization": f"Bearer {access_token}"},
            )

            if response.status_code != 200:
                raise OAuthProviderError(f"Failed to get user info: {response.text}")

            user_data = response.json()

        return OAuthUserInfo(
            id=user_data["id"],
            email=user_data["email"],
            email_verified=user_data.get("verified_email", False),
            name=user_data.get("name"),
            picture=user_data.get("picture"),
            raw_data=user_data,
        )
