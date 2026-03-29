"""GitHub OAuth provider implementation."""

from __future__ import annotations

import logging
from urllib.parse import urlencode

import httpx

from derp.auth.providers.base import BaseOAuthProvider, OAuthTokens, OAuthUserInfo
from derp.config import GitHubOAuthConfig

logger = logging.getLogger(__name__)


class GitHubProvider(BaseOAuthProvider[GitHubOAuthConfig]):
    """GitHub OAuth provider."""

    provider_name = "github"

    # GitHub OAuth endpoints
    AUTHORIZATION_URL = "https://github.com/login/oauth/authorize"
    TOKEN_URL = "https://github.com/login/oauth/access_token"
    USERINFO_URL = "https://api.github.com/user"
    EMAILS_URL = "https://api.github.com/user/emails"

    def get_authorization_url(
        self,
        state: str,
        scopes: list[str] | None = None,
        redirect_uri: str | None = None,
    ) -> str:
        """Generate GitHub OAuth authorization URL."""
        params = {
            "client_id": self._config.client_id,
            "redirect_uri": redirect_uri or self._config.redirect_uri,
            "state": state,
            "scope": " ".join(scopes or self._config.scopes),
        }
        return f"{self.AUTHORIZATION_URL}?{urlencode(params)}"

    async def exchange_code(
        self,
        code: str,
        redirect_uri: str | None = None,
    ) -> OAuthTokens | None:
        """Exchange authorization code for GitHub tokens."""
        data = {
            "client_id": self._config.client_id,
            "client_secret": self._config.client_secret,
            "code": code,
            "redirect_uri": redirect_uri or self._config.redirect_uri,
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.TOKEN_URL,
                data=data,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )

            if response.status_code != 200:
                logger.error("GitHub code exchange failed: %s", response.text)
                return None

            token_data = response.json()

            if "error" in token_data:
                message = token_data.get("error_description", token_data["error"])
                logger.error("GitHub code exchange failed: %s", message)
                return None

        return OAuthTokens(
            access_token=token_data["access_token"],
            token_type=token_data.get("token_type", "bearer"),
            scope=token_data.get("scope"),
        )

    async def get_user_info(self, access_token: str) -> OAuthUserInfo | None:
        """Get user info from GitHub."""
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

        async with httpx.AsyncClient() as client:
            # Get basic user info
            response = await client.get(self.USERINFO_URL, headers=headers)

            if response.status_code != 200:
                logger.error(
                    "GitHub get user info failed: %s", response.text
                )
                return None

            user_data = response.json()

            # Get user emails (GitHub may not include email in user response)
            email = user_data.get("email")
            email_verified = False

            if not email:
                email_response = await client.get(self.EMAILS_URL, headers=headers)
                if email_response.status_code == 200:
                    emails = email_response.json()
                    # Find primary verified email
                    for email_entry in emails:
                        if email_entry.get("primary") and email_entry.get("verified"):
                            email = email_entry["email"]
                            email_verified = True
                            break
                    # Fallback to any verified email
                    if not email:
                        for email_entry in emails:
                            if email_entry.get("verified"):
                                email = email_entry["email"]
                                email_verified = True
                                break
                    # Last resort: any email
                    if not email and emails:
                        email = emails[0]["email"]

            if not email:
                logger.error(
                    "GitHub OAuth: could not retrieve email. "
                    "Ensure the user:email scope is granted."
                )
                return None

        return OAuthUserInfo(
            id=str(user_data["id"]),
            email=email,
            email_verified=email_verified,
            name=user_data.get("name") or user_data.get("login"),
            picture=user_data.get("avatar_url"),
            raw_data=user_data,
        )
