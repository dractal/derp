"""OAuth provider implementations."""

from __future__ import annotations

from derp.auth.providers.base import BaseOAuthProvider, OAuthUserInfo
from derp.auth.providers.github import GitHubProvider
from derp.auth.providers.google import GoogleProvider

__all__ = [
    "BaseOAuthProvider",
    "GitHubProvider",
    "GoogleProvider",
    "OAuthUserInfo",
]
