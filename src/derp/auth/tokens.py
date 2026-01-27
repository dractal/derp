"""OAuth2 token models and storage interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime


@dataclass
class TokenResponse:
    """OAuth2 token response.

    Attributes:
        access_token: Access token for API requests
        token_type: Token type (usually "Bearer")
        expires_in: Token expiration time in seconds
        refresh_token: Refresh token for obtaining new access tokens
        scope: Granted scopes (space-separated string)
        expires_at: Calculated expiration timestamp
    """

    access_token: str
    token_type: str = "Bearer"
    expires_in: int | None = None
    refresh_token: str | None = None
    scope: str | None = None
    expires_at: datetime | None = None

    def is_expired(self) -> bool:
        """Check if the access token is expired.

        Returns:
            True if expired, False otherwise
        """
        if self.expires_at is None:
            return False
        from datetime import UTC, datetime

        return datetime.now(UTC) >= self.expires_at


class TokenStore(ABC):
    """Abstract interface for token storage.

    Implement this interface to provide custom token storage
    (e.g., database, Redis, file system).
    """

    @abstractmethod
    async def get_token(self, user_id: str) -> TokenResponse | None:
        """Get stored tokens for a user.

        Args:
            user_id: Unique identifier for the user

        Returns:
            TokenResponse if found, None otherwise
        """
        ...

    @abstractmethod
    async def save_token(self, user_id: str, token: TokenResponse) -> None:
        """Save tokens for a user.

        Args:
            user_id: Unique identifier for the user
            token: TokenResponse to store
        """
        ...

    @abstractmethod
    async def delete_token(self, user_id: str) -> None:
        """Delete stored tokens for a user.

        Args:
            user_id: Unique identifier for the user
        """
        ...


class MemoryTokenStore(TokenStore):
    """In-memory token storage (for testing or single-process apps).

    Note: Tokens are lost when the process restarts.
    """

    def __init__(self) -> None:
        """Initialize in-memory token store."""
        self._tokens: dict[str, TokenResponse] = {}

    async def get_token(self, user_id: str) -> TokenResponse | None:
        """Get stored tokens for a user."""
        return self._tokens.get(user_id)

    async def save_token(self, user_id: str, token: TokenResponse) -> None:
        """Save tokens for a user."""
        self._tokens[user_id] = token

    async def delete_token(self, user_id: str) -> None:
        """Delete stored tokens for a user."""
        self._tokens.pop(user_id, None)
