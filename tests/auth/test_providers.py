"""Tests for OAuth providers."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from derp.config import GitHubOAuthConfig, GoogleOAuthConfig
from derp.auth.exceptions import OAuthProviderError
from derp.auth.providers import GitHubProvider, GoogleProvider, OAuthUserInfo


@pytest.fixture
def google_config() -> GoogleOAuthConfig:
    """Create Google OAuth config for testing."""
    return GoogleOAuthConfig(
        client_id="test-client-id",
        client_secret="test-client-secret",
        redirect_uri="http://localhost:8000/auth/oauth/google/callback",
    )


@pytest.fixture
def github_config() -> GitHubOAuthConfig:
    """Create GitHub OAuth config for testing."""
    return GitHubOAuthConfig(
        client_id="test-client-id",
        client_secret="test-client-secret",
        redirect_uri="http://localhost:8000/auth/oauth/github/callback",
    )


class TestGoogleProvider:
    """Tests for Google OAuth provider."""

    def test_get_authorization_url(self, google_config: GoogleOAuthConfig) -> None:
        """Test generating authorization URL."""
        provider = GoogleProvider(google_config)
        state = "random-state-token"

        url = provider.get_authorization_url(state)

        assert "accounts.google.com" in url
        assert "client_id=test-client-id" in url
        assert f"state={state}" in url
        assert "redirect_uri=" in url
        assert "scope=" in url
        assert "response_type=code" in url

    def test_get_authorization_url_custom_scopes(
        self, google_config: GoogleOAuthConfig
    ) -> None:
        """Test authorization URL with custom scopes."""
        provider = GoogleProvider(google_config)

        url = provider.get_authorization_url("state", scopes=["email"])

        assert "scope=email" in url

    def test_get_authorization_url_custom_redirect(
        self, google_config: GoogleOAuthConfig
    ) -> None:
        """Test authorization URL with custom redirect."""
        provider = GoogleProvider(google_config)
        custom_redirect = "http://custom.com/callback"

        url = provider.get_authorization_url("state", redirect_uri=custom_redirect)

        assert "redirect_uri=http%3A%2F%2Fcustom.com%2Fcallback" in url

    async def test_exchange_code_success(
        self, google_config: GoogleOAuthConfig
    ) -> None:
        """Test successful code exchange."""
        provider = GoogleProvider(google_config)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "access-token-123",
            "token_type": "Bearer",
            "refresh_token": "refresh-token-456",
            "expires_in": 3600,
            "scope": "openid email profile",
            "id_token": "id-token-789",
        }

        with patch("derp.auth.providers.google.httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client

            tokens = await provider.exchange_code("auth-code")

        assert tokens.access_token == "access-token-123"
        assert tokens.refresh_token == "refresh-token-456"
        assert tokens.expires_in == 3600
        assert tokens.id_token == "id-token-789"

    async def test_exchange_code_failure(
        self, google_config: GoogleOAuthConfig
    ) -> None:
        """Test code exchange failure."""
        provider = GoogleProvider(google_config)

        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.content = b'{"error": "invalid_grant"}'
        mock_response.json.return_value = {
            "error": "invalid_grant",
            "error_description": "Invalid code",
        }
        mock_response.text = "Invalid code"

        with patch("derp.auth.providers.google.httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client

            with pytest.raises(OAuthProviderError):
                await provider.exchange_code("invalid-code")

    async def test_get_user_info_success(
        self, google_config: GoogleOAuthConfig
    ) -> None:
        """Test getting user info."""
        provider = GoogleProvider(google_config)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": "12345",
            "email": "user@gmail.com",
            "verified_email": True,
            "name": "Test User",
            "picture": "https://example.com/photo.jpg",
        }

        with patch("derp.auth.providers.google.httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client

            user_info = await provider.get_user_info("access-token")

        assert user_info.id == "12345"
        assert user_info.email == "user@gmail.com"
        assert user_info.email_verified is True
        assert user_info.name == "Test User"
        assert user_info.picture == "https://example.com/photo.jpg"

    async def test_get_user_info_failure(
        self, google_config: GoogleOAuthConfig
    ) -> None:
        """Test user info failure."""
        provider = GoogleProvider(google_config)

        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"

        with patch("derp.auth.providers.google.httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client

            with pytest.raises(OAuthProviderError):
                await provider.get_user_info("invalid-token")


class TestGitHubProvider:
    """Tests for GitHub OAuth provider."""

    def test_get_authorization_url(self, github_config: GitHubOAuthConfig) -> None:
        """Test generating authorization URL."""
        provider = GitHubProvider(github_config)
        state = "random-state-token"

        url = provider.get_authorization_url(state)

        assert "github.com/login/oauth/authorize" in url
        assert "client_id=test-client-id" in url
        assert f"state={state}" in url
        assert "redirect_uri=" in url
        assert "scope=" in url

    async def test_exchange_code_success(
        self, github_config: GitHubOAuthConfig
    ) -> None:
        """Test successful code exchange."""
        provider = GitHubProvider(github_config)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "access-token-123",
            "token_type": "bearer",
            "scope": "user:email",
        }

        with patch("derp.auth.providers.github.httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client

            tokens = await provider.exchange_code("auth-code")

        assert tokens.access_token == "access-token-123"
        assert tokens.scope == "user:email"

    async def test_exchange_code_failure(
        self, github_config: GitHubOAuthConfig
    ) -> None:
        """Test code exchange failure."""
        provider = GitHubProvider(github_config)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "error": "bad_verification_code",
            "error_description": "The code passed is incorrect",
        }

        with patch("derp.auth.providers.github.httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client

            with pytest.raises(OAuthProviderError):
                await provider.exchange_code("invalid-code")

    async def test_get_user_info_success(
        self, github_config: GitHubOAuthConfig
    ) -> None:
        """Test getting user info."""
        provider = GitHubProvider(github_config)

        user_response = MagicMock()
        user_response.status_code = 200
        user_response.json.return_value = {
            "id": 12345,
            "login": "testuser",
            "name": "Test User",
            "email": "user@github.com",
            "avatar_url": "https://github.com/avatar.jpg",
        }

        with patch("derp.auth.providers.github.httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get.return_value = user_response
            mock_client_class.return_value.__aenter__.return_value = mock_client

            user_info = await provider.get_user_info("access-token")

        assert user_info.id == "12345"
        assert user_info.email == "user@github.com"
        assert user_info.name == "Test User"
        assert user_info.picture == "https://github.com/avatar.jpg"

    async def test_get_user_info_fetches_email(
        self, github_config: GitHubOAuthConfig
    ) -> None:
        """Test fetching email when not in user response."""
        provider = GitHubProvider(github_config)

        user_response = MagicMock()
        user_response.status_code = 200
        user_response.json.return_value = {
            "id": 12345,
            "login": "testuser",
            "name": "Test User",
            "email": None,  # No email in user response
            "avatar_url": "https://github.com/avatar.jpg",
        }

        email_response = MagicMock()
        email_response.status_code = 200
        email_response.json.return_value = [
            {"email": "public@example.com", "primary": False, "verified": True},
            {"email": "primary@example.com", "primary": True, "verified": True},
        ]

        with patch("derp.auth.providers.github.httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get.side_effect = [user_response, email_response]
            mock_client_class.return_value.__aenter__.return_value = mock_client

            user_info = await provider.get_user_info("access-token")

        assert user_info.email == "primary@example.com"
        assert user_info.email_verified is True


class TestOAuthUserInfo:
    """Tests for OAuthUserInfo dataclass."""

    def test_create_user_info(self) -> None:
        """Test creating user info."""
        info = OAuthUserInfo(
            id="123",
            email="test@example.com",
            email_verified=True,
            name="Test User",
            picture="https://example.com/photo.jpg",
            raw_data={"extra": "data"},
        )

        assert info.id == "123"
        assert info.email == "test@example.com"
        assert info.email_verified is True
        assert info.name == "Test User"
        assert info.picture == "https://example.com/photo.jpg"
        assert info.raw_data == {"extra": "data"}

    def test_default_values(self) -> None:
        """Test default values."""
        info = OAuthUserInfo(id="123", email="test@example.com")

        assert info.email_verified is False
        assert info.name is None
        assert info.picture is None
        assert info.raw_data is None
