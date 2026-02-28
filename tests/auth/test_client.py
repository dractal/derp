"""Tests for the authentication client."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from derp.auth.exceptions import (
    InvalidCredentialsError,
    MagicLinkExpiredError,
    PasswordValidationError,
    RecoveryTokenInvalidError,
    RefreshTokenReusedError,
    RefreshTokenRevokedError,
    SignupDisabledError,
    UserAlreadyExistsError,
    UserNotActiveError,
    UserNotFoundError,
)
from derp.auth.jwt import decode_token
from derp.auth.password import generate_secure_token
from derp.derp_client import DerpClient
from derp.kv.valkey import ValkeyClient
from tests.auth.conftest import User, get_confirmation_token


class TestSignUp:
    """Tests for sign up functionality."""

    async def test_sign_up_success(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test successful user signup."""
        user, tokens = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )

        assert user.email == "test@example.com"
        assert user.provider == "email"
        assert user.is_active is True
        assert user.is_superuser is False
        assert tokens.access_token is not None
        assert tokens.refresh_token is not None

    async def test_sign_up_with_metadata(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test signup with user metadata."""
        metadata = {"name": "Test User", "locale": "en-US"}
        user, _ = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
            user_metadata=metadata,
        )

        assert user.user_metadata == metadata

    async def test_sign_up_normalizes_email(
        self,
        derp: DerpClient[User],
        mock_smtp: AsyncMock,
    ) -> None:
        """Test that email is lowercased during signup."""
        user, _ = await derp.auth.sign_up(
            email="Test@Example.COM",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )

        assert user.email == "test@example.com"

    async def test_sign_up_duplicate_email(
        self,
        derp: DerpClient[User],
        mock_smtp: AsyncMock,
    ) -> None:
        """Test signup with duplicate email."""
        await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )

        with pytest.raises(UserAlreadyExistsError):
            await derp.auth.sign_up(
                email="test@example.com",
                password="different_password",
                confirmation_url="http://localhost:3000/auth/confirm",
            )

    async def test_sign_up_weak_password(
        self,
        derp: DerpClient[User],
        mock_smtp: AsyncMock,
    ) -> None:
        """Test signup with weak password."""
        with pytest.raises(PasswordValidationError):
            await derp.auth.sign_up(
                email="test@example.com",
                password="short",  # Too short
                confirmation_url="http://localhost:3000/auth/confirm",
            )

    async def test_sign_up_disabled(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test signup when disabled."""
        derp.auth._config.enable_signup = False

        with pytest.raises(SignupDisabledError):
            await derp.auth.sign_up(
                email="test@example.com",
                password="password123",
                confirmation_url="http://localhost:3000/auth/confirm",
            )


class TestSignIn:
    """Tests for sign in functionality."""

    async def test_sign_in_success(
        self,
        derp: DerpClient[User],
        kv_client: ValkeyClient,
        mock_smtp: AsyncMock,
    ) -> None:
        """Test successful sign in."""
        user, _ = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        token = await get_confirmation_token(
            kv_client, derp.auth._config.cache_prefix, str(user.id)
        )
        assert token is not None
        await derp.auth.confirm_email(token)

        user, tokens = await derp.auth.sign_in_with_password(
            email="test@example.com", password="password123"
        )

        assert user.email == "test@example.com"
        assert tokens.access_token is not None
        assert tokens.refresh_token is not None

    async def test_sign_in_wrong_password(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test sign in with wrong password."""
        await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )

        with pytest.raises(InvalidCredentialsError):
            await derp.auth.sign_in_with_password(
                email="test@example.com", password="wrongpassword"
            )

    async def test_sign_in_user_not_found(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test sign in with non-existent user."""
        with pytest.raises(InvalidCredentialsError):
            await derp.auth.sign_in_with_password(
                email="nonexistent@example.com", password="password123"
            )

    async def test_sign_in_inactive_user(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test sign in with inactive user."""
        user, _ = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )

        # Deactivate user
        await derp.auth.update_user(user_id=user.id, is_active=False)

        with pytest.raises(UserNotActiveError):
            await derp.auth.sign_in_with_password(
                email="test@example.com", password="password123"
            )

    async def test_sign_in_case_insensitive_email(
        self,
        derp: DerpClient[User],
        kv_client: ValkeyClient,
        mock_smtp: AsyncMock,
    ) -> None:
        """Test sign in with different email case."""
        user, _ = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        token = await get_confirmation_token(
            kv_client, derp.auth._config.cache_prefix, str(user.id)
        )
        assert token is not None
        await derp.auth.confirm_email(token)

        user, _ = await derp.auth.sign_in_with_password(
            email="TEST@EXAMPLE.COM", password="password123"
        )

        assert user.email == "test@example.com"


class TestTokenRefresh:
    """Tests for token refresh functionality."""

    async def test_refresh_success(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test successful token refresh."""
        _, tokens = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )

        new_tokens = await derp.auth.refresh_token(tokens.refresh_token)

        assert new_tokens.access_token is not None
        assert new_tokens.refresh_token is not None
        # Refresh token should be different (rotation)
        assert new_tokens.refresh_token != tokens.refresh_token

    async def test_refresh_invalid_token(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test refresh with invalid token."""
        with pytest.raises(RefreshTokenRevokedError):
            await derp.auth.refresh_token("invalid_token")

    async def test_refresh_revoked_token(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test refresh with revoked token."""
        _, tokens = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )

        # Use the token once
        await derp.auth.refresh_token(tokens.refresh_token)

        # Try to use it again (should be revoked due to rotation)
        with pytest.raises(RefreshTokenReusedError):
            await derp.auth.refresh_token(tokens.refresh_token)


class TestMagicLink:
    """Tests for magic link functionality."""

    async def test_send_magic_link(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test sending a magic link."""
        # Create user first
        await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )

        # Send magic link
        await derp.auth.sign_in_with_magic_link(
            email="test@example.com",
            magic_link_url="http://localhost:3000/auth/magic-link",
        )

    async def test_verify_magic_link(
        self,
        derp: DerpClient[User],
        kv_client: ValkeyClient,
        mock_smtp: AsyncMock,
    ) -> None:
        """Test verifying a magic link."""
        # Create user
        await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )

        # Store magic link token in KV
        token = generate_secure_token()
        prefix = derp.auth._config.cache_prefix
        await kv_client.set(
            f"{prefix}:magic_link:{token}".encode(),
            b"test@example.com",
            ttl=3600,
        )

        # Verify magic link
        verified_user, tokens = await derp.auth.verify_magic_link(token)

        assert verified_user.email == "test@example.com"
        assert tokens.access_token is not None

    async def test_verify_expired_magic_link(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test verifying an expired magic link (not found in KV)."""
        await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )

        # Token doesn't exist in KV = expired/invalid
        token = generate_secure_token()

        with pytest.raises(MagicLinkExpiredError):
            await derp.auth.verify_magic_link(token)

    async def test_verify_magic_link_single_use(
        self,
        derp: DerpClient[User],
        kv_client: ValkeyClient,
        mock_smtp: AsyncMock,
    ) -> None:
        """Test that a magic link can only be used once."""
        await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )

        # Store magic link token in KV
        token = generate_secure_token()
        prefix = derp.auth._config.cache_prefix
        await kv_client.set(
            f"{prefix}:magic_link:{token}".encode(),
            b"test@example.com",
            ttl=3600,
        )

        # First use succeeds
        await derp.auth.verify_magic_link(token)

        # Second use fails (deleted from KV)
        with pytest.raises(MagicLinkExpiredError):
            await derp.auth.verify_magic_link(token)


class TestPasswordRecovery:
    """Tests for password recovery functionality."""

    async def test_request_recovery(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test requesting password recovery."""
        await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )

        # Should not raise
        await derp.auth.request_password_recovery(
            email="test@example.com", recovery_url="http://localhost:3000/auth/recovery"
        )

    async def test_request_recovery_nonexistent_user(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test requesting recovery for non-existent user."""
        # Should not raise (don't reveal user existence)
        await derp.auth.request_password_recovery(
            email="nonexistent@example.com",
            recovery_url="http://localhost:3000/auth/recovery",
        )

    async def test_reset_password(
        self,
        derp: DerpClient[User],
        kv_client: ValkeyClient,
        mock_smtp: AsyncMock,
    ) -> None:
        """Test resetting password."""
        user, _ = await derp.auth.sign_up(
            email="test@example.com",
            password="oldpassword123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        conf_token = await get_confirmation_token(
            kv_client, derp.auth._config.cache_prefix, str(user.id)
        )
        assert conf_token is not None
        await derp.auth.confirm_email(conf_token)

        # Store recovery token in KV
        token = generate_secure_token()
        prefix = derp.auth._config.cache_prefix
        await kv_client.set(
            f"{prefix}:recovery:{token}".encode(),
            str(user.id).encode(),
            ttl=3600,
        )

        # Reset password
        await derp.auth.reset_password(token, "newpassword123")

        # Should be able to sign in with new password
        user, _ = await derp.auth.sign_in_with_password(
            email="test@example.com", password="newpassword123"
        )
        assert user is not None

    async def test_reset_password_invalid_token(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test reset with invalid token."""
        with pytest.raises(RecoveryTokenInvalidError):
            await derp.auth.reset_password("invalid_token", "newpassword123")

    async def test_reset_password_expired_token(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test reset with expired token (not found in KV)."""
        await derp.auth.sign_up(
            email="test@example.com",
            password="oldpassword123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )

        # Token doesn't exist in KV = expired/invalid
        token = generate_secure_token()

        with pytest.raises(RecoveryTokenInvalidError):
            await derp.auth.reset_password(token, "newpassword123")


class TestSessionManagement:
    """Tests for session management."""

    async def test_sign_out(self, derp: DerpClient[User], mock_smtp: AsyncMock) -> None:
        """Test signing out a session."""
        user, tokens = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )

        payload = decode_token(derp.auth._config.jwt, tokens.access_token)

        # Sign out
        await derp.auth.sign_out(payload.session_id)

        # Refresh should fail
        with pytest.raises(RefreshTokenRevokedError):
            await derp.auth.refresh_token(tokens.refresh_token)

    async def test_sign_out_all(
        self,
        derp: DerpClient[User],
        kv_client: ValkeyClient,
        mock_smtp: AsyncMock,
    ) -> None:
        """Test signing out all sessions."""
        user, tokens1 = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        token = await get_confirmation_token(
            kv_client, derp.auth._config.cache_prefix, str(user.id)
        )
        assert token is not None
        await derp.auth.confirm_email(token)

        # Create another session
        _, tokens2 = await derp.auth.sign_in_with_password(
            email="test@example.com",
            password="password123",
        )

        # Sign out all
        await derp.auth.sign_out_all(user.id)

        # Both refresh tokens should fail
        with pytest.raises(RefreshTokenRevokedError):
            await derp.auth.refresh_token(tokens1.refresh_token)

        with pytest.raises(RefreshTokenRevokedError):
            await derp.auth.refresh_token(tokens2.refresh_token)


class TestUserManagement:
    """Tests for user management."""

    async def test_get_user_by_id(
        self,
        derp: DerpClient[User],
        kv_client: ValkeyClient,
        mock_smtp: AsyncMock,
    ) -> None:
        """Test getting user by ID."""
        user, _ = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        token = await get_confirmation_token(
            kv_client, derp.auth._config.cache_prefix, str(user.id)
        )
        assert token is not None
        await derp.auth.confirm_email(token)

        found = await derp.auth.get_user(user.id)

        assert found is not None
        assert found.email == "test@example.com"

    async def test_get_user_by_id_not_found(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test getting non-existent user by ID."""
        found = await derp.auth.get_user(uuid.uuid4())
        assert found is None

    async def test_get_user_by_email(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test getting user by email."""
        await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )

        found = await derp.auth.get_user(email="test@example.com")

        assert found is not None
        assert found.email == "test@example.com"

    async def test_update_user(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test updating user."""
        user, _ = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )

        updated = await derp.auth.update_user(
            user_id=user.id, user_metadata={"name": "Updated Name"}
        )

        assert updated.user_metadata == {"name": "Updated Name"}

    async def test_update_user_not_found(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test updating non-existent user."""
        with pytest.raises(UserNotFoundError):
            await derp.auth.update_user(user_id=uuid.uuid4())
