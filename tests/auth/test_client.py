"""Tests for the authentication client."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from derp.auth import AuthMagicLink
from derp.auth.exceptions import (
    InvalidCredentialsError,
    MagicLinkExpiredError,
    MagicLinkUsedError,
    PasswordValidationError,
    RecoveryTokenExpiredError,
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
from tests.auth.conftest import User


class TestSignUp:
    """Tests for sign up functionality."""

    async def test_sign_up_success(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test successful user signup."""
        user, tokens = await derp.auth.sign_up(
            email="test@example.com", password="password123"
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
        )

        assert user.email == "test@example.com"

    async def test_sign_up_duplicate_email(
        self,
        derp: DerpClient[User],
        mock_smtp: AsyncMock,
    ) -> None:
        """Test signup with duplicate email."""
        await derp.auth.sign_up(email="test@example.com", password="password123")

        with pytest.raises(UserAlreadyExistsError):
            await derp.auth.sign_up(
                email="test@example.com",
                password="different_password",
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
            )

    async def test_sign_up_disabled(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test signup when disabled."""
        derp.auth._config.email.enable_signup = False

        with pytest.raises(SignupDisabledError):
            await derp.auth.sign_up(
                email="test@example.com",
                password="password123",
            )


class TestSignIn:
    """Tests for sign in functionality."""

    async def test_sign_in_success(
        self,
        derp: DerpClient[User],
        mock_smtp: AsyncMock,
    ) -> None:
        """Test successful sign in."""
        user, _ = await derp.auth.sign_up(
            email="test@example.com", password="password123"
        )
        assert user.confirmation_token is not None
        await derp.auth.confirm_email(user.confirmation_token)

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
        await derp.auth.sign_up(email="test@example.com", password="password123")

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
            email="test@example.com", password="password123"
        )

        # Deactivate user
        await derp.auth.update_user(user_id=user.id, is_active=False)

        with pytest.raises(UserNotActiveError):
            await derp.auth.sign_in_with_password(
                email="test@example.com", password="password123"
            )

    async def test_sign_in_case_insensitive_email(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test sign in with different email case."""
        user, _ = await derp.auth.sign_up(
            email="test@example.com", password="password123"
        )
        assert user.confirmation_token is not None
        await derp.auth.confirm_email(user.confirmation_token)

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
            email="test@example.com", password="password123"
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
            email="test@example.com", password="password123"
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
        await derp.auth.sign_up(email="test@example.com", password="password123")

        # Send magic link
        await derp.auth.sign_in_with_magic_link("test@example.com")

        # Verify email was sent (mock)
        # In real test, would verify the magic link record was created

    async def test_verify_magic_link(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test verifying a magic link."""
        # Create user
        user, _ = await derp.auth.sign_up(
            email="test@example.com", password="password123"
        )

        token = generate_secure_token()
        expires_at = datetime.now(UTC) + timedelta(hours=1)

        await (
            derp.db.insert(AuthMagicLink)
            .values(
                email="test@example.com",
                token=token,
                expires_at=expires_at,
            )
            .execute()
        )

        # Verify magic link
        verified_user, tokens = await derp.auth.verify_magic_link(token)

        assert verified_user.email == "test@example.com"
        assert tokens.access_token is not None

    async def test_verify_expired_magic_link(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test verifying an expired magic link."""
        await derp.auth.sign_up(email="test@example.com", password="password123")

        token = generate_secure_token()
        expires_at = datetime.now(UTC) - timedelta(hours=1)  # Already expired

        await (
            derp.db.insert(AuthMagicLink)
            .values(
                email="test@example.com",
                token=token,
                expires_at=expires_at,
            )
            .execute()
        )

        with pytest.raises(MagicLinkExpiredError):
            await derp.auth.verify_magic_link(token)

    async def test_verify_used_magic_link(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test verifying a used magic link."""
        await derp.auth.sign_up(email="test@example.com", password="password123")

        token = generate_secure_token()
        expires_at = datetime.now(UTC) + timedelta(hours=1)

        await (
            derp.db.insert(AuthMagicLink)
            .values(
                email="test@example.com",
                token=token,
                expires_at=expires_at,
                used=True,  # Already used
            )
            .execute()
        )

        with pytest.raises(MagicLinkUsedError):
            await derp.auth.verify_magic_link(token)


class TestPasswordRecovery:
    """Tests for password recovery functionality."""

    async def test_request_recovery(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test requesting password recovery."""
        await derp.auth.sign_up(email="test@example.com", password="password123")

        # Should not raise
        await derp.auth.request_password_recovery("test@example.com")

    async def test_request_recovery_nonexistent_user(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test requesting recovery for non-existent user."""
        # Should not raise (don't reveal user existence)
        await derp.auth.request_password_recovery("nonexistent@example.com")

    async def test_reset_password(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test resetting password."""
        user, _ = await derp.auth.sign_up(
            email="test@example.com", password="oldpassword123"
        )
        assert user.confirmation_token is not None
        await derp.auth.confirm_email(user.confirmation_token)

        token = generate_secure_token()
        await (
            derp.db.update(User)
            .set(
                recovery_token=token,
                recovery_sent_at=datetime.now(UTC),
            )
            .where(User.c.id == user.id)
            .execute()
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
        """Test reset with expired token."""
        user, _ = await derp.auth.sign_up(
            email="test@example.com", password="oldpassword123"
        )

        token = generate_secure_token()
        await (
            derp.db.update(User)
            .set(
                recovery_token=token,
                recovery_sent_at=datetime.now(UTC) - timedelta(hours=2),  # Expired
            )
            .where(User.c.id == user.id)
            .execute()
        )

        with pytest.raises(RecoveryTokenExpiredError):
            await derp.auth.reset_password(token, "newpassword123")


class TestSessionManagement:
    """Tests for session management."""

    async def test_sign_out(self, derp: DerpClient[User], mock_smtp: AsyncMock) -> None:
        """Test signing out a session."""
        user, tokens = await derp.auth.sign_up(
            email="test@example.com", password="password123"
        )

        payload = decode_token(derp.auth._config.jwt, tokens.access_token)

        # Sign out
        await derp.auth.sign_out(payload.session_id)

        # Refresh should fail
        with pytest.raises(RefreshTokenRevokedError):
            await derp.auth.refresh_token(tokens.refresh_token)

    async def test_sign_out_all(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test signing out all sessions."""
        user, tokens1 = await derp.auth.sign_up(
            email="test@example.com", password="password123"
        )
        assert user.confirmation_token is not None
        await derp.auth.confirm_email(user.confirmation_token)

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
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test getting user by ID."""
        user, _ = await derp.auth.sign_up(
            email="test@example.com", password="password123"
        )
        assert user.confirmation_token is not None
        await derp.auth.confirm_email(user.confirmation_token)

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
        await derp.auth.sign_up(email="test@example.com", password="password123")

        found = await derp.auth.get_user(email="test@example.com")

        assert found is not None
        assert found.email == "test@example.com"

    async def test_update_user(
        self, derp: DerpClient[User], mock_smtp: AsyncMock
    ) -> None:
        """Test updating user."""
        user, _ = await derp.auth.sign_up(
            email="test@example.com", password="password123"
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
