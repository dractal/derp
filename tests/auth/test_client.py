"""Tests for the authentication client."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from derp.auth.exceptions import SignupDisabledError
from derp.auth.jwt import decode_token
from derp.auth.password import generate_secure_token
from derp.derp_client import DerpClient
from derp.kv.valkey import ValkeyClient
from tests.auth.conftest import get_confirmation_token
from tests.conftest import bearer_request


class TestSignUp:
    """Tests for sign up functionality."""

    async def test_sign_up_success(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """Test successful user signup."""
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None
        assert result.user is not None
        assert result.tokens is not None
        assert result.user.email == "test@example.com"
        assert result.user.metadata["provider"] == "email"
        assert result.user.is_active is True
        assert result.user.is_superuser is False
        assert result.tokens.access_token is not None
        assert result.tokens.refresh_token is not None

    async def test_sign_up_normalizes_email(
        self,
        derp: DerpClient,
        mock_smtp: AsyncMock,
    ) -> None:
        """Test that email is lowercased during signup."""
        result = await derp.auth.sign_up(
            email="Test@Example.COM",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None

        assert result.user.email == "test@example.com"

    async def test_sign_up_duplicate_email(
        self,
        derp: DerpClient,
        mock_smtp: AsyncMock,
    ) -> None:
        """Test signup with duplicate email."""
        await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )

        result = await derp.auth.sign_up(
            email="test@example.com",
            password="different_password",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is None

    async def test_sign_up_weak_password(
        self,
        derp: DerpClient,
        mock_smtp: AsyncMock,
    ) -> None:
        """Test signup with weak password."""
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="short",  # Too short
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is None

    async def test_sign_up_disabled(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """Test signup when disabled."""
        assert derp.config.auth is not None and derp.config.auth.native is not None
        derp.config.auth.native.enable_signup = False

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
        derp: DerpClient,
        kv_client: ValkeyClient,
        mock_smtp: AsyncMock,
    ) -> None:
        """Test successful sign in."""
        assert derp.config.auth is not None and derp.config.auth.native is not None
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None
        token = await get_confirmation_token(
            kv_client, derp.config.auth.native.cache_prefix, str(result.user.id)
        )
        assert token is not None
        await derp.auth.confirm_email(token)

        sign_in_result = await derp.auth.sign_in_with_password(
            email="test@example.com", password="password123"
        )
        
        assert sign_in_result is not None
        assert sign_in_result.user.email == "test@example.com"
        assert sign_in_result.tokens.access_token is not None
        assert sign_in_result.tokens.refresh_token is not None

    async def test_sign_in_wrong_password(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """Test sign in with wrong password."""
        await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )

        result = await derp.auth.sign_in_with_password(
            email="test@example.com", password="wrongpassword"
        )
        assert result is None

    async def test_sign_in_user_not_found(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """Test sign in with non-existent user."""
        result = await derp.auth.sign_in_with_password(
            email="nonexistent@example.com", password="password123"
        )
        assert result is None

    async def test_sign_in_inactive_user(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """Test sign in with inactive user."""
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None

        # Deactivate user
        await derp.auth.update_user(user_id=result.user.id, is_active=False)

        result = await derp.auth.sign_in_with_password(
            email="test@example.com", password="password123"
        )
        assert result is None

    async def test_sign_in_case_insensitive_email(
        self,
        derp: DerpClient,
        kv_client: ValkeyClient,
        mock_smtp: AsyncMock,
    ) -> None:
        """Test sign in with different email case."""
        assert derp.config.auth is not None and derp.config.auth.native is not None
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None
        token = await get_confirmation_token(
            kv_client, derp.config.auth.native.cache_prefix, str(result.user.id)
        )
        assert token is not None
        await derp.auth.confirm_email(token)

        sign_in_result = await derp.auth.sign_in_with_password(
            email="TEST@EXAMPLE.COM", password="password123"
        )

        assert sign_in_result is not None
        assert sign_in_result.user.email == "test@example.com"


class TestTokenRefresh:
    """Tests for token refresh functionality."""

    async def test_refresh_success(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """Test successful token refresh."""
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None

        new_tokens = await derp.auth.refresh_token(result.tokens.refresh_token)

        assert new_tokens is not None
        assert new_tokens.access_token is not None
        assert new_tokens.refresh_token is not None
        # Refresh token should be different (rotation)
        assert new_tokens.refresh_token != result.tokens.refresh_token

    async def test_refresh_invalid_token(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """Test refresh with invalid token."""
        result = await derp.auth.refresh_token("invalid_token")
        assert result is None

    async def test_refresh_revoked_token(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """Test refresh with revoked token."""
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None

        # Use the token once
        await derp.auth.refresh_token(result.tokens.refresh_token)

        # Try to use it again (should be revoked due to rotation)
        result2 = await derp.auth.refresh_token(result.tokens.refresh_token)
        assert result2 is None


class TestMagicLink:
    """Tests for magic link functionality."""

    async def test_send_magic_link(
        self, derp: DerpClient, mock_smtp: AsyncMock
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
        derp: DerpClient,
        kv_client: ValkeyClient,
        mock_smtp: AsyncMock,
    ) -> None:
        """Test verifying a magic link."""
        assert derp.config.auth is not None and derp.config.auth.native is not None
        # Create user
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None

        # Store magic link token in KV (keyed by user ID)
        token = generate_secure_token()
        prefix = derp.config.auth.native.cache_prefix
        await kv_client.set(
            f"{prefix}:magic_link:{token}".encode(),
            str(result.user.id).encode(),
            ttl=3600,
        )

        # Verify magic link
        verify_result = await derp.auth.verify_magic_link(token)

        assert verify_result is not None
        assert verify_result.user.email == "test@example.com"
        assert verify_result.tokens.access_token is not None

    async def test_verify_expired_magic_link(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """Test verifying an expired magic link (not found in KV)."""
        await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )

        # Token doesn't exist in KV = expired/invalid
        token = generate_secure_token()

        result = await derp.auth.verify_magic_link(token)
        assert result is None

    async def test_verify_magic_link_single_use(
        self,
        derp: DerpClient,
        kv_client: ValkeyClient,
        mock_smtp: AsyncMock,
    ) -> None:
        """Test that a magic link can only be used once."""
        assert derp.config.auth is not None and derp.config.auth.native is not None
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None

        # Store magic link token in KV (keyed by user ID)
        token = generate_secure_token()
        prefix = derp.config.auth.native.cache_prefix
        await kv_client.set(
            f"{prefix}:magic_link:{token}".encode(),
            str(result.user.id).encode(),
            ttl=3600,
        )

        # First use succeeds
        await derp.auth.verify_magic_link(token)

        # Second use fails (deleted from KV)
        result = await derp.auth.verify_magic_link(token)
        assert result is None


class TestPasswordRecovery:
    """Tests for password recovery functionality."""

    async def test_request_recovery(
        self, derp: DerpClient, mock_smtp: AsyncMock
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
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """Test requesting recovery for non-existent user."""
        # Should not raise (don't reveal user existence)
        await derp.auth.request_password_recovery(
            email="nonexistent@example.com",
            recovery_url="http://localhost:3000/auth/recovery",
        )

    async def test_reset_password(
        self,
        derp: DerpClient,
        kv_client: ValkeyClient,
        mock_smtp: AsyncMock,
    ) -> None:
        """Test resetting password."""
        assert derp.config.auth is not None and derp.config.auth.native is not None
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="oldpassword123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None
        conf_token = await get_confirmation_token(
            kv_client, derp.config.auth.native.cache_prefix, str(result.user.id)
        )
        assert conf_token is not None
        await derp.auth.confirm_email(conf_token)

        # Store recovery token in KV
        token = generate_secure_token()
        prefix = derp.config.auth.native.cache_prefix
        await kv_client.set(
            f"{prefix}:recovery:{token}".encode(),
            str(result.user.id).encode(),
            ttl=3600,
        )

        # Reset password
        await derp.auth.reset_password(token, "newpassword123")

        # Should be able to sign in with new password
        sign_in_result = await derp.auth.sign_in_with_password(
            email="test@example.com", password="newpassword123"
        )
        assert sign_in_result is not None

    async def test_reset_password_invalid_token(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """Test reset with invalid token."""
        result = await derp.auth.reset_password("invalid_token", "newpassword123")
        assert result is None

    async def test_reset_password_expired_token(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """Test reset with expired token (not found in KV)."""
        await derp.auth.sign_up(
            email="test@example.com",
            password="oldpassword123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )

        # Token doesn't exist in KV = expired/invalid
        token = generate_secure_token()

        result = await derp.auth.reset_password(token, "newpassword123")
        assert result is None


class TestSessionManagement:
    """Tests for session management."""

    async def test_sign_out(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        """Test signing out a session."""
        assert derp.config.auth is not None and derp.config.auth.native is not None
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None

        payload = decode_token(derp.config.auth.native.jwt, result.tokens.access_token)
        assert payload is not None

        # Sign out
        await derp.auth.sign_out(payload.session_id)

        # Refresh should fail
        refreshed = await derp.auth.refresh_token(result.tokens.refresh_token)
        assert refreshed is None

    async def test_sign_out_all(
        self,
        derp: DerpClient,
        kv_client: ValkeyClient,
        mock_smtp: AsyncMock,
    ) -> None:
        """Test signing out all sessions."""
        assert derp.config.auth is not None and derp.config.auth.native is not None
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None
        token = await get_confirmation_token(
            kv_client, derp.config.auth.native.cache_prefix, str(result.user.id)
        )
        assert token is not None
        await derp.auth.confirm_email(token)

        # Create another session
        sign_in_result = await derp.auth.sign_in_with_password(
            email="test@example.com",
            password="password123",
        )
        assert sign_in_result is not None

        # Sign out all
        await derp.auth.sign_out_all(result.user.id)

        # Both refresh tokens should fail
        refreshed1 = await derp.auth.refresh_token(result.tokens.refresh_token)
        assert refreshed1 is None

        refreshed2 = await derp.auth.refresh_token(sign_in_result.tokens.refresh_token)
        assert refreshed2 is None


class TestUserManagement:
    """Tests for user management."""

    async def test_get_user_by_id(
        self,
        derp: DerpClient,
        kv_client: ValkeyClient,
        mock_smtp: AsyncMock,
    ) -> None:
        """Test getting user by ID."""
        assert derp.config.auth is not None and derp.config.auth.native is not None
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None
        token = await get_confirmation_token(
            kv_client, derp.config.auth.native.cache_prefix, str(result.user.id)
        )
        assert token is not None
        await derp.auth.confirm_email(token)

        found = await derp.auth.get_user(result.user.id)

        assert found is not None
        assert found.email == "test@example.com"

    async def test_get_user_by_id_not_found(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """Test getting non-existent user by ID."""
        found = await derp.auth.get_user(uuid.uuid4())
        assert found is None

    async def test_update_user(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        """Test updating user."""
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None

        updated = await derp.auth.update_user(user_id=result.user.id, role="admin")

        assert updated is not None
        assert updated.role == "admin"

    async def test_update_user_not_found(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """Test updating non-existent user."""
        result = await derp.auth.update_user(user_id=uuid.uuid4())
        assert result is None


class TestRBAC:
    """Tests for role-based access control."""

    async def test_default_role_on_signup(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """Test that new users get the default role."""
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None

        assert result.user.role == "default"

    async def test_role_embedded_in_jwt(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """Test that role is embedded in the access token."""
        assert derp.config.auth is not None and derp.config.auth.native is not None
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None

        payload = decode_token(derp.config.auth.native.jwt, result.tokens.access_token)
        assert payload is not None
        assert payload.extra is not None
        assert payload.extra["role"] == "default"

    async def test_custom_role_in_jwt(
        self,
        derp: DerpClient,
        kv_client: ValkeyClient,
        mock_smtp: AsyncMock,
    ) -> None:
        """Test that updated role appears in JWT after re-login."""
        assert derp.config.auth is not None and derp.config.auth.native is not None
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None
        token = await get_confirmation_token(
            kv_client, derp.config.auth.native.cache_prefix, str(result.user.id)
        )
        assert token is not None
        await derp.auth.confirm_email(token)

        # Promote to admin
        await derp.auth.update_user(user_id=result.user.id, role="admin")

        # Re-login to get new JWT with updated role
        sign_in_result = await derp.auth.sign_in_with_password(
            email="test@example.com", password="password123"
        )
        assert sign_in_result is not None

        payload = decode_token(
            derp.config.auth.native.jwt, sign_in_result.tokens.access_token
        )
        assert payload is not None
        assert payload.extra is not None
        assert payload.extra["role"] == "admin"

    async def test_is_authorized_matching_role(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """Test is_authorized returns True for matching role."""
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None

        session = await derp.auth.authenticate(
            bearer_request(result.tokens.access_token)
        )
        assert session is not None
        assert derp.auth.is_authorized(session, "default") is True

    async def test_is_authorized_multiple_roles(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """Test is_authorized with multiple allowed roles."""
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None

        session = await derp.auth.authenticate(
            bearer_request(result.tokens.access_token)
        )
        assert session is not None
        assert derp.auth.is_authorized(session, "admin", "default") is True

    async def test_is_authorized_wrong_role(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """Test is_authorized returns False for non-matching role."""
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None

        session = await derp.auth.authenticate(
            bearer_request(result.tokens.access_token)
        )
        assert session is not None
        assert derp.auth.is_authorized(session, "admin") is False

    async def test_role_survives_token_refresh(
        self,
        derp: DerpClient,
        kv_client: ValkeyClient,
        mock_smtp: AsyncMock,
    ) -> None:
        """Test that refresh carries forward the session role (no DB hit)."""
        assert derp.config.auth is not None and derp.config.auth.native is not None
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None
        token = await get_confirmation_token(
            kv_client, derp.config.auth.native.cache_prefix, str(result.user.id)
        )
        assert token is not None
        await derp.auth.confirm_email(token)

        # Refresh token — role carried from session row, no user fetch
        new_tokens = await derp.auth.refresh_token(result.tokens.refresh_token)
        assert new_tokens is not None

        payload = decode_token(derp.config.auth.native.jwt, new_tokens.access_token)
        assert payload is not None
        assert payload.extra is not None
        assert payload.extra["role"] == "default"

    async def test_role_change_takes_effect_on_new_sign_in(
        self,
        derp: DerpClient,
        kv_client: ValkeyClient,
        mock_smtp: AsyncMock,
    ) -> None:
        """Test that a role change is picked up on next sign-in, not refresh."""
        assert derp.config.auth is not None and derp.config.auth.native is not None
        result = await derp.auth.sign_up(
            email="test@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None
        token = await get_confirmation_token(
            kv_client, derp.config.auth.native.cache_prefix, str(result.user.id)
        )
        assert token is not None
        await derp.auth.confirm_email(token)

        # Promote to admin
        await derp.auth.update_user(user_id=result.user.id, role="admin")

        # Refresh still carries old session role
        refreshed = await derp.auth.refresh_token(result.tokens.refresh_token)
        assert refreshed is not None
        payload = decode_token(derp.config.auth.native.jwt, refreshed.access_token)
        assert payload is not None
        assert payload.extra is not None
        assert payload.extra["role"] == "default"

        # New sign-in picks up the updated role
        sign_in_result = await derp.auth.sign_in_with_password(
            email="test@example.com",
            password="password123",
        )
        assert sign_in_result is not None
        payload = decode_token(
            derp.config.auth.native.jwt,
            sign_in_result.tokens.access_token,
        )
        assert payload is not None
        assert payload.extra is not None
        assert payload.extra["role"] == "admin"
