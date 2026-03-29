"""Base interface for auth clients."""

from __future__ import annotations

import abc
import uuid
from typing import Any

from derp.auth.email import EmailClient
from derp.auth.jwt import TokenPair
from derp.auth.models import (
    AuthProvider,
    AuthRequest,
    AuthResult,
    OrgInfo,
    OrgMemberInfo,
    SessionInfo,
    UserInfo,
)
from derp.auth.providers import BaseOAuthProvider
from derp.kv.base import KVClient
from derp.orm import DatabaseEngine


class BaseAuthClient(abc.ABC):
    """Abstract base authentication client.

    Defines the full interface shared by all auth backends
    (native, Clerk, etc.). Core methods are abstract; optional
    methods raise ``NotImplementedError`` by default.
    """

    # ------------------------------------------------------------------
    # Infrastructure wiring (optional)
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Initialize backend-specific connections."""

    async def disconnect(self) -> None:
        """Close backend-specific connections."""

    def set_db(self, db: DatabaseEngine | None) -> None:
        """Set the database client."""

    def set_kv(self, kv: KVClient | None) -> None:
        """Set the KV store for caching and token storage."""

    def set_email(self, email_client: EmailClient | None) -> None:
        """Set the email client."""

    # ------------------------------------------------------------------
    # User management (abstract)
    # ------------------------------------------------------------------

    @abc.abstractmethod
    async def get_user(self, user_id: str | uuid.UUID) -> UserInfo | None:
        """Get a user by their ID."""

    @abc.abstractmethod
    async def list_users(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> list[UserInfo]:
        """List users."""

    @abc.abstractmethod
    async def update_user(
        self,
        *,
        user_id: str | uuid.UUID,
        email: str | None = None,
        **kwargs: Any,
    ) -> UserInfo | None:
        """Update user data. Returns ``None`` if the user is not found."""

    @abc.abstractmethod
    async def delete_user(self, user_id: str | uuid.UUID) -> bool:
        """Delete a user and all their sessions. Returns ``False`` if not found."""

    @abc.abstractmethod
    async def count_users(self) -> int:
        """Return the total number of users."""

    # ------------------------------------------------------------------
    # Sessions / tokens (abstract)
    # ------------------------------------------------------------------

    @abc.abstractmethod
    async def authenticate(self, request: AuthRequest) -> SessionInfo | None:
        """Authenticate a request and return session info."""

    @abc.abstractmethod
    async def list_sessions(
        self,
        *,
        user_id: str | uuid.UUID | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[Any]:
        """List active sessions, optionally filtered by user."""

    @abc.abstractmethod
    async def sign_out(self, session_id: str | uuid.UUID) -> None:
        """Sign out by revoking a session."""

    @abc.abstractmethod
    async def sign_out_all(self, user_id: str | uuid.UUID) -> None:
        """Sign out all sessions for a user."""

    # ------------------------------------------------------------------
    # Authorization
    # ------------------------------------------------------------------

    def is_authorized(self, session: SessionInfo, *roles: str) -> bool:
        """Check if the session's role is in the allowed set."""
        return session.role in roles

    # ------------------------------------------------------------------
    # Sign-up / sign-in (optional)
    # ------------------------------------------------------------------

    async def sign_up(
        self,
        *,
        email: str,
        password: str,
        request: AuthRequest | None = None,
        confirmation_url: str | None = None,
        confirmation_subject: str = "Confirm your email address",
        user_agent: str | None = None,
        ip_address: str | None = None,
        **kwargs: Any,
    ) -> AuthResult | None:
        """Register a new user with email and password.

        If *request* is provided, ``user_agent`` and ``ip_address`` are
        extracted automatically when not explicitly given.

        Returns ``None`` if password validation fails or the email is taken.
        """
        raise NotImplementedError

    async def sign_in_with_password(
        self,
        email: str,
        password: str,
        *,
        request: AuthRequest | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
        user_agent: str | None = None,
        ip_address: str | None = None,
    ) -> AuthResult | None:
        """Sign in with email and password."""
        raise NotImplementedError

    async def sign_in_with_magic_link(self, *, email: str, magic_link_url: str) -> None:
        """Send a magic link email for passwordless sign in."""
        raise NotImplementedError

    async def verify_magic_link(
        self,
        token: str,
        *,
        user_agent: str | None = None,
        ip_address: str | None = None,
    ) -> AuthResult | None:
        """Verify a magic link and sign in."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # OAuth (optional)
    # ------------------------------------------------------------------

    def get_oauth_provider(self, provider: str | AuthProvider) -> BaseOAuthProvider:
        """Get an OAuth provider by name."""
        raise NotImplementedError

    def get_oauth_authorization_url(
        self,
        provider: str | AuthProvider,
        state: str,
        scopes: list[str] | None = None,
        redirect_uri: str | None = None,
    ) -> str:
        """Get the OAuth authorization URL for a provider."""
        raise NotImplementedError

    async def sign_in_with_oauth(
        self,
        provider: str | AuthProvider,
        code: str,
        *,
        redirect_uri: str | None = None,
        user_agent: str | None = None,
        ip_address: str | None = None,
    ) -> AuthResult | None:
        """Complete OAuth sign in with authorization code."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Tokens (optional)
    # ------------------------------------------------------------------

    async def refresh_token(self, refresh_token: str) -> TokenPair | None:
        """Refresh an access token using a refresh token."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Password recovery (optional)
    # ------------------------------------------------------------------

    async def request_password_recovery(
        self,
        *,
        email: str,
        recovery_url: str,
        recovery_subject: str = "Reset your password",
        **kwargs: Any,
    ) -> None:
        """Send a password recovery email."""
        raise NotImplementedError

    async def reset_password(self, token: str, new_password: str) -> UserInfo | None:
        """Reset password using recovery token. Returns ``None`` for invalid tokens."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Email confirmation (optional)
    # ------------------------------------------------------------------

    async def confirm_email(self, token: str) -> UserInfo | None:
        """Confirm email address with token. Returns ``None`` for invalid tokens."""
        raise NotImplementedError

    async def resend_confirmation_email(
        self,
        *,
        email: str,
        confirmation_url: str,
        confirmation_subject: str = "Confirm your email address",
        **kwargs: Any,
    ) -> None:
        """Resend email confirmation."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Organizations (optional)
    # ------------------------------------------------------------------

    async def create_org(
        self, *, name: str, slug: str, creator_id: str | uuid.UUID, **kwargs: Any
    ) -> OrgInfo | None:
        """Create an organization. The creator is added as owner.

        Returns ``None`` if the slug is already taken.
        """
        raise NotImplementedError

    async def get_org(self, org_id: str | uuid.UUID) -> OrgInfo | None:
        """Get an organization by ID."""
        raise NotImplementedError

    async def get_org_by_slug(self, slug: str) -> OrgInfo | None:
        """Get an organization by slug."""
        raise NotImplementedError

    async def update_org(
        self,
        *,
        org_id: str | uuid.UUID,
        name: str | None = None,
        slug: str | None = None,
        **kwargs: Any,
    ) -> OrgInfo | None:
        """Update an organization. Returns ``None`` if not found."""
        raise NotImplementedError

    async def delete_org(self, org_id: str | uuid.UUID) -> bool:
        """Delete an organization and all its memberships.

        Returns ``False`` if not found.
        """
        raise NotImplementedError

    async def list_orgs(
        self,
        *,
        user_id: str | uuid.UUID | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[OrgInfo]:
        """List organizations, optionally filtered by user membership."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Organization membership (optional)
    # ------------------------------------------------------------------

    async def add_org_member(
        self,
        *,
        org_id: str | uuid.UUID,
        user_id: str | uuid.UUID,
        role: str = "member",
    ) -> OrgMemberInfo | None:
        """Add a user to an organization.

        Returns ``None`` if the user is already a member.
        """
        raise NotImplementedError

    async def update_org_member(
        self,
        *,
        org_id: str | uuid.UUID,
        user_id: str | uuid.UUID,
        role: str,
    ) -> OrgMemberInfo | None:
        """Update a member's role. Returns ``None`` if not found."""
        raise NotImplementedError

    async def remove_org_member(
        self,
        *,
        org_id: str | uuid.UUID,
        user_id: str | uuid.UUID,
    ) -> bool:
        """Remove a user from an organization.

        Returns ``False`` if not found or if the user is the last owner.
        """
        raise NotImplementedError

    async def list_org_members(
        self,
        org_id: str | uuid.UUID,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[OrgMemberInfo]:
        """List members of an organization."""
        raise NotImplementedError

    async def get_org_member(
        self,
        *,
        org_id: str | uuid.UUID,
        user_id: str | uuid.UUID,
    ) -> OrgMemberInfo | None:
        """Get a single membership record."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Organization session context (optional)
    # ------------------------------------------------------------------

    async def set_active_org(
        self,
        *,
        session_id: str | uuid.UUID,
        org_id: str | uuid.UUID | None,
    ) -> TokenPair | None:
        """Switch the active organization for a session.

        Returns new tokens, or ``None`` if the user is not a member.
        """
        raise NotImplementedError

    def is_org_authorized(self, session: SessionInfo, org_id: str, *roles: str) -> bool:
        """Check if the session has an active org with an allowed role."""
        return session.org_id == org_id and session.org_role in roles
