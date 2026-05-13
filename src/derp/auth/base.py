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
    CursorResult,
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
    (native, Supabase, WorkOS). Core methods are abstract; optional
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
    async def get_user(self, user_id: str | uuid.UUID) -> UserInfo:
        """Get a user by their ID.

        Raises:
            UserNotFoundError: No user with that id.
        """

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
    ) -> UserInfo:
        """Update user data.

        Raises:
            UserNotFoundError: No user with that id.
        """

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
    ) -> list[SessionInfo]:
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
    ) -> AuthResult:
        """Register a new user with email and password.

        If *request* is provided, ``user_agent`` and ``ip_address`` are
        extracted automatically when not explicitly given.

        Raises:
            EmailAlreadyExistsError: An account with this email exists.
            PasswordValidationError: Password did not meet requirements.
            SignupDisabledError: Signup is turned off via config.
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
    ) -> AuthResult:
        """Sign in with email and password.

        Raises:
            InvalidCredentialsError: Email or password did not match.
        """
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
    ) -> AuthResult:
        """Verify a magic link and sign in.

        Raises:
            InvalidTokenError: Token is unknown, expired, or already used.
        """
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
    ) -> AuthResult:
        """Complete OAuth sign in with authorization code.

        Raises:
            InvalidCredentialsError: Provider rejected the code or user lookup failed.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Tokens (optional)
    # ------------------------------------------------------------------

    async def refresh_token(self, refresh_token: str) -> TokenPair:
        """Refresh an access token using a refresh token.

        Raises:
            InvalidTokenError: Refresh token is unknown, expired, or revoked.
        """
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

    async def reset_password(self, token: str, new_password: str) -> UserInfo:
        """Reset password using recovery token.

        Raises:
            InvalidTokenError: Recovery token is unknown, expired, or used.
            PasswordValidationError: New password did not meet requirements.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Email confirmation (optional)
    # ------------------------------------------------------------------

    async def confirm_email(self, token: str) -> UserInfo:
        """Confirm email address with token.

        Raises:
            InvalidTokenError: Confirmation token is unknown, expired, or used.
        """
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
    ) -> OrgInfo:
        """Create an organization. The creator is added as owner.

        Raises:
            OrgSlugConflictError: Slug is already taken.
        """
        raise NotImplementedError

    async def get_org(
        self,
        *,
        org_id: str | uuid.UUID | None = None,
        slug: str | None = None,
    ) -> OrgInfo:
        """Get an organization by ID or slug. Provide exactly one.

        Raises:
            OrgNotFoundError: No org matches the given id or slug.
        """
        raise NotImplementedError

    async def update_org(
        self,
        *,
        org_id: str | uuid.UUID | None = None,
        org_slug: str | None = None,
        name: str | None = None,
        slug: str | None = None,
        **kwargs: Any,
    ) -> OrgInfo:
        """Update an organization.

        Identify the org by ``org_id`` OR ``org_slug`` (exactly one). The
        ``slug`` kwarg sets the org's new slug — it does NOT identify the
        target. This is the one place ``slug=`` means a value rather than
        a lookup, since slug is updateable.

        Raises:
            OrgNotFoundError: No org matches the identifier.
            OrgSlugConflictError: New slug is already taken.
        """
        raise NotImplementedError

    async def delete_org(
        self,
        *,
        org_id: str | uuid.UUID | None = None,
        slug: str | None = None,
    ) -> bool:
        """Delete an organization and all its memberships.

        Identify by ``org_id`` or ``slug``. Returns ``False`` if not found.
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
        org_id: str | uuid.UUID | None = None,
        slug: str | None = None,
        user_id: str | uuid.UUID,
        role: str = "member",
    ) -> OrgMemberInfo:
        """Add a user to an organization (identify by ``org_id`` or ``slug``).

        Raises:
            OrgNotFoundError: Org identifier did not resolve.
            MemberAlreadyExistsError: User is already a member.
        """
        raise NotImplementedError

    async def update_org_member(
        self,
        *,
        org_id: str | uuid.UUID | None = None,
        slug: str | None = None,
        user_id: str | uuid.UUID,
        role: str,
    ) -> OrgMemberInfo:
        """Update a member's role.

        Raises:
            OrgNotFoundError: Org identifier did not resolve.
            OrgMemberNotFoundError: User is not a member of the org.
        """
        raise NotImplementedError

    async def remove_org_member(
        self,
        *,
        org_id: str | uuid.UUID | None = None,
        slug: str | None = None,
        user_id: str | uuid.UUID,
    ) -> bool:
        """Remove a user from an organization.

        Returns ``False`` if the org or membership does not exist.

        Raises:
            LastOwnerError: Removing this member would leave the org without
                an owner.
        """
        raise NotImplementedError

    async def list_org_members(
        self,
        *,
        org_id: str | uuid.UUID | None = None,
        slug: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[OrgMemberInfo]:
        """List members of an organization (identify by ``org_id`` or ``slug``).

        Raises:
            OrgNotFoundError: Org identifier did not resolve.
        """
        raise NotImplementedError

    async def get_org_member(
        self,
        *,
        org_id: str | uuid.UUID | None = None,
        slug: str | None = None,
        user_id: str | uuid.UUID,
    ) -> OrgMemberInfo:
        """Get a single membership record.

        Raises:
            OrgNotFoundError: Org identifier did not resolve.
            OrgMemberNotFoundError: User is not a member of the org.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Cursor-based pagination (optional)
    # ------------------------------------------------------------------

    async def list_users_by_cursor(
        self, *, limit: int = 10, after: str | None = None
    ) -> CursorResult[UserInfo]:
        """List users with cursor-based pagination."""
        raise NotImplementedError

    async def list_sessions_by_cursor(
        self,
        *,
        user_id: str | uuid.UUID,
        limit: int = 10,
        after: str | None = None,
    ) -> CursorResult[Any]:
        """List sessions with cursor-based pagination."""
        raise NotImplementedError

    async def list_orgs_by_cursor(
        self,
        *,
        user_id: str | uuid.UUID | None = None,
        limit: int = 10,
        after: str | None = None,
    ) -> CursorResult[OrgInfo]:
        """List organizations with cursor-based pagination."""
        raise NotImplementedError

    async def list_org_members_by_cursor(
        self,
        *,
        org_id: str | uuid.UUID | None = None,
        slug: str | None = None,
        limit: int = 10,
        after: str | None = None,
    ) -> CursorResult[OrgMemberInfo]:
        """List organization members with cursor-based pagination.

        Identify the org by ``org_id`` or ``slug``.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Organization session context (optional)
    # ------------------------------------------------------------------

    async def set_active_org(
        self,
        *,
        session_id: str | uuid.UUID,
        org_id: str | uuid.UUID | None,
    ) -> TokenPair:
        """Switch the active organization for a session.

        Pass ``org_id=None`` to clear the active org (sign out of org context).

        Raises:
            OrgMemberNotFoundError: User is not a member of the target org.
        """
        raise NotImplementedError

    def is_org_authorized(self, session: SessionInfo, org_id: str, *roles: str) -> bool:
        """Check if the session has an active org with an allowed role."""
        return session.org_id == org_id and session.org_role in roles

    def is_same_org(self, session: SessionInfo, org_id: str | uuid.UUID) -> bool:
        """True iff the session's active org matches ``org_id``.

        Tenant-scoping check with no role gating — use this in request
        handlers that do their own role logic, or just need to confirm
        the resource belongs to the caller's org.
        """
        return session.org_id is not None and session.org_id == str(org_id)

    def assert_same_org(self, session: SessionInfo, org_id: str | uuid.UUID) -> None:
        """Raise ``OrgMismatchError`` if the session's org does not match.

        The framework-recommended one-liner for tenant scoping at the top
        of an endpoint:

            session = await derp.auth.authenticate(request)
            derp.auth.assert_same_org(session, resource.org_id)

        Both sides hold the same id type (no translation step), so the
        comparison is correct by construction. Catch ``OrgMismatchError``
        and map it to whatever HTTP/RPC error your framework expects.
        """
        if not self.is_same_org(session, org_id):
            from derp.auth.exceptions import OrgMismatchError

            raise OrgMismatchError()
