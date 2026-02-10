"""Core authentication service."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from derp.auth.email import EmailClient
from derp.auth.exceptions import (
    ConfirmationTokenExpiredError,
    ConfirmationTokenInvalidError,
    ConfirmationURLMissingError,
    EmailNotConfirmedError,
    InvalidCredentialsError,
    MagicLinkExpiredError,
    MagicLinkUsedError,
    PasswordValidationError,
    RecoveryTokenExpiredError,
    RecoveryTokenInvalidError,
    RefreshTokenReusedError,
    RefreshTokenRevokedError,
    SessionExpiredError,
    SessionNotFoundError,
    SignupDisabledError,
    UserAlreadyExistsError,
    UserNotActiveError,
    UserNotFoundError,
)
from derp.auth.jwt import TokenPair, create_token_pair, decode_token
from derp.auth.models import (
    AuthMagicLink,
    AuthProvider,
    AuthRefreshToken,
    AuthSession,
    BaseUser,
)
from derp.auth.password import (
    Argon2Hasher,
    PasswordHasher,
    generate_secure_token,
    validate_password,
)
from derp.auth.providers.base import BaseOAuthProvider
from derp.auth.providers.github import GitHubProvider
from derp.auth.providers.google import GoogleProvider
from derp.config import AuthConfig
from derp.orm import DatabaseEngine, Table
from derp.orm.loader import find_table_by_name


class AuthClient[UserT: BaseUser]:
    """Core authentication client handling all auth operations."""

    def __init__(self, config: AuthConfig, schema_path: str):
        tables: list[type[Table]] = []
        for base_class, enabled, name in [
            (BaseUser, True, "users"),
            (AuthSession, True, "auth_sessions"),
            (AuthRefreshToken, True, "auth_refresh_tokens"),
            (AuthMagicLink, config.enable_magic_link, "auth_magic_links"),
        ]:
            table = find_table_by_name(schema_path, name, base_class=base_class)
            if enabled and table is None:
                raise ValueError(
                    f"Expected table '{name}' in schema but it was not found under"
                    f" the specified schema path '{schema_path}'."
                )
            if enabled and table is not None and not issubclass(table, base_class):
                raise ValueError(
                    f"Table '{name}' must be a subclass of '{base_class.__name__}'"
                    f" but got instance of '{table}'. Make sure that you import"
                    " and implement `BaseUser`, `AuthSession`, `AuthRefreshToken`,"
                    " tables and optionally the`AuthMagicLink` table and include "
                    " them in one of the schema modules."
                )
            tables.append(table if table is not None else base_class)

        (
            user_table,
            auth_session_table,
            auth_refresh_token_table,
            auth_magic_link_table,
        ) = tables

        self._config: AuthConfig = config
        self._user_table: type[UserT] = user_table
        self._auth_session_table: type[AuthSession] = auth_session_table
        self._auth_refresh_token_table: type[AuthRefreshToken] = (
            auth_refresh_token_table
        )
        self._auth_magic_link_table: type[AuthMagicLink] = auth_magic_link_table
        self._hasher: PasswordHasher = Argon2Hasher()
        self._email_client: EmailClient | None = None
        self._oauth_providers: dict[AuthProvider, BaseOAuthProvider] = {}
        self._database_client: DatabaseEngine | None = None

        if self._config.google_oauth is not None:
            self._oauth_providers[AuthProvider.GOOGLE] = GoogleProvider(
                self._config.google_oauth
            )
        if self._config.github_oauth is not None:
            self._oauth_providers[AuthProvider.GITHUB] = GitHubProvider(
                self._config.github_oauth
            )

    def set_db(
        self, db: DatabaseEngine | None, replica_db: DatabaseEngine | None = None
    ) -> None:
        """Set the database client."""
        self._database_client = db
        self._replica_database_client = replica_db

    def _db(self) -> DatabaseEngine:
        """Get the database client."""
        if self._database_client is None:
            raise ValueError("Database client not set. Must call `set_db()` first.")
        return self._database_client

    def _maybe_replica_db(self) -> DatabaseEngine:
        """Get the replica database client."""
        if self._replica_database_client is not None:
            return self._replica_database_client
        return self._db()

    def set_email(self, email_client: EmailClient | None) -> None:
        """Set the email client."""
        self._email_client = email_client

    def _email(self) -> EmailClient:
        """Get the email client."""
        if self._email_client is None:
            raise ValueError("Email client not set. Must call `set_email()` first.")
        return self._email_client

    # =========================================================================
    # User Management
    # =========================================================================

    async def get_user(
        self,
        user_id: str | uuid.UUID | None = None,
        *,
        email: str | None = None,
        use_primary: bool = False,
    ) -> UserT | None:
        """Get a user by their ID or email address."""
        # Default to replica for fetching users to reduce load on primary.
        # This endpoint tends to be called more frequently than others.
        db = self._db() if use_primary else self._maybe_replica_db()

        if user_id is not None and email is not None:
            raise ValueError("Cannot get a user by both ID and email address.")
        elif user_id is not None:
            result = await (
                db.select(self._user_table)
                .where(self._user_table.c.id == str(user_id))
                .execute()
            )
        elif email is not None:
            result = await (
                db.select(self._user_table)
                .where(self._user_table.c.email == email.lower())
                .execute()
            )
        else:
            raise ValueError("Must provide either ID or email address.")

        return result[0] if result else None

    async def update_user(
        self,
        *,
        user_id: str | uuid.UUID,
        email: str | None = None,
        **kwargs: Any,
    ) -> UserT:
        """Update user data."""
        user = await self.get_user(user_id=user_id)
        if not user:
            raise UserNotFoundError()

        updates: dict[str, Any] = {"updated_at": datetime.now(UTC)}

        if email is not None:
            updates["email"] = email.lower()

        for key, value in kwargs.items():
            if key in self._user_table.c:
                updates[key] = value
            else:
                raise ValueError(f"Invalid user field: {key}.")

        [result] = await (
            self._db()
            .update(self._user_table)
            .set(**updates)
            .where(self._user_table.c.id == str(user_id))
            .returning(self._user_table)
            .execute()
        )
        return result

    # =========================================================================
    # Email/Password Authentication
    # =========================================================================

    async def sign_up(
        self,
        *,
        email: str,
        password: str,
        confirmation_url: str | None = None,
        confirmation_subject: str = "Confirm your email address",
        user_agent: str | None = None,
        ip_address: str | None = None,
        **kwargs: Any,
    ) -> tuple[UserT, TokenPair]:
        """Register a new user with email and password.

        Returns:
            Tuple of (user, token_pair)

        Raises:
            SignupDisabledError: If signup is disabled
            UserAlreadyExistsError: If user already exists
            PasswordValidationError: If password doesn't meet requirements
        """
        if not self._config.enable_signup:
            raise SignupDisabledError()
        if confirmation_url is None and self._config.enable_confirmation:
            raise ConfirmationURLMissingError(
                "`confirmation_url` is required when confirmation is enabled."
            )

        # Validate password
        validation = validate_password(self._config.password, password)
        if not validation.valid:
            raise PasswordValidationError("; ".join(validation.errors))

        # Check if user exists
        existing = await self.get_user(email=email.lower())
        if existing:
            raise UserAlreadyExistsError()

        # Create user
        hashed_password = self._hasher.hash(password)
        now = datetime.now(UTC)

        # Generate confirmation token if email confirmation is enabled
        confirmation_token = None
        confirmation_sent_at = None
        email_confirmed_at = None

        if self._config.enable_confirmation:
            confirmation_token = generate_secure_token()
            confirmation_sent_at = now
        else:
            # Auto-confirm email if confirmation not required
            email_confirmed_at = now

        vals: dict[str, Any] = {}
        for key, value in kwargs.items():
            if key in self._user_table.c:
                vals[key] = value
            else:
                raise ValueError(f"Invalid user field: {key}.")

        user = await (
            self._db()
            .insert(self._user_table)
            .values(
                email=email.lower(),
                encrypted_password=hashed_password,
                provider=AuthProvider.EMAIL,
                email_confirmed_at=email_confirmed_at,
                confirmation_token=confirmation_token,
                confirmation_sent_at=confirmation_sent_at,
                created_at=now,
                updated_at=now,
                last_sign_in_at=now,
                **vals,
            )
            .returning(self._user_table)
            .execute()
        )

        # Send confirmation email if needed
        if self._config.enable_confirmation and confirmation_token:
            await self._email().send_email(
                subject=confirmation_subject,
                to_email=email.lower(),
                template="confirmation.html",
                confirmation_url=f"{confirmation_url}?token={confirmation_token}",
            )

        # Create session and tokens
        token_pair = await self._create_session(
            user.id,
            user_agent=user_agent,
            ip_address=ip_address,
        )

        return user, token_pair

    async def sign_in_with_password(
        self,
        email: str,
        password: str,
        *,
        user_agent: str | None = None,
        ip_address: str | None = None,
    ) -> tuple[UserT, TokenPair]:
        """Sign in with email and password.

        Returns:
            Tuple of (user, token_pair)

        Raises:
            InvalidCredentialsError: If credentials are invalid
            UserNotActiveError: If user is disabled
            EmailNotConfirmedError: If email confirmation is required but not done
        """
        user = await self.get_user(email=email.lower())
        if not user:
            raise InvalidCredentialsError()

        if not user.encrypted_password:
            raise InvalidCredentialsError()

        if not self._hasher.verify(password, user.encrypted_password):
            raise InvalidCredentialsError()

        if not user.is_active:
            raise UserNotActiveError()

        if self._config.enable_confirmation and not user.email_confirmed_at:
            raise EmailNotConfirmedError()

        # Update last sign in
        now = datetime.now(UTC)
        await (
            self._db()
            .update(self._user_table)
            .set(last_sign_in_at=now, updated_at=now)
            .where(self._user_table.c.id == user.id)
            .execute()
        )

        # Rehash password if needed
        if self._hasher.needs_rehash(user.encrypted_password):
            new_hash = self._hasher.hash(password)
            await (
                self._db()
                .update(self._user_table)
                .set(encrypted_password=new_hash)
                .where(self._user_table.c.id == user.id)
                .execute()
            )

        token_pair = await self._create_session(
            user.id,
            user_agent=user_agent,
            ip_address=ip_address,
        )

        return user, token_pair

    # =========================================================================
    # Magic Link Authentication
    # =========================================================================

    async def sign_in_with_magic_link(self, *, email: str, magic_link_url: str) -> None:
        """Send a magic link email for passwordless sign in.

        Creates user if they don't exist (if signup enabled).

        Raises:
            SignupDisabledError: If user doesn't exist and signup is disabled
        """
        user = await self.get_user(email=email.lower())

        if not user:
            if not self._config.enable_signup:
                raise SignupDisabledError()

            # Create user for magic link
            now = datetime.now(UTC)
            user = await (
                self._db()
                .insert(self._user_table)
                .values(
                    email=email.lower(),
                    provider=AuthProvider.MAGIC_LINK,
                    email_confirmed_at=now,  # Magic link confirms email
                    created_at=now,
                    updated_at=now,
                )
                .returning(self._user_table)
                .execute()
            )

        # Create magic link
        token = generate_secure_token()
        expires_at = datetime.now(UTC) + timedelta(
            minutes=self._config.magic_link_expire_minutes
        )

        await (
            self._db()
            .insert(self._auth_magic_link_table)
            .values(
                email=email.lower(),
                token=token,
                expires_at=expires_at,
            )
            .execute()
        )

        # Send email
        await self._email().send_email(
            subject="Sign in to your account",
            to_email=email.lower(),
            template="magic_link.html",
            magic_link_url=f"{magic_link_url}?token={token}",
        )

    async def verify_magic_link(
        self,
        token: str,
        *,
        user_agent: str | None = None,
        ip_address: str | None = None,
    ) -> tuple[UserT, TokenPair]:
        """Verify a magic link and sign in.

        Returns:
            Tuple of (user, token_pair)

        Raises:
            MagicLinkUsedError: If the magic link was already used
            MagicLinkExpiredError: If the magic link has expired
            UserNotFoundError: If the user doesn't exist
            UserNotActiveError: If the user is disabled
        """
        # Find magic link
        result = await (
            self._db()
            .select(self._auth_magic_link_table)
            .where(self._auth_magic_link_table.c.token == token)
            .execute()
        )

        if not result:
            raise MagicLinkExpiredError("Invalid magic link")

        magic_link = result[0]

        if magic_link.used:
            raise MagicLinkUsedError()

        if magic_link.expires_at < datetime.now(UTC):
            raise MagicLinkExpiredError()

        # Mark as used
        await (
            self._db()
            .update(self._auth_magic_link_table)
            .set(used=True)
            .where(self._auth_magic_link_table.c.id == magic_link.id)
            .execute()
        )

        # Get user
        user = await self.get_user(email=magic_link.email)
        if not user:
            raise UserNotFoundError()

        if not user.is_active:
            raise UserNotActiveError()

        # Update user
        now = datetime.now(UTC)
        updates: dict[str, Any] = {
            "last_sign_in_at": now,
            "updated_at": now,
        }
        # Magic link confirms email
        if not user.email_confirmed_at:
            updates["email_confirmed_at"] = now

        await (
            self._db()
            .update(self._user_table)
            .set(**updates)
            .where(self._user_table.c.id == user.id)
            .execute()
        )

        token_pair = await self._create_session(
            user.id,
            user_agent=user_agent,
            ip_address=ip_address,
        )

        return user, token_pair

    # =========================================================================
    # OAuth Authentication
    # =========================================================================

    def get_oauth_provider(self, provider: str | AuthProvider) -> BaseOAuthProvider:
        if isinstance(provider, str):
            provider = AuthProvider(provider)
        oauth_provider = self._oauth_providers.get(provider)
        if oauth_provider is None:
            raise ValueError(f"OAuth provider not configured: {provider}")
        return oauth_provider

    def get_oauth_authorization_url(
        self,
        provider: str | AuthProvider,
        state: str,
        scopes: list[str] | None = None,
        redirect_uri: str | None = None,
    ) -> str:
        """Get the OAuth authorization URL for a provider.

        Args:
            provider_name: Name of the OAuth provider
            state: CSRF protection state token
            scopes: Optional scopes to request
            redirect_uri: Optional redirect URI override

        Returns:
            Authorization URL
        """
        oauth_provider = self.get_oauth_provider(provider)
        return oauth_provider.get_authorization_url(state, scopes, redirect_uri)

    async def sign_in_with_oauth(
        self,
        provider: str | AuthProvider,
        code: str,
        *,
        redirect_uri: str | None = None,
        user_agent: str | None = None,
        ip_address: str | None = None,
    ) -> tuple[UserT, TokenPair]:
        """Complete OAuth sign in with authorization code.

        Creates user if they don't exist.

        Returns:
            Tuple of (user, token_pair)

        Raises:
            ValueError: If provider is not registered
            OAuthError: If OAuth flow fails
            UserNotActiveError: If user is disabled
        """
        oauth_provider = self.get_oauth_provider(provider)

        # Get user info from provider
        user_info = await oauth_provider.authenticate(code, redirect_uri)

        # Find or create user
        user = await self.get_user(email=user_info.email)
        now = datetime.now(UTC)

        if user:
            # Update existing user
            if not user.is_active:
                raise UserNotActiveError()

            updates: dict[str, Any] = {
                "last_sign_in_at": now,
                "updated_at": now,
            }
            # Update provider info if first time with this provider
            if user.provider == AuthProvider.EMAIL:
                updates["provider"] = provider
                updates["provider_id"] = user_info.id

            # Confirm email if provider verified it
            if user_info.email_verified and not user.email_confirmed_at:
                updates["email_confirmed_at"] = now

            await (
                self._db()
                .update(self._user_table)
                .set(**updates)
                .where(self._user_table.c.id == user.id)
                .execute()
            )
        else:
            user = await (
                self._db()
                .insert(self._user_table)
                .values(
                    email=user_info.email.lower(),
                    provider=provider,
                    provider_id=user_info.id,
                    email_confirmed_at=now if user_info.email_verified else None,
                    created_at=now,
                    updated_at=now,
                    last_sign_in_at=now,
                )
                .returning(self._user_table)
                .execute()
            )

        token_pair = await self._create_session(
            user.id,
            user_agent=user_agent,
            ip_address=ip_address,
        )

        return user, token_pair

    # =========================================================================
    # Session Management
    # =========================================================================

    async def _create_session(
        self,
        user_id: uuid.UUID,
        *,
        user_agent: str | None = None,
        ip_address: str | None = None,
    ) -> TokenPair:
        """Create a new session and return tokens."""
        now = datetime.now(UTC)
        not_after = now + timedelta(days=self._config.session_expire_days)

        # Create session
        session = await (
            self._db()
            .insert(self._auth_session_table)
            .values(
                user_id=user_id,
                user_agent=user_agent,
                ip_address=ip_address,
                created_at=now,
                not_after=not_after,
            )
            .returning(self._auth_session_table)
            .execute()
        )

        # Create refresh token
        refresh_token = generate_secure_token()
        await (
            self._db()
            .insert(self._auth_refresh_token_table)
            .values(
                session_id=session.id,
                token=refresh_token,
                created_at=now,
            )
            .execute()
        )

        return create_token_pair(self._config.jwt, user_id, session.id, refresh_token)

    async def refresh_token(self, refresh_token: str) -> TokenPair:
        """Refresh an access token using a refresh token.

        Implements token rotation for security.

        Returns:
            New TokenPair

        Raises:
            RefreshTokenRevokedError: If token was revoked
            RefreshTokenReusedError: If token reuse detected (potential theft)
            SessionExpiredError: If session has expired
        """
        # Find refresh token
        result = await (
            self._db()
            .select(self._auth_refresh_token_table)
            .where(self._auth_refresh_token_table.c.token == refresh_token)
            .execute()
        )

        if not result:
            raise RefreshTokenRevokedError("Invalid refresh token")

        token_record = result[0]

        if token_record.revoked:
            # Token was already used - potential theft, revoke all tokens for session
            await (
                self._db()
                .update(self._auth_refresh_token_table)
                .set(revoked=True)
                .where(
                    self._auth_refresh_token_table.c.session_id
                    == token_record.session_id
                )
                .execute()
            )
            raise RefreshTokenReusedError()

        # Get session
        session_result = await (
            self._db()
            .select(self._auth_session_table)
            .where(self._auth_session_table.c.id == token_record.session_id)
            .execute()
        )

        if not session_result:
            raise SessionNotFoundError()

        session = session_result[0]

        if session.not_after < datetime.now(UTC):
            raise SessionExpiredError()

        # Revoke old token
        await (
            self._db()
            .update(self._auth_refresh_token_table)
            .set(revoked=True)
            .where(self._auth_refresh_token_table.c.id == token_record.id)
            .execute()
        )

        # Create new refresh token (rotation)
        new_refresh_token = generate_secure_token()
        await (
            self._db()
            .insert(self._auth_refresh_token_table)
            .values(
                session_id=session.id,
                token=new_refresh_token,
                parent=refresh_token,  # Track lineage for reuse detection
                created_at=datetime.now(UTC),
            )
            .execute()
        )

        return create_token_pair(
            self._config.jwt, session.user_id, session.id, new_refresh_token
        )

    async def validate_session(self, token: str) -> AuthSession | None:
        """Validate a session is active and not expired."""
        payload = decode_token(self._config.jwt, token)

        result = await (
            self._db()
            .select(self._auth_session_table)
            .where(self._auth_session_table.c.id == str(payload.session_id))
            .execute()
        )

        if not result:
            return None

        [session] = result

        if session.not_after < datetime.now(UTC):
            return None

        return session

    async def sign_out(self, session_id: str | uuid.UUID) -> None:
        """Sign out and revoke a session."""
        # Revoke all refresh tokens for this session
        await (
            self._db()
            .update(self._auth_refresh_token_table)
            .set(revoked=True)
            .where(self._auth_refresh_token_table.c.session_id == str(session_id))
            .execute()
        )

        # Delete session
        await (
            self._db()
            .delete(self._auth_session_table)
            .where(self._auth_session_table.c.id == str(session_id))
            .execute()
        )

    async def sign_out_all(self, user_id: str | uuid.UUID) -> None:
        """Sign out all sessions for a user."""
        # Get all session IDs
        sessions = await (
            self._db()
            .select(self._auth_session_table)
            .where(self._auth_session_table.c.user_id == str(user_id))
            .execute()
        )

        for session in sessions:
            await self.sign_out(session.id)

    # =========================================================================
    # Password Recovery
    # =========================================================================

    async def request_password_recovery(
        self,
        *,
        email: str,
        recovery_url: str,
        recovery_subject: str = "Reset your password",
        **kwargs: Any,
    ) -> None:
        """Send a password recovery email.

        Does not reveal whether user exists for security.
        """
        user = await self.get_user(email=email)
        if not user:
            return  # Don't reveal user doesn't exist

        if not user.is_active:
            return  # Don't reveal user is disabled

        # Generate recovery token
        token = generate_secure_token()
        now = datetime.now(UTC)

        await (
            self._db()
            .update(self._user_table)
            .set(recovery_token=token, recovery_sent_at=now, updated_at=now)
            .where(self._user_table.c.id == user.id)
            .execute()
        )

        await self._email().send_email(
            subject=recovery_subject,
            to_email=email.lower(),
            template="recovery.html",
            recovery_url=f"{recovery_url}?token={token}",
            **kwargs,
        )

    async def reset_password(self, token: str, new_password: str) -> UserT:
        """Reset password using recovery token.

        Returns:
            Updated user

        Raises:
            RecoveryTokenInvalidError: If token is invalid
            RecoveryTokenExpiredError: If token has expired
            PasswordValidationError: If new password doesn't meet requirements
        """
        # Validate password
        validation = validate_password(self._config.password, new_password)
        if not validation.valid:
            raise PasswordValidationError("; ".join(validation.errors))

        # Find user with this token
        result = await (
            self._db()
            .select(self._user_table)
            .where(self._user_table.c.recovery_token == token)
            .execute()
        )

        if not result:
            raise RecoveryTokenInvalidError()

        user = result[0]

        # Check expiry
        if user.recovery_sent_at:
            expires_at = user.recovery_sent_at + timedelta(
                minutes=self._config.recovery_token_expire_minutes
            )
            if datetime.now(UTC) > expires_at:
                raise RecoveryTokenExpiredError()

        # Update password and clear recovery token
        hashed_password = self._hasher.hash(new_password)
        now = datetime.now(UTC)

        [result] = await (
            self._db()
            .update(self._user_table)
            .set(
                encrypted_password=hashed_password,
                recovery_token=None,
                recovery_sent_at=None,
                updated_at=now,
            )
            .where(self._user_table.c.id == user.id)
            .returning(self._user_table)
            .execute()
        )

        # Sign out all sessions (security measure)
        await self.sign_out_all(user.id)

        return result

    # =========================================================================
    # Email Confirmation
    # =========================================================================

    async def confirm_email(self, token: str) -> UserT:
        """Confirm email address with token.

        Returns:
            Updated user

        Raises:
            ConfirmationTokenInvalidError: If token is invalid
            ConfirmationTokenExpiredError: If token has expired
        """
        # Find user with this token
        result = await (
            self._db()
            .select(self._user_table)
            .where(self._user_table.c.confirmation_token == token)
            .execute()
        )

        if not result:
            raise ConfirmationTokenInvalidError()

        user = result[0]

        # Check expiry
        if user.confirmation_sent_at:
            expires_at = user.confirmation_sent_at + timedelta(
                hours=self._config.confirmation_token_expire_hours
            )
            if datetime.now(UTC) > expires_at:
                raise ConfirmationTokenExpiredError()

        # Confirm email
        now = datetime.now(UTC)
        [result] = await (
            self._db()
            .update(self._user_table)
            .set(
                email_confirmed_at=now,
                confirmation_token=None,
                confirmation_sent_at=None,
                updated_at=now,
            )
            .where(self._user_table.c.id == user.id)
            .returning(self._user_table)
            .execute()
        )

        return result

    async def resend_confirmation_email(
        self,
        *,
        email: str,
        confirmation_url: str,
        confirmation_subject: str = "Confirm your email address",
        **kwargs: Any,
    ) -> None:
        """Resend email confirmation.

        Does not reveal whether user exists for security.
        """
        user = await self.get_user(email=email)
        if not user:
            return

        if user.email_confirmed_at:
            return  # Already confirmed

        # Generate new token
        token = generate_secure_token()
        now = datetime.now(UTC)

        await (
            self._db()
            .update(self._user_table)
            .set(
                confirmation_token=token,
                confirmation_sent_at=now,
                updated_at=now,
            )
            .where(self._user_table.c.id == user.id)
            .execute()
        )

        await self._email().send_email(
            subject=confirmation_subject,
            to_email=email.lower(),
            template="confirmation.html",
            confirmation_url=f"{confirmation_url}?token={token}",
            **kwargs,
        )
