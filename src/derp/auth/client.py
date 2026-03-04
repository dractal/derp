"""Core authentication service."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from derp.auth.email import EmailClient
from derp.auth.exceptions import (
    ConfirmationTokenInvalidError,
    ConfirmationURLMissingError,
    EmailNotConfirmedError,
    InvalidCredentialsError,
    MagicLinkExpiredError,
    PasswordValidationError,
    RecoveryTokenInvalidError,
    RefreshTokenReusedError,
    RefreshTokenRevokedError,
    SessionExpiredError,
    SignupDisabledError,
    UserAlreadyExistsError,
    UserNotActiveError,
    UserNotFoundError,
)
from derp.auth.jwt import TokenPair, TokenPayload, create_token_pair, decode_token
from derp.auth.models import (
    AuthProvider,
    AuthSession,
    AuthUser,
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
from derp.kv.base import KVClient
from derp.orm import DatabaseEngine
from derp.orm.loader import discover_tables


class AuthClient[UserT: AuthUser]:
    """Core authentication client handling all auth operations."""

    def __init__(self, config: AuthConfig, schema_path: str):
        all_tables = discover_tables(schema_path, include_auth=True)

        user_table = next((t for t in all_tables if issubclass(t, AuthUser)), None)
        if user_table is None:
            raise ValueError(
                "No AuthUser table found. Make sure your schema includes "
                "AuthUser or a subclass of it."
            )

        auth_session_table = next(
            (t for t in all_tables if issubclass(t, AuthSession)), None
        )
        if auth_session_table is None:
            raise ValueError(
                "No AuthSession table found. Make sure your schema includes "
                "AuthSession or a subclass of it."
            )

        self._config: AuthConfig = config
        self._user_table: type[UserT] = user_table
        self._auth_session_table: type[AuthSession] = auth_session_table
        self._hasher: PasswordHasher = Argon2Hasher()
        self._email_client: EmailClient | None = None
        self._oauth_providers: dict[AuthProvider, BaseOAuthProvider] = {}
        self._database_client: DatabaseEngine | None = None
        self._kv_client: KVClient | None = None

        if self._config.google_oauth is not None:
            self._oauth_providers[AuthProvider.GOOGLE] = GoogleProvider(
                self._config.google_oauth
            )
        if self._config.github_oauth is not None:
            self._oauth_providers[AuthProvider.GITHUB] = GitHubProvider(
                self._config.github_oauth
            )

    def set_db(self, db: DatabaseEngine | None) -> None:
        """Set the database client."""
        self._database_client = db

    def _db(self) -> DatabaseEngine:
        """Get the database client."""
        if self._database_client is None:
            raise ValueError("Database client not set. Must call `set_db()` first.")
        return self._database_client

    def set_kv(self, kv: KVClient | None) -> None:
        """Set the KV store for caching and token storage."""
        self._kv_client = kv

    def _kv(self) -> KVClient:
        """Get the KV client. Required for token operations."""
        if self._kv_client is None:
            raise ValueError(
                "KV client not set. Token operations (recovery, confirmation, "
                "magic link) require a KV store. Call `set_kv()` first."
            )
        return self._kv_client

    def set_email(self, email_client: EmailClient | None) -> None:
        """Set the email client."""
        self._email_client = email_client

    def _email(self) -> EmailClient:
        """Get the email client."""
        if self._email_client is None:
            raise ValueError("Email client not set. Must call `set_email()` first.")
        return self._email_client

    async def _invalidate_user_cache(self, user_id: str | uuid.UUID) -> None:
        """Invalidate cached user data in KV store."""
        if self._kv_client is not None:
            await self._kv_client.delete(
                f"{self._config.cache_prefix}:user:{user_id}".encode()
            )

    # =========================================================================
    # User Management
    # =========================================================================

    async def get_user(
        self, user_id: str | uuid.UUID | None = None, *, email: str | None = None
    ) -> UserT | None:
        """Get a user by their ID or email address."""

        if user_id is not None and email is not None:
            raise ValueError("Cannot get a user by both ID and email address.")
        elif user_id is not None:
            if self._config.use_kv_cache and self._kv_client is not None:
                cache_key = f"{self._config.cache_prefix}:user:{user_id}".encode()

                async def _fetch_user() -> bytes:
                    row = await (
                        self._db()
                        .select(self._user_table)
                        .where(self._user_table.c.id == str(user_id))
                        .first_or_none()
                    )
                    if row is None:
                        return b""
                    return row.model_dump_json().encode()

                cached = await self._kv_client.guarded_get(
                    cache_key,
                    compute=_fetch_user,
                    ttl=self._config.cache_user_ttl_seconds,
                )
                if cached == b"":
                    return None
                return self._user_table.model_validate_json(cached)

            result = await (
                self._db()
                .select(self._user_table)
                .where(self._user_table.c.id == str(user_id))
                .first_or_none()
            )
        elif email is not None:
            result = await (
                self._db()
                .select(self._user_table)
                .where(self._user_table.c.email == email.lower())
                .first_or_none()
            )
        else:
            raise ValueError("Must provide either ID or email address.")

        return result

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

        await self._invalidate_user_cache(user_id)

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
        hashed_password = await self._hasher.async_hash(password)
        now = datetime.now(UTC)

        email_confirmed_at = None if self._config.enable_confirmation else now

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
                created_at=now,
                updated_at=now,
                last_sign_in_at=now,
                **vals,
            )
            .returning(self._user_table)
            .execute()
        )

        # Store confirmation token in KV and send email if needed
        if self._config.enable_confirmation:
            confirmation_token = generate_secure_token()
            ttl = self._config.confirmation_token_expire_hours * 3600
            await self._kv().set(
                f"{self._config.cache_prefix}:confirmation:{confirmation_token}".encode(),
                str(user.id).encode(),
                ttl=ttl,
            )

            await self._email().send_email(
                subject=confirmation_subject,
                to_email=email.lower(),
                template="confirmation.html",
                confirmation_url=f"{confirmation_url}?token={confirmation_token}",
            )

        # Create session and tokens
        token_pair = await self._create_session(
            user.id,
            role=user.role,
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

        if not await self._hasher.async_verify(password, user.encrypted_password):
            raise InvalidCredentialsError()

        if not user.is_active:
            raise UserNotActiveError()

        if self._config.enable_confirmation and not user.email_confirmed_at:
            raise EmailNotConfirmedError()

        # Update last sign in (and rehash password if needed) in a single write
        now = datetime.now(UTC)
        updates: dict[str, Any] = {"last_sign_in_at": now, "updated_at": now}

        if self._hasher.needs_rehash(user.encrypted_password):
            updates["encrypted_password"] = await self._hasher.async_hash(password)

        await (
            self._db()
            .update(self._user_table)
            .set(**updates)
            .where(self._user_table.c.id == user.id)
            .execute()
        )

        await self._invalidate_user_cache(user.id)

        token_pair = await self._create_session(
            user.id,
            role=user.role,
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
            ValueError: If magic link authentication is not enabled
        """
        if not self._config.enable_magic_link:
            raise ValueError("Magic link authentication is not enabled.")

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

        # Store magic link token in KV
        token = generate_secure_token()
        ttl = self._config.magic_link_expire_minutes * 60
        await self._kv().set(
            f"{self._config.cache_prefix}:magic_link:{token}".encode(),
            email.lower().encode(),
            ttl=ttl,
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
            MagicLinkExpiredError: If the magic link has expired or is invalid
            UserNotFoundError: If the user doesn't exist
            UserNotActiveError: If the user is disabled
        """
        kv_key = f"{self._config.cache_prefix}:magic_link:{token}".encode()
        email_bytes = await self._kv().get(kv_key)

        if email_bytes is None:
            raise MagicLinkExpiredError()

        # Delete on use (single use)
        await self._kv().delete(kv_key)

        email = email_bytes.decode()
        user = await self.get_user(email=email)
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

        await self._invalidate_user_cache(user.id)

        token_pair = await self._create_session(
            user.id,
            role=user.role,
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

            await self._invalidate_user_cache(user.id)
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
            role=user.role,
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
        role: str = "default",
        user_agent: str | None = None,
        ip_address: str | None = None,
    ) -> TokenPair:
        """Create a new session and return tokens."""
        now = datetime.now(UTC)
        not_after = now + timedelta(days=self._config.session_expire_days)
        refresh_token = generate_secure_token()

        session = await (
            self._db()
            .insert(self._auth_session_table)
            .values(
                user_id=user_id,
                token=refresh_token,
                role=role,
                user_agent=user_agent,
                ip_address=ip_address,
                not_after=not_after,
                created_at=now,
            )
            .returning(self._auth_session_table)
            .execute()
        )

        return create_token_pair(
            self._config.jwt,
            user_id,
            session.session_id,
            refresh_token,
            extra_claims={"role": role},
        )

    async def refresh_token(self, refresh_token: str) -> TokenPair:
        """Refresh an access token using a refresh token.

        Implements token rotation for security. Happy path is 2 DB calls:
        one UPDATE…RETURNING to atomically revoke the old token, one INSERT
        for the new token.

        Returns:
            New TokenPair

        Raises:
            RefreshTokenRevokedError: If token was revoked
            RefreshTokenReusedError: If token reuse detected (potential theft)
            SessionExpiredError: If session has expired
        """
        # Atomically revoke and return the token in one query.
        # If the token doesn't exist or is already revoked, this returns [].
        revoked_rows = await (
            self._db()
            .update(self._auth_session_table)
            .set(revoked=True)
            .eq(self._auth_session_table.c.token, refresh_token)
            .not_(self._auth_session_table.c.revoked)
            .returning(self._auth_session_table)
            .execute()
        )

        if not revoked_rows:
            # Token not found or already revoked — check which case.
            existing = await (
                self._db()
                .select(self._auth_session_table)
                .eq(self._auth_session_table.c.revoked, False)
                .first_or_none()
            )
            if existing is not None and existing.revoked:
                # Reuse detected — revoke all tokens for this session.
                await (
                    self._db()
                    .update(self._auth_session_table)
                    .set(revoked=True)
                    .eq(self._auth_session_table.c.session_id, existing.session_id)
                    .execute()
                )
                if self._kv_client is not None:
                    await self._kv_client.delete(
                        f"{self._config.cache_prefix}:session:{existing.session_id}".encode()
                    )
                raise RefreshTokenReusedError()
            raise RefreshTokenRevokedError("Invalid refresh token")

        [token_record] = revoked_rows

        if token_record.not_after < datetime.now(UTC):
            raise SessionExpiredError()

        # Insert rotated token
        new_refresh_token = generate_secure_token()
        await (
            self._db()
            .insert(self._auth_session_table)
            .values(
                user_id=token_record.user_id,
                session_id=token_record.session_id,
                token=new_refresh_token,
                role=token_record.role,
                user_agent=token_record.user_agent,
                ip_address=token_record.ip_address,
                not_after=token_record.not_after,
                created_at=datetime.now(UTC),
            )
            .execute()
        )

        # Invalidate stale session cache so next validate_session re-fetches
        if self._kv_client is not None:
            await self._kv_client.delete(
                f"{self._config.cache_prefix}:session:{token_record.session_id}".encode()
            )

        return create_token_pair(
            self._config.jwt,
            token_record.user_id,
            token_record.session_id,
            new_refresh_token,
            extra_claims={"role": token_record.role},
        )

    async def validate_session(self, token: str) -> AuthSession | None:
        """Validate a session is active and not expired."""
        payload = decode_token(self._config.jwt, token)
        if payload is None:
            return None
        session_id = str(payload.session_id)
        cache_key = f"{self._config.cache_prefix}:session:{session_id}".encode()

        if self._kv_client is not None and self._config.use_kv_cache:

            async def _fetch_session() -> bytes:
                row = await (
                    self._db()
                    .select(self._auth_session_table)
                    .where(
                        (self._auth_session_table.c.session_id == session_id)
                        & ~self._auth_session_table.c.revoked
                    )
                    .first_or_none()
                )
                if row is None:
                    return b""
                return row.model_dump_json().encode()

            remaining = self._config.cache_session_ttl_seconds
            cached = await self._kv_client.guarded_get(
                cache_key, compute=_fetch_session, ttl=remaining
            )
            if cached == b"":
                return None
            session = self._auth_session_table.model_validate_json(cached)
            if session.revoked or session.not_after < datetime.now(UTC):
                return None
            return session

        result = await (
            self._db()
            .select(self._auth_session_table)
            .where(
                (self._auth_session_table.c.session_id == session_id)
                & ~self._auth_session_table.c.revoked
            )
            .first_or_none()
        )

        if not result or result.not_after < datetime.now(UTC):
            return None

        return result

    async def sign_out(self, session_id: str | uuid.UUID) -> None:
        """Sign out by deleting all tokens for a session."""
        await (
            self._db()
            .delete(self._auth_session_table)
            .where(self._auth_session_table.c.session_id == str(session_id))
            .execute()
        )

        # Invalidate session cache
        if self._kv_client is not None:
            await self._kv_client.delete(
                f"{self._config.cache_prefix}:session:{session_id}".encode()
            )

    async def sign_out_all(self, user_id: str | uuid.UUID) -> None:
        """Sign out all sessions for a user by deleting all tokens."""
        sessions = await (
            self._db()
            .select(self._auth_session_table)
            .where(self._auth_session_table.c.user_id == str(user_id))
            .execute()
        )

        if not sessions:
            return

        session_ids = {str(s.session_id) for s in sessions}

        await (
            self._db()
            .delete(self._auth_session_table)
            .where(self._auth_session_table.c.user_id == str(user_id))
            .execute()
        )

        # Invalidate all session caches
        if self._kv_client is not None:
            cache_keys = [
                f"{self._config.cache_prefix}:session:{sid}".encode()
                for sid in session_ids
            ]
            await self._kv_client.delete_many(cache_keys)

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

        # Store recovery token in KV
        token = generate_secure_token()
        ttl = self._config.recovery_token_expire_minutes * 60
        await self._kv().set(
            f"{self._config.cache_prefix}:recovery:{token}".encode(),
            str(user.id).encode(),
            ttl=ttl,
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
            RecoveryTokenInvalidError: If token is invalid or expired
            PasswordValidationError: If new password doesn't meet requirements
        """
        # Validate password
        validation = validate_password(self._config.password, new_password)
        if not validation.valid:
            raise PasswordValidationError("; ".join(validation.errors))

        # Look up recovery token in KV
        kv_key = f"{self._config.cache_prefix}:recovery:{token}".encode()
        user_id_bytes = await self._kv().get(kv_key)

        if user_id_bytes is None:
            raise RecoveryTokenInvalidError()

        # Delete token (single use)
        await self._kv().delete(kv_key)

        user = await self.get_user(user_id=user_id_bytes.decode())
        if user is None:
            raise RecoveryTokenInvalidError()

        # Update password
        hashed_password = await self._hasher.async_hash(new_password)
        now = datetime.now(UTC)

        [result] = await (
            self._db()
            .update(self._user_table)
            .set(encrypted_password=hashed_password, updated_at=now)
            .where(self._user_table.c.id == user.id)
            .returning(self._user_table)
            .execute()
        )

        await self._invalidate_user_cache(user.id)

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
            ConfirmationTokenInvalidError: If token is invalid or expired
        """
        kv_key = f"{self._config.cache_prefix}:confirmation:{token}".encode()
        user_id_bytes = await self._kv().get(kv_key)

        if user_id_bytes is None:
            raise ConfirmationTokenInvalidError()

        # Delete token (single use)
        await self._kv().delete(kv_key)

        user = await self.get_user(user_id=user_id_bytes.decode())
        if user is None:
            raise ConfirmationTokenInvalidError()

        # Confirm email
        now = datetime.now(UTC)
        [result] = await (
            self._db()
            .update(self._user_table)
            .set(email_confirmed_at=now, updated_at=now)
            .where(self._user_table.c.id == user.id)
            .returning(self._user_table)
            .execute()
        )

        await self._invalidate_user_cache(user.id)

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

        # Store new confirmation token in KV
        token = generate_secure_token()
        ttl = self._config.confirmation_token_expire_hours * 3600
        await self._kv().set(
            f"{self._config.cache_prefix}:confirmation:{token}".encode(),
            str(user.id).encode(),
            ttl=ttl,
        )

        await self._email().send_email(
            subject=confirmation_subject,
            to_email=email.lower(),
            template="confirmation.html",
            confirmation_url=f"{confirmation_url}?token={token}",
            **kwargs,
        )

    # =========================================================================
    # Role-Based Access Control
    # =========================================================================

    def is_authorized(self, payload: TokenPayload, *roles: str) -> bool:
        """Check if the token's role is in the allowed set.

        Reads the ``role`` claim embedded in the JWT at session creation.
        No database hit required.

        Args:
            payload: Decoded JWT token payload.
            roles: One or more role strings that are permitted.

        Returns:
            True if the token's role matches one of the allowed roles,
            False otherwise.
        """
        role = (payload.extra or {}).get("role")
        return role in roles
