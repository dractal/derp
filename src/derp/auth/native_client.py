"""Core authentication service."""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from derp.auth.base import BaseAuthClient
from derp.auth.email import EmailClient
from derp.auth.exceptions import (
    ConfirmationURLMissingError,
    PasswordValidationError,
    SignupDisabledError,
)
from derp.auth.jwt import TokenPair, create_token_pair, decode_token
from derp.auth.models import (
    AuthOrganization,
    AuthOrgMember,
    AuthProvider,
    AuthRequest,
    AuthResult,
    AuthSession,
    AuthUser,
    OrgInfo,
    OrgMemberInfo,
    SessionInfo,
    UserInfo,
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
from derp.config import NativeAuthConfig
from derp.kv.base import KVClient
from derp.orm import DatabaseEngine

logger = logging.getLogger(__name__)


class NativeAuthClient(BaseAuthClient):
    """Native authentication client (email/password, magic link, OAuth)."""

    def __init__(self, config: NativeAuthConfig):
        self._config: NativeAuthConfig = config
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

    @staticmethod
    def _extract_client_info(
        request: AuthRequest | None,
        user_agent: str | None,
        ip_address: str | None,
    ) -> tuple[str | None, str | None]:
        """Extract user_agent and ip_address from *request* when not given."""
        if request is None:
            return user_agent, ip_address
        if user_agent is None:
            user_agent = request.headers.get("User-Agent")
        if ip_address is None:
            forwarded = request.headers.get("X-Forwarded-For")
            if forwarded:
                ip_address = forwarded.split(",")[0].strip()
            else:
                client = getattr(request, "client", None)
                if client is not None:
                    ip_address = getattr(client, "host", None)
        return user_agent, ip_address

    async def _invalidate_user_cache(
        self, user_id: str | uuid.UUID, email: str | None = None
    ) -> None:
        """Invalidate cached user data in KV store."""
        if self._kv_client is not None:
            await self._kv_client.delete(
                f"{self._config.cache_prefix}:user:{user_id}".encode()
            )
            if email is not None:
                await self._kv_client.delete(
                    f"{self._config.cache_prefix}:user:email:{email.lower()}".encode()
                )

    # =========================================================================
    # User Management
    # =========================================================================

    def _to_user_info(self, user: AuthUser) -> UserInfo:
        """Convert an internal AuthUser ORM model to a public UserInfo."""
        return UserInfo(
            id=str(user.id),
            email=user.email,
            first_name=user.first_name,
            last_name=user.last_name,
            username=user.username,
            image_url=user.image_url,
            role=user.role,
            is_active=user.is_active,
            is_superuser=user.is_superuser,
            created_at=user.created_at,
            updated_at=user.updated_at,
            last_sign_in_at=user.last_sign_in_at,
            email_confirmed_at=user.email_confirmed_at,
            metadata={
                "provider": user.provider.value
                if hasattr(user.provider, "value")
                else user.provider,
                "provider_id": user.provider_id,
            },
        )

    async def _fetch_user(self, user_id: str | uuid.UUID) -> AuthUser | None:
        """Fetch a user by ID (internal, with caching)."""
        if self._config.use_kv_cache and self._kv_client is not None:
            cache_key = f"{self._config.cache_prefix}:user:{user_id}".encode()

            async def _compute() -> bytes:
                row = await (
                    self._db()
                    .select(AuthUser)
                    .where(AuthUser.id == str(user_id))
                    .first_or_none()
                )
                if row is None:
                    return b""
                return row.to_json().encode()

            cached = await self._kv_client.guarded_get(
                cache_key,
                compute=_compute,
                ttl=self._config.cache_user_ttl_seconds,
            )
            if cached == b"":
                return None
            return AuthUser.from_json(cached)

        return await (
            self._db()
            .select(AuthUser)
            .where(AuthUser.id == str(user_id))
            .first_or_none()
        )

    async def get_user(self, user_id: str | uuid.UUID) -> UserInfo | None:
        """Get a user by their ID."""
        user = await self._fetch_user(user_id)
        return self._to_user_info(user) if user is not None else None

    async def list_users(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> list[UserInfo]:
        """List users ordered by creation date (newest first)."""
        q = self._db().select(AuthUser).order_by(AuthUser.created_at, asc=False)
        if limit is not None:
            q = q.limit(limit)
        if offset is not None:
            q = q.offset(offset)
        return [self._to_user_info(u) for u in await q.execute()]

    async def _get_user_by_email(self, email: str) -> AuthUser | None:
        """Get a user by their email address (internal use only).

        Unlike ``get_user``, negative results (user not found) are **not**
        cached because email lookups are used in write paths (sign-up, OAuth)
        where a subsequent insert would leave a stale "not found" entry.
        """
        normalized = email.lower()

        if self._config.use_kv_cache and self._kv_client is not None:
            cache_key = f"{self._config.cache_prefix}:user:email:{normalized}".encode()
            cached = await self._kv_client.get(cache_key)
            if cached is not None:
                return AuthUser.from_json(cached)

            user = await (
                self._db()
                .select(AuthUser)
                .where(AuthUser.email == normalized)
                .first_or_none()
            )
            if user is not None:
                await self._kv_client.set(
                    cache_key,
                    user.to_json().encode(),
                    ttl=self._config.cache_user_ttl_seconds,
                )
            return user

        return await (
            self._db()
            .select(AuthUser)
            .where(AuthUser.email == normalized)
            .first_or_none()
        )

    async def update_user(
        self,
        *,
        user_id: str | uuid.UUID,
        email: str | None = None,
        **kwargs: Any,
    ) -> UserInfo | None:
        """Update user data."""
        user = await self._fetch_user(user_id=user_id)
        if not user:
            return None

        updates: dict[str, Any] = {"updated_at": datetime.now(UTC)}

        if email is not None:
            updates["email"] = email.lower()

        for key, value in kwargs.items():
            if key in AuthUser.get_columns():
                updates[key] = value
            else:
                raise ValueError(f"Invalid user field: {key}.")

        [result] = await (
            self._db()
            .update(AuthUser)
            .set(**updates)
            .where(AuthUser.id == str(user_id))
            .returning(AuthUser)
            .execute()
        )

        await self._invalidate_user_cache(user_id, user.email)
        if email is not None and email.lower() != user.email:
            await self._invalidate_user_cache(user_id, email)

        return self._to_user_info(result)

    async def delete_user(self, user_id: str | uuid.UUID) -> bool:
        """Delete a user and all their sessions."""
        row = await (
            self._db()
            .select(AuthUser.id, AuthUser.email)
            .from_(AuthUser)
            .where(AuthUser.id == str(user_id))
            .first_or_none()
        )
        if not row:
            return False

        _, email = row
        await self.sign_out_all(user_id)

        await self._db().delete(AuthUser).where(AuthUser.id == str(user_id)).execute()

        await self._invalidate_user_cache(user_id, email)
        return True

    async def count_users(self) -> int:
        """Return the total number of users."""
        return await self._db().select(AuthUser).count()

    # =========================================================================
    # Email/Password Authentication
    # =========================================================================

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

        Returns:
            Tuple of (user, token_pair), or ``None`` if the password
            fails validation or the email is already taken.

        Raises:
            SignupDisabledError: If signup is disabled
        """
        user_agent, ip_address = self._extract_client_info(
            request, user_agent, ip_address
        )
        if not self._config.enable_signup:
            raise SignupDisabledError()
        if confirmation_url is None and self._config.enable_confirmation:
            raise ConfirmationURLMissingError(
                "`confirmation_url` is required when confirmation is enabled."
            )

        # Validate password
        validation = validate_password(self._config.password, password)
        if not validation.valid:
            return None

        # Check if user exists
        exists = await (
            self._db()
            .select(AuthUser.id)
            .from_(AuthUser)
            .where(AuthUser.email == email.lower())
            .first_or_none()
        )
        if exists:
            return None

        # Create user
        hashed_password = await self._hasher.async_hash(password)
        now = datetime.now(UTC)

        email_confirmed_at = None if self._config.enable_confirmation else now

        vals: dict[str, Any] = {}
        for key, value in kwargs.items():
            if key in AuthUser.get_columns():
                vals[key] = value
            else:
                raise ValueError(f"Invalid user field: {key}.")

        user = await (
            self._db()
            .insert(AuthUser)
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
            .returning(AuthUser)
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

        return AuthResult(user=self._to_user_info(user), tokens=token_pair)

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
        """Sign in with email and password.

        Returns ``None`` if credentials are invalid or the account cannot sign in.
        """
        user_agent, ip_address = self._extract_client_info(
            request, user_agent, ip_address
        )
        user = await self._get_user_by_email(email=email.lower())
        if not user:
            logger.warning("Sign-in failed: user not found for %s", email)
            return None

        if not user.encrypted_password:
            logger.warning("Sign-in failed: no password set for %s", email)
            return None

        if not await self._hasher.async_verify(password, user.encrypted_password):
            logger.warning("Sign-in failed: invalid password for %s", email)
            return None

        if not user.is_active:
            logger.warning("Sign-in failed: account disabled for %s", email)
            return None

        if self._config.enable_confirmation and not user.email_confirmed_at:
            logger.warning("Sign-in failed: email not confirmed for %s", email)
            return None

        # Update last sign in (and rehash password if needed) in a single write
        now = datetime.now(UTC)
        updates: dict[str, Any] = {"last_sign_in_at": now, "updated_at": now}

        if self._hasher.needs_rehash(user.encrypted_password):
            updates["encrypted_password"] = await self._hasher.async_hash(password)

        [user] = await (
            self._db()
            .update(AuthUser)
            .set(**updates)
            .where(AuthUser.id == user.id)
            .returning(AuthUser)
            .execute()
        )

        await self._invalidate_user_cache(user.id, user.email)

        token_pair = await self._create_session(
            user.id,
            role=user.role,
            user_agent=user_agent,
            ip_address=ip_address,
        )

        return AuthResult(user=self._to_user_info(user), tokens=token_pair)

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

        user = await self._get_user_by_email(email=email.lower())

        if user:
            user_id = user.id
        else:
            if not self._config.enable_signup:
                raise SignupDisabledError()

            # Create user for magic link
            now = datetime.now(UTC)
            user_id = await (
                self._db()
                .insert(AuthUser)
                .values(
                    email=email.lower(),
                    provider=AuthProvider.MAGIC_LINK,
                    email_confirmed_at=now,  # Magic link confirms email
                    created_at=now,
                    updated_at=now,
                )
                .returning(AuthUser.id)
                .execute()
            )

        # Store magic link token in KV (keyed by user ID)
        token = generate_secure_token()
        ttl = self._config.magic_link_expire_minutes * 60
        await self._kv().set(
            f"{self._config.cache_prefix}:magic_link:{token}".encode(),
            str(user_id).encode(),
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
    ) -> AuthResult | None:
        """Verify a magic link and sign in.

        Returns ``None`` if the link is expired, invalid, or the account cannot sign in.
        """
        kv_key = f"{self._config.cache_prefix}:magic_link:{token}".encode()
        user_id_bytes = await self._kv().get(kv_key)

        if user_id_bytes is None:
            logger.warning("Magic link verification failed: token expired or invalid")
            return None

        # Delete on use (single use)
        await self._kv().delete(kv_key)

        user = await self._fetch_user(user_id_bytes.decode())
        if not user:
            logger.warning("Magic link verification failed: user not found")
            return None

        if not user.is_active:
            logger.warning(
                "Magic link verification failed: account disabled for %s", user.email
            )
            return None

        # Update user
        now = datetime.now(UTC)
        updates: dict[str, Any] = {
            "last_sign_in_at": now,
            "updated_at": now,
        }
        # Magic link confirms email
        if not user.email_confirmed_at:
            updates["email_confirmed_at"] = now

        [user] = await (
            self._db()
            .update(AuthUser)
            .set(**updates)
            .where(AuthUser.id == user.id)
            .returning(AuthUser)
            .execute()
        )

        await self._invalidate_user_cache(user.id, user.email)

        token_pair = await self._create_session(
            user.id,
            role=user.role,
            user_agent=user_agent,
            ip_address=ip_address,
        )

        return AuthResult(user=self._to_user_info(user), tokens=token_pair)

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
    ) -> AuthResult | None:
        """Complete OAuth sign in with authorization code.

        Creates user if they don't exist.
        Returns ``None`` if the OAuth flow fails or the account is disabled.
        """
        oauth_provider = self.get_oauth_provider(provider)

        # Get user info from provider
        user_info = await oauth_provider.authenticate(code, redirect_uri)
        if user_info is None:
            return None

        # Find or create user
        user = await self._get_user_by_email(email=user_info.email)
        now = datetime.now(UTC)

        if user:
            # Update existing user
            if not user.is_active:
                logger.warning(
                    "OAuth sign-in failed: account disabled for %s",
                    user.email,
                )
                return None

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

            [user] = await (
                self._db()
                .update(AuthUser)
                .set(**updates)
                .where(AuthUser.id == user.id)
                .returning(AuthUser)
                .execute()
            )

            await self._invalidate_user_cache(user.id, user.email)
        else:
            user = await (
                self._db()
                .insert(AuthUser)
                .values(
                    email=user_info.email.lower(),
                    provider=provider,
                    provider_id=user_info.id,
                    email_confirmed_at=now if user_info.email_verified else None,
                    created_at=now,
                    updated_at=now,
                    last_sign_in_at=now,
                )
                .returning(AuthUser)
                .execute()
            )

        token_pair = await self._create_session(
            user.id,
            role=user.role,
            user_agent=user_agent,
            ip_address=ip_address,
        )

        return AuthResult(user=self._to_user_info(user), tokens=token_pair)

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

        session_id = await (
            self._db()
            .insert(AuthSession)
            .values(
                user_id=user_id,
                token=refresh_token,
                role=role,
                user_agent=user_agent,
                ip_address=ip_address,
                not_after=not_after,
                created_at=now,
            )
            .returning(AuthSession.session_id)
            .execute()
        )

        return create_token_pair(
            self._config.jwt,
            user_id,
            session_id,
            refresh_token,
            extra_claims={"role": role},
        )

    async def refresh_token(self, refresh_token: str) -> TokenPair | None:
        """Refresh an access token using a refresh token.

        Implements token rotation for security. Happy path is 2 DB calls:
        one UPDATE…RETURNING to atomically revoke the old token, one INSERT
        for the new token.

        Returns ``None`` if the token is invalid, revoked, reused, or expired.
        """
        # Atomically revoke and return the token in one query.
        # If the token doesn't exist or is already revoked, this returns [].
        revoked_rows = await (
            self._db()
            .update(AuthSession)
            .set(revoked=True)
            .eq(AuthSession.token, refresh_token)
            .not_(AuthSession.revoked)
            .returning(AuthSession)
            .execute()
        )

        if not revoked_rows:
            # Token not found or already revoked — check which case.
            existing = await (
                self._db()
                .select(AuthSession)
                .eq(AuthSession.token, refresh_token)
                .first_or_none()
            )
            if existing is not None and existing.revoked:
                # Reuse detected — revoke all tokens for this session.
                await (
                    self._db()
                    .update(AuthSession)
                    .set(revoked=True)
                    .eq(AuthSession.session_id, existing.session_id)
                    .execute()
                )
                if self._kv_client is not None:
                    await self._kv_client.delete(
                        f"{self._config.cache_prefix}:session:{existing.session_id}".encode()
                    )
                logger.warning("Refresh token reuse detected, all sessions revoked")
                return None
            logger.warning("Refresh token invalid or revoked")
            return None

        [token_record] = revoked_rows

        if token_record.not_after < datetime.now(UTC):
            logger.warning("Refresh token failed: session expired")
            return None

        # Insert rotated token
        new_refresh_token = generate_secure_token()
        await (
            self._db()
            .insert(AuthSession)
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

        # Invalidate stale session cache so next authenticate re-fetches
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

    async def authenticate(self, request: AuthRequest) -> SessionInfo | None:
        """Authenticate a request via JWT (networkless).

        Extracts the Bearer token from the Authorization header,
        decodes and verifies the JWT signature and expiry.
        Returns ``SessionInfo`` built from JWT claims, or ``None``
        if the token is missing, invalid, or expired.
        """
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return None
        token = auth_header.removeprefix("Bearer ")
        payload = decode_token(self._config.jwt, token)
        if payload is None:
            return None
        extra = payload.extra or {}
        return SessionInfo(
            user_id=payload.sub,
            session_id=payload.session_id,
            role=extra.get("role", "default"),
            expires_at=payload.exp,
            metadata=extra,
            org_id=extra.get("org_id"),
            org_role=extra.get("org_role"),
        )

    async def list_sessions(
        self,
        *,
        user_id: str | uuid.UUID | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[AuthSession]:
        """List active (non-revoked) sessions ordered by creation date."""
        q = (
            self._db()
            .select(AuthSession)
            .where(~AuthSession.revoked)
            .order_by(AuthSession.created_at, asc=False)
        )
        if user_id is not None:
            q = q.where(AuthSession.user_id == str(user_id))
        if limit is not None:
            q = q.limit(limit)
        if offset is not None:
            q = q.offset(offset)
        return await q.execute()

    async def sign_out(self, session_id: str | uuid.UUID) -> None:
        """Sign out by deleting all tokens for a session."""
        await (
            self._db()
            .delete(AuthSession)
            .where(AuthSession.session_id == str(session_id))
            .execute()
        )

        # Invalidate session cache
        if self._kv_client is not None:
            await self._kv_client.delete(
                f"{self._config.cache_prefix}:session:{session_id}".encode()
            )

    async def sign_out_all(self, user_id: str | uuid.UUID) -> None:
        """Sign out all sessions for a user by deleting all tokens."""
        session_ids = await (
            self._db()
            .delete(AuthSession)
            .where(AuthSession.user_id == str(user_id))
            .returning(AuthSession.session_id)
            .execute()
        )

        # Invalidate all session caches
        if session_ids and self._kv_client is not None:
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
        row = await (
            self._db()
            .select(AuthUser.id, AuthUser.is_active)
            .from_(AuthUser)
            .where(AuthUser.email == email.lower())
            .first_or_none()
        )
        if not row:
            return  # Don't reveal user doesn't exist

        uid, is_active = row
        if not is_active:
            return  # Don't reveal user is disabled

        # Store recovery token in KV
        token = generate_secure_token()
        ttl = self._config.recovery_token_expire_minutes * 60
        await self._kv().set(
            f"{self._config.cache_prefix}:recovery:{token}".encode(),
            str(uid).encode(),
            ttl=ttl,
        )

        await self._email().send_email(
            subject=recovery_subject,
            to_email=email.lower(),
            template="recovery.html",
            recovery_url=f"{recovery_url}?token={token}",
            **kwargs,
        )

    async def reset_password(self, token: str, new_password: str) -> UserInfo | None:
        """Reset password using recovery token.

        Returns:
            Updated user, or ``None`` if the token is invalid or expired.

        Raises:
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
            return None

        # Delete token (single use)
        await self._kv().delete(kv_key)

        user = await self._fetch_user(user_id=user_id_bytes.decode())
        if user is None:
            return None

        # Update password
        hashed_password = await self._hasher.async_hash(new_password)
        now = datetime.now(UTC)

        [result] = await (
            self._db()
            .update(AuthUser)
            .set(encrypted_password=hashed_password, updated_at=now)
            .where(AuthUser.id == user.id)
            .returning(AuthUser)
            .execute()
        )

        await self._invalidate_user_cache(user.id, user.email)

        # Sign out all sessions (security measure)
        await self.sign_out_all(user.id)

        return self._to_user_info(result)

    # =========================================================================
    # Email Confirmation
    # =========================================================================

    async def confirm_email(self, token: str) -> UserInfo | None:
        """Confirm email address with token.

        Returns:
            Updated user, or ``None`` if the token is invalid or expired.
        """
        kv_key = f"{self._config.cache_prefix}:confirmation:{token}".encode()
        user_id_bytes = await self._kv().get(kv_key)

        if user_id_bytes is None:
            return None

        # Delete token (single use)
        await self._kv().delete(kv_key)

        user = await self._fetch_user(user_id=user_id_bytes.decode())
        if user is None:
            return None

        # Confirm email
        now = datetime.now(UTC)
        [result] = await (
            self._db()
            .update(AuthUser)
            .set(email_confirmed_at=now, updated_at=now)
            .where(AuthUser.id == user.id)
            .returning(AuthUser)
            .execute()
        )

        await self._invalidate_user_cache(user.id, user.email)

        return self._to_user_info(result)

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
        row = await (
            self._db()
            .select(AuthUser.id, AuthUser.email_confirmed_at)
            .from_(AuthUser)
            .where(AuthUser.email == email.lower())
            .first_or_none()
        )
        if not row:
            return

        uid, confirmed_at = row
        if confirmed_at:
            return  # Already confirmed

        # Store new confirmation token in KV
        token = generate_secure_token()
        ttl = self._config.confirmation_token_expire_hours * 3600
        await self._kv().set(
            f"{self._config.cache_prefix}:confirmation:{token}".encode(),
            str(uid).encode(),
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
    # Organizations
    # =========================================================================

    def _to_org_info(self, org: AuthOrganization) -> OrgInfo:
        """Convert an AuthOrganization ORM model to a public OrgInfo."""
        return OrgInfo(
            id=str(org.id),
            name=org.name,
            slug=org.slug,
            metadata=org.metadata or {},
            created_at=org.created_at,
            updated_at=org.updated_at,
        )

    def _to_org_member_info(self, member: AuthOrgMember) -> OrgMemberInfo:
        """Convert an AuthOrgMember ORM model to a public OrgMemberInfo."""
        return OrgMemberInfo(
            org_id=str(member.org_id),
            user_id=str(member.user_id),
            role=member.role,
            created_at=member.created_at,
            updated_at=member.updated_at,
        )

    async def create_org(
        self,
        *,
        name: str,
        slug: str,
        creator_id: str | uuid.UUID,
        **kwargs: Any,
    ) -> OrgInfo | None:
        """Create an organization. The creator is added as owner."""
        now = datetime.now(UTC)
        org = await (
            self._db()
            .insert(AuthOrganization)
            .values(name=name, slug=slug, created_at=now, updated_at=now)
            .ignore_conflicts(target=AuthOrganization.slug)
            .returning(AuthOrganization)
            .execute()
        )
        if org is None:
            return None

        # Add creator as owner
        await (
            self._db()
            .insert(AuthOrgMember)
            .values(
                org_id=org.id,
                user_id=str(creator_id),
                role="owner",
                created_at=now,
                updated_at=now,
            )
            .execute()
        )

        return self._to_org_info(org)

    async def get_org(self, org_id: str | uuid.UUID) -> OrgInfo | None:
        """Get an organization by ID."""
        org = await (
            self._db()
            .select(AuthOrganization)
            .where(AuthOrganization.id == str(org_id))
            .first_or_none()
        )
        return self._to_org_info(org) if org is not None else None

    async def get_org_by_slug(self, slug: str) -> OrgInfo | None:
        """Get an organization by slug."""
        org = await (
            self._db()
            .select(AuthOrganization)
            .where(AuthOrganization.slug == slug)
            .first_or_none()
        )
        return self._to_org_info(org) if org is not None else None

    async def update_org(
        self,
        *,
        org_id: str | uuid.UUID,
        name: str | None = None,
        slug: str | None = None,
        **kwargs: Any,
    ) -> OrgInfo | None:
        """Update an organization."""
        existing = await (
            self._db()
            .select(AuthOrganization)
            .where(AuthOrganization.id == str(org_id))
            .first_or_none()
        )
        if existing is None:
            return None

        updates: dict[str, Any] = {"updated_at": datetime.now(UTC)}
        if name is not None:
            updates["name"] = name
        if slug is not None:
            updates["slug"] = slug

        [result] = await (
            self._db()
            .update(AuthOrganization)
            .set(**updates)
            .where(AuthOrganization.id == str(org_id))
            .returning(AuthOrganization)
            .execute()
        )

        return self._to_org_info(result)

    async def delete_org(self, org_id: str | uuid.UUID) -> bool:
        """Delete an organization and all its memberships."""
        existing = await (
            self._db()
            .select(AuthOrganization)
            .where(AuthOrganization.id == str(org_id))
            .first_or_none()
        )
        if existing is None:
            return False

        await (
            self._db()
            .delete(AuthOrganization)
            .where(AuthOrganization.id == str(org_id))
            .execute()
        )
        return True

    async def list_orgs(
        self,
        *,
        user_id: str | uuid.UUID | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[OrgInfo]:
        """List organizations, optionally filtered by user membership."""
        q = (
            self._db()
            .select(AuthOrganization)
            .order_by(AuthOrganization.created_at, asc=False)
        )

        if user_id is not None:
            q = q.inner_join(
                AuthOrgMember,
                AuthOrgMember.org_id == AuthOrganization.id,
            ).where(AuthOrgMember.user_id == str(user_id))

        if limit is not None:
            q = q.limit(limit)
        if offset is not None:
            q = q.offset(offset)
        return [self._to_org_info(o) for o in await q.execute()]

    # =========================================================================
    # Organization Membership
    # =========================================================================

    async def add_org_member(
        self,
        *,
        org_id: str | uuid.UUID,
        user_id: str | uuid.UUID,
        role: str = "member",
    ) -> OrgMemberInfo | None:
        """Add a user to an organization."""
        now = datetime.now(UTC)
        member = await (
            self._db()
            .insert(AuthOrgMember)
            .values(
                org_id=str(org_id),
                user_id=str(user_id),
                role=role,
                created_at=now,
                updated_at=now,
            )
            .ignore_conflicts(target=(AuthOrgMember.org_id, AuthOrgMember.user_id))
            .returning(AuthOrgMember)
            .execute()
        )
        if member is None:
            return None

        return self._to_org_member_info(member)

    async def update_org_member(
        self,
        *,
        org_id: str | uuid.UUID,
        user_id: str | uuid.UUID,
        role: str,
    ) -> OrgMemberInfo | None:
        """Update a member's role. Returns ``None`` if not found."""
        existing = await (
            self._db()
            .select(AuthOrgMember)
            .where(AuthOrgMember.org_id == str(org_id))
            .where(AuthOrgMember.user_id == str(user_id))
            .first_or_none()
        )
        if existing is None:
            return None

        [result] = await (
            self._db()
            .update(AuthOrgMember)
            .set(role=role, updated_at=datetime.now(UTC))
            .where(AuthOrgMember.org_id == str(org_id))
            .where(AuthOrgMember.user_id == str(user_id))
            .returning(AuthOrgMember)
            .execute()
        )

        return self._to_org_member_info(result)

    async def remove_org_member(
        self,
        *,
        org_id: str | uuid.UUID,
        user_id: str | uuid.UUID,
    ) -> bool:
        """Remove a user from an organization.

        Returns ``False`` if not found.
        """
        existing = await (
            self._db()
            .select(AuthOrgMember)
            .where(AuthOrgMember.org_id == str(org_id))
            .where(AuthOrgMember.user_id == str(user_id))
            .first_or_none()
        )
        if existing is None:
            return False

        # Prevent removing the last owner
        if existing.role == "owner":
            owner_count = await (
                self._db()
                .select(AuthOrgMember)
                .where(AuthOrgMember.org_id == str(org_id))
                .where(AuthOrgMember.role == "owner")
                .count()
            )
            if owner_count <= 1:
                logger.error(
                    "Remove org member failed: cannot remove last owner of org %s",
                    org_id,
                )
                return False

        await (
            self._db()
            .delete(AuthOrgMember)
            .where(AuthOrgMember.org_id == str(org_id))
            .where(AuthOrgMember.user_id == str(user_id))
            .execute()
        )
        return True

    async def list_org_members(
        self,
        org_id: str | uuid.UUID,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[OrgMemberInfo]:
        """List members of an organization."""
        q = (
            self._db()
            .select(AuthOrgMember)
            .where(AuthOrgMember.org_id == str(org_id))
            .order_by(AuthOrgMember.created_at, asc=True)
        )
        if limit is not None:
            q = q.limit(limit)
        if offset is not None:
            q = q.offset(offset)
        return [self._to_org_member_info(m) for m in await q.execute()]

    async def get_org_member(
        self,
        *,
        org_id: str | uuid.UUID,
        user_id: str | uuid.UUID,
    ) -> OrgMemberInfo | None:
        """Get a single membership record."""
        member = await (
            self._db()
            .select(AuthOrgMember)
            .where(AuthOrgMember.org_id == str(org_id))
            .where(AuthOrgMember.user_id == str(user_id))
            .first_or_none()
        )
        return self._to_org_member_info(member) if member is not None else None

    # =========================================================================
    # Organization Session Context
    # =========================================================================

    async def set_active_org(
        self,
        *,
        session_id: str | uuid.UUID,
        org_id: str | uuid.UUID | None,
    ) -> TokenPair | None:
        """Switch the active organization for a session.

        Returns new tokens, or ``None`` if the user is not a member.
        """
        # Find the active session
        session = await (
            self._db()
            .select(AuthSession)
            .where(AuthSession.session_id == str(session_id))
            .where(~AuthSession.revoked)
            .order_by(AuthSession.created_at, asc=False)
            .first_or_none()
        )
        if session is None:
            logger.error("Set active org failed: session not found")
            return None

        extra_claims: dict[str, Any] = {"role": session.role}

        if org_id is not None:
            # Verify user is a member
            member = await (
                self._db()
                .select(AuthOrgMember)
                .where(AuthOrgMember.org_id == str(org_id))
                .where(AuthOrgMember.user_id == str(session.user_id))
                .first_or_none()
            )
            if member is None:
                return None

            extra_claims["org_id"] = str(org_id)
            extra_claims["org_role"] = member.role

            # Update session's org_id
            await (
                self._db()
                .update(AuthSession)
                .set(org_id=str(org_id))
                .where(AuthSession.session_id == str(session_id))
                .where(~AuthSession.revoked)
                .execute()
            )
        else:
            # Clear org context
            await (
                self._db()
                .update(AuthSession)
                .set(org_id=None)
                .where(AuthSession.session_id == str(session_id))
                .where(~AuthSession.revoked)
                .execute()
            )

        return create_token_pair(
            self._config.jwt,
            session.user_id,
            session.session_id,
            session.token,
            extra_claims=extra_claims,
        )
