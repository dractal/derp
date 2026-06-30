"""Native authentication service — derp is its own IdP.

Native mints and verifies its own JWTs and stores users, sessions, and orgs
in Postgres. It is single-tenant: ``tenant_id`` is always ``None``.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from derp.auth.base import (
    AuthOutcome,
    AuthResult,
    BaseAuthClient,
    Identity,
    MFAStatus,
    Org,
    OrgMember,
    Page,
    Session,
    TokenSet,
)
from derp.auth.email import EmailClient
from derp.auth.exceptions import (
    CapabilityNotSupportedError,
    ConfirmationURLMissingError,
    EmailAlreadyExistsError,
    InvalidCredentialsError,
    InvalidTokenError,
    LastOwnerError,
    MemberAlreadyExistsError,
    OrgMemberNotFoundError,
    OrgNotFoundError,
    OrgSlugConflictError,
    PasswordValidationError,
    SignupDisabledError,
    UserNotFoundError,
)
from derp.auth.jwt import TokenPair, create_token_pair, decode_token
from derp.auth.models import (
    AuthOrganization,
    AuthOrgMember,
    AuthProvider,
    AuthRequest,
    AuthSession,
    AuthStatus,
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
from derp.config import NativeAuthConfig
from derp.kv.base import KVClient
from derp.orm import DatabaseEngine

logger = logging.getLogger(__name__)


class NativeAuthClient(BaseAuthClient):
    """Native authentication client (email/password, magic link, OAuth)."""

    supports_password = True
    supports_oauth = True
    supports_magic_link = True
    supports_orgs = True
    supports_sessions = True
    supports_user_admin = True

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
    # Response mapping
    # =========================================================================

    def _to_identity(self, user: AuthUser) -> Identity:
        """Convert an internal AuthUser ORM model to a public Identity."""
        return Identity(
            id=str(user.id),
            tenant_id=None,
            email=user.email,
            email_verified=user.email_confirmed_at is not None,
            phone=None,
            phone_verified=False,
            is_anonymous=False,
            disabled=not user.is_active,
            roles=(user.role,),
            created_at=user.created_at,
            updated_at=user.updated_at,
            last_sign_in_at=user.last_sign_in_at,
            metadata={
                "provider": user.provider.value
                if hasattr(user.provider, "value")
                else user.provider,
                "provider_id": user.provider_id,
            },
        )

    def _to_token_set(self, pair: TokenPair) -> TokenSet:
        """Convert an internal TokenPair into a public TokenSet."""
        return TokenSet(
            access_token=pair.access_token,
            refresh_token=pair.refresh_token,
            id_token=pair.access_token or None,
            token_type=pair.token_type,
            expires_in=pair.expires_in,
            expires_at=pair.expires_at,
        )

    # =========================================================================
    # User Management
    # =========================================================================

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

    async def get_user(self, user_id: str) -> Identity:
        """Get a user by their ID.

        Raises:
            UserNotFoundError: No user with that id.
        """
        user = await self._fetch_user(user_id)
        if user is None:
            raise UserNotFoundError(f"User {user_id!r} not found")
        return self._to_identity(user)

    async def find_user(
        self,
        *,
        user_id: str | None = None,
        email: str | None = None,
        phone: str | None = None,
    ) -> Identity | None:
        """Find a user by id or email; ``None`` if no match.

        Provide exactly one of ``user_id`` / ``email`` / ``phone``. Native
        has no phone column, so ``phone`` lookup is unsupported.
        """
        provided = [k for k in (user_id, email, phone) if k is not None]
        if len(provided) != 1:
            raise ValueError("Provide exactly one of user_id=, email=, or phone=")
        if phone is not None:
            raise ValueError(
                "Native auth has no phone column; phone lookup unsupported"
            )
        user = (
            await self._fetch_user(user_id)
            if user_id is not None
            else await self._get_user_by_email(email)  # type: ignore[arg-type]
        )
        return self._to_identity(user) if user is not None else None

    async def list_users(
        self, *, limit: int = 50, cursor: str | None = None
    ) -> Page[Identity]:
        """List users ordered by creation date (newest first).

        ``cursor`` is treated as an opaque stringified offset; ``None`` starts
        at the first row.
        """
        offset = int(cursor) if cursor else 0
        q = (
            self._db()
            .select(AuthUser)
            .order_by(AuthUser.created_at, asc=False)
            .limit(limit)
            .offset(offset)
        )
        items = [self._to_identity(u) for u in await q.execute()]
        has_more = len(items) == limit
        next_cursor = str(offset + limit) if has_more else None
        return Page(items=items, next_cursor=next_cursor, has_more=has_more)

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

    async def update_user(self, *, user_id: str, **kwargs: Any) -> AuthResult[Identity]:
        """Update user data.

        On success ``result.value`` is the updated :class:`Identity`. Expected
        failure (attached to ``result.error``): ``UserNotFoundError`` when no
        user has that id.
        """
        user = await self._fetch_user(user_id=user_id)
        if not user:
            return AuthResult(error=UserNotFoundError(f"User {user_id!r} not found"))

        updates: dict[str, Any] = {"updated_at": datetime.now(UTC)}

        email = kwargs.pop("email", None)
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

        return AuthResult(value=self._to_identity(result))

    async def delete_user(self, user_id: str) -> bool:
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
        await self.revoke_all_sessions(user_id)

        await self._db().delete(AuthUser).where(AuthUser.id == str(user_id)).execute()

        await self._invalidate_user_cache(user_id, email)
        return True

    # =========================================================================
    # Email/Password Authentication
    # =========================================================================

    async def sign_up(
        self,
        *,
        email: str | None = None,
        phone: str | None = None,
        password: str | None = None,
        request: AuthRequest | None = None,
        confirmation_url: str | None = None,
        confirmation_subject: str = "Confirm your email address",
        **kwargs: Any,
    ) -> AuthOutcome:
        """Register a new user with email and password.

        Outcome status: ``COMPLETE``, ``EMAIL_EXISTS``, or ``WEAK_PASSWORD``.

        Raises:
            SignupDisabledError: Signup is disabled in config.
            ConfirmationURLMissingError: confirmation_url omitted while
                ``enable_confirmation`` is on.
        """
        if email is None:
            raise ValueError("Native sign_up requires an email")
        if not self._config.enable_signup:
            raise SignupDisabledError()
        if confirmation_url is None and self._config.enable_confirmation:
            raise ConfirmationURLMissingError(
                "`confirmation_url` is required when confirmation is enabled."
            )

        # Validate password
        validation = validate_password(self._config.password, password or "")
        if not validation.valid:
            return AuthOutcome(
                status=AuthStatus.WEAK_PASSWORD,
                error=PasswordValidationError("; ".join(validation.errors)),
            )

        # Check if user exists
        exists = await (
            self._db()
            .select(AuthUser.id)
            .from_(AuthUser)
            .where(AuthUser.email == email.lower())
            .first_or_none()
        )
        if exists:
            return AuthOutcome(
                status=AuthStatus.EMAIL_EXISTS,
                error=EmailAlreadyExistsError(email.lower()),
            )

        # Create user
        hashed_password = await self._hasher.async_hash(password or "")
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
        token_pair = await self._create_session(user.id, role=user.role)

        return AuthOutcome(
            status=AuthStatus.COMPLETE,
            identity=self._to_identity(user),
            tokens=token_pair,
        )

    async def sign_in_with_password(
        self, *, identifier: str, password: str
    ) -> AuthOutcome:
        """Sign in with an email *identifier* + password.

        Outcome status: ``COMPLETE`` or ``INVALID_CREDENTIALS``. Wrong
        password, unknown user, disabled, and unconfirmed all collapse to
        ``INVALID_CREDENTIALS`` (logged at ``WARNING``) to avoid
        email-enumeration leaks.
        """
        email = identifier
        user = await self._get_user_by_email(email=email.lower())
        if not user:
            logger.warning("Sign-in failed: user not found for %s", email)
            return AuthOutcome(
                status=AuthStatus.INVALID_CREDENTIALS, error=InvalidCredentialsError()
            )

        if not user.encrypted_password:
            logger.warning("Sign-in failed: no password set for %s", email)
            return AuthOutcome(
                status=AuthStatus.INVALID_CREDENTIALS, error=InvalidCredentialsError()
            )

        if not await self._hasher.async_verify(password, user.encrypted_password):
            logger.warning("Sign-in failed: invalid password for %s", email)
            return AuthOutcome(
                status=AuthStatus.INVALID_CREDENTIALS, error=InvalidCredentialsError()
            )

        if not user.is_active:
            logger.warning("Sign-in failed: account disabled for %s", email)
            return AuthOutcome(
                status=AuthStatus.INVALID_CREDENTIALS, error=InvalidCredentialsError()
            )

        if self._config.enable_confirmation and not user.email_confirmed_at:
            logger.warning("Sign-in failed: email not confirmed for %s", email)
            return AuthOutcome(
                status=AuthStatus.INVALID_CREDENTIALS, error=InvalidCredentialsError()
            )

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

        token_pair = await self._create_session(user.id, role=user.role)

        return AuthOutcome(
            status=AuthStatus.COMPLETE,
            identity=self._to_identity(user),
            tokens=token_pair,
        )

    # =========================================================================
    # Magic Link Authentication
    # =========================================================================

    async def send_magic_link(self, *, email: str, redirect_url: str) -> None:
        """Send a magic link email for passwordless sign in.

        Creates user if they don't exist (if signup enabled).

        Raises:
            ValueError: If magic link authentication is not enabled.
            SignupDisabledError: User doesn't exist and signup is disabled.
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
            magic_link_url=f"{redirect_url}?token={token}",
        )

    async def verify_magic_link(
        self, token: str, *, email: str | None = None
    ) -> AuthOutcome:
        """Verify a magic link and sign in.

        Outcome status: ``COMPLETE`` or ``INVALID_TOKEN`` (unknown, expired,
        already used, or pointing at a missing/disabled account).
        """
        kv_key = f"{self._config.cache_prefix}:magic_link:{token}".encode()
        user_id_bytes = await self._kv().get(kv_key)

        if user_id_bytes is None:
            logger.warning("Magic link verification failed: token expired or invalid")
            return AuthOutcome(
                status=AuthStatus.INVALID_TOKEN,
                error=InvalidTokenError("Magic link is invalid or expired"),
            )

        # Delete on use (single use)
        await self._kv().delete(kv_key)

        user = await self._fetch_user(user_id_bytes.decode())
        if not user:
            logger.warning("Magic link verification failed: user not found")
            return AuthOutcome(
                status=AuthStatus.INVALID_TOKEN,
                error=InvalidTokenError("Magic link is invalid or expired"),
            )

        if not user.is_active:
            logger.warning(
                "Magic link verification failed: account disabled for %s", user.email
            )
            return AuthOutcome(
                status=AuthStatus.INVALID_TOKEN,
                error=InvalidTokenError("Magic link is invalid or expired"),
            )

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

        token_pair = await self._create_session(user.id, role=user.role)

        return AuthOutcome(
            status=AuthStatus.COMPLETE,
            identity=self._to_identity(user),
            tokens=token_pair,
        )

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

    def authorization_url(
        self,
        *,
        provider: str | AuthProvider | None = None,
        organization: str | None = None,
        connection: str | None = None,
        domain: str | None = None,
        state: str,
        redirect_uri: str | None = None,
        scopes: list[str] | None = None,
    ) -> str:
        """Build the social-OAuth authorization URL for a provider.

        Native only supports the social ``provider`` path; SSO selectors
        (``organization`` / ``connection`` / ``domain``) are unsupported.
        """
        if organization or connection or domain:
            raise CapabilityNotSupportedError("sso")
        if provider is None:
            raise ValueError("Native authorization_url requires a provider")
        oauth_provider = self.get_oauth_provider(provider)
        return oauth_provider.get_authorization_url(state, scopes, redirect_uri)

    async def sign_in_with_oauth(
        self,
        code: str,
        *,
        provider: str | AuthProvider | None = None,
        redirect_uri: str | None = None,
    ) -> AuthOutcome:
        """Complete OAuth sign in with authorization code.

        Creates the user record if this email has never signed in before.

        Outcome status: ``COMPLETE`` or ``INVALID_CREDENTIALS`` (provider
        rejected the code, or the matching account is disabled).
        """
        if provider is None:
            raise ValueError("Native sign_in_with_oauth requires a provider")
        oauth_provider = self.get_oauth_provider(provider)

        # Get user info from provider
        user_info = await oauth_provider.authenticate(code, redirect_uri)
        if user_info is None:
            return AuthOutcome(
                status=AuthStatus.INVALID_CREDENTIALS,
                error=InvalidCredentialsError("OAuth code rejected by provider"),
            )

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
                return AuthOutcome(
                    status=AuthStatus.INVALID_CREDENTIALS,
                    error=InvalidCredentialsError(),
                )

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

        token_pair = await self._create_session(user.id, role=user.role)

        return AuthOutcome(
            status=AuthStatus.COMPLETE,
            identity=self._to_identity(user),
            tokens=token_pair,
        )

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
    ) -> TokenSet:
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

        pair = create_token_pair(
            self._config.jwt,
            user_id,
            session_id,
            refresh_token,
            extra_claims={"role": role},
        )
        return self._to_token_set(pair)

    async def refresh(self, refresh_token: str) -> AuthResult[TokenSet]:
        """Refresh an access token using a refresh token.

        Implements token rotation for security. Happy path is 2 DB calls:
        one UPDATE…RETURNING to atomically revoke the old token, one INSERT
        for the new token.

        Raises:
            InvalidTokenError: Token is unknown, revoked, reused, or expired.
                Reuse detection additionally revokes every token for the
                session before raising.
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
                return AuthResult(
                    error=InvalidTokenError("Refresh token reuse detected")
                )
            logger.warning("Refresh token invalid or revoked")
            return AuthResult(
                error=InvalidTokenError("Refresh token is invalid or revoked")
            )

        [token_record] = revoked_rows

        if token_record.not_after < datetime.now(UTC):
            logger.warning("Refresh token failed: session expired")
            return AuthResult(
                error=InvalidTokenError("Refresh token session has expired")
            )

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

        pair = create_token_pair(
            self._config.jwt,
            token_record.user_id,
            token_record.session_id,
            new_refresh_token,
            extra_claims={"role": token_record.role},
        )
        return AuthResult(value=self._to_token_set(pair))

    async def verify_token(self, token: str) -> Session | None:
        """Verify a JWT access token (networkless) and return its session.

        Decodes and verifies the JWT signature and expiry, building a
        ``Session`` from its claims; returns ``None`` if the token is invalid
        or expired.
        """
        payload = decode_token(self._config.jwt, token)
        if payload is None:
            return None
        extra = payload.extra or {}
        return Session(
            user_id=payload.sub,
            session_id=payload.session_id,
            tenant_id=None,
            org_id=extra.get("org_id"),
            org_role=extra.get("org_role"),
            roles=(extra.get("role", "default"),),
            scopes=(),
            is_anonymous=False,
            mfa=MFAStatus(enrolled=False, satisfied=False, factor_types=()),
            issued_at=payload.iat,
            expires_at=payload.exp,
            claims=extra,
        )

    async def list_sessions(
        self, *, user_id: str | None = None, limit: int = 50, cursor: str | None = None
    ) -> Page[Session]:
        """List active (non-revoked) sessions ordered by creation date.

        ``cursor`` is an opaque stringified offset.
        """
        offset = int(cursor) if cursor else 0
        q = (
            self._db()
            .select(AuthSession)
            .where(~AuthSession.revoked)
            .order_by(AuthSession.created_at, asc=False)
        )
        if user_id is not None:
            q = q.where(AuthSession.user_id == str(user_id))
        q = q.limit(limit).offset(offset)
        sessions = await q.execute()
        items = [
            Session(
                user_id=str(s.user_id),
                session_id=str(s.session_id),
                tenant_id=None,
                org_id=str(s.org_id) if s.org_id is not None else None,
                org_role=None,
                roles=(s.role,),
                scopes=(),
                is_anonymous=False,
                mfa=MFAStatus(enrolled=False, satisfied=False, factor_types=()),
                issued_at=s.created_at,
                expires_at=s.not_after,
                claims={},
            )
            for s in sessions
        ]
        has_more = len(items) == limit
        next_cursor = str(offset + limit) if has_more else None
        return Page(items=items, next_cursor=next_cursor, has_more=has_more)

    async def revoke_session(self, session_id: str) -> AuthResult[bool]:
        """Revoke a session by deleting all of its tokens."""
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
        return AuthResult(value=True)

    async def revoke_all_sessions(self, user_id: str) -> AuthResult[bool]:
        """Revoke all sessions for a user by deleting all their tokens."""
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
        return AuthResult(value=True)

    # =========================================================================
    # Password Recovery
    # =========================================================================

    async def request_password_reset(
        self,
        *,
        email: str,
        redirect_url: str | None = None,
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
            recovery_url=f"{redirect_url}?token={token}",
            **kwargs,
        )

    async def reset_password(
        self, token: str, new_password: str
    ) -> AuthResult[Identity]:
        """Reset password using recovery token.

        Raises:
            PasswordValidationError: New password did not meet requirements.
            InvalidTokenError: Recovery token is unknown, expired, used, or
                points at a missing user.
        """
        # Validate password
        validation = validate_password(self._config.password, new_password)
        if not validation.valid:
            return AuthResult(
                error=PasswordValidationError("; ".join(validation.errors))
            )

        # Look up recovery token in KV
        kv_key = f"{self._config.cache_prefix}:recovery:{token}".encode()
        user_id_bytes = await self._kv().get(kv_key)

        if user_id_bytes is None:
            return AuthResult(
                error=InvalidTokenError("Recovery token is invalid or expired")
            )

        # Delete token (single use)
        await self._kv().delete(kv_key)

        user = await self._fetch_user(user_id=user_id_bytes.decode())
        if user is None:
            return AuthResult(
                error=InvalidTokenError("Recovery token is invalid or expired")
            )

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
        await self.revoke_all_sessions(str(user.id))

        return AuthResult(value=self._to_identity(result))

    # =========================================================================
    # Email Confirmation
    # =========================================================================

    async def verify_email(self, token: str) -> AuthResult[Identity]:
        """Confirm email address with token.

        Raises:
            InvalidTokenError: Confirmation token is unknown, expired, used,
                or points at a missing user.
        """
        kv_key = f"{self._config.cache_prefix}:confirmation:{token}".encode()
        user_id_bytes = await self._kv().get(kv_key)

        if user_id_bytes is None:
            return AuthResult(
                error=InvalidTokenError("Confirmation token is invalid or expired")
            )

        # Delete token (single use)
        await self._kv().delete(kv_key)

        user = await self._fetch_user(user_id=user_id_bytes.decode())
        if user is None:
            return AuthResult(
                error=InvalidTokenError("Confirmation token is invalid or expired")
            )

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

        return AuthResult(value=self._to_identity(result))

    # =========================================================================
    # Organizations
    # =========================================================================

    def _to_org(self, org: AuthOrganization) -> Org:
        """Convert an AuthOrganization ORM model to a public Org."""
        return Org(
            id=str(org.id),
            tenant_id=None,
            name=org.name,
            slug=org.slug,
            metadata=org.metadata or {},
            created_at=org.created_at,
            updated_at=org.updated_at,
        )

    def _to_org_member(self, member: AuthOrgMember) -> OrgMember:
        """Convert an AuthOrgMember ORM model to a public OrgMember."""
        return OrgMember(
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
        creator_id: str,
        **kwargs: Any,
    ) -> AuthResult[Org]:
        """Create an organization. The creator is added as owner.

        On success, ``result.value`` is the new :class:`Org`. Expected failure:
        :class:`OrgSlugConflictError` (slug already taken) is attached to
        ``result.error``.
        """
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
            return AuthResult(error=OrgSlugConflictError(slug))

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

        return AuthResult(value=self._to_org(org))

    async def get_org(
        self, *, org_id: str | None = None, slug: str | None = None
    ) -> AuthResult[Org]:
        """Get an org by id or slug (pass exactly one).

        Expected failure: :class:`OrgNotFoundError` attached to ``result.error``
        when nothing matches.
        """
        self._check_org_ref(org_id, slug)
        q = self._db().select(AuthOrganization)
        q = (
            q.where(AuthOrganization.id == org_id)
            if org_id is not None
            else q.where(AuthOrganization.slug == slug)
        )
        found = await q.first_or_none()
        if found is None:
            return AuthResult(
                error=OrgNotFoundError(f"No org matching {org_id or slug!r}")
            )
        return AuthResult(value=self._to_org(found))

    async def update_org(
        self,
        *,
        org_id: str | None = None,
        slug: str | None = None,
        name: str | None = None,
        new_slug: str | None = None,
        **kwargs: Any,
    ) -> AuthResult[Org]:
        """Update an org's fields. Address by ``org_id`` or ``slug`` (one).

        ``name`` / ``new_slug`` are the new values to assign (``new_slug`` is
        spelled distinctly so it never collides with the addressing ``slug``).

        Expected failures attached to ``result.error``:
        :class:`OrgNotFoundError`, :class:`OrgSlugConflictError`.
        """
        self._check_org_ref(org_id, slug)
        q = self._db().select(AuthOrganization)
        q = (
            q.where(AuthOrganization.id == org_id)
            if org_id is not None
            else q.where(AuthOrganization.slug == slug)
        )
        existing = await q.first_or_none()
        if existing is None:
            return AuthResult(
                error=OrgNotFoundError(f"No org matching {org_id or slug!r}")
            )

        updates: dict[str, Any] = {"updated_at": datetime.now(UTC)}
        if name is not None:
            updates["name"] = name
        if new_slug is not None:
            updates["slug"] = new_slug

        try:
            [result] = await (
                self._db()
                .update(AuthOrganization)
                .set(**updates)
                .where(AuthOrganization.id == existing.id)
                .returning(AuthOrganization)
                .execute()
            )
        except Exception:
            # Slug uniqueness is enforced at the column level; surface a
            # typed conflict only if the caller actually changed the slug.
            if new_slug is not None:
                return AuthResult(error=OrgSlugConflictError(new_slug))
            raise

        return AuthResult(value=self._to_org(result))

    async def delete_org(
        self, *, org_id: str | None = None, slug: str | None = None
    ) -> bool:
        """Delete an org (by id or slug) and its memberships. ``False`` if absent."""
        self._check_org_ref(org_id, slug)
        q = self._db().select(AuthOrganization)
        q = (
            q.where(AuthOrganization.id == org_id)
            if org_id is not None
            else q.where(AuthOrganization.slug == slug)
        )
        existing = await q.first_or_none()
        if existing is None:
            return False

        await (
            self._db()
            .delete(AuthOrganization)
            .where(AuthOrganization.id == existing.id)
            .execute()
        )
        return True

    async def list_orgs(
        self,
        *,
        user_id: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> Page[Org]:
        """List organizations, optionally filtered by user membership.

        ``cursor`` is an opaque stringified offset.
        """
        offset = int(cursor) if cursor else 0
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

        q = q.limit(limit).offset(offset)
        items = [self._to_org(o) for o in await q.execute()]
        has_more = len(items) == limit
        next_cursor = str(offset + limit) if has_more else None
        return Page(items=items, next_cursor=next_cursor, has_more=has_more)

    # =========================================================================
    # Organization Membership
    # =========================================================================

    async def add_member(
        self,
        *,
        user_id: str,
        org_id: str | None = None,
        slug: str | None = None,
        role: str = "member",
    ) -> AuthResult[OrgMember]:
        """Add a user to an org (by id or slug).

        Expected failures attached to ``result.error``:
        :class:`OrgNotFoundError`, :class:`MemberAlreadyExistsError`.
        """
        self._check_org_ref(org_id, slug)
        db = self._db()
        # The FK needs the canonical id: use org_id directly, else resolve the
        # slug with a single lookup.
        canonical = org_id
        if canonical is None:
            org = await (
                db.select(AuthOrganization)
                .where(AuthOrganization.slug == slug)
                .first_or_none()
            )
            if org is None:
                return AuthResult(error=OrgNotFoundError(f"No org matching {slug!r}"))
            canonical = str(org.id)

        member = await (
            db.insert(AuthOrgMember)
            .values(org_id=canonical, user_id=str(user_id), role=role)
            .ignore_conflicts(target=(AuthOrgMember.org_id, AuthOrgMember.user_id))
            .returning(AuthOrgMember)
            .execute()
        )
        if member is None:
            return AuthResult(error=MemberAlreadyExistsError())

        return AuthResult(value=self._to_org_member(member))

    async def update_member(
        self,
        *,
        user_id: str,
        role: str,
        org_id: str | None = None,
        slug: str | None = None,
    ) -> AuthResult[OrgMember]:
        """Change a member's role in an org (by id or slug).

        Expected failure: :class:`OrgMemberNotFoundError` attached to
        ``result.error`` (not a member, or no such org).
        """
        self._check_org_ref(org_id, slug)
        db = self._db()
        sel = db.select(AuthOrgMember).where(AuthOrgMember.user_id == str(user_id))
        if org_id is not None:
            sel = sel.where(AuthOrgMember.org_id == org_id)
        else:
            sel = sel.inner_join(
                AuthOrganization, AuthOrganization.id == AuthOrgMember.org_id
            ).where(AuthOrganization.slug == slug)
        existing = await sel.first_or_none()
        if existing is None:
            return AuthResult(
                error=OrgMemberNotFoundError(
                    f"User {user_id!r} is not a member of org {org_id or slug!r}"
                )
            )

        # existing.org_id is the canonical id — reuse it, no extra lookup.
        [result] = await (
            db.update(AuthOrgMember)
            .set(role=role, updated_at=datetime.now(UTC))
            .where(AuthOrgMember.org_id == existing.org_id)
            .where(AuthOrgMember.user_id == str(user_id))
            .returning(AuthOrgMember)
            .execute()
        )

        return AuthResult(value=self._to_org_member(result))

    async def remove_member(
        self, *, user_id: str, org_id: str | None = None, slug: str | None = None
    ) -> AuthResult[bool]:
        """Remove a member from an org (by id or slug).

        ``result.value`` is ``True`` if a row was removed, ``False`` if the
        user wasn't a member. Expected failure: :class:`LastOwnerError`
        attached to ``result.error`` when removing would leave the org without
        an owner.
        """
        self._check_org_ref(org_id, slug)
        db = self._db()
        sel = db.select(AuthOrgMember).where(AuthOrgMember.user_id == str(user_id))
        if org_id is not None:
            sel = sel.where(AuthOrgMember.org_id == org_id)
        else:
            sel = sel.inner_join(
                AuthOrganization, AuthOrganization.id == AuthOrgMember.org_id
            ).where(AuthOrganization.slug == slug)
        existing = await sel.first_or_none()
        if existing is None:
            return AuthResult(value=False)

        # Prevent removing the last owner. existing.org_id is the canonical id.
        if existing.role == "owner":
            owner_count = await (
                db.select(AuthOrgMember)
                .where(AuthOrgMember.org_id == existing.org_id)
                .where(AuthOrgMember.role == "owner")
                .count()
            )
            if owner_count <= 1:
                logger.error(
                    "Remove org member failed: cannot remove last owner of org %s",
                    existing.org_id,
                )
                return AuthResult(
                    error=LastOwnerError(
                        f"Cannot remove the last owner of org {existing.org_id!r}"
                    )
                )

        await (
            db.delete(AuthOrgMember)
            .where(AuthOrgMember.org_id == existing.org_id)
            .where(AuthOrgMember.user_id == str(user_id))
            .execute()
        )
        return AuthResult(value=True)

    async def list_members(
        self,
        *,
        org_id: str | None = None,
        slug: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> Page[OrgMember]:
        """List members of an org (by id or slug). ``cursor`` is an opaque offset.

        An unknown org yields an empty page (no existence query needed).
        """
        self._check_org_ref(org_id, slug)
        db = self._db()
        offset = int(cursor) if cursor else 0
        q = db.select(AuthOrgMember)
        if org_id is not None:
            q = q.where(AuthOrgMember.org_id == org_id)
        else:
            q = q.inner_join(
                AuthOrganization, AuthOrganization.id == AuthOrgMember.org_id
            ).where(AuthOrganization.slug == slug)
        q = q.order_by(AuthOrgMember.created_at, asc=True).limit(limit).offset(offset)
        items = [self._to_org_member(m) for m in await q.execute()]
        has_more = len(items) == limit
        next_cursor = str(offset + limit) if has_more else None
        return Page(items=items, next_cursor=next_cursor, has_more=has_more)

    # =========================================================================
    # Organization Session Context
    # =========================================================================

    async def set_active_org(
        self,
        *,
        session_id: str,
        org_id: str | None = None,
        slug: str | None = None,
    ) -> AuthResult[TokenSet]:
        """Switch a session's active org (by id or slug); reissues tokens.

        Pass neither ``org_id`` nor ``slug`` to clear the active org. On success
        ``result.value`` is the reissued :class:`TokenSet`. Expected failures
        (attached to ``result.error``): ``InvalidTokenError`` (session unknown
        or revoked), ``OrgNotFoundError`` (slug matches no org), and
        ``OrgMemberNotFoundError`` (user isn't a member of the target org).
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
            return AuthResult(error=InvalidTokenError("Session not found or revoked"))

        extra_claims: dict[str, Any] = {"role": session.role}

        if org_id is not None or slug is not None:
            self._check_org_ref(org_id, slug)
            # The membership check + claim need the canonical id: use org_id
            # directly, else resolve the slug with a single lookup.
            canonical = org_id
            if canonical is None:
                org = await (
                    self._db()
                    .select(AuthOrganization)
                    .where(AuthOrganization.slug == slug)
                    .first_or_none()
                )
                if org is None:
                    return AuthResult(
                        error=OrgNotFoundError(f"No org matching {slug!r}")
                    )
                canonical = str(org.id)

            # Verify user is a member
            member = await (
                self._db()
                .select(AuthOrgMember)
                .where(AuthOrgMember.org_id == canonical)
                .where(AuthOrgMember.user_id == str(session.user_id))
                .first_or_none()
            )
            if member is None:
                return AuthResult(
                    error=OrgMemberNotFoundError(
                        f"User {session.user_id!r} is not a member of org {canonical!r}"
                    )
                )

            extra_claims["org_id"] = canonical
            extra_claims["org_role"] = member.role

            # Update session's org_id
            await (
                self._db()
                .update(AuthSession)
                .set(org_id=canonical)
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

        pair = create_token_pair(
            self._config.jwt,
            session.user_id,
            session.session_id,
            session.token,
            extra_claims=extra_claims,
        )
        return AuthResult(value=self._to_token_set(pair))
