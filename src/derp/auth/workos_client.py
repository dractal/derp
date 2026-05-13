"""WorkOS authentication client."""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import Any

import jwt as pyjwt
from etils import epy
from jwt import PyJWKClient

from derp.auth.base import BaseAuthClient
from derp.auth.exceptions import (
    AuthBackendError,
    AuthNotConnectedError,
    EmailAlreadyExistsError,
    InvalidCredentialsError,
    InvalidTokenError,
    MemberAlreadyExistsError,
    OrgMemberNotFoundError,
    OrgNotFoundError,
    OrgSlugConflictError,
    UserNotFoundError,
)
from derp.auth.jwt import TokenPair
from derp.auth.models import (
    AuthRequest,
    AuthResult,
    CursorResult,
    OrgInfo,
    OrgMemberInfo,
    SessionInfo,
    UserInfo,
    WorkOSOrganization,
)
from derp.config import WorkOSConfig
from derp.orm import DatabaseEngine

with epy.lazy_imports():
    import workos
    import workos.exceptions as workos_exc

logger = logging.getLogger(__name__)

_PROVIDER_MAP: dict[str, str] = {
    "google": "GoogleOAuth",
    "github": "GitHubOAuth",
    "apple": "AppleOAuth",
    "microsoft": "MicrosoftOAuth",
    "salesforce": "SalesforceOAuth",
}


class WorkOSAuthClient(BaseAuthClient):
    """WorkOS-backed authentication client.

    Delegates user management, sign-up, sign-in, and organization
    management to the WorkOS API. JWT verification is performed locally
    against the WorkOS JWKS endpoint.

    WorkOS uses cursor-based pagination. The offset-based ``list_users``,
    ``count_users``, and ``list_org_members`` methods raise
    ``NotImplementedError`` — use the ``*_by_cursor`` variants instead.
    """

    def __init__(self, config: WorkOSConfig) -> None:
        self._config = config
        self._workos: workos.AsyncWorkOSClient | None = None
        self._jwks_client: PyJWKClient | None = None
        self._database_client: DatabaseEngine | None = None

    async def connect(self) -> None:
        """Create the WorkOS client connection."""
        self._workos = workos.AsyncWorkOSClient(
            api_key=self._config.api_key,
            client_id=self._config.client_id,
        )
        self._jwks_client = PyJWKClient(self._workos.user_management.get_jwks_url())

    async def disconnect(self) -> None:
        """Close the underlying WorkOS HTTP client."""
        if self._workos is not None:
            await self._workos._http_client.close()
            self._workos = None

    def set_db(self, db: DatabaseEngine | None) -> None:
        self._database_client = db

    def _db(self) -> DatabaseEngine:
        if self._database_client is None:
            raise ValueError(
                "Database client not set. Organization methods require "
                "a database. Call `set_db()` first."
            )
        return self._database_client

    # ------------------------------------------------------------------
    # Org id resolution
    #
    # The WorkOS org id is the canonical handle everywhere — it's what
    # JWTs carry, what the WorkOS API expects, and what app FKs target.
    # The local ``WorkOSOrganization`` table is purely a slug → id index
    # (its UNIQUE constraint also enforces slug uniqueness across orgs).
    #
    # When callers pass ``org_id=``, we use it directly with no DB hit.
    # When they pass ``slug=``, we look up the matching id locally.
    #
    # The slug VALUE itself comes from WorkOS org metadata, written by
    # ``create_org`` / ``update_org`` and read straight off the returned
    # org object — no DB hit needed for the id → slug direction.
    # ------------------------------------------------------------------

    async def _resolve_workos_org_id(
        self,
        *,
        org_id: str | uuid.UUID | None,
        slug: str | None,
    ) -> str | None:
        """Resolve (org_id, slug) → the WorkOS org id.

        Exactly one of ``org_id`` / ``slug`` must be provided. Returns
        the WorkOS-string id, or ``None`` when ``slug`` was given but no
        matching local row exists. Raises ``ValueError`` on bad input.

        The ``org_id`` path does NOT hit the database — passthrough only.
        Slug paths read one row from the local table.
        """
        if (org_id is None) == (slug is None):
            raise ValueError("Provide exactly one of org_id= or slug=")
        if slug is not None:
            row = await (
                self._db()
                .select(WorkOSOrganization)
                .where(WorkOSOrganization.slug == slug)
                .first_or_none()
            )
            return row.id if row else None
        return str(org_id)

    # ------------------------------------------------------------------
    # Mappers
    #
    # Conversion from WorkOS API objects to our public types is inlined
    # at each call site rather than extracted into helpers. The shape is
    # uniform — read the raw fields, normalise dates/metadata, build the
    # dataclass — and inlining keeps each method readable end-to-end.
    # The price is some duplication; the gain is no indirection.
    # ------------------------------------------------------------------

    def _cursor_result[T](self, items: list[T], list_metadata: Any) -> CursorResult[T]:
        """Build a CursorResult from a WorkOS list response."""
        after = getattr(list_metadata, "after", None)
        return CursorResult(data=items, has_more=after is not None, next_cursor=after)

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    async def authenticate(self, request: AuthRequest) -> SessionInfo | None:
        """Verify a WorkOS JWT from the Authorization header."""
        if self._jwks_client is None:
            raise AuthNotConnectedError()

        auth_header = request.headers.get("authorization") or request.headers.get(
            "Authorization"
        )
        if not auth_header or not auth_header.startswith("Bearer "):
            return None

        token = auth_header[7:]

        try:
            signing_key = self._jwks_client.get_signing_key_from_jwt(token)
            claims = pyjwt.decode(
                token,
                signing_key.key,
                algorithms=["RS256"],
                options={"verify_aud": False},
            )
        except (pyjwt.exceptions.PyJWKClientError, pyjwt.exceptions.InvalidTokenError):
            return None

        return SessionInfo(
            user_id=claims["sub"],
            session_id=claims.get("sid", claims["sub"]),
            role=claims.get("role", "default"),
            expires_at=datetime.fromtimestamp(claims["exp"], tz=UTC),
            metadata={},
            org_id=claims.get("org_id"),
            org_role=claims.get("role") if claims.get("org_id") else None,
        )

    # ------------------------------------------------------------------
    # User management
    # ------------------------------------------------------------------

    async def get_user(self, user_id: str | uuid.UUID) -> UserInfo:
        if self._workos is None:
            raise AuthNotConnectedError()
        try:
            user = await self._workos.user_management.get_user(user_id=str(user_id))
        except workos_exc.NotFoundException as e:
            raise UserNotFoundError(f"User {user_id!r} not found") from e
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS get_user failed: {e}") from e
        return UserInfo(
            id=user.id,
            email=user.email,
            first_name=user.first_name,
            last_name=user.last_name,
            image_url=user.profile_picture_url,
            role=user.metadata.get("role", "default"),
            is_active=user.email_verified,
            # WorkOS metadata is Dict[str, str], so booleans round-trip as
            # "true"/"false" strings — compare explicitly rather than relying
            # on truthiness (a stored "false" would have been truthy).
            is_superuser=user.metadata.get("is_superuser") == "true",
            email_confirmed_at=(
                datetime.fromisoformat(user.created_at) if user.email_verified else None
            ),
            last_sign_in_at=(
                datetime.fromisoformat(user.last_sign_in_at)
                if user.last_sign_in_at
                else None
            ),
            created_at=datetime.fromisoformat(user.created_at),
            updated_at=datetime.fromisoformat(user.updated_at),
            metadata=dict(user.metadata),
        )

    async def list_users(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> list[UserInfo]:
        raise NotImplementedError(
            "WorkOS uses cursor-based pagination. Use list_users_by_cursor() instead."
        )

    async def update_user(
        self,
        *,
        user_id: str | uuid.UUID,
        email: str | None = None,
        **kwargs: Any,
    ) -> UserInfo:
        if self._workos is None:
            raise AuthNotConnectedError()
        params: dict[str, Any] = {"user_id": str(user_id)}
        if email is not None:
            params["email"] = email
        for field in ("first_name", "last_name"):
            if field in kwargs:
                params[field] = kwargs.pop(field)
        params["metadata"] = kwargs
        try:
            user = await self._workos.user_management.update_user(**params)
        except workos_exc.NotFoundException as e:
            raise UserNotFoundError(f"User {user_id!r} not found") from e
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS update_user failed: {e}") from e
        return UserInfo(
            id=user.id,
            email=user.email,
            first_name=user.first_name,
            last_name=user.last_name,
            image_url=user.profile_picture_url,
            role=user.metadata.get("role", "default"),
            is_active=user.email_verified,
            is_superuser=user.metadata.get("is_superuser") == "true",
            email_confirmed_at=(
                datetime.fromisoformat(user.created_at) if user.email_verified else None
            ),
            last_sign_in_at=(
                datetime.fromisoformat(user.last_sign_in_at)
                if user.last_sign_in_at
                else None
            ),
            created_at=datetime.fromisoformat(user.created_at),
            updated_at=datetime.fromisoformat(user.updated_at),
            metadata=dict(user.metadata),
        )

    async def delete_user(self, user_id: str | uuid.UUID) -> bool:
        if self._workos is None:
            raise AuthNotConnectedError()
        try:
            await self._workos.user_management.delete_user(user_id=str(user_id))
        except workos_exc.NotFoundException:
            return False
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS delete_user failed: {e}") from e
        return True

    async def count_users(self) -> int:
        raise NotImplementedError(
            "WorkOS does not support count_users. "
            "Use list_users_by_cursor() to paginate."
        )

    # ------------------------------------------------------------------
    # Sessions
    # ------------------------------------------------------------------

    async def list_sessions(
        self,
        *,
        user_id: str | uuid.UUID | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[SessionInfo]:
        if self._workos is None:
            raise AuthNotConnectedError()
        if user_id is None:
            return []
        kwargs: dict[str, Any] = {"user_id": str(user_id)}
        if limit is not None:
            kwargs["limit"] = limit
        try:
            result = await self._workos.user_management.list_sessions(**kwargs)
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS list_sessions failed: {e}") from e

        sessions = [
            SessionInfo(
                user_id=session.user_id,
                session_id=session.id,
                role="default",
                expires_at=datetime.fromisoformat(session.expires_at),
                metadata={},
                org_id=None,
                org_role=None,
            )
            for session in result.data
        ]
        return sessions

    async def sign_out(self, session_id: str | uuid.UUID) -> None:
        if self._workos is None:
            raise AuthNotConnectedError()
        try:
            await self._workos.user_management.revoke_session(
                session_id=str(session_id)
            )
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS revoke_session failed: {e}") from e

    async def sign_out_all(self, user_id: str | uuid.UUID) -> None:
        if self._workos is None:
            raise AuthNotConnectedError()
        try:
            result = await self._workos.user_management.list_sessions(
                user_id=str(user_id)
            )
            for session in result.data:
                await self._workos.user_management.revoke_session(session_id=session.id)
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS sign_out_all failed: {e}") from e

    # ------------------------------------------------------------------
    # Sign-up / sign-in
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
        if self._workos is None:
            raise AuthNotConnectedError()
        try:
            await self._workos.user_management.create_user(
                email=email,
                password=password,
                first_name=kwargs.get("first_name"),
                last_name=kwargs.get("last_name"),
            )
        except workos_exc.ConflictException as e:
            raise EmailAlreadyExistsError(email) from e
        # WorkOS sometimes signals duplicate-email as 422 BadRequest depending
        # on settings; treat that as the same conflict to keep one error shape.
        except workos_exc.BadRequestException as e:
            raise EmailAlreadyExistsError(email) from e
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS create_user failed: {e}") from e
        try:
            auth_resp = await self._workos.user_management.authenticate_with_password(
                email=email,
                password=password,
                ip_address=ip_address,
                user_agent=user_agent,
            )
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS post-signup auth failed: {e}") from e
        try:
            claims = pyjwt.decode(
                auth_resp.access_token, options={"verify_signature": False}
            )
        except pyjwt.exceptions.InvalidTokenError as e:
            raise InvalidTokenError(f"WorkOS post-signup auth failed: {e}") from e

        return AuthResult(
            user=UserInfo(
                id=auth_resp.user.id,
                email=auth_resp.user.email,
                first_name=auth_resp.user.first_name,
                last_name=auth_resp.user.last_name,
                image_url=auth_resp.user.profile_picture_url,
                role=auth_resp.user.metadata.get("role", "default"),
                is_active=auth_resp.user.email_verified,
                is_superuser=auth_resp.user.metadata.get("is_superuser") == "true",
                email_confirmed_at=(
                    datetime.fromisoformat(auth_resp.user.created_at)
                    if auth_resp.user.email_verified
                    else None
                ),
                last_sign_in_at=(
                    datetime.fromisoformat(auth_resp.user.last_sign_in_at)
                    if auth_resp.user.last_sign_in_at
                    else None
                ),
                created_at=datetime.fromisoformat(auth_resp.user.created_at),
                updated_at=datetime.fromisoformat(auth_resp.user.updated_at),
                metadata=auth_resp.user.metadata,
            ),
            tokens=TokenPair(
                access_token=auth_resp.access_token,
                refresh_token=auth_resp.refresh_token,
                expires_at=datetime.fromtimestamp(claims["exp"], tz=UTC),
            ),
        )

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
        if self._workos is None:
            raise AuthNotConnectedError()
        try:
            resp = await self._workos.user_management.authenticate_with_password(
                email=email,
                password=password,
                ip_address=ip_address,
                user_agent=user_agent,
            )
        except workos_exc.AuthenticationException as e:
            raise InvalidCredentialsError() from e
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(
                f"WorkOS authenticate_with_password failed: {e}"
            ) from e
        try:
            claims = pyjwt.decode(
                resp.access_token, options={"verify_signature": False}
            )
        except pyjwt.exceptions.InvalidTokenError as e:
            raise InvalidTokenError(
                f"WorkOS authenticate_with_password failed: {e}"
            ) from e

        return AuthResult(
            user=UserInfo(
                id=resp.user.id,
                email=resp.user.email,
                first_name=resp.user.first_name,
                last_name=resp.user.last_name,
                image_url=resp.user.profile_picture_url,
                role=resp.user.metadata.get("role", "default"),
                is_active=resp.user.email_verified,
                is_superuser=resp.user.metadata.get("is_superuser") == "true",
                email_confirmed_at=(
                    datetime.fromisoformat(resp.user.created_at)
                    if resp.user.email_verified
                    else None
                ),
                last_sign_in_at=(
                    datetime.fromisoformat(resp.user.last_sign_in_at)
                    if resp.user.last_sign_in_at
                    else None
                ),
                created_at=datetime.fromisoformat(resp.user.created_at),
                updated_at=datetime.fromisoformat(resp.user.updated_at),
                metadata=resp.user.metadata,
            ),
            tokens=TokenPair(
                access_token=resp.access_token,
                refresh_token=resp.refresh_token,
                expires_at=datetime.fromtimestamp(claims["exp"], tz=UTC),
            ),
        )

    async def sign_in_with_magic_link(self, *, email: str, magic_link_url: str) -> None:
        if self._workos is None:
            raise AuthNotConnectedError()
        try:
            await self._workos.user_management.create_magic_auth(email=email)
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS create_magic_auth failed: {e}") from e

    async def verify_magic_link(
        self,
        token: str,
        *,
        email: str | None = None,
        user_agent: str | None = None,
        ip_address: str | None = None,
    ) -> AuthResult:
        if self._workos is None:
            raise AuthNotConnectedError()
        if email is None:
            raise ValueError(
                "WorkOS requires email for magic auth verification. "
                "Pass email= to verify_magic_link()."
            )
        try:
            resp = await self._workos.user_management.authenticate_with_magic_auth(
                code=token,
                email=email,
                ip_address=ip_address,
                user_agent=user_agent,
            )
        except workos_exc.AuthenticationException as e:
            raise InvalidTokenError("Magic link is invalid or expired") from e
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(
                f"WorkOS authenticate_with_magic_auth failed: {e}"
            ) from e
        claims = pyjwt.decode(resp.access_token, options={"verify_signature": False})
        user = resp.user
        return AuthResult(
            user=UserInfo(
                id=user.id,
                email=user.email,
                first_name=user.first_name,
                last_name=user.last_name,
                image_url=user.profile_picture_url,
                role=user.metadata.get("role", "default"),
                is_active=user.email_verified,
                is_superuser=user.metadata.get("is_superuser") == "true",
                email_confirmed_at=(
                    datetime.fromisoformat(user.created_at)
                    if user.email_verified
                    else None
                ),
                last_sign_in_at=(
                    datetime.fromisoformat(user.last_sign_in_at)
                    if user.last_sign_in_at
                    else None
                ),
                created_at=datetime.fromisoformat(user.created_at),
                updated_at=datetime.fromisoformat(user.updated_at),
                metadata=dict(user.metadata),
            ),
            tokens=TokenPair(
                access_token=resp.access_token,
                refresh_token=resp.refresh_token,
                expires_at=datetime.fromtimestamp(claims["exp"], tz=UTC),
            ),
        )

    # ------------------------------------------------------------------
    # OAuth
    # ------------------------------------------------------------------

    def get_oauth_authorization_url(
        self,
        provider: str,
        state: str,
        scopes: list[str] | None = None,
        redirect_uri: str | None = None,
    ) -> str:
        if self._workos is None:
            raise AuthNotConnectedError()
        workos_provider = _PROVIDER_MAP.get(str(provider), str(provider))
        kwargs: dict[str, Any] = {
            "provider": workos_provider,
            "redirect_uri": redirect_uri or self._config.redirect_uri or "",
            "state": state,
        }
        if scopes is not None:
            kwargs["provider_scopes"] = scopes
        return self._workos.user_management.get_authorization_url(**kwargs)

    async def sign_in_with_oauth(
        self,
        provider: str,
        code: str,
        *,
        redirect_uri: str | None = None,
        user_agent: str | None = None,
        ip_address: str | None = None,
    ) -> AuthResult:
        if self._workos is None:
            raise AuthNotConnectedError()
        try:
            resp = await self._workos.user_management.authenticate_with_code(
                code=code,
                ip_address=ip_address,
                user_agent=user_agent,
            )
        except workos_exc.AuthenticationException as e:
            raise InvalidCredentialsError("OAuth code rejected") from e
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS authenticate_with_code failed: {e}") from e
        claims = pyjwt.decode(resp.access_token, options={"verify_signature": False})
        user = resp.user
        return AuthResult(
            user=UserInfo(
                id=user.id,
                email=user.email,
                first_name=user.first_name,
                last_name=user.last_name,
                image_url=user.profile_picture_url,
                role=user.metadata.get("role", "default"),
                is_active=user.email_verified,
                is_superuser=user.metadata.get("is_superuser") == "true",
                email_confirmed_at=(
                    datetime.fromisoformat(user.created_at)
                    if user.email_verified
                    else None
                ),
                last_sign_in_at=(
                    datetime.fromisoformat(user.last_sign_in_at)
                    if user.last_sign_in_at
                    else None
                ),
                created_at=datetime.fromisoformat(user.created_at),
                updated_at=datetime.fromisoformat(user.updated_at),
                metadata=dict(user.metadata),
            ),
            tokens=TokenPair(
                access_token=resp.access_token,
                refresh_token=resp.refresh_token,
                expires_at=datetime.fromtimestamp(claims["exp"], tz=UTC),
            ),
        )

    # ------------------------------------------------------------------
    # Tokens
    # ------------------------------------------------------------------

    async def refresh_token(self, refresh_token: str) -> TokenPair:
        if self._workos is None:
            raise AuthNotConnectedError()
        try:
            resp = await self._workos.user_management.authenticate_with_refresh_token(
                refresh_token=refresh_token,
            )
        except (
            workos_exc.AuthenticationException,
            workos_exc.BadRequestException,
        ) as e:
            raise InvalidTokenError("Refresh token is invalid or expired") from e
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(
                f"WorkOS authenticate_with_refresh_token failed: {e}"
            ) from e
        claims = pyjwt.decode(
            resp.access_token,
            options={"verify_signature": False},
        )
        return TokenPair(
            access_token=resp.access_token,
            refresh_token=resp.refresh_token,
            expires_at=datetime.fromtimestamp(claims["exp"], tz=UTC),
        )

    # ------------------------------------------------------------------
    # Organizations
    # ------------------------------------------------------------------

    async def create_org(
        self, *, name: str, slug: str, creator_id: str | uuid.UUID, **kwargs: Any
    ) -> OrgInfo:
        """Create an org on WorkOS and record its local mapping.

        WorkOS is created first so we can capture the workos_org_id for the
        local row. If the local INSERT loses a slug race, we roll back the
        WorkOS org so we never leave it dangling without a local mapping.

        Raises:
            OrgSlugConflictError: Slug is already taken (locally or on WorkOS).
        """
        if self._workos is None:
            raise AuthNotConnectedError()

        try:
            org = await self._workos.organizations.create_organization(
                name=name,
                metadata={"slug": slug},
            )
        except workos_exc.ConflictException as e:
            raise OrgSlugConflictError(slug) from e
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS create_organization failed: {e}") from e

        try:
            local = await (
                self._db()
                .insert(WorkOSOrganization)
                .values(id=org.id, slug=slug)
                .ignore_conflicts(target=WorkOSOrganization.slug)
                .returning(WorkOSOrganization)
                .execute()
            )
            if local is None:
                # Slug claimed by a concurrent request between the WorkOS
                # create and the local insert. Roll back the WorkOS org.
                await self._workos.organizations.delete_organization(
                    organization_id=org.id,
                )
                raise OrgSlugConflictError(slug)

            await self._workos.user_management.create_organization_membership(
                organization_id=org.id,
                user_id=str(creator_id),
                role_slug="owner",
            )
            return OrgInfo(
                id=org.id,
                name=org.name,
                slug=org.metadata.get("slug", ""),
                metadata=dict(org.metadata),
                created_at=datetime.fromisoformat(org.created_at),
                updated_at=datetime.fromisoformat(org.updated_at),
            )
        except OrgSlugConflictError:
            # Already cleaned up above; re-raise without double-deleting.
            raise
        except Exception:
            # Anything past the local insert failed (membership creation,
            # network error). Tear both sides down so callers can retry.
            await self._workos.organizations.delete_organization(
                organization_id=org.id,
            )
            await (
                self._db()
                .delete(WorkOSOrganization)
                .where(WorkOSOrganization.id == org.id)
                .execute()
            )
            raise

    async def get_org(
        self,
        *,
        org_id: str | uuid.UUID | None = None,
        slug: str | None = None,
    ) -> OrgInfo:
        """Look up an org by id (passthrough) or slug (local lookup)."""
        if self._workos is None:
            raise AuthNotConnectedError()
        workos_id = await self._resolve_workos_org_id(org_id=org_id, slug=slug)
        if workos_id is None:
            raise OrgNotFoundError(f"No org with slug {slug!r}")
        try:
            org = await self._workos.organizations.get_organization(
                organization_id=workos_id,
            )
        except workos_exc.NotFoundException as e:
            raise OrgNotFoundError(f"No org with id {workos_id!r}") from e
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS get_organization failed: {e}") from e
        return OrgInfo(
            id=org.id,
            name=org.name,
            slug=org.metadata.get("slug", ""),
            metadata=dict(org.metadata),
            created_at=datetime.fromisoformat(org.created_at),
            updated_at=datetime.fromisoformat(org.updated_at),
        )

    async def update_org(
        self,
        *,
        org_id: str | uuid.UUID | None = None,
        org_slug: str | None = None,
        name: str | None = None,
        slug: str | None = None,
        **kwargs: Any,
    ) -> OrgInfo:
        """Update name and/or slug.

        Identify by ``org_id`` or ``org_slug``; ``slug`` is the new value.

        Raises:
            OrgNotFoundError: Org identifier did not resolve.
            OrgSlugConflictError: New slug is already taken.
        """
        if self._workos is None:
            raise AuthNotConnectedError()
        workos_id = await self._resolve_workos_org_id(org_id=org_id, slug=org_slug)
        if workos_id is None:
            raise OrgNotFoundError(f"No org with slug {org_slug!r}")

        if slug is not None:
            # Local slug update first — fails fast on uniqueness conflict
            # before we touch WorkOS.
            try:
                await (
                    self._db()
                    .update(WorkOSOrganization)
                    .set(slug=slug)
                    .where(WorkOSOrganization.id == workos_id)
                    .execute()
                )
            except Exception as e:
                raise OrgSlugConflictError(slug) from e

        params: dict[str, Any] = {"organization_id": workos_id}
        if name is not None:
            params["name"] = name
        if slug is not None:
            params["metadata"] = {"slug": slug}
        try:
            org = await self._workos.organizations.update_organization(**params)
        except workos_exc.NotFoundException as e:
            raise OrgNotFoundError(f"No org with id {workos_id!r}") from e
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS update_organization failed: {e}") from e

        return OrgInfo(
            id=org.id,
            name=org.name,
            slug=org.metadata.get("slug", ""),
            metadata=dict(org.metadata),
            created_at=datetime.fromisoformat(org.created_at),
            updated_at=datetime.fromisoformat(org.updated_at),
        )

    async def delete_org(
        self,
        *,
        org_id: str | uuid.UUID | None = None,
        slug: str | None = None,
    ) -> bool:
        """Delete on WorkOS + locally. Cleans up local row even if WorkOS 404s.

        Returns ``False`` if the org cannot be found by id or slug.
        """
        if self._workos is None:
            raise AuthNotConnectedError()
        workos_id = await self._resolve_workos_org_id(org_id=org_id, slug=slug)
        if workos_id is None:
            return False
        try:
            await self._workos.organizations.delete_organization(
                organization_id=workos_id,
            )
        except workos_exc.NotFoundException:
            # WorkOS already lost it. Still scrub the local mapping.
            pass
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS delete_organization failed: {e}") from e
        await (
            self._db()
            .delete(WorkOSOrganization)
            .where(WorkOSOrganization.id == workos_id)
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
        """List orgs, scoped to a user's memberships if ``user_id`` is given.

        Slugs come from each org's WorkOS metadata; orgs without a slug
        in metadata surface with an empty slug rather than being filtered.
        """
        if self._workos is None:
            raise AuthNotConnectedError()

        if user_id is None:
            kwargs: dict[str, Any] = {}
            if limit is not None:
                kwargs["limit"] = limit
            try:
                result = await self._workos.organizations.list_organizations(**kwargs)
            except workos_exc.BaseRequestException as e:
                raise AuthBackendError(f"WorkOS list_organizations failed: {e}") from e
            return [
                OrgInfo(
                    id=o.id,
                    name=o.name,
                    slug=o.metadata.get("slug", ""),
                    metadata=dict(o.metadata),
                    created_at=datetime.fromisoformat(o.created_at),
                    updated_at=datetime.fromisoformat(o.updated_at),
                )
                for o in result.data
            ]

        # Memberships only carry org ids, so fetch each org for its
        # name/metadata. One WorkOS round-trip per org.
        try:
            memberships = (
                await self._workos.user_management.list_organization_memberships(
                    user_id=str(user_id),
                )
            )
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(
                f"WorkOS list_organization_memberships failed: {e}"
            ) from e
        out: list[OrgInfo] = []
        for m in memberships.data:
            try:
                org = await self._workos.organizations.get_organization(
                    organization_id=m.organization_id,
                )
            except workos_exc.NotFoundException:
                continue
            except workos_exc.BaseRequestException as e:
                raise AuthBackendError(f"WorkOS get_organization failed: {e}") from e
            out.append(
                OrgInfo(
                    id=org.id,
                    name=org.name,
                    slug=org.metadata.get("slug", ""),
                    metadata=dict(org.metadata),
                    created_at=datetime.fromisoformat(org.created_at),
                    updated_at=datetime.fromisoformat(org.updated_at),
                )
            )
        return out

    # ------------------------------------------------------------------
    # Organization membership
    # ------------------------------------------------------------------

    async def add_org_member(
        self,
        *,
        org_id: str | uuid.UUID | None = None,
        slug: str | None = None,
        user_id: str | uuid.UUID,
        role: str = "member",
    ) -> OrgMemberInfo:
        if self._workos is None:
            raise AuthNotConnectedError()
        workos_id = await self._resolve_workos_org_id(org_id=org_id, slug=slug)
        if workos_id is None:
            raise OrgNotFoundError(f"No org with slug {slug!r}")
        try:
            membership = (
                await self._workos.user_management.create_organization_membership(
                    organization_id=workos_id,
                    user_id=str(user_id),
                    role_slug=role,
                )
            )
        except workos_exc.ConflictException as e:
            raise MemberAlreadyExistsError() from e
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(
                f"WorkOS create_organization_membership failed: {e}"
            ) from e
        return OrgMemberInfo(
            org_id=membership.organization_id,
            user_id=membership.user_id,
            role=membership.role["slug"],
            created_at=datetime.fromisoformat(membership.created_at),
            updated_at=datetime.fromisoformat(membership.updated_at),
        )

    async def update_org_member(
        self,
        *,
        org_id: str | uuid.UUID | None = None,
        slug: str | None = None,
        user_id: str | uuid.UUID,
        role: str,
    ) -> OrgMemberInfo:
        if self._workos is None:
            raise AuthNotConnectedError()
        workos_id = await self._resolve_workos_org_id(org_id=org_id, slug=slug)
        if workos_id is None:
            raise OrgNotFoundError(f"No org with slug {slug!r}")
        try:
            memberships = (
                await self._workos.user_management.list_organization_memberships(
                    organization_id=workos_id,
                    user_id=str(user_id),
                )
            )
            for m in memberships.data:
                if m.user_id == str(user_id):
                    updated = await (
                        self._workos.user_management.update_organization_membership(
                            organization_membership_id=m.id,
                            role_slug=role,
                        )
                    )
                    return OrgMemberInfo(
                        org_id=updated.organization_id,
                        user_id=updated.user_id,
                        role=updated.role["slug"],
                        created_at=datetime.fromisoformat(updated.created_at),
                        updated_at=datetime.fromisoformat(updated.updated_at),
                    )
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS update_org_member failed: {e}") from e
        raise OrgMemberNotFoundError(
            f"User {user_id!r} is not a member of org {workos_id!r}"
        )

    async def remove_org_member(
        self,
        *,
        org_id: str | uuid.UUID | None = None,
        slug: str | None = None,
        user_id: str | uuid.UUID,
    ) -> bool:
        if self._workos is None:
            raise AuthNotConnectedError()
        workos_id = await self._resolve_workos_org_id(org_id=org_id, slug=slug)
        if workos_id is None:
            return False
        try:
            memberships = (
                await self._workos.user_management.list_organization_memberships(
                    organization_id=workos_id,
                    user_id=str(user_id),
                )
            )
            for m in memberships.data:
                if m.user_id == str(user_id):
                    await self._workos.user_management.delete_organization_membership(
                        organization_membership_id=m.id,
                    )
                    return True
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS remove_org_member failed: {e}") from e
        return False

    async def list_org_members(
        self,
        *,
        org_id: str | uuid.UUID | None = None,
        slug: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[OrgMemberInfo]:
        raise NotImplementedError(
            "WorkOS uses cursor-based pagination. "
            "Use list_org_members_by_cursor() instead."
        )

    async def get_org_member(
        self,
        *,
        org_id: str | uuid.UUID | None = None,
        slug: str | None = None,
        user_id: str | uuid.UUID,
    ) -> OrgMemberInfo:
        if self._workos is None:
            raise AuthNotConnectedError()
        workos_id = await self._resolve_workos_org_id(org_id=org_id, slug=slug)
        if workos_id is None:
            raise OrgNotFoundError(f"No org with slug {slug!r}")
        try:
            memberships = (
                await self._workos.user_management.list_organization_memberships(
                    organization_id=workos_id,
                    user_id=str(user_id),
                )
            )
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(
                f"WorkOS list_organization_memberships failed: {e}"
            ) from e
        for m in memberships.data:
            if m.user_id == str(user_id):
                return OrgMemberInfo(
                    org_id=m.organization_id,
                    user_id=m.user_id,
                    role=m.role["slug"],
                    created_at=datetime.fromisoformat(m.created_at),
                    updated_at=datetime.fromisoformat(m.updated_at),
                )
        raise OrgMemberNotFoundError(
            f"User {user_id!r} is not a member of org {workos_id!r}"
        )

    # ------------------------------------------------------------------
    # Cursor-based pagination
    # ------------------------------------------------------------------

    async def list_users_by_cursor(
        self, *, limit: int = 10, after: str | None = None
    ) -> CursorResult[UserInfo]:
        if self._workos is None:
            raise AuthNotConnectedError()
        kwargs: dict[str, Any] = {"limit": limit}
        if after is not None:
            kwargs["after"] = after
        try:
            result = await self._workos.user_management.list_users(**kwargs)
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS list_users failed: {e}") from e
        users = [
            UserInfo(
                id=u.id,
                email=u.email,
                first_name=u.first_name,
                last_name=u.last_name,
                image_url=u.profile_picture_url,
                role=u.metadata.get("role", "default"),
                is_active=u.email_verified,
                is_superuser=u.metadata.get("is_superuser") == "true",
                email_confirmed_at=(
                    datetime.fromisoformat(u.created_at) if u.email_verified else None
                ),
                last_sign_in_at=(
                    datetime.fromisoformat(u.last_sign_in_at)
                    if u.last_sign_in_at
                    else None
                ),
                created_at=datetime.fromisoformat(u.created_at),
                updated_at=datetime.fromisoformat(u.updated_at),
                metadata=dict(u.metadata),
            )
            for u in result.data
        ]
        return self._cursor_result(users, result.list_metadata)

    async def list_sessions_by_cursor(
        self,
        *,
        user_id: str | uuid.UUID,
        limit: int = 10,
        after: str | None = None,
    ) -> CursorResult[Any]:
        if self._workos is None:
            raise AuthNotConnectedError()
        kwargs: dict[str, Any] = {"user_id": str(user_id), "limit": limit}
        if after is not None:
            kwargs["after"] = after
        try:
            result = await self._workos.user_management.list_sessions(**kwargs)
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS list_sessions failed: {e}") from e
        return self._cursor_result(list(result.data), result.list_metadata)

    async def list_orgs_by_cursor(
        self,
        *,
        user_id: str | uuid.UUID | None = None,
        limit: int = 10,
        after: str | None = None,
    ) -> CursorResult[OrgInfo]:
        """Cursor-paginated org list.

        WorkOS drives the cursor; slugs come from each org's metadata.
        Orgs missing a slug in metadata surface with an empty slug.
        """
        if self._workos is None:
            raise AuthNotConnectedError()
        kwargs: dict[str, Any] = {"limit": limit}
        if after is not None:
            kwargs["after"] = after
        try:
            result = await self._workos.organizations.list_organizations(**kwargs)
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS list_organizations failed: {e}") from e
        orgs = [
            OrgInfo(
                id=o.id,
                name=o.name,
                slug=o.metadata.get("slug", ""),
                metadata=dict(o.metadata),
                created_at=datetime.fromisoformat(o.created_at),
                updated_at=datetime.fromisoformat(o.updated_at),
            )
            for o in result.data
        ]
        return self._cursor_result(orgs, result.list_metadata)

    async def list_org_members_by_cursor(
        self,
        *,
        org_id: str | uuid.UUID | None = None,
        slug: str | None = None,
        limit: int = 10,
        after: str | None = None,
    ) -> CursorResult[OrgMemberInfo]:
        if self._workos is None:
            raise AuthNotConnectedError()
        workos_id = await self._resolve_workos_org_id(org_id=org_id, slug=slug)
        if workos_id is None:
            raise OrgNotFoundError(f"No org with slug {slug!r}")
        kwargs: dict[str, Any] = {"organization_id": workos_id, "limit": limit}
        if after is not None:
            kwargs["after"] = after
        try:
            result = await self._workos.user_management.list_organization_memberships(
                **kwargs
            )
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(
                f"WorkOS list_organization_memberships failed: {e}"
            ) from e
        members = [
            OrgMemberInfo(
                org_id=m.organization_id,
                user_id=m.user_id,
                role=m.role["slug"],
                created_at=datetime.fromisoformat(m.created_at),
                updated_at=datetime.fromisoformat(m.updated_at),
            )
            for m in result.data
        ]
        return self._cursor_result(members, result.list_metadata)
