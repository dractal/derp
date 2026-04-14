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
from derp.auth.exceptions import AuthNotConnectedError
from derp.auth.jwt import TokenPair
from derp.auth.models import (
    AuthRequest,
    AuthResult,
    CursorResult,
    OrgInfo,
    OrgMemberInfo,
    SessionInfo,
    UserInfo,
)
from derp.config import WorkOSConfig

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

    # ------------------------------------------------------------------
    # Mappers
    # ------------------------------------------------------------------

    def _to_user_info(self, user: Any) -> UserInfo:
        """Convert a WorkOS User object to UserInfo."""
        metadata = dict(user.metadata) if user.metadata else {}
        return UserInfo(
            id=user.id,
            email=user.email,
            first_name=user.first_name,
            last_name=user.last_name,
            image_url=user.profile_picture_url,
            role=metadata.get("role", "default"),
            is_active=user.email_verified,
            is_superuser=metadata.get("is_superuser", False),
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
            metadata=metadata,
        )

    def _to_org_info(self, org: Any) -> OrgInfo:
        """Convert a WorkOS Organization object to OrgInfo."""
        metadata = dict(org.metadata) if org.metadata else {}
        return OrgInfo(
            id=org.id,
            name=org.name,
            slug=metadata.get("slug", ""),
            metadata=metadata,
            created_at=datetime.fromisoformat(org.created_at),
            updated_at=datetime.fromisoformat(org.updated_at),
        )

    def _to_org_member_info(self, membership: Any) -> OrgMemberInfo:
        """Convert a WorkOS OrganizationMembership to OrgMemberInfo."""
        role = membership.role
        if isinstance(role, dict):
            role = role.get("slug", "member")
        return OrgMemberInfo(
            org_id=membership.organization_id,
            user_id=membership.user_id,
            role=role,
            created_at=datetime.fromisoformat(membership.created_at),
            updated_at=datetime.fromisoformat(membership.updated_at),
        )

    def _to_auth_result(self, resp: Any) -> AuthResult:
        """Convert a WorkOS AuthenticationResponse to AuthResult."""
        claims = pyjwt.decode(
            resp.access_token,
            options={"verify_signature": False},
        )
        return AuthResult(
            user=self._to_user_info(resp.user),
            tokens=TokenPair(
                access_token=resp.access_token,
                refresh_token=resp.refresh_token,
                expires_at=datetime.fromtimestamp(claims["exp"], tz=UTC),
            ),
        )

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

    async def get_user(self, user_id: str | uuid.UUID) -> UserInfo | None:
        if self._workos is None:
            raise AuthNotConnectedError()
        try:
            user = await self._workos.user_management.get_user(user_id=str(user_id))
            return self._to_user_info(user)
        except workos_exc.NotFoundException:
            return None

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
    ) -> UserInfo | None:
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
        except workos_exc.NotFoundException:
            return None
        return self._to_user_info(user)

    async def delete_user(self, user_id: str | uuid.UUID) -> bool:
        if self._workos is None:
            raise AuthNotConnectedError()
        try:
            await self._workos.user_management.delete_user(user_id=str(user_id))
        except workos_exc.NotFoundException:
            return False
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
    ) -> list[Any]:
        if self._workos is None:
            raise AuthNotConnectedError()
        if user_id is None:
            return []
        kwargs: dict[str, Any] = {"user_id": str(user_id)}
        if limit is not None:
            kwargs["limit"] = limit
        result = await self._workos.user_management.list_sessions(**kwargs)
        return list(result.data)

    async def sign_out(self, session_id: str | uuid.UUID) -> None:
        if self._workos is None:
            raise AuthNotConnectedError()
        await self._workos.user_management.revoke_session(session_id=str(session_id))

    async def sign_out_all(self, user_id: str | uuid.UUID) -> None:
        if self._workos is None:
            raise AuthNotConnectedError()
        result = await self._workos.user_management.list_sessions(user_id=str(user_id))
        for session in result.data:
            await self._workos.user_management.revoke_session(session_id=session.id)

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
    ) -> AuthResult | None:
        if self._workos is None:
            raise AuthNotConnectedError()
        try:
            await self._workos.user_management.create_user(
                email=email,
                password=password,
                first_name=kwargs.get("first_name"),
                last_name=kwargs.get("last_name"),
            )
            auth_resp = await self._workos.user_management.authenticate_with_password(
                email=email,
                password=password,
                ip_address=ip_address,
                user_agent=user_agent,
            )
            return self._to_auth_result(auth_resp)
        except (workos_exc.ConflictException, workos_exc.BadRequestException):
            return None

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
        if self._workos is None:
            raise AuthNotConnectedError()
        try:
            resp = await self._workos.user_management.authenticate_with_password(
                email=email,
                password=password,
                ip_address=ip_address,
                user_agent=user_agent,
            )
            return self._to_auth_result(resp)
        except workos_exc.AuthenticationException:
            return None

    async def sign_in_with_magic_link(self, *, email: str, magic_link_url: str) -> None:
        if self._workos is None:
            raise AuthNotConnectedError()
        await self._workos.user_management.create_magic_auth(email=email)

    async def verify_magic_link(
        self,
        token: str,
        *,
        email: str | None = None,
        user_agent: str | None = None,
        ip_address: str | None = None,
    ) -> AuthResult | None:
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
            return self._to_auth_result(resp)
        except workos_exc.AuthenticationException:
            return None

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
    ) -> AuthResult | None:
        if self._workos is None:
            raise AuthNotConnectedError()
        try:
            resp = await self._workos.user_management.authenticate_with_code(
                code=code,
                ip_address=ip_address,
                user_agent=user_agent,
            )
            return self._to_auth_result(resp)
        except workos_exc.AuthenticationException:
            return None

    # ------------------------------------------------------------------
    # Tokens
    # ------------------------------------------------------------------

    async def refresh_token(self, refresh_token: str) -> TokenPair | None:
        if self._workos is None:
            raise AuthNotConnectedError()
        try:
            resp = await self._workos.user_management.authenticate_with_refresh_token(
                refresh_token=refresh_token,
            )
        except (
            workos_exc.AuthenticationException,
            workos_exc.BadRequestException,
        ):
            return None
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
    ) -> OrgInfo | None:
        if self._workos is None:
            raise AuthNotConnectedError()
        try:
            org = await self._workos.organizations.create_organization(
                name=name,
                metadata={"slug": slug},
            )
            await self._workos.user_management.create_organization_membership(
                organization_id=org.id,
                user_id=str(creator_id),
                role_slug="owner",
            )
            return self._to_org_info(org)
        except workos_exc.ConflictException:
            return None

    async def get_org(self, org_id: str | uuid.UUID) -> OrgInfo | None:
        if self._workos is None:
            raise AuthNotConnectedError()
        try:
            org = await self._workos.organizations.get_organization(
                organization_id=str(org_id),
            )
            return self._to_org_info(org)
        except workos_exc.NotFoundException:
            return None

    async def get_org_by_slug(self, slug: str) -> OrgInfo | None:
        if self._workos is None:
            raise AuthNotConnectedError()
        after: str | None = None
        while True:
            kwargs: dict[str, Any] = {"limit": 100}
            if after is not None:
                kwargs["after"] = after
            result = await self._workos.organizations.list_organizations(**kwargs)
            for org in result.data:
                info = self._to_org_info(org)
                if info.slug == slug:
                    return info
            after = getattr(result.list_metadata, "after", None)
            if after is None:
                return None

    async def update_org(
        self,
        *,
        org_id: str | uuid.UUID,
        name: str | None = None,
        slug: str | None = None,
        **kwargs: Any,
    ) -> OrgInfo | None:
        if self._workos is None:
            raise AuthNotConnectedError()
        try:
            params: dict[str, Any] = {"organization_id": str(org_id)}
            if name is not None:
                params["name"] = name
            if slug is not None:
                params["metadata"] = {"slug": slug}
            org = await self._workos.organizations.update_organization(**params)
            return self._to_org_info(org)
        except workos_exc.NotFoundException:
            return None

    async def delete_org(self, org_id: str | uuid.UUID) -> bool:
        if self._workos is None:
            raise AuthNotConnectedError()
        try:
            await self._workos.organizations.delete_organization(
                organization_id=str(org_id),
            )
            return True
        except workos_exc.NotFoundException:
            return False

    async def list_orgs(
        self,
        *,
        user_id: str | uuid.UUID | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[OrgInfo]:
        if self._workos is None:
            raise AuthNotConnectedError()
        if user_id is not None:
            memberships = (
                await self._workos.user_management.list_organization_memberships(
                    user_id=str(user_id),
                )
            )
            orgs = []
            for m in memberships.data:
                org = await self.get_org(m.organization_id)
                if org is not None:
                    orgs.append(org)
            return orgs
        kwargs: dict[str, Any] = {}
        if limit is not None:
            kwargs["limit"] = limit
        result = await self._workos.organizations.list_organizations(**kwargs)
        return [self._to_org_info(o) for o in result.data]

    # ------------------------------------------------------------------
    # Organization membership
    # ------------------------------------------------------------------

    async def add_org_member(
        self,
        *,
        org_id: str | uuid.UUID,
        user_id: str | uuid.UUID,
        role: str = "member",
    ) -> OrgMemberInfo | None:
        if self._workos is None:
            raise AuthNotConnectedError()
        try:
            membership = (
                await self._workos.user_management.create_organization_membership(
                    organization_id=str(org_id),
                    user_id=str(user_id),
                    role_slug=role,
                )
            )
            return self._to_org_member_info(membership)
        except workos_exc.ConflictException:
            return None

    async def update_org_member(
        self,
        *,
        org_id: str | uuid.UUID,
        user_id: str | uuid.UUID,
        role: str,
    ) -> OrgMemberInfo | None:
        if self._workos is None:
            raise AuthNotConnectedError()
        memberships = await self._workos.user_management.list_organization_memberships(
            organization_id=str(org_id),
            user_id=str(user_id),
        )
        for m in memberships.data:
            if m.user_id == str(user_id):
                updated = (
                    await self._workos.user_management.update_organization_membership(
                        organization_membership_id=m.id,
                        role_slug=role,
                    )
                )
                return self._to_org_member_info(updated)
        return None

    async def remove_org_member(
        self,
        *,
        org_id: str | uuid.UUID,
        user_id: str | uuid.UUID,
    ) -> bool:
        if self._workos is None:
            raise AuthNotConnectedError()
        memberships = await self._workos.user_management.list_organization_memberships(
            organization_id=str(org_id),
            user_id=str(user_id),
        )
        for m in memberships.data:
            if m.user_id == str(user_id):
                await self._workos.user_management.delete_organization_membership(
                    organization_membership_id=m.id,
                )
                return True
        return False

    async def list_org_members(
        self,
        org_id: str | uuid.UUID,
        *,
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
        org_id: str | uuid.UUID,
        user_id: str | uuid.UUID,
    ) -> OrgMemberInfo | None:
        if self._workos is None:
            raise AuthNotConnectedError()
        memberships = await self._workos.user_management.list_organization_memberships(
            organization_id=str(org_id),
            user_id=str(user_id),
        )
        for m in memberships.data:
            if m.user_id == str(user_id):
                return self._to_org_member_info(m)
        return None

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
        result = await self._workos.user_management.list_users(**kwargs)
        users = [self._to_user_info(u) for u in result.data]
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
        result = await self._workos.user_management.list_sessions(**kwargs)
        return self._cursor_result(list(result.data), result.list_metadata)

    async def list_orgs_by_cursor(
        self,
        *,
        user_id: str | uuid.UUID | None = None,
        limit: int = 10,
        after: str | None = None,
    ) -> CursorResult[OrgInfo]:
        if self._workos is None:
            raise AuthNotConnectedError()
        kwargs: dict[str, Any] = {"limit": limit}
        if after is not None:
            kwargs["after"] = after
        result = await self._workos.organizations.list_organizations(**kwargs)
        orgs = [self._to_org_info(o) for o in result.data]
        return self._cursor_result(orgs, result.list_metadata)

    async def list_org_members_by_cursor(
        self,
        org_id: str | uuid.UUID,
        *,
        limit: int = 10,
        after: str | None = None,
    ) -> CursorResult[OrgMemberInfo]:
        if self._workos is None:
            raise AuthNotConnectedError()
        kwargs: dict[str, Any] = {"organization_id": str(org_id), "limit": limit}
        if after is not None:
            kwargs["after"] = after
        result = await self._workos.user_management.list_organization_memberships(
            **kwargs
        )
        members = [self._to_org_member_info(m) for m in result.data]
        return self._cursor_result(members, result.list_metadata)
