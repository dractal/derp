"""Clerk authentication client."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from etils import epy

from derp.auth.base import BaseAuthClient
from derp.auth.models import AuthRequest, OrgInfo, OrgMemberInfo, SessionInfo, UserInfo
from derp.config import ClerkConfig

with epy.lazy_imports():
    import clerk_backend_api
    import clerk_backend_api.models.user as clerk_user_models
    from clerk_backend_api import security as clerk_security
    from clerk_backend_api.security import types as clerk_security_types


class ClerkAuthClient(BaseAuthClient):
    """Clerk-backed authentication client.

    Delegates authentication to Clerk's Backend API. Sign-up and sign-in
    are handled by Clerk's frontend SDK / hosted UI; this client provides
    server-side session validation, user lookup, and authorization.

    JWT verification is networkless — uses the PEM public key from
    ``ClerkConfig.jwt_key`` (Clerk Dashboard > API Keys > Show JWT public key).
    """

    def __init__(self, config: ClerkConfig):
        self._config = config
        self._clerk = clerk_backend_api.Clerk(bearer_auth=config.secret_key)

    def _to_user_info(self, user: Any) -> UserInfo:
        """Convert a Clerk SDK user object to UserInfo."""
        assert isinstance(user, clerk_user_models.User)
        email = ""
        if user.email_addresses:
            primary = next(
                (
                    e
                    for e in user.email_addresses
                    if e.id == user.primary_email_address_id
                ),
                user.email_addresses[0],
            )
            email = primary.email_address

        public_metadata = user.public_metadata if user.public_metadata else {}

        return UserInfo(
            id=user.id,
            email=email,
            first_name=user.first_name,
            last_name=user.last_name,
            username=user.username if user.username else None,
            image_url=user.image_url,
            role=public_metadata.get("role", "default"),
            is_active=not user.banned and not user.locked,
            is_superuser=public_metadata.get("is_superuser", False),
            created_at=datetime.fromtimestamp(user.created_at),
            updated_at=datetime.fromtimestamp(user.updated_at),
            last_sign_in_at=(
                datetime.fromtimestamp(user.last_sign_in_at)
                if user.last_sign_in_at
                else None
            ),
            email_confirmed_at=None,
            metadata={
                "public_metadata": public_metadata,
                "private_metadata": (
                    user.private_metadata if user.private_metadata else {}
                ),
                "banned": user.banned,
                "locked": user.locked,
            },
        )

    # ------------------------------------------------------------------
    # User management
    # ------------------------------------------------------------------

    async def get_user(self, user_id: str | uuid.UUID) -> UserInfo | None:
        """Get a user by their Clerk user ID."""
        user = await self._clerk.users.get_async(user_id=str(user_id))
        return self._to_user_info(user)

    async def list_users(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> list[UserInfo]:
        """List Clerk users ordered by creation date (newest first)."""
        kwargs: dict[str, Any] = {"order_by": "-created_at"}
        if limit is not None:
            kwargs["limit"] = limit
        if offset is not None:
            kwargs["offset"] = offset
        users = await self._clerk.users.list_async(**kwargs)
        return [self._to_user_info(u) for u in users]

    async def update_user(
        self,
        *,
        user_id: str | uuid.UUID,
        email: str | None = None,
        **kwargs: Any,
    ) -> UserInfo:
        """Update a Clerk user.

        Supports ``first_name``, ``last_name``, ``username`` as direct kwargs.
        All other kwargs are merged into ``public_metadata``.
        """
        if email is not None:
            raise NotImplementedError(
                "Email updates are not supported via update_user for Clerk. "
                "Use Clerk's email_addresses API instead."
            )

        params: dict[str, Any] = {}

        for field_name in ("first_name", "last_name", "username"):
            if field_name in kwargs:
                params[field_name] = kwargs.pop(field_name)

        if kwargs:
            params["public_metadata"] = kwargs

        user = await self._clerk.users.update_async(
            user_id=str(user_id),
            **params,
        )
        return self._to_user_info(user)

    async def delete_user(self, user_id: str | uuid.UUID) -> bool:
        """Delete a Clerk user."""
        result = await self._clerk.users.delete_async(user_id=str(user_id))
        return result.deleted

    async def count_users(self) -> int:
        """Return the total number of Clerk users."""
        return await self._clerk.users.count_async()

    # ------------------------------------------------------------------
    # Session validation
    # ------------------------------------------------------------------

    async def authenticate(self, request: AuthRequest) -> SessionInfo | None:
        """Authenticate a request via Clerk (networkless).

        Uses the Clerk SDK's ``authenticate_request`` with the PEM
        public key from ``ClerkConfig.jwt_key``. Returns ``SessionInfo``
        if valid, ``None`` otherwise.
        """
        options = clerk_security.AuthenticateRequestOptions(
            jwt_key=self._config.jwt_key,
            authorized_parties=(
                list(self._config.authorized_parties)
                if self._config.authorized_parties
                else None
            ),
        )
        result = clerk_security.authenticate_request(request, options)
        if not result.is_signed_in or result.payload is None:
            return None

        auth = result.to_auth()
        if isinstance(auth, clerk_security_types.SessionAuthObjectV2):
            return SessionInfo(
                user_id=auth.sub or "",
                session_id=auth.sid or "",
                role=auth.org_role or auth.role or "default",
                expires_at=(
                    datetime.fromtimestamp(auth.exp, tz=UTC)
                    if auth.exp is not None
                    else datetime.now(UTC)
                ),
                metadata=result.payload or {},
                org_id=getattr(auth, "org_id", None),
                org_role=auth.org_role or None,
            )
        elif isinstance(auth, clerk_security_types.SessionAuthObjectV1):
            claims = auth.claims if auth.claims is not None else {}
            return SessionInfo(
                user_id=auth.user_id or "",
                session_id=auth.session_id or "",
                role=auth.org_role or claims.get("role", "default"),
                expires_at=(
                    datetime.fromtimestamp(claims["exp"], tz=UTC)
                    if "exp" in claims
                    else datetime.now(UTC)
                ),
                metadata=result.payload or {},
                org_id=getattr(auth, "org_id", None),
                org_role=auth.org_role or None,
            )
        raise ValueError(f"Unknown auth object type: {type(auth)}")

    async def list_sessions(
        self,
        *,
        user_id: str | uuid.UUID | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[Any]:
        """List active Clerk sessions."""
        kwargs: dict[str, Any] = {"status": "active"}
        if user_id is not None:
            kwargs["user_id"] = str(user_id)
        if limit is not None:
            kwargs["limit"] = limit
        if offset is not None:
            kwargs["offset"] = offset
        return await self._clerk.sessions.list_async(**kwargs)

    # ------------------------------------------------------------------
    # Sign-out
    # ------------------------------------------------------------------

    async def sign_out(self, session_id: str | uuid.UUID) -> None:
        """Revoke a Clerk session."""
        await self._clerk.sessions.revoke_async(session_id=str(session_id))

    async def sign_out_all(self, user_id: str | uuid.UUID) -> None:
        """Revoke all active sessions for a user."""
        sessions = await self._clerk.sessions.list_async(
            user_id=str(user_id), status="active"
        )
        if sessions:
            for session in sessions:
                await self._clerk.sessions.revoke_async(session_id=session.id)

    # ------------------------------------------------------------------
    # Organizations
    # ------------------------------------------------------------------

    def _to_org_info(self, org: Any) -> OrgInfo:
        """Convert a Clerk SDK organization object to OrgInfo."""
        return OrgInfo(
            id=org.id,
            name=org.name,
            slug=org.slug or "",
            metadata=org.public_metadata if org.public_metadata else {},
            created_at=datetime.fromtimestamp(org.created_at),
            updated_at=datetime.fromtimestamp(org.updated_at),
        )

    def _to_org_member_info(self, member: Any) -> OrgMemberInfo:
        """Convert a Clerk SDK organization membership to OrgMemberInfo."""
        return OrgMemberInfo(
            org_id=member.organization.id if member.organization else "",
            user_id=member.public_user_data.user_id if member.public_user_data else "",
            role=member.role or "member",
            created_at=datetime.fromtimestamp(member.created_at),
            updated_at=datetime.fromtimestamp(member.updated_at),
        )

    async def create_org(
        self,
        *,
        name: str,
        slug: str,
        creator_id: str | uuid.UUID,
        **kwargs: Any,
    ) -> OrgInfo:
        """Create a Clerk organization."""
        org = await self._clerk.organizations.create_async(
            request={
                "name": name,
                "slug": slug,
                "created_by": str(creator_id),
            },
        )
        return self._to_org_info(org)

    async def get_org(self, org_id: str | uuid.UUID) -> OrgInfo | None:
        """Get a Clerk organization by ID."""
        org = await self._clerk.organizations.get_async(
            organization_id=str(org_id),
        )
        return self._to_org_info(org) if org else None

    async def get_org_by_slug(self, slug: str) -> OrgInfo | None:
        """Get a Clerk organization by slug."""
        orgs = await self._clerk.organizations.list_async(query=slug, limit=1)
        for org in orgs:
            if org.slug == slug:
                return self._to_org_info(org)
        return None

    async def update_org(
        self,
        *,
        org_id: str | uuid.UUID,
        name: str | None = None,
        slug: str | None = None,
        **kwargs: Any,
    ) -> OrgInfo:
        """Update a Clerk organization."""
        params: dict[str, Any] = {}
        if name is not None:
            params["name"] = name
        if slug is not None:
            params["slug"] = slug
        org = await self._clerk.organizations.update_async(
            organization_id=str(org_id),
            **params,
        )
        return self._to_org_info(org)

    async def delete_org(self, org_id: str | uuid.UUID) -> bool:
        """Delete a Clerk organization."""
        result = await self._clerk.organizations.delete_async(
            organization_id=str(org_id),
        )
        return result.deleted

    async def list_orgs(
        self,
        *,
        user_id: str | uuid.UUID | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[OrgInfo]:
        """List Clerk organizations."""
        kwargs: dict[str, Any] = {}
        if user_id is not None:
            kwargs["user_id"] = [str(user_id)]
        if limit is not None:
            kwargs["limit"] = limit
        if offset is not None:
            kwargs["offset"] = offset
        orgs = await self._clerk.organizations.list_async(**kwargs)
        return [self._to_org_info(o) for o in orgs]

    # ------------------------------------------------------------------
    # Organization membership
    # ------------------------------------------------------------------

    async def add_org_member(
        self,
        *,
        org_id: str | uuid.UUID,
        user_id: str | uuid.UUID,
        role: str = "member",
    ) -> OrgMemberInfo:
        """Add a user to a Clerk organization."""
        member = await self._clerk.organization_memberships.create_async(
            organization_id=str(org_id),
            user_id=str(user_id),
            role=role,
        )
        return self._to_org_member_info(member)

    async def update_org_member(
        self,
        *,
        org_id: str | uuid.UUID,
        user_id: str | uuid.UUID,
        role: str,
    ) -> OrgMemberInfo:
        """Update a member's role in a Clerk organization."""
        member = await self._clerk.organization_memberships.update_async(
            organization_id=str(org_id),
            user_id=str(user_id),
            role=role,
        )
        return self._to_org_member_info(member)

    async def remove_org_member(
        self,
        *,
        org_id: str | uuid.UUID,
        user_id: str | uuid.UUID,
    ) -> bool:
        """Remove a user from a Clerk organization."""
        await self._clerk.organization_memberships.delete_async(
            organization_id=str(org_id),
            user_id=str(user_id),
        )
        return True

    async def list_org_members(
        self,
        org_id: str | uuid.UUID,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[OrgMemberInfo]:
        """List members of a Clerk organization."""
        kwargs: dict[str, Any] = {}
        if limit is not None:
            kwargs["limit"] = limit
        if offset is not None:
            kwargs["offset"] = offset
        members = await self._clerk.organization_memberships.list_async(
            organization_id=str(org_id),
            **kwargs,
        )
        return [self._to_org_member_info(m) for m in members]

    async def get_org_member(
        self,
        *,
        org_id: str | uuid.UUID,
        user_id: str | uuid.UUID,
    ) -> OrgMemberInfo | None:
        """Get a single membership from a Clerk organization."""
        members = await self._clerk.organization_memberships.list_async(
            organization_id=str(org_id),
        )
        for m in members:
            uid = m.public_user_data.user_id if m.public_user_data else None
            if uid == str(user_id):
                return self._to_org_member_info(m)
        return None
