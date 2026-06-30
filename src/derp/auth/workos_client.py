"""WorkOS authentication client."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import jwt as pyjwt
from etils import epy
from jwt import PyJWKClient

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
from derp.auth.models import (
    AuthProvider,
    AuthStatus,
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

    Delegates user management, sign-up, sign-in, organization management, and
    enterprise SSO to the WorkOS API. JWT verification is performed locally
    against the WorkOS JWKS endpoint.

    WorkOS is cursor-native: ``list_users`` / ``list_orgs`` / ``list_members``
    / ``list_sessions`` map the base's opaque ``cursor`` straight onto WorkOS's
    ``after`` cursor and surface ``Page.next_cursor`` from the list metadata.
    """

    supports_password = True
    supports_oauth = True
    supports_sso = True
    supports_magic_link = True
    supports_orgs = True
    supports_sessions = True

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
                "Database client not set. Organization slug lookups require "
                "a database. Call `set_db()` first."
            )
        return self._database_client

    def _client(self) -> workos.AsyncWorkOSClient:
        if self._workos is None:
            raise AuthNotConnectedError()
        return self._workos

    # ------------------------------------------------------------------
    # Org resolution (org_id or slug)
    #
    # The WorkOS org id is the canonical handle everywhere — it's what
    # JWTs carry, what the WorkOS API expects, and what app FKs target.
    # The local ``WorkOSOrganization`` table is purely a slug → id index
    # (its UNIQUE constraint also enforces slug uniqueness across orgs),
    # and its ``id`` column IS the WorkOS org id.
    #
    # Admin methods address an org by exactly one of ``org_id`` / ``slug``.
    # When ``org_id`` is given it IS the WorkOS id — used directly with no
    # DB round-trip. When ``slug`` is given, ``_resolve_slug`` reads the
    # local index (``slug = $1``) and returns the row's ``id`` (the WorkOS
    # org id), or ``None`` when nothing maps that slug.
    #
    # The slug VALUE itself comes from WorkOS org metadata, written by
    # ``create_org`` / ``update_org`` and read straight off the returned
    # org object — no DB hit needed for the id → slug direction.
    # ------------------------------------------------------------------

    async def _resolve_slug(self, slug: str) -> str | None:
        """Resolve *slug* → the WorkOS org id via the local index, or ``None``.

        Reads one row from the local slug index matching ``slug = $1`` and
        returns the row's ``id`` (the WorkOS org id), or ``None`` when no
        local row maps the given slug.
        """
        row = await (
            self._db()
            .select(WorkOSOrganization)
            .where(WorkOSOrganization.slug == slug)
            .first_or_none()
        )
        return row.id if row else None

    async def _workos_id(self, org_id: str | None, slug: str | None) -> str | None:
        """Resolve an ``org_id``/``slug`` reference to a WorkOS org id, or ``None``.

        Callers validate the reference with :meth:`_check_org_ref` first. An
        ``org_id`` is the WorkOS id itself (no DB hit); a ``slug`` is looked up
        in the local index, yielding ``None`` when no row maps it.
        """
        if org_id is not None:
            return org_id
        assert slug is not None
        return await self._resolve_slug(slug)

    # ------------------------------------------------------------------
    # Mappers (single source of truth)
    #
    # WorkOS API objects are mapped to our public dataclasses here rather
    # than inline at every call site. The shape is uniform across get /
    # update / sign-in / list, so one helper kills the duplication.
    # ------------------------------------------------------------------

    def _to_identity(self, user: Any) -> Identity:
        """Build an ``Identity`` from a WorkOS user object.

        WorkOS metadata is ``Dict[str, str]``, so booleans round-trip as
        ``"true"``/``"false"`` strings — compare explicitly. ``disabled``
        mirrors the old ``is_active = email_verified`` rule (inverted): an
        unverified WorkOS user is treated as disabled.
        """
        metadata = dict(user.metadata)
        return Identity(
            id=user.id,
            tenant_id=None,
            email=user.email,
            email_verified=user.email_verified,
            phone=None,
            phone_verified=False,
            is_anonymous=False,
            disabled=not user.email_verified,
            roles=(metadata.get("role", "default"),),
            created_at=datetime.fromisoformat(user.created_at),
            updated_at=datetime.fromisoformat(user.updated_at),
            last_sign_in_at=(
                datetime.fromisoformat(user.last_sign_in_at)
                if user.last_sign_in_at
                else None
            ),
            metadata=metadata,
        )

    def _to_token_set(self, resp: Any) -> TokenSet:
        """Build a ``TokenSet`` from a WorkOS authentication response."""
        claims = pyjwt.decode(resp.access_token, options={"verify_signature": False})
        expires_at = datetime.fromtimestamp(claims["exp"], tz=UTC)
        expires_in = max(int(expires_at.timestamp() - datetime.now(UTC).timestamp()), 0)
        return TokenSet(
            access_token=resp.access_token,
            refresh_token=resp.refresh_token,
            id_token=None,
            token_type="bearer",
            expires_in=expires_in,
            expires_at=expires_at,
        )

    def _to_org(self, org: Any) -> Org:
        """Build an ``Org`` from a WorkOS organization object."""
        return Org(
            id=org.id,
            tenant_id=None,
            name=org.name,
            slug=org.metadata.get("slug", ""),
            created_at=datetime.fromisoformat(org.created_at),
            updated_at=datetime.fromisoformat(org.updated_at),
            metadata=dict(org.metadata),
        )

    def _to_member(self, membership: Any) -> OrgMember:
        """Build an ``OrgMember`` from a WorkOS membership object."""
        return OrgMember(
            org_id=membership.organization_id,
            user_id=membership.user_id,
            role=membership.role["slug"],
            created_at=datetime.fromisoformat(membership.created_at),
            updated_at=datetime.fromisoformat(membership.updated_at),
        )

    def _completed(self, resp: Any) -> AuthOutcome:
        """Build a ``COMPLETE`` outcome (identity + tokens) from an auth response.

        The token decode is the only system-vs-user distinction left in the
        flows: a provider response carrying a malformed access token is a
        backend bug, not a user error, so it raises rather than returning a
        failure outcome.
        """
        try:
            tokens = self._to_token_set(resp)
        except pyjwt.exceptions.InvalidTokenError as e:
            raise InvalidTokenError(f"WorkOS returned a malformed token: {e}") from e
        return AuthOutcome(
            status=AuthStatus.COMPLETE,
            identity=self._to_identity(resp.user),
            tokens=tokens,
        )

    # ------------------------------------------------------------------
    # Authentication / runtime
    # ------------------------------------------------------------------

    async def verify_token(self, token: str) -> Session | None:
        """Verify a WorkOS JWT against the JWKS and return its session."""
        if self._jwks_client is None:
            raise AuthNotConnectedError()

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

        org_id = claims.get("org_id")
        role = claims.get("role", "default")
        return Session(
            user_id=claims["sub"],
            session_id=claims.get("sid", claims["sub"]),
            tenant_id=None,
            org_id=org_id,
            org_role=role if org_id else None,
            roles=(role,),
            scopes=(),
            is_anonymous=False,
            mfa=MFAStatus(enrolled=False, satisfied=False, factor_types=()),
            issued_at=datetime.fromtimestamp(claims["iat"], tz=UTC),
            expires_at=datetime.fromtimestamp(claims["exp"], tz=UTC),
            claims=claims,
        )

    # ------------------------------------------------------------------
    # User management
    # ------------------------------------------------------------------

    async def get_user(self, user_id: str) -> Identity:
        client = self._client()
        try:
            user = await client.user_management.get_user(user_id=str(user_id))
        except workos_exc.NotFoundException as e:
            raise UserNotFoundError(f"User {user_id!r} not found") from e
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS get_user failed: {e}") from e
        return self._to_identity(user)

    async def find_user(
        self,
        *,
        user_id: str | None = None,
        email: str | None = None,
        phone: str | None = None,
    ) -> Identity | None:
        """Find a user by a unique key; ``None`` if no match.

        Provide exactly one of ``user_id`` / ``email`` / ``phone``. WorkOS has
        no phone directory, so a ``phone=`` lookup always resolves to ``None``.
        """
        provided = [k for k in (user_id, email, phone) if k is not None]
        if len(provided) != 1:
            raise ValueError("Provide exactly one of user_id=, email=, or phone=")
        client = self._client()
        if user_id is not None:
            try:
                return await self.get_user(user_id)
            except UserNotFoundError:
                return None
        if phone is not None:
            return None
        try:
            result = await client.user_management.list_users(email=email, limit=1)
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS find_user failed: {e}") from e
        users = list(result.data)
        return self._to_identity(users[0]) if users else None

    async def list_users(
        self, *, limit: int = 50, cursor: str | None = None
    ) -> Page[Identity]:
        """List users using WorkOS's native cursor (``after``)."""
        client = self._client()
        kwargs: dict[str, Any] = {"limit": limit}
        if cursor is not None:
            kwargs["after"] = cursor
        try:
            result = await client.user_management.list_users(**kwargs)
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS list_users failed: {e}") from e
        after = getattr(result.list_metadata, "after", None)
        return Page(
            items=[self._to_identity(u) for u in result.data],
            next_cursor=after,
            has_more=after is not None,
        )

    async def update_user(self, *, user_id: str, **kwargs: Any) -> AuthResult[Identity]:
        client = self._client()
        params: dict[str, Any] = {"user_id": str(user_id)}
        if kwargs.get("email") is not None:
            params["email"] = kwargs.pop("email")
        else:
            kwargs.pop("email", None)
        for field in ("first_name", "last_name"):
            if field in kwargs:
                params[field] = kwargs.pop(field)
        params["metadata"] = kwargs
        try:
            user = await client.user_management.update_user(**params)
        except workos_exc.NotFoundException:
            return AuthResult(error=UserNotFoundError(f"User {user_id!r} not found"))
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS update_user failed: {e}") from e
        return AuthResult(value=self._to_identity(user))

    async def delete_user(self, user_id: str) -> bool:
        client = self._client()
        try:
            await client.user_management.delete_user(user_id=str(user_id))
        except workos_exc.NotFoundException:
            return False
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS delete_user failed: {e}") from e
        return True

    # ------------------------------------------------------------------
    # Sessions
    # ------------------------------------------------------------------

    async def list_sessions(
        self, *, user_id: str | None = None, limit: int = 50, cursor: str | None = None
    ) -> Page[Session]:
        """List a user's active sessions (WorkOS requires a ``user_id``).

        Without a ``user_id`` WorkOS has nothing to enumerate, so this returns
        an empty page.
        """
        client = self._client()
        if user_id is None:
            return Page(items=[], next_cursor=None, has_more=False)
        kwargs: dict[str, Any] = {"user_id": str(user_id), "limit": limit}
        if cursor is not None:
            kwargs["after"] = cursor
        try:
            result = await client.user_management.list_sessions(**kwargs)
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS list_sessions failed: {e}") from e
        sessions = [
            Session(
                user_id=s.user_id,
                session_id=s.id,
                tenant_id=None,
                org_id=getattr(s, "organization_id", None),
                org_role=None,
                roles=(),
                scopes=(),
                is_anonymous=False,
                mfa=MFAStatus(enrolled=False, satisfied=False, factor_types=()),
                issued_at=datetime.fromisoformat(s.created_at),
                expires_at=datetime.fromisoformat(s.expires_at),
                claims={},
            )
            for s in result.data
        ]
        after = getattr(result.list_metadata, "after", None)
        return Page(items=sessions, next_cursor=after, has_more=after is not None)

    async def revoke_session(self, session_id: str) -> AuthResult[bool]:
        client = self._client()
        try:
            await client.user_management.revoke_session(session_id=str(session_id))
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS revoke_session failed: {e}") from e
        return AuthResult(value=True)

    async def revoke_all_sessions(self, user_id: str) -> AuthResult[bool]:
        client = self._client()
        try:
            result = await client.user_management.list_sessions(user_id=str(user_id))
            for session in result.data:
                await client.user_management.revoke_session(session_id=session.id)
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS revoke_all_sessions failed: {e}") from e
        return AuthResult(value=True)

    # ------------------------------------------------------------------
    # Sign-up / sign-in (password)
    # ------------------------------------------------------------------

    async def sign_up(
        self,
        *,
        email: str | None = None,
        phone: str | None = None,
        password: str | None = None,
        **kwargs: Any,
    ) -> AuthOutcome:
        if email is None or password is None:
            raise ValueError("WorkOS sign_up requires both email= and password=")
        client = self._client()
        try:
            await client.user_management.create_user(
                email=email,
                password=password,
                first_name=kwargs.get("first_name"),
                last_name=kwargs.get("last_name"),
            )
        except workos_exc.ConflictException:
            return AuthOutcome(
                status=AuthStatus.EMAIL_EXISTS,
                error=EmailAlreadyExistsError(email),
            )
        # WorkOS sometimes signals duplicate-email as 422 BadRequest depending
        # on settings; treat that as the same conflict to keep one error shape.
        except workos_exc.BadRequestException:
            return AuthOutcome(
                status=AuthStatus.EMAIL_EXISTS,
                error=EmailAlreadyExistsError(email),
            )
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS create_user failed: {e}") from e
        try:
            auth_resp = await client.user_management.authenticate_with_password(
                email=email,
                password=password,
            )
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS post-signup auth failed: {e}") from e
        return self._completed(auth_resp)

    async def sign_in_with_password(
        self, *, identifier: str, password: str
    ) -> AuthOutcome:
        client = self._client()
        try:
            resp = await client.user_management.authenticate_with_password(
                email=identifier,
                password=password,
            )
        except workos_exc.AuthenticationException:
            return AuthOutcome(
                status=AuthStatus.INVALID_CREDENTIALS,
                error=InvalidCredentialsError(),
            )
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(
                f"WorkOS authenticate_with_password failed: {e}"
            ) from e
        return self._completed(resp)

    # ------------------------------------------------------------------
    # Magic link
    # ------------------------------------------------------------------

    async def send_magic_link(self, *, email: str, redirect_url: str) -> None:
        client = self._client()
        try:
            await client.user_management.create_magic_auth(email=email)
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS create_magic_auth failed: {e}") from e

    async def verify_magic_link(
        self, token: str, *, email: str | None = None
    ) -> AuthOutcome:
        client = self._client()
        if email is None:
            raise ValueError(
                "WorkOS requires email for magic auth verification. "
                "Pass email= to verify_magic_link()."
            )
        try:
            resp = await client.user_management.authenticate_with_magic_auth(
                code=token,
                email=email,
            )
        except workos_exc.AuthenticationException:
            return AuthOutcome(
                status=AuthStatus.INVALID_TOKEN,
                error=InvalidTokenError("Magic link is invalid or expired"),
            )
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(
                f"WorkOS authenticate_with_magic_auth failed: {e}"
            ) from e
        return self._completed(resp)

    # ------------------------------------------------------------------
    # Redirect flows: OAuth (provider) + enterprise SSO (organization/connection)
    # ------------------------------------------------------------------

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
        """Build the WorkOS redirect URL for social OAuth or enterprise SSO.

        Pass ``provider`` for social/OIDC; pass ``organization`` /
        ``connection`` / ``domain`` for an SSO connection. WorkOS brokers both
        through one ``get_authorization_url`` call.
        """
        client = self._client()
        kwargs: dict[str, Any] = {
            "redirect_uri": redirect_uri or self._config.redirect_uri or "",
            "state": state,
        }
        if provider is not None:
            name = provider.value if isinstance(provider, AuthProvider) else provider
            kwargs["provider"] = _PROVIDER_MAP.get(name, name)
        if organization is not None:
            kwargs["organization_id"] = organization
        if connection is not None:
            kwargs["connection_id"] = connection
        if domain is not None:
            kwargs["domain_hint"] = domain
        if scopes is not None:
            kwargs["provider_scopes"] = scopes
        return client.user_management.get_authorization_url(**kwargs)

    async def sign_in_with_oauth(
        self,
        code: str,
        *,
        provider: str | AuthProvider | None = None,
        redirect_uri: str | None = None,
    ) -> AuthOutcome:
        """Exchange a WorkOS authorization code for a session."""
        return await self._authenticate_with_code(code, op="sign_in_with_oauth")

    async def sign_in_with_sso(
        self, credential: str, *, redirect_uri: str | None = None
    ) -> AuthOutcome:
        """Complete enterprise SSO. WorkOS returns an org-scoped session.

        WorkOS hands back an authorization ``code`` for the SSO callback, the
        same artifact as social OAuth, so both go through
        ``authenticate_with_code``.
        """
        return await self._authenticate_with_code(credential, op="sign_in_with_sso")

    async def _authenticate_with_code(self, code: str, *, op: str) -> AuthOutcome:
        client = self._client()
        try:
            resp = await client.user_management.authenticate_with_code(code=code)
        except workos_exc.AuthenticationException:
            return AuthOutcome(
                status=AuthStatus.INVALID_CREDENTIALS,
                error=InvalidCredentialsError("Authorization code rejected"),
            )
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS {op} failed: {e}") from e
        return self._completed(resp)

    # ------------------------------------------------------------------
    # Tokens
    # ------------------------------------------------------------------

    async def refresh(self, refresh_token: str) -> AuthResult[TokenSet]:
        client = self._client()
        try:
            resp = await client.user_management.authenticate_with_refresh_token(
                refresh_token=refresh_token,
            )
        except (
            workos_exc.AuthenticationException,
            workos_exc.BadRequestException,
        ):
            return AuthResult(
                error=InvalidTokenError("Refresh token is invalid or expired")
            )
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(
                f"WorkOS authenticate_with_refresh_token failed: {e}"
            ) from e
        return AuthResult(value=self._to_token_set(resp))

    # ------------------------------------------------------------------
    # Organizations
    # ------------------------------------------------------------------

    async def create_org(
        self, *, name: str, slug: str, creator_id: str, **kwargs: Any
    ) -> AuthResult[Org]:
        """Create an org on WorkOS and record its local slug mapping.

        WorkOS is created first so we can capture the org id for the local
        row. If the local INSERT loses a slug race, we roll back the WorkOS
        org so we never leave it dangling without a local mapping.

        On success, ``result.value`` is the new :class:`Org`. Expected failure:
        :class:`OrgSlugConflictError` (slug already taken locally or on WorkOS)
        is attached to ``result.error``.
        """
        client = self._client()
        try:
            org = await client.organizations.create_organization(
                name=name,
                metadata={"slug": slug},
            )
        except workos_exc.ConflictException:
            return AuthResult(error=OrgSlugConflictError(slug))
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
                await client.organizations.delete_organization(organization_id=org.id)
                return AuthResult(error=OrgSlugConflictError(slug))

            await client.user_management.create_organization_membership(
                organization_id=org.id,
                user_id=str(creator_id),
                role_slug="owner",
            )
            return AuthResult(value=self._to_org(org))
        except Exception:
            # Anything past the local insert failed (membership creation,
            # network error). Tear both sides down so callers can retry.
            await client.organizations.delete_organization(organization_id=org.id)
            await (
                self._db()
                .delete(WorkOSOrganization)
                .where(WorkOSOrganization.id == org.id)
                .execute()
            )
            raise

    async def get_org(
        self, *, org_id: str | None = None, slug: str | None = None
    ) -> AuthResult[Org]:
        """Get an org by id or slug (pass exactly one).

        ``org_id`` is the WorkOS id, used directly. ``slug`` is resolved to a
        WorkOS id via the local index.

        Expected failure: :class:`OrgNotFoundError` attached to ``result.error``
        when nothing matches.
        """
        self._check_org_ref(org_id, slug)
        client = self._client()
        workos_id = await self._workos_id(org_id, slug)
        if workos_id is None:
            return AuthResult(
                error=OrgNotFoundError(f"No org matching {org_id or slug!r}")
            )
        try:
            org_obj = await client.organizations.get_organization(
                organization_id=workos_id,
            )
        except workos_exc.NotFoundException:
            return AuthResult(error=OrgNotFoundError(f"No org with id {workos_id!r}"))
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS get_organization failed: {e}") from e
        return AuthResult(value=self._to_org(org_obj))

    async def list_orgs(
        self, *, user_id: str | None = None, limit: int = 50, cursor: str | None = None
    ) -> Page[Org]:
        """List orgs (cursor-paginated), scoped to a member if ``user_id`` given.

        Slugs come from each org's WorkOS metadata; orgs without a slug in
        metadata surface with an empty slug rather than being filtered.
        """
        client = self._client()

        if user_id is None:
            kwargs: dict[str, Any] = {"limit": limit}
            if cursor is not None:
                kwargs["after"] = cursor
            try:
                result = await client.organizations.list_organizations(**kwargs)
            except workos_exc.BaseRequestException as e:
                raise AuthBackendError(f"WorkOS list_organizations failed: {e}") from e
            after = getattr(result.list_metadata, "after", None)
            return Page(
                items=[self._to_org(o) for o in result.data],
                next_cursor=after,
                has_more=after is not None,
            )

        # Memberships only carry org ids, so fetch each org for its
        # name/metadata. WorkOS drives the membership cursor.
        mkwargs: dict[str, Any] = {"user_id": str(user_id), "limit": limit}
        if cursor is not None:
            mkwargs["after"] = cursor
        try:
            memberships = await client.user_management.list_organization_memberships(
                **mkwargs
            )
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(
                f"WorkOS list_organization_memberships failed: {e}"
            ) from e
        out: list[Org] = []
        for m in memberships.data:
            try:
                org = await client.organizations.get_organization(
                    organization_id=m.organization_id,
                )
            except workos_exc.NotFoundException:
                continue
            except workos_exc.BaseRequestException as e:
                raise AuthBackendError(f"WorkOS get_organization failed: {e}") from e
            out.append(self._to_org(org))
        after = getattr(memberships.list_metadata, "after", None)
        return Page(items=out, next_cursor=after, has_more=after is not None)

    async def update_org(
        self,
        *,
        org_id: str | None = None,
        slug: str | None = None,
        name: str | None = None,
        new_slug: str | None = None,
        **kwargs: Any,
    ) -> AuthResult[Org]:
        """Update an org's name and/or slug. Address by ``org_id`` or ``slug`` (one).

        ``name`` / ``new_slug`` are the *new* values to assign (``new_slug`` is
        spelled distinctly from the addressing ``slug``); the local slug-index
        row is updated first so a uniqueness conflict fails fast before touching
        WorkOS.

        Expected failures attached to ``result.error``: :class:`OrgNotFoundError`
        (reference did not resolve) and :class:`OrgSlugConflictError` (new slug
        is already taken).
        """
        self._check_org_ref(org_id, slug)
        client = self._client()
        workos_id = await self._workos_id(org_id, slug)
        if workos_id is None:
            return AuthResult(
                error=OrgNotFoundError(f"No org matching {org_id or slug!r}")
            )

        if new_slug is not None:
            try:
                await (
                    self._db()
                    .update(WorkOSOrganization)
                    .set(slug=new_slug)
                    .where(WorkOSOrganization.id == workos_id)
                    .execute()
                )
            except Exception:
                return AuthResult(error=OrgSlugConflictError(new_slug))

        params: dict[str, Any] = {"organization_id": workos_id}
        if name is not None:
            params["name"] = name
        if new_slug is not None:
            params["metadata"] = {"slug": new_slug}
        try:
            org_obj = await client.organizations.update_organization(**params)
        except workos_exc.NotFoundException:
            return AuthResult(error=OrgNotFoundError(f"No org with id {workos_id!r}"))
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS update_organization failed: {e}") from e
        return AuthResult(value=self._to_org(org_obj))

    async def delete_org(
        self, *, org_id: str | None = None, slug: str | None = None
    ) -> bool:
        """Delete on WorkOS + locally. Address by ``org_id`` or ``slug`` (one).

        Cleans up the local row even on a WorkOS 404. Returns ``False`` if the
        org cannot be found (a given slug maps to no local row).
        """
        self._check_org_ref(org_id, slug)
        client = self._client()
        workos_id = await self._workos_id(org_id, slug)
        if workos_id is None:
            return False
        try:
            await client.organizations.delete_organization(organization_id=workos_id)
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

    # ------------------------------------------------------------------
    # Organization membership
    # ------------------------------------------------------------------

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
        :class:`OrgNotFoundError` (given slug matches no org) and
        :class:`MemberAlreadyExistsError` (already a member).
        """
        self._check_org_ref(org_id, slug)
        client = self._client()
        workos_id = await self._workos_id(org_id, slug)
        if workos_id is None:
            return AuthResult(
                error=OrgNotFoundError(f"No org matching {org_id or slug!r}")
            )
        try:
            membership = await client.user_management.create_organization_membership(
                organization_id=workos_id,
                user_id=str(user_id),
                role_slug=role,
            )
        except workos_exc.ConflictException:
            return AuthResult(error=MemberAlreadyExistsError())
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(
                f"WorkOS create_organization_membership failed: {e}"
            ) from e
        return AuthResult(value=self._to_member(membership))

    async def update_member(
        self,
        *,
        user_id: str,
        role: str,
        org_id: str | None = None,
        slug: str | None = None,
    ) -> AuthResult[OrgMember]:
        """Change a member's role in an org (by id or slug).

        Expected failure attached to ``result.error``:
        :class:`OrgMemberNotFoundError` (not a member, or the org slug matches
        no org).
        """
        self._check_org_ref(org_id, slug)
        client = self._client()
        workos_id = await self._workos_id(org_id, slug)
        if workos_id is None:
            return AuthResult(
                error=OrgMemberNotFoundError(f"No org matching {org_id or slug!r}")
            )
        try:
            memberships = await client.user_management.list_organization_memberships(
                organization_id=workos_id,
                user_id=str(user_id),
            )
            for m in memberships.data:
                if m.user_id == str(user_id):
                    updated = (
                        await client.user_management.update_organization_membership(
                            organization_membership_id=m.id,
                            role_slug=role,
                        )
                    )
                    return AuthResult(value=self._to_member(updated))
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS update_member failed: {e}") from e
        return AuthResult(
            error=OrgMemberNotFoundError(
                f"User {user_id!r} is not a member of org {workos_id!r}"
            )
        )

    async def remove_member(
        self, *, user_id: str, org_id: str | None = None, slug: str | None = None
    ) -> AuthResult[bool]:
        """Remove a member from an org (by id or slug).

        ``result.value`` is ``True`` if a row was removed, ``False`` if the
        user wasn't a member (or a given slug matches no org). WorkOS doesn't
        expose a last-owner constraint here, so :class:`LastOwnerError` isn't
        produced by this backend.
        """
        self._check_org_ref(org_id, slug)
        client = self._client()
        workos_id = await self._workos_id(org_id, slug)
        if workos_id is None:
            return AuthResult(value=False)
        try:
            memberships = await client.user_management.list_organization_memberships(
                organization_id=workos_id,
                user_id=str(user_id),
            )
            for m in memberships.data:
                if m.user_id == str(user_id):
                    await client.user_management.delete_organization_membership(
                        organization_membership_id=m.id,
                    )
                    return AuthResult(value=True)
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(f"WorkOS remove_member failed: {e}") from e
        return AuthResult(value=False)

    async def list_members(
        self,
        *,
        org_id: str | None = None,
        slug: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> Page[OrgMember]:
        """List members of an org (by id or slug) using WorkOS's native cursor.

        Raises:
            OrgNotFoundError: A given slug matches no org.
        """
        self._check_org_ref(org_id, slug)
        client = self._client()
        workos_id = await self._workos_id(org_id, slug)
        if workos_id is None:
            raise OrgNotFoundError(f"No org matching {org_id or slug!r}")
        kwargs: dict[str, Any] = {"organization_id": workos_id, "limit": limit}
        if cursor is not None:
            kwargs["after"] = cursor
        try:
            result = await client.user_management.list_organization_memberships(
                **kwargs
            )
        except workos_exc.BaseRequestException as e:
            raise AuthBackendError(
                f"WorkOS list_organization_memberships failed: {e}"
            ) from e
        after = getattr(result.list_metadata, "after", None)
        return Page(
            items=[self._to_member(m) for m in result.data],
            next_cursor=after,
            has_more=after is not None,
        )
