"""Supabase GoTrue authentication client."""

from __future__ import annotations

import dataclasses
import hashlib
import hmac
import json
import logging
from datetime import UTC, datetime
from typing import Any

import httpx
import jwt as pyjwt

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
    LastOwnerError,
    MemberAlreadyExistsError,
    OrgMemberNotFoundError,
    OrgNotFoundError,
    OrgSlugConflictError,
    UserNotFoundError,
)
from derp.auth.models import (
    AuthOrganization,
    AuthProvider,
    AuthRequest,
    AuthStatus,
    SupabaseOrgMember,
)
from derp.config import SupabaseConfig
from derp.orm import DatabaseEngine

logger = logging.getLogger(__name__)


class SupabaseAuthClient(BaseAuthClient):
    """Supabase GoTrue-backed authentication client.

    Delegates user management, sign-up, sign-in, and token operations
    to the Supabase GoTrue REST API via raw httpx calls. JWT verification
    is performed locally using the project's JWT secret. Organizations
    are stored in the local database.
    """

    supports_password = True
    supports_oauth = True
    supports_magic_link = True
    supports_orgs = True
    supports_user_admin = True

    def __init__(self, config: SupabaseConfig) -> None:
        self._config = config
        self._base_url = f"{config.url.rstrip('/')}/auth/v1"
        self._http: httpx.AsyncClient | None = None
        self._database_client: DatabaseEngine | None = None

    # -- Lifecycle ---------------------------------------------------------------

    async def connect(self) -> None:
        if self._http is not None:
            return
        self._http = httpx.AsyncClient(
            base_url=self._base_url,
            headers={
                "apikey": self._config.anon_key,
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )

    async def disconnect(self) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    def set_db(self, db: DatabaseEngine | None) -> None:
        self._database_client = db

    def _db(self) -> DatabaseEngine:
        if self._database_client is None:
            raise ValueError(
                "Database client not set. Organization methods require "
                "a database. Call `set_db()` first."
            )
        return self._database_client

    def _ensure_http(self) -> httpx.AsyncClient:
        if self._http is None:
            raise AuthNotConnectedError()
        return self._http

    def _admin_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._config.service_role_key}"}

    def _gotrue_error_code(self, resp: httpx.Response) -> str | None:
        """Best-effort extraction of GoTrue's `error_code` JSON field."""
        try:
            body = resp.json()
        except (ValueError, json.JSONDecodeError):
            return None
        if isinstance(body, dict):
            code = body.get("error_code") or body.get("code")
            return str(code) if code is not None else None
        return None

    def _backend_error(self, op: str, resp: httpx.Response) -> AuthBackendError:
        """Wrap a non-success GoTrue response in AuthBackendError.

        Includes the status code and the raw body (truncated) so ops can
        diagnose without re-running the request.
        """
        body = resp.text[:500]
        return AuthBackendError(f"Supabase {op} failed: HTTP {resp.status_code} {body}")

    # -- Response mapping --------------------------------------------------------

    def _to_identity(self, data: dict[str, Any]) -> Identity:
        user_meta = data.get("user_metadata") or {}
        app_meta = data.get("app_metadata") or {}

        role = app_meta.get("role", data.get("role", "authenticated"))

        banned_until = data.get("banned_until")
        is_active = banned_until is None

        email_confirmed_at = data.get("email_confirmed_at")
        if isinstance(email_confirmed_at, str):
            email_confirmed_at = datetime.fromisoformat(email_confirmed_at)

        last_sign_in_at = data.get("last_sign_in_at")
        if isinstance(last_sign_in_at, str):
            last_sign_in_at = datetime.fromisoformat(last_sign_in_at)

        created_at = data.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        else:
            created_at = created_at or datetime.now(UTC)

        updated_at = data.get("updated_at")
        if isinstance(updated_at, str):
            updated_at = datetime.fromisoformat(updated_at)
        else:
            updated_at = updated_at or datetime.now(UTC)

        # Remaining user_metadata goes into metadata (exclude consumed keys).
        _consumed = {"first_name", "last_name", "avatar_url"}
        metadata = {k: v for k, v in user_meta.items() if k not in _consumed}

        return Identity(
            id=data.get("id", ""),
            tenant_id=None,
            email=data.get("email") or None,
            email_verified=email_confirmed_at is not None,
            phone=None,
            phone_verified=False,
            is_anonymous=False,
            disabled=not is_active,
            roles=(role,),
            created_at=created_at,
            updated_at=updated_at,
            last_sign_in_at=last_sign_in_at,
            metadata=metadata,
        )

    def _to_token_set(self, data: dict[str, Any]) -> TokenSet:
        expires_in = data.get("expires_in", 3600)
        expires_at_ts = data.get("expires_at")
        if expires_at_ts:
            expires_at = datetime.fromtimestamp(int(expires_at_ts), tz=UTC)
        else:
            expires_at = datetime.now(UTC)
        access_token = data.get("access_token", "")
        return TokenSet(
            access_token=access_token,
            refresh_token=data.get("refresh_token") or None,
            id_token=access_token or None,
            token_type="bearer",
            expires_in=expires_in,
            expires_at=expires_at,
        )

    def _to_outcome(self, data: dict[str, Any]) -> AuthOutcome:
        return AuthOutcome(
            status=AuthStatus.COMPLETE,
            identity=self._to_identity(data["user"]),
            tokens=self._to_token_set(data),
        )

    # -- Org context signing (HMAC) ----------------------------------------------

    def _sign_org_context(self, user_id: str, org_id: str, org_role: str) -> str:
        key = self._config.jwt_secret.encode()
        msg = f"{user_id}:{org_id}:{org_role}".encode()
        sig = hmac.new(key, msg, hashlib.sha256).hexdigest()
        return f"{org_id}:{org_role}:{sig}"

    def _verify_org_context(self, user_id: str, header: str) -> tuple[str, str] | None:
        parts = header.split(":", 2)
        if len(parts) != 3:
            return None
        org_id, org_role, sig = parts
        key = self._config.jwt_secret.encode()
        msg = f"{user_id}:{org_id}:{org_role}".encode()
        expected = hmac.new(key, msg, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return None
        return org_id, org_role

    # -- Authentication ----------------------------------------------------------

    async def verify_token(self, token: str) -> Session | None:
        """Verify a Supabase JWT locally (HS256) and return its session."""
        try:
            claims = pyjwt.decode(
                token,
                self._config.jwt_secret,
                algorithms=["HS256"],
                audience="authenticated",
            )
        except pyjwt.exceptions.InvalidTokenError:
            return None

        user_id = claims.get("sub")
        if not user_id:
            return None

        iat = claims.get("iat")
        issued_at = (
            datetime.fromtimestamp(iat, tz=UTC)
            if iat is not None
            else datetime.now(UTC)
        )

        return Session(
            user_id=user_id,
            session_id=claims.get("session_id", user_id),
            tenant_id=None,
            org_id=None,
            org_role=None,
            roles=(claims.get("role", "authenticated"),),
            scopes=(),
            is_anonymous=False,
            mfa=MFAStatus(enrolled=False, satisfied=False, factor_types=()),
            issued_at=issued_at,
            expires_at=datetime.fromtimestamp(claims["exp"], tz=UTC),
            claims=claims,
        )

    async def authenticate(self, request: AuthRequest) -> Session | None:
        """Verify the Bearer token, then layer on org context from ``X-Org-Context``.

        Overrides the base helper because Supabase carries the active org in a
        signed request header, not in the access token itself.
        """
        auth_header = request.headers.get("authorization") or request.headers.get(
            "Authorization"
        )
        if not auth_header or not auth_header.startswith("Bearer "):
            return None
        session = await self.verify_token(auth_header[7:])
        if session is None:
            return None

        org_header = request.headers.get("X-Org-Context")
        if org_header:
            result = self._verify_org_context(session.user_id, org_header)
            if result is not None:
                org_id, org_role = result
                session = dataclasses.replace(session, org_id=org_id, org_role=org_role)
        return session

    # -- User management (admin API) ---------------------------------------------

    async def get_user(self, user_id: str) -> Identity:
        http = self._ensure_http()
        try:
            resp = await http.get(
                f"admin/users/{user_id}", headers=self._admin_headers()
            )
        except httpx.HTTPError as e:
            raise AuthBackendError(f"Supabase get_user network error: {e}") from e
        if resp.status_code == 404:
            raise UserNotFoundError(f"User {user_id!r} not found")
        if not resp.is_success:
            raise self._backend_error("get_user", resp)
        return self._to_identity(resp.json())

    async def find_user(
        self,
        *,
        user_id: str | None = None,
        email: str | None = None,
        phone: str | None = None,
    ) -> Identity | None:
        """Find a user by id; ``None`` if no match (provide exactly one key).

        Email lookup is unsupported: GoTrue exposes no admin get-by-email
        endpoint, so resolving an email would mean paginating the directory.
        Phone lookup is likewise unsupported.
        """
        provided = [k for k in (user_id, email, phone) if k is not None]
        if len(provided) != 1:
            raise ValueError("Provide exactly one of user_id=, email=, or phone=")
        if email is not None:
            raise NotImplementedError(
                "Supabase has no admin email-lookup endpoint; "
                "find_user supports user_id only."
            )
        if phone is not None:
            raise NotImplementedError(
                "Supabase has no admin phone-lookup endpoint; "
                "find_user supports user_id only."
            )
        assert user_id is not None  # guaranteed by the exactly-one check above
        try:
            return await self.get_user(user_id)
        except UserNotFoundError:
            return None

    async def list_users(
        self, *, limit: int = 50, cursor: str | None = None
    ) -> Page[Identity]:
        """List users. GoTrue pages by 1-based page number.

        ``cursor`` is treated as an opaque stringified page index; ``None``
        starts at page 1.
        """
        http = self._ensure_http()
        page = int(cursor) if cursor else 1
        params: dict[str, Any] = {"per_page": limit, "page": page}

        try:
            resp = await http.get(
                "admin/users", headers=self._admin_headers(), params=params
            )
        except httpx.HTTPError as e:
            raise AuthBackendError(f"Supabase list_users network error: {e}") from e
        if not resp.is_success:
            raise self._backend_error("list_users", resp)
        data = resp.json()
        users = data.get("users", []) if isinstance(data, dict) else data
        items = [self._to_identity(u) for u in users]
        has_more = len(items) == limit
        next_cursor = str(page + 1) if has_more else None
        return Page(items=items, next_cursor=next_cursor, has_more=has_more)

    async def update_user(self, *, user_id: str, **kwargs: Any) -> AuthResult[Identity]:
        http = self._ensure_http()
        body: dict[str, Any] = {}
        email = kwargs.get("email")
        if email is not None:
            body["email"] = email

        user_metadata: dict[str, Any] = {}
        meta_map = {
            "first_name": "first_name",
            "last_name": "last_name",
            "image_url": "avatar_url",
        }
        for key, meta_key in meta_map.items():
            if key in kwargs and kwargs[key] is not None:
                user_metadata[meta_key] = kwargs[key]
        if user_metadata:
            body["user_metadata"] = user_metadata

        try:
            resp = await http.put(
                f"admin/users/{user_id}",
                headers=self._admin_headers(),
                json=body,
            )
        except httpx.HTTPError as e:
            raise AuthBackendError(f"Supabase update_user network error: {e}") from e
        if resp.status_code == 404:
            return AuthResult(error=UserNotFoundError(f"User {user_id!r} not found"))
        if not resp.is_success:
            raise self._backend_error("update_user", resp)
        return AuthResult(value=self._to_identity(resp.json()))

    async def delete_user(self, user_id: str) -> bool:
        http = self._ensure_http()
        try:
            resp = await http.delete(
                f"admin/users/{user_id}", headers=self._admin_headers()
            )
        except httpx.HTTPError as e:
            raise AuthBackendError(f"Supabase delete_user network error: {e}") from e
        if resp.status_code == 404:
            return False
        if not resp.is_success:
            raise self._backend_error("delete_user", resp)
        return True

    # -- Sign-up / sign-in -------------------------------------------------------

    async def sign_up(
        self,
        *,
        email: str | None = None,
        phone: str | None = None,
        password: str | None = None,
        **kwargs: Any,
    ) -> AuthOutcome:
        http = self._ensure_http()
        body: dict[str, Any] = {"email": email, "password": password}
        if kwargs.get("data"):
            body["data"] = kwargs["data"]

        try:
            resp = await http.post("signup", json=body)
        except httpx.HTTPError as e:
            raise AuthBackendError(f"Supabase sign_up network error: {e}") from e

        if resp.status_code == 422 and self._gotrue_error_code(resp) in {
            "user_already_exists",
            "email_exists",
        }:
            return AuthOutcome(
                status=AuthStatus.EMAIL_EXISTS,
                error=EmailAlreadyExistsError(email or ""),
            )
        if not resp.is_success:
            logger.error("Supabase sign-up failed: %s", resp.text)
            raise self._backend_error("sign_up", resp)

        data = resp.json()
        if "access_token" not in data:
            # Supabase returns a user without tokens when confirmation is
            # required. The signup itself succeeded.
            return AuthOutcome(
                status=AuthStatus.VERIFICATION_REQUIRED,
                identity=self._to_identity(data.get("user", data)),
            )
        return self._to_outcome(data)

    async def sign_in_with_password(
        self, *, identifier: str, password: str
    ) -> AuthOutcome:
        http = self._ensure_http()
        try:
            resp = await http.post(
                "token",
                params={"grant_type": "password"},
                json={"email": identifier, "password": password},
            )
        except httpx.HTTPError as e:
            raise AuthBackendError(
                f"Supabase sign_in_with_password network error: {e}"
            ) from e
        # GoTrue returns 400 for bad credentials, 401 for unconfirmed email.
        if resp.status_code in (400, 401):
            return AuthOutcome(
                status=AuthStatus.INVALID_CREDENTIALS,
                error=InvalidCredentialsError(),
            )
        if not resp.is_success:
            raise self._backend_error("sign_in_with_password", resp)
        return self._to_outcome(resp.json())

    # -- Passwordless: magic link ------------------------------------------------

    async def send_magic_link(self, *, email: str, redirect_url: str) -> None:
        http = self._ensure_http()
        try:
            resp = await http.post("otp", json={"email": email})
        except httpx.HTTPError as e:
            raise AuthBackendError(
                f"Supabase send_magic_link network error: {e}"
            ) from e
        if not resp.is_success:
            raise self._backend_error("send_magic_link", resp)

    async def verify_magic_link(
        self, token: str, *, email: str | None = None
    ) -> AuthOutcome:
        http = self._ensure_http()
        body: dict[str, Any] = {"type": "magiclink", "token": token}
        if email is not None:
            body["email"] = email
        try:
            resp = await http.post("verify", json=body)
        except httpx.HTTPError as e:
            raise AuthBackendError(
                f"Supabase verify_magic_link network error: {e}"
            ) from e
        if resp.status_code in (400, 401, 403, 404):
            return AuthOutcome(
                status=AuthStatus.INVALID_TOKEN,
                error=InvalidTokenError("Magic link is invalid or expired"),
            )
        if not resp.is_success:
            raise self._backend_error("verify_magic_link", resp)
        return self._to_outcome(resp.json())

    # -- Token refresh -----------------------------------------------------------

    async def refresh(self, refresh_token: str) -> AuthResult[TokenSet]:
        http = self._ensure_http()
        try:
            resp = await http.post(
                "token",
                params={"grant_type": "refresh_token"},
                json={"refresh_token": refresh_token},
            )
        except httpx.HTTPError as e:
            raise AuthBackendError(f"Supabase refresh network error: {e}") from e
        if resp.status_code in (400, 401):
            return AuthResult(
                error=InvalidTokenError("Refresh token is invalid or expired")
            )
        if not resp.is_success:
            raise self._backend_error("refresh", resp)
        return AuthResult(value=self._to_token_set(resp.json()))

    # -- Password recovery -------------------------------------------------------

    async def request_password_reset(
        self, *, email: str, redirect_url: str | None = None
    ) -> None:
        http = self._ensure_http()
        try:
            resp = await http.post("recover", json={"email": email})
        except httpx.HTTPError as e:
            raise AuthBackendError(
                f"Supabase request_password_reset network error: {e}"
            ) from e
        if not resp.is_success:
            raise self._backend_error("request_password_reset", resp)

    async def reset_password(
        self, token: str, new_password: str
    ) -> AuthResult[Identity]:
        http = self._ensure_http()
        try:
            resp = await http.put(
                "user",
                headers={"Authorization": f"Bearer {token}"},
                json={"password": new_password},
            )
        except httpx.HTTPError as e:
            raise AuthBackendError(f"Supabase reset_password network error: {e}") from e
        if resp.status_code in (400, 401, 403, 404):
            return AuthResult(
                error=InvalidTokenError("Recovery token is invalid or expired")
            )
        if not resp.is_success:
            raise self._backend_error("reset_password", resp)
        return AuthResult(value=self._to_identity(resp.json()))

    # -- Email confirmation ------------------------------------------------------

    async def verify_email(self, token: str) -> AuthResult[Identity]:
        http = self._ensure_http()
        try:
            resp = await http.post(
                "verify",
                json={"type": "signup", "token": token},
            )
        except httpx.HTTPError as e:
            raise AuthBackendError(f"Supabase verify_email network error: {e}") from e
        if resp.status_code in (400, 401, 403, 404):
            return AuthResult(
                error=InvalidTokenError("Confirmation token is invalid or expired")
            )
        if not resp.is_success:
            raise self._backend_error("verify_email", resp)
        data = resp.json()
        user_data = data.get("user", data)
        return AuthResult(value=self._to_identity(user_data))

    # -- Sessions ----------------------------------------------------------------

    async def revoke_session(self, session_id: str) -> AuthResult[bool]:
        http = self._ensure_http()
        try:
            resp = await http.post(
                "logout",
                headers={"Authorization": f"Bearer {session_id}"},
            )
        except httpx.HTTPError as e:
            raise AuthBackendError(f"Supabase revoke_session network error: {e}") from e
        if not resp.is_success and resp.status_code != 401:
            # 401 means the token's already invalid — treat as success.
            raise self._backend_error("revoke_session", resp)
        return AuthResult(value=True)

    async def revoke_all_sessions(self, user_id: str) -> AuthResult[bool]:
        http = self._ensure_http()
        try:
            resp = await http.post(
                "logout",
                headers={"Authorization": f"Bearer {user_id}"},
                params={"scope": "global"},
            )
        except httpx.HTTPError as e:
            raise AuthBackendError(
                f"Supabase revoke_all_sessions network error: {e}"
            ) from e
        if not resp.is_success and resp.status_code != 401:
            raise self._backend_error("revoke_all_sessions", resp)
        return AuthResult(value=True)

    # -- OAuth -------------------------------------------------------------------

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
        if provider is None:
            raise ValueError("Supabase authorization_url requires a provider")
        provider_name = (
            provider.value if isinstance(provider, AuthProvider) else provider
        )
        uri = redirect_uri or self._config.redirect_uri
        params = f"provider={provider_name}&state={state}"
        if uri:
            params += f"&redirect_to={uri}"
        if scopes:
            params += f"&scopes={'+'.join(scopes)}"
        return f"{self._base_url}/authorize?{params}"

    async def sign_in_with_oauth(
        self,
        code: str,
        *,
        provider: str | AuthProvider | None = None,
        redirect_uri: str | None = None,
    ) -> AuthOutcome:
        http = self._ensure_http()
        uri = redirect_uri or self._config.redirect_uri
        body: dict[str, Any] = {"auth_code": code}
        if uri:
            body["redirect_to"] = uri

        try:
            resp = await http.post(
                "token",
                params={"grant_type": "pkce"},
                json=body,
            )
        except httpx.HTTPError as e:
            raise AuthBackendError(
                f"Supabase sign_in_with_oauth network error: {e}"
            ) from e
        if resp.status_code in (400, 401, 403):
            return AuthOutcome(
                status=AuthStatus.INVALID_CREDENTIALS,
                error=InvalidCredentialsError("OAuth code rejected by Supabase"),
            )
        if not resp.is_success:
            logger.error("Supabase OAuth token exchange failed: %s", resp.text)
            raise self._backend_error("sign_in_with_oauth", resp)
        return self._to_outcome(resp.json())

    # -- Organizations (database-backed) -----------------------------------------

    def _to_org(self, org: AuthOrganization) -> Org:
        return Org(
            id=str(org.id),
            tenant_id=None,
            name=org.name,
            slug=org.slug,
            metadata=org.metadata or {},
            created_at=org.created_at,
            updated_at=org.updated_at,
        )

    def _to_org_member(self, member: SupabaseOrgMember) -> OrgMember:
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
        """Create an org; creator becomes the owner.

        Expected failure attached as ``result.error``:
            OrgSlugConflictError: Slug is already taken.
        """
        org = await (
            self._db()
            .insert(AuthOrganization)
            .values(name=name, slug=slug)
            .ignore_conflicts(target=AuthOrganization.slug)
            .returning(AuthOrganization)
            .execute()
        )
        if org is None:
            return AuthResult(error=OrgSlugConflictError(slug))

        await (
            self._db()
            .insert(SupabaseOrgMember)
            .values(org_id=org.id, user_id=str(creator_id), role="owner")
            .execute()
        )
        return AuthResult(value=self._to_org(org))

    async def get_org(
        self, *, org_id: str | None = None, slug: str | None = None
    ) -> AuthResult[Org]:
        """Get an org by id or slug (pass exactly one).

        Expected failure attached as ``result.error``:
            OrgNotFoundError: No matching org.
        """
        self._check_org_ref(org_id, slug)
        q = self._db().select(AuthOrganization)
        q = (
            q.where(AuthOrganization.id == org_id)
            if org_id is not None
            else q.where(AuthOrganization.slug == slug)
        )
        result = await q.first_or_none()
        if result is None:
            return AuthResult(
                error=OrgNotFoundError(f"No org matching {org_id or slug!r}")
            )
        return AuthResult(value=self._to_org(result))

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

        Expected failures attached as ``result.error``:
            OrgNotFoundError: No such org.
            OrgSlugConflictError: New slug collides with another org.
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
        offset = int(cursor) if cursor else 0
        q = (
            self._db()
            .select(AuthOrganization)
            .order_by(AuthOrganization.created_at, asc=False)
        )
        if user_id is not None:
            q = q.inner_join(
                SupabaseOrgMember,
                SupabaseOrgMember.org_id == AuthOrganization.id,
            ).where(SupabaseOrgMember.user_id == str(user_id))
        q = q.limit(limit).offset(offset)
        items = [self._to_org(o) for o in await q.execute()]
        has_more = len(items) == limit
        next_cursor = str(offset + limit) if has_more else None
        return Page(items=items, next_cursor=next_cursor, has_more=has_more)

    async def add_member(
        self,
        *,
        user_id: str,
        org_id: str | None = None,
        slug: str | None = None,
        role: str = "member",
    ) -> AuthResult[OrgMember]:
        """Add a user to an org (by id or slug).

        Expected failures attached as ``result.error``:
            OrgNotFoundError: A given slug matches no org.
            MemberAlreadyExistsError: User is already a member.
        """
        self._check_org_ref(org_id, slug)
        db = self._db()
        canonical = org_id
        if canonical is None:
            org = await (
                db.select(AuthOrganization)
                .where(AuthOrganization.slug == slug)
                .first_or_none()
            )
            if org is None:
                return AuthResult(error=OrgNotFoundError(f"No org matching {slug!r}"))
            canonical = org.id

        member = await (
            db.insert(SupabaseOrgMember)
            .values(org_id=canonical, user_id=str(user_id), role=role)
            .ignore_conflicts(
                target=(SupabaseOrgMember.org_id, SupabaseOrgMember.user_id),
            )
            .returning(SupabaseOrgMember)
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

        Expected failure attached as ``result.error``:
            OrgMemberNotFoundError: User is not a member (or no such org).
        """
        self._check_org_ref(org_id, slug)
        db = self._db()
        sel = db.select(SupabaseOrgMember).where(
            SupabaseOrgMember.user_id == str(user_id)
        )
        if org_id is not None:
            sel = sel.where(SupabaseOrgMember.org_id == org_id)
        else:
            sel = sel.inner_join(
                AuthOrganization, AuthOrganization.id == SupabaseOrgMember.org_id
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
            db.update(SupabaseOrgMember)
            .set(role=role, updated_at=datetime.now(UTC))
            .where(SupabaseOrgMember.org_id == existing.org_id)
            .where(SupabaseOrgMember.user_id == str(user_id))
            .returning(SupabaseOrgMember)
            .execute()
        )
        return AuthResult(value=self._to_org_member(result))

    async def remove_member(
        self,
        *,
        user_id: str,
        org_id: str | None = None,
        slug: str | None = None,
    ) -> AuthResult[bool]:
        """Remove a member from an org (by id or slug).

        ``result.value`` is ``True`` if a row was removed, ``False`` if the user
        wasn't a member.

        Expected failure attached as ``result.error``:
            LastOwnerError: Removing this member would leave the org without an owner.
        """
        self._check_org_ref(org_id, slug)
        db = self._db()
        sel = db.select(SupabaseOrgMember).where(
            SupabaseOrgMember.user_id == str(user_id)
        )
        if org_id is not None:
            sel = sel.where(SupabaseOrgMember.org_id == org_id)
        else:
            sel = sel.inner_join(
                AuthOrganization, AuthOrganization.id == SupabaseOrgMember.org_id
            ).where(AuthOrganization.slug == slug)
        existing = await sel.first_or_none()
        if existing is None:
            return AuthResult(value=False)

        if existing.role == "owner":
            owner_count = await (
                db.select(SupabaseOrgMember)
                .where(SupabaseOrgMember.org_id == existing.org_id)
                .where(SupabaseOrgMember.role == "owner")
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
            db.delete(SupabaseOrgMember)
            .where(SupabaseOrgMember.org_id == existing.org_id)
            .where(SupabaseOrgMember.user_id == str(user_id))
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
        """List members of an org (by id or slug; cursor-paginated).

        An unknown org yields an empty page (no existence query needed).
        """
        self._check_org_ref(org_id, slug)
        db = self._db()
        offset = int(cursor) if cursor else 0
        q = db.select(SupabaseOrgMember)
        if org_id is not None:
            q = q.where(SupabaseOrgMember.org_id == org_id)
        else:
            q = q.inner_join(
                AuthOrganization, AuthOrganization.id == SupabaseOrgMember.org_id
            ).where(AuthOrganization.slug == slug)
        q = (
            q.order_by(SupabaseOrgMember.created_at, asc=True)
            .limit(limit)
            .offset(offset)
        )
        items = [self._to_org_member(m) for m in await q.execute()]
        has_more = len(items) == limit
        next_cursor = str(offset + limit) if has_more else None
        return Page(items=items, next_cursor=next_cursor, has_more=has_more)

    # -- Organization session context --------------------------------------------

    async def set_active_org(
        self,
        *,
        session_id: str,
        org_id: str | None = None,
        slug: str | None = None,
    ) -> AuthResult[TokenSet]:
        """Switch a session's active org (by id or slug).

        Pass neither ``org_id`` nor ``slug`` to clear the active org. On success
        ``result.value`` is the org-context :class:`TokenSet`. Expected failure
        (attached to ``result.error``): ``OrgMemberNotFoundError`` when the user
        isn't a member of the target org.
        """
        if org_id is None and slug is None:
            return AuthResult(
                value=TokenSet(
                    access_token="",
                    refresh_token=None,
                    id_token=None,
                    token_type="bearer",
                    expires_in=0,
                    expires_at=datetime.now(UTC),
                )
            )

        self._check_org_ref(org_id, slug)
        user_id = str(session_id)
        db = self._db()
        sel = db.select(SupabaseOrgMember).where(SupabaseOrgMember.user_id == user_id)
        if org_id is not None:
            sel = sel.where(SupabaseOrgMember.org_id == org_id)
        else:
            sel = sel.inner_join(
                AuthOrganization, AuthOrganization.id == SupabaseOrgMember.org_id
            ).where(AuthOrganization.slug == slug)
        member = await sel.first_or_none()
        if member is None:
            return AuthResult(
                error=OrgMemberNotFoundError(
                    f"User {user_id!r} is not a member of org {org_id or slug!r}"
                )
            )

        signed = self._sign_org_context(user_id, str(member.org_id), member.role)
        return AuthResult(
            value=TokenSet(
                access_token=signed,
                refresh_token=None,
                id_token=None,
                token_type="bearer",
                expires_in=0,
                expires_at=datetime.now(UTC),
            )
        )
