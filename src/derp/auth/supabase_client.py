"""Supabase GoTrue authentication client."""

from __future__ import annotations

import hashlib
import hmac
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

import httpx
import jwt as pyjwt

from derp.auth.base import BaseAuthClient
from derp.auth.exceptions import AuthNotConnectedError
from derp.auth.jwt import TokenPair
from derp.auth.models import (
    AuthOrganization,
    AuthProvider,
    AuthRequest,
    AuthResult,
    OrgInfo,
    OrgMemberInfo,
    SessionInfo,
    SupabaseOrgMember,
    UserInfo,
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

    # -- Response mapping --------------------------------------------------------

    def _to_user_info(self, data: dict[str, Any]) -> UserInfo:
        user_meta = data.get("user_metadata") or {}
        app_meta = data.get("app_metadata") or {}

        role = app_meta.get("role", data.get("role", "authenticated"))
        is_superuser = bool(app_meta.get("is_superuser", False))

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

        return UserInfo(
            id=data.get("id", ""),
            email=data.get("email", ""),
            first_name=user_meta.get("first_name"),
            last_name=user_meta.get("last_name"),
            username=None,
            image_url=user_meta.get("avatar_url"),
            role=role,
            is_active=is_active,
            is_superuser=is_superuser,
            email_confirmed_at=email_confirmed_at,
            last_sign_in_at=last_sign_in_at,
            created_at=created_at,
            updated_at=updated_at,
            metadata=metadata,
        )

    def _to_token_pair(self, data: dict[str, Any]) -> TokenPair:
        expires_in = data.get("expires_in", 3600)
        expires_at_ts = data.get("expires_at")
        if expires_at_ts:
            expires_at = datetime.fromtimestamp(int(expires_at_ts), tz=UTC)
        else:
            expires_at = datetime.now(UTC)
        return TokenPair(
            access_token=data.get("access_token", ""),
            refresh_token=data.get("refresh_token", ""),
            token_type="bearer",
            expires_in=expires_in,
            expires_at=expires_at,
        )

    def _to_auth_result(self, data: dict[str, Any]) -> AuthResult:
        user = self._to_user_info(data["user"])
        tokens = self._to_token_pair(data)
        return AuthResult(user=user, tokens=tokens)

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

    async def authenticate(self, request: AuthRequest) -> SessionInfo | None:
        auth_header = request.headers.get("authorization") or request.headers.get(
            "Authorization"
        )
        if not auth_header or not auth_header.startswith("Bearer "):
            return None

        token = auth_header[7:]

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

        session_id = claims.get("session_id", user_id)
        role = claims.get("role", "authenticated")

        org_id: str | None = None
        org_role: str | None = None
        org_header = request.headers.get("X-Org-Context")
        if org_header:
            result = self._verify_org_context(user_id, org_header)
            if result is not None:
                org_id, org_role = result

        return SessionInfo(
            user_id=user_id,
            session_id=session_id,
            role=role,
            expires_at=datetime.fromtimestamp(claims["exp"], tz=UTC),
            metadata={},
            org_id=org_id,
            org_role=org_role,
        )

    # -- User management (admin API) ---------------------------------------------

    async def get_user(self, user_id: str | uuid.UUID) -> UserInfo | None:
        http = self._ensure_http()
        resp = await http.get(f"admin/users/{user_id}", headers=self._admin_headers())
        if not resp.is_success:
            return None
        return self._to_user_info(resp.json())

    async def list_users(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> list[UserInfo]:
        http = self._ensure_http()
        params: dict[str, Any] = {}
        if limit is not None:
            params["per_page"] = limit
        if offset is not None and limit:
            params["page"] = (offset // limit) + 1
        elif offset is not None:
            params["page"] = offset + 1

        resp = await http.get(
            "admin/users", headers=self._admin_headers(), params=params
        )
        if not resp.is_success:
            return []
        data = resp.json()
        users = data.get("users", []) if isinstance(data, dict) else data
        return [self._to_user_info(u) for u in users]

    async def update_user(
        self,
        *,
        user_id: str | uuid.UUID,
        email: str | None = None,
        **kwargs: Any,
    ) -> UserInfo | None:
        http = self._ensure_http()
        body: dict[str, Any] = {}
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

        resp = await http.put(
            f"admin/users/{user_id}",
            headers=self._admin_headers(),
            json=body,
        )
        if not resp.is_success:
            return None
        return self._to_user_info(resp.json())

    async def delete_user(self, user_id: str | uuid.UUID) -> bool:
        http = self._ensure_http()
        resp = await http.delete(
            f"admin/users/{user_id}", headers=self._admin_headers()
        )
        return resp.is_success

    async def count_users(self) -> int:
        http = self._ensure_http()
        resp = await http.get(
            "admin/users",
            headers=self._admin_headers(),
            params={"per_page": 1},
        )
        if not resp.is_success:
            return 0

        total = resp.headers.get("x-total-count")
        if total is not None:
            return int(total)

        data = resp.json()
        users = data.get("users", []) if isinstance(data, dict) else data
        return len(users)

    # -- Sign-up / sign-in -------------------------------------------------------

    async def sign_up(
        self,
        *,
        email: str,
        password: str,
        request: AuthRequest | None = None,
        confirmation_url: str | None = None,
        user_agent: str | None = None,
        ip_address: str | None = None,
        **kwargs: Any,
    ) -> AuthResult | None:
        http = self._ensure_http()
        body: dict[str, Any] = {"email": email, "password": password}
        if kwargs.get("data"):
            body["data"] = kwargs["data"]

        resp = await http.post("signup", json=body)
        if not resp.is_success:
            logger.error("Supabase sign-up failed: %s", resp.text)
            return None

        data = resp.json()
        if "access_token" not in data:
            # Supabase returns user without tokens when confirmation is required.
            return None
        return self._to_auth_result(data)

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
        http = self._ensure_http()
        resp = await http.post(
            "token",
            params={"grant_type": "password"},
            json={"email": email, "password": password},
        )
        if not resp.is_success:
            return None
        return self._to_auth_result(resp.json())

    async def sign_in_with_magic_link(self, *, email: str, magic_link_url: str) -> None:
        http = self._ensure_http()
        await http.post("otp", json={"email": email})

    async def verify_magic_link(
        self,
        token: str,
        *,
        user_agent: str | None = None,
        ip_address: str | None = None,
    ) -> AuthResult | None:
        http = self._ensure_http()
        resp = await http.post(
            "verify",
            json={"type": "magiclink", "token": token},
        )
        if not resp.is_success:
            return None
        return self._to_auth_result(resp.json())

    # -- Token refresh -----------------------------------------------------------

    async def refresh_token(self, refresh_token: str) -> TokenPair | None:
        http = self._ensure_http()
        resp = await http.post(
            "token",
            params={"grant_type": "refresh_token"},
            json={"refresh_token": refresh_token},
        )
        if not resp.is_success:
            return None
        return self._to_token_pair(resp.json())

    # -- Password recovery -------------------------------------------------------

    async def request_password_recovery(
        self,
        *,
        email: str,
        recovery_url: str = "",
        recovery_subject: str = "Reset your password",
        **kwargs: Any,
    ) -> None:
        http = self._ensure_http()
        await http.post("recover", json={"email": email})

    async def reset_password(self, token: str, new_password: str) -> UserInfo | None:
        http = self._ensure_http()
        resp = await http.put(
            "user",
            headers={"Authorization": f"Bearer {token}"},
            json={"password": new_password},
        )
        if not resp.is_success:
            return None
        return self._to_user_info(resp.json())

    # -- Email confirmation ------------------------------------------------------

    async def confirm_email(self, token: str) -> UserInfo | None:
        http = self._ensure_http()
        resp = await http.post(
            "verify",
            json={"type": "signup", "token": token},
        )
        if not resp.is_success:
            return None
        data = resp.json()
        user_data = data.get("user", data)
        return self._to_user_info(user_data)

    # -- Sessions ----------------------------------------------------------------

    async def list_sessions(
        self,
        *,
        user_id: str | uuid.UUID | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[Any]:
        return []

    async def sign_out(self, session_id: str | uuid.UUID) -> None:
        http = self._ensure_http()
        await http.post(
            "logout",
            headers={"Authorization": f"Bearer {session_id}"},
        )

    async def sign_out_all(self, user_id: str | uuid.UUID) -> None:
        http = self._ensure_http()
        await http.post(
            "logout",
            headers={"Authorization": f"Bearer {user_id}"},
            params={"scope": "global"},
        )

    # -- OAuth -------------------------------------------------------------------

    def get_oauth_authorization_url(
        self,
        provider: str | AuthProvider,
        state: str,
        scopes: list[str] | None = None,
        redirect_uri: str | None = None,
    ) -> str:
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
        provider: str | AuthProvider,
        code: str,
        *,
        redirect_uri: str | None = None,
        user_agent: str | None = None,
        ip_address: str | None = None,
    ) -> AuthResult | None:
        http = self._ensure_http()
        uri = redirect_uri or self._config.redirect_uri
        body: dict[str, Any] = {"auth_code": code}
        if uri:
            body["redirect_to"] = uri

        resp = await http.post(
            "token",
            params={"grant_type": "pkce"},
            json=body,
        )
        if not resp.is_success:
            logger.error("Supabase OAuth token exchange failed: %s", resp.text)
            return None
        return self._to_auth_result(resp.json())

    # -- Organizations (database-backed) -----------------------------------------

    def _to_org_info(self, org: AuthOrganization) -> OrgInfo:
        return OrgInfo(
            id=str(org.id),
            name=org.name,
            slug=org.slug,
            metadata=org.metadata or {},
            created_at=org.created_at,
            updated_at=org.updated_at,
        )

    def _to_org_member_info(self, member: SupabaseOrgMember) -> OrgMemberInfo:
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

        await (
            self._db()
            .insert(SupabaseOrgMember)
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
        org = await (
            self._db()
            .select(AuthOrganization)
            .where(AuthOrganization.id == str(org_id))
            .first_or_none()
        )
        return self._to_org_info(org) if org is not None else None

    async def get_org_by_slug(self, slug: str) -> OrgInfo | None:
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
        if limit is not None:
            q = q.limit(limit)
        if offset is not None:
            q = q.offset(offset)
        return [self._to_org_info(o) for o in await q.execute()]

    async def add_org_member(
        self,
        *,
        org_id: str | uuid.UUID,
        user_id: str | uuid.UUID,
        role: str = "member",
    ) -> OrgMemberInfo | None:
        now = datetime.now(UTC)
        member = await (
            self._db()
            .insert(SupabaseOrgMember)
            .values(
                org_id=str(org_id),
                user_id=str(user_id),
                role=role,
                created_at=now,
                updated_at=now,
            )
            .ignore_conflicts(
                target=(SupabaseOrgMember.org_id, SupabaseOrgMember.user_id),
            )
            .returning(SupabaseOrgMember)
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
        existing = await (
            self._db()
            .select(SupabaseOrgMember)
            .where(SupabaseOrgMember.org_id == str(org_id))
            .where(SupabaseOrgMember.user_id == str(user_id))
            .first_or_none()
        )
        if existing is None:
            return None

        [result] = await (
            self._db()
            .update(SupabaseOrgMember)
            .set(role=role, updated_at=datetime.now(UTC))
            .where(SupabaseOrgMember.org_id == str(org_id))
            .where(SupabaseOrgMember.user_id == str(user_id))
            .returning(SupabaseOrgMember)
            .execute()
        )
        return self._to_org_member_info(result)

    async def remove_org_member(
        self,
        *,
        org_id: str | uuid.UUID,
        user_id: str | uuid.UUID,
    ) -> bool:
        existing = await (
            self._db()
            .select(SupabaseOrgMember)
            .where(SupabaseOrgMember.org_id == str(org_id))
            .where(SupabaseOrgMember.user_id == str(user_id))
            .first_or_none()
        )
        if existing is None:
            return False

        if existing.role == "owner":
            owner_count = await (
                self._db()
                .select(SupabaseOrgMember)
                .where(SupabaseOrgMember.org_id == str(org_id))
                .where(SupabaseOrgMember.role == "owner")
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
            .delete(SupabaseOrgMember)
            .where(SupabaseOrgMember.org_id == str(org_id))
            .where(SupabaseOrgMember.user_id == str(user_id))
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
        q = (
            self._db()
            .select(SupabaseOrgMember)
            .where(SupabaseOrgMember.org_id == str(org_id))
            .order_by(SupabaseOrgMember.created_at, asc=True)
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
        member = await (
            self._db()
            .select(SupabaseOrgMember)
            .where(SupabaseOrgMember.org_id == str(org_id))
            .where(SupabaseOrgMember.user_id == str(user_id))
            .first_or_none()
        )
        return self._to_org_member_info(member) if member is not None else None

    # -- Organization session context --------------------------------------------

    async def set_active_org(
        self,
        *,
        session_id: str | uuid.UUID,
        org_id: str | uuid.UUID | None,
    ) -> TokenPair | None:
        if org_id is None:
            return TokenPair(
                access_token="",
                refresh_token="",
                token_type="bearer",
                expires_in=0,
                expires_at=datetime.now(UTC),
            )

        user_id = str(session_id)
        role = await (
            self._db()
            .select(SupabaseOrgMember.role)
            .where(SupabaseOrgMember.org_id == str(org_id))
            .where(SupabaseOrgMember.user_id == user_id)
            .first_or_none()
        )
        if role is None:
            return None

        signed = self._sign_org_context(user_id, str(org_id), role)
        return TokenPair(
            access_token=signed,
            refresh_token="",
            token_type="bearer",
            expires_in=0,
            expires_at=datetime.now(UTC),
        )
