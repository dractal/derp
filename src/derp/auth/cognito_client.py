"""AWS Cognito authentication client."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import logging
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import asyncpg
import httpx
import jwt as pyjwt
from etils import epy
from jwt.algorithms import RSAAlgorithm

from derp.auth.base import BaseAuthClient
from derp.auth.exceptions import (
    AuthNotConnectedError,
    OrgAlreadyExistsError,
    OrgMemberExistsError,
    PasswordValidationError,
)
from derp.auth.jwt import TokenPair
from derp.auth.models import (
    AuthOrganization,
    AuthProvider,
    AuthRequest,
    AuthResult,
    CognitoOrgMember,
    OrgInfo,
    OrgMemberInfo,
    SessionInfo,
    UserInfo,
)
from derp.config import CognitoConfig
from derp.orm import DatabaseEngine

with epy.lazy_imports():
    import aiobotocore.client as aio_client
    import aiobotocore.session as aio_session


logger = logging.getLogger(__name__)

# JWKS cache TTL in seconds.
_JWKS_TTL = 3600.0


def _cognito_error_code(exc: Exception) -> str | None:
    """Extract the Cognito error code from a botocore ClientError."""
    resp = getattr(exc, "response", None)
    if resp and "Error" in resp:
        return resp["Error"].get("Code")
    return None


class CognitoAuthClient(BaseAuthClient):
    """AWS Cognito-backed authentication client.

    Delegates user management, sign-up, sign-in, and token refresh to
    a Cognito User Pool. JWT verification is performed locally against
    the pool's JWKS endpoint.
    """

    def __init__(self, config: CognitoConfig) -> None:
        self._config: CognitoConfig = config
        self._jwks: dict[str, Any] | None = None
        self._jwks_fetched_at: float = 0.0
        self._jwks_lock = asyncio.Lock()
        self._issuer = (
            f"https://cognito-idp.{config.region}.amazonaws.com/{config.user_pool_id}"
        )
        self._session: aio_session.AioSession | None = None
        self._client: aio_client.AioBaseClient | None = None
        self._client_ctx: Any | None = None
        self._database_client: DatabaseEngine | None = None

    async def connect(self) -> None:
        """Create the Cognito client connection."""
        if self._client is not None:
            return
        kwargs: dict[str, Any] = {"region_name": self._config.region}
        if self._config.access_key_id:
            kwargs["aws_access_key_id"] = self._config.access_key_id
        if self._config.secret_access_key:
            kwargs["aws_secret_access_key"] = self._config.secret_access_key
        self._session = aio_session.get_session()
        self._client = await self._session.create_client(
            "cognito-idp", **kwargs
        ).__aenter__()

    async def disconnect(self) -> None:
        """Close the Cognito client connection."""
        if self._client is not None:
            await self._client.__aexit__(None, None, None)
            self._client = None
        if self._session is not None:
            self._session = None

    def set_db(self, db: DatabaseEngine | None) -> None:
        """Set the database client for organization support."""
        self._database_client = db

    def _db(self) -> DatabaseEngine:
        if self._database_client is None:
            raise ValueError(
                "Database client not set. Organization methods require "
                "a database. Call `set_db()` first."
            )
        return self._database_client

    async def _fetch_jwks(self, *, force: bool = False) -> dict[str, Any]:
        """Fetch and cache JWKS keys from Cognito."""
        async with self._jwks_lock:
            now = time.monotonic()
            if (
                not force
                and self._jwks is not None
                and (now - self._jwks_fetched_at) < _JWKS_TTL
            ):
                return self._jwks

            url = f"{self._issuer}/.well-known/jwks.json"
            async with httpx.AsyncClient() as http:
                resp = await http.get(url)
                resp.raise_for_status()
                data = resp.json()

            keys: dict[str, Any] = {}
            for key_data in data.get("keys", []):
                kid = key_data.get("kid")
                if kid:
                    public_key = RSAAlgorithm.from_jwk(key_data)
                    keys[kid] = public_key
            self._jwks = keys
            self._jwks_fetched_at = now
            return keys

    def _attrs_to_dict(self, attributes: list[dict[str, str]]) -> dict[str, Any]:
        """Convert Cognito attribute list to a flat dict.

        Boolean strings (``"true"``/``"false"``) are coerced to ``bool``.
        """
        result: dict[str, Any] = {}
        for attr in attributes:
            value: Any = attr["Value"]
            if value in ("true", "false"):
                value = value == "true"
            result[attr["Name"]] = value
        return result

    def _to_user_info(self, user: dict[str, Any]) -> UserInfo:
        """Convert a Cognito admin_get_user / list_users response to UserInfo."""
        attrs = self._attrs_to_dict(
            user.get("UserAttributes") or user.get("Attributes") or []
        )
        sub = attrs.get("sub", user.get("Username", ""))
        role = attrs.get("custom:role", "default")
        is_superuser = bool(attrs.get("custom:is_superuser", False))

        email_verified = bool(attrs.get("email_verified", False))
        updated_at = user.get("UserLastModifiedDate") or datetime.now(UTC)
        email_confirmed_at = updated_at if email_verified else None

        # Standard attributes consumed above — the rest goes into metadata.
        _consumed = {
            "sub",
            "email",
            "given_name",
            "family_name",
            "preferred_username",
            "picture",
            "email_verified",
            "custom:role",
            "custom:is_superuser",
        }
        metadata = {k: v for k, v in attrs.items() if k not in _consumed}

        return UserInfo(
            id=sub,
            email=attrs.get("email", ""),
            first_name=attrs.get("given_name"),
            last_name=attrs.get("family_name"),
            username=attrs.get("preferred_username") or user.get("Username"),
            image_url=attrs.get("picture"),
            role=role,
            is_active=user.get("Enabled", True),
            is_superuser=is_superuser,
            email_confirmed_at=email_confirmed_at,
            last_sign_in_at=None,
            created_at=user.get("UserCreateDate") or datetime.now(UTC),
            updated_at=updated_at,
            metadata=metadata,
        )

    def _auth_result_to_token_pair(self, result: dict[str, Any]) -> TokenPair:
        """Map Cognito AuthenticationResult to TokenPair."""
        expires_in = result.get("ExpiresIn", 3600)
        return TokenPair(
            access_token=result["AccessToken"],
            refresh_token=result.get("RefreshToken", ""),
            token_type="bearer",
            expires_in=expires_in,
            expires_at=datetime.now(UTC),
        )

    # -- Org context signing ---------------------------------------------------

    def _sign_org_context(self, user_id: str, org_id: str, org_role: str) -> str:
        """Sign ``user_id:org_id:org_role`` with HMAC-SHA256.

        Uses ``client_secret`` if available, otherwise ``client_id``.
        Returns ``org_id:org_role:hex_signature``.
        """
        key = self._config.client_secret.encode()
        msg = f"{user_id}:{org_id}:{org_role}".encode()
        sig = hmac.new(key, msg, hashlib.sha256).hexdigest()
        return f"{org_id}:{org_role}:{sig}"

    def _verify_org_context(self, user_id: str, header: str) -> tuple[str, str] | None:
        """Verify a signed org context header.

        Returns ``(org_id, org_role)`` on success, ``None`` on failure.
        """

        parts = header.split(":", 2)
        if len(parts) != 3:
            return None
        org_id, org_role, sig = parts

        key = self._config.client_secret.encode()
        msg = f"{user_id}:{org_id}:{org_role}".encode()
        expected = hmac.new(key, msg, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return None
        return org_id, org_role

    # -- Authentication -------------------------------------------------------

    async def authenticate(self, request: AuthRequest) -> SessionInfo | None:
        """Verify a Cognito JWT from the Authorization header."""
        auth_header = request.headers.get("authorization") or request.headers.get(
            "Authorization"
        )
        if not auth_header or not auth_header.startswith("Bearer "):
            return None

        token = auth_header[7:]

        try:
            unverified_header = pyjwt.get_unverified_header(token)
        except pyjwt.exceptions.DecodeError:
            logger.error("Failed to decode Cognito JWT: %s", token)
            return None

        kid = unverified_header.get("kid")
        if not kid:
            logger.error("Cognito JWT missing kid: %s", token)
            return None

        keys = await self._fetch_jwks()
        key = keys.get(kid)

        # Key rotation: if kid not found, force-refresh once.
        if key is None:
            keys = await self._fetch_jwks(force=True)
            key = keys.get(kid)
            if key is None:
                logger.error("Cognito JWT kid not found: %s", kid)
                return None

        try:
            claims = pyjwt.decode(
                token,
                key,
                algorithms=["RS256"],
                issuer=self._issuer,
                options={"verify_aud": False},
            )
        except pyjwt.exceptions.InvalidTokenError:
            logger.error("Cognito JWT invalid: %s", token)
            return None

        # Only accept access tokens for API authentication.
        if claims.get("token_use") != "access":
            logger.error("Cognito JWT invalid token use: %s", token)
            return None

        # Cognito access tokens use "client_id", ID tokens use "aud".
        token_client_id = claims.get("client_id") or claims.get("aud")
        if token_client_id != self._config.client_id:
            logger.error("Cognito JWT invalid client ID: %s", token)
            return None

        groups = claims.get("cognito:groups", [])
        role = groups[0] if groups else "default"
        user_id = claims["sub"]

        # Resolve org context from signed X-Org-Context header (networkless).
        org_id: str | None = None
        org_role: str | None = None
        org_header = request.headers.get("X-Org-Context")
        if org_header:
            result = self._verify_org_context(user_id, org_header)
            if result is not None:
                org_id, org_role = result

        return SessionInfo(
            user_id=user_id,
            session_id=claims.get("jti", user_id),
            role=role,
            expires_at=datetime.fromtimestamp(claims["exp"], tz=UTC),
            metadata={"token_use": claims.get("token_use", "access")},
            org_id=org_id,
            org_role=org_role,
        )

    # -- User management ------------------------------------------------------

    async def get_user(self, user_id: str | uuid.UUID) -> UserInfo | None:
        if self._client is None:
            raise AuthNotConnectedError()

        try:
            resp = await self._client.admin_get_user(
                UserPoolId=self._config.user_pool_id,
                Username=str(user_id),
            )
            return self._to_user_info(resp)
        except self._client.exceptions.UserNotFoundException:
            return None

    async def list_users(
        self,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[UserInfo]:
        if self._client is None:
            raise AuthNotConnectedError()

        kwargs: dict[str, Any] = {"UserPoolId": self._config.user_pool_id}
        if limit is not None:
            if limit > 60:
                raise ValueError("Cognito limit must be less than 60")
            kwargs["Limit"] = limit

        resp = await self._client.list_users(**kwargs)
        users = [self._to_user_info(u) for u in resp.get("Users", [])]

        if offset:
            users = users[offset:]
        return users

    async def update_user(
        self,
        *,
        user_id: str | uuid.UUID,
        email: str | None = None,
        **kwargs: Any,
    ) -> UserInfo | None:
        if self._client is None:
            raise AuthNotConnectedError()

        attrs: list[dict[str, str]] = []
        if email is not None:
            attrs.append({"Name": "email", "Value": email})
        attr_map = {
            "first_name": "given_name",
            "last_name": "family_name",
            "username": "preferred_username",
            "image_url": "picture",
        }
        for key, cognito_name in attr_map.items():
            if key in kwargs and kwargs[key] is not None:
                attrs.append({"Name": cognito_name, "Value": str(kwargs[key])})

        if attrs:
            await self._client.admin_update_user_attributes(
                UserPoolId=self._config.user_pool_id,
                Username=str(user_id),
                UserAttributes=attrs,
            )

        user = await self.get_user(user_id)
        if user is None:
            logger.error("Update user failed: could not retrieve user %s", user_id)
            return None
        return user

    async def delete_user(self, user_id: str | uuid.UUID) -> bool:
        if self._client is None:
            raise AuthNotConnectedError()

        try:
            await self._client.admin_delete_user(
                UserPoolId=self._config.user_pool_id,
                Username=str(user_id),
            )
            return True
        except self._client.exceptions.UserNotFoundException:
            return False

    async def count_users(self) -> int:
        """Return approximate user count from Cognito."""
        if self._client is None:
            raise AuthNotConnectedError()

        resp = await self._client.describe_user_pool(
            UserPoolId=self._config.user_pool_id,
        )
        return resp["UserPool"].get("EstimatedNumberOfUsers", 0)

    # -- Sign-up / sign-in ---------------------------------------------------

    async def sign_up(
        self,
        *,
        email: str,
        password: str,
        user_agent: str | None = None,
        ip_address: str | None = None,
        **kwargs: Any,
    ) -> AuthResult | None:
        if self._client is None:
            raise AuthNotConnectedError()

        sign_up_kwargs: dict[str, Any] = {
            "ClientId": self._config.client_id,
            "Username": email,
            "Password": password,
            "UserAttributes": [{"Name": "email", "Value": email}],
            "SecretHash": self._compute_secret_hash(email),
        }

        await self._client.sign_up(**sign_up_kwargs)

        # Auto-confirm so we can return tokens immediately.
        await self._client.admin_confirm_sign_up(
            UserPoolId=self._config.user_pool_id,
            Username=email,
        )

        # Get tokens via admin auth.
        auth_kwargs: dict[str, Any] = {
            "UserPoolId": self._config.user_pool_id,
            "ClientId": self._config.client_id,
            "AuthFlow": "ADMIN_USER_PASSWORD_AUTH",
            "AuthParameters": {
                "USERNAME": email,
                "PASSWORD": password,
                "SECRET_HASH": self._compute_secret_hash(email),
            },
        }

        auth_resp = await self._client.admin_initiate_auth(**auth_kwargs)
        token_pair = self._auth_result_to_token_pair(auth_resp["AuthenticationResult"])

        user = await self.get_user(email)
        if user is None:
            logger.error("Sign-up failed: could not retrieve user %s", email)
            return None
        return AuthResult(user=user, tokens=token_pair)

    async def sign_in_with_password(
        self,
        email: str,
        password: str,
        *,
        user_agent: str | None = None,
        ip_address: str | None = None,
        **kwargs: Any,
    ) -> AuthResult | None:
        if self._client is None:
            raise AuthNotConnectedError()

        auth_kwargs: dict[str, Any] = {
            "ClientId": self._config.client_id,
            "AuthFlow": "USER_PASSWORD_AUTH",
            "AuthParameters": {
                "USERNAME": email,
                "PASSWORD": password,
                "SECRET_HASH": self._compute_secret_hash(email),
            },
        }

        resp = await self._client.initiate_auth(**auth_kwargs)

        token_pair = self._auth_result_to_token_pair(resp["AuthenticationResult"])

        # Look up user by email via list_users filter.
        users_resp = await self._client.list_users(
            UserPoolId=self._config.user_pool_id,
            Filter=f'email = "{email}"',
            Limit=1,
        )
        users = users_resp.get("Users", [])
        if not users:
            logger.error("Sign-in failed: user %s not found after auth", email)
            return None
        user = self._to_user_info(users[0])
        return AuthResult(user=user, tokens=token_pair)

    # -- Token refresh --------------------------------------------------------

    async def refresh_token(
        self, refresh_token: str, **kwargs: Any
    ) -> TokenPair | None:
        if self._client is None:
            raise AuthNotConnectedError()

        auth_kwargs: dict[str, Any] = {
            "ClientId": self._config.client_id,
            "AuthFlow": "REFRESH_TOKEN_AUTH",
            "AuthParameters": {
                "REFRESH_TOKEN": refresh_token,
                "SECRET_HASH": self._compute_secret_hash(kwargs.get("username", "")),
            },
        }

        resp = await self._client.initiate_auth(**auth_kwargs)

        result = resp["AuthenticationResult"]
        # Cognito does not return a new refresh token on refresh.
        if "RefreshToken" not in result:
            result["RefreshToken"] = refresh_token
        return self._auth_result_to_token_pair(result)

    # -- Password recovery ----------------------------------------------------

    async def request_password_recovery(
        self,
        *,
        email: str,
        recovery_url: str = "",
        recovery_subject: str = "Reset your password",
        **kwargs: Any,
    ) -> None:
        """Initiate Cognito's forgot-password flow (Cognito sends the email)."""
        if self._client is None:
            raise AuthNotConnectedError()

        forgot_kwargs: dict[str, Any] = {
            "ClientId": self._config.client_id,
            "Username": email,
            "SecretHash": self._compute_secret_hash(email),
        }

        try:
            await self._client.forgot_password(**forgot_kwargs)
        except Exception as exc:
            code = _cognito_error_code(exc)
            if code == "UserNotFoundException":
                return  # Don't reveal user existence
            raise

    async def reset_password(self, token: str, new_password: str) -> UserInfo | None:
        """Confirm password reset. *token* should be ``email:code``."""
        if ":" not in token:
            return None
        email, code = token.split(":", 1)

        if self._client is None:
            raise AuthNotConnectedError()

        confirm_kwargs: dict[str, Any] = {
            "ClientId": self._config.client_id,
            "Username": email,
            "ConfirmationCode": code,
            "Password": new_password,
            "SecretHash": self._compute_secret_hash(email),
        }

        try:
            await self._client.confirm_forgot_password(**confirm_kwargs)
        except Exception as exc:
            code_str = _cognito_error_code(exc)
            if code_str == "InvalidPasswordException":
                raise PasswordValidationError(str(exc)) from exc
            raise

        users_resp = await self._client.list_users(
            UserPoolId=self._config.user_pool_id,
            Filter=f'email = "{email}"',
            Limit=1,
        )
        users = users_resp.get("Users", [])
        if not users:
            return None
        return self._to_user_info(users[0])

    # -- Email confirmation ---------------------------------------------------

    async def confirm_email(self, token: str) -> UserInfo | None:
        """Confirm sign-up. *token* should be ``email:code``."""
        if ":" not in token:
            return None
        email, code = token.split(":", 1)

        if self._client is None:
            raise AuthNotConnectedError()

        confirm_kwargs: dict[str, Any] = {
            "ClientId": self._config.client_id,
            "Username": email,
            "ConfirmationCode": code,
            "SecretHash": self._compute_secret_hash(email),
        }

        await self._client.confirm_sign_up(**confirm_kwargs)

        users_resp = await self._client.list_users(
            UserPoolId=self._config.user_pool_id,
            Filter=f'email = "{email}"',
            Limit=1,
        )
        users = users_resp.get("Users", [])
        if not users:
            return None
        return self._to_user_info(users[0])

    # -- Sessions -------------------------------------------------------------

    async def list_sessions(
        self,
        *,
        user_id: str | uuid.UUID | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[Any]:
        """Cognito does not expose individual session objects."""
        return []

    async def sign_out(self, session_id: str | uuid.UUID) -> None:
        """Cognito does not support per-session revocation.

        Falls back to global sign-out for the user identified by
        *session_id* (treated as user_id).
        """
        await self.sign_out_all(session_id)

    async def sign_out_all(self, user_id: str | uuid.UUID) -> None:
        if self._client is None:
            raise AuthNotConnectedError()

        await self._client.admin_user_global_sign_out(
            UserPoolId=self._config.user_pool_id,
            Username=str(user_id),
        )

    # -- OAuth ----------------------------------------------------------------

    def get_oauth_authorization_url(
        self,
        provider: str | AuthProvider,
        state: str,
        scopes: list[str] | None = None,
        redirect_uri: str | None = None,
    ) -> str:
        """Build Cognito hosted-UI authorization URL.

        *provider* is the Cognito identity provider name
        (e.g. ``"Google"``, ``"Facebook"``, ``"SignInWithApple"``).
        """
        if not self._config.domain:
            raise ValueError("Cognito OAuth requires `domain` in config.")
        uri = redirect_uri or self._config.redirect_uri
        if not uri:
            raise ValueError("redirect_uri is required for Cognito OAuth.")
        scope = "+".join(scopes) if scopes else "openid+email+profile"
        provider_name = (
            provider.value if isinstance(provider, AuthProvider) else provider
        )
        return (
            f"https://{self._config.domain}/oauth2/authorize"
            f"?response_type=code"
            f"&client_id={self._config.client_id}"
            f"&redirect_uri={uri}"
            f"&identity_provider={provider_name}"
            f"&scope={scope}"
            f"&state={state}"
        )

    async def sign_in_with_oauth(
        self,
        provider: str | AuthProvider,
        code: str,
        *,
        redirect_uri: str | None = None,
        user_agent: str | None = None,
        ip_address: str | None = None,
    ) -> AuthResult | None:
        """Exchange a Cognito authorization code for tokens."""
        if not self._config.domain:
            raise ValueError("Cognito OAuth requires `domain` in config.")
        uri = redirect_uri or self._config.redirect_uri
        if not uri:
            raise ValueError("redirect_uri is required for Cognito OAuth.")

        token_url = f"https://{self._config.domain}/oauth2/token"
        data = {
            "grant_type": "authorization_code",
            "client_id": self._config.client_id,
            "code": code,
            "redirect_uri": uri,
        }
        credentials = base64.b64encode(
            f"{self._config.client_id}:{self._config.client_secret}".encode()
        ).decode()
        headers: dict[str, str] = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {credentials}",
        }

        async with httpx.AsyncClient() as http:
            resp = await http.post(token_url, data=data, headers=headers)
            if not resp.is_success:
                logger.error(
                    "Cognito OAuth token exchange failed: %s",
                    resp.text,
                )
                return None
            token_data = resp.json()

        token_pair = TokenPair(
            access_token=token_data["access_token"],
            refresh_token=token_data.get("refresh_token", ""),
            token_type="bearer",
            expires_in=token_data.get("expires_in", 3600),
            expires_at=datetime.now(UTC),
        )

        # Look up user from the access token's sub claim.
        claims = pyjwt.decode(
            token_data["access_token"],
            options={"verify_signature": False},
        )

        user = await self.get_user(claims["sub"])
        if user is None:
            logger.error(
                "OAuth sign-in failed: user %s not found after token exchange",
                claims.get("sub"),
            )
            return None
        return AuthResult(user=user, tokens=token_pair)

    # -- Organizations (database-backed) ---------------------------------------

    def _to_org_info(self, org: AuthOrganization) -> OrgInfo:
        return OrgInfo(
            id=str(org.id),
            name=org.name,
            slug=org.slug,
            metadata=org.metadata or {},
            created_at=org.created_at,
            updated_at=org.updated_at,
        )

    def _to_org_member_info(self, member: CognitoOrgMember) -> OrgMemberInfo:
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
    ) -> OrgInfo:
        now = datetime.now(UTC)
        try:
            org = await (
                self._db()
                .insert(AuthOrganization)
                .values(name=name, slug=slug, created_at=now, updated_at=now)
                .returning(AuthOrganization)
                .execute()
            )
        except asyncpg.UniqueViolationError as exc:
            raise OrgAlreadyExistsError() from exc

        await (
            self._db()
            .insert(CognitoOrgMember)
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
                CognitoOrgMember,
                CognitoOrgMember.org_id == AuthOrganization.id,
            ).where(CognitoOrgMember.user_id == str(user_id))
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
    ) -> OrgMemberInfo:
        now = datetime.now(UTC)
        try:
            member = await (
                self._db()
                .insert(CognitoOrgMember)
                .values(
                    org_id=str(org_id),
                    user_id=str(user_id),
                    role=role,
                    created_at=now,
                    updated_at=now,
                )
                .returning(CognitoOrgMember)
                .execute()
            )
        except asyncpg.UniqueViolationError as exc:
            raise OrgMemberExistsError() from exc
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
            .select(CognitoOrgMember)
            .where(CognitoOrgMember.org_id == str(org_id))
            .where(CognitoOrgMember.user_id == str(user_id))
            .first_or_none()
        )
        if existing is None:
            return None

        [result] = await (
            self._db()
            .update(CognitoOrgMember)
            .set(role=role, updated_at=datetime.now(UTC))
            .where(CognitoOrgMember.org_id == str(org_id))
            .where(CognitoOrgMember.user_id == str(user_id))
            .returning(CognitoOrgMember)
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
            .select(CognitoOrgMember)
            .where(CognitoOrgMember.org_id == str(org_id))
            .where(CognitoOrgMember.user_id == str(user_id))
            .first_or_none()
        )
        if existing is None:
            return False

        if existing.role == "owner":
            owner_count = await (
                self._db()
                .select(CognitoOrgMember)
                .where(CognitoOrgMember.org_id == str(org_id))
                .where(CognitoOrgMember.role == "owner")
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
            .delete(CognitoOrgMember)
            .where(CognitoOrgMember.org_id == str(org_id))
            .where(CognitoOrgMember.user_id == str(user_id))
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
            .select(CognitoOrgMember)
            .where(CognitoOrgMember.org_id == str(org_id))
            .order_by(CognitoOrgMember.created_at, asc=True)
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
            .select(CognitoOrgMember)
            .where(CognitoOrgMember.org_id == str(org_id))
            .where(CognitoOrgMember.user_id == str(user_id))
            .first_or_none()
        )
        return self._to_org_member_info(member) if member is not None else None

    # -- Organization session context -----------------------------------------

    async def set_active_org(
        self,
        *,
        session_id: str | uuid.UUID,
        org_id: str | uuid.UUID | None,
    ) -> TokenPair | None:
        """Switch the active organization for a Cognito user.

        Verifies membership and returns a ``TokenPair`` whose
        ``access_token`` is a signed ``X-Org-Context`` value.  The
        frontend should send this as the ``X-Org-Context`` header on
        subsequent requests.  ``authenticate()`` verifies the signature
        locally — no network call.

        Returns ``None`` if the user is not a member.
        """
        if org_id is None:
            return TokenPair(
                access_token="",
                refresh_token="",
                token_type="bearer",
                expires_in=0,
                expires_at=datetime.now(UTC),
            )

        user_id = str(session_id)
        member = await (
            self._db()
            .select(CognitoOrgMember)
            .where(CognitoOrgMember.org_id == str(org_id))
            .where(CognitoOrgMember.user_id == user_id)
            .first_or_none()
        )
        if member is None:
            return None

        signed = self._sign_org_context(user_id, str(org_id), member.role)
        return TokenPair(
            access_token=signed,
            refresh_token="",
            token_type="bearer",
            expires_in=0,
            expires_at=datetime.now(UTC),
        )

    # -- Secret hash ----------------------------------------------------------

    def _compute_secret_hash(self, username: str) -> str:
        """Compute the Cognito SECRET_HASH for app clients with a secret."""

        msg = username + self._config.client_id
        dig = hmac.new(
            self._config.client_secret.encode("utf-8"),
            msg.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        return base64.b64encode(dig).decode("utf-8")
