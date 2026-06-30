"""Tests for the Supabase authentication client."""

from __future__ import annotations

import hashlib
import hmac
import time
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import jwt as pyjwt
import pytest

from derp.auth.exceptions import (
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
from derp.auth.models import AuthStatus
from derp.config import (
    AuthConfig,
    JWTConfig,
    NativeAuthConfig,
    SupabaseConfig,
)
from derp.orm import DatabaseEngine

# ── Constants ─────────────────────────────────────────────────────

SUPABASE_URL = "https://test-project.supabase.co"
ANON_KEY = "test-anon-key"
SERVICE_ROLE_KEY = "test-service-role-key"
JWT_SECRET = "test-jwt-secret-at-least-32-chars-long!!"
TEST_EMAIL = "alice@example.com"
TEST_PASSWORD = "Str0ng!Pass123"
BASE_AUTH_URL = f"{SUPABASE_URL}/auth/v1"


# ── Fixtures ──────────────────────────────────────────────────────


@pytest.fixture
def supabase_config() -> SupabaseConfig:
    return SupabaseConfig(
        url=SUPABASE_URL,
        anon_key=ANON_KEY,
        service_role_key=SERVICE_ROLE_KEY,
        jwt_secret=JWT_SECRET,
    )


@pytest.fixture
def supabase_client(supabase_config: SupabaseConfig):
    from derp.auth.supabase_client import SupabaseAuthClient

    return SupabaseAuthClient(supabase_config)


@pytest.fixture
async def connected_client(supabase_client):
    """Client with a mocked httpx.AsyncClient so no real HTTP is made."""
    mock_http = AsyncMock()
    supabase_client._http = mock_http
    yield supabase_client
    supabase_client._http = None


def _make_jwt(
    *,
    sub: str = "user-123",
    session_id: str = "session-456",
    role: str = "authenticated",
    secret: str = JWT_SECRET,
    expired: bool = False,
    aud: str = "authenticated",
) -> str:
    now = datetime.now(UTC)
    exp = now - timedelta(hours=1) if expired else now + timedelta(hours=1)
    payload = {
        "sub": sub,
        "session_id": session_id,
        "role": role,
        "aud": aud,
        "iat": int(now.timestamp()),
        "exp": int(exp.timestamp()),
    }
    return pyjwt.encode(payload, secret, algorithm="HS256")


def _make_gotrue_user(
    *,
    user_id: str = "user-123",
    email: str = TEST_EMAIL,
    confirmed: bool = True,
) -> dict:
    now = datetime.now(UTC).isoformat()
    return {
        "id": user_id,
        "email": email,
        "email_confirmed_at": now if confirmed else None,
        "last_sign_in_at": now,
        "created_at": now,
        "updated_at": now,
        "banned_until": None,
        "user_metadata": {
            "first_name": "Alice",
            "last_name": "Smith",
            "avatar_url": "https://example.com/avatar.jpg",
        },
        "app_metadata": {
            "provider": "email",
            "role": "admin",
        },
        "role": "authenticated",
    }


def _make_auth_response(
    *,
    user_id: str = "user-123",
    email: str = TEST_EMAIL,
) -> dict:
    user = _make_gotrue_user(user_id=user_id, email=email)
    return {
        "access_token": _make_jwt(sub=user_id),
        "refresh_token": "refresh-token-abc",
        "token_type": "bearer",
        "expires_in": 3600,
        "expires_at": int(time.time()) + 3600,
        "user": user,
    }


def _mock_response(*, status_code: int = 200, json_data: dict | list | None = None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.is_success = 200 <= status_code < 300
    resp.json.return_value = json_data or {}
    resp.text = ""
    return resp


# ── Config ────────────────────────────────────────────────────────


class TestSupabaseConfig:
    def test_valid_config(self, supabase_config: SupabaseConfig) -> None:
        assert supabase_config.url == SUPABASE_URL
        assert supabase_config.anon_key == ANON_KEY
        assert supabase_config.service_role_key == SERVICE_ROLE_KEY
        assert supabase_config.jwt_secret == JWT_SECRET
        assert supabase_config.redirect_uri is None

    def test_mutual_exclusion_with_native(
        self, supabase_config: SupabaseConfig
    ) -> None:
        with pytest.raises(ValueError, match="Only one auth backend"):
            AuthConfig(
                supabase=supabase_config,
                native=NativeAuthConfig(jwt=JWTConfig(secret="s")),
            )

    def test_standalone_valid(self, supabase_config: SupabaseConfig) -> None:
        config = AuthConfig(supabase=supabase_config)
        assert config.supabase is not None


# ── Capabilities ──────────────────────────────────────────────────


class TestCapabilities:
    def test_supported_flags(self, supabase_client) -> None:
        assert supabase_client.supports_password is True
        assert supabase_client.supports_oauth is True
        assert supabase_client.supports_magic_link is True
        assert supabase_client.supports_orgs is True
        assert supabase_client.supports_user_admin is True

    def test_unsupported_flags(self, supabase_client) -> None:
        assert supabase_client.supports_mfa is False
        assert supabase_client.supports_passkeys is False
        assert supabase_client.supports_otp is False
        assert supabase_client.supports_anonymous is False
        assert supabase_client.supports_invitations is False
        assert supabase_client.supports_sso is False
        assert supabase_client.supports_multi_tenant is False
        assert supabase_client.supports_sessions is False


# ── verify_token / authenticate ───────────────────────────────────


class TestAuthenticate:
    async def test_verify_token_returns_session(self, supabase_client) -> None:
        token = _make_jwt()
        session = await supabase_client.verify_token(token)

        assert session is not None
        assert session.user_id == "user-123"
        assert session.session_id == "session-456"
        assert session.roles == ("authenticated",)
        assert session.tenant_id is None
        assert session.org_id is None
        assert session.is_anonymous is False
        assert session.mfa.enrolled is False
        assert session.mfa.satisfied is False
        assert session.claims["sub"] == "user-123"

    async def test_valid_token(self, supabase_client) -> None:
        token = _make_jwt()
        request = MagicMock()
        request.headers = {"Authorization": f"Bearer {token}"}

        session = await supabase_client.authenticate(request)

        assert session is not None
        assert session.user_id == "user-123"
        assert session.session_id == "session-456"
        assert session.roles == ("authenticated",)

    async def test_missing_auth_header(self, supabase_client) -> None:
        request = MagicMock()
        request.headers = {}
        assert await supabase_client.authenticate(request) is None

    async def test_invalid_token(self, supabase_client) -> None:
        request = MagicMock()
        request.headers = {"Authorization": "Bearer not-a-jwt"}
        assert await supabase_client.authenticate(request) is None

    async def test_expired_token(self, supabase_client) -> None:
        token = _make_jwt(expired=True)
        request = MagicMock()
        request.headers = {"Authorization": f"Bearer {token}"}
        assert await supabase_client.authenticate(request) is None

    async def test_wrong_secret(self, supabase_client) -> None:
        token = _make_jwt(secret="wrong-secret-that-is-long-enough!!")
        request = MagicMock()
        request.headers = {"Authorization": f"Bearer {token}"}
        assert await supabase_client.authenticate(request) is None

    async def test_org_context(self, supabase_client) -> None:
        token = _make_jwt()

        # Sign an org context header using the same HMAC pattern
        user_id = "user-123"
        org_id = "org-789"
        org_role = "owner"
        key = JWT_SECRET.encode()
        msg = f"{user_id}:{org_id}:{org_role}".encode()
        sig = hmac.new(key, msg, hashlib.sha256).hexdigest()
        org_header = f"{org_id}:{org_role}:{sig}"

        request = MagicMock()
        request.headers = {
            "Authorization": f"Bearer {token}",
            "X-Org-Context": org_header,
        }

        session = await supabase_client.authenticate(request)
        assert session is not None
        assert session.org_id == org_id
        assert session.org_role == org_role

    async def test_invalid_org_context(self, supabase_client) -> None:
        token = _make_jwt()
        request = MagicMock()
        request.headers = {
            "Authorization": f"Bearer {token}",
            "X-Org-Context": "org-789:owner:bad-signature",
        }

        session = await supabase_client.authenticate(request)
        assert session is not None
        assert session.org_id is None
        assert session.org_role is None


# ── Sign Up ───────────────────────────────────────────────────────


class TestSignUp:
    async def test_success(self, connected_client) -> None:
        response_data = _make_auth_response()
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(json_data=response_data)
        )

        outcome = await connected_client.sign_up(
            email=TEST_EMAIL, password=TEST_PASSWORD
        )

        assert outcome.status is AuthStatus.COMPLETE
        assert outcome.identity is not None
        assert outcome.identity.email == TEST_EMAIL
        assert outcome.tokens is not None
        assert outcome.tokens.access_token
        assert outcome.tokens.refresh_token == "refresh-token-abc"

        connected_client._http.post.assert_called_once()
        call_args = connected_client._http.post.call_args
        assert "signup" in call_args[0][0]

    async def test_confirmation_required(self, connected_client) -> None:
        # Tokenless response: GoTrue returns just the user pending confirmation.
        user = _make_gotrue_user(confirmed=False)
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(json_data={"user": user})
        )

        outcome = await connected_client.sign_up(
            email=TEST_EMAIL, password=TEST_PASSWORD
        )

        assert outcome.status is AuthStatus.VERIFICATION_REQUIRED
        assert outcome.identity is not None
        assert outcome.identity.email == TEST_EMAIL
        assert outcome.tokens is None

    async def test_email_taken(self, connected_client) -> None:
        # GoTrue's actual duplicate-email response: 422 + ``error_code``.
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(
                status_code=422,
                json_data={
                    "error_code": "user_already_exists",
                    "msg": "User already registered",
                },
            )
        )

        outcome = await connected_client.sign_up(
            email=TEST_EMAIL, password=TEST_PASSWORD
        )
        assert outcome.status is AuthStatus.EMAIL_EXISTS
        assert isinstance(outcome.error, EmailAlreadyExistsError)
        assert outcome.error.email == TEST_EMAIL


# ── Sign In With Password ─────────────────────────────────────────


class TestSignInWithPassword:
    async def test_success(self, connected_client) -> None:
        response_data = _make_auth_response()
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(json_data=response_data)
        )

        outcome = await connected_client.sign_in_with_password(
            identifier=TEST_EMAIL, password=TEST_PASSWORD
        )

        assert outcome.status is AuthStatus.COMPLETE
        assert outcome.identity is not None
        assert outcome.identity.email == TEST_EMAIL
        assert outcome.tokens is not None
        assert outcome.tokens.refresh_token == "refresh-token-abc"

        call_args = connected_client._http.post.call_args
        assert "token" in call_args[0][0]

    async def test_invalid_credentials(self, connected_client) -> None:
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(
                status_code=400,
                json_data={"error": "Invalid login credentials"},
            )
        )

        outcome = await connected_client.sign_in_with_password(
            identifier=TEST_EMAIL, password="wrong-password"
        )
        assert outcome.status is AuthStatus.INVALID_CREDENTIALS
        assert isinstance(outcome.error, InvalidCredentialsError)


# ── Refresh ───────────────────────────────────────────────────────


class TestRefresh:
    async def test_success(self, connected_client) -> None:
        response_data = _make_auth_response()
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(json_data=response_data)
        )

        tokens = (
            await connected_client.refresh("old-refresh-token")
        ).raise_for_status()

        assert tokens is not None
        assert tokens.access_token
        assert tokens.refresh_token == "refresh-token-abc"
        assert tokens.id_token == tokens.access_token

    async def test_invalid_refresh_token(self, connected_client) -> None:
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(
                status_code=400,
                json_data={"error": "Invalid Refresh Token"},
            )
        )

        result = await connected_client.refresh("bad-token")
        assert isinstance(result.error, InvalidTokenError)


# ── Sessions: revoke ──────────────────────────────────────────────


class TestRevokeSessions:
    async def test_revoke_session(self, connected_client) -> None:
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(status_code=204)
        )

        await connected_client.revoke_session("session-456")
        connected_client._http.post.assert_called_once()

    async def test_revoke_all_sessions(self, connected_client) -> None:
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(status_code=204)
        )

        await connected_client.revoke_all_sessions("user-123")
        connected_client._http.post.assert_called_once()


# ── User CRUD (Admin API) ────────────────────────────────────────


class TestGetUser:
    async def test_success(self, connected_client) -> None:
        user_data = _make_gotrue_user()
        connected_client._http.get = AsyncMock(
            return_value=_mock_response(json_data=user_data)
        )

        identity = await connected_client.get_user("user-123")

        assert identity is not None
        assert identity.id == "user-123"
        assert identity.email == TEST_EMAIL
        assert identity.email_verified is True
        assert identity.roles == ("admin",)
        assert identity.tenant_id is None
        assert identity.disabled is False
        assert identity.metadata == {}

    async def test_not_found(self, connected_client) -> None:
        connected_client._http.get = AsyncMock(
            return_value=_mock_response(
                status_code=404, json_data={"error": "User not found"}
            )
        )

        with pytest.raises(UserNotFoundError):
            await connected_client.get_user("nonexistent")


class TestListUsers:
    async def test_list(self, connected_client) -> None:
        users = [
            _make_gotrue_user(user_id="u1", email="a@test.com"),
            _make_gotrue_user(user_id="u2", email="b@test.com"),
        ]
        connected_client._http.get = AsyncMock(
            return_value=_mock_response(json_data={"users": users})
        )

        page = await connected_client.list_users(limit=10)

        assert len(page.items) == 2
        emails = {u.email for u in page.items}
        assert "a@test.com" in emails
        assert "b@test.com" in emails
        # Fewer items than limit → no more pages.
        assert page.has_more is False
        assert page.next_cursor is None

    async def test_pagination_cursor(self, connected_client) -> None:
        # A full page signals more results; cursor advances the page number.
        users = [_make_gotrue_user(user_id=f"u{i}") for i in range(5)]
        connected_client._http.get = AsyncMock(
            return_value=_mock_response(json_data={"users": users})
        )

        page = await connected_client.list_users(limit=5, cursor="2")

        call_args = connected_client._http.get.call_args
        params = call_args[1].get("params", {})
        assert params.get("per_page") == 5
        assert params.get("page") == 2
        assert page.has_more is True
        assert page.next_cursor == "3"


class TestUpdateUser:
    async def test_success(self, connected_client) -> None:
        updated = _make_gotrue_user(email="new@test.com")
        connected_client._http.put = AsyncMock(
            return_value=_mock_response(json_data=updated)
        )

        identity = (
            await connected_client.update_user(user_id="user-123", email="new@test.com")
        ).raise_for_status()

        assert identity is not None
        assert identity.email == "new@test.com"

    async def test_not_found(self, connected_client) -> None:
        connected_client._http.put = AsyncMock(
            return_value=_mock_response(
                status_code=404, json_data={"error": "User not found"}
            )
        )

        result = await connected_client.update_user(
            user_id="nonexistent", email="x@test.com"
        )
        assert isinstance(result.error, UserNotFoundError)


class TestDeleteUser:
    async def test_success(self, connected_client) -> None:
        connected_client._http.delete = AsyncMock(
            return_value=_mock_response(status_code=200)
        )

        result = await connected_client.delete_user("user-123")
        assert result is True

    async def test_not_found(self, connected_client) -> None:
        connected_client._http.delete = AsyncMock(
            return_value=_mock_response(
                status_code=404, json_data={"error": "User not found"}
            )
        )

        result = await connected_client.delete_user("nonexistent")
        assert result is False


class TestFindUser:
    async def test_by_user_id(self, connected_client) -> None:
        user_data = _make_gotrue_user()
        connected_client._http.get = AsyncMock(
            return_value=_mock_response(json_data=user_data)
        )

        identity = await connected_client.find_user(user_id="user-123")
        assert identity is not None
        assert identity.id == "user-123"

    async def test_missing_returns_none(self, connected_client) -> None:
        connected_client._http.get = AsyncMock(
            return_value=_mock_response(
                status_code=404, json_data={"error": "User not found"}
            )
        )

        assert await connected_client.find_user(user_id="nope") is None

    async def test_email_unsupported(self, connected_client) -> None:
        with pytest.raises(NotImplementedError):
            await connected_client.find_user(email=TEST_EMAIL)

    async def test_requires_exactly_one_key(self, connected_client) -> None:
        with pytest.raises(ValueError):
            await connected_client.find_user()


# ── Password Recovery ─────────────────────────────────────────────


class TestPasswordRecovery:
    async def test_request_reset(self, connected_client) -> None:
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(status_code=200)
        )

        await connected_client.request_password_reset(email=TEST_EMAIL)

        connected_client._http.post.assert_called_once()
        call_args = connected_client._http.post.call_args
        assert "recover" in call_args[0][0]

    async def test_reset_password(self, connected_client) -> None:
        user_data = _make_gotrue_user()
        connected_client._http.put = AsyncMock(
            return_value=_mock_response(json_data=user_data)
        )

        identity = (
            await connected_client.reset_password("reset-token", "NewP@ss123")
        ).raise_for_status()
        assert identity is not None
        assert identity.id == "user-123"

    async def test_reset_password_invalid(self, connected_client) -> None:
        connected_client._http.put = AsyncMock(
            return_value=_mock_response(status_code=401, json_data={})
        )

        result = await connected_client.reset_password("bad-token", "NewP@ss123")
        assert isinstance(result.error, InvalidTokenError)


# ── Magic Link ────────────────────────────────────────────────────


class TestMagicLink:
    async def test_send_magic_link(self, connected_client) -> None:
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(status_code=200)
        )

        await connected_client.send_magic_link(
            email=TEST_EMAIL, redirect_url="https://app.com/magic"
        )

        connected_client._http.post.assert_called_once()
        call_args = connected_client._http.post.call_args
        assert "otp" in call_args[0][0]

    async def test_verify_magic_link(self, connected_client) -> None:
        response_data = _make_auth_response()
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(json_data=response_data)
        )

        outcome = await connected_client.verify_magic_link("otp-token-123")

        assert outcome.status is AuthStatus.COMPLETE
        assert outcome.identity is not None
        assert outcome.identity.email == TEST_EMAIL

    async def test_verify_magic_link_invalid(self, connected_client) -> None:
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(status_code=401, json_data={})
        )

        outcome = await connected_client.verify_magic_link("bad-token")
        assert outcome.status is AuthStatus.INVALID_TOKEN
        assert isinstance(outcome.error, InvalidTokenError)


# ── OAuth ─────────────────────────────────────────────────────────


class TestOAuth:
    def test_authorization_url(self, supabase_client) -> None:
        url = supabase_client.authorization_url(
            provider="google",
            state="random-state",
            redirect_uri="https://app.com/callback",
        )

        assert f"{SUPABASE_URL}/auth/v1/authorize" in url
        assert "provider=google" in url
        assert "state=random-state" in url
        assert "redirect_to=" in url

    def test_authorization_url_with_scopes(self, supabase_client) -> None:
        url = supabase_client.authorization_url(
            provider="github",
            state="state",
            scopes=["user:email", "read:org"],
        )

        assert "scopes=" in url

    async def test_sign_in_with_oauth(self, connected_client) -> None:
        response_data = _make_auth_response()
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(json_data=response_data)
        )

        outcome = await connected_client.sign_in_with_oauth(
            "auth-code-123",
            provider="google",
            redirect_uri="https://app.com/callback",
        )

        assert outcome.status is AuthStatus.COMPLETE
        assert outcome.identity is not None
        assert outcome.identity.email == TEST_EMAIL
        assert outcome.tokens is not None
        assert outcome.tokens.access_token

    async def test_sign_in_with_oauth_rejected(self, connected_client) -> None:
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(status_code=400, json_data={})
        )

        outcome = await connected_client.sign_in_with_oauth(
            "bad-code", provider="google"
        )
        assert outcome.status is AuthStatus.INVALID_CREDENTIALS
        assert isinstance(outcome.error, InvalidCredentialsError)


# ── Sessions: list is unsupported ─────────────────────────────────


class TestListSessionsUnsupported:
    async def test_list_sessions_raises(self, supabase_client) -> None:
        from derp.auth.exceptions import CapabilityNotSupportedError

        with pytest.raises(CapabilityNotSupportedError):
            await supabase_client.list_sessions()


# ── Organizations (database-backed) ───────────────────────────────
#
# Orgs live in Postgres for the Supabase backend, so these run against a real
# database. Admin methods address an org by exactly one of ``org_id=`` or
# ``slug=``; backends select on whichever was given (no resolve round-trip).


async def _create_supabase_org_tables(db: DatabaseEngine) -> None:
    await db.execute("""
        CREATE TABLE IF NOT EXISTS organizations (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name VARCHAR(255) NOT NULL,
            slug VARCHAR(255) UNIQUE NOT NULL,
            metadata TEXT,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        )
    """)
    # Supabase keys members by the GoTrue user id (a string), no users FK.
    await db.execute("""
        CREATE TABLE IF NOT EXISTS org_members (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
            user_id VARCHAR(255) NOT NULL,
            role VARCHAR(50) NOT NULL DEFAULT 'member',
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            UNIQUE (org_id, user_id)
        )
    """)


@pytest.fixture
async def org_db(clean_database: str) -> AsyncGenerator[DatabaseEngine, None]:
    engine = DatabaseEngine(clean_database)
    await engine.connect()
    await _create_supabase_org_tables(engine)
    yield engine
    await engine.disconnect()


@pytest.fixture
def org_client(supabase_config: SupabaseConfig, org_db: DatabaseEngine):
    from derp.auth.supabase_client import SupabaseAuthClient

    client = SupabaseAuthClient(supabase_config)
    client.set_db(org_db)
    return client


class TestOrgCrud:
    async def test_create_and_get_by_id(self, org_client) -> None:
        created = (
            await org_client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        assert created.slug == "acme"

        fetched = (await org_client.get_org(org_id=created.id)).raise_for_status()
        assert fetched.id == created.id
        assert fetched.name == "Acme"

    async def test_get_by_slug_and_by_id_match(self, org_client) -> None:
        created = (
            await org_client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        by_slug = (await org_client.get_org(slug="acme")).raise_for_status()
        by_id = (await org_client.get_org(org_id=created.id)).raise_for_status()
        assert by_slug.id == by_id.id == created.id

    async def test_get_requires_exactly_one_ref(self, org_client) -> None:
        with pytest.raises(ValueError):
            await org_client.get_org()
        with pytest.raises(ValueError):
            await org_client.get_org(org_id="x", slug="y")

    async def test_get_missing_slug_returns_error(self, org_client) -> None:
        result = await org_client.get_org(slug="does-not-exist")
        assert isinstance(result.error, OrgNotFoundError)

    async def test_get_missing_uuid_returns_error(self, org_client) -> None:
        result = await org_client.get_org(org_id=str(uuid.uuid4()))
        assert isinstance(result.error, OrgNotFoundError)

    async def test_update_by_slug(self, org_client) -> None:
        created = (
            await org_client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        updated = (
            await org_client.update_org(
                slug="acme", name="Acme Inc", new_slug="acme-inc"
            )
        ).raise_for_status()
        assert updated.id == created.id
        assert updated.name == "Acme Inc"
        assert updated.slug == "acme-inc"

    async def test_update_by_id(self, org_client) -> None:
        created = (
            await org_client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        updated = (
            await org_client.update_org(org_id=created.id, name="Renamed")
        ).raise_for_status()
        assert updated.name == "Renamed"
        assert updated.slug == "acme"

    async def test_update_missing_returns_error(self, org_client) -> None:
        result = await org_client.update_org(slug="nope", name="x")
        assert isinstance(result.error, OrgNotFoundError)

    async def test_update_slug_conflict(self, org_client) -> None:
        (
            await org_client.create_org(name="Acme", slug="acme", creator_id="o1")
        ).raise_for_status()
        (
            await org_client.create_org(name="Beta", slug="beta", creator_id="o2")
        ).raise_for_status()
        result = await org_client.update_org(slug="beta", new_slug="acme")
        assert isinstance(result.error, OrgSlugConflictError)

    async def test_delete_by_slug(self, org_client) -> None:
        (
            await org_client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        assert await org_client.delete_org(slug="acme") is True
        result = await org_client.get_org(slug="acme")
        assert isinstance(result.error, OrgNotFoundError)

    async def test_delete_by_id(self, org_client) -> None:
        created = (
            await org_client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        assert await org_client.delete_org(org_id=created.id) is True

    async def test_delete_missing_returns_false(self, org_client) -> None:
        assert await org_client.delete_org(slug="nope") is False
        assert await org_client.delete_org(org_id=str(uuid.uuid4())) is False


class TestOrgMembership:
    async def test_add_list_update_remove_by_id(self, org_client) -> None:
        org = (
            await org_client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()

        member = (
            await org_client.add_member(org_id=org.id, user_id="u2", role="member")
        ).raise_for_status()
        assert member.role == "member"

        updated = (
            await org_client.update_member(org_id=org.id, user_id="u2", role="admin")
        ).raise_for_status()
        assert updated.role == "admin"

        members = await org_client.list_members(org_id=org.id)
        assert {m.user_id for m in members.items} == {"owner-1", "u2"}

        removed = (
            await org_client.remove_member(org_id=org.id, user_id="u2")
        ).raise_for_status()
        assert removed is True
        members = await org_client.list_members(org_id=org.id)
        assert {m.user_id for m in members.items} == {"owner-1"}

    async def test_add_list_update_remove_by_slug(self, org_client) -> None:
        org = (
            await org_client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        # Address the org by its slug rather than its id.
        member = (
            await org_client.add_member(slug="acme", user_id="u2", role="member")
        ).raise_for_status()
        assert member.org_id == org.id
        assert member.role == "member"

        # And list/update/remove also work addressed by slug.
        members = await org_client.list_members(slug="acme")
        assert {m.user_id for m in members.items} == {"owner-1", "u2"}

        updated = (
            await org_client.update_member(slug="acme", user_id="u2", role="admin")
        ).raise_for_status()
        assert updated.role == "admin"
        removed = (
            await org_client.remove_member(slug="acme", user_id="u2")
        ).raise_for_status()
        assert removed is True

    async def test_add_member_requires_exactly_one_ref(self, org_client) -> None:
        with pytest.raises(ValueError):
            await org_client.add_member(user_id="u2")
        with pytest.raises(ValueError):
            await org_client.add_member(user_id="u2", org_id="x", slug="y")

    async def test_add_member_slug_not_found(self, org_client) -> None:
        result = await org_client.add_member(slug="ghost", user_id="u2")
        assert isinstance(result.error, OrgNotFoundError)

    async def test_add_duplicate_member(self, org_client) -> None:
        org = (
            await org_client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        (await org_client.add_member(org_id=org.id, user_id="u2")).raise_for_status()
        result = await org_client.add_member(org_id=org.id, user_id="u2")
        assert isinstance(result.error, MemberAlreadyExistsError)

    async def test_update_member_slug_not_found(self, org_client) -> None:
        result = await org_client.update_member(
            slug="ghost", user_id="u2", role="admin"
        )
        assert isinstance(result.error, OrgMemberNotFoundError)

    async def test_update_member_not_a_member(self, org_client) -> None:
        org = (
            await org_client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        result = await org_client.update_member(
            org_id=org.id, user_id="ghost", role="admin"
        )
        assert isinstance(result.error, OrgMemberNotFoundError)

    async def test_remove_member_not_a_member_returns_false(self, org_client) -> None:
        org = (
            await org_client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        result = await org_client.remove_member(org_id=org.id, user_id="ghost")
        assert result.ok
        assert result.value is False

    async def test_remove_missing_org_returns_false(self, org_client) -> None:
        result = await org_client.remove_member(slug="ghost", user_id="u2")
        assert result.ok
        assert result.value is False

    async def test_remove_last_owner_returns_error(self, org_client) -> None:
        org = (
            await org_client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        result = await org_client.remove_member(org_id=org.id, user_id="owner-1")
        assert isinstance(result.error, LastOwnerError)

    async def test_list_members_unknown_org_empty(self, org_client) -> None:
        page = await org_client.list_members(slug="ghost")
        assert page.items == []
        assert page.has_more is False


class TestSetActiveOrg:
    async def test_by_slug_and_id(self, org_client) -> None:
        org = (
            await org_client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        by_id = (
            await org_client.set_active_org(session_id="owner-1", org_id=org.id)
        ).raise_for_status()
        by_slug = (
            await org_client.set_active_org(session_id="owner-1", slug="acme")
        ).raise_for_status()
        assert by_id.access_token
        # Both address the same canonical org, so the signed context matches.
        assert by_id.access_token == by_slug.access_token

    async def test_clear(self, org_client) -> None:
        tokens = (
            await org_client.set_active_org(session_id="owner-1")
        ).raise_for_status()
        assert tokens.access_token == ""

    async def test_not_a_member(self, org_client) -> None:
        (
            await org_client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        result = await org_client.set_active_org(session_id="stranger", slug="acme")
        assert isinstance(result.error, OrgMemberNotFoundError)

    async def test_missing_org(self, org_client) -> None:
        result = await org_client.set_active_org(session_id="owner-1", slug="ghost")
        assert isinstance(result.error, OrgMemberNotFoundError)
