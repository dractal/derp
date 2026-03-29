"""Tests for the Supabase authentication client."""

from __future__ import annotations

import hashlib
import hmac
import time
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import jwt as pyjwt
import pytest

from derp.config import (
    AuthConfig,
    ClerkConfig,
    CognitoConfig,
    JWTConfig,
    NativeAuthConfig,
    SupabaseConfig,
)

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

    def test_mutual_exclusion_with_clerk(self, supabase_config: SupabaseConfig) -> None:
        with pytest.raises(ValueError, match="Only one auth backend"):
            AuthConfig(
                supabase=supabase_config,
                clerk=ClerkConfig(secret_key="sk"),
            )

    def test_mutual_exclusion_with_cognito(
        self,
        supabase_config: SupabaseConfig,
    ) -> None:
        with pytest.raises(ValueError, match="Only one auth backend"):
            AuthConfig(
                supabase=supabase_config,
                cognito=CognitoConfig(
                    user_pool_id="us-east-1_x",
                    client_id="c",
                    client_secret="s",
                    region="us-east-1",
                ),
            )

    def test_standalone_valid(self, supabase_config: SupabaseConfig) -> None:
        config = AuthConfig(supabase=supabase_config)
        assert config.supabase is not None


# ── Authenticate ──────────────────────────────────────────────────


class TestAuthenticate:
    async def test_valid_token(self, supabase_client) -> None:
        token = _make_jwt()
        request = MagicMock()
        request.headers = {"Authorization": f"Bearer {token}"}

        session = await supabase_client.authenticate(request)

        assert session is not None
        assert session.user_id == "user-123"
        assert session.session_id == "session-456"
        assert session.role == "authenticated"

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

        result = await connected_client.sign_up(
            email=TEST_EMAIL, password=TEST_PASSWORD
        )

        assert result is not None
        assert result.user.email == TEST_EMAIL
        assert result.tokens.access_token
        assert result.tokens.refresh_token == "refresh-token-abc"

        connected_client._http.post.assert_called_once()
        call_args = connected_client._http.post.call_args
        assert "signup" in call_args[0][0]

    async def test_email_taken(self, connected_client) -> None:
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(
                status_code=400,
                json_data={"error": "User already registered"},
            )
        )

        result = await connected_client.sign_up(
            email=TEST_EMAIL, password=TEST_PASSWORD
        )
        assert result is None


# ── Sign In With Password ─────────────────────────────────────────


class TestSignInWithPassword:
    async def test_success(self, connected_client) -> None:
        response_data = _make_auth_response()
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(json_data=response_data)
        )

        result = await connected_client.sign_in_with_password(TEST_EMAIL, TEST_PASSWORD)

        assert result is not None
        assert result.user.email == TEST_EMAIL
        assert result.tokens.refresh_token == "refresh-token-abc"

        call_args = connected_client._http.post.call_args
        assert "token" in call_args[0][0]

    async def test_invalid_credentials(self, connected_client) -> None:
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(
                status_code=400,
                json_data={"error": "Invalid login credentials"},
            )
        )

        result = await connected_client.sign_in_with_password(
            TEST_EMAIL, "wrong-password"
        )
        assert result is None


# ── Refresh Token ─────────────────────────────────────────────────


class TestRefreshToken:
    async def test_success(self, connected_client) -> None:
        response_data = _make_auth_response()
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(json_data=response_data)
        )

        result = await connected_client.refresh_token("old-refresh-token")

        assert result is not None
        assert result.access_token
        assert result.refresh_token == "refresh-token-abc"

    async def test_invalid_refresh_token(self, connected_client) -> None:
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(
                status_code=400,
                json_data={"error": "Invalid Refresh Token"},
            )
        )

        result = await connected_client.refresh_token("bad-token")
        assert result is None


# ── Sign Out ──────────────────────────────────────────────────────


class TestSignOut:
    async def test_sign_out(self, connected_client) -> None:
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(status_code=204)
        )

        await connected_client.sign_out("session-456")
        connected_client._http.post.assert_called_once()

    async def test_sign_out_all(self, connected_client) -> None:
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(status_code=204)
        )

        await connected_client.sign_out_all("user-123")
        connected_client._http.post.assert_called_once()


# ── User CRUD (Admin API) ────────────────────────────────────────


class TestGetUser:
    async def test_success(self, connected_client) -> None:
        user_data = _make_gotrue_user()
        connected_client._http.get = AsyncMock(
            return_value=_mock_response(json_data=user_data)
        )

        user = await connected_client.get_user("user-123")

        assert user is not None
        assert user.id == "user-123"
        assert user.email == TEST_EMAIL
        assert user.first_name == "Alice"
        assert user.last_name == "Smith"
        assert user.image_url == "https://example.com/avatar.jpg"
        assert user.role == "admin"

    async def test_not_found(self, connected_client) -> None:
        connected_client._http.get = AsyncMock(
            return_value=_mock_response(
                status_code=404, json_data={"error": "User not found"}
            )
        )

        user = await connected_client.get_user("nonexistent")
        assert user is None


class TestListUsers:
    async def test_list(self, connected_client) -> None:
        users = [
            _make_gotrue_user(user_id="u1", email="a@test.com"),
            _make_gotrue_user(user_id="u2", email="b@test.com"),
        ]
        connected_client._http.get = AsyncMock(
            return_value=_mock_response(json_data={"users": users})
        )

        result = await connected_client.list_users(limit=10)

        assert len(result) == 2
        emails = {u.email for u in result}
        assert "a@test.com" in emails
        assert "b@test.com" in emails

    async def test_with_pagination(self, connected_client) -> None:
        connected_client._http.get = AsyncMock(
            return_value=_mock_response(json_data={"users": []})
        )

        await connected_client.list_users(limit=5, offset=10)

        call_args = connected_client._http.get.call_args
        params = call_args[1].get("params", {})
        assert params.get("per_page") == 5
        assert params.get("page") == 3  # offset=10, limit=5 → page 3


class TestUpdateUser:
    async def test_success(self, connected_client) -> None:
        updated = _make_gotrue_user(email="new@test.com")
        connected_client._http.put = AsyncMock(
            return_value=_mock_response(json_data=updated)
        )

        user = await connected_client.update_user(
            user_id="user-123", email="new@test.com"
        )

        assert user is not None
        assert user.email == "new@test.com"

    async def test_not_found(self, connected_client) -> None:
        connected_client._http.put = AsyncMock(
            return_value=_mock_response(
                status_code=404, json_data={"error": "User not found"}
            )
        )

        user = await connected_client.update_user(
            user_id="nonexistent", email="x@test.com"
        )
        assert user is None


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


class TestCountUsers:
    async def test_count(self, connected_client) -> None:
        users = [_make_gotrue_user(user_id=f"u{i}") for i in range(3)]
        resp = _mock_response(json_data={"users": users})
        resp.headers = {"x-total-count": "42"}
        connected_client._http.get = AsyncMock(return_value=resp)

        count = await connected_client.count_users()
        assert count == 42

    async def test_count_fallback(self, connected_client) -> None:
        """Falls back to len(users) if x-total-count header is missing."""
        users = [_make_gotrue_user(user_id=f"u{i}") for i in range(3)]
        resp = _mock_response(json_data={"users": users})
        resp.headers = {}
        connected_client._http.get = AsyncMock(return_value=resp)

        count = await connected_client.count_users()
        assert count == 3


# ── Password Recovery ─────────────────────────────────────────────


class TestPasswordRecovery:
    async def test_request_recovery(self, connected_client) -> None:
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(status_code=200)
        )

        await connected_client.request_password_recovery(
            email=TEST_EMAIL, recovery_url="https://app.com/reset"
        )

        connected_client._http.post.assert_called_once()
        call_args = connected_client._http.post.call_args
        assert "recover" in call_args[0][0]


# ── Magic Link ────────────────────────────────────────────────────


class TestMagicLink:
    async def test_send_magic_link(self, connected_client) -> None:
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(status_code=200)
        )

        await connected_client.sign_in_with_magic_link(
            email=TEST_EMAIL, magic_link_url="https://app.com/magic"
        )

        connected_client._http.post.assert_called_once()
        call_args = connected_client._http.post.call_args
        assert "otp" in call_args[0][0]

    async def test_verify_magic_link(self, connected_client) -> None:
        response_data = _make_auth_response()
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(json_data=response_data)
        )

        result = await connected_client.verify_magic_link("otp-token-123")

        assert result is not None
        assert result.user.email == TEST_EMAIL


# ── OAuth ─────────────────────────────────────────────────────────


class TestOAuth:
    def test_get_authorization_url(self, supabase_client) -> None:
        url = supabase_client.get_oauth_authorization_url(
            "google",
            state="random-state",
            redirect_uri="https://app.com/callback",
        )

        assert f"{SUPABASE_URL}/auth/v1/authorize" in url
        assert "provider=google" in url
        assert "state=random-state" in url
        assert "redirect_to=" in url

    def test_get_authorization_url_with_scopes(self, supabase_client) -> None:
        url = supabase_client.get_oauth_authorization_url(
            "github",
            state="state",
            scopes=["user:email", "read:org"],
        )

        assert "scopes=" in url

    async def test_sign_in_with_oauth(self, connected_client) -> None:
        response_data = _make_auth_response()
        connected_client._http.post = AsyncMock(
            return_value=_mock_response(json_data=response_data)
        )

        result = await connected_client.sign_in_with_oauth(
            "google", "auth-code-123", redirect_uri="https://app.com/callback"
        )

        assert result is not None
        assert result.user.email == TEST_EMAIL
        assert result.tokens.access_token


# ── Sessions ──────────────────────────────────────────────────────


class TestSessions:
    async def test_list_sessions_returns_empty(self, supabase_client) -> None:
        result = await supabase_client.list_sessions()
        assert result == []
