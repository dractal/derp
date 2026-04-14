"""Tests for the WorkOS authentication client."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import jwt as pyjwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from workos.exceptions import (
    AuthenticationException,
    BadRequestException,
    ConflictException,
    NotFoundException,
)

from derp.config import (
    AuthConfig,
    ClerkConfig,
    CognitoConfig,
    JWTConfig,
    NativeAuthConfig,
    SupabaseConfig,
    WorkOSConfig,
)

# ── Constants ─────────────────────────────────────────────────────

API_KEY = "sk_test_workos_api_key"
CLIENT_ID = "client_01ABC"
TEST_EMAIL = "alice@example.com"
TEST_PASSWORD = "Str0ng!Pass123"

# ── Fixtures ──────────────────────────────────────────────────────


@pytest.fixture
def workos_config() -> WorkOSConfig:
    return WorkOSConfig(api_key=API_KEY, client_id=CLIENT_ID)


@pytest.fixture
def workos_client(workos_config: WorkOSConfig):
    from derp.auth.workos_client import WorkOSAuthClient

    return WorkOSAuthClient(workos_config)


@pytest.fixture
def connected_client(workos_client):
    """Client with mocked internal AsyncWorkOSClient."""
    mock_workos = MagicMock()
    mock_workos.user_management = MagicMock()
    mock_workos.organizations = MagicMock()
    workos_client._workos = mock_workos
    return workos_client


# ── RSA keypair / JWKS helpers ───────────────────────────────────


@pytest.fixture
def rsa_keypair():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key, private_key.public_key()


def _make_jwt(
    private_key,
    *,
    sub: str = "user_01XYZ",
    sid: str = "session_01ABC",
    org_id: str | None = None,
    role: str | None = None,
    expired: bool = False,
    kid: str = "test-kid-1",
) -> str:
    now = datetime.now(UTC)
    exp = now - timedelta(hours=1) if expired else now + timedelta(hours=1)
    payload: dict = {
        "sub": sub,
        "sid": sid,
        "iat": int(now.timestamp()),
        "exp": int(exp.timestamp()),
    }
    if org_id:
        payload["org_id"] = org_id
    if role:
        payload["role"] = role
    return pyjwt.encode(
        payload,
        private_key,
        algorithm="RS256",
        headers={"kid": kid},
    )


def _workos_exc(exc_class: type, message: str = "Not Found"):
    """Create a WorkOS SDK exception with a mocked httpx response."""
    resp = MagicMock()
    resp.headers = {"X-Request-ID": "req_test"}
    return exc_class(response=resp, response_json={"message": message})


def _mock_jwks_client(rsa_keypair):
    """Return a mock PyJWKClient that returns the test public key."""
    _, public_key = rsa_keypair
    mock_client = MagicMock()
    mock_signing_key = MagicMock()
    mock_signing_key.key = public_key
    mock_client.get_signing_key_from_jwt = MagicMock(return_value=mock_signing_key)
    return mock_client


def _make_workos_user(
    *,
    user_id: str = "user_01XYZ",
    email: str = TEST_EMAIL,
    first_name: str | None = "Alice",
    last_name: str | None = "Smith",
    email_verified: bool = True,
) -> MagicMock:
    now = datetime.now(UTC).isoformat()
    user = MagicMock()
    user.id = user_id
    user.email = email
    user.first_name = first_name
    user.last_name = last_name
    user.email_verified = email_verified
    user.profile_picture_url = "https://example.com/avatar.jpg"
    user.last_sign_in_at = now
    user.created_at = now
    user.updated_at = now
    user.external_id = None
    user.metadata = {}
    return user


def _make_workos_org(
    *,
    org_id: str = "org_01ABC",
    name: str = "Acme Corp",
) -> MagicMock:
    now = datetime.now(UTC).isoformat()
    org = MagicMock()
    org.id = org_id
    org.name = name
    org.created_at = now
    org.updated_at = now
    org.metadata = {}
    org.domains = []
    return org


def _make_workos_membership(
    *,
    membership_id: str = "om_01ABC",
    org_id: str = "org_01ABC",
    user_id: str = "user_01XYZ",
    role_slug: str = "member",
) -> MagicMock:
    now = datetime.now(UTC).isoformat()
    membership = MagicMock()
    membership.id = membership_id
    membership.organization_id = org_id
    membership.organization_name = "Acme Corp"
    membership.user_id = user_id
    membership.role = {"slug": role_slug}
    membership.status = "active"
    membership.created_at = now
    membership.updated_at = now
    return membership


def _make_access_token(sub: str = "user_01XYZ") -> str:
    """Create a minimal unsigned JWT for testing token parsing."""
    now = datetime.now(UTC)
    payload = {
        "sub": sub,
        "exp": int((now + timedelta(hours=1)).timestamp()),
        "iat": int(now.timestamp()),
    }
    return pyjwt.encode(payload, "test-secret", algorithm="HS256")


def _make_auth_response(
    *,
    user_id: str = "user_01XYZ",
    email: str = TEST_EMAIL,
) -> MagicMock:
    resp = MagicMock()
    resp.user = _make_workos_user(user_id=user_id, email=email)
    resp.access_token = _make_access_token(sub=user_id)
    resp.refresh_token = "refresh-token-abc"
    resp.authentication_method = "Password"
    resp.organization_id = None
    resp.sealed_session = None
    return resp


def _make_list_resource(items: list) -> MagicMock:
    """Mock a WorkOSListResource that supports iteration and .data."""
    resource = MagicMock()
    resource.data = items
    resource.__iter__ = MagicMock(return_value=iter(items))
    resource.list_metadata = MagicMock()
    resource.list_metadata.after = None
    return resource


def _make_session(
    *,
    session_id: str = "session_01ABC",
    user_id: str = "user_01XYZ",
    org_id: str | None = None,
    status: str = "active",
) -> MagicMock:
    now = datetime.now(UTC).isoformat()
    session = MagicMock()
    session.id = session_id
    session.user_id = user_id
    session.organization_id = org_id
    session.status = status
    session.expires_at = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    session.created_at = now
    session.updated_at = now
    return session


# ── Config ────────────────────────────────────────────────────────


class TestWorkOSConfig:
    def test_valid_config(self, workos_config: WorkOSConfig) -> None:
        assert workos_config.api_key == API_KEY
        assert workos_config.client_id == CLIENT_ID
        assert workos_config.redirect_uri is None

    def test_standalone_valid(self, workos_config: WorkOSConfig) -> None:
        config = AuthConfig(workos=workos_config)
        assert config.workos is not None

    def test_mutual_exclusion_with_native(self, workos_config: WorkOSConfig) -> None:
        with pytest.raises(ValueError, match="Only one auth backend"):
            AuthConfig(
                workos=workos_config,
                native=NativeAuthConfig(jwt=JWTConfig(secret="s")),
            )

    def test_mutual_exclusion_with_clerk(self, workos_config: WorkOSConfig) -> None:
        with pytest.raises(ValueError, match="Only one auth backend"):
            AuthConfig(workos=workos_config, clerk=ClerkConfig(secret_key="sk"))

    def test_mutual_exclusion_with_cognito(self, workos_config: WorkOSConfig) -> None:
        with pytest.raises(ValueError, match="Only one auth backend"):
            AuthConfig(
                workos=workos_config,
                cognito=CognitoConfig(
                    user_pool_id="us-east-1_x",
                    client_id="c",
                    client_secret="s",
                    region="us-east-1",
                ),
            )

    def test_mutual_exclusion_with_supabase(self, workos_config: WorkOSConfig) -> None:
        with pytest.raises(ValueError, match="Only one auth backend"):
            AuthConfig(
                workos=workos_config,
                supabase=SupabaseConfig(
                    url="https://x.supabase.co",
                    anon_key="a",
                    service_role_key="s",
                    jwt_secret="j" * 32,
                ),
            )


# ── Authenticate ──────────────────────────────────────────────────


class TestAuthenticate:
    async def test_valid_token(self, workos_client, rsa_keypair) -> None:
        private_key, _ = rsa_keypair
        token = _make_jwt(private_key)
        request = MagicMock()
        request.headers = {"Authorization": f"Bearer {token}"}

        workos_client._jwks_client = _mock_jwks_client(rsa_keypair)
        session = await workos_client.authenticate(request)

        assert session is not None
        assert session.user_id == "user_01XYZ"
        assert session.session_id == "session_01ABC"

    async def test_missing_auth_header(self, workos_client, rsa_keypair) -> None:
        workos_client._jwks_client = _mock_jwks_client(rsa_keypair)
        request = MagicMock()
        request.headers = {}
        assert await workos_client.authenticate(request) is None

    async def test_invalid_token(self, workos_client) -> None:
        request = MagicMock()
        request.headers = {"Authorization": "Bearer not-a-jwt"}

        mock_client = MagicMock()
        mock_client.get_signing_key_from_jwt = MagicMock(
            side_effect=pyjwt.exceptions.PyJWKClientError("Invalid token")
        )
        workos_client._jwks_client = mock_client
        assert await workos_client.authenticate(request) is None

    async def test_expired_token(self, workos_client, rsa_keypair) -> None:
        private_key, _ = rsa_keypair
        token = _make_jwt(private_key, expired=True)
        request = MagicMock()
        request.headers = {"Authorization": f"Bearer {token}"}

        workos_client._jwks_client = _mock_jwks_client(rsa_keypair)
        session = await workos_client.authenticate(request)
        assert session is None

    async def test_unknown_kid(self, workos_client) -> None:
        request = MagicMock()
        request.headers = {
            "Authorization": "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6InVuay0xIn0.e30.sig"
        }

        mock_client = MagicMock()
        mock_client.get_signing_key_from_jwt = MagicMock(
            side_effect=pyjwt.exceptions.PyJWKClientError(
                "Unable to find a signing key"
            )
        )
        workos_client._jwks_client = mock_client
        assert await workos_client.authenticate(request) is None

    async def test_org_id_in_token(self, workos_client, rsa_keypair) -> None:
        private_key, _ = rsa_keypair
        token = _make_jwt(private_key, org_id="org_01ABC", role="admin")
        request = MagicMock()
        request.headers = {"Authorization": f"Bearer {token}"}

        workos_client._jwks_client = _mock_jwks_client(rsa_keypair)
        session = await workos_client.authenticate(request)

        assert session is not None
        assert session.org_id == "org_01ABC"
        assert session.role == "admin"


# ── User CRUD ────────────────────────────────────────────────────


class TestGetUser:
    async def test_success(self, connected_client) -> None:
        user = _make_workos_user()
        connected_client._workos.user_management.get_user = AsyncMock(return_value=user)

        result = await connected_client.get_user("user_01XYZ")
        assert result is not None
        assert result.id == "user_01XYZ"
        assert result.email == TEST_EMAIL
        assert result.first_name == "Alice"

    async def test_not_found(self, connected_client) -> None:
        connected_client._workos.user_management.get_user = AsyncMock(
            side_effect=_workos_exc(NotFoundException)
        )
        result = await connected_client.get_user("nonexistent")
        assert result is None


class TestListUsers:
    async def test_raises_not_implemented(self, connected_client) -> None:
        with pytest.raises(NotImplementedError, match="list_users_by_cursor"):
            await connected_client.list_users(limit=10)


class TestUpdateUser:
    async def test_success(self, connected_client) -> None:
        updated = _make_workos_user(email="new@test.com")
        connected_client._workos.user_management.update_user = AsyncMock(
            return_value=updated
        )

        result = await connected_client.update_user(
            user_id="user_01XYZ", email="new@test.com"
        )
        assert result is not None
        assert result.email == "new@test.com"

    async def test_not_found(self, connected_client) -> None:
        connected_client._workos.user_management.update_user = AsyncMock(
            side_effect=_workos_exc(NotFoundException)
        )
        result = await connected_client.update_user(
            user_id="nonexistent", email="x@test.com"
        )
        assert result is None


class TestDeleteUser:
    async def test_success(self, connected_client) -> None:
        connected_client._workos.user_management.delete_user = AsyncMock(
            return_value=None
        )
        result = await connected_client.delete_user("user_01XYZ")
        assert result is True

    async def test_not_found(self, connected_client) -> None:
        connected_client._workos.user_management.delete_user = AsyncMock(
            side_effect=_workos_exc(NotFoundException)
        )
        result = await connected_client.delete_user("nonexistent")
        assert result is False


class TestCountUsers:
    async def test_raises_not_implemented(self, connected_client) -> None:
        with pytest.raises(NotImplementedError, match="list_users_by_cursor"):
            await connected_client.count_users()


# ── Sign Up ──────────────────────────────────────────────────────


class TestSignUp:
    async def test_success(self, connected_client) -> None:
        user = _make_workos_user()
        auth_resp = _make_auth_response()
        connected_client._workos.user_management.create_user = AsyncMock(
            return_value=user
        )
        connected_client._workos.user_management.authenticate_with_password = AsyncMock(
            return_value=auth_resp
        )

        result = await connected_client.sign_up(
            email=TEST_EMAIL, password=TEST_PASSWORD
        )
        assert result is not None
        assert result.user.email == TEST_EMAIL
        assert result.tokens.access_token

    async def test_email_taken(self, connected_client) -> None:
        connected_client._workos.user_management.create_user = AsyncMock(
            side_effect=_workos_exc(ConflictException, "User already exists")
        )
        result = await connected_client.sign_up(
            email=TEST_EMAIL, password=TEST_PASSWORD
        )
        assert result is None


# ── Sign In With Password ────────────────────────────────────────


class TestSignInWithPassword:
    async def test_success(self, connected_client) -> None:
        auth_resp = _make_auth_response()
        connected_client._workos.user_management.authenticate_with_password = AsyncMock(
            return_value=auth_resp
        )

        result = await connected_client.sign_in_with_password(TEST_EMAIL, TEST_PASSWORD)
        assert result is not None
        assert result.user.email == TEST_EMAIL
        assert result.tokens.refresh_token == "refresh-token-abc"

    async def test_invalid_credentials(self, connected_client) -> None:
        connected_client._workos.user_management.authenticate_with_password = AsyncMock(
            side_effect=_workos_exc(AuthenticationException, "Invalid credentials")
        )
        result = await connected_client.sign_in_with_password(
            TEST_EMAIL, "wrong-password"
        )
        assert result is None


# ── Magic Auth ───────────────────────────────────────────────────


class TestMagicAuth:
    async def test_send_magic_auth_code(self, connected_client) -> None:
        magic_auth = MagicMock()
        magic_auth.id = "magic_01ABC"
        connected_client._workos.user_management.create_magic_auth = AsyncMock(
            return_value=magic_auth
        )

        await connected_client.sign_in_with_magic_link(
            email=TEST_EMAIL, magic_link_url="https://app.com/magic"
        )
        connected_client._workos.user_management.create_magic_auth.assert_called_once()

    async def test_verify_magic_auth(self, connected_client) -> None:
        auth_resp = _make_auth_response()
        connected_client._workos.user_management.authenticate_with_magic_auth = (
            AsyncMock(return_value=auth_resp)
        )

        result = await connected_client.verify_magic_link("code-123", email=TEST_EMAIL)
        assert result is not None
        assert result.user.email == TEST_EMAIL

    async def test_verify_magic_auth_without_email_raises(
        self, connected_client
    ) -> None:
        with pytest.raises(ValueError, match="WorkOS requires email"):
            await connected_client.verify_magic_link("code-123")


# ── OAuth ────────────────────────────────────────────────────────


class TestOAuth:
    async def test_get_authorization_url(self, connected_client) -> None:
        connected_client._workos.user_management.get_authorization_url = MagicMock(
            return_value="https://api.workos.com/authorize?..."
        )

        url = connected_client.get_oauth_authorization_url(
            "GoogleOAuth",
            state="random-state",
            redirect_uri="https://app.com/callback",
        )
        assert "workos.com" in url

    async def test_provider_name_mapping(self, connected_client) -> None:
        mock_fn = MagicMock(return_value="https://api.workos.com/authorize")
        connected_client._workos.user_management.get_authorization_url = mock_fn

        connected_client.get_oauth_authorization_url(
            "google", state="s", redirect_uri="https://app.com/cb"
        )
        mock_fn.assert_called_once()
        assert mock_fn.call_args.kwargs["provider"] == "GoogleOAuth"

    async def test_scopes_passed(self, connected_client) -> None:
        mock_fn = MagicMock(return_value="https://api.workos.com/authorize")
        connected_client._workos.user_management.get_authorization_url = mock_fn

        connected_client.get_oauth_authorization_url(
            "google",
            state="s",
            scopes=["email", "profile"],
            redirect_uri="https://app.com/cb",
        )
        assert mock_fn.call_args.kwargs["provider_scopes"] == ["email", "profile"]

    async def test_sign_in_with_oauth(self, connected_client) -> None:
        auth_resp = _make_auth_response()
        connected_client._workos.user_management.authenticate_with_code = AsyncMock(
            return_value=auth_resp
        )

        result = await connected_client.sign_in_with_oauth(
            "GoogleOAuth", "auth-code-123", redirect_uri="https://app.com/callback"
        )
        assert result is not None
        assert result.user.email == TEST_EMAIL


# ── Refresh Token ────────────────────────────────────────────────


class TestRefreshToken:
    async def test_success(self, connected_client) -> None:
        resp = MagicMock()
        resp.access_token = _make_access_token()
        resp.refresh_token = "new-refresh-token"
        connected_client._workos.user_management.authenticate_with_refresh_token = (
            AsyncMock(return_value=resp)
        )

        result = await connected_client.refresh_token("old-refresh-token")
        assert result is not None
        assert result.access_token == resp.access_token
        assert result.refresh_token == "new-refresh-token"

    async def test_invalid_refresh_token(self, connected_client) -> None:
        connected_client._workos.user_management.authenticate_with_refresh_token = (
            AsyncMock(
                side_effect=_workos_exc(
                    AuthenticationException, "Invalid Refresh Token"
                )
            )
        )
        result = await connected_client.refresh_token("bad-token")
        assert result is None

    async def test_expired_refresh_token(self, connected_client) -> None:
        connected_client._workos.user_management.authenticate_with_refresh_token = (
            AsyncMock(
                side_effect=_workos_exc(BadRequestException, "Refresh token expired")
            )
        )
        result = await connected_client.refresh_token("expired-token")
        assert result is None


# ── Sessions / Sign Out ──────────────────────────────────────────


class TestSessions:
    async def test_list_sessions(self, connected_client) -> None:
        sessions = [_make_session(), _make_session(session_id="session_02")]
        connected_client._workos.user_management.list_sessions = AsyncMock(
            return_value=_make_list_resource(sessions)
        )

        result = await connected_client.list_sessions(user_id="user_01XYZ")
        assert len(result) == 2


class TestSignOut:
    async def test_sign_out(self, connected_client) -> None:
        connected_client._workos.user_management.revoke_session = AsyncMock(
            return_value=None
        )
        await connected_client.sign_out("session_01ABC")
        connected_client._workos.user_management.revoke_session.assert_called_once()

    async def test_sign_out_all(self, connected_client) -> None:
        sessions = [
            _make_session(session_id="s1"),
            _make_session(session_id="s2"),
        ]
        connected_client._workos.user_management.list_sessions = AsyncMock(
            return_value=_make_list_resource(sessions)
        )
        connected_client._workos.user_management.revoke_session = AsyncMock(
            return_value=None
        )

        await connected_client.sign_out_all("user_01XYZ")
        assert connected_client._workos.user_management.revoke_session.call_count == 2


# ── Organizations ────────────────────────────────────────────────


class TestOrganizations:
    async def test_create(self, connected_client) -> None:
        org = _make_workos_org()
        connected_client._workos.organizations.create_organization = AsyncMock(
            return_value=org
        )
        connected_client._workos.user_management.create_organization_membership = (
            AsyncMock(return_value=_make_workos_membership())
        )

        result = await connected_client.create_org(
            name="Acme Corp", slug="acme-corp", creator_id="user_01XYZ"
        )
        assert result is not None
        assert result.name == "Acme Corp"

    async def test_get(self, connected_client) -> None:
        org = _make_workos_org()
        connected_client._workos.organizations.get_organization = AsyncMock(
            return_value=org
        )

        result = await connected_client.get_org("org_01ABC")
        assert result is not None
        assert result.id == "org_01ABC"

    async def test_get_not_found(self, connected_client) -> None:
        connected_client._workos.organizations.get_organization = AsyncMock(
            side_effect=_workos_exc(NotFoundException)
        )
        result = await connected_client.get_org("nonexistent")
        assert result is None

    async def test_update(self, connected_client) -> None:
        org = _make_workos_org(name="New Name")
        connected_client._workos.organizations.update_organization = AsyncMock(
            return_value=org
        )

        result = await connected_client.update_org(org_id="org_01ABC", name="New Name")
        assert result is not None
        assert result.name == "New Name"

    async def test_delete(self, connected_client) -> None:
        connected_client._workos.organizations.delete_organization = AsyncMock(
            return_value=None
        )
        result = await connected_client.delete_org("org_01ABC")
        assert result is True

    async def test_delete_not_found(self, connected_client) -> None:
        connected_client._workos.organizations.delete_organization = AsyncMock(
            side_effect=_workos_exc(NotFoundException)
        )
        result = await connected_client.delete_org("nonexistent")
        assert result is False

    async def test_list(self, connected_client) -> None:
        orgs = [
            _make_workos_org(org_id="o1", name="Org A"),
            _make_workos_org(org_id="o2", name="Org B"),
        ]
        connected_client._workos.organizations.list_organizations = AsyncMock(
            return_value=_make_list_resource(orgs)
        )

        result = await connected_client.list_orgs()
        assert len(result) == 2


# ── Organization Memberships ─────────────────────────────────────


class TestOrgMemberships:
    async def test_add_member(self, connected_client) -> None:
        membership = _make_workos_membership()
        connected_client._workos.user_management.create_organization_membership = (
            AsyncMock(return_value=membership)
        )

        result = await connected_client.add_org_member(
            org_id="org_01ABC", user_id="user_01XYZ", role="member"
        )
        assert result is not None
        assert result.org_id == "org_01ABC"
        assert result.user_id == "user_01XYZ"
        assert result.role == "member"

    async def test_update_member(self, connected_client) -> None:
        # First list to find membership ID, then update
        membership = _make_workos_membership(role_slug="admin")
        connected_client._workos.user_management.list_organization_memberships = (
            AsyncMock(return_value=_make_list_resource([membership]))
        )
        updated = _make_workos_membership(role_slug="admin")
        connected_client._workos.user_management.update_organization_membership = (
            AsyncMock(return_value=updated)
        )

        result = await connected_client.update_org_member(
            org_id="org_01ABC", user_id="user_01XYZ", role="admin"
        )
        assert result is not None
        assert result.role == "admin"

    async def test_remove_member(self, connected_client) -> None:
        membership = _make_workos_membership()
        connected_client._workos.user_management.list_organization_memberships = (
            AsyncMock(return_value=_make_list_resource([membership]))
        )
        connected_client._workos.user_management.delete_organization_membership = (
            AsyncMock(return_value=None)
        )

        result = await connected_client.remove_org_member(
            org_id="org_01ABC", user_id="user_01XYZ"
        )
        assert result is True

    async def test_remove_member_not_found(self, connected_client) -> None:
        connected_client._workos.user_management.list_organization_memberships = (
            AsyncMock(return_value=_make_list_resource([]))
        )

        result = await connected_client.remove_org_member(
            org_id="org_01ABC", user_id="nonexistent"
        )
        assert result is False

    async def test_list_members_raises_not_implemented(self, connected_client) -> None:
        with pytest.raises(NotImplementedError, match="list_org_members_by_cursor"):
            await connected_client.list_org_members("org_01ABC")

    async def test_get_member(self, connected_client) -> None:
        membership = _make_workos_membership()
        connected_client._workos.user_management.list_organization_memberships = (
            AsyncMock(return_value=_make_list_resource([membership]))
        )

        result = await connected_client.get_org_member(
            org_id="org_01ABC", user_id="user_01XYZ"
        )
        assert result is not None
        assert result.user_id == "user_01XYZ"

    async def test_get_member_not_found(self, connected_client) -> None:
        connected_client._workos.user_management.list_organization_memberships = (
            AsyncMock(return_value=_make_list_resource([]))
        )

        result = await connected_client.get_org_member(
            org_id="org_01ABC", user_id="nonexistent"
        )
        assert result is None


# ── Cursor-based pagination ──────────────────────────────────────


def _make_list_resource_with_cursor(items: list, after: str | None) -> MagicMock:
    """Mock a WorkOSListResource with a cursor."""
    resource = MagicMock()
    resource.data = items
    resource.list_metadata = MagicMock()
    resource.list_metadata.after = after
    return resource


class TestListUsersByCursor:
    async def test_first_page(self, connected_client) -> None:
        users = [_make_workos_user(user_id="u1"), _make_workos_user(user_id="u2")]
        connected_client._workos.user_management.list_users = AsyncMock(
            return_value=_make_list_resource_with_cursor(users, after="cursor_abc")
        )

        result = await connected_client.list_users_by_cursor(limit=2)
        assert len(result.data) == 2
        assert result.has_more is True
        assert result.next_cursor == "cursor_abc"

    async def test_last_page(self, connected_client) -> None:
        users = [_make_workos_user(user_id="u3")]
        connected_client._workos.user_management.list_users = AsyncMock(
            return_value=_make_list_resource_with_cursor(users, after=None)
        )

        result = await connected_client.list_users_by_cursor(
            limit=2, after="cursor_abc"
        )
        assert len(result.data) == 1
        assert result.has_more is False
        assert result.next_cursor is None

    async def test_passes_after_to_sdk(self, connected_client) -> None:
        connected_client._workos.user_management.list_users = AsyncMock(
            return_value=_make_list_resource_with_cursor([], after=None)
        )
        await connected_client.list_users_by_cursor(limit=5, after="cur_xyz")
        connected_client._workos.user_management.list_users.assert_called_once_with(
            limit=5, after="cur_xyz"
        )


class TestListOrgMembersByCursor:
    async def test_first_page(self, connected_client) -> None:
        members = [
            _make_workos_membership(user_id="u1"),
            _make_workos_membership(user_id="u2"),
        ]
        connected_client._workos.user_management.list_organization_memberships = (
            AsyncMock(
                return_value=_make_list_resource_with_cursor(
                    members, after="cursor_abc"
                )
            )
        )

        result = await connected_client.list_org_members_by_cursor("org_01ABC", limit=2)
        assert len(result.data) == 2
        assert result.has_more is True
        assert result.next_cursor == "cursor_abc"

    async def test_last_page(self, connected_client) -> None:
        connected_client._workos.user_management.list_organization_memberships = (
            AsyncMock(return_value=_make_list_resource_with_cursor([], after=None))
        )

        result = await connected_client.list_org_members_by_cursor(
            "org_01ABC", limit=10, after="cursor_abc"
        )
        assert result.has_more is False
        assert result.next_cursor is None


class TestListOrgsByCursor:
    async def test_returns_cursor_result(self, connected_client) -> None:
        orgs = [_make_workos_org(org_id="o1"), _make_workos_org(org_id="o2")]
        connected_client._workos.organizations.list_organizations = AsyncMock(
            return_value=_make_list_resource_with_cursor(orgs, after="cur_next")
        )

        result = await connected_client.list_orgs_by_cursor(limit=2)
        assert len(result.data) == 2
        assert result.has_more is True
        assert result.next_cursor == "cur_next"


class TestListSessionsByCursor:
    async def test_returns_cursor_result(self, connected_client) -> None:
        sessions = [_make_session(), _make_session(session_id="s2")]
        connected_client._workos.user_management.list_sessions = AsyncMock(
            return_value=_make_list_resource_with_cursor(sessions, after=None)
        )

        result = await connected_client.list_sessions_by_cursor(
            user_id="user_01XYZ", limit=10
        )
        assert len(result.data) == 2
        assert result.has_more is False
