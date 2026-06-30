"""Tests for the WorkOS authentication client."""

from __future__ import annotations

from collections.abc import AsyncGenerator
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

from derp.auth.base import (
    AuthOutcome,
    Identity,
    Org,
    OrgMember,
    Page,
    Session,
    TokenSet,
)
from derp.auth.exceptions import (
    EmailAlreadyExistsError,
    InvalidCredentialsError,
    InvalidTokenError,
    OrgMemberNotFoundError,
    OrgNotFoundError,
    OrgSlugConflictError,
    UserNotFoundError,
)
from derp.auth.models import AuthStatus, WorkOSOrganization
from derp.config import (
    AuthConfig,
    JWTConfig,
    NativeAuthConfig,
    SupabaseConfig,
    WorkOSConfig,
)
from derp.orm import DatabaseEngine

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
    """Client with a mocked internal AsyncWorkOSClient (no DB attached)."""
    mock_workos = MagicMock()
    mock_workos.user_management = MagicMock()
    mock_workos.organizations = MagicMock()
    workos_client._workos = mock_workos
    return workos_client


@pytest.fixture
async def connected_client_with_db(
    workos_client, clean_database: str
) -> AsyncGenerator:
    """Client with a mocked WorkOS API plus a real DB and the WorkOS schema.

    The DB is needed for slug lookups and for ``create_org`` (which inserts
    the local slug-index row). Methods that resolve via ``org_id=`` directly
    do NOT touch the database.
    """
    mock_workos = MagicMock()
    mock_workos.user_management = MagicMock()
    mock_workos.organizations = MagicMock()
    workos_client._workos = mock_workos

    db = DatabaseEngine(clean_database, min_size=1, max_size=2)
    await db.connect()
    # Two-column slug index — id is the WorkOS org id directly.
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS organizations (
            id VARCHAR(255) PRIMARY KEY,
            slug VARCHAR(255) UNIQUE NOT NULL
        )
        """
    )
    workos_client.set_db(db)
    try:
        yield workos_client
    finally:
        await db.disconnect()


async def _seed_workos_org(
    client,
    *,
    workos_org_id: str = "org_01ABC",
    slug: str = "acme-corp",
) -> str:
    """Insert a slug-index row; returns the WorkOS org id (== row id)."""
    row = await (
        client._db()
        .insert(WorkOSOrganization)
        .values(id=workos_org_id, slug=slug)
        .returning(WorkOSOrganization)
        .execute()
    )
    return row.id


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
    slug: str | None = None,
) -> MagicMock:
    """Build a mock WorkOS Organization.

    ``slug`` is written to ``metadata["slug"]`` to match real WorkOS behaviour
    after ``create_org`` / ``update_org`` (we always store the slug there).
    Pass ``slug=None`` to simulate an org with no slug in metadata (e.g.,
    one created outside this client).
    """
    now = datetime.now(UTC).isoformat()
    org = MagicMock()
    org.id = org_id
    org.name = name
    org.created_at = now
    org.updated_at = now
    org.metadata = {"slug": slug} if slug is not None else {}
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


def _make_list_resource(items: list, after: str | None = None) -> MagicMock:
    """Mock a WorkOSListResource that supports iteration, .data, and a cursor."""
    resource = MagicMock()
    resource.data = items
    resource.__iter__ = MagicMock(return_value=iter(items))
    resource.list_metadata = MagicMock()
    resource.list_metadata.after = after
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


# ── Capabilities ──────────────────────────────────────────────────


class TestCapabilities:
    def test_advertised_flags(self, workos_client) -> None:
        assert workos_client.supports_password is True
        assert workos_client.supports_oauth is True
        assert workos_client.supports_sso is True
        assert workos_client.supports_magic_link is True
        assert workos_client.supports_orgs is True
        assert workos_client.supports_sessions is True
        # Left at the base default.
        assert workos_client.supports_multi_tenant is False
        assert workos_client.supports_mfa is False


# ── verify_token / authenticate ───────────────────────────────────


class TestAuthenticate:
    async def test_valid_token(self, workos_client, rsa_keypair) -> None:
        private_key, _ = rsa_keypair
        token = _make_jwt(private_key)
        request = MagicMock()
        request.headers = {"Authorization": f"Bearer {token}"}

        workos_client._jwks_client = _mock_jwks_client(rsa_keypair)
        session = await workos_client.authenticate(request)

        assert isinstance(session, Session)
        assert session.user_id == "user_01XYZ"
        assert session.session_id == "session_01ABC"
        assert session.is_anonymous is False
        assert session.mfa.enrolled is False

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

    async def test_org_id_passthrough_no_db(self, workos_client, rsa_keypair) -> None:
        """Auth is pure JWT — JWT.org_id surfaces unchanged with NO DB hit.

        ``Session.org_id`` matches the value app FKs target (the WorkOS org id)
        so tenant checks compare apples to apples.
        """
        private_key, _ = rsa_keypair
        token = _make_jwt(private_key, org_id="org_01ABC", role="admin")
        request = MagicMock()
        request.headers = {"Authorization": f"Bearer {token}"}

        workos_client._jwks_client = _mock_jwks_client(rsa_keypair)
        # Crucially: no set_db() call. Authenticate must not need a DB.
        assert workos_client._database_client is None
        session = await workos_client.authenticate(request)

        assert session is not None
        assert session.org_id == "org_01ABC"
        assert session.org_role == "admin"
        assert session.roles == ("admin",)

    async def test_org_id_omitted_when_jwt_has_none(
        self, workos_client, rsa_keypair
    ) -> None:
        """JWT without org_id → Session.org_id is None, roles still set."""
        private_key, _ = rsa_keypair
        token = _make_jwt(private_key, role="admin")
        request = MagicMock()
        request.headers = {"Authorization": f"Bearer {token}"}

        workos_client._jwks_client = _mock_jwks_client(rsa_keypair)
        session = await workos_client.authenticate(request)

        assert session is not None
        assert session.org_id is None
        assert session.org_role is None
        assert session.roles == ("admin",)

    async def test_default_role_when_absent(self, workos_client, rsa_keypair) -> None:
        """A token with no role claim falls back to the default role."""
        private_key, _ = rsa_keypair
        token = _make_jwt(private_key)
        request = MagicMock()
        request.headers = {"Authorization": f"Bearer {token}"}

        workos_client._jwks_client = _mock_jwks_client(rsa_keypair)
        session = await workos_client.authenticate(request)

        assert session is not None
        assert session.roles == ("default",)


# ── User CRUD ────────────────────────────────────────────────────


class TestGetUser:
    async def test_success(self, connected_client) -> None:
        user = _make_workos_user()
        connected_client._workos.user_management.get_user = AsyncMock(return_value=user)

        result = await connected_client.get_user("user_01XYZ")
        assert isinstance(result, Identity)
        assert result.id == "user_01XYZ"
        assert result.email == TEST_EMAIL
        assert result.email_verified is True
        assert result.disabled is False
        assert result.roles == ("default",)

    async def test_unverified_user_is_disabled(self, connected_client) -> None:
        user = _make_workos_user(email_verified=False)
        connected_client._workos.user_management.get_user = AsyncMock(return_value=user)

        result = await connected_client.get_user("user_01XYZ")
        assert result.email_verified is False
        assert result.disabled is True

    async def test_not_found(self, connected_client) -> None:
        connected_client._workos.user_management.get_user = AsyncMock(
            side_effect=_workos_exc(NotFoundException)
        )
        with pytest.raises(UserNotFoundError):
            await connected_client.get_user("nonexistent")


class TestFindUser:
    async def test_by_id_hit(self, connected_client) -> None:
        connected_client._workos.user_management.get_user = AsyncMock(
            return_value=_make_workos_user()
        )
        result = await connected_client.find_user(user_id="user_01XYZ")
        assert result is not None
        assert result.id == "user_01XYZ"

    async def test_by_id_miss_returns_none(self, connected_client) -> None:
        connected_client._workos.user_management.get_user = AsyncMock(
            side_effect=_workos_exc(NotFoundException)
        )
        assert await connected_client.find_user(user_id="nope") is None

    async def test_by_email_hit(self, connected_client) -> None:
        connected_client._workos.user_management.list_users = AsyncMock(
            return_value=_make_list_resource([_make_workos_user()])
        )
        result = await connected_client.find_user(email=TEST_EMAIL)
        assert result is not None
        assert result.email == TEST_EMAIL

    async def test_by_email_miss_returns_none(self, connected_client) -> None:
        connected_client._workos.user_management.list_users = AsyncMock(
            return_value=_make_list_resource([])
        )
        assert await connected_client.find_user(email="ghost@example.com") is None

    async def test_phone_unsupported_returns_none(self, connected_client) -> None:
        assert await connected_client.find_user(phone="+15555550100") is None

    async def test_requires_exactly_one(self, connected_client) -> None:
        with pytest.raises(ValueError, match="exactly one"):
            await connected_client.find_user()
        with pytest.raises(ValueError, match="exactly one"):
            await connected_client.find_user(user_id="x", email="y@z.com")


class TestListUsers:
    async def test_first_page(self, connected_client) -> None:
        users = [_make_workos_user(user_id="u1"), _make_workos_user(user_id="u2")]
        connected_client._workos.user_management.list_users = AsyncMock(
            return_value=_make_list_resource(users, after="cursor_abc")
        )

        page = await connected_client.list_users(limit=2)
        assert isinstance(page, Page)
        assert len(page.items) == 2
        assert all(isinstance(i, Identity) for i in page.items)
        assert page.has_more is True
        assert page.next_cursor == "cursor_abc"

    async def test_last_page(self, connected_client) -> None:
        connected_client._workos.user_management.list_users = AsyncMock(
            return_value=_make_list_resource(
                [_make_workos_user(user_id="u3")], after=None
            )
        )

        page = await connected_client.list_users(limit=2, cursor="cursor_abc")
        assert len(page.items) == 1
        assert page.has_more is False
        assert page.next_cursor is None

    async def test_cursor_maps_to_after(self, connected_client) -> None:
        mock_fn = AsyncMock(return_value=_make_list_resource([], after=None))
        connected_client._workos.user_management.list_users = mock_fn
        await connected_client.list_users(limit=5, cursor="cur_xyz")
        mock_fn.assert_awaited_once_with(limit=5, after="cur_xyz")


class TestUpdateUser:
    async def test_success(self, connected_client) -> None:
        updated = _make_workos_user(email="new@test.com")
        connected_client._workos.user_management.update_user = AsyncMock(
            return_value=updated
        )

        result = (
            await connected_client.update_user(
                user_id="user_01XYZ", email="new@test.com"
            )
        ).raise_for_status()
        assert isinstance(result, Identity)
        assert result.email == "new@test.com"

    async def test_not_found(self, connected_client) -> None:
        connected_client._workos.user_management.update_user = AsyncMock(
            side_effect=_workos_exc(NotFoundException)
        )
        result = await connected_client.update_user(
            user_id="nonexistent", email="x@test.com"
        )
        assert isinstance(result.error, UserNotFoundError)


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

        outcome = await connected_client.sign_up(
            email=TEST_EMAIL, password=TEST_PASSWORD
        )
        assert isinstance(outcome, AuthOutcome)
        assert outcome.status is AuthStatus.COMPLETE
        assert outcome.ok is True
        assert outcome.identity is not None
        assert outcome.identity.email == TEST_EMAIL
        assert outcome.tokens is not None
        assert outcome.tokens.access_token

    async def test_email_taken(self, connected_client) -> None:
        connected_client._workos.user_management.create_user = AsyncMock(
            side_effect=_workos_exc(ConflictException, "User already exists")
        )
        outcome = await connected_client.sign_up(
            email=TEST_EMAIL, password=TEST_PASSWORD
        )
        assert outcome.status is AuthStatus.EMAIL_EXISTS
        assert outcome.ok is False
        assert isinstance(outcome.error, EmailAlreadyExistsError)
        assert outcome.error.email == TEST_EMAIL

    async def test_email_taken_via_bad_request(self, connected_client) -> None:
        connected_client._workos.user_management.create_user = AsyncMock(
            side_effect=_workos_exc(BadRequestException, "email taken")
        )
        outcome = await connected_client.sign_up(
            email=TEST_EMAIL, password=TEST_PASSWORD
        )
        assert outcome.status is AuthStatus.EMAIL_EXISTS


# ── Sign In With Password ────────────────────────────────────────


class TestSignInWithPassword:
    async def test_success(self, connected_client) -> None:
        auth_resp = _make_auth_response()
        connected_client._workos.user_management.authenticate_with_password = AsyncMock(
            return_value=auth_resp
        )

        outcome = await connected_client.sign_in_with_password(
            identifier=TEST_EMAIL, password=TEST_PASSWORD
        )
        assert outcome.status is AuthStatus.COMPLETE
        assert outcome.identity is not None
        assert outcome.identity.email == TEST_EMAIL
        assert isinstance(outcome.tokens, TokenSet)
        assert outcome.tokens.refresh_token == "refresh-token-abc"
        assert outcome.tokens.id_token is None

    async def test_invalid_credentials(self, connected_client) -> None:
        connected_client._workos.user_management.authenticate_with_password = AsyncMock(
            side_effect=_workos_exc(AuthenticationException, "Invalid credentials")
        )
        outcome = await connected_client.sign_in_with_password(
            identifier=TEST_EMAIL, password="wrong-password"
        )
        assert outcome.status is AuthStatus.INVALID_CREDENTIALS
        assert isinstance(outcome.error, InvalidCredentialsError)


# ── Magic Auth ───────────────────────────────────────────────────


class TestMagicAuth:
    async def test_send_magic_link(self, connected_client) -> None:
        magic_auth = MagicMock()
        magic_auth.id = "magic_01ABC"
        connected_client._workos.user_management.create_magic_auth = AsyncMock(
            return_value=magic_auth
        )

        await connected_client.send_magic_link(
            email=TEST_EMAIL, redirect_url="https://app.com/magic"
        )
        connected_client._workos.user_management.create_magic_auth.assert_called_once()

    async def test_verify_magic_link(self, connected_client) -> None:
        auth_resp = _make_auth_response()
        connected_client._workos.user_management.authenticate_with_magic_auth = (
            AsyncMock(return_value=auth_resp)
        )

        outcome = await connected_client.verify_magic_link("code-123", email=TEST_EMAIL)
        assert outcome.status is AuthStatus.COMPLETE
        assert outcome.identity is not None
        assert outcome.identity.email == TEST_EMAIL

    async def test_verify_invalid_token(self, connected_client) -> None:
        connected_client._workos.user_management.authenticate_with_magic_auth = (
            AsyncMock(side_effect=_workos_exc(AuthenticationException, "bad code"))
        )
        outcome = await connected_client.verify_magic_link("bad", email=TEST_EMAIL)
        assert outcome.status is AuthStatus.INVALID_TOKEN
        assert isinstance(outcome.error, InvalidTokenError)

    async def test_verify_without_email_raises(self, connected_client) -> None:
        with pytest.raises(ValueError, match="WorkOS requires email"):
            await connected_client.verify_magic_link("code-123")


# ── OAuth / SSO ──────────────────────────────────────────────────


class TestOAuth:
    async def test_authorization_url(self, connected_client) -> None:
        connected_client._workos.user_management.get_authorization_url = MagicMock(
            return_value="https://api.workos.com/authorize?..."
        )

        url = connected_client.authorization_url(
            provider="GoogleOAuth",
            state="random-state",
            redirect_uri="https://app.com/callback",
        )
        assert "workos.com" in url

    async def test_provider_name_mapping(self, connected_client) -> None:
        mock_fn = MagicMock(return_value="https://api.workos.com/authorize")
        connected_client._workos.user_management.get_authorization_url = mock_fn

        connected_client.authorization_url(
            provider="google", state="s", redirect_uri="https://app.com/cb"
        )
        mock_fn.assert_called_once()
        assert mock_fn.call_args.kwargs["provider"] == "GoogleOAuth"

    async def test_scopes_passed(self, connected_client) -> None:
        mock_fn = MagicMock(return_value="https://api.workos.com/authorize")
        connected_client._workos.user_management.get_authorization_url = mock_fn

        connected_client.authorization_url(
            provider="google",
            state="s",
            scopes=["email", "profile"],
            redirect_uri="https://app.com/cb",
        )
        assert mock_fn.call_args.kwargs["provider_scopes"] == ["email", "profile"]

    async def test_sso_authorization_url_by_organization(
        self, connected_client
    ) -> None:
        mock_fn = MagicMock(return_value="https://api.workos.com/authorize")
        connected_client._workos.user_management.get_authorization_url = mock_fn

        connected_client.authorization_url(
            organization="org_01ABC", state="s", redirect_uri="https://app.com/cb"
        )
        assert mock_fn.call_args.kwargs["organization_id"] == "org_01ABC"

    async def test_sign_in_with_oauth(self, connected_client) -> None:
        auth_resp = _make_auth_response()
        connected_client._workos.user_management.authenticate_with_code = AsyncMock(
            return_value=auth_resp
        )

        outcome = await connected_client.sign_in_with_oauth(
            "auth-code-123",
            provider="GoogleOAuth",
            redirect_uri="https://app.com/callback",
        )
        assert outcome.status is AuthStatus.COMPLETE
        assert outcome.identity is not None
        assert outcome.identity.email == TEST_EMAIL

    async def test_sign_in_with_oauth_rejected(self, connected_client) -> None:
        connected_client._workos.user_management.authenticate_with_code = AsyncMock(
            side_effect=_workos_exc(AuthenticationException, "bad code")
        )
        outcome = await connected_client.sign_in_with_oauth("bad", provider="google")
        assert outcome.status is AuthStatus.INVALID_CREDENTIALS
        assert isinstance(outcome.error, InvalidCredentialsError)

    async def test_sign_in_with_sso(self, connected_client) -> None:
        auth_resp = _make_auth_response()
        connected_client._workos.user_management.authenticate_with_code = AsyncMock(
            return_value=auth_resp
        )

        outcome = await connected_client.sign_in_with_sso("sso-code-123")
        assert outcome.status is AuthStatus.COMPLETE
        assert outcome.identity is not None


# ── Refresh ──────────────────────────────────────────────────────


class TestRefresh:
    async def test_success(self, connected_client) -> None:
        resp = MagicMock()
        resp.access_token = _make_access_token()
        resp.refresh_token = "new-refresh-token"
        connected_client._workos.user_management.authenticate_with_refresh_token = (
            AsyncMock(return_value=resp)
        )

        result = (
            await connected_client.refresh("old-refresh-token")
        ).raise_for_status()
        assert isinstance(result, TokenSet)
        assert result.access_token == resp.access_token
        assert result.refresh_token == "new-refresh-token"
        assert result.id_token is None

    async def test_invalid_refresh_token(self, connected_client) -> None:
        connected_client._workos.user_management.authenticate_with_refresh_token = (
            AsyncMock(
                side_effect=_workos_exc(
                    AuthenticationException, "Invalid Refresh Token"
                )
            )
        )
        result = await connected_client.refresh("bad-token")
        assert isinstance(result.error, InvalidTokenError)

    async def test_expired_refresh_token(self, connected_client) -> None:
        connected_client._workos.user_management.authenticate_with_refresh_token = (
            AsyncMock(
                side_effect=_workos_exc(BadRequestException, "Refresh token expired")
            )
        )
        result = await connected_client.refresh("expired-token")
        assert isinstance(result.error, InvalidTokenError)


# ── Sessions ─────────────────────────────────────────────────────


class TestSessions:
    async def test_list_sessions(self, connected_client) -> None:
        sessions = [_make_session(), _make_session(session_id="session_02")]
        connected_client._workos.user_management.list_sessions = AsyncMock(
            return_value=_make_list_resource(sessions)
        )

        page = await connected_client.list_sessions(user_id="user_01XYZ")
        assert isinstance(page, Page)
        assert len(page.items) == 2
        assert all(isinstance(s, Session) for s in page.items)
        assert page.has_more is False

    async def test_list_sessions_no_user_returns_empty(self, connected_client) -> None:
        page = await connected_client.list_sessions()
        assert page.items == []
        assert page.has_more is False

    async def test_list_sessions_cursor(self, connected_client) -> None:
        sessions = [_make_session()]
        connected_client._workos.user_management.list_sessions = AsyncMock(
            return_value=_make_list_resource(sessions, after="cur_next")
        )
        page = await connected_client.list_sessions(user_id="user_01XYZ", limit=1)
        assert page.has_more is True
        assert page.next_cursor == "cur_next"

    async def test_revoke_session(self, connected_client) -> None:
        connected_client._workos.user_management.revoke_session = AsyncMock(
            return_value=None
        )
        await connected_client.revoke_session("session_01ABC")
        connected_client._workos.user_management.revoke_session.assert_called_once()

    async def test_revoke_all_sessions(self, connected_client) -> None:
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

        await connected_client.revoke_all_sessions("user_01XYZ")
        assert connected_client._workos.user_management.revoke_session.call_count == 2


# ── Organizations ────────────────────────────────────────────────


class TestOrganizations:
    async def test_create_inserts_local_mapping(self, connected_client_with_db) -> None:
        """Happy path: WorkOS create succeeds, local row is inserted."""
        client = connected_client_with_db
        org = _make_workos_org(slug="acme-corp")
        client._workos.organizations.create_organization = AsyncMock(return_value=org)
        client._workos.user_management.create_organization_membership = AsyncMock(
            return_value=_make_workos_membership()
        )

        result = (
            await client.create_org(
                name="Acme Corp", slug="acme-corp", creator_id="user_01XYZ"
            )
        ).raise_for_status()

        assert isinstance(result, Org)
        assert result.name == "Acme Corp"
        assert result.slug == "acme-corp"
        assert result.tenant_id is None
        # Org.id IS the WorkOS string id — no translation.
        assert result.id == "org_01ABC"
        rows = await (
            client._db()
            .select(WorkOSOrganization)
            .where(WorkOSOrganization.id == "org_01ABC")
            .execute()
        )
        assert len(rows) == 1
        assert rows[0].id == "org_01ABC"
        assert rows[0].slug == "acme-corp"

    async def test_create_workos_conflict_returns_error(
        self, connected_client_with_db
    ) -> None:
        """WorkOS-side conflict (duplicate org) → OrgSlugConflictError attached,
        no local row."""
        client = connected_client_with_db
        client._workos.organizations.create_organization = AsyncMock(
            side_effect=_workos_exc(ConflictException, "Conflict")
        )
        result = await client.create_org(
            name="Acme", slug="acme", creator_id="user_01XYZ"
        )
        assert isinstance(result.error, OrgSlugConflictError)
        assert result.error.slug == "acme"
        rows = await client._db().select(WorkOSOrganization).execute()
        assert rows == []

    async def test_create_local_slug_conflict_rolls_back_workos(
        self, connected_client_with_db
    ) -> None:
        """Slug already taken locally → roll back WorkOS create + return error."""
        client = connected_client_with_db
        await _seed_workos_org(client, workos_org_id="org_existing", slug="taken-slug")
        client._workos.organizations.create_organization = AsyncMock(
            return_value=_make_workos_org(org_id="org_new")
        )
        client._workos.organizations.delete_organization = AsyncMock(return_value=None)

        result = await client.create_org(
            name="Other", slug="taken-slug", creator_id="user_01XYZ"
        )
        assert isinstance(result.error, OrgSlugConflictError)
        assert result.error.slug == "taken-slug"
        client._workos.organizations.delete_organization.assert_awaited_once_with(
            organization_id="org_new"
        )

    async def test_get_by_id(self, connected_client_with_db) -> None:
        """``get_org(<id>)`` resolves via the local index, then calls WorkOS.

        With no slug in WorkOS metadata, the slug comes back empty.
        """
        client = connected_client_with_db
        await _seed_workos_org(client, workos_org_id="org_01ABC", slug="acme-corp")
        client._workos.organizations.get_organization = AsyncMock(
            return_value=_make_workos_org()
        )

        result = (await client.get_org(org_id="org_01ABC")).raise_for_status()

        assert result.id == "org_01ABC"
        assert result.slug == ""  # no metadata slug → empty
        client._workos.organizations.get_organization.assert_awaited_once_with(
            organization_id="org_01ABC"
        )

    async def test_get_by_id_returns_metadata_slug(
        self, connected_client_with_db
    ) -> None:
        """Slug on the returned ``Org`` is read from WorkOS metadata."""
        client = connected_client_with_db
        await _seed_workos_org(client, workos_org_id="org_01ABC", slug="acme-corp")
        client._workos.organizations.get_organization = AsyncMock(
            return_value=_make_workos_org(slug="acme-corp")
        )

        result = (await client.get_org(org_id="org_01ABC")).raise_for_status()
        assert result.id == "org_01ABC"
        assert result.slug == "acme-corp"

    async def test_get_workos_404(self, connected_client_with_db) -> None:
        """WorkOS NotFound → OrgNotFoundError attached to the result."""
        client = connected_client_with_db
        await _seed_workos_org(client, workos_org_id="org_does_not_exist", slug="ghost")
        client._workos.organizations.get_organization = AsyncMock(
            side_effect=_workos_exc(NotFoundException)
        )
        result = await client.get_org(org_id="org_does_not_exist")
        assert isinstance(result.error, OrgNotFoundError)

    async def test_get_by_slug_uses_local_index(self, connected_client_with_db) -> None:
        """Slug lookup hits the local table — never paginates WorkOS."""
        client = connected_client_with_db
        workos_id = await _seed_workos_org(client, slug="my-slug")
        client._workos.organizations.get_organization = AsyncMock(
            return_value=_make_workos_org(slug="my-slug")
        )
        client._workos.organizations.list_organizations = AsyncMock()

        result = (await client.get_org(slug="my-slug")).raise_for_status()
        assert result.id == workos_id
        assert result.slug == "my-slug"
        client._workos.organizations.list_organizations.assert_not_called()

    async def test_get_by_slug_or_id_resolve_same_org(
        self, connected_client_with_db
    ) -> None:
        """``org_id=`` and ``slug=`` resolve to the same org (slug via local index)."""
        client = connected_client_with_db
        workos_id = await _seed_workos_org(
            client, workos_org_id="org_01ABC", slug="acme-corp"
        )
        client._workos.organizations.get_organization = AsyncMock(
            return_value=_make_workos_org(org_id="org_01ABC", slug="acme-corp")
        )

        by_slug = (await client.get_org(slug="acme-corp")).raise_for_status()
        by_id = (await client.get_org(org_id="org_01ABC")).raise_for_status()

        assert by_slug.id == by_id.id == workos_id == "org_01ABC"
        # Both addressing modes hit WorkOS with the resolved id.
        for call in client._workos.organizations.get_organization.await_args_list:
            assert call.kwargs == {"organization_id": "org_01ABC"}

    async def test_get_unknown_slug_returns_error(
        self, connected_client_with_db
    ) -> None:
        client = connected_client_with_db
        client._workos.organizations.get_organization = AsyncMock()
        result = await client.get_org(slug="never-existed")
        assert isinstance(result.error, OrgNotFoundError)
        client._workos.organizations.get_organization.assert_not_called()

    async def test_get_requires_exactly_one_ref(self, connected_client_with_db) -> None:
        client = connected_client_with_db
        with pytest.raises(ValueError, match="exactly one"):
            await client.get_org()
        with pytest.raises(ValueError, match="exactly one"):
            await client.get_org(org_id="org_01ABC", slug="acme-corp")

    async def test_update_name_only(self, connected_client_with_db) -> None:
        client = connected_client_with_db
        workos_id = await _seed_workos_org(client)
        client._workos.organizations.update_organization = AsyncMock(
            return_value=_make_workos_org(name="New Name")
        )

        result = (
            await client.update_org(org_id=workos_id, name="New Name")
        ).raise_for_status()
        assert result.name == "New Name"
        assert result.id == workos_id

    async def test_update_by_slug(self, connected_client_with_db) -> None:
        """``update_org`` can address the row by slug; resolves to the WorkOS id."""
        client = connected_client_with_db
        workos_id = await _seed_workos_org(
            client, workos_org_id="org_01ABC", slug="acme-corp"
        )
        client._workos.organizations.update_organization = AsyncMock(
            return_value=_make_workos_org(org_id="org_01ABC", name="Renamed")
        )

        result = (
            await client.update_org(slug="acme-corp", name="Renamed")
        ).raise_for_status()
        assert result.name == "Renamed"
        assert result.id == workos_id
        client._workos.organizations.update_organization.assert_awaited_once_with(
            organization_id="org_01ABC", name="Renamed"
        )

    async def test_update_slug_changes_local_row(
        self, connected_client_with_db
    ) -> None:
        client = connected_client_with_db
        workos_id = await _seed_workos_org(client, slug="old-slug")
        client._workos.organizations.update_organization = AsyncMock(
            return_value=_make_workos_org(slug="new-slug")
        )

        result = (
            await client.update_org(org_id=workos_id, new_slug="new-slug")
        ).raise_for_status()
        assert result.slug == "new-slug"
        rows = await (
            client._db()
            .select(WorkOSOrganization)
            .where(WorkOSOrganization.id == workos_id)
            .execute()
        )
        assert rows[0].slug == "new-slug"

    async def test_update_unknown_slug_returns_error(
        self, connected_client_with_db
    ) -> None:
        """A slug that resolves to nothing → OrgNotFoundError attached, no WorkOS."""
        client = connected_client_with_db
        client._workos.organizations.update_organization = AsyncMock()
        result = await client.update_org(slug="does-not-exist", name="X")
        assert isinstance(result.error, OrgNotFoundError)
        client._workos.organizations.update_organization.assert_not_called()

    async def test_delete_removes_local_row(self, connected_client_with_db) -> None:
        client = connected_client_with_db
        workos_id = await _seed_workos_org(client)
        client._workos.organizations.delete_organization = AsyncMock(return_value=None)

        result = await client.delete_org(org_id=workos_id)
        assert result is True
        rows = await (
            client._db()
            .select(WorkOSOrganization)
            .where(WorkOSOrganization.id == workos_id)
            .execute()
        )
        assert rows == []

    async def test_delete_by_slug(self, connected_client_with_db) -> None:
        """``delete_org`` can address the row by slug; deletes by resolved id."""
        client = connected_client_with_db
        workos_id = await _seed_workos_org(
            client, workos_org_id="org_01ABC", slug="acme-corp"
        )
        client._workos.organizations.delete_organization = AsyncMock(return_value=None)

        result = await client.delete_org(slug="acme-corp")
        assert result is True
        client._workos.organizations.delete_organization.assert_awaited_once_with(
            organization_id=workos_id
        )
        rows = await client._db().select(WorkOSOrganization).execute()
        assert rows == []

    async def test_delete_unknown_slug_returns_false(
        self, connected_client_with_db
    ) -> None:
        """A slug that resolves to nothing → False, no WorkOS call."""
        client = connected_client_with_db
        client._workos.organizations.delete_organization = AsyncMock()
        result = await client.delete_org(slug="never-existed")
        assert result is False
        client._workos.organizations.delete_organization.assert_not_called()

    async def test_delete_workos_404_still_cleans_local(
        self, connected_client_with_db
    ) -> None:
        """If WorkOS already lost the org, scrub the local row anyway."""
        client = connected_client_with_db
        workos_id = await _seed_workos_org(client)
        client._workos.organizations.delete_organization = AsyncMock(
            side_effect=_workos_exc(NotFoundException)
        )

        result = await client.delete_org(org_id=workos_id)
        assert result is True
        rows = await client._db().select(WorkOSOrganization).execute()
        assert rows == []

    async def test_list_orgs_surfaces_all_with_slugs_from_metadata(
        self, connected_client_with_db
    ) -> None:
        """All WorkOS orgs surface; slug comes from each org's metadata."""
        client = connected_client_with_db
        # o2 has no slug in metadata → still surfaces, but with empty slug.
        list_orgs = [
            _make_workos_org(org_id="o1", name="Org A", slug="a"),
            _make_workos_org(org_id="o2", name="Org B"),
        ]
        client._workos.organizations.list_organizations = AsyncMock(
            return_value=_make_list_resource(list_orgs, after=None)
        )

        page = await client.list_orgs()
        assert isinstance(page, Page)
        assert len(page.items) == 2
        by_id = {o.id: o for o in page.items}
        assert by_id["o1"].slug == "a"
        assert by_id["o1"].name == "Org A"
        assert by_id["o2"].slug == ""  # no metadata slug → empty
        assert by_id["o2"].name == "Org B"

    async def test_list_orgs_cursor(self, connected_client_with_db) -> None:
        client = connected_client_with_db
        orgs = [_make_workos_org(org_id="o1", slug="a")]
        client._workos.organizations.list_organizations = AsyncMock(
            return_value=_make_list_resource(orgs, after="cur_next")
        )
        page = await client.list_orgs(limit=2)
        assert page.has_more is True
        assert page.next_cursor == "cur_next"

    async def test_list_orgs_scoped_to_user(self, connected_client_with_db) -> None:
        client = connected_client_with_db
        memberships = _make_list_resource([_make_workos_membership(org_id="o1")])
        client._workos.user_management.list_organization_memberships = AsyncMock(
            return_value=memberships
        )
        client._workos.organizations.get_organization = AsyncMock(
            return_value=_make_workos_org(org_id="o1", slug="a")
        )

        page = await client.list_orgs(user_id="user_01XYZ")
        assert len(page.items) == 1
        assert page.items[0].id == "o1"


# ── Organization Memberships ─────────────────────────────────────


class TestOrgMemberships:
    async def test_add_member(self, connected_client_with_db) -> None:
        client = connected_client_with_db
        await _seed_workos_org(client, workos_org_id="org_01ABC", slug="acme-corp")
        client._workos.user_management.create_organization_membership = AsyncMock(
            return_value=_make_workos_membership()
        )

        result = (
            await client.add_member(
                org_id="org_01ABC", user_id="user_01XYZ", role="member"
            )
        ).raise_for_status()

        assert isinstance(result, OrgMember)
        assert result.org_id == "org_01ABC"
        assert result.user_id == "user_01XYZ"
        assert result.role == "member"
        client._workos.user_management.create_organization_membership.assert_awaited_once_with(
            organization_id="org_01ABC",
            user_id="user_01XYZ",
            role_slug="member",
        )

    async def test_add_member_by_slug(self, connected_client_with_db) -> None:
        """Addressing the org by slug resolves to the WorkOS id for the API call."""
        client = connected_client_with_db
        await _seed_workos_org(client, workos_org_id="org_01ABC", slug="acme-corp")
        client._workos.user_management.create_organization_membership = AsyncMock(
            return_value=_make_workos_membership()
        )

        result = (
            await client.add_member(
                slug="acme-corp", user_id="user_01XYZ", role="member"
            )
        ).raise_for_status()

        assert isinstance(result, OrgMember)
        assert result.org_id == "org_01ABC"
        client._workos.user_management.create_organization_membership.assert_awaited_once_with(
            organization_id="org_01ABC",
            user_id="user_01XYZ",
            role_slug="member",
        )

    async def test_add_member_unknown_slug_returns_error(
        self, connected_client_with_db
    ) -> None:
        client = connected_client_with_db
        client._workos.user_management.create_organization_membership = AsyncMock()
        result = await client.add_member(slug="never-existed", user_id="user_01XYZ")
        assert isinstance(result.error, OrgNotFoundError)
        client._workos.user_management.create_organization_membership.assert_not_called()

    async def test_update_member(self, connected_client_with_db) -> None:
        client = connected_client_with_db
        await _seed_workos_org(client, workos_org_id="org_01ABC", slug="acme-corp")
        membership = _make_workos_membership(role_slug="admin")
        client._workos.user_management.list_organization_memberships = AsyncMock(
            return_value=_make_list_resource([membership])
        )
        client._workos.user_management.update_organization_membership = AsyncMock(
            return_value=_make_workos_membership(role_slug="admin")
        )

        result = (
            await client.update_member(
                org_id="org_01ABC", user_id="user_01XYZ", role="admin"
            )
        ).raise_for_status()
        assert result.role == "admin"
        assert result.org_id == "org_01ABC"

    async def test_update_member_not_found(self, connected_client_with_db) -> None:
        client = connected_client_with_db
        await _seed_workos_org(client, workos_org_id="org_01ABC", slug="acme-corp")
        client._workos.user_management.list_organization_memberships = AsyncMock(
            return_value=_make_list_resource([])
        )
        result = await client.update_member(
            org_id="org_01ABC", user_id="nonexistent", role="admin"
        )
        assert isinstance(result.error, OrgMemberNotFoundError)

    async def test_remove_member(self, connected_client_with_db) -> None:
        client = connected_client_with_db
        await _seed_workos_org(client, workos_org_id="org_01ABC", slug="acme-corp")
        client._workos.user_management.list_organization_memberships = AsyncMock(
            return_value=_make_list_resource([_make_workos_membership()])
        )
        client._workos.user_management.delete_organization_membership = AsyncMock(
            return_value=None
        )

        result = (
            await client.remove_member(org_id="org_01ABC", user_id="user_01XYZ")
        ).raise_for_status()
        assert result is True

    async def test_remove_member_not_found(self, connected_client_with_db) -> None:
        client = connected_client_with_db
        await _seed_workos_org(client, workos_org_id="org_01ABC", slug="acme-corp")
        client._workos.user_management.list_organization_memberships = AsyncMock(
            return_value=_make_list_resource([])
        )

        result = (
            await client.remove_member(org_id="org_01ABC", user_id="nonexistent")
        ).raise_for_status()
        assert result is False

    async def test_remove_member_unknown_slug_returns_false(
        self, connected_client_with_db
    ) -> None:
        client = connected_client_with_db
        client._workos.user_management.list_organization_memberships = AsyncMock()
        result = (
            await client.remove_member(slug="never-existed", user_id="user_01XYZ")
        ).raise_for_status()
        assert result is False
        client._workos.user_management.list_organization_memberships.assert_not_called()

    async def test_list_members_first_page(self, connected_client_with_db) -> None:
        client = connected_client_with_db
        await _seed_workos_org(client, workos_org_id="org_01ABC", slug="acme-corp")
        members = [
            _make_workos_membership(user_id="u1"),
            _make_workos_membership(user_id="u2"),
        ]
        client._workos.user_management.list_organization_memberships = AsyncMock(
            return_value=_make_list_resource(members, after="cursor_abc")
        )

        page = await client.list_members(org_id="org_01ABC", limit=2)
        assert isinstance(page, Page)
        assert len(page.items) == 2
        assert all(isinstance(m, OrgMember) for m in page.items)
        assert page.has_more is True
        assert page.next_cursor == "cursor_abc"

    async def test_list_members_last_page(self, connected_client_with_db) -> None:
        client = connected_client_with_db
        await _seed_workos_org(client, workos_org_id="org_01ABC", slug="acme-corp")
        client._workos.user_management.list_organization_memberships = AsyncMock(
            return_value=_make_list_resource([], after=None)
        )

        page = await client.list_members(
            org_id="org_01ABC", limit=10, cursor="cursor_abc"
        )
        assert page.has_more is False
        assert page.next_cursor is None

    async def test_list_members_by_slug_maps_to_after(
        self, connected_client_with_db
    ) -> None:
        client = connected_client_with_db
        await _seed_workos_org(client, workos_org_id="org_01ABC", slug="acme-corp")
        mock_fn = AsyncMock(return_value=_make_list_resource([], after=None))
        client._workos.user_management.list_organization_memberships = mock_fn
        # Address by slug; the resolved WorkOS id flows to the API.
        await client.list_members(slug="acme-corp", limit=5, cursor="cur_xyz")
        mock_fn.assert_awaited_once_with(
            organization_id="org_01ABC", limit=5, after="cur_xyz"
        )
