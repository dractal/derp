"""Tests for the Google Cloud Identity Platform (GCIP) authentication client."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import jwt as pyjwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from pydantic import ValidationError

from derp.auth.exceptions import (
    AuthBackendError,
    AuthNotConnectedError,
    CapabilityNotSupportedError,
    EmailAlreadyExistsError,
    InvalidCredentialsError,
    InvalidTokenError,
    PasswordValidationError,
    UserNotFoundError,
)
from derp.auth.gcip_client import GCIPAuthClient, _Connection
from derp.auth.models import AuthStatus
from derp.config import (
    AuthConfig,
    GCIPConfig,
    SupabaseConfig,
)

# ── Constants ─────────────────────────────────────────────────────

PROJECT_ID = "test-project-123"
API_KEY = "test-api-key"
TEST_EMAIL = "alice@example.com"
TEST_PASSWORD = "Str0ng!Pass123"
TEST_UID = "uid-abc-123"
EXPECTED_ISSUER = f"https://securetoken.google.com/{PROJECT_ID}"
TEST_KID = "test-kid-1"


# ── RSA keypair / service account ─────────────────────────────────


@pytest.fixture(scope="module")
def rsa_keypair():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key, private_key.public_key()


@pytest.fixture(scope="module")
def sa_pem(rsa_keypair) -> str:
    private_key, _ = rsa_keypair
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


@pytest.fixture(scope="module")
def service_account_json(sa_pem: str) -> str:
    return json.dumps(
        {
            "type": "service_account",
            "project_id": PROJECT_ID,
            "private_key_id": "pk-id-1",
            "private_key": sa_pem,
            "client_email": "sa@test-project.iam.gserviceaccount.com",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    )


@pytest.fixture
def gcip_config(service_account_json: str) -> GCIPConfig:
    return GCIPConfig(
        project_id=PROJECT_ID,
        public_api_key=API_KEY,
        service_account_json=service_account_json,
        org_context_secret="test-org-context-secret-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    )


@pytest.fixture
def gcip_client(gcip_config: GCIPConfig) -> GCIPAuthClient:
    return GCIPAuthClient(gcip_config)


@pytest.fixture
def connected_client(
    gcip_client: GCIPAuthClient, rsa_keypair, sa_pem: str
) -> GCIPAuthClient:
    """Client with a mocked connection (HTTP + JWKS) and a pre-seeded token.

    The JWKS client returns the test public key; the admin token is seeded so
    admin calls don't try to mint one. The real PEM is supplied so the
    admin-token tests that exercise minting can actually sign an assertion.
    """
    _, public_key = rsa_keypair
    jwks = MagicMock()
    jwks.get_signing_key = AsyncMock(return_value=public_key)

    gcip_client._conn = _Connection(
        http=AsyncMock(),
        jwks=jwks,
        client_email="sa@test-project.iam.gserviceaccount.com",
        private_key=sa_pem,
        private_key_id="pk-id-1",
        token_uri="https://oauth2.googleapis.com/token",
        access_token="test-admin-token",
        token_expires_at=time.time() + 3600,
    )
    return gcip_client


# ── ID-token / response builders ──────────────────────────────────


def _make_id_token(
    rsa_keypair,
    *,
    sub: str = TEST_UID,
    role: str | None = None,
    issuer: str = EXPECTED_ISSUER,
    audience: str = PROJECT_ID,
    expired: bool = False,
    future_iat: bool = False,
    kid: str = TEST_KID,
    omit_sub: bool = False,
) -> str:
    private_key, _ = rsa_keypair
    now = datetime.now(UTC)
    iat = now + timedelta(hours=1) if future_iat else now
    exp = now - timedelta(hours=1) if expired else now + timedelta(hours=1)
    payload: dict = {
        "iss": issuer,
        "aud": audience,
        "iat": int(iat.timestamp()),
        "exp": int(exp.timestamp()),
        "email": TEST_EMAIL,
    }
    if not omit_sub:
        payload["sub"] = sub
        payload["user_id"] = sub
    if role:
        payload["role"] = role
    return pyjwt.encode(payload, private_key, algorithm="RS256", headers={"kid": kid})


def _gcip_user(
    *,
    uid: str = TEST_UID,
    email: str = TEST_EMAIL,
    email_verified: bool = True,
    custom: dict | None = None,
    disabled: bool = False,
) -> dict:
    now_ms = str(int(time.time() * 1000))
    record: dict = {
        "localId": uid,
        "email": email,
        "emailVerified": email_verified,
        "displayName": "Alice Smith",
        "photoUrl": "https://example.com/avatar.jpg",
        "createdAt": now_ms,
        "lastLoginAt": now_ms,
        "disabled": disabled,
    }
    if custom is not None:
        record["customAttributes"] = json.dumps(custom)
    return record


def _resp(status_code: int = 200, body: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.is_success = 200 <= status_code < 300
    resp.json = MagicMock(return_value=body or {})
    resp.text = json.dumps(body or {})
    return resp


def _error_body(message: str) -> dict:
    return {"error": {"code": 400, "message": message}}


def _auth_response(*, uid: str = TEST_UID) -> dict:
    return {
        "idToken": "id-token-xyz",
        "refreshToken": "refresh-token-abc",
        "expiresIn": "3600",
        "localId": uid,
        "email": TEST_EMAIL,
    }


# ── Config validation ─────────────────────────────────────────────


class TestConfig:
    def test_valid_config(self, service_account_json: str) -> None:
        cfg = GCIPConfig(
            project_id=PROJECT_ID,
            public_api_key=API_KEY,
            service_account_json=service_account_json,
            org_context_secret="secret-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        )
        assert cfg.project_id == PROJECT_ID

    @pytest.mark.parametrize(
        "missing",
        ["project_id", "public_api_key", "service_account_json", "org_context_secret"],
    )
    def test_missing_required_field(
        self, service_account_json: str, missing: str
    ) -> None:
        fields = {
            "project_id": PROJECT_ID,
            "public_api_key": API_KEY,
            "service_account_json": service_account_json,
            "org_context_secret": "secret-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        }
        del fields[missing]
        with pytest.raises(ValidationError):
            GCIPConfig(**fields)  # ty:ignore[invalid-argument-type]

    def test_single_backend_enforced(self, gcip_config: GCIPConfig) -> None:
        with pytest.raises(ValidationError):
            AuthConfig(
                gcip=gcip_config,
                supabase=SupabaseConfig(
                    url="https://x.supabase.co",
                    anon_key="a",
                    service_role_key="s",
                    jwt_secret="j",
                ),
            )

    def test_gcip_is_a_valid_sole_backend(self, gcip_config: GCIPConfig) -> None:
        cfg = AuthConfig(gcip=gcip_config)
        assert cfg.gcip is gcip_config

    def test_env_resolution(
        self, tmp_path, monkeypatch, service_account_json: str
    ) -> None:
        from derp.config import DerpConfig

        monkeypatch.setenv("GCIP_PROJECT_ID", PROJECT_ID)
        monkeypatch.setenv("GCIP_API_KEY", API_KEY)
        monkeypatch.setenv("GCIP_SA_JSON", service_account_json)
        monkeypatch.setenv("TEST_DB_URL", "postgresql://localhost/test")

        toml = tmp_path / "derp.toml"
        toml.write_text(
            "[database]\n"
            'db_url = "$TEST_DB_URL"\n'
            'schema_path = "src/schema.py"\n\n'
            "[auth.gcip]\n"
            'project_id = "$GCIP_PROJECT_ID"\n'
            'public_api_key = "$GCIP_API_KEY"\n'
            'service_account_json = "$GCIP_SA_JSON"\n'
            'org_context_secret = "test-secret-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"\n'
        )
        config = DerpConfig.load(toml)
        assert config.auth is not None
        assert config.auth.gcip is not None
        assert config.auth.gcip.project_id == PROJECT_ID
        assert config.auth.gcip.service_account_json == service_account_json


# ── Capability flags ──────────────────────────────────────────────


class TestCapabilities:
    def test_capability_flags(self, gcip_client) -> None:
        assert gcip_client.supports_password is True
        assert gcip_client.supports_oauth is True
        assert gcip_client.supports_magic_link is True
        assert gcip_client.supports_user_admin is True
        # Orgs/invitations are layered by derp in Postgres + token claims.
        assert gcip_client.supports_orgs is True
        assert gcip_client.supports_invitations is True
        # GCIP has no server-side session listing or multi-tenancy.
        assert gcip_client.supports_sessions is False
        assert gcip_client.supports_multi_tenant is False


# ── JWT verification / authenticate ───────────────────────────────


class TestAuthenticate:
    async def test_valid_token(self, connected_client, rsa_keypair) -> None:
        token = _make_id_token(rsa_keypair, role="admin")
        request = MagicMock()
        request.headers = {"Authorization": f"Bearer {token}"}
        session = await connected_client.authenticate(request)
        assert session is not None
        assert session.user_id == TEST_UID
        assert session.session_id == TEST_UID
        assert session.roles == ("admin",)
        assert session.tenant_id is None
        assert session.org_id is None
        assert session.org_role is None
        assert session.is_anonymous is False
        assert session.mfa.enrolled is False
        assert session.mfa.satisfied is False
        # The raw verified claims are exposed as an escape hatch.
        assert session.claims["email"] == TEST_EMAIL

    async def test_role_defaults_when_absent(
        self, connected_client, rsa_keypair
    ) -> None:
        token = _make_id_token(rsa_keypair)
        request = MagicMock()
        request.headers = {"Authorization": f"Bearer {token}"}
        session = await connected_client.authenticate(request)
        assert session is not None
        assert session.roles == ("default",)

    async def test_verify_token_directly(self, connected_client, rsa_keypair) -> None:
        token = _make_id_token(rsa_keypair, role="editor")
        session = await connected_client.verify_token(token)
        assert session is not None
        assert session.roles == ("editor",)

    async def test_verify_token_has_no_active_org(
        self, connected_client, rsa_keypair
    ) -> None:
        """verify_token is identity-only; authenticate layers on the active org."""
        session = await connected_client.verify_token(_make_id_token(rsa_keypair))
        assert session is not None
        assert session.org_id is None
        assert session.org_slug is None
        assert session.org_role is None

    # NOTE: the active-org binding paths (bind / fresh-role-after-demotion /
    # removed-member / user-binding / cookie-ignored) are exercised end-to-end
    # against a real Postgres DB in tests/auth/test_gcip_orgs.py
    # (TestAuthenticateActiveOrg), since binding now resolves the role from the
    # DB rather than from the pointer.

    async def test_authenticate_garbage_org_context_degrades(
        self, connected_client, rsa_keypair
    ) -> None:
        """A malformed credential degrades to no active org — never 500s."""
        token = _make_id_token(rsa_keypair)
        request = MagicMock()
        request.headers = {
            "Authorization": f"Bearer {token}",
            "X-Org-Context": "not-a-valid-credential",
        }
        session = await connected_client.authenticate(request)
        assert session is not None
        assert session.org_id is None

    async def test_missing_auth_header(self, connected_client) -> None:
        request = MagicMock()
        request.headers = {}
        assert await connected_client.authenticate(request) is None

    async def test_non_bearer_header(self, connected_client) -> None:
        request = MagicMock()
        request.headers = {"Authorization": "Basic abc"}
        assert await connected_client.authenticate(request) is None

    async def test_expired_token(self, connected_client, rsa_keypair) -> None:
        token = _make_id_token(rsa_keypair, expired=True)
        request = MagicMock()
        request.headers = {"Authorization": f"Bearer {token}"}
        assert await connected_client.authenticate(request) is None

    async def test_wrong_issuer(self, connected_client, rsa_keypair) -> None:
        token = _make_id_token(rsa_keypair, issuer="https://evil.example.com")
        request = MagicMock()
        request.headers = {"Authorization": f"Bearer {token}"}
        assert await connected_client.authenticate(request) is None

    async def test_wrong_audience(self, connected_client, rsa_keypair) -> None:
        token = _make_id_token(rsa_keypair, audience="other-project")
        request = MagicMock()
        request.headers = {"Authorization": f"Bearer {token}"}
        assert await connected_client.authenticate(request) is None

    async def test_bad_signature(self, connected_client, rsa_keypair) -> None:
        # Sign with a different key than the JWKS public key.
        other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        token = pyjwt.encode(
            {
                "iss": EXPECTED_ISSUER,
                "aud": PROJECT_ID,
                "sub": TEST_UID,
                "iat": int(time.time()),
                "exp": int(time.time()) + 3600,
            },
            other_key,
            algorithm="RS256",
            headers={"kid": TEST_KID},
        )
        request = MagicMock()
        request.headers = {"Authorization": f"Bearer {token}"}
        assert await connected_client.authenticate(request) is None

    async def test_future_iat(self, connected_client, rsa_keypair) -> None:
        token = _make_id_token(rsa_keypair, future_iat=True)
        request = MagicMock()
        request.headers = {"Authorization": f"Bearer {token}"}
        assert await connected_client.authenticate(request) is None

    async def test_jwks_key_resolution_error(self, connected_client) -> None:
        connected_client._conn.jwks.get_signing_key_from_jwt = MagicMock(
            side_effect=pyjwt.exceptions.PyJWKClientError("no key")
        )
        request = MagicMock()
        request.headers = {"Authorization": "Bearer some.jwt.token"}
        assert await connected_client.authenticate(request) is None

    async def test_jwks_cached_no_refetch(self, connected_client, rsa_keypair) -> None:
        """Two authenticate calls reuse the in-memory PyJWKClient key cache."""
        token = _make_id_token(rsa_keypair)
        request = MagicMock()
        request.headers = {"Authorization": f"Bearer {token}"}
        await connected_client.authenticate(request)
        await connected_client.authenticate(request)
        # PyJWKClient is the single key source; no HTTP fetch happens through
        # our own client (it manages caching internally).
        connected_client._conn.http.get.assert_not_called()

    async def test_authenticate_requires_connect(self, gcip_client) -> None:
        request = MagicMock()
        request.headers = {"Authorization": "Bearer some.token"}
        with pytest.raises(AuthNotConnectedError):
            await gcip_client.authenticate(request)


# ── Admin token minting + 401 refresh ─────────────────────────────


class TestAdminToken:
    async def test_mint_admin_token(self, connected_client) -> None:
        conn = connected_client._conn
        conn.access_token = None  # force a mint
        conn.http.post = AsyncMock(
            return_value=_resp(
                200, {"access_token": "minted-token", "expires_in": 3600}
            )
        )
        token = await connected_client._ensure_admin_token(conn)
        assert token == "minted-token"
        # The exchange POSTs a JWT-bearer assertion to the token URI.
        _, kwargs = conn.http.post.call_args
        assert kwargs["data"]["grant_type"] == (
            "urn:ietf:params:oauth:grant-type:jwt-bearer"
        )
        assert "assertion" in kwargs["data"]

    async def test_admin_token_cached(self, connected_client) -> None:
        conn = connected_client._conn
        conn.access_token = None
        conn.http.post = AsyncMock(
            return_value=_resp(
                200, {"access_token": "minted-token", "expires_in": 3600}
            )
        )
        await connected_client._ensure_admin_token(conn)
        await connected_client._ensure_admin_token(conn)
        assert conn.http.post.call_count == 1

    async def test_admin_token_minted_once_under_concurrency(
        self, connected_client
    ) -> None:
        # N concurrent requests on a cold token must mint exactly one (the lock
        # collapses the thundering herd); without it each would mint its own.
        conn = connected_client._conn
        conn.access_token = None
        conn.token_expires_at = 0.0
        posts = 0

        async def fake_post(url: str, **kwargs: object) -> object:
            nonlocal posts
            await asyncio.sleep(0)  # yield so the herd forms on the lock
            posts += 1
            return _resp(200, {"access_token": "minted", "expires_in": 3600})

        conn.http.post = fake_post
        tokens = await asyncio.gather(
            *(connected_client._ensure_admin_token(conn) for _ in range(20))
        )
        assert posts == 1
        assert all(t == "minted" for t in tokens)

    async def test_admin_token_exchange_failure(self, connected_client) -> None:
        conn = connected_client._conn
        conn.access_token = None
        conn.http.post = AsyncMock(return_value=_resp(403, {"error": "denied"}))
        with pytest.raises(AuthBackendError):
            await connected_client._ensure_admin_token(conn)

    async def test_invalid_service_account_json(self) -> None:
        # A malformed service-account key fails fast at connect().
        client = GCIPAuthClient(
            GCIPConfig(
                project_id=PROJECT_ID,
                public_api_key=API_KEY,
                service_account_json="not-json",
                org_context_secret="secret-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            )
        )
        with pytest.raises(AuthBackendError):
            await client.connect()

    async def test_admin_request_refreshes_on_401(self, connected_client) -> None:
        conn = connected_client._conn
        # First admin call 401s, mint refreshes, retry succeeds.
        conn.http.request = AsyncMock(
            side_effect=[_resp(401, {}), _resp(200, {"users": [_gcip_user()]})]
        )
        conn.http.post = AsyncMock(
            return_value=_resp(200, {"access_token": "fresh-token", "expires_in": 3600})
        )
        conn.access_token = "stale-token"
        conn.token_expires_at = time.time() + 3600

        resp = await connected_client._admin_request(
            "POST", "/accounts:lookup", json={"localId": [TEST_UID]}
        )
        assert resp.is_success
        assert conn.http.request.call_count == 2
        # The retry used the freshly-minted token.
        _, kwargs = conn.http.request.call_args
        assert kwargs["headers"]["Authorization"] == "Bearer fresh-token"


# ── User management ───────────────────────────────────────────────


class TestUserManagement:
    async def test_get_user(self, connected_client) -> None:
        connected_client._conn.http.request = AsyncMock(
            return_value=_resp(200, {"users": [_gcip_user(custom={"role": "admin"})]})
        )
        user = await connected_client.get_user(TEST_UID)
        assert user.id == TEST_UID
        assert user.email == TEST_EMAIL
        assert user.roles == ("admin",)
        assert user.disabled is False
        assert user.tenant_id is None
        assert user.is_anonymous is False
        assert user.email_verified is True

    async def test_get_user_excludes_reserved_keys_from_metadata(
        self, connected_client
    ) -> None:
        """derp-reserved custom keys are kept out of user-facing metadata."""
        custom = {
            "role": "editor",
            "is_superuser": False,
            "first_name": "Alice",
            "last_name": "Smith",
            "plan": "pro",
        }
        connected_client._conn.http.request = AsyncMock(
            return_value=_resp(200, {"users": [_gcip_user(custom=custom)]})
        )
        identity = await connected_client.get_user(TEST_UID)
        assert identity.metadata == {"plan": "pro"}
        assert identity.roles == ("editor",)

    async def test_get_user_not_found(self, connected_client) -> None:
        connected_client._conn.http.request = AsyncMock(
            return_value=_resp(200, {"users": []})
        )
        with pytest.raises(UserNotFoundError):
            await connected_client.get_user("missing")

    async def test_get_user_routes_through_admin_url(self, connected_client) -> None:
        connected_client._conn.http.request = AsyncMock(
            return_value=_resp(200, {"users": [_gcip_user()]})
        )
        await connected_client.get_user(TEST_UID)
        args, kwargs = connected_client._conn.http.request.call_args
        url = args[1]
        assert url == (
            "https://identitytoolkit.googleapis.com/v1/projects/"
            f"{PROJECT_ID}/accounts:lookup"
        )

    async def test_find_user_by_email(self, connected_client) -> None:
        connected_client._conn.http.request = AsyncMock(
            return_value=_resp(200, {"users": [_gcip_user()]})
        )
        user = await connected_client.find_user(email=TEST_EMAIL)
        assert user is not None
        assert user.id == TEST_UID

    async def test_find_user_absent(self, connected_client) -> None:
        connected_client._conn.http.request = AsyncMock(
            return_value=_resp(200, {"users": []})
        )
        assert await connected_client.find_user(user_id="missing") is None

    async def test_find_user_requires_one_key(self, connected_client) -> None:
        with pytest.raises(ValueError):
            await connected_client.find_user()
        with pytest.raises(ValueError):
            await connected_client.find_user(user_id=TEST_UID, email=TEST_EMAIL)

    async def test_find_user_phone_unsupported(self, connected_client) -> None:
        with pytest.raises(ValueError):
            await connected_client.find_user(phone="+15551234567")

    async def test_list_users(self, connected_client) -> None:
        connected_client._conn.http.request = AsyncMock(
            return_value=_resp(
                200,
                {"users": [_gcip_user(uid="u1"), _gcip_user(uid="u2")]},
            )
        )
        page = await connected_client.list_users()
        assert [u.id for u in page.items] == ["u1", "u2"]
        assert page.next_cursor is None
        assert page.has_more is False

    async def test_list_users_cursor(self, connected_client) -> None:
        connected_client._conn.http.request = AsyncMock(
            return_value=_resp(
                200,
                {"users": [_gcip_user(uid="u1")], "nextPageToken": "next-tok"},
            )
        )
        page = await connected_client.list_users(limit=1)
        assert [u.id for u in page.items] == ["u1"]
        assert page.has_more is True
        assert page.next_cursor == "next-tok"
        # The cursor is forwarded as GCIP's nextPageToken on the next call.
        await connected_client.list_users(limit=1, cursor="next-tok")
        _, kwargs = connected_client._conn.http.request.call_args
        assert kwargs["params"]["nextPageToken"] == "next-tok"

    async def test_update_user(self, connected_client) -> None:
        # read-merge lookup -> accounts:update -> get_user re-reads canonical.
        connected_client._conn.http.request = AsyncMock(
            side_effect=[
                _resp(200, {"users": [_gcip_user(custom={"role": "old"})]}),
                _resp(200, {"localId": TEST_UID}),
                _resp(200, {"users": [_gcip_user(custom={"role": "editor"})]}),
            ]
        )
        user = (
            await connected_client.update_user(
                user_id=TEST_UID, email="new@example.com", role="editor"
            )
        ).raise_for_status()
        assert user.roles == ("editor",)
        # The accounts:update call (index 1) carries the merged custom attrs.
        update_call = connected_client._conn.http.request.call_args_list[1]
        body = update_call.kwargs["json"]
        assert body["email"] == "new@example.com"
        assert json.loads(body["customAttributes"])["role"] == "editor"

    async def test_update_user_preserves_org_claim(self, connected_client) -> None:
        """A profile update must not clobber the derp-projected ``orgs`` claim."""
        existing = {"orgs": {"acme": {"id": "o1", "role": "admin"}}, "role": "old"}
        connected_client._conn.http.request = AsyncMock(
            side_effect=[
                _resp(
                    200,
                    {
                        "users": [
                            {
                                "localId": TEST_UID,
                                "customAttributes": json.dumps(existing),
                            }
                        ]
                    },
                ),
                _resp(200, {"localId": TEST_UID}),
                _resp(200, {"users": [_gcip_user()]}),
            ]
        )
        await connected_client.update_user(user_id=TEST_UID, role="new")
        written = json.loads(
            connected_client._conn.http.request.call_args_list[1].kwargs["json"][
                "customAttributes"
            ]
        )
        assert written["orgs"] == {"acme": {"id": "o1", "role": "admin"}}  # preserved
        assert written["role"] == "new"  # updated

    async def test_update_user_not_found(self, connected_client) -> None:
        connected_client._conn.http.request = AsyncMock(
            return_value=_resp(400, _error_body("USER_NOT_FOUND"))
        )
        result = await connected_client.update_user(user_id="missing", email="x@y.com")
        assert isinstance(result.error, UserNotFoundError)

    async def test_delete_user(self, connected_client) -> None:
        connected_client._conn.http.request = AsyncMock(return_value=_resp(200, {}))
        assert await connected_client.delete_user(TEST_UID) is True

    async def test_delete_user_not_found(self, connected_client) -> None:
        connected_client._conn.http.request = AsyncMock(
            return_value=_resp(400, _error_body("USER_NOT_FOUND"))
        )
        assert await connected_client.delete_user("missing") is False


# ── Sign-up / sign-in flows ───────────────────────────────────────


class TestSignUpSignIn:
    async def test_sign_up(self, connected_client) -> None:
        connected_client._conn.http.post = AsyncMock(
            side_effect=[
                _resp(200, _auth_response()),  # accounts:signUp
                _resp(200, {"users": [_gcip_user()]}),  # accounts:lookup
            ]
        )
        outcome = await connected_client.sign_up(
            email=TEST_EMAIL, password=TEST_PASSWORD
        )
        assert outcome.status is AuthStatus.COMPLETE
        assert outcome.ok is True
        assert outcome.identity is not None
        assert outcome.identity.email == TEST_EMAIL
        assert outcome.tokens is not None
        assert outcome.tokens.access_token == "id-token-xyz"
        assert outcome.tokens.id_token == "id-token-xyz"
        assert outcome.tokens.refresh_token == "refresh-token-abc"

    async def test_sign_up_email_exists(self, connected_client) -> None:
        connected_client._conn.http.post = AsyncMock(
            return_value=_resp(400, _error_body("EMAIL_EXISTS"))
        )
        outcome = await connected_client.sign_up(
            email=TEST_EMAIL, password=TEST_PASSWORD
        )
        assert outcome.status is AuthStatus.EMAIL_EXISTS
        assert outcome.ok is False
        assert isinstance(outcome.error, EmailAlreadyExistsError)

    async def test_sign_up_weak_password(self, connected_client) -> None:
        connected_client._conn.http.post = AsyncMock(
            return_value=_resp(400, _error_body("WEAK_PASSWORD : too short"))
        )
        outcome = await connected_client.sign_up(email=TEST_EMAIL, password="x")
        assert outcome.status is AuthStatus.WEAK_PASSWORD
        assert isinstance(outcome.error, PasswordValidationError)

    async def test_sign_in_with_password(self, connected_client) -> None:
        connected_client._conn.http.post = AsyncMock(
            side_effect=[
                _resp(200, _auth_response()),
                _resp(200, {"users": [_gcip_user()]}),
            ]
        )
        outcome = await connected_client.sign_in_with_password(
            identifier=TEST_EMAIL, password=TEST_PASSWORD
        )
        assert outcome.status is AuthStatus.COMPLETE
        assert outcome.identity is not None
        assert outcome.identity.id == TEST_UID

    async def test_sign_in_bad_credentials(self, connected_client) -> None:
        connected_client._conn.http.post = AsyncMock(
            return_value=_resp(400, _error_body("INVALID_LOGIN_CREDENTIALS"))
        )
        outcome = await connected_client.sign_in_with_password(
            identifier=TEST_EMAIL, password="wrong"
        )
        assert outcome.status is AuthStatus.INVALID_CREDENTIALS
        assert isinstance(outcome.error, InvalidCredentialsError)

    async def test_send_magic_link(self, connected_client) -> None:
        connected_client._conn.http.post = AsyncMock(
            return_value=_resp(200, {"email": TEST_EMAIL})
        )
        await connected_client.send_magic_link(
            email=TEST_EMAIL, redirect_url="https://app.example.com/finish"
        )
        _, kwargs = connected_client._conn.http.post.call_args
        assert kwargs["json"]["requestType"] == "EMAIL_SIGNIN"
        assert kwargs["json"]["continueUrl"] == "https://app.example.com/finish"

    async def test_verify_magic_link(self, connected_client) -> None:
        connected_client._conn.http.post = AsyncMock(
            side_effect=[
                _resp(200, _auth_response()),
                _resp(200, {"users": [_gcip_user()]}),
            ]
        )
        outcome = await connected_client.verify_magic_link("oob-code", email=TEST_EMAIL)
        assert outcome.status is AuthStatus.COMPLETE
        assert outcome.identity is not None
        assert outcome.identity.email == TEST_EMAIL

    async def test_verify_magic_link_requires_email(self, connected_client) -> None:
        with pytest.raises(ValueError):
            await connected_client.verify_magic_link("oob-code")

    async def test_verify_magic_link_invalid(self, connected_client) -> None:
        connected_client._conn.http.post = AsyncMock(
            return_value=_resp(400, _error_body("INVALID_OOB_CODE"))
        )
        outcome = await connected_client.verify_magic_link("bad", email=TEST_EMAIL)
        assert outcome.status is AuthStatus.INVALID_TOKEN
        assert isinstance(outcome.error, InvalidTokenError)

    async def test_sign_in_with_oauth(self, connected_client) -> None:
        connected_client._conn.http.post = AsyncMock(
            side_effect=[
                _resp(200, _auth_response()),
                _resp(200, {"users": [_gcip_user()]}),
            ]
        )
        outcome = await connected_client.sign_in_with_oauth(
            "idp-id-token", provider="google"
        )
        assert outcome.status is AuthStatus.COMPLETE
        assert outcome.identity is not None
        assert outcome.identity.id == TEST_UID
        first_call = connected_client._conn.http.post.call_args_list[0]
        assert "providerId=google.com" in first_call.kwargs["json"]["postBody"]

    async def test_sign_in_with_oauth_rejected(self, connected_client) -> None:
        connected_client._conn.http.post = AsyncMock(
            return_value=_resp(400, _error_body("INVALID_IDP_RESPONSE"))
        )
        outcome = await connected_client.sign_in_with_oauth("bad", provider="google")
        assert outcome.status is AuthStatus.INVALID_CREDENTIALS
        assert isinstance(outcome.error, InvalidCredentialsError)


# ── Tokens / recovery ─────────────────────────────────────────────


class TestTokensAndRecovery:
    async def test_refresh(self, connected_client) -> None:
        connected_client._conn.http.post = AsyncMock(
            return_value=_resp(
                200,
                {
                    "id_token": "new-id-token",
                    "refresh_token": "new-refresh-token",
                    "expires_in": "3600",
                },
            )
        )
        tokens = (
            await connected_client.refresh("old-refresh-token")
        ).raise_for_status()
        assert tokens.access_token == "new-id-token"
        assert tokens.id_token == "new-id-token"
        assert tokens.refresh_token == "new-refresh-token"

    async def test_refresh_invalid(self, connected_client) -> None:
        connected_client._conn.http.post = AsyncMock(
            return_value=_resp(400, _error_body("INVALID_REFRESH_TOKEN"))
        )
        result = await connected_client.refresh("bad")
        assert isinstance(result.error, InvalidTokenError)

    async def test_request_password_reset(self, connected_client) -> None:
        connected_client._conn.http.post = AsyncMock(
            return_value=_resp(200, {"email": TEST_EMAIL})
        )
        await connected_client.request_password_reset(email=TEST_EMAIL)
        _, kwargs = connected_client._conn.http.post.call_args
        assert kwargs["json"]["requestType"] == "PASSWORD_RESET"

    async def test_reset_password_success(self, connected_client) -> None:
        conn = connected_client._conn
        conn.http.post = AsyncMock(return_value=_resp(200, {"email": TEST_EMAIL}))
        conn.http.request = AsyncMock(
            return_value=_resp(200, {"users": [_gcip_user()]})
        )
        result = await connected_client.reset_password("oob-code", "Str0ng!Pass123")
        assert result.ok
        assert result.value.email == TEST_EMAIL
        args, kwargs = conn.http.post.call_args
        assert "accounts:resetPassword" in args[0]
        assert kwargs["json"] == {
            "oobCode": "oob-code",
            "newPassword": "Str0ng!Pass123",
        }

    async def test_reset_password_invalid_code(self, connected_client) -> None:
        connected_client._conn.http.post = AsyncMock(
            return_value=_resp(400, _error_body("INVALID_OOB_CODE"))
        )
        result = await connected_client.reset_password("bad", "Str0ng!Pass123")
        assert not result.ok
        assert isinstance(result.error, InvalidTokenError)

    async def test_reset_password_weak_password(self, connected_client) -> None:
        connected_client._conn.http.post = AsyncMock(
            return_value=_resp(400, _error_body("WEAK_PASSWORD : too short"))
        )
        result = await connected_client.reset_password("oob-code", "123")
        assert isinstance(result.error, PasswordValidationError)

    async def test_verify_email_success(self, connected_client) -> None:
        conn = connected_client._conn
        conn.http.post = AsyncMock(
            return_value=_resp(200, {"localId": TEST_UID, "emailVerified": True})
        )
        conn.http.request = AsyncMock(
            return_value=_resp(200, {"users": [_gcip_user()]})
        )
        result = await connected_client.verify_email("oob-code")
        assert result.ok
        assert result.value.id == TEST_UID
        args, kwargs = conn.http.post.call_args
        assert "accounts:update" in args[0]
        assert kwargs["json"] == {"oobCode": "oob-code"}

    async def test_verify_email_expired_code(self, connected_client) -> None:
        connected_client._conn.http.post = AsyncMock(
            return_value=_resp(400, _error_body("EXPIRED_OOB_CODE"))
        )
        result = await connected_client.verify_email("bad")
        assert isinstance(result.error, InvalidTokenError)


# ── Sessions / unsupported features ───────────────────────────────


class TestSessionsAndUnsupported:
    async def test_revoke_all_sessions(self, connected_client) -> None:
        connected_client._conn.http.request = AsyncMock(return_value=_resp(200, {}))
        await connected_client.revoke_all_sessions(TEST_UID)
        _, kwargs = connected_client._conn.http.request.call_args
        assert "validSince" in kwargs["json"]

    async def test_revoke_all_sessions_user_not_found(self, connected_client) -> None:
        connected_client._conn.http.request = AsyncMock(
            return_value=_resp(400, _error_body("USER_NOT_FOUND"))
        )
        result = await connected_client.revoke_all_sessions("missing")
        assert isinstance(result.error, UserNotFoundError)

    async def test_list_sessions_unsupported(self, connected_client) -> None:
        with pytest.raises(CapabilityNotSupportedError):
            await connected_client.list_sessions(user_id=TEST_UID)

    async def test_revoke_session_unsupported(self, connected_client) -> None:
        with pytest.raises(CapabilityNotSupportedError):
            await connected_client.revoke_session("session-1")

    async def test_set_active_org_clear_returns_empty(self, connected_client) -> None:
        # Clearing the active org needs no DB and returns an empty credential.
        tokens = (
            await connected_client.set_active_org(session_id=TEST_UID)
        ).raise_for_status()
        assert tokens.access_token == ""

    async def test_authorization_url_unsupported(self, connected_client) -> None:
        # GCIP drives OAuth/SSO redirects client-side via its SDK.
        with pytest.raises(CapabilityNotSupportedError):
            connected_client.authorization_url(provider="google", state="xyz")
