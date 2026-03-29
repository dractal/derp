"""Tests for the Cognito authentication client."""

from __future__ import annotations

import base64
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import jwt as pyjwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from derp.auth.cognito_client import CognitoAuthClient
from derp.config import (
    AuthConfig,
    ClerkConfig,
    CognitoConfig,
    JWTConfig,
    NativeAuthConfig,
)

# ── Constants ─────────────────────────────────────────────────────

REGION = "us-east-1"
TEST_EMAIL = "alice@example.com"
TEST_PASSWORD = "Str0ng!Pass123"


# ── Moto server fixture ──────────────────────────────────────────


@pytest.fixture(scope="session")
def moto_server():
    """Start a moto server for the entire test session."""
    server = ThreadedMotoServer(port=0, verbose=False)
    server.start()
    yield f"http://localhost:{server._server.server_address[1]}"  # ty:ignore[possibly-missing-attribute]
    server.stop()


@pytest.fixture(autouse=True)
def _reset_moto(moto_server):
    """Reset moto state before each test."""
    import requests

    requests.post(f"{moto_server}/moto-api/reset")
    yield


@pytest.fixture
async def cognito_env(moto_server):
    """Create a Cognito pool, app client, and connected CognitoAuthClient."""
    import aiobotocore.session

    session = aiobotocore.session.get_session()
    async with session.create_client(
        "cognito-idp",
        region_name=REGION,
        endpoint_url=moto_server,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    ) as admin:
        pool_resp = await admin.create_user_pool(
            PoolName="test-pool",
            Policies={
                "PasswordPolicy": {
                    "MinimumLength": 8,
                    "RequireUppercase": True,
                    "RequireLowercase": True,
                    "RequireNumbers": True,
                    "RequireSymbols": True,
                }
            },
        )
        pool_id = pool_resp["UserPool"]["Id"]

        app_resp = await admin.create_user_pool_client(
            UserPoolId=pool_id,
            ClientName="test-app",
            ExplicitAuthFlows=[
                "ALLOW_USER_PASSWORD_AUTH",
                "ALLOW_ADMIN_USER_PASSWORD_AUTH",
                "ALLOW_REFRESH_TOKEN_AUTH",
            ],
            GenerateSecret=True,
        )
        client_id = app_resp["UserPoolClient"]["ClientId"]
        client_secret = app_resp["UserPoolClient"]["ClientSecret"]

    config = CognitoConfig(
        user_pool_id=pool_id,
        client_id=client_id,
        client_secret=client_secret,
        region=REGION,
        access_key_id="testing",
        secret_access_key="testing",
    )
    # Patch the session to use the moto endpoint
    auth_client = CognitoAuthClient(config)
    auth_client._session = aiobotocore.session.get_session()
    # Override connect to use the moto endpoint
    ctx = auth_client._session.create_client(
        "cognito-idp",
        region_name=REGION,
        endpoint_url=moto_server,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    auth_client._client_ctx = ctx
    auth_client._client = await ctx.__aenter__()

    yield auth_client, pool_id, admin

    await auth_client.disconnect()


async def _create_confirmed_user(
    cognito_env: tuple,
    email: str = TEST_EMAIL,
    password: str = TEST_PASSWORD,
) -> str:
    """Create and confirm a user, return sub."""
    client, pool_id, _ = cognito_env
    import aiobotocore.session

    # Extract the moto endpoint from the client's existing connection.
    endpoint_url = client._client._endpoint.host

    session = aiobotocore.session.get_session()
    async with session.create_client(
        "cognito-idp",
        region_name=REGION,
        endpoint_url=endpoint_url,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    ) as admin:
        resp = await admin.admin_create_user(
            UserPoolId=pool_id,
            Username=email,
            UserAttributes=[
                {"Name": "email", "Value": email},
                {"Name": "email_verified", "Value": "true"},
            ],
            TemporaryPassword=password,
            MessageAction="SUPPRESS",
        )
        sub = next(a["Value"] for a in resp["User"]["Attributes"] if a["Name"] == "sub")
        await admin.admin_set_user_password(
            UserPoolId=pool_id,
            Username=email,
            Password=password,
            Permanent=True,
        )
    return sub


# ── Authenticate (JWKS + JWT — no moto needed) ───────────────────


@pytest.fixture
def rsa_keypair():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key, private_key.public_key()


@pytest.fixture
def jwks_json(rsa_keypair):
    _, public_key = rsa_keypair
    pub_numbers = public_key.public_numbers()

    def _int_to_base64(n: int, length: int) -> str:
        return (
            base64.urlsafe_b64encode(n.to_bytes(length, "big"))
            .decode("ascii")
            .rstrip("=")
        )

    return {
        "keys": [
            {
                "kty": "RSA",
                "kid": "test-kid-1",
                "use": "sig",
                "alg": "RS256",
                "n": _int_to_base64(pub_numbers.n, 256),
                "e": _int_to_base64(pub_numbers.e, 3),
            }
        ]
    }


@pytest.fixture
def jwt_client() -> CognitoAuthClient:
    """Client for JWT-only tests (no moto needed)."""
    config = CognitoConfig(
        user_pool_id="us-east-1_TestPool",
        client_id="test-client-id",
        client_secret="test-client-secret",
        region=REGION,
    )
    return CognitoAuthClient(config)


class TestAuthenticate:
    async def test_valid_token(self, jwt_client, rsa_keypair, jwks_json):
        private_key, _ = rsa_keypair
        now = datetime.now(UTC)
        payload = {
            "sub": "user-123",
            "jti": "session-456",
            "iss": ("https://cognito-idp.us-east-1.amazonaws.com/us-east-1_TestPool"),
            "client_id": "test-client-id",
            "token_use": "access",
            "iat": int(now.timestamp()),
            "exp": int((now + timedelta(hours=1)).timestamp()),
            "cognito:groups": ["admin"],
        }
        token = pyjwt.encode(
            payload,
            private_key,
            algorithm="RS256",
            headers={"kid": "test-kid-1"},
        )
        request = MagicMock()
        request.headers = {"Authorization": f"Bearer {token}"}

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = jwks_json
        mock_resp.raise_for_status = MagicMock()

        with patch("derp.auth.cognito_client.httpx.AsyncClient") as mock_http:
            mock_http.return_value.__aenter__ = AsyncMock(
                return_value=MagicMock(get=AsyncMock(return_value=mock_resp))
            )
            mock_http.return_value.__aexit__ = AsyncMock(return_value=False)
            session = await jwt_client.authenticate(request)

        assert session is not None
        assert session.user_id == "user-123"
        assert session.session_id == "session-456"
        assert session.role == "admin"

    async def test_missing_auth_header(self, jwt_client):
        request = MagicMock()
        request.headers = {}
        assert await jwt_client.authenticate(request) is None

    async def test_invalid_token(self, jwt_client):
        request = MagicMock()
        request.headers = {"Authorization": "Bearer not-a-jwt"}
        assert await jwt_client.authenticate(request) is None


# ── User Management (moto) ───────────────────────────────────────


class TestGetUser:
    async def test_success(self, cognito_env):
        client, _, _ = cognito_env
        await _create_confirmed_user(cognito_env)

        user = await client.get_user(TEST_EMAIL)
        assert user is not None
        assert user.email == TEST_EMAIL
        assert user.is_active is True

    async def test_not_found(self, cognito_env):
        client, _, _ = cognito_env
        user = await client.get_user("nonexistent")
        assert user is None


class TestListUsers:
    async def test_list(self, cognito_env):
        client, _, _ = cognito_env
        await _create_confirmed_user(cognito_env, "a@test.com")
        await _create_confirmed_user(cognito_env, "b@test.com")

        users = await client.list_users(limit=10)
        assert len(users) == 2
        emails = {u.email for u in users}
        assert "a@test.com" in emails
        assert "b@test.com" in emails


class TestDeleteUser:
    async def test_success(self, cognito_env):
        client, _, _ = cognito_env
        await _create_confirmed_user(cognito_env)

        result = await client.delete_user(TEST_EMAIL)
        assert result is True

        user = await client.get_user(TEST_EMAIL)
        assert user is None

    async def test_not_found(self, cognito_env):
        client, _, _ = cognito_env
        result = await client.delete_user("nonexistent")
        assert result is False


# ── Sign-up / Sign-in (moto) ─────────────────────────────────────


class TestSignUp:
    async def test_success(self, cognito_env):
        client, _, _ = cognito_env

        result = await client.sign_up(email="new@example.com", password=TEST_PASSWORD)
        assert result is not None
        assert result.user.email == "new@example.com"
        assert result.tokens.access_token

    async def test_user_exists(self, cognito_env):
        client, _, _ = cognito_env
        await _create_confirmed_user(cognito_env)

        with pytest.raises(Exception):
            await client.sign_up(email=TEST_EMAIL, password=TEST_PASSWORD)


class TestSignIn:
    async def test_success(self, cognito_env):
        client, _, _ = cognito_env
        await _create_confirmed_user(cognito_env)

        result = await client.sign_in_with_password(TEST_EMAIL, TEST_PASSWORD)
        assert result is not None
        assert result.user.email == TEST_EMAIL
        assert result.tokens.access_token
        assert result.tokens.refresh_token

    async def test_invalid_credentials(self, cognito_env):
        client, _, _ = cognito_env
        await _create_confirmed_user(cognito_env)

        with pytest.raises(Exception):
            await client.sign_in_with_password(TEST_EMAIL, "WrongPass123!")


# ── Token Refresh (moto) ─────────────────────────────────────────


class TestRefreshToken:
    async def test_success(self, cognito_env):
        client, _, _ = cognito_env
        await _create_confirmed_user(cognito_env)

        sign_in = await client.sign_in_with_password(TEST_EMAIL, TEST_PASSWORD)
        assert sign_in is not None

        tokens = await client.refresh_token(
            sign_in.tokens.refresh_token,
            username=TEST_EMAIL,
        )
        assert tokens is not None
        assert tokens.access_token


# ── Sign-out (moto) ──────────────────────────────────────────────


class TestSignOut:
    async def test_sign_out_all(self, cognito_env):
        client, _, _ = cognito_env
        await _create_confirmed_user(cognito_env)
        await client.sign_out_all(TEST_EMAIL)

    async def test_sign_out_delegates_to_global(self, cognito_env):
        client, _, _ = cognito_env
        await _create_confirmed_user(cognito_env)
        await client.sign_out(TEST_EMAIL)


# ── Config Validation ─────────────────────────────────────────────


class TestCognitoConfig:
    def test_valid(self):
        config = AuthConfig(
            cognito=CognitoConfig(
                user_pool_id="us-east-1_Test",
                client_id="id",
                client_secret="secret",
                region="us-east-1",
            )
        )
        assert config.cognito is not None

    def test_mutual_exclusion_with_clerk(self):
        with pytest.raises(ValueError, match="Only one auth backend"):
            AuthConfig(
                cognito=CognitoConfig(
                    user_pool_id="pool",
                    client_id="id",
                    client_secret="secret",
                    region="us-east-1",
                ),
                clerk=ClerkConfig(secret_key="sk_test"),
            )

    def test_mutual_exclusion_with_native(self):
        with pytest.raises(ValueError, match="Only one auth backend"):
            AuthConfig(
                cognito=CognitoConfig(
                    user_pool_id="pool",
                    client_id="id",
                    client_secret="secret",
                    region="us-east-1",
                ),
                native=NativeAuthConfig(
                    jwt=JWTConfig(secret="s"),
                ),
            )
