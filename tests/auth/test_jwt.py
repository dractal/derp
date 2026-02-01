"""Tests for JWT token creation and validation."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

import pytest

from derp.auth.config import JWTConfig
from derp.auth.exceptions import InvalidTokenError, TokenExpiredError
from derp.auth.jwt import (
    TokenPair,
    create_access_token,
    create_token_pair,
    decode_token,
)


class TestJWT:
    """Tests for JWT manager."""

    def test_create_access_token(self, jwt_config: JWTConfig) -> None:
        """Test creating an access token."""
        user_id = uuid.uuid4()
        session_id = uuid.uuid4()

        token = create_access_token(jwt_config, user_id, session_id)

        assert token is not None
        assert isinstance(token, str)
        assert len(token) > 0

    def test_decode_valid_token(self, jwt_config: JWTConfig) -> None:
        """Test decoding a valid token."""
        user_id = uuid.uuid4()
        session_id = uuid.uuid4()

        token = create_access_token(jwt_config, user_id, session_id)
        payload = decode_token(jwt_config, token)

        assert payload.sub == str(user_id)
        assert payload.session_id == str(session_id)
        assert payload.exp > datetime.now(UTC)
        assert payload.iat <= datetime.now(UTC)

    def test_decode_with_extra_claims(self, jwt_config: JWTConfig) -> None:
        """Test decoding token with extra claims."""
        user_id = uuid.uuid4()
        session_id = uuid.uuid4()
        extra = {"role": "admin", "permissions": ["read", "write"]}

        token = create_access_token(jwt_config, user_id, session_id, extra_claims=extra)
        payload = decode_token(jwt_config, token)

        assert payload.sub == str(user_id)
        assert payload.session_id == str(session_id)
        assert payload.exp > datetime.now(UTC)
        assert payload.iat <= datetime.now(UTC)
        assert payload.extra is not None
        assert payload.extra["role"] == "admin"
        assert payload.extra["permissions"] == ["read", "write"]

    def test_decode_expired_token(self, jwt_config: JWTConfig) -> None:
        """Test decoding an expired token."""
        config = JWTConfig(
            secret=jwt_config.secret,
            access_token_expire_minutes=-1,  # Already expired
        )
        user_id = uuid.uuid4()
        session_id = uuid.uuid4()

        token = create_access_token(config, user_id, session_id)

        with pytest.raises(TokenExpiredError):
            decode_token(config, token)

    def test_decode_invalid_token(self, jwt_config: JWTConfig) -> None:
        """Test decoding an invalid token."""
        with pytest.raises(InvalidTokenError):
            decode_token(jwt_config, "invalid.token.here")

    def test_decode_tampered_token(self, jwt_config: JWTConfig) -> None:
        """Test decoding a tampered token."""
        user_id = uuid.uuid4()
        session_id = uuid.uuid4()

        token = create_access_token(jwt_config, user_id, session_id)
        # Tamper with the token
        parts = token.split(".")
        parts[1] = parts[1][:-5] + "XXXXX"  # Modify payload
        tampered = ".".join(parts)

        with pytest.raises(InvalidTokenError):
            decode_token(jwt_config, tampered)

    def test_decode_wrong_secret(self, jwt_config: JWTConfig) -> None:
        """Test decoding with wrong secret."""
        user_id = uuid.uuid4()
        session_id = uuid.uuid4()

        token = create_access_token(jwt_config, user_id, session_id)

        config2 = JWTConfig(secret="different-secret-padded-to-16-bytes")
        with pytest.raises(InvalidTokenError):
            decode_token(config2, token)

    def test_create_token_pair(self, jwt_config: JWTConfig) -> None:
        """Test creating a token pair."""
        user_id = uuid.uuid4()
        session_id = uuid.uuid4()
        refresh_token = "test_refresh_token"

        pair = create_token_pair(jwt_config, user_id, session_id, refresh_token)

        assert isinstance(pair, TokenPair)
        assert pair.access_token is not None
        assert pair.refresh_token == refresh_token
        assert pair.token_type == "bearer"
        assert pair.expires_in == 15 * 60  # 15 minutes in seconds
        assert pair.expires_at is not None

    def test_issuer_and_audience(self, jwt_config: JWTConfig) -> None:
        """Test tokens with issuer and audience."""
        config = JWTConfig(
            secret=jwt_config.secret,
            issuer="test-issuer",
            audience="test-audience",
        )
        user_id = uuid.uuid4()
        session_id = uuid.uuid4()

        token = create_access_token(config, user_id, session_id)
        payload = decode_token(config, token)

        assert payload.iss == "test-issuer"
        assert payload.aud == "test-audience"

        os.environ.pop("TEST_JWT_SECRET", None)
