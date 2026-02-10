"""JWT token creation and validation."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import jwt

from derp.auth.exceptions import InvalidTokenError, TokenExpiredError
from derp.config import JWTConfig


@dataclass(kw_only=True)
class TokenPayload:
    """Payload data from a decoded JWT token."""

    sub: str  # User ID
    session_id: str
    exp: datetime
    iat: datetime
    iss: str | None = None
    aud: str | None = None
    extra: dict[str, Any] | None = None


@dataclass(kw_only=True)
class TokenPair:
    """Access and refresh token pair."""

    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int = 0  # Access token expiry in seconds
    expires_at: datetime


def create_access_token(
    config: JWTConfig,
    user_id: str | uuid.UUID,
    session_id: str | uuid.UUID,
    extra_claims: dict[str, Any] | None = None,
) -> str:
    """Create a short-lived JWT access token.

    Args:
        user_id: The user's unique identifier
        session_id: The session's unique identifier
        extra_claims: Additional claims to include in the token

    Returns:
        Encoded JWT token string
    """
    now = datetime.now(UTC)
    expires = now + timedelta(minutes=config.access_token_expire_minutes)

    payload: dict[str, Any] = {
        "sub": str(user_id),
        "session_id": str(session_id),
        "iat": now,
        "exp": expires,
    }

    if config.issuer:
        payload["iss"] = config.issuer

    if config.audience:
        payload["aud"] = config.audience

    if extra_claims:
        payload.update(extra_claims)

    return jwt.encode(
        payload,
        config.secret,
        algorithm=config.algorithm,
    )


def decode_token(config: JWTConfig, token: str) -> TokenPayload:
    """Decode and validate a JWT token.

    Args:
        token: The JWT token string

    Returns:
        TokenPayload with decoded data

    Raises:
        TokenExpiredError: If the token has expired
        InvalidTokenError: If the token is invalid
    """
    try:
        payload = jwt.decode(
            token,
            config.secret,
            algorithms=[config.algorithm],
            audience=config.audience,
            issuer=config.issuer,
            options={"require": ["aud"]} if config.audience else None,
        )

        # Extract known fields
        sub = payload.get("sub")
        session_id = payload.get("session_id")
        exp = payload.get("exp")
        iat = payload.get("iat")

        if not sub or not session_id:
            raise InvalidTokenError("Token missing required claims")

        # Convert timestamps
        exp_dt = datetime.fromtimestamp(exp, tz=UTC) if exp else datetime.now(UTC)
        iat_dt = datetime.fromtimestamp(iat, tz=UTC) if iat else datetime.now(UTC)

        # Extract extra claims
        known_keys = {"sub", "session_id", "exp", "iat", "iss", "aud"}
        extra = {k: v for k, v in payload.items() if k not in known_keys}
        iss = payload.get("iss")
        aud = payload.get("aud")
    except jwt.ExpiredSignatureError as e:
        raise TokenExpiredError() from e
    except jwt.InvalidTokenError as e:
        raise InvalidTokenError(str(e)) from e

    return TokenPayload(
        sub=sub,
        session_id=session_id,
        exp=exp_dt,
        iat=iat_dt,
        iss=iss,
        aud=aud,
        extra=extra if extra else None,
    )


def create_token_pair(
    config: JWTConfig,
    user_id: str | uuid.UUID,
    session_id: str | uuid.UUID,
    refresh_token: str,
    extra_claims: dict[str, Any] | None = None,
) -> TokenPair:
    """Create a token pair with access token and refresh token.

    Args:
        user_id: The user's unique identifier
        session_id: The session's unique identifier
        refresh_token: The refresh token string (created separately)
        extra_claims: Additional claims for the access token

    Returns:
        TokenPair with both tokens
    """
    access_token = create_access_token(config, user_id, session_id, extra_claims)
    expires_in = config.access_token_expire_minutes * 60
    expires_at = datetime.now(UTC) + timedelta(
        minutes=config.access_token_expire_minutes
    )

    return TokenPair(
        access_token=access_token,
        refresh_token=refresh_token,
        token_type="bearer",
        expires_in=expires_in,
        expires_at=expires_at,
    )
