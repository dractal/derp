"""FastAPI dependencies."""

from __future__ import annotations

from fastapi import Depends, HTTPException, Request, status

from app.models import User
from derp import DerpClient
from derp.auth.exceptions import (
    InvalidTokenError,
    SessionExpiredError,
    SessionNotFoundError,
    TokenExpiredError,
)
from derp.auth.jwt import decode_token


def get_derp(request: Request) -> DerpClient[User]:
    """Get DerpClient from app state."""
    return request.app.state.derp_client


async def get_current_user(
    request: Request, derp: DerpClient[User] = Depends(get_derp)
) -> User:
    """Get the current authenticated user from JWT token.

    Extracts the token from the Authorization header, validates it,
    and returns the user.
    """
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    parts = auth_header.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authorization header format",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = parts[1]

    try:
        payload = decode_token(derp.auth._config.jwt, token)
        await derp.auth.validate_session(token)  # Pass token, not session_id
        user = await derp.auth.get_user(payload.sub)

        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found",
                headers={"WWW-Authenticate": "Bearer"},
            )

        request.state.session_id = payload.session_id
        return user

    except TokenExpiredError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        ) from None
    except InvalidTokenError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"},
        ) from None
    except (SessionNotFoundError, SessionExpiredError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session not found or expired",
            headers={"WWW-Authenticate": "Bearer"},
        ) from None
