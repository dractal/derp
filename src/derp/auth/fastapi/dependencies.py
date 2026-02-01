"""FastAPI dependencies for authentication."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, HTTPException, Request, status

from derp.auth.client import AuthClient
from derp.auth.exceptions import (
    InvalidTokenError,
    SessionExpiredError,
    SessionNotFoundError,
    TokenExpiredError,
)
from derp.auth.jwt import decode_token
from derp.auth.models import BaseUser


def get_auth_service(request: Request) -> AuthClient:
    """Get the auth service from request state.

    The auth service should be set up in app.state.auth_service during startup.
    """
    auth_service = getattr(request.app.state, "auth_service", None)
    if auth_service is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Auth service not configured",
        )
    return auth_service


async def get_current_user(
    request: Request,
    auth_client: Annotated[AuthClient, Depends(get_auth_service)],
) -> BaseUser:
    """Get the current authenticated user.

    Extracts user from JWT token in Authorization header.

    Raises:
        HTTPException 401: If not authenticated
        HTTPException 401: If token is invalid or expired
    """
    # Get token from header
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
        # Decode token
        payload = decode_token(auth_client._config.jwt, token)

        # Validate session
        session = await auth_client.validate_session(payload.session_id)
        if not session:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Session not found or expired",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Get user
        user = await auth_client.get_user(payload.sub)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Store session info in request state for later use
        request.state.session_id = payload.session_id
        request.state.token_payload = payload

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


async def get_current_user_optional(
    request: Request,
    auth_service: Annotated[AuthClient, Depends(get_auth_service)],
) -> BaseUser | None:
    """Get the current user if authenticated, None otherwise.

    Does not raise an error if not authenticated.
    """
    try:
        return await get_current_user(request, auth_service)
    except HTTPException:
        return None


async def require_active_user(
    user: Annotated[BaseUser, Depends(get_current_user)],
) -> BaseUser:
    """Require that the current user is active (not disabled).

    Raises:
        HTTPException 403: If user is disabled
    """
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is disabled",
        )
    return user


async def require_superuser(
    user: Annotated[BaseUser, Depends(require_active_user)],
) -> BaseUser:
    """Require that the current user is a superuser.

    Raises:
        HTTPException 403: If user is not a superuser
    """
    if not user.is_superuser:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Superuser access required",
        )
    return user


# Type aliases for convenience
CurrentUser = Annotated[BaseUser, Depends(get_current_user)]
CurrentUserOptional = Annotated[BaseUser | None, Depends(get_current_user_optional)]
ActiveUser = Annotated[BaseUser, Depends(require_active_user)]
Superuser = Annotated[BaseUser, Depends(require_superuser)]
