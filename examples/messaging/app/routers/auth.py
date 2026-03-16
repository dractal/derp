"""Authentication endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.dependencies import get_current_user, get_derp
from app.schemas import (
    AuthResponse,
    MessageResponse,
    RefreshTokenRequest,
    SignInRequest,
    SignUpRequest,
    TokenResponse,
    UserPublicResponse,
)
from derp import DerpClient
from derp.auth.exceptions import (
    EmailNotConfirmedError,
    InvalidCredentialsError,
    PasswordValidationError,
    RefreshTokenReusedError,
    RefreshTokenRevokedError,
    SessionExpiredError,
    SignupDisabledError,
    UserAlreadyExistsError,
    UserNotActiveError,
)
from derp.auth.models import UserInfo

router = APIRouter(prefix="/auth", tags=["auth"])


def _get_client_info(request: Request) -> tuple[str | None, str | None]:
    """Extract user agent and IP address from request."""
    user_agent = request.headers.get("User-Agent")
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        ip_address = forwarded_for.split(",")[0].strip()
    else:
        ip_address = request.client.host if request.client else None
    return user_agent, ip_address


@router.post(
    "/signup", response_model=AuthResponse, status_code=status.HTTP_201_CREATED
)
async def signup(
    request: Request, data: SignUpRequest, derp: DerpClient = Depends(get_derp)
) -> AuthResponse:
    """Register a new user with email and password."""
    user_agent, ip_address = _get_client_info(request)

    try:
        user, tokens = await derp.auth.sign_up(
            email=data.email,
            password=data.password,
            user_agent=user_agent,
            ip_address=ip_address,
        )
    except SignupDisabledError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Signup is currently disabled",
        ) from None
    except UserAlreadyExistsError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User with this email already exists",
        ) from None
    except PasswordValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=e.message,
        ) from None

    return AuthResponse(
        user=UserPublicResponse.model_validate(user),
        access_token=tokens.access_token,
        refresh_token=tokens.refresh_token,
        token_type=tokens.token_type,
        expires_in=tokens.expires_in,
        expires_at=tokens.expires_at,
    )


@router.post("/signin", response_model=AuthResponse)
async def signin(
    request: Request, data: SignInRequest, derp: DerpClient = Depends(get_derp)
) -> AuthResponse:
    """Sign in with email and password."""
    user_agent, ip_address = _get_client_info(request)

    try:
        user, tokens = await derp.auth.sign_in_with_password(
            email=data.email,
            password=data.password,
            user_agent=user_agent,
            ip_address=ip_address,
        )
    except InvalidCredentialsError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        ) from None
    except UserNotActiveError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is disabled",
        ) from None
    except EmailNotConfirmedError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Email address has not been confirmed",
        ) from None

    return AuthResponse(
        user=UserPublicResponse.model_validate(user),
        access_token=tokens.access_token,
        refresh_token=tokens.refresh_token,
        token_type=tokens.token_type,
        expires_in=tokens.expires_in,
        expires_at=tokens.expires_at,
    )


@router.post("/signout", response_model=MessageResponse)
async def signout(
    request: Request,
    user: UserInfo = Depends(get_current_user),
    derp: DerpClient = Depends(get_derp),
) -> MessageResponse:
    """Sign out the current session."""
    session_id = getattr(request.state, "session_id", None)
    if session_id:
        await derp.auth.sign_out(session_id)
    return MessageResponse(message="Successfully signed out")


@router.post("/refresh", response_model=TokenResponse)
async def refresh(
    data: RefreshTokenRequest, derp: DerpClient = Depends(get_derp)
) -> TokenResponse:
    """Refresh access token using refresh token."""
    try:
        tokens = await derp.auth.refresh_token(data.refresh_token)
    except (RefreshTokenRevokedError, RefreshTokenReusedError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or revoked refresh token",
        ) from None
    except SessionExpiredError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session has expired",
        ) from None

    return TokenResponse(
        access_token=tokens.access_token,
        refresh_token=tokens.refresh_token,
        token_type=tokens.token_type,
        expires_in=tokens.expires_in,
        expires_at=tokens.expires_at,
    )


@router.get("/user", response_model=UserPublicResponse)
async def get_current_user_info(
    user: UserInfo = Depends(get_current_user),
) -> UserPublicResponse:
    """Get the current authenticated user."""
    return UserPublicResponse.model_validate(user)
