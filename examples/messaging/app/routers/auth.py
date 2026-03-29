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
from derp.auth.models import UserInfo

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post(
    "/signup", response_model=AuthResponse, status_code=status.HTTP_201_CREATED
)
async def signup(
    request: Request, data: SignUpRequest, derp: DerpClient = Depends(get_derp)
) -> AuthResponse:
    """Register a new user with email and password."""
    result = await derp.auth.sign_up(
        email=data.email,
        password=data.password,
        request=request,
    )
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Signup failed",
        )

    return AuthResponse(
        user=UserPublicResponse.model_validate(result.user),
        access_token=result.tokens.access_token,
        refresh_token=result.tokens.refresh_token,
        token_type=result.tokens.token_type,
        expires_in=result.tokens.expires_in,
        expires_at=result.tokens.expires_at,
    )


@router.post("/signin", response_model=AuthResponse)
async def signin(
    request: Request, data: SignInRequest, derp: DerpClient = Depends(get_derp)
) -> AuthResponse:
    """Sign in with email and password."""
    result = await derp.auth.sign_in_with_password(
        email=data.email,
        password=data.password,
        request=request,
    )
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    return AuthResponse(
        user=UserPublicResponse.model_validate(result.user),
        access_token=result.tokens.access_token,
        refresh_token=result.tokens.refresh_token,
        token_type=result.tokens.token_type,
        expires_in=result.tokens.expires_in,
        expires_at=result.tokens.expires_at,
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
    tokens = await derp.auth.refresh_token(data.refresh_token)
    if tokens is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token",
        )

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
