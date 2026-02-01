"""Pre-built FastAPI router for authentication endpoints."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from derp.auth.client import AuthClient
from derp.auth.exceptions import (
    AuthError,
    ConfirmationTokenExpiredError,
    ConfirmationTokenInvalidError,
    EmailNotConfirmedError,
    InvalidCredentialsError,
    MagicLinkExpiredError,
    MagicLinkUsedError,
    OAuthError,
    PasswordValidationError,
    RecoveryTokenExpiredError,
    RecoveryTokenInvalidError,
    RefreshTokenReusedError,
    RefreshTokenRevokedError,
    SessionExpiredError,
    SignupDisabledError,
    UserAlreadyExistsError,
    UserNotActiveError,
)
from derp.auth.fastapi.dependencies import (
    CurrentUser,
    get_auth_service,
)
from derp.auth.fastapi.schemas import (
    AuthResponse,
    ConfirmEmailRequest,
    MagicLinkRequest,
    MessageResponse,
    OAuthStartResponse,
    PasswordRecoveryRequest,
    PasswordResetRequest,
    RefreshTokenRequest,
    SignInRequest,
    SignUpRequest,
    TokenResponse,
    UserResponse,
    UserUpdateRequest,
)
from derp.auth.password import generate_secure_token


def _get_client_info(request: Request) -> tuple[str | None, str | None]:
    """Extract user agent and IP address from request."""
    user_agent = request.headers.get("User-Agent")
    # Handle X-Forwarded-For for proxied requests
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        ip_address = forwarded_for.split(",")[0].strip()
    else:
        ip_address = request.client.host if request.client else None
    return user_agent, ip_address


def create_auth_router(
    prefix: str = "/auth", tags: Sequence[str] = ("auth",)
) -> APIRouter:
    """Create a FastAPI router with all auth endpoints.

    Args:
        prefix: URL prefix for all routes (default: "/auth")
        tags: OpenAPI tags for the routes

    Returns:
        Configured APIRouter

    Usage:
        app.include_router(create_auth_router())
    """
    router = APIRouter(prefix=prefix, tags=list(tags))

    @router.post("/signup", response_model=AuthResponse)
    async def signup(
        request: Request,
        data: SignUpRequest,
        auth_service: Annotated[AuthClient, Depends(get_auth_service)],
    ) -> AuthResponse:
        """Register a new user with email and password."""
        user_agent, ip_address = _get_client_info(request)

        try:
            user, tokens = await auth_service.sign_up(
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
            user=UserResponse.model_validate(user),
            access_token=tokens.access_token,
            refresh_token=tokens.refresh_token,
            token_type=tokens.token_type,
            expires_in=tokens.expires_in,
            expires_at=tokens.expires_at,
        )

    @router.post("/signin", response_model=AuthResponse)
    async def signin(
        request: Request,
        data: SignInRequest,
        auth_service: Annotated[AuthClient, Depends(get_auth_service)],
    ) -> AuthResponse:
        """Sign in with email and password."""
        user_agent, ip_address = _get_client_info(request)

        try:
            user, tokens = await auth_service.sign_in_with_password(
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
            user=UserResponse.model_validate(user),
            access_token=tokens.access_token,
            refresh_token=tokens.refresh_token,
            token_type=tokens.token_type,
            expires_in=tokens.expires_in,
            expires_at=tokens.expires_at,
        )

    @router.post("/signout", response_model=MessageResponse)
    async def signout(
        request: Request,
        user: CurrentUser,
        auth_service: Annotated[AuthClient, Depends(get_auth_service)],
    ) -> MessageResponse:
        """Sign out the current session."""
        session_id = getattr(request.state, "session_id", None)
        if session_id:
            await auth_service.sign_out(session_id)
        return MessageResponse(message="Successfully signed out")

    @router.post("/refresh", response_model=TokenResponse)
    async def refresh(
        data: RefreshTokenRequest,
        auth_service: Annotated[AuthClient, Depends(get_auth_service)],
    ) -> TokenResponse:
        """Refresh access token using refresh token."""
        try:
            tokens = await auth_service.refresh_token(data.refresh_token)
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

    @router.get("/user", response_model=UserResponse)
    async def get_user(user: CurrentUser) -> UserResponse:
        """Get the current authenticated user."""
        return UserResponse.model_validate(user)

    @router.patch("/user", response_model=UserResponse)
    async def update_user(
        data: UserUpdateRequest,
        user: CurrentUser,
        auth_service: Annotated[AuthClient, Depends(get_auth_service)],
    ) -> UserResponse:
        """Update the current user's data."""
        updated_user = await auth_service.update_user(user_id=user.id, email=data.email)
        return UserResponse.model_validate(updated_user)

    @router.post("/recovery", response_model=MessageResponse)
    async def request_recovery(
        data: PasswordRecoveryRequest,
        auth_service: Annotated[AuthClient, Depends(get_auth_service)],
    ) -> MessageResponse:
        """Request a password recovery email."""
        await auth_service.request_password_recovery(data.email)
        # Always return success to not reveal user existence
        return MessageResponse(
            message="If an account exists, a recovery email has been sent"
        )

    @router.post("/recovery/reset", response_model=MessageResponse)
    async def reset_password(
        data: PasswordResetRequest,
        auth_service: Annotated[AuthClient, Depends(get_auth_service)],
    ) -> MessageResponse:
        """Reset password using recovery token."""
        try:
            await auth_service.reset_password(data.token, data.password)
        except RecoveryTokenInvalidError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid recovery token",
            ) from None
        except RecoveryTokenExpiredError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Recovery token has expired",
            ) from None
        except PasswordValidationError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=e.message,
            ) from None

        return MessageResponse(message="Password has been reset successfully")

    @router.post("/confirm", response_model=UserResponse)
    async def confirm_email(
        data: ConfirmEmailRequest,
        auth_service: Annotated[AuthClient, Depends(get_auth_service)],
    ) -> UserResponse:
        """Confirm email address with token."""
        try:
            user = await auth_service.confirm_email(data.token)
        except ConfirmationTokenInvalidError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid confirmation token",
            ) from None
        except ConfirmationTokenExpiredError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Confirmation token has expired",
            ) from None

        return UserResponse.model_validate(user)

    @router.post("/magic-link", response_model=MessageResponse)
    async def request_magic_link(
        data: MagicLinkRequest,
        auth_service: Annotated[AuthClient, Depends(get_auth_service)],
    ) -> MessageResponse:
        """Request a magic link for passwordless sign in."""
        try:
            await auth_service.sign_in_with_magic_link(data.email)
        except SignupDisabledError:
            # Don't reveal user doesn't exist
            pass
        except AuthError:
            pass

        return MessageResponse(
            message="If the email is valid, a magic link has been sent"
        )

    @router.get("/magic-link/verify", response_model=AuthResponse)
    async def verify_magic_link(
        request: Request,
        token: Annotated[str, Query()],
        auth_service: Annotated[AuthClient, Depends(get_auth_service)],
    ) -> AuthResponse:
        """Verify a magic link and sign in."""
        user_agent, ip_address = _get_client_info(request)

        try:
            user, tokens = await auth_service.verify_magic_link(
                token,
                user_agent=user_agent,
                ip_address=ip_address,
            )
        except MagicLinkUsedError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Magic link has already been used",
            ) from None
        except MagicLinkExpiredError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Magic link has expired or is invalid",
            ) from None
        except UserNotActiveError:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User account is disabled",
            ) from None

        return AuthResponse(
            user=UserResponse.model_validate(user),
            access_token=tokens.access_token,
            refresh_token=tokens.refresh_token,
            token_type=tokens.token_type,
            expires_in=tokens.expires_in,
            expires_at=tokens.expires_at,
        )

    @router.get("/oauth/{provider}", response_model=OAuthStartResponse)
    async def start_oauth(
        provider: str,
        auth_service: Annotated[AuthClient, Depends(get_auth_service)],
        redirect_uri: Annotated[str | None, Query()] = None,
    ) -> OAuthStartResponse:
        """Start OAuth flow by getting the authorization URL."""
        oauth_provider = auth_service.get_oauth_provider(provider)
        if not oauth_provider:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unknown OAuth provider: {provider}",
            )

        state = generate_secure_token(32)
        # In production, store state in session or cache for verification
        authorization_url = auth_service.get_oauth_authorization_url(
            provider, state, redirect_uri=redirect_uri
        )

        return OAuthStartResponse(
            authorization_url=authorization_url,
            state=state,
        )

    @router.get("/oauth/{provider}/callback", response_model=AuthResponse)
    async def oauth_callback(
        request: Request,
        provider: str,
        code: Annotated[str, Query()],
        auth_service: Annotated[AuthClient, Depends(get_auth_service)],
        state: Annotated[str | None, Query()] = None,
        redirect_uri: Annotated[str | None, Query()] = None,
    ) -> AuthResponse:
        """Handle OAuth callback and complete sign in."""
        # In production, verify state matches stored value
        user_agent, ip_address = _get_client_info(request)

        try:
            user, tokens = await auth_service.sign_in_with_oauth(
                provider,
                code,
                redirect_uri=redirect_uri,
                user_agent=user_agent,
                ip_address=ip_address,
            )
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(e),
            ) from None
        except OAuthError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=e.message,
            ) from None
        except UserNotActiveError:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User account is disabled",
            ) from None

        return AuthResponse(
            user=UserResponse.model_validate(user),
            access_token=tokens.access_token,
            refresh_token=tokens.refresh_token,
            token_type=tokens.token_type,
            expires_in=tokens.expires_in,
            expires_at=tokens.expires_at,
        )

    return router
