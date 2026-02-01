"""FastAPI integration for Derp Auth."""

from __future__ import annotations

from derp.auth.fastapi.dependencies import (
    ActiveUser,
    CurrentUser,
    CurrentUserOptional,
    Superuser,
    get_auth_service,
    get_current_user,
    get_current_user_optional,
    require_active_user,
    require_superuser,
)
from derp.auth.fastapi.middleware import JWTMiddleware
from derp.auth.fastapi.router import create_auth_router
from derp.auth.fastapi.schemas import (
    ConfirmEmailRequest,
    MagicLinkRequest,
    PasswordRecoveryRequest,
    PasswordResetRequest,
    RefreshTokenRequest,
    SignInRequest,
    SignUpRequest,
    TokenResponse,
    UserResponse,
    UserUpdateRequest,
)

__all__ = [
    # Dependencies
    "ActiveUser",
    "CurrentUser",
    "CurrentUserOptional",
    "Superuser",
    "get_auth_service",
    "get_current_user",
    "get_current_user_optional",
    "require_active_user",
    "require_superuser",
    # Middleware
    "JWTMiddleware",
    # Router
    "create_auth_router",
    # Schemas
    "ConfirmEmailRequest",
    "MagicLinkRequest",
    "PasswordRecoveryRequest",
    "PasswordResetRequest",
    "RefreshTokenRequest",
    "SignInRequest",
    "SignUpRequest",
    "TokenResponse",
    "UserResponse",
    "UserUpdateRequest",
]
