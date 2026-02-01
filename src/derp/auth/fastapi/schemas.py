"""Pydantic schemas for FastAPI auth endpoints."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, EmailStr, Field


class SignUpRequest(BaseModel):
    """Request schema for user signup."""

    email: EmailStr
    password: str = Field(min_length=1)


class SignInRequest(BaseModel):
    """Request schema for password sign in."""

    email: EmailStr
    password: str


class MagicLinkRequest(BaseModel):
    """Request schema for magic link sign in."""

    email: EmailStr


class MagicLinkVerifyRequest(BaseModel):
    """Request schema for magic link verification."""

    token: str


class RefreshTokenRequest(BaseModel):
    """Request schema for token refresh."""

    refresh_token: str


class PasswordRecoveryRequest(BaseModel):
    """Request schema for password recovery."""

    email: EmailStr


class PasswordResetRequest(BaseModel):
    """Request schema for password reset."""

    token: str
    password: str = Field(min_length=1)


class ConfirmEmailRequest(BaseModel):
    """Request schema for email confirmation."""

    token: str


class UserUpdateRequest(BaseModel):
    """Request schema for user update."""

    email: EmailStr | None = None


class TokenResponse(BaseModel):
    """Response schema for authentication tokens."""

    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    expires_at: datetime | None = None


class UserResponse(BaseModel):
    """Response schema for user data."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    email: str
    email_confirmed_at: datetime | None = None
    provider: str
    is_active: bool
    is_superuser: bool
    raw_user_meta_data: dict[str, Any] | None = None
    created_at: datetime
    updated_at: datetime
    last_sign_in_at: datetime | None = None


class AuthResponse(BaseModel):
    """Response schema for authentication success."""

    user: UserResponse
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    expires_at: datetime | None = None


class MessageResponse(BaseModel):
    """Simple message response."""

    message: str


class ErrorResponse(BaseModel):
    """Error response schema."""

    error: str
    code: str
    detail: str | None = None


class OAuthStartResponse(BaseModel):
    """Response for starting OAuth flow."""

    authorization_url: str
    state: str
