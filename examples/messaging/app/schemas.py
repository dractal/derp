"""Pydantic schemas for request/response models."""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


# Auth schemas
class SignUpRequest(BaseModel):
    """Request to register a new user."""

    email: str
    password: str = Field(min_length=8, max_length=128)


class SignInRequest(BaseModel):
    """Request to sign in."""

    email: str
    password: str


class RefreshTokenRequest(BaseModel):
    """Request to refresh access token."""

    refresh_token: str


class TokenResponse(BaseModel):
    """Token response."""

    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    expires_at: datetime


class AuthResponse(BaseModel):
    """Response after authentication."""

    user: UserPublicResponse
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    expires_at: datetime


class MessageResponse(BaseModel):
    """Generic message response."""

    message: str


# User schemas
class UserPublicResponse(BaseModel):
    """Public user information."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    email: str
    username: str | None = None
    avatar_url: str | None = None
    bio: str | None = None


class UserProfileUpdateRequest(BaseModel):
    """Request to update user profile."""

    username: str | None = Field(None, min_length=1, max_length=100)
    bio: str | None = Field(None, max_length=500)


class AvatarUploadResponse(BaseModel):
    """Response after avatar upload."""

    avatar_url: str


# Conversation schemas
class StartConversationRequest(BaseModel):
    """Request to start a conversation."""

    user_id: uuid.UUID


class ConversationResponse(BaseModel):
    """Conversation with the other user."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    other_user: UserPublicResponse
    last_message_at: datetime | None = None
    created_at: datetime
    unread_count: int = 0


class ConversationDetailResponse(BaseModel):
    """Conversation with messages."""

    id: uuid.UUID
    other_user: UserPublicResponse
    messages: list[ChatMessageResponse]
    created_at: datetime


# Message schemas
class ChatMessageResponse(BaseModel):
    """Message response."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    sender_id: uuid.UUID
    content: str
    read_at: datetime | None = None
    created_at: datetime
    is_mine: bool = False


class SendMessageRequest(BaseModel):
    """Request to send a message."""

    content: str = Field(min_length=1, max_length=5000)


class MarkReadResponse(BaseModel):
    """Response after marking messages as read."""

    marked_count: int


# Fix forward reference
AuthResponse.model_rebuild()
