"""Pydantic schemas for request/response models."""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


class SignUpRequest(BaseModel):
    email: str
    password: str = Field(min_length=8, max_length=128)


class SignInRequest(BaseModel):
    email: str
    password: str


class RefreshTokenRequest(BaseModel):
    refresh_token: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    expires_at: datetime


class AuthResponse(BaseModel):
    user: UserPublicResponse
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    expires_at: datetime


class MessageResponse(BaseModel):
    message: str


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------


class UserPublicResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    email: str
    username: str | None = None
    display_name: str | None = None
    avatar_url: str | None = None


class UserProfileUpdateRequest(BaseModel):
    username: str | None = Field(None, min_length=1, max_length=100)
    display_name: str | None = Field(None, min_length=1, max_length=255)


class AvatarUploadResponse(BaseModel):
    avatar_url: str


# ---------------------------------------------------------------------------
# Workspaces
# ---------------------------------------------------------------------------


class CreateWorkspaceRequest(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    slug: str = Field(min_length=1, max_length=255, pattern=r"^[a-z0-9-]+$")


class WorkspaceResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    slug: str
    created_at: datetime


class WorkspaceMemberResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    user_id: str
    role: str
    user: UserPublicResponse | None = None


class InviteMemberRequest(BaseModel):
    user_id: uuid.UUID
    role: str = "member"


# ---------------------------------------------------------------------------
# Channels
# ---------------------------------------------------------------------------


class CreateChannelRequest(BaseModel):
    name: str = Field(min_length=1, max_length=80, pattern=r"^[a-z0-9_-]+$")
    topic: str | None = Field(None, max_length=500)
    is_private: bool = False


class UpdateChannelRequest(BaseModel):
    name: str | None = Field(
        None, min_length=1, max_length=80, pattern=r"^[a-z0-9_-]+$"
    )
    topic: str | None = Field(None, max_length=500)


class ChannelResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    workspace_id: uuid.UUID
    name: str
    topic: str | None = None
    is_private: bool = False
    is_dm: bool = False
    member_count: int = 0
    last_message_at: datetime | None = None
    created_at: datetime


class ChannelMemberResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    user_id: uuid.UUID
    username: str | None = None
    display_name: str | None = None
    avatar_url: str | None = None
    joined_at: datetime


class StartDMRequest(BaseModel):
    user_id: uuid.UUID


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


class SendMessageRequest(BaseModel):
    content: str = Field(min_length=1, max_length=5000)


class EditMessageRequest(BaseModel):
    content: str = Field(min_length=1, max_length=5000)


class ChatMessageResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    channel_id: uuid.UUID
    sender_id: uuid.UUID
    sender_name: str = ""
    sender_avatar: str | None = None
    content: str
    created_at: datetime
    edited_at: datetime | None = None


# Fix forward reference
AuthResponse.model_rebuild()
