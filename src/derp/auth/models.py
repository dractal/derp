"""Database models for the auth module."""

from __future__ import annotations

import enum
import uuid
from datetime import datetime

from derp.orm import Table
from derp.orm.fields import (
    UUID,
    Boolean,
    Enum,
    Field,
    ForeignKey,
    ForeignKeyAction,
    Text,
    Timestamp,
    Varchar,
)


class AuthProvider(enum.StrEnum):
    """Authentication provider types."""

    EMAIL = "email"
    MAGIC_LINK = "magic_link"
    GOOGLE = "google"
    GITHUB = "github"


class AuthUser(Table, table="users"):
    """User authentication table."""

    id: uuid.UUID = Field(UUID(), primary_key=True, default="gen_random_uuid()")
    email: str = Field(Varchar(255), unique=True, index=True)
    email_confirmed_at: datetime | None = Field(
        Timestamp(with_timezone=True), nullable=True
    )
    encrypted_password: str | None = Field(Text(), nullable=True)
    provider: AuthProvider = Field(Enum(AuthProvider))
    provider_id: str | None = Field(Varchar(255), nullable=True)
    is_active: bool = Field(Boolean(), default=True)
    is_superuser: bool = Field(Boolean(), default=False)
    role: str = Field(Varchar(50), default="default")
    created_at: datetime = Field(Timestamp(with_timezone=True), default="now()")
    updated_at: datetime = Field(Timestamp(with_timezone=True), default="now()")
    last_sign_in_at: datetime | None = Field(
        Timestamp(with_timezone=True), nullable=True
    )


class AuthSession(Table, table="auth_sessions"):
    """Authentication session table with integrated refresh tokens.

    Each row represents a refresh token. Rows sharing the same ``session_id``
    belong to the same logical session (one login event). Token rotation
    inserts a new row and revokes the old one.
    """

    __indexes__ = [("session_id", "revoked")]

    id: uuid.UUID = Field(UUID(), primary_key=True, default="gen_random_uuid()")
    user_id: uuid.UUID = Field(
        UUID(),
        foreign_key=ForeignKey("users.id", on_delete=ForeignKeyAction.CASCADE),
        index=True,
    )
    session_id: uuid.UUID = Field(UUID(), index=True, default="gen_random_uuid()")
    token: str = Field(Varchar(255), unique=True, index=True)
    role: str = Field(Varchar(50), default="default")
    revoked: bool = Field(Boolean(), default=False)
    user_agent: str | None = Field(Text(), nullable=True)
    ip_address: str | None = Field(Varchar(45), nullable=True)  # IPv6 compatible
    not_after: datetime = Field(Timestamp(with_timezone=True))
    created_at: datetime = Field(Timestamp(with_timezone=True), default="now()")
