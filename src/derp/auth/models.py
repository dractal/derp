"""Database models for the auth module."""

from __future__ import annotations

import abc
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
    Serial,
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


class BaseUser(abc.ABC, Table):
    """User authentication table."""

    id: uuid.UUID = Field(UUID(), primary_key=True, default="gen_random_uuid()")
    email: str = Field(Varchar(255), unique=True, index=True)
    email_confirmed_at: datetime | None = Field(
        Timestamp(with_timezone=True), nullable=True
    )
    encrypted_password: str | None = Field(Text(), nullable=True)
    provider: AuthProvider = Field(Enum(AuthProvider))
    is_active: bool = Field(Boolean(), default=True)
    is_superuser: bool = Field(Boolean(), default=False)
    recovery_token: str | None = Field(Varchar(255), nullable=True)
    recovery_sent_at: datetime | None = Field(
        Timestamp(with_timezone=True), nullable=True
    )
    confirmation_token: str | None = Field(Varchar(255), nullable=True)
    confirmation_sent_at: datetime | None = Field(
        Timestamp(with_timezone=True), nullable=True
    )
    created_at: datetime = Field(Timestamp(with_timezone=True), default="now()")
    updated_at: datetime = Field(Timestamp(with_timezone=True), default="now()")
    last_sign_in_at: datetime | None = Field(
        Timestamp(with_timezone=True), nullable=True
    )


class AuthSession(Table):
    """Authentication session table."""

    id: uuid.UUID = Field(UUID(), primary_key=True, default="gen_random_uuid()")
    user_id: uuid.UUID = Field(
        UUID(),
        foreign_key=ForeignKey("users.id", on_delete=ForeignKeyAction.CASCADE),
        index=True,
    )
    user_agent: str | None = Field(Text(), nullable=True)
    ip_address: str | None = Field(Varchar(45), nullable=True)  # IPv6 compatible
    created_at: datetime = Field(Timestamp(with_timezone=True), default="now()")
    not_after: datetime = Field(Timestamp(with_timezone=True))


class AuthRefreshToken(Table):
    """Refresh token table for token rotation."""

    id: int = Field(Serial(), primary_key=True)
    session_id: uuid.UUID = Field(
        UUID(),
        foreign_key=ForeignKey("auth_sessions.id", on_delete=ForeignKeyAction.CASCADE),
        index=True,
    )
    token: str = Field(Varchar(255), unique=True, index=True)
    revoked: bool = Field(Boolean(), default=False)
    parent: str | None = Field(Varchar(255), nullable=True)  # For rotation detection
    created_at: datetime = Field(Timestamp(with_timezone=True), default="now()")


class AuthMagicLink(Table):
    """Magic link table for passwordless authentication."""

    id: uuid.UUID = Field(UUID(), primary_key=True, default="gen_random_uuid()")
    email: str = Field(Varchar(255), index=True)
    token: str = Field(Varchar(255), unique=True, index=True)
    used: bool = Field(Boolean(), default=False)
    expires_at: datetime = Field(Timestamp(with_timezone=True))
    created_at: datetime = Field(Timestamp(with_timezone=True), default="now()")
