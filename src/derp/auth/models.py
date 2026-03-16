"""Database models for the auth module."""

from __future__ import annotations

import enum
import uuid
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict

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


@runtime_checkable
class AuthRequest(Protocol):
    """Protocol for objects that carry HTTP headers (e.g. FastAPI Request)."""

    @property
    def headers(self) -> Mapping[str, str]: ...


class UserInfo(BaseModel):
    """Unified user information returned by all auth backends."""

    id: str
    email: str
    first_name: str | None = None
    last_name: str | None = None
    username: str | None = None
    image_url: str | None = None
    role: str
    is_active: bool
    is_superuser: bool
    email_confirmed_at: datetime | None
    last_sign_in_at: datetime | None
    created_at: datetime
    updated_at: datetime
    metadata: dict[str, Any]

    model_config = ConfigDict(frozen=True)


class SessionInfo(BaseModel):
    """Unified session information returned by authenticate."""

    user_id: str
    session_id: str
    role: str
    expires_at: datetime
    metadata: dict[str, Any]
    org_id: str | None = None
    org_role: str | None = None

    model_config = ConfigDict(frozen=True)


class AuthUser(Table, table="users"):
    """User authentication table."""

    id: uuid.UUID = Field(UUID(), primary_key=True, default="gen_random_uuid()")
    email: str = Field(Varchar(255), unique=True, index=True)
    email_confirmed_at: datetime | None = Field(
        Timestamp(with_timezone=True), nullable=True
    )
    encrypted_password: str | None = Field(Text(), nullable=True)
    first_name: str | None = Field(Varchar(255), nullable=True)
    last_name: str | None = Field(Varchar(255), nullable=True)
    username: str | None = Field(Varchar(255), nullable=True)
    image_url: str | None = Field(Text(), nullable=True)
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
    org_id: uuid.UUID | None = Field(UUID(), nullable=True)
    not_after: datetime = Field(Timestamp(with_timezone=True))
    created_at: datetime = Field(Timestamp(with_timezone=True), default="now()")


class OrgInfo(BaseModel):
    """Unified organization information returned by all auth backends."""

    id: str
    name: str
    slug: str
    metadata: dict[str, Any]
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(frozen=True)


class OrgMemberInfo(BaseModel):
    """Unified organization membership information."""

    org_id: str
    user_id: str
    role: str
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(frozen=True)


class AuthOrganization(Table, table="organizations"):
    """Organization table for multi-tenancy."""

    id: uuid.UUID = Field(UUID(), primary_key=True, default="gen_random_uuid()")
    name: str = Field(Varchar(255))
    slug: str = Field(Varchar(255), unique=True, index=True)
    metadata: str | None = Field(Text(), nullable=True)
    created_at: datetime = Field(Timestamp(with_timezone=True), default="now()")
    updated_at: datetime = Field(Timestamp(with_timezone=True), default="now()")


class AuthOrgMember(Table, table="org_members"):
    """Organization membership table."""

    __indexes__ = [("org_id", "user_id")]

    id: uuid.UUID = Field(UUID(), primary_key=True, default="gen_random_uuid()")
    org_id: uuid.UUID = Field(
        UUID(),
        foreign_key=ForeignKey("organizations.id", on_delete=ForeignKeyAction.CASCADE),
        index=True,
    )
    user_id: uuid.UUID = Field(
        UUID(),
        foreign_key=ForeignKey("users.id", on_delete=ForeignKeyAction.CASCADE),
        index=True,
    )
    role: str = Field(Varchar(50), default="member")
    created_at: datetime = Field(Timestamp(with_timezone=True), default="now()")
    updated_at: datetime = Field(Timestamp(with_timezone=True), default="now()")
