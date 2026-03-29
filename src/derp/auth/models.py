"""Database models for the auth module."""

from __future__ import annotations

import enum
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict

from derp.auth.jwt import TokenPair
from derp.orm import (
    FK,
    JSONB,
    UUID,
    Boolean,
    Enum,
    Field,
    Fn,
    Index,
    L,
    Nullable,
    Text,
    TimestampTZ,
    Varchar,
)
from derp.orm.table import Table


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


@dataclass(frozen=True, kw_only=True)
class AuthResult:
    """Result of a sign-up or sign-in operation."""

    user: UserInfo
    tokens: TokenPair


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

    id: UUID = Field(primary=True, default=Fn.gen_random_uuid())
    email: Varchar[L[255]] = Field(unique=True)
    email_confirmed_at: Nullable[TimestampTZ] = Field()
    encrypted_password: Nullable[Text] = Field()
    first_name: Nullable[Varchar[L[255]]] = Field()
    last_name: Nullable[Varchar[L[255]]] = Field()
    username: Nullable[Varchar[L[255]]] = Field()
    image_url: Nullable[Text] = Field()
    provider: Enum[AuthProvider] = Field()
    provider_id: Nullable[Varchar[L[255]]] = Field()
    is_active: Boolean = Field(default=True)
    is_superuser: Boolean = Field(default=False)
    role: Varchar[L[50]] = Field(default="default")
    created_at: TimestampTZ = Field(default=Fn.now())
    updated_at: TimestampTZ = Field(default=Fn.now())
    last_sign_in_at: Nullable[TimestampTZ] = Field()

    @classmethod
    def indexes(cls) -> list[Index]:
        return [Index(cls.email)]


class AuthSession(Table, table="auth_sessions"):
    """Authentication session table with integrated refresh tokens.

    Each row represents a refresh token. Rows sharing the same ``session_id``
    belong to the same logical session (one login event). Token rotation
    inserts a new row and revokes the old one.
    """

    id: UUID = Field(primary=True, default=Fn.gen_random_uuid())
    user_id: UUID = Field(foreign_key=AuthUser.id, on_delete=FK.CASCADE)
    session_id: UUID = Field(default=Fn.gen_random_uuid())
    token: Varchar[L[255]] = Field(unique=True)
    role: Varchar[L[50]] = Field(default="default")
    revoked: Boolean = Field(default=False)
    user_agent: Nullable[Text] = Field()
    ip_address: Nullable[Varchar[L[45]]] = Field()  # IPv6 compatible
    org_id: Nullable[UUID] = Field()
    not_after: TimestampTZ = Field()
    created_at: TimestampTZ = Field(default=Fn.now())

    @classmethod
    def indexes(cls) -> list[Index]:
        return [
            Index(cls.session_id, cls.revoked),
            Index(cls.user_id),
            Index(cls.session_id),
            Index(cls.token),
        ]


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

    id: UUID = Field(primary=True, default=Fn.gen_random_uuid())
    name: Varchar[L[255]] = Field()
    slug: Varchar[L[255]] = Field(unique=True)
    metadata: Nullable[JSONB] = Field()
    created_at: TimestampTZ = Field(default=Fn.now())
    updated_at: TimestampTZ = Field(default=Fn.now())

    @classmethod
    def indexes(cls) -> list[Index]:
        return [Index(cls.slug)]


class AuthOrgMember(Table, table="org_members"):
    """Organization membership table (native auth — FK to AuthUser)."""

    id: UUID = Field(primary=True, default=Fn.gen_random_uuid())
    org_id: UUID = Field(foreign_key=AuthOrganization.id, on_delete=FK.CASCADE)
    user_id: UUID = Field(foreign_key=AuthUser.id, on_delete=FK.CASCADE)
    role: Varchar[L[50]] = Field(default="member")
    created_at: TimestampTZ = Field(default=Fn.now())
    updated_at: TimestampTZ = Field(default=Fn.now())

    @classmethod
    def indexes(cls) -> list[Index]:
        return [
            Index(cls.org_id, cls.user_id, unique=True),
            Index(cls.org_id),
            Index(cls.user_id),
        ]


class CognitoOrgMember(Table, table="org_members"):
    """Organization membership table (Cognito — no FK to users table)."""

    id: UUID = Field(primary=True, default=Fn.gen_random_uuid())
    org_id: UUID = Field(foreign_key=AuthOrganization.id, on_delete=FK.CASCADE)
    user_id: UUID = Field()
    role: Varchar[L[50]] = Field(default="member")
    created_at: TimestampTZ = Field(default=Fn.now())
    updated_at: TimestampTZ = Field(default=Fn.now())

    @classmethod
    def indexes(cls) -> list[Index]:
        return [
            Index(cls.org_id, cls.user_id, unique=True),
            Index(cls.org_id),
            Index(cls.user_id),
        ]


class SupabaseOrgMember(Table, table="org_members"):
    """Organization membership table (Supabase — no FK to users table)."""

    id: UUID = Field(primary=True, default=Fn.gen_random_uuid())
    org_id: UUID = Field(foreign_key=AuthOrganization.id, on_delete=FK.CASCADE)
    user_id: UUID = Field()
    role: Varchar[L[50]] = Field(default="member")
    created_at: TimestampTZ = Field(default=Fn.now())
    updated_at: TimestampTZ = Field(default=Fn.now())

    @classmethod
    def indexes(cls) -> list[Index]:
        return [
            Index(cls.org_id, cls.user_id, unique=True),
            Index(cls.org_id),
            Index(cls.user_id),
        ]
