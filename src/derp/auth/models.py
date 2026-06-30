"""Database models for the auth module."""

from __future__ import annotations

import enum
from collections.abc import Mapping
from typing import Protocol, runtime_checkable

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


class AuthStatus(enum.StrEnum):
    """Outcome of a sign-up / sign-in step.

    Sign-in is neither always one-shot nor always a success: continuation
    states and *expected* user failures (wrong password, taken email) are
    routine outcomes, modelled here as data rather than exceptions. Only
    ``COMPLETE`` carries tokens. Genuinely exceptional failures (provider
    5xx/network ``AuthBackendError``, misconfiguration) still raise.

    ``INVALID_CREDENTIALS`` deliberately collapses wrong-password and
    unknown-user to avoid enumeration leaks.
    """

    COMPLETE = "complete"  # tokens issued
    MFA_REQUIRED = "mfa_required"  # a second factor must be satisfied first
    VERIFICATION_REQUIRED = "verification_required"  # confirm email/phone first
    INVALID_CREDENTIALS = "invalid_credentials"  # wrong password OR unknown user
    EMAIL_EXISTS = "email_exists"  # sign-up against an existing account
    WEAK_PASSWORD = "weak_password"  # password failed policy
    INVALID_TOKEN = "invalid_token"  # magic link / oob code unknown/expired/used
    ACCOUNT_DISABLED = "account_disabled"  # user is banned/suspended


class InvitationState(enum.StrEnum):
    """Lifecycle of an org invitation."""

    PENDING = "pending"  # awaiting acceptance
    ACCEPTED = "accepted"  # redeemed; the invitee is now a member
    REVOKED = "revoked"  # withdrawn before acceptance
    EXPIRED = "expired"  # past its expiry without acceptance


@runtime_checkable
class AuthRequest(Protocol):
    """Protocol for objects that carry HTTP headers (e.g. FastAPI Request)."""

    @property
    def headers(self) -> Mapping[str, str]: ...


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


class WorkOSOrganization(Table, table="organizations"):
    """WorkOS-managed organizations — slug index for the WorkOS API.

    WorkOS owns name, metadata, members, and timestamps. This local table
    exists ONLY as a slug index: ``id`` IS the WorkOS org id (the same
    string the WorkOS API and JWT carry), and ``slug`` is a locally-
    enforced unique handle so slug→id lookup is O(1) instead of paginating
    the WorkOS API.

    Application FKs to ``organizations.id`` therefore hold the same value
    as ``SessionInfo.org_id``, so tenant-scoping comparisons need no
    translation step (which is the most common source of confused-deputy
    bugs in dual-id setups).

    The unique columns get implicit unique indexes from PostgreSQL.
    """

    id: Varchar[L[255]] = Field(primary=True)
    slug: Varchar[L[255]] = Field(unique=True)


class GCIPOrgMember(Table, table="org_members"):
    """Organization membership for IdP-layered backends (GCIP).

    Identical to :class:`AuthOrgMember` except ``user_id`` is the IdP's user id
    (a string — GCIP's ``localId``), with no FK to a users table: the IdP owns
    users, derp owns the org layer. The org itself is still a derp-owned row in
    :class:`AuthOrganization` (a UUID), so ``org_id`` keeps its FK.
    """

    id: UUID = Field(primary=True, default=Fn.gen_random_uuid())
    org_id: UUID = Field(foreign_key=AuthOrganization.id, on_delete=FK.CASCADE)
    user_id: Varchar[L[255]] = Field()  # IdP uid (GCIP localId), no FK
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


class AuthInvitation(Table, table="org_invitations"):
    """A pending invite for an email to join an org.

    Backend-agnostic: invitations reference the org (a derp-owned UUID) and an
    email — never a user, since the invitee may not have an account yet.
    ``token`` is the opaque secret embedded in the invite link; ``state``
    tracks the lifecycle (:class:`InvitationState`).
    """

    id: UUID = Field(primary=True, default=Fn.gen_random_uuid())
    org_id: UUID = Field(foreign_key=AuthOrganization.id, on_delete=FK.CASCADE)
    email: Varchar[L[255]] = Field()
    role: Varchar[L[50]] = Field(default="member")
    state: Enum[InvitationState] = Field(default=InvitationState.PENDING)
    token: Varchar[L[255]] = Field(unique=True)
    expires_at: Nullable[TimestampTZ] = Field()
    created_at: TimestampTZ = Field(default=Fn.now())
    updated_at: TimestampTZ = Field(default=Fn.now())

    @classmethod
    def indexes(cls) -> list[Index]:
        return [
            Index(cls.token),
            Index(cls.org_id),
            Index(cls.email),
        ]
