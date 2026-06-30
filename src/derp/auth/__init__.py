"""Derp Auth - Authentication library for FastAPI applications."""

from __future__ import annotations

from derp.auth.base import (
    AuthOutcome,
    AuthResult,
    BaseAuthClient,
    Challenge,
    Enrollment,
    FactorInfo,
    FactorType,
    Identity,
    Invitation,
    MFAStatus,
    Org,
    OrgMember,
    Page,
    Session,
    Tenant,
    TokenSet,
)
from derp.auth.email import EmailClient
from derp.auth.exceptions import (
    AuthError,
    ConfirmationURLMissingError,
    EmailSendError,
    ForbiddenError,
    OrgMismatchError,
    PasswordValidationError,
    SignupDisabledError,
)
from derp.auth.gcip_client import GCIPAuthClient
from derp.auth.jwt import TokenPair, TokenPayload
from derp.auth.models import (
    AuthInvitation,
    AuthOrganization,
    AuthOrgMember,
    AuthProvider,
    AuthRequest,
    AuthSession,
    AuthStatus,
    AuthUser,
    GCIPOrgMember,
    InvitationState,
    SupabaseOrgMember,
    WorkOSOrganization,
)
from derp.auth.native_client import NativeAuthClient
from derp.auth.password import (
    Argon2Hasher,
    PasswordHasher,
    PasswordValidationResult,
    generate_secure_token,
)
from derp.auth.providers import (
    BaseOAuthProvider,
    GitHubProvider,
    GoogleProvider,
    OAuthUserInfo,
)
from derp.auth.supabase_client import SupabaseAuthClient
from derp.auth.workos_client import WorkOSAuthClient
from derp.config import (
    AuthConfig,
    EmailConfig,
    GCIPConfig,
    GitHubOAuthConfig,
    GoogleOAuthConfig,
    JWTConfig,
    NativeAuthConfig,
    PasswordConfig,
    SupabaseConfig,
    WorkOSConfig,
)

__all__ = [
    # Config
    "AuthConfig",
    "EmailConfig",
    "GCIPConfig",
    "GitHubOAuthConfig",
    "GoogleOAuthConfig",
    "JWTConfig",
    "NativeAuthConfig",
    "PasswordConfig",
    "SupabaseConfig",
    "WorkOSConfig",
    # Exceptions
    "AuthError",
    "ConfirmationURLMissingError",
    "EmailSendError",
    "ForbiddenError",
    "OrgMismatchError",
    "PasswordValidationError",
    "SignupDisabledError",
    # JWT
    "TokenPair",
    "TokenPayload",
    # Result / domain types
    "AuthOutcome",
    "AuthResult",
    "AuthStatus",
    "Identity",
    "Session",
    "TokenSet",
    "Org",
    "OrgMember",
    "Invitation",
    "Tenant",
    "Page",
    "FactorType",
    "FactorInfo",
    "MFAStatus",
    "Enrollment",
    "Challenge",
    # DB models
    "AuthInvitation",
    "AuthOrgMember",
    "AuthOrganization",
    "AuthProvider",
    "AuthRequest",
    "AuthSession",
    "AuthUser",
    "GCIPOrgMember",
    "InvitationState",
    # Password
    "Argon2Hasher",
    "PasswordHasher",
    "PasswordValidationResult",
    "generate_secure_token",
    # OAuth Providers
    "BaseOAuthProvider",
    "GitHubProvider",
    "GoogleProvider",
    "OAuthUserInfo",
    # Email
    "EmailClient",
    # Client
    "BaseAuthClient",
    "GCIPAuthClient",
    "NativeAuthClient",
    "SupabaseAuthClient",
    "SupabaseOrgMember",
    "WorkOSAuthClient",
    "WorkOSOrganization",
]
