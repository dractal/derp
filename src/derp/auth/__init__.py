"""Derp Auth - Authentication library for FastAPI applications."""

from __future__ import annotations

from derp.auth.base import BaseAuthClient
from derp.auth.email import EmailClient
from derp.auth.exceptions import (
    AuthError,
    ConfirmationURLMissingError,
    EmailSendError,
    OrgMismatchError,
    PasswordValidationError,
    SignupDisabledError,
)
from derp.auth.jwt import TokenPair, TokenPayload
from derp.auth.models import (
    AuthOrganization,
    AuthOrgMember,
    AuthProvider,
    AuthRequest,
    AuthResult,
    AuthSession,
    AuthUser,
    CursorResult,
    OrgInfo,
    OrgMemberInfo,
    SessionInfo,
    SupabaseOrgMember,
    UserInfo,
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
    "OrgMismatchError",
    "PasswordValidationError",
    "SignupDisabledError",
    # JWT
    "TokenPair",
    "TokenPayload",
    # Models
    "AuthResult",
    "CursorResult",
    "AuthOrgMember",
    "AuthOrganization",
    "AuthProvider",
    "AuthRequest",
    "AuthSession",
    "AuthUser",
    "OrgInfo",
    "OrgMemberInfo",
    "SessionInfo",
    "UserInfo",
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
    "NativeAuthClient",
    "SupabaseAuthClient",
    "SupabaseOrgMember",
    "WorkOSAuthClient",
    "WorkOSOrganization",
]
