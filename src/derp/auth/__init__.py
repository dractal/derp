"""Derp Auth - Authentication library for FastAPI applications."""

from __future__ import annotations

from derp.auth.base import BaseAuthClient
from derp.auth.clerk_client import ClerkAuthClient
from derp.auth.email import EmailClient
from derp.auth.exceptions import (
    AuthError,
    ConfirmationTokenInvalidError,
    EmailNotConfirmedError,
    EmailSendError,
    InvalidCredentialsError,
    InvalidTokenError,
    MagicLinkExpiredError,
    NotOrgMemberError,
    OAuthError,
    OAuthProviderError,
    OAuthStateError,
    OrgAlreadyExistsError,
    OrgLastOwnerError,
    OrgMemberExistsError,
    OrgMemberNotFoundError,
    OrgNotFoundError,
    PasswordValidationError,
    RecoveryTokenInvalidError,
    RefreshTokenReusedError,
    RefreshTokenRevokedError,
    SessionExpiredError,
    SessionNotFoundError,
    SignupDisabledError,
    TokenExpiredError,
    UserAlreadyExistsError,
    UserNotActiveError,
    UserNotFoundError,
)
from derp.auth.jwt import TokenPair, TokenPayload
from derp.auth.models import (
    AuthOrganization,
    AuthOrgMember,
    AuthProvider,
    AuthRequest,
    AuthSession,
    AuthUser,
    OrgInfo,
    OrgMemberInfo,
    SessionInfo,
    UserInfo,
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
from derp.config import (
    AuthConfig,
    ClerkConfig,
    EmailConfig,
    GitHubOAuthConfig,
    GoogleOAuthConfig,
    JWTConfig,
    NativeAuthConfig,
    PasswordConfig,
)

__all__ = [
    # Config
    "AuthConfig",
    "ClerkConfig",
    "EmailConfig",
    "GitHubOAuthConfig",
    "GoogleOAuthConfig",
    "JWTConfig",
    "NativeAuthConfig",
    "PasswordConfig",
    # Exceptions
    "AuthError",
    "ConfirmationTokenInvalidError",
    "EmailNotConfirmedError",
    "EmailSendError",
    "InvalidCredentialsError",
    "InvalidTokenError",
    "MagicLinkExpiredError",
    "NotOrgMemberError",
    "OAuthError",
    "OAuthProviderError",
    "OAuthStateError",
    "OrgAlreadyExistsError",
    "OrgLastOwnerError",
    "OrgMemberExistsError",
    "OrgMemberNotFoundError",
    "OrgNotFoundError",
    "PasswordValidationError",
    "RecoveryTokenInvalidError",
    "RefreshTokenRevokedError",
    "RefreshTokenReusedError",
    "SessionExpiredError",
    "SessionNotFoundError",
    "SignupDisabledError",
    "TokenExpiredError",
    "UserAlreadyExistsError",
    "UserNotActiveError",
    "UserNotFoundError",
    # JWT
    "JWTManager",
    "TokenPair",
    "TokenPayload",
    # Models
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
    "BcryptHasher",
    "CompositeHasher",
    "PasswordHasher",
    "PasswordValidationResult",
    "PasswordValidator",
    "create_default_hasher",
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
    "ClerkAuthClient",
    "NativeAuthClient",
]
