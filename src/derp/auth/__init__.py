"""Derp Auth - Authentication library for FastAPI applications."""

from __future__ import annotations

from derp.auth.client import AuthClient
from derp.auth.email import EmailClient
from derp.auth.exceptions import (
    AuthError,
    ConfirmationTokenInvalidError,
    EmailNotConfirmedError,
    EmailSendError,
    InvalidCredentialsError,
    InvalidTokenError,
    MagicLinkExpiredError,
    OAuthError,
    OAuthProviderError,
    OAuthStateError,
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
    AuthProvider,
    AuthSession,
    AuthUser,
)
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
    EmailConfig,
    GitHubOAuthConfig,
    GoogleOAuthConfig,
    JWTConfig,
    PasswordConfig,
)

__all__ = [
    # Config
    "AuthConfig",
    "EmailConfig",
    "GitHubOAuthConfig",
    "GoogleOAuthConfig",
    "JWTConfig",
    "PasswordConfig",
    # Exceptions
    "AuthError",
    "ConfirmationTokenInvalidError",
    "EmailNotConfirmedError",
    "EmailSendError",
    "InvalidCredentialsError",
    "InvalidTokenError",
    "MagicLinkExpiredError",
    "OAuthError",
    "OAuthProviderError",
    "OAuthStateError",
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
    "AuthProvider",
    "AuthSession",
    "AuthUser",
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
    "AuthClient",
]
