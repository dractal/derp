"""Custom exceptions for the auth module."""

from __future__ import annotations


class AuthError(Exception):
    """Base exception for all auth errors."""

    def __init__(self, message: str, code: str | None = None):
        super().__init__(message)
        self.message = message
        self.code = code or "auth_error"


class InvalidCredentialsError(AuthError):
    """Raised when login credentials are invalid."""

    def __init__(self, message: str = "Invalid email or password"):
        super().__init__(message, code="invalid_credentials")


class UserNotFoundError(AuthError):
    """Raised when a user cannot be found."""

    def __init__(self, message: str = "User not found"):
        super().__init__(message, code="user_not_found")


class UserAlreadyExistsError(AuthError):
    """Raised when attempting to create a user that already exists."""

    def __init__(self, message: str = "User with this email already exists"):
        super().__init__(message, code="user_already_exists")


class UserNotActiveError(AuthError):
    """Raised when a user account is disabled."""

    def __init__(self, message: str = "User account is disabled"):
        super().__init__(message, code="user_not_active")


class EmailNotConfirmedError(AuthError):
    """Raised when email confirmation is required but not completed."""

    def __init__(self, message: str = "Email address has not been confirmed"):
        super().__init__(message, code="email_not_confirmed")


class ConfirmationURLMissingError(AuthError):
    """Raised when a confirmation URL is missing."""

    def __init__(self, message: str = "Confirmation URL is missing"):
        super().__init__(message, code="confirmation_url_missing")


class InvalidTokenError(AuthError):
    """Raised when a token is invalid or expired."""

    def __init__(self, message: str = "Invalid or expired token"):
        super().__init__(message, code="invalid_token")


class TokenExpiredError(InvalidTokenError):
    """Raised when a token has expired."""

    def __init__(self, message: str = "Token has expired"):
        super().__init__(message)
        self.code = "token_expired"


class SessionNotFoundError(AuthError):
    """Raised when a session cannot be found."""

    def __init__(self, message: str = "Session not found"):
        super().__init__(message, code="session_not_found")


class SessionExpiredError(AuthError):
    """Raised when a session has expired."""

    def __init__(self, message: str = "Session has expired"):
        super().__init__(message, code="session_expired")


class RefreshTokenRevokedError(AuthError):
    """Raised when a refresh token has been revoked."""

    def __init__(self, message: str = "Refresh token has been revoked"):
        super().__init__(message, code="refresh_token_revoked")


class RefreshTokenReusedError(AuthError):
    """Raised when token rotation detects a reused refresh token (potential theft)."""

    def __init__(
        self, message: str = "Refresh token reuse detected, all sessions revoked"
    ):
        super().__init__(message, code="refresh_token_reused")


class MagicLinkExpiredError(AuthError):
    """Raised when a magic link has expired."""

    def __init__(self, message: str = "Magic link has expired"):
        super().__init__(message, code="magic_link_expired")


class PasswordValidationError(AuthError):
    """Raised when a password fails validation."""

    def __init__(self, message: str = "Password does not meet requirements"):
        super().__init__(message, code="password_validation_error")


class OAuthError(AuthError):
    """Raised when OAuth authentication fails."""

    def __init__(self, message: str = "OAuth authentication failed"):
        super().__init__(message, code="oauth_error")


class OAuthStateError(OAuthError):
    """Raised when OAuth state validation fails."""

    def __init__(self, message: str = "Invalid OAuth state"):
        super().__init__(message)
        self.code = "oauth_state_error"


class OAuthProviderError(OAuthError):
    """Raised when the OAuth provider returns an error."""

    def __init__(self, message: str = "OAuth provider returned an error"):
        super().__init__(message)
        self.code = "oauth_provider_error"


class SignupDisabledError(AuthError):
    """Raised when signup is disabled."""

    def __init__(self, message: str = "Signup is currently disabled"):
        super().__init__(message, code="signup_disabled")


class EmailSendError(AuthError):
    """Raised when sending an email fails."""

    def __init__(self, message: str = "Failed to send email"):
        super().__init__(message, code="email_send_error")


class RecoveryTokenInvalidError(AuthError):
    """Raised when a password recovery token is invalid."""

    def __init__(self, message: str = "Invalid recovery token"):
        super().__init__(message, code="recovery_token_invalid")


class ConfirmationTokenInvalidError(AuthError):
    """Raised when an email confirmation token is invalid."""

    def __init__(self, message: str = "Invalid confirmation token"):
        super().__init__(message, code="confirmation_token_invalid")
