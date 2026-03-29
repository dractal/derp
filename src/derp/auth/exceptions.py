"""Custom exceptions for the auth module."""

from __future__ import annotations


class AuthError(Exception):
    """Base exception for all auth errors."""

    def __init__(self, message: str, code: str | None = None):
        super().__init__(message)
        self.message = message
        self.code = code or "auth_error"


class AuthNotConnectedError(AuthError):
    """Raised when an auth method is called before connect()."""

    def __init__(self, message: str = "Auth client not connected"):
        super().__init__(message, code="auth_not_connected")


class ConfirmationURLMissingError(AuthError):
    """Raised when a confirmation URL is missing."""

    def __init__(self, message: str = "Confirmation URL is missing"):
        super().__init__(message, code="confirmation_url_missing")


class PasswordValidationError(AuthError):
    """Raised when a password fails validation."""

    def __init__(self, message: str = "Password does not meet requirements"):
        super().__init__(message, code="password_validation_error")


class SignupDisabledError(AuthError):
    """Raised when signup is disabled."""

    def __init__(self, message: str = "Signup is currently disabled"):
        super().__init__(message, code="signup_disabled")


class EmailSendError(AuthError):
    """Raised when sending an email fails."""

    def __init__(self, message: str = "Failed to send email"):
        super().__init__(message, code="email_send_error")


class OrgAlreadyExistsError(AuthError):
    """Raised when an organization with the same slug already exists."""

    def __init__(self, message: str = "Organization with this slug already exists"):
        super().__init__(message, code="org_already_exists")


class OrgMemberExistsError(AuthError):
    """Raised when a user is already a member of the organization."""

    def __init__(self, message: str = "User is already a member of this organization"):
        super().__init__(message, code="org_member_exists")


class OrgLastOwnerError(AuthError):
    """Raised when attempting to remove or demote the last owner."""

    def __init__(self, message: str = "Cannot remove or demote the last owner"):
        super().__init__(message, code="org_last_owner")
