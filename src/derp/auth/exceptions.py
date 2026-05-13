"""Custom exceptions for the auth module.

The hierarchy is rooted at ``AuthError``. Backend clients normalise their
internal errors to these types so callers can handle failures without
knowing whether they're talking to native, Supabase, or WorkOS.

Conventions:

* Lookups that don't find anything raise ``*NotFoundError``. (Bool-returning
  ``delete_*`` / ``remove_*`` methods are the exception — they return
  ``False`` for "wasn't there" so callers don't need try/except for the
  idempotent case.)
* Mutations that conflict raise the matching ``*ConflictError`` /
  ``*AlreadyExistsError`` carrying the conflicting value.
* Sign-in failures collapse to a single ``InvalidCredentialsError`` to
  avoid email-enumeration leaks.
* Token verification failures (refresh, magic link, password reset, email
  confirm) raise ``InvalidTokenError``.
* Anything else from the backend (network, 5xx, unrecognised 4xx) is
  wrapped in ``AuthBackendError`` with the original exception chained.
"""

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


class AuthBackendError(AuthError):
    """Wraps an unrecognised backend failure (network, 5xx, unknown 4xx).

    Always chains the original exception via ``raise ... from e`` so the
    backend-specific type and traceback are preserved for debugging.
    """

    def __init__(self, message: str = "Auth backend error"):
        super().__init__(message, code="auth_backend_error")


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


class OrgMismatchError(AuthError):
    """Raised when ``assert_same_org`` finds the session belongs to a different org.

    Used as the default tenant-scoping failure so handlers can map it to a
    403 (or whatever their framework prefers) without inspecting strings.
    """

    def __init__(self, message: str = "Session does not belong to this organization"):
        super().__init__(message, code="org_mismatch")


# ── Credential / token errors ────────────────────────────────────


class InvalidCredentialsError(AuthError):
    """Raised when sign-in fails for any credential reason.

    Deliberately collapses "wrong password" and "no such user" into one
    error to avoid email-enumeration. Apps that need to distinguish (e.g.,
    for internal rate-limit dashboards) should consult backend logs.
    """

    def __init__(self, message: str = "Invalid email or password"):
        super().__init__(message, code="invalid_credentials")


class InvalidTokenError(AuthError):
    """Raised when a token (refresh, magic link, reset, confirm) is rejected."""

    def __init__(self, message: str = "Token is invalid or expired"):
        super().__init__(message, code="invalid_token")


# ── Not-found errors ─────────────────────────────────────────────


class UserNotFoundError(AuthError):
    """Raised when a user lookup or update targets a missing user."""

    def __init__(self, message: str = "User not found"):
        super().__init__(message, code="user_not_found")


class OrgNotFoundError(AuthError):
    """Raised when an org lookup or operation targets a missing org."""

    def __init__(self, message: str = "Organization not found"):
        super().__init__(message, code="org_not_found")


class OrgMemberNotFoundError(AuthError):
    """Raised when a membership lookup or update targets a missing membership."""

    def __init__(self, message: str = "Organization membership not found"):
        super().__init__(message, code="org_member_not_found")


# ── Conflict errors ──────────────────────────────────────────────


class EmailAlreadyExistsError(AuthError):
    """Raised when sign_up is called with an email that already has an account.

    The conflicting email is exposed via ``.email`` for logging.
    """

    def __init__(self, email: str, message: str | None = None):
        super().__init__(
            message or f"An account with email {email!r} already exists",
            code="email_already_exists",
        )
        self.email = email


class OrgSlugConflictError(AuthError):
    """Raised when create_org / update_org would create a duplicate slug.

    The conflicting slug is exposed via ``.slug`` for logging.
    """

    def __init__(self, slug: str, message: str | None = None):
        super().__init__(
            message or f"Organization slug {slug!r} is already taken",
            code="org_slug_conflict",
        )
        self.slug = slug


class MemberAlreadyExistsError(AuthError):
    """Raised when add_org_member is called for a user already in the org."""

    def __init__(self, message: str = "User is already a member of this organization"):
        super().__init__(message, code="member_already_exists")


class LastOwnerError(AuthError):
    """Raised when removing a member would leave the org with no owners.

    Distinct from a generic "not found" failure on remove_org_member —
    this is a structural constraint, not a missing record.
    """

    def __init__(
        self,
        message: str = "Cannot remove the last owner of an organization",
    ):
        super().__init__(message, code="last_owner")
