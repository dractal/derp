"""Base interface for auth clients."""

from __future__ import annotations

import abc
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, cast

from derp.auth.exceptions import (
    AuthError,
    CapabilityNotSupportedError,
    ForbiddenError,
    OrgMismatchError,
)
from derp.auth.models import AuthProvider, AuthRequest, AuthStatus
from derp.kv.base import KVClient
from derp.orm import DatabaseEngine


class FactorType(StrEnum):
    """A second-factor / strong-credential type."""

    TOTP = "totp"
    SMS = "sms"
    EMAIL = "email"
    PASSKEY = "passkey"  # WebAuthn / FIDO2


@dataclass(frozen=True, kw_only=True)
class TokenSet:
    """Issued credentials. ``refresh_token`` powers long-lived mobile sessions."""

    access_token: str
    refresh_token: str | None
    id_token: str | None
    token_type: str
    expires_in: int
    expires_at: datetime


@dataclass(frozen=True, kw_only=True)
class FactorInfo:
    """An enrolled MFA factor."""

    id: str
    type: FactorType
    verified: bool
    label: str | None  # masked phone, device name, etc.
    created_at: datetime


@dataclass(frozen=True, kw_only=True)
class MFAStatus:
    """The MFA state reflected in a verified session."""

    enrolled: bool  # the user has at least one verified factor
    satisfied: bool  # this session cleared the step-up
    factor_types: Sequence[FactorType]


@dataclass(frozen=True, kw_only=True)
class Identity:
    """A user as the IdP sees it.

    ``email``/``phone`` are nullable — phone-only and anonymous users are
    first-class, unlike V1's mandatory email.
    """

    id: str
    tenant_id: str | None
    email: str | None
    email_verified: bool
    phone: str | None
    phone_verified: bool
    is_anonymous: bool
    disabled: bool
    roles: Sequence[str]
    created_at: datetime
    updated_at: datetime
    last_sign_in_at: datetime | None
    metadata: Mapping[str, Any]


@dataclass(frozen=True, kw_only=True)
class Session:
    """Verified token state — the single active org, roles, scopes, MFA level.

    ``org_id``/``org_slug``/``org_role`` are the one **active org** this session
    is bound to (``None`` until a B2B session is org-scoped or
    :meth:`BaseAuthClient.set_active_org` switches it). A request authorizes
    against this single org — :meth:`BaseAuthClient.require_org` asserts the
    route's slug matches it, with no IO (``org_id`` is the stable key to persist
    / FK against, not the mutable slug). The user's *full* membership list is
    read separately via :meth:`BaseAuthClient.list_orgs` (``user_id=``).

    ``permissions`` are the active org's RBAC grants (e.g. ``"billing:manage"``),
    checked zero-IO with :meth:`BaseAuthClient.has` / :meth:`require_permission`;
    empty for backends that don't model per-org permissions. Distinct from
    ``scopes`` (the IdP's token-level grants).
    """

    user_id: str
    session_id: str
    tenant_id: str | None
    org_id: str | None
    org_role: str | None
    roles: Sequence[str]
    scopes: Sequence[str]
    is_anonymous: bool
    mfa: MFAStatus
    issued_at: datetime
    expires_at: datetime
    claims: Mapping[str, Any]  # raw verified claims, escape hatch
    org_slug: str | None = None
    permissions: Sequence[str] = ()


@dataclass(frozen=True, kw_only=True)
class Enrollment:
    """Handle from enrolling a factor (TOTP secret/QR, or a pending SMS)."""

    factor_id: str
    type: FactorType
    secret: str | None  # TOTP shared secret
    qr_code_uri: str | None  # otpauth:// provisioning URI
    expires_at: datetime | None


@dataclass(frozen=True, kw_only=True)
class Challenge:
    """Handle for an in-progress MFA challenge (step-up)."""

    id: str
    factor_id: str
    type: FactorType
    expires_at: datetime


@dataclass(frozen=True, kw_only=True)
class AuthOutcome:
    """Result of a sign-in/sign-up step — a total state machine over ``status``.

    * ``COMPLETE`` carries ``identity`` + ``tokens``.
    * ``MFA_REQUIRED`` / ``VERIFICATION_REQUIRED`` carry a ``pending_token``
      continuation handle and the ``factors`` that can satisfy the step.
    * failure statuses (``INVALID_CREDENTIALS`` etc.) carry ``error`` — the
      typed :class:`AuthError` with the underlying detail, attached (not
      raised) so it's available for logging without leaking to the end user.

    Callers ``match`` on ``status`` to handle every case uniformly. Those who
    prefer exceptions (e.g. a global 401 handler) call :meth:`raise_for_status`.
    """

    status: AuthStatus
    identity: Identity | None = None
    tokens: TokenSet | None = None
    pending_token: str | None = None
    factors: Sequence[FactorInfo] = ()
    error: AuthError | None = None

    @property
    def ok(self) -> bool:
        """True iff the flow completed and tokens were issued."""
        return self.status is AuthStatus.COMPLETE

    def raise_for_status(self) -> AuthOutcome:
        """Return self if COMPLETE; otherwise raise — the opt-in exception bridge.

        Re-raises the attached typed ``error`` for failure statuses; raises a
        generic ``AuthError`` for an unconsumed continuation (MFA/verification).
        Lets exception-style callers write
        ``(await auth.sign_in(...)).raise_for_status()``.
        """
        if self.status is AuthStatus.COMPLETE:
            return self
        if self.error is not None:
            raise self.error
        raise AuthError(
            f"Authentication did not complete: {self.status}",
            code=str(self.status),
        )


@dataclass(frozen=True, kw_only=True)
class AuthResult[T]:
    """A value-or-error outcome from an org/membership operation.

    The :class:`AuthOutcome` analogue for ops that aren't a state machine —
    create an org, add a member, get an org — where the routine outcomes are
    "success with a value" or "an expected domain failure" (slug taken, no
    such org, already a member). The typed :class:`AuthError` is *attached* on
    failure, not raised, so callers don't have to ctrl-click into derp to
    discover which exceptions to catch; truly exceptional things (network,
    backend 5xx, programmer errors) still raise.

    ``ok`` branches inline; :meth:`raise_for_status` is the opt-in exception
    bridge for callers (e.g. FastAPI handlers) that prefer raising — it
    returns the unwrapped value on success.
    """

    value: T | None = None
    error: AuthError | None = None

    @property
    def ok(self) -> bool:
        """True iff the operation succeeded (no attached error)."""
        return self.error is None

    def raise_for_status(self) -> T:
        """Return :attr:`value` on success; re-raise the attached error otherwise."""
        if self.error is not None:
            raise self.error
        return cast(T, self.value)


@dataclass(frozen=True, kw_only=True)
class Tenant:
    """An isolation boundary: its own user pool, signing keys, and config."""

    id: str
    name: str
    created_at: datetime
    updated_at: datetime
    metadata: Mapping[str, Any]


@dataclass(frozen=True, kw_only=True)
class Org:
    """A B2B grouping of users within a tenant."""

    id: str
    tenant_id: str | None
    name: str
    slug: str
    created_at: datetime
    updated_at: datetime
    metadata: Mapping[str, Any]


@dataclass(frozen=True, kw_only=True)
class OrgMember:
    """A user's membership of an org, with a role."""

    org_id: str
    user_id: str
    role: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, kw_only=True)
class Invitation:
    """A pending invite for an email to join an org."""

    id: str
    org_id: str
    email: str
    role: str
    state: str  # pending | accepted | revoked | expired
    expires_at: datetime | None


@dataclass(frozen=True, kw_only=True)
class Page[T]:
    """A cursor-paginated slice. ``next_cursor`` is opaque; ``None`` at the end."""

    items: Sequence[T]
    next_cursor: str | None
    has_more: bool


class BaseAuthClient(abc.ABC):
    """IdP-first authentication interface.

    The contract models what an identity provider does, with consumer (B2C),
    enterprise (B2B), multi-tenancy, mobile, and MFA treated as first-class:

    * **Runtime** — :meth:`verify_token` is the per-request hot path; the only
      abstract method. :meth:`authenticate` is a bearer-extracting helper.
    * **Multi-tenancy** — :meth:`tenant` returns a tenant-scoped view; every
      ``Identity``/``Session`` carries ``tenant_id``; tenant CRUD provisions
      isolation boundaries.
    * **B2C** — password, magic link, email/SMS OTP, anonymous guests with
      credential upgrade, and passkeys (WebAuthn).
    * **B2B** — enterprise SSO (JIT-provisioned, org-scoped sessions), orgs,
      membership, and invitations.
    * **MFA** — explicit step-up: a sign-in may return ``MFA_REQUIRED`` with a
      ``pending_token``; enrol/challenge/verify complete it.
    * **Mobile** — refresh tokens, anonymous→permanent upgrade, passkeys.

    Every operation lives on this one type (no ``isinstance`` narrowing).
    Unsupported operations raise :class:`CapabilityNotSupportedError`; the
    ``supports_*`` flags let a caller branch ahead of a call when it prefers.
    """

    # ------------------------------------------------------------------
    # Capability advertisement
    # ------------------------------------------------------------------

    supports_multi_tenant: bool = False
    supports_password: bool = False
    supports_magic_link: bool = False
    supports_otp: bool = False  # email / SMS one-time codes
    supports_anonymous: bool = False  # guest sessions + upgrade
    supports_passkeys: bool = False  # WebAuthn / FIDO2
    supports_oauth: bool = False  # social / OIDC
    supports_sso: bool = False  # enterprise SAML/OIDC connections
    supports_mfa: bool = False
    supports_user_admin: bool = False
    supports_sessions: bool = False
    supports_orgs: bool = False
    supports_invitations: bool = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Initialise backend connections."""

    async def disconnect(self) -> None:
        """Close backend connections."""

    def set_db(self, db: DatabaseEngine | None) -> None:
        """Provide a database engine (store-backed capabilities)."""

    def set_kv(self, kv: KVClient | None) -> None:
        """Provide a KV store (caching / transient flow state)."""

    def set_email(self, email_client: Any) -> None:
        """Provide an email client. Only self-emailing backends (native) use it.

        Hosted IdPs send their own mail, so the default is a no-op; ``DerpClient``
        calls this uniformly regardless of backend.
        """

    # ------------------------------------------------------------------
    # Runtime / relying party  (verify_token is the only abstract method)
    # ------------------------------------------------------------------

    @abc.abstractmethod
    async def verify_token(self, token: str) -> Session | None:
        """Verify an access/ID token and return its session, or ``None``.

        The per-request hot path. Missing/expired/invalid tokens return
        ``None``. In a multi-tenant deployment the token's tenant is reflected
        in ``Session.tenant_id``.
        """

    async def authenticate(self, request: AuthRequest) -> Session | None:
        """Extract a bearer token from *request* and verify it (HTTP helper)."""
        header = request.headers.get("authorization") or request.headers.get(
            "Authorization"
        )
        if not header or not header.startswith("Bearer "):
            return None
        return await self.verify_token(header[7:])

    # ------------------------------------------------------------------
    # Authorization — zero-IO checks over a verified Session's ACTIVE org.
    #
    # `authenticate` answers *who you are* (authentication); these answer
    # *what you may do* (authorization), the two-step split every IdP uses.
    # A session is bound to ONE active org (`org_id`/`org_slug`/`org_role`,
    # switched via `set_active_org`); these read it with no lookup. A request
    # supplies the org *slug* from its own route (`/orgs/{slug}/...`); the
    # framework never guesses it from a header. To ask about a DIFFERENT org
    # (membership/role outside the active one), use `get_member_role` (one IO).
    # ------------------------------------------------------------------

    def active_role(self, session: Session) -> str | None:
        """Return the session's role in its ACTIVE org, or ``None`` if unbound.

        Zero IO — reads ``Session.org_role``. The nullable counterpart to
        :meth:`require_role`: use it when an unbound session is an expected
        branch rather than a 403.
        """
        return session.org_role

    def require_org(self, session: Session, org: str) -> Session:
        """Assert the session's ACTIVE org is *org* (a slug); return it, or raise.

        The guard for ``/orgs/{slug}/...`` routes in the single-active-org
        model: the route's slug must equal the session's already-bound org, or
        it is a 403 (:class:`OrgMismatchError`). The session is returned
        unchanged — it is already bound (``org_id`` is the stable key to
        persist / FK against, not the slug). To act in a *different* org, switch
        first with :meth:`set_active_org`; to merely check membership in another
        org without switching, use :meth:`get_member_role`.
        """
        if session.org_slug != org:
            raise OrgMismatchError()
        return session

    def require_role(self, session: Session, *roles: str) -> None:
        """Raise unless the session's ACTIVE-org role is one of *roles*. Zero IO.

        Pair with :meth:`require_org` when a route needs both the right org and
        a sufficient role::

            self.require_org(session, slug)
            self.require_role(session, "owner", "admin")

        Raises :class:`ForbiddenError`.
        """
        if session.org_role not in roles:
            raise ForbiddenError(
                f"Role {session.org_role!r} is not one of the required {roles!r}"
            )

    def has(self, session: Session, permission: str) -> bool:
        """Whether the session's ACTIVE org grants *permission*. Zero IO.

        A membership test over :attr:`Session.permissions` (the active org's
        RBAC grants). The Clerk ``auth().has({permission})`` analogue.
        """
        return permission in session.permissions

    def require_permission(self, session: Session, permission: str) -> None:
        """Raise :class:`ForbiddenError` unless the active org grants *permission*."""
        if permission not in session.permissions:
            raise ForbiddenError(f"Missing required permission: {permission!r}")

    async def get_member_role(self, *, user_id: str, org: str) -> str | None:
        """Authoritatively resolve a user's role in *org* (a slug) — one IO.

        The DB-backed companion to the zero-IO active-org helpers, for the
        *separate-API* path: questions about an org the session is **not**
        currently acting in — validating membership before :meth:`set_active_org`,
        or cross-org admin checks. Returns ``None`` if the user isn't a member::

            role = await auth.get_member_role(user_id=session.user_id, org=other)
        """
        raise CapabilityNotSupportedError("orgs")

    # ------------------------------------------------------------------
    # Multi-tenancy  (supports_multi_tenant)
    # ------------------------------------------------------------------

    def tenant(self, tenant_id: str) -> BaseAuthClient:
        """Return a view scoped to *tenant_id*; all ops act on that pool.

        The scoping seam for multi-tenancy — instead of threading
        ``tenant_id`` through every method, you bind it once:
        ``await auth.tenant("acme").sign_in_with_password(...)``.
        """
        raise CapabilityNotSupportedError("multi_tenant")

    async def create_tenant(self, *, name: str, **kwargs: Any) -> Tenant:
        """Provision a new isolation boundary (user pool + signing config)."""
        raise CapabilityNotSupportedError("multi_tenant")

    async def get_tenant(self, tenant_id: str) -> Tenant:
        """Fetch a tenant. Raises TenantNotFound-style errors when absent."""
        raise CapabilityNotSupportedError("multi_tenant")

    async def list_tenants(
        self, *, limit: int = 50, cursor: str | None = None
    ) -> Page[Tenant]:
        """List tenants (cursor-paginated)."""
        raise CapabilityNotSupportedError("multi_tenant")

    async def update_tenant(self, *, tenant_id: str, **kwargs: Any) -> Tenant:
        """Update tenant configuration."""
        raise CapabilityNotSupportedError("multi_tenant")

    async def delete_tenant(self, tenant_id: str) -> bool:
        """Delete a tenant and its pool. Returns ``False`` if not found."""
        raise CapabilityNotSupportedError("multi_tenant")

    # ------------------------------------------------------------------
    # B2C: password  (supports_password)
    # ------------------------------------------------------------------

    async def sign_up(
        self,
        *,
        email: str | None = None,
        phone: str | None = None,
        password: str | None = None,
        **kwargs: Any,
    ) -> AuthOutcome:
        """Register a user by email or phone.

        Outcome status: ``COMPLETE``, ``VERIFICATION_REQUIRED`` (confirm
        email/phone first), ``EMAIL_EXISTS``, or ``WEAK_PASSWORD``.
        """
        raise CapabilityNotSupportedError("password")

    async def sign_in_with_password(
        self, *, identifier: str, password: str
    ) -> AuthOutcome:
        """Sign in with an email-or-phone *identifier* + password.

        Outcome status: ``COMPLETE``, ``MFA_REQUIRED`` (with a
        ``pending_token``), ``INVALID_CREDENTIALS``, or ``ACCOUNT_DISABLED``.
        Provider/network failures still raise ``AuthBackendError``.
        """
        raise CapabilityNotSupportedError("password")

    # ------------------------------------------------------------------
    # B2C: passwordless — magic link + OTP  (supports_magic_link / supports_otp)
    # ------------------------------------------------------------------

    async def send_magic_link(self, *, email: str, redirect_url: str) -> None:
        """Email a passwordless sign-in link."""
        raise CapabilityNotSupportedError("magic_link")

    async def verify_magic_link(
        self, token: str, *, email: str | None = None
    ) -> AuthOutcome:
        """Complete magic-link sign-in (``email`` required by some providers).

        Outcome status: ``COMPLETE``, ``MFA_REQUIRED``, or ``INVALID_TOKEN``.
        """
        raise CapabilityNotSupportedError("magic_link")

    async def send_otp(
        self, *, phone: str | None = None, email: str | None = None
    ) -> None:
        """Send a one-time code over SMS or email."""
        raise CapabilityNotSupportedError("otp")

    async def verify_otp(self, *, identifier: str, code: str) -> AuthOutcome:
        """Verify an OTP and sign in (JIT-creates the user on first use).

        Outcome status: ``COMPLETE``, ``MFA_REQUIRED``, or ``INVALID_TOKEN``.
        """
        raise CapabilityNotSupportedError("otp")

    # ------------------------------------------------------------------
    # B2C / mobile: anonymous guests + credential upgrade  (supports_anonymous)
    # ------------------------------------------------------------------

    async def sign_in_anonymous(self) -> AuthOutcome:
        """Create an anonymous guest session (try-before-signup, mobile carts)."""
        raise CapabilityNotSupportedError("anonymous")

    async def link_credential(
        self,
        *,
        pending_token: str,
        email: str | None = None,
        phone: str | None = None,
        password: str | None = None,
        **kwargs: Any,
    ) -> AuthOutcome:
        """Upgrade an anonymous user to a permanent one, preserving their id."""
        raise CapabilityNotSupportedError("anonymous")

    # ------------------------------------------------------------------
    # B2C / mobile: passkeys (WebAuthn)  (supports_passkeys)
    # ------------------------------------------------------------------

    async def begin_passkey_registration(self, *, user_id: str) -> Mapping[str, Any]:
        """Return WebAuthn creation options to pass to the authenticator."""
        raise CapabilityNotSupportedError("passkeys")

    async def complete_passkey_registration(
        self, *, user_id: str, attestation: Mapping[str, Any]
    ) -> FactorInfo:
        """Verify the attestation and register the passkey."""
        raise CapabilityNotSupportedError("passkeys")

    async def begin_passkey_login(
        self, *, identifier: str | None = None
    ) -> Mapping[str, Any]:
        """Return WebAuthn request options.

        Omit ``identifier`` for discoverable (usernameless) credentials.
        """
        raise CapabilityNotSupportedError("passkeys")

    async def complete_passkey_login(
        self, *, assertion: Mapping[str, Any]
    ) -> AuthOutcome:
        """Verify the assertion and sign in."""
        raise CapabilityNotSupportedError("passkeys")

    # ------------------------------------------------------------------
    # Redirect flows: social OAuth (supports_oauth) + enterprise SSO (supports_sso)
    #
    # Both are redirect -> credential -> exchange, so they share one URL
    # builder (scoped by `provider` for social, or `organization`/`connection`/
    # `domain` for SSO). The completers stay split because the callback artifact
    # differs: an OAuth code vs a SAML response / IdP id-token. Backends that
    # drive the redirect client-side (e.g. GCIP via its SDK) leave
    # `authorization_url` unimplemented and only handle the callback.
    # ------------------------------------------------------------------

    def authorization_url(
        self,
        *,
        provider: str | AuthProvider | None = None,
        organization: str | None = None,
        connection: str | None = None,
        domain: str | None = None,
        state: str,
        redirect_uri: str | None = None,
        scopes: list[str] | None = None,
    ) -> str:
        """Build the redirect URL that begins a social-OAuth or SSO sign-in.

        Pass ``provider`` for social/OIDC; pass ``organization`` /
        ``connection`` / ``domain`` for an enterprise SSO connection. Only
        needed for backend-orchestrated redirects — client-SDK-driven backends
        build the URL themselves and skip this.
        """
        raise CapabilityNotSupportedError(
            "sso" if (organization or connection or domain) else "oauth"
        )

    async def sign_in_with_oauth(
        self,
        code: str,
        *,
        provider: str | AuthProvider | None = None,
        redirect_uri: str | None = None,
    ) -> AuthOutcome:
        """Exchange a social/OIDC authorization code for a session.

        Outcome status: ``COMPLETE``, ``MFA_REQUIRED``, ``INVALID_CREDENTIALS``
        (provider rejected the code), or ``ACCOUNT_DISABLED``.
        """
        raise CapabilityNotSupportedError("oauth")

    async def sign_in_with_sso(
        self, credential: str, *, redirect_uri: str | None = None
    ) -> AuthOutcome:
        """Complete enterprise SSO. JIT-provisions; the session is org-scoped.

        Outcome status: ``COMPLETE``, ``MFA_REQUIRED``, or
        ``INVALID_CREDENTIALS`` (the SSO assertion was rejected).
        """
        raise CapabilityNotSupportedError("sso")

    # ------------------------------------------------------------------
    # Tokens & account verification
    # ------------------------------------------------------------------

    async def refresh(self, refresh_token: str) -> AuthResult[TokenSet]:
        """Exchange a refresh token for a fresh token set (mobile longevity).

        On success ``result.value`` is the new :class:`TokenSet`. Expected
        failure (attached to ``result.error``): ``InvalidTokenError`` when the
        refresh token is unknown, expired, or revoked.
        """
        raise CapabilityNotSupportedError("token_refresh")

    async def request_password_reset(
        self, *, email: str, redirect_url: str | None = None
    ) -> None:
        """Begin password recovery.

        ``redirect_url`` is the link the reset email points back to — required
        by backends that send their own mail (native); hosted IdPs that send it
        themselves may ignore it.
        """
        raise CapabilityNotSupportedError("password")

    async def reset_password(
        self, token: str, new_password: str
    ) -> AuthResult[Identity]:
        """Complete a password reset.

        On success ``result.value`` is the updated :class:`Identity`. Expected
        failures (attached to ``result.error``): ``InvalidTokenError`` (reset
        token unknown or expired) and ``PasswordValidationError`` (new password
        fails policy).
        """
        raise CapabilityNotSupportedError("password")

    async def verify_email(self, token: str) -> AuthResult[Identity]:
        """Confirm an email address with a verification token.

        On success ``result.value`` is the updated :class:`Identity`. Expected
        failure (attached to ``result.error``): ``InvalidTokenError`` when the
        confirmation token is unknown or expired.
        """
        raise CapabilityNotSupportedError("password")

    async def send_verification(self, *, user_id: str) -> None:
        """(Re)send an email/phone verification challenge."""
        raise CapabilityNotSupportedError("password")

    # ------------------------------------------------------------------
    # MFA — step-up first-class  (supports_mfa)
    # ------------------------------------------------------------------

    async def enroll_factor(
        self, *, user_id: str, type: FactorType, phone: str | None = None
    ) -> Enrollment:
        """Begin enrolling a second factor (returns TOTP secret/QR, or sends SMS)."""
        raise CapabilityNotSupportedError("mfa")

    async def confirm_factor(self, *, factor_id: str, code: str) -> FactorInfo:
        """Confirm a factor enrollment by verifying the first code."""
        raise CapabilityNotSupportedError("mfa")

    async def list_factors(self, user_id: str) -> Sequence[FactorInfo]:
        """List a user's enrolled factors."""
        raise CapabilityNotSupportedError("mfa")

    async def remove_factor(self, *, factor_id: str) -> bool:
        """Unenroll a factor. Returns ``False`` if not found."""
        raise CapabilityNotSupportedError("mfa")

    async def challenge_factor(
        self, *, pending_token: str, factor_id: str
    ) -> Challenge:
        """Start a step-up challenge for a sign-in that returned MFA_REQUIRED."""
        raise CapabilityNotSupportedError("mfa")

    async def verify_factor(
        self, *, pending_token: str, challenge_id: str, code: str
    ) -> AuthOutcome:
        """Complete the step-up after an ``MFA_REQUIRED`` sign-in.

        Outcome status: ``COMPLETE`` (with tokens) or ``INVALID_TOKEN`` (wrong
        or expired code).
        """
        raise CapabilityNotSupportedError("mfa")

    # ------------------------------------------------------------------
    # Admin: users  (supports_user_admin)
    # ------------------------------------------------------------------

    async def create_user(
        self,
        *,
        email: str | None = None,
        phone: str | None = None,
        password: str | None = None,
        **kwargs: Any,
    ) -> Identity:
        """Admin-create a user (no sign-in flow)."""
        raise CapabilityNotSupportedError("user_admin")

    async def get_user(self, user_id: str) -> Identity:
        """Resolve a user by id (assertive primary-key lookup).

        Raises:
            UserNotFoundError: No user with that id.
        """
        raise CapabilityNotSupportedError("user_admin")

    async def find_user(
        self,
        *,
        user_id: str | None = None,
        email: str | None = None,
        phone: str | None = None,
    ) -> Identity | None:
        """Find a user by a unique key. Returns ``None`` if none matches.

        Provide exactly one of ``user_id`` / ``email`` / ``phone``. This is the
        nullable counterpart to :meth:`get_user` — same lookup, but absence is
        an expected answer ("is this email taken?", "does this possibly-stale
        id still resolve?") rather than an error. Prefer :meth:`get_user` when
        the id is trusted and expected to exist (so a miss surfaces as a bug).
        Within a tenant scope it searches only that tenant's pool.

        Raises:
            ValueError: Not exactly one lookup key was given.
        """
        raise CapabilityNotSupportedError("user_admin")

    async def list_users(
        self, *, limit: int = 50, cursor: str | None = None
    ) -> Page[Identity]:
        """List users (cursor-paginated — the IdP norm)."""
        raise CapabilityNotSupportedError("user_admin")

    async def update_user(self, *, user_id: str, **kwargs: Any) -> AuthResult[Identity]:
        """Update user fields.

        On success ``result.value`` is the updated :class:`Identity`. Expected
        failure (attached to ``result.error``): ``UserNotFoundError`` when no
        user has that id.
        """
        raise CapabilityNotSupportedError("user_admin")

    async def delete_user(self, user_id: str) -> bool:
        """Delete a user. Returns ``False`` if not found."""
        raise CapabilityNotSupportedError("user_admin")

    async def set_user_roles(self, *, user_id: str, roles: list[str]) -> Identity:
        """Set a user's roles (surfaced in tokens / ``Session.roles``)."""
        raise CapabilityNotSupportedError("user_admin")

    async def set_user_disabled(self, *, user_id: str, disabled: bool) -> Identity:
        """Disable (ban) or re-enable a user."""
        raise CapabilityNotSupportedError("user_admin")

    # ------------------------------------------------------------------
    # Admin: sessions  (supports_sessions)
    # ------------------------------------------------------------------

    async def list_sessions(
        self, *, user_id: str | None = None, limit: int = 50, cursor: str | None = None
    ) -> Page[Session]:
        """List active sessions, optionally filtered by user."""
        raise CapabilityNotSupportedError("sessions")

    async def revoke_session(self, session_id: str) -> AuthResult[bool]:
        """Revoke a single session.

        On success ``result.value`` is ``True`` if a session was revoked,
        ``False`` if none matched. Expected failures are attached to
        ``result.error``.
        """
        raise CapabilityNotSupportedError("sessions")

    async def revoke_all_sessions(self, user_id: str) -> AuthResult[bool]:
        """Revoke all of a user's sessions / refresh tokens (global sign-out).

        On success ``result.value`` is ``True``. Expected failure (attached to
        ``result.error``): ``UserNotFoundError`` when no user has that id.
        """
        raise CapabilityNotSupportedError("sessions")

    # ------------------------------------------------------------------
    # B2B: organizations & membership  (supports_orgs)
    # ------------------------------------------------------------------

    async def create_org(
        self, *, name: str, slug: str, creator_id: str, **kwargs: Any
    ) -> AuthResult[Org]:
        """Create an org; the creator becomes owner.

        On success, ``result.value`` is the new :class:`Org`. Expected failure:
        ``OrgSlugConflictError`` (slug already taken). System failures still
        raise.
        """
        raise CapabilityNotSupportedError("orgs")

    # Org-addressing convention: every org-scoped method takes ``org_id`` and
    # ``slug`` as optional keywords — pass exactly one. A request usually has
    # the slug (it's in the URL), rarely the id, so slug is the primary handle;
    # backends select on whichever was given (no resolve round-trip). Persisted
    # references and FKs still use the stable id (returned on :class:`Org`).

    @staticmethod
    def _check_org_ref(
        org_id: str | None, slug: str | None, *, allow_clear: bool = False
    ) -> None:
        """Validate org addressing: exactly one of ``org_id`` / ``slug``.

        With ``allow_clear=True`` (the :meth:`set_active_org` clear case), passing
        neither is also valid and means "no active org".
        """
        if allow_clear and org_id is None and slug is None:
            return
        if (org_id is None) == (slug is None):
            raise ValueError("Provide exactly one of org_id= or slug=")

    async def get_org(
        self, *, org_id: str | None = None, slug: str | None = None
    ) -> AuthResult[Org]:
        """Get an org by id or slug (pass exactly one).

        Expected failure: ``OrgNotFoundError`` when nothing matches.
        """
        raise CapabilityNotSupportedError("orgs")

    async def list_orgs(
        self, *, user_id: str | None = None, limit: int = 50, cursor: str | None = None
    ) -> Page[Org]:
        """List orgs (cursor-paginated), optionally scoped to a member."""
        raise CapabilityNotSupportedError("orgs")

    async def update_org(
        self,
        *,
        org_id: str | None = None,
        slug: str | None = None,
        name: str | None = None,
        new_slug: str | None = None,
        **kwargs: Any,
    ) -> AuthResult[Org]:
        """Update an org's fields. Address by ``org_id`` or ``slug`` (one).

        ``name`` / ``new_slug`` are the new values to assign (``new_slug`` is
        spelled distinctly so it never collides with the addressing ``slug``).
        Expected failures: ``OrgNotFoundError``, ``OrgSlugConflictError``.
        """
        raise CapabilityNotSupportedError("orgs")

    async def delete_org(
        self, *, org_id: str | None = None, slug: str | None = None
    ) -> bool:
        """Delete an org (by id or slug) and its memberships. ``False`` if absent."""
        raise CapabilityNotSupportedError("orgs")

    async def add_member(
        self,
        *,
        user_id: str,
        org_id: str | None = None,
        slug: str | None = None,
        role: str = "member",
    ) -> AuthResult[OrgMember]:
        """Add a user to an org (by id or slug).

        Expected failures: ``OrgNotFoundError``, ``MemberAlreadyExistsError``.
        """
        raise CapabilityNotSupportedError("orgs")

    async def update_member(
        self,
        *,
        user_id: str,
        role: str,
        org_id: str | None = None,
        slug: str | None = None,
    ) -> AuthResult[OrgMember]:
        """Change a member's role in an org (by id or slug).

        Expected failure: ``OrgMemberNotFoundError``.
        """
        raise CapabilityNotSupportedError("orgs")

    async def remove_member(
        self, *, user_id: str, org_id: str | None = None, slug: str | None = None
    ) -> AuthResult[bool]:
        """Remove a member from an org (by id or slug).

        ``result.value`` is ``True`` if a row was removed, ``False`` if the user
        wasn't a member. Expected failure: ``LastOwnerError``.
        """
        raise CapabilityNotSupportedError("orgs")

    async def list_members(
        self,
        *,
        org_id: str | None = None,
        slug: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> Page[OrgMember]:
        """List members of an org (by id or slug; cursor-paginated)."""
        raise CapabilityNotSupportedError("orgs")

    async def set_active_org(
        self, *, session_id: str, org_id: str | None = None, slug: str | None = None
    ) -> AuthResult[TokenSet]:
        """Switch a session's active org (by id or slug); reissues tokens.

        Pass neither ``org_id`` nor ``slug`` to clear the active org. On success
        ``result.value`` is the reissued :class:`TokenSet`. Expected failure
        (attached to ``result.error``): ``OrgMemberNotFoundError`` when the user
        isn't a member of the target org.
        """
        raise CapabilityNotSupportedError("orgs")

    # ------------------------------------------------------------------
    # B2B: invitations  (supports_invitations)
    # ------------------------------------------------------------------

    async def invite_to_org(
        self,
        *,
        email: str,
        org_id: str | None = None,
        slug: str | None = None,
        role: str = "member",
    ) -> AuthResult[Invitation]:
        """Invite an email address to join an org (by id or slug).

        Expected failure: ``OrgNotFoundError``.
        """
        raise CapabilityNotSupportedError("invitations")

    async def accept_invitation(self, *, invitation_token: str) -> AuthOutcome:
        """Accept an invite; signs the user in (JIT-creating them if new)."""
        raise CapabilityNotSupportedError("invitations")

    async def revoke_invitation(self, *, invitation_id: str) -> bool:
        """Revoke a pending invite. Returns ``False`` if not found."""
        raise CapabilityNotSupportedError("invitations")

    async def list_invitations(
        self,
        *,
        org_id: str | None = None,
        slug: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> Page[Invitation]:
        """List an org's invitations (by id or slug; cursor-paginated)."""
        raise CapabilityNotSupportedError("invitations")

    # ------------------------------------------------------------------
    # Auto-pagination — drain a cursor-paginated list_* into one async stream.
    #
    # `async for u in auth.iter_users(): ...` instead of hand-rolling the
    # cursor loop. Each is a one-line wrapper over the private `_paginate`
    # drainer at the bottom of the module.
    # ------------------------------------------------------------------

    def iter_users(self, *, page_size: int = 100) -> AsyncIterator[Identity]:
        """Iterate every user, fetching pages as needed."""
        return _paginate(self.list_users, page_size=page_size)

    def iter_sessions(
        self, *, user_id: str | None = None, page_size: int = 100
    ) -> AsyncIterator[Session]:
        """Iterate every session, optionally filtered by user."""
        return _paginate(self.list_sessions, page_size=page_size, user_id=user_id)

    def iter_orgs(
        self, *, user_id: str | None = None, page_size: int = 100
    ) -> AsyncIterator[Org]:
        """Iterate every org, optionally scoped to a member."""
        return _paginate(self.list_orgs, page_size=page_size, user_id=user_id)

    def iter_members(
        self,
        *,
        org_id: str | None = None,
        slug: str | None = None,
        page_size: int = 100,
    ) -> AsyncIterator[OrgMember]:
        """Iterate every member of an org (by id or slug)."""
        return _paginate(
            self.list_members, page_size=page_size, org_id=org_id, slug=slug
        )

    def iter_invitations(
        self,
        *,
        org_id: str | None = None,
        slug: str | None = None,
        page_size: int = 100,
    ) -> AsyncIterator[Invitation]:
        """Iterate every invitation for an org (by id or slug)."""
        return _paginate(
            self.list_invitations, page_size=page_size, org_id=org_id, slug=slug
        )

    def iter_tenants(self, *, page_size: int = 100) -> AsyncIterator[Tenant]:
        """Iterate every tenant."""
        return _paginate(self.list_tenants, page_size=page_size)


async def _paginate[T](
    list_fn: Callable[..., Awaitable[Page[T]]], *, page_size: int, **kwargs: Any
) -> AsyncIterator[T]:
    """Drain a cursor-paginated ``list_*`` method into one async stream.

    The shared engine behind the ``iter_*`` methods: it walks the cursor and
    stops on ``has_more``, so the forgot-to-advance infinite-loop footgun lives
    in exactly one audited place. ``**kwargs`` threads each list method's
    non-paging arguments (``org_id``, ``user_id``) straight through.
    """
    cursor: str | None = None
    while True:
        page = await list_fn(limit=page_size, cursor=cursor, **kwargs)
        for item in page.items:
            yield item
        if not page.has_more:
            return
        cursor = page.next_cursor
