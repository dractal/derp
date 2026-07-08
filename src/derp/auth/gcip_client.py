"""Google Cloud Identity Platform (GCIP) authentication client.

GCIP is Google's CIAM product. It exposes the Identity Toolkit REST API at
``identitytoolkit.googleapis.com`` for user-facing operations (sign-up,
sign-in, password reset) authenticated via an API key, and admin operations
authenticated via a service-account-signed bearer token. It issues
RS256-signed JWTs verifiable against the secure-token JWKS endpoint.

This backend is single-tenant. Multi-tenancy is a future cross-cutting change
to :class:`BaseAuthClient`; the helpers here (``_make_identity``, the
project-scoped admin URL builder, and keyword-only internal args) are shaped so
a ``tenant_id`` can be threaded through later without breaking call sites.

Differences from the native backend worth calling out:

* GCIP sends its own magic-link and password-reset emails via
  ``accounts:sendOobCode`` — this client never needs an :class:`EmailClient`.
* GCIP has no server-side session store. ``Session`` is synthesised from
  verified ID-token claims; there is no derp-side session table. Per-session
  revocation and session listing are therefore unsupported (they inherit the
  base class's capability-not-supported default); use ``revoke_all_sessions``,
  which revokes all refresh tokens via the ``validSince`` field.
* GCIP has no organisation concept, so derp layers one: orgs, memberships, and
  invitations live in derp's own Postgres tables (the IdP owns users, derp owns
  the org graph). Org mutations are plain Postgres writes — they have no token
  side-effects. A session's single *active* org is carried by a derp-signed
  *pointer* minted by ``set_active_org`` (the Google ID token cannot hold a
  per-session claim — its custom claims are per-user). The pointer only names
  the org; it is delivered in the ``X-Org-Context`` header, and
  :meth:`authenticate` resolves the **current** role from Postgres on each
  request. So authority stays in the DB: a demotion or removal takes effect on
  the next request — no staleness window, no token to refresh — at the cost of
  one indexed membership read per request. See :meth:`BaseAuthClient.require_org`.

Token verification mirrors the WorkOS backend: GCIP issues RS256 tokens, so we
verify them locally against Google's public JWKS using :class:`AsyncJWKS`, which
fetches the signing keys with the shared async HTTP client and caches them with
a TTL. Between refetches ``verify_token`` is a local CPU operation with no
network round-trip — the sign-in and admin methods are the only network-bound
calls, as with any external IdP.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import secrets
import time
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import jwt as pyjwt

from derp.auth.aiojwks import AsyncJWKS
from derp.auth.base import (
    AuthOutcome,
    AuthResult,
    BaseAuthClient,
    Identity,
    Invitation,
    MFAStatus,
    Org,
    OrgMember,
    Page,
    Session,
    TokenSet,
)
from derp.auth.exceptions import (
    AuthBackendError,
    AuthNotConnectedError,
    EmailAlreadyExistsError,
    InvalidCredentialsError,
    InvalidTokenError,
    LastOwnerError,
    MemberAlreadyExistsError,
    OrgMemberNotFoundError,
    OrgNotFoundError,
    OrgSlugConflictError,
    PasswordValidationError,
    UserNotFoundError,
)
from derp.auth.models import (
    AuthInvitation,
    AuthOrganization,
    AuthProvider,
    AuthRequest,
    AuthStatus,
    GCIPOrgMember,
    InvitationState,
)
from derp.config import GCIPConfig
from derp.kv.base import KVClient
from derp.orm import DatabaseEngine

logger = logging.getLogger(__name__)

# Refresh the admin bearer token this many seconds before its stated expiry so
# an in-flight request never races the boundary.
_ADMIN_TOKEN_SKEW_SECONDS = 300

# Default OAuth token endpoint for the service-account JWT-bearer grant. The
# service-account JSON usually carries its own ``token_uri``; this is the
# fallback.
_DEFAULT_TOKEN_URI = "https://oauth2.googleapis.com/token"

# Scope required to call the Identity Toolkit admin endpoints.
_ADMIN_SCOPE = "https://www.googleapis.com/auth/identitytoolkit"

# Active-org credential delivery: clients resend the credential returned by
# `set_active_org` in this header on every request, attached by the same client
# code that already sets `Authorization: Bearer <ID token>`. Header-only (no
# cookie): identity is itself a Bearer header here (Firebase SDK stores tokens in
# IndexedDB, not cookies), and the credential is only ever honoured next to a
# valid Bearer token — so a cookie would add no reach, only cookies-off and
# third-party-cookie failure modes.
_ORG_CONTEXT_HEADER = "X-Org-Context"


@dataclass
class _Connection:
    """Live state for a connected GCIP client.

    Everything an operation needs once ``connect()`` has run is bundled here,
    so call sites do a single ``_ensure_connected()`` check instead of
    None-guarding the HTTP client, JWKS client, service-account fields, and
    admin token separately. ``access_token``/``token_expires_at`` are the
    minted admin bearer token, refreshed in place by ``_ensure_admin_token``.
    """

    http: httpx.AsyncClient
    jwks: AsyncJWKS
    # Service-account fields are None on a verify-only deployment (no
    # ``service_account_json`` configured); admin calls guard on them.
    client_email: str | None
    private_key: str | None
    private_key_id: str | None
    token_uri: str
    access_token: str | None = None
    token_expires_at: float = 0.0
    # Serializes admin-token minting so concurrent requests on a cold/expired
    # token don't each mint one (thundering herd).
    admin_token_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class GCIPAuthClient(BaseAuthClient):
    """Google Cloud Identity Platform-backed authentication client.

    User-facing flows (sign-up, sign-in, magic link, OAuth, password reset)
    go through the API-key-authenticated Identity Toolkit endpoints. User
    administration (lookup, list, update, delete, token revocation) goes
    through the service-account-authenticated admin endpoints. JWT
    verification is performed locally against the secure-token JWKS.

    GCIP uses page-token pagination, so ``list_users`` is cursor-paginated:
    its ``cursor`` is the opaque ``nextPageToken`` from the previous page.

    **Frontend contract.** Identity comes from the Firebase JS SDK (tokens live
    in IndexedDB, not cookies) and rides the ``Authorization: Bearer`` header.
    The active org is a separate, derp-issued credential from ``set_active_org``
    that the client resends in the ``X-Org-Context`` header — attached by the
    same fetch interceptor that sets the bearer token (header-only; no cookie):

    .. code-block:: javascript

        import { getAuth, signInWithEmailAndPassword } from "firebase/auth";

        const auth = getAuth();
        await signInWithEmailAndPassword(auth, email, password);

        // The active-org pointer is held in memory and resent on every call. It
        // is stable (no expiry) and only changes when the user switches org — so
        // there is nothing to refresh. X-Org-Context is only sent once an org is
        // active, so identity-only calls omit it.
        let orgContext = null;
        async function apiRequest(path, init = {}) {
          const idToken = await auth.currentUser.getIdToken(); // SDK auto-refreshes
          const headers = { ...init.headers, Authorization: `Bearer ${idToken}` };
          if (orgContext) headers["X-Org-Context"] = orgContext;
          return fetch(path, { ...init, headers });
        }

        // List the orgs the user belongs to — identity-only, since you call it
        // BEFORE choosing an active org. Backed by list_orgs(user_id=...) on the
        // server; use it to populate an org switcher.
        async function loadOrgs() {
          const r = await apiRequest("/api/auth/orgs");
          return (await r.json()).items; // [{ id, slug, name }, ...]
        }

        // Pick one → derp returns the signed active-org pointer. Role/membership
        // changes are picked up server-side per request, so no re-mint is needed
        // unless the user switches to a different org.
        async function switchOrg(slug) {
          const r = await apiRequest("/api/auth/set-active-org", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ slug }),
          });
          orgContext = (await r.json()).access_token;
        }
    """

    supports_password = True
    supports_oauth = True
    supports_magic_link = True
    supports_user_admin = True
    supports_orgs = True
    supports_invitations = True

    IDENTITY_TOOLKIT_BASE = "https://identitytoolkit.googleapis.com/v1"
    SECURE_TOKEN_BASE = "https://securetoken.googleapis.com/v1"
    JWKS_URL = (
        "https://www.googleapis.com/service_accounts/v1/jwk/"
        "securetoken@system.gserviceaccount.com"
    )

    def __init__(self, config: GCIPConfig) -> None:
        self._config = config
        self._project_id = config.project_id
        self._expected_issuer = f"https://securetoken.google.com/{config.project_id}"
        self._expected_audience = config.project_id
        self._database: DatabaseEngine | None = None
        self._kv_client: KVClient | None = None
        self._conn: _Connection | None = None

    # ------------------------------------------------------------------
    # Lifecycle / infrastructure wiring
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Build the connection state: HTTP client, JWKS client, SA credentials.

        GCIP spans several hosts (Identity Toolkit, secure-token, the OAuth
        token endpoint), so the HTTP client carries no ``base_url`` and every
        call uses a fully-qualified URL. :class:`AsyncJWKS` shares that HTTP
        client to fetch and cache the secure-token signing keys.

        The service-account JSON is optional (a verify-only deployment omits
        it); when present it is parsed here so a malformed key fails at startup
        rather than on the first admin call (the ``$ENV`` ref is already resolved
        by the config loader, so this is literal JSON). When absent, the SA
        fields stay ``None`` and admin methods raise a clear error at the point
        of use.
        """
        if self._conn is not None:
            return
        client_email: str | None = None
        private_key: str | None = None
        private_key_id: str | None = None
        token_uri = _DEFAULT_TOKEN_URI
        if self._config.service_account_json is not None:
            try:
                sa = json.loads(self._config.service_account_json)
            except (ValueError, json.JSONDecodeError) as e:
                raise AuthBackendError(
                    "GCIP service_account_json is not valid JSON"
                ) from e
            if (
                not isinstance(sa, dict)
                or not {
                    "client_email",
                    "private_key",
                }
                <= sa.keys()
            ):
                raise AuthBackendError(
                    "GCIP service_account_json is missing required fields "
                    "(client_email, private_key)"
                )
            client_email = sa["client_email"]
            private_key = sa["private_key"]
            private_key_id = sa.get("private_key_id")
            token_uri = sa.get("token_uri", _DEFAULT_TOKEN_URI)
        http = httpx.AsyncClient(timeout=30.0)
        self._conn = _Connection(
            http=http,
            jwks=AsyncJWKS(self.JWKS_URL, http),
            client_email=client_email,
            private_key=private_key,
            private_key_id=private_key_id,
            token_uri=token_uri,
        )

    async def disconnect(self) -> None:
        if self._conn is not None:
            await self._conn.http.aclose()
            self._conn = None

    def set_db(self, db: DatabaseEngine | None) -> None:
        self._database = db

    def set_kv(self, kv: KVClient | None) -> None:
        """Wire a KV client used to cache the per-request membership lookup.

        Optional. When set, ``get_member_role`` checks the KV first and falls
        back to Postgres on a miss, bounded by ``GCIPConfig.role_cache_ttl_seconds``.
        Without a KV the lookup always hits Postgres (still indexed, still fast).
        """
        self._kv_client = kv

    def _ensure_connected(self) -> _Connection:
        if self._conn is None:
            raise AuthNotConnectedError()
        return self._conn

    # ------------------------------------------------------------------
    # Feature-credential guards
    #
    # Only ``project_id`` is required to verify tokens; each of the credentials
    # below gates one feature and is validated here, at the point of use, so a
    # verify-only deployment can omit them and still call ``authenticate``.
    # ------------------------------------------------------------------

    def _require_api_key(self) -> str:
        """Return the Web API key, or explain that sign-in needs it."""
        key = self._config.public_api_key
        if key is None:
            raise AuthBackendError(
                "GCIP sign-in, sign-up, OAuth, magic-link, password-reset and "
                "token-refresh operations require `public_api_key` in "
                "[auth.gcip]. Omit them by signing in client-side with the "
                "Firebase JS SDK and using this backend only to verify tokens."
            )
        return key

    def _require_org_secret(self) -> str:
        """Return the active-org HMAC secret, or explain that orgs need it."""
        secret = self._config.org_context_secret
        if secret is None:
            raise AuthBackendError(
                "GCIP active-org features (set_active_org and X-Org-Context "
                "resolution) require `org_context_secret` (>= 32 chars) in "
                "[auth.gcip]."
            )
        return secret

    # ------------------------------------------------------------------
    # Error helpers
    # ------------------------------------------------------------------

    def _error_code(self, resp: httpx.Response) -> str | None:
        """Extract GCIP's leading error code from an error response.

        GCIP returns ``{"error": {"code": 400, "message": "EMAIL_EXISTS"}}``.
        Some messages append a human-readable detail after the code
        (``"WEAK_PASSWORD : Password should be at least 6 characters"``); we
        return just the leading token.
        """
        try:
            body = resp.json()
        except (ValueError, json.JSONDecodeError):
            return None
        if not isinstance(body, dict):
            return None
        err = body.get("error")
        if isinstance(err, dict):
            message = err.get("message")
            if isinstance(message, str):
                return message.split(":")[0].strip().split(" ")[0]
        return None

    def _backend_error(self, op: str, resp: httpx.Response) -> AuthBackendError:
        """Wrap an unrecognised non-success response in AuthBackendError."""
        body = resp.text[:500]
        return AuthBackendError(f"GCIP {op} failed: HTTP {resp.status_code} {body}")

    # ------------------------------------------------------------------
    # Service-account admin token (hand-rolled JWT-bearer grant)
    # ------------------------------------------------------------------

    async def _ensure_admin_token(
        self, conn: _Connection, *, force: bool = False
    ) -> str:
        """Return a valid admin bearer token, minting a new one if required.

        Mints via the service-account JWT-bearer grant when the cached token
        is missing, expired, or ``force``d (after a 401), updating the token
        state on *conn* in place. The cached token is reused until shortly
        before its stated expiry.

        Minting is serialized by ``conn.admin_token_lock`` with a double-checked
        read, so N concurrent requests on a cold/expired token (or N requests
        that all 401 at once) mint exactly one token and the rest reuse it.
        """
        # Admin endpoints need a service account; a verify-only deployment has
        # none, so fail here (covers every admin method — they all route through
        # ``_admin_request`` → here) rather than emitting an opaque token error.
        if conn.client_email is None or conn.private_key is None:
            raise AuthBackendError(
                "GCIP user-administration operations (get/find/list/update/"
                "delete user and revoke_all_sessions) require "
                "`service_account_json` in [auth.gcip]."
            )
        client_email, private_key = conn.client_email, conn.private_key

        # Fast path: a still-valid cached token needs no lock.
        if (
            not force
            and conn.access_token is not None
            and time.time() < conn.token_expires_at
        ):
            return conn.access_token

        # `stale` is the token we believe needs replacing. On a forced refresh
        # (after a 401) we only re-mint if it is still the current token —
        # otherwise another coroutine already rotated past it and we reuse theirs.
        stale = conn.access_token
        async with conn.admin_token_lock:
            # Re-check under the lock: another coroutine may have minted while we
            # waited. On a forced refresh, only re-mint if the token we 401'd on
            # is still current (else reuse the one they already rotated to).
            cached = conn.access_token
            if (
                cached is not None
                and time.time() < conn.token_expires_at
                and (not force or cached != stale)
            ):
                return cached

            now = int(time.time())
            assertion = pyjwt.encode(
                {
                    "iss": client_email,
                    "sub": client_email,
                    "aud": conn.token_uri,
                    "scope": _ADMIN_SCOPE,
                    "iat": now,
                    "exp": now + 3600,
                },
                private_key,
                algorithm="RS256",
                headers={"kid": conn.private_key_id},
            )
            try:
                resp = await conn.http.post(
                    conn.token_uri,
                    data={
                        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                        "assertion": assertion,
                    },
                )
            except httpx.HTTPError as e:
                raise AuthBackendError(f"GCIP admin token request failed: {e}") from e
            if not resp.is_success:
                raise self._backend_error("admin token exchange", resp)

            data = resp.json()
            access_token = data.get("access_token")
            if not access_token:
                raise AuthBackendError("GCIP admin token response missing access_token")
            conn.access_token = access_token
            conn.token_expires_at = (
                time.time()
                + int(data.get("expires_in", 3600))
                - _ADMIN_TOKEN_SKEW_SECONDS
            )
        return access_token

    # ------------------------------------------------------------------
    # HTTP request helpers (the only places admin/public calls are made)
    # ------------------------------------------------------------------

    async def _admin_request(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> httpx.Response:
        """Make a service-account-authenticated admin call.

        All admin HTTP goes through here: it builds the project-scoped URL,
        attaches the bearer token, and refreshes the token once on a 401.
        Returns the raw response so callers can map GCIP error codes
        (mirroring the Supabase client's pattern).
        """
        conn = self._ensure_connected()
        url = f"{self.IDENTITY_TOOLKIT_BASE}/projects/{self._project_id}{path}"
        token = await self._ensure_admin_token(conn)
        try:
            resp = await conn.http.request(
                method,
                url,
                json=json,
                params=params,
                headers={"Authorization": f"Bearer {token}"},
            )
            if resp.status_code == 401:
                # Token rejected mid-flight — force a refresh and retry once.
                token = await self._ensure_admin_token(conn, force=True)
                resp = await conn.http.request(
                    method,
                    url,
                    json=json,
                    params=params,
                    headers={"Authorization": f"Bearer {token}"},
                )
        except httpx.HTTPError as e:
            raise AuthBackendError(f"GCIP admin request to {path} failed: {e}") from e
        return resp

    async def _public_request(
        self, endpoint: str, json: dict[str, Any]
    ) -> httpx.Response:
        """Make an API-key-authenticated user-facing call."""
        conn = self._ensure_connected()
        url = f"{self.IDENTITY_TOOLKIT_BASE}/{endpoint}?key={self._require_api_key()}"
        try:
            resp = await conn.http.post(url, json=json)
        except httpx.HTTPError as e:
            raise AuthBackendError(f"GCIP request to {endpoint} failed: {e}") from e
        return resp

    # ------------------------------------------------------------------
    # Response mapping (single source of truth)
    # ------------------------------------------------------------------

    @staticmethod
    def _ms_to_datetime(value: Any) -> datetime | None:
        """Convert a GCIP epoch-milliseconds string/int to a datetime."""
        if value in (None, ""):
            return None
        try:
            return datetime.fromtimestamp(int(value) / 1000, tz=UTC)
        except (ValueError, TypeError):
            return None

    def _make_identity(self, gcip_user: dict[str, Any]) -> Identity:
        """Build an ``Identity`` from a GCIP ``accounts:lookup`` user record.

        Single source of truth for user mapping. Custom claims live in the
        ``customAttributes`` JSON string; ``role`` is read from there (matching
        how token verification reads ``role`` off the token), and the remaining
        custom attributes flow into ``metadata``.
        """
        custom = self._parse_custom_attributes(gcip_user.get("customAttributes"))

        email_verified = bool(gcip_user.get("emailVerified", False))
        created_at = self._ms_to_datetime(gcip_user.get("createdAt")) or datetime.now(
            UTC
        )
        last_sign_in_at = self._ms_to_datetime(gcip_user.get("lastLoginAt"))

        updated_at = created_at
        last_refresh = gcip_user.get("lastRefreshAt")
        if isinstance(last_refresh, str) and last_refresh:
            try:
                updated_at = datetime.fromisoformat(last_refresh.replace("Z", "+00:00"))
            except ValueError:
                pass

        # `customAttributes` holds derp-reserved keys (role, etc.) alongside the
        # user-facing keys. Keep the reserved keys out of user metadata. derp no
        # longer writes org data here — the active org rides in a separate
        # org-context credential, not a custom claim.
        role = custom.get("role", "default")
        consumed = {"role", "is_superuser", "first_name", "last_name"}
        metadata = {k: v for k, v in custom.items() if k not in consumed}

        return Identity(
            id=gcip_user.get("localId", ""),
            tenant_id=None,
            email=gcip_user.get("email") or None,
            email_verified=email_verified,
            phone=None,
            phone_verified=False,
            is_anonymous=False,
            disabled=bool(gcip_user.get("disabled", False)),
            roles=(role,),
            created_at=created_at,
            updated_at=updated_at,
            last_sign_in_at=last_sign_in_at,
            metadata=metadata,
        )

    @staticmethod
    def _parse_custom_attributes(raw: Any) -> dict[str, Any]:
        """Parse a GCIP ``customAttributes`` JSON string into a dict ({} on miss)."""
        if isinstance(raw, str) and raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return parsed
            except (ValueError, json.JSONDecodeError):
                pass
        return {}

    async def _auth_outcome_from_response(self, data: dict[str, Any]) -> AuthOutcome:
        """Turn an Identity Toolkit auth response into a completed ``AuthOutcome``.

        Fetches the full user record via the API-key ``accounts:lookup``
        endpoint (no admin token needed) so the outcome carries a complete
        ``Identity`` from the single ``_make_identity`` mapper.
        """
        resp = await self._public_request(
            "accounts:lookup", {"idToken": data["idToken"]}
        )
        if not resp.is_success:
            raise self._backend_error("accounts:lookup", resp)
        users = resp.json().get("users") or []
        if not users:
            raise AuthBackendError("GCIP accounts:lookup returned no user")

        expires_in = int(data.get("expiresIn", 3600))
        id_token = data.get("idToken", "")
        return AuthOutcome(
            status=AuthStatus.COMPLETE,
            identity=self._make_identity(users[0]),
            tokens=TokenSet(
                access_token=id_token,
                refresh_token=data.get("refreshToken") or None,
                id_token=id_token,
                token_type="bearer",
                expires_in=expires_in,
                expires_at=datetime.now(UTC) + timedelta(seconds=expires_in),
            ),
        )

    # ------------------------------------------------------------------
    # Runtime / relying party
    # ------------------------------------------------------------------

    async def verify_token(self, token: str) -> Session | None:
        """Verify a GCIP ID token and return its session.

        Returns ``None`` when the token fails verification — a bad token maps
        to an unauthenticated request, not an error. The RS256 signature is
        checked against Google's JWKS (fetched and cached off the event loop by
        :class:`AsyncJWKS`), along with ``iss``, ``aud``, ``exp``, and a
        future-``iat`` guard.
        """
        conn = self._ensure_connected()
        try:
            signing_key = await conn.jwks.get_signing_key(token)
            claims = pyjwt.decode(
                token,
                signing_key,
                algorithms=["RS256"],
                audience=self._expected_audience,
                issuer=self._expected_issuer,
            )
        except (
            pyjwt.exceptions.PyJWKClientError,
            pyjwt.exceptions.InvalidTokenError,
        ):
            return None

        # pyjwt validates exp/aud/iss/nbf but not a future iat; reject tokens
        # minted in the future (small leeway for clock skew).
        iat = claims.get("iat")
        if isinstance(iat, (int, float)) and iat > time.time() + 60:
            return None

        # GCIP has no session id, so session_id falls back to the user id;
        # role comes from a custom claim on the token.
        user_id = claims.get("sub") or claims.get("user_id", "")
        issued_at = self._ms_to_datetime(int(iat) * 1000) if iat is not None else None
        return Session(
            user_id=user_id,
            session_id=user_id,
            tenant_id=None,
            # Identity only — the active org is layered on by `authenticate`
            # from the org-context credential (X-Org-Context header).
            org_id=None,
            org_role=None,
            roles=(claims.get("role", "default"),),
            scopes=(),
            is_anonymous=False,
            mfa=MFAStatus(enrolled=False, satisfied=False, factor_types=()),
            issued_at=issued_at or datetime.now(UTC),
            expires_at=datetime.fromtimestamp(claims["exp"], tz=UTC),
            claims=claims,
        )

    async def authenticate(self, request: AuthRequest) -> Session | None:
        """Verify the Google ID token, then layer on the active org.

        Identity comes from the Google ID token (the base ``Bearer`` helper).
        The active org rides in a derp-signed *pointer* the client got from
        :meth:`set_active_org`, resent in the ``X-Org-Context`` header. The
        pointer only names the org; the **current** role is resolved here from
        derp's Postgres tables (one indexed read), so a since-removed member
        binds no org and a demoted member gets their new role immediately — no
        staleness window, nothing to refresh. A missing/invalid/foreign pointer,
        or a user who is no longer a member, yields an identity-only session — it
        never raises.
        """
        session = await super().authenticate(request)
        if session is None:
            return None
        raw = request.headers.get(_ORG_CONTEXT_HEADER)
        if not raw:
            return session
        # Active orgs are opt-in: with no `org_context_secret` configured there
        # is nothing to verify the pointer against, so ignore any header and
        # stay identity-only rather than raise (authenticate never raises on a
        # client-supplied pointer).
        secret = self._config.org_context_secret
        if secret is None:
            logger.debug(
                "org-context header present but org_context_secret is unset; "
                "ignoring for user %s",
                session.user_id,
            )
            return session
        # Verify the signed pointer (``org_id:slug:sig``, HMAC bound to the user
        # — see set_active_org for the matching format). A malformed or foreign
        # pointer is ignored, yielding an identity-only session. Each branch
        # logs at DEBUG so prod can diagnose "why is the session org-less" —
        # never the HMAC itself.
        parts = raw.split(":", 2)
        if len(parts) != 3:
            logger.debug(
                "org-context pointer malformed for user %s; ignoring", session.user_id
            )
            return session
        org_id, slug, sig = parts
        expected = hmac.new(
            secret.encode(),
            f"{session.user_id}:{org_id}:{slug}".encode(),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(sig, expected):
            logger.debug(
                "org-context HMAC mismatch for user %s on org slug %r; "
                "ignoring (foreign pointer or rotated secret)",
                session.user_id,
                slug,
            )
            return session
        # Authority is the DB, not the pointer: resolve the *current* role. A
        # since-removed member resolves to None → no active org is bound.
        role = await self.get_member_role(user_id=session.user_id, org=slug)
        if role is None:
            logger.debug(
                "org-context references org %r but user %s is no longer a member; "
                "binding no active org",
                slug,
                session.user_id,
            )
            return session
        # Permissions are the role's static grants from config; empty when the
        # app hasn't set up role_permissions (has()/require_permission then 403).
        permissions = tuple(self._config.role_permissions.get(role, ()))
        return replace(
            session,
            org_id=org_id,
            org_slug=slug,
            org_role=role,
            permissions=permissions,
        )

    # ------------------------------------------------------------------
    # User management (admin API)
    # ------------------------------------------------------------------

    async def get_user(self, user_id: str) -> Identity:
        resp = await self._admin_request(
            "POST", "/accounts:lookup", json={"localId": [str(user_id)]}
        )
        if not resp.is_success:
            raise self._backend_error("get_user", resp)
        users = resp.json().get("users") or []
        if not users:
            raise UserNotFoundError(f"User {str(user_id)!r} not found")
        return self._make_identity(users[0])

    async def find_user(
        self,
        *,
        user_id: str | None = None,
        email: str | None = None,
        phone: str | None = None,
    ) -> Identity | None:
        """Find a user by localId or email via accounts:lookup; ``None`` if absent.

        Provide exactly one of ``user_id`` / ``email``. GCIP's
        ``accounts:lookup`` does not support phone-number lookup, so ``phone``
        is rejected.
        """
        if phone is not None:
            raise ValueError("GCIP does not support phone-number lookup")
        if len([k for k in (user_id, email, phone) if k is not None]) != 1:
            raise ValueError("Provide exactly one of user_id=, email=, or phone=")
        payload = (
            {"localId": [str(user_id)]} if user_id is not None else {"email": [email]}
        )
        resp = await self._admin_request("POST", "/accounts:lookup", json=payload)
        if not resp.is_success:
            raise self._backend_error("find_user", resp)
        users = resp.json().get("users") or []
        return self._make_identity(users[0]) if users else None

    async def list_users(
        self,
        *,
        limit: int = 50,
        cursor: str | None = None,
        batch_max_results: int = 1000,
    ) -> Page[Identity]:
        """List users using GCIP's native page tokens.

        ``cursor`` is the opaque ``nextPageToken`` from a previous result;
        the returned ``Page.next_cursor`` feeds the next call.
        """
        params: dict[str, Any] = {"maxResults": min(limit, batch_max_results)}
        if cursor is not None:
            params["nextPageToken"] = cursor
        resp = await self._admin_request("GET", "/accounts:batchGet", params=params)
        if not resp.is_success:
            raise self._backend_error("list_users", resp)
        body = resp.json()
        items = [self._make_identity(u) for u in (body.get("users") or [])]
        next_cursor = body.get("nextPageToken")
        return Page(
            items=items,
            next_cursor=next_cursor,
            has_more=bool(next_cursor),
        )

    async def update_user(
        self,
        *,
        user_id: str,
        email: str | None = None,
        **kwargs: Any,
    ) -> AuthResult[Identity]:
        body: dict[str, Any] = {"localId": str(user_id)}
        if email is not None:
            body["email"] = email
        if kwargs.get("image_url") is not None:
            body["photoUrl"] = kwargs["image_url"]

        display_name = kwargs.get("display_name")
        if display_name is None:
            parts = [kwargs.get("first_name"), kwargs.get("last_name")]
            joined = " ".join(p for p in parts if p)
            display_name = joined or None
        if display_name is not None:
            body["displayName"] = display_name

        # role / is_superuser / first_name / last_name are stored as GCIP custom
        # attributes, a single shared bag. Read-merge so we don't clobber derp's
        # org-claim map (or any other key) that shares it.
        custom_updates = {
            key: kwargs[key]
            for key in ("role", "is_superuser", "first_name", "last_name")
            if kwargs.get(key) is not None
        }
        if custom_updates:
            lookup = await self._admin_request(
                "POST", "/accounts:lookup", json={"localId": [str(user_id)]}
            )
            users = (lookup.json().get("users") or []) if lookup.is_success else []
            custom = self._parse_custom_attributes(
                users[0].get("customAttributes") if users else None
            )
            custom.update(custom_updates)
            body["customAttributes"] = json.dumps(custom)

        resp = await self._admin_request("POST", "/accounts:update", json=body)
        if resp.status_code == 400 and self._error_code(resp) in {
            "USER_NOT_FOUND",
            "EMAIL_NOT_FOUND",
        }:
            return AuthResult(
                error=UserNotFoundError(f"User {str(user_id)!r} not found")
            )
        if not resp.is_success:
            raise self._backend_error("update_user", resp)
        # The update response is partial; re-read the canonical record so the
        # returned Identity goes through the single mapper with full fields.
        return AuthResult(value=await self.get_user(user_id))

    async def delete_user(self, user_id: str) -> bool:
        resp = await self._admin_request(
            "POST", "/accounts:delete", json={"localId": str(user_id)}
        )
        if resp.status_code == 400 and self._error_code(resp) == "USER_NOT_FOUND":
            return False
        if not resp.is_success:
            raise self._backend_error("delete_user", resp)
        # The IdP owns users but derp owns the org layer, so membership rows have
        # no FK to cascade. Delete them here so a deleted user's roles cannot be
        # inherited if GCIP later reuses the localId. (Invitations are email-
        # scoped, not user-scoped, so they are intentionally left untouched.)
        if self._database is not None:
            await (
                self._database.delete(GCIPOrgMember)
                .where(GCIPOrgMember.user_id == str(user_id))
                .execute()
            )
        return True

    # ------------------------------------------------------------------
    # Sessions
    # ------------------------------------------------------------------

    async def revoke_all_sessions(self, user_id: str) -> AuthResult[bool]:
        """Revoke all refresh tokens for a user (global sign-out).

        Sets ``validSince`` to now; GCIP rejects refresh tokens issued before
        that instant (and ID tokens become unverifiable once they expire).
        GCIP has no per-session revocation (``revoke_session``) and no
        server-side session list (``list_sessions``), so those inherit the
        base class's capability-not-supported default.
        """
        resp = await self._admin_request(
            "POST",
            "/accounts:update",
            json={"localId": str(user_id), "validSince": str(int(time.time()))},
        )
        if resp.status_code == 400 and self._error_code(resp) == "USER_NOT_FOUND":
            return AuthResult(
                error=UserNotFoundError(f"User {str(user_id)!r} not found")
            )
        if not resp.is_success:
            raise self._backend_error("revoke_all_sessions", resp)
        return AuthResult(value=True)

    # ------------------------------------------------------------------
    # Sign-up / sign-in (API-key endpoints)
    # ------------------------------------------------------------------

    async def sign_up(
        self,
        *,
        email: str | None = None,
        phone: str | None = None,
        password: str | None = None,
        **kwargs: Any,
    ) -> AuthOutcome:
        resp = await self._public_request(
            "accounts:signUp",
            {"email": email, "password": password, "returnSecureToken": True},
        )
        if not resp.is_success:
            code = self._error_code(resp)
            if code == "EMAIL_EXISTS":
                return AuthOutcome(
                    status=AuthStatus.EMAIL_EXISTS,
                    error=EmailAlreadyExistsError(email or ""),
                )
            if code in {"WEAK_PASSWORD", "PASSWORD_LOGIN_DISABLED"}:
                return AuthOutcome(
                    status=AuthStatus.WEAK_PASSWORD,
                    error=PasswordValidationError(),
                )
            raise self._backend_error("sign_up", resp)
        return await self._auth_outcome_from_response(resp.json())

    async def sign_in_with_password(
        self, *, identifier: str, password: str
    ) -> AuthOutcome:
        resp = await self._public_request(
            "accounts:signInWithPassword",
            {"email": identifier, "password": password, "returnSecureToken": True},
        )
        if not resp.is_success:
            code = self._error_code(resp)
            if code in {
                "EMAIL_NOT_FOUND",
                "INVALID_PASSWORD",
                "INVALID_LOGIN_CREDENTIALS",
                "USER_DISABLED",
            }:
                return AuthOutcome(
                    status=AuthStatus.INVALID_CREDENTIALS,
                    error=InvalidCredentialsError(),
                )
            raise self._backend_error("sign_in_with_password", resp)
        return await self._auth_outcome_from_response(resp.json())

    async def send_magic_link(self, *, email: str, redirect_url: str) -> None:
        """Send an email sign-in link via ``accounts:sendOobCode``.

        GCIP composes and sends the email itself; ``redirect_url`` is the
        continue URL the link returns to (where the client completes sign-in
        with :meth:`verify_magic_link`).
        """
        resp = await self._public_request(
            "accounts:sendOobCode",
            {
                "requestType": "EMAIL_SIGNIN",
                "email": email,
                "continueUrl": redirect_url,
            },
        )
        if not resp.is_success:
            raise self._backend_error("send_magic_link", resp)

    async def verify_magic_link(
        self, token: str, *, email: str | None = None
    ) -> AuthOutcome:
        """Complete email-link sign-in.

        ``token`` is the ``oobCode`` from the email link. GCIP also requires
        the ``email`` the link was sent to (the client retains it), so it is a
        required keyword here — mirroring the WorkOS backend.

        Raises:
            ValueError: ``email`` was not supplied.
        """
        if email is None:
            raise ValueError(
                "GCIP requires the email address to verify an email-link "
                "sign-in. Pass email= to verify_magic_link()."
            )
        resp = await self._public_request(
            "accounts:signInWithEmailLink",
            {"email": email, "oobCode": token, "returnSecureToken": True},
        )
        if not resp.is_success:
            code = self._error_code(resp)
            if code in {"INVALID_OOB_CODE", "EXPIRED_OOB_CODE", "INVALID_EMAIL"}:
                return AuthOutcome(
                    status=AuthStatus.INVALID_TOKEN,
                    error=InvalidTokenError("Magic link is invalid or expired"),
                )
            raise self._backend_error("verify_magic_link", resp)
        return await self._auth_outcome_from_response(resp.json())

    # ------------------------------------------------------------------
    # OAuth (API-key endpoint)
    # ------------------------------------------------------------------

    async def sign_in_with_oauth(
        self,
        code: str,
        *,
        provider: str | AuthProvider | None = None,
        redirect_uri: str | None = None,
    ) -> AuthOutcome:
        """Sign in with a federated IdP credential via ``accounts:signInWithIdp``.

        NOTE: for GCIP, ``code`` is the *IdP's ID token* (e.g. the Google
        ``id_token`` obtained client-side), NOT an OAuth authorization code.
        The provider name is mapped to a GCIP provider id by appending
        ``.com`` (e.g. ``google`` -> ``google.com``). GCIP drives the redirect
        client-side via its SDK, so it does not implement
        :meth:`authorization_url`.

        Raises:
            InvalidCredentialsError: GCIP rejected the IdP credential.
        """
        provider_name = (
            provider.value if isinstance(provider, AuthProvider) else (provider or "")
        )
        resp = await self._public_request(
            "accounts:signInWithIdp",
            {
                "postBody": f"id_token={code}&providerId={provider_name}.com",
                "requestUri": (
                    redirect_uri or self._config.redirect_uri or "http://localhost"
                ),
                "returnSecureToken": True,
            },
        )
        if not resp.is_success:
            code_ = self._error_code(resp)
            if code_ in {
                "INVALID_IDP_RESPONSE",
                "INVALID_CREDENTIAL_OR_PROVIDER_ID",
                "FEDERATED_USER_ID_ALREADY_LINKED",
                "USER_DISABLED",
            }:
                return AuthOutcome(
                    status=AuthStatus.INVALID_CREDENTIALS,
                    error=InvalidCredentialsError("OAuth credential rejected by GCIP"),
                )
            raise self._backend_error("sign_in_with_oauth", resp)
        return await self._auth_outcome_from_response(resp.json())

    # ------------------------------------------------------------------
    # Tokens
    # ------------------------------------------------------------------

    async def refresh(self, refresh_token: str) -> AuthResult[TokenSet]:
        """Exchange a refresh token for a fresh ID token.

        Uses the secure-token endpoint (a different host from the Identity
        Toolkit API) with the OAuth ``refresh_token`` grant.

        Raises:
            InvalidTokenError: Refresh token is unknown, expired, or revoked.
        """
        conn = self._ensure_connected()
        url = f"{self.SECURE_TOKEN_BASE}/token?key={self._require_api_key()}"
        try:
            resp = await conn.http.post(
                url,
                data={"grant_type": "refresh_token", "refresh_token": refresh_token},
            )
        except httpx.HTTPError as e:
            raise AuthBackendError(f"GCIP refresh failed: {e}") from e
        if not resp.is_success:
            code = self._error_code(resp)
            if code in {"INVALID_REFRESH_TOKEN", "TOKEN_EXPIRED", "USER_DISABLED"}:
                return AuthResult(
                    error=InvalidTokenError("Refresh token is invalid or expired")
                )
            raise self._backend_error("refresh", resp)
        data = resp.json()
        # The secure-token endpoint returns snake_case fields.
        expires_in = int(data.get("expires_in", 3600))
        id_token = data.get("id_token", "")
        return AuthResult(
            value=TokenSet(
                access_token=id_token,
                refresh_token=data.get("refresh_token") or None,
                id_token=id_token,
                token_type="bearer",
                expires_in=expires_in,
                expires_at=datetime.now(UTC) + timedelta(seconds=expires_in),
            )
        )

    # ------------------------------------------------------------------
    # Password recovery (API-key endpoint)
    # ------------------------------------------------------------------

    async def request_password_reset(
        self, *, email: str, redirect_url: str | None = None
    ) -> None:
        """Send a password-reset email via ``accounts:sendOobCode``.

        GCIP composes and sends the email itself; ``redirect_url`` becomes the
        continue URL the reset link returns to.
        """
        body: dict[str, Any] = {"requestType": "PASSWORD_RESET", "email": email}
        if redirect_url:
            body["continueUrl"] = redirect_url
        resp = await self._public_request("accounts:sendOobCode", body)
        if not resp.is_success:
            raise self._backend_error("request_password_reset", resp)

    async def reset_password(
        self, token: str, new_password: str
    ) -> AuthResult[Identity]:
        """Complete a password reset via ``accounts:resetPassword``.

        *token* is the ``oobCode`` from the reset email. Expected failures
        (attached to ``result.error``): ``InvalidTokenError`` (code unknown or
        expired) and ``PasswordValidationError`` (new password fails policy).
        """
        resp = await self._public_request(
            "accounts:resetPassword",
            {"oobCode": token, "newPassword": new_password},
        )
        if not resp.is_success:
            code = self._error_code(resp)
            if code in {"INVALID_OOB_CODE", "EXPIRED_OOB_CODE"}:
                return AuthResult(
                    error=InvalidTokenError("Reset code is invalid or expired")
                )
            if code == "WEAK_PASSWORD":
                return AuthResult(
                    error=PasswordValidationError(
                        "Password does not meet GCIP requirements"
                    )
                )
            raise self._backend_error("reset_password", resp)
        # Re-read the canonical record so the Identity goes through one mapper.
        email = resp.json().get("email")
        identity = await self.find_user(email=email) if email else None
        if identity is None:
            raise AuthBackendError("reset_password succeeded but user lookup failed")
        return AuthResult(value=identity)

    async def verify_email(self, token: str) -> AuthResult[Identity]:
        """Confirm an email address via the ``accounts:update`` OOB flow.

        *token* is the ``oobCode`` from the verification email. Expected failure
        (attached to ``result.error``): ``InvalidTokenError`` when the code is
        unknown or expired.
        """
        resp = await self._public_request("accounts:update", {"oobCode": token})
        if not resp.is_success:
            if self._error_code(resp) in {"INVALID_OOB_CODE", "EXPIRED_OOB_CODE"}:
                return AuthResult(
                    error=InvalidTokenError("Verification code is invalid or expired")
                )
            raise self._backend_error("verify_email", resp)
        local_id = resp.json().get("localId")
        if not local_id:
            raise AuthBackendError("verify_email succeeded but response had no localId")
        return AuthResult(value=await self.get_user(local_id))

    # ------------------------------------------------------------------
    # Organizations (derp-layered)
    #
    # GCIP has no org concept, so derp owns the org graph in Postgres. Org
    # mutations are plain Postgres writes with no token side-effects. A request's
    # single active org is carried by a short-lived derp-signed org-context
    # credential (`set_active_org`), verified next to the Google ID token in
    # `authenticate` — the hot path authorizes against it with no DB IO.
    # ------------------------------------------------------------------

    def _db(self) -> DatabaseEngine:
        if self._database is None:
            raise AuthBackendError(
                "GCIP organizations require a database. Configure [database] so "
                "DerpClient wires it into the auth client via set_db()."
            )
        return self._database

    def _to_org(self, org: AuthOrganization) -> Org:
        return Org(
            id=str(org.id),
            tenant_id=None,
            name=org.name,
            slug=org.slug,
            metadata=org.metadata or {},
            created_at=org.created_at,
            updated_at=org.updated_at,
        )

    def _to_member(self, member: GCIPOrgMember) -> OrgMember:
        return OrgMember(
            org_id=str(member.org_id),
            user_id=str(member.user_id),
            role=member.role,
            created_at=member.created_at,
            updated_at=member.updated_at,
        )

    def _to_invitation(self, inv: AuthInvitation) -> Invitation:
        return Invitation(
            id=str(inv.id),
            org_id=str(inv.org_id),
            email=inv.email,
            role=inv.role,
            state=str(inv.state),
            expires_at=inv.expires_at,
        )

    async def get_member_role(self, *, user_id: str, org: str) -> str | None:
        """Authoritatively resolve a user's role in *org* (a slug).

        The DB-backed companion to the zero-IO active-org helpers, for the
        separate-API path: a membership/role question about an org the session
        is **not** currently acting in — e.g. validating before
        :meth:`set_active_org`, or a cross-org admin check. Returns ``None`` if
        the user isn't a member.

        Also the per-request hot path for :meth:`authenticate`'s active-org
        resolve, so a KV cache fronts the PG read when one is wired
        (:meth:`set_kv`): hits return in sub-millisecond, misses fall through to
        Postgres and back-fill. ``role_cache_ttl_seconds`` bounds revocation
        latency (a removed/demoted member loses their old role within the TTL).
        Both hits and "not a member" answers are cached to neutralise probe loads.
        """
        ttl = self._config.role_cache_ttl_seconds
        cache_key: bytes | None = None
        if self._kv_client is not None and ttl > 0:
            cache_key = f"derp:auth:gcip:member:{user_id}:{org}".encode()
            cached = await self._kv_client.get(cache_key)
            if cached is not None:
                # Empty bytes is the sentinel for "definitely not a member".
                return cached.decode() if cached else None

        role = await (
            self._db()
            .select(GCIPOrgMember.role)
            .from_(GCIPOrgMember)
            .inner_join(AuthOrganization, AuthOrganization.id == GCIPOrgMember.org_id)
            .where(AuthOrganization.slug == org)
            .where(GCIPOrgMember.user_id == str(user_id))
            .first_or_none()
        )

        if cache_key is not None and self._kv_client is not None:
            await self._kv_client.set(cache_key, (role or "").encode(), ttl=ttl)
        return role

    async def set_active_org(
        self,
        *,
        session_id: str,
        org_id: str | None = None,
        slug: str | None = None,
    ) -> AuthResult[TokenSet]:
        """Switch the active org; return a signed active-org pointer.

        GCIP has no server session, so ``session_id`` is the user id (the value
        :meth:`verify_token` puts in ``Session.session_id``). Validates
        membership in Postgres; on success ``result.value`` is a
        :class:`TokenSet` whose ``access_token`` is the pointer the client
        resends in the ``X-Org-Context`` header. The pointer does not expire
        (``expires_in`` is 0) — the client re-mints only when switching org, and
        the current role is resolved from the DB on each request, so role and
        membership changes take effect without a re-mint. Pass neither ``org_id``
        nor ``slug`` to clear the active org (empty credential). Expected failure
        (attached to ``result.error``): ``OrgMemberNotFoundError`` when the user
        isn't a member.
        """
        self._check_org_ref(org_id, slug, allow_clear=True)
        if org_id is None and slug is None:
            # Clearing the active org mints an empty credential and needs no
            # secret (there is nothing to sign).
            return AuthResult(
                value=TokenSet(
                    access_token="",
                    refresh_token=None,
                    id_token=None,
                    token_type="bearer",
                    expires_in=0,
                    expires_at=datetime.now(UTC),
                )
            )
        # Fail fast on a missing secret before any DB I/O — signing the pointer
        # needs it, and it is a config error, not an expected auth failure.
        secret = self._require_org_secret()
        user_id = str(session_id)
        db = self._db()
        # One query for both fields — the pointer carries slug *and* id, and
        # require_org compares the slug, so we read both here. A single join
        # avoids a slug-resolve race where the org could be deleted between
        # two separate reads.
        sel = (
            db.select(GCIPOrgMember.org_id, AuthOrganization.slug)
            .from_(GCIPOrgMember)
            .inner_join(AuthOrganization, AuthOrganization.id == GCIPOrgMember.org_id)
            .where(GCIPOrgMember.user_id == user_id)
        )
        if org_id is not None:
            sel = sel.where(GCIPOrgMember.org_id == org_id)
        else:
            sel = sel.where(AuthOrganization.slug == slug)
        row = await sel.first_or_none()
        if row is None:
            return AuthResult(
                error=OrgMemberNotFoundError(
                    f"User {user_id!r} is not a member of org {org_id or slug!r}"
                )
            )
        member_org_id, member_slug = row

        # A stable, user-bound pointer: ``org_id:slug:sig``. No role and no
        # expiry — authority over the current role/membership is the DB, resolved
        # per request in authenticate (which recomputes this exact signature).
        sig = hmac.new(
            secret.encode(),
            f"{user_id}:{member_org_id}:{member_slug}".encode(),
            hashlib.sha256,
        ).hexdigest()
        signed = f"{member_org_id}:{member_slug}:{sig}"
        return AuthResult(
            value=TokenSet(
                access_token=signed,
                refresh_token=None,
                id_token=None,
                token_type="bearer",
                # A stable pointer — it does not expire; the client re-mints only
                # when the user switches org, and the DB is authority for role.
                expires_in=0,
                expires_at=datetime.now(UTC),
            )
        )

    async def create_org(
        self, *, name: str, slug: str, creator_id: str, **kwargs: Any
    ) -> AuthResult[Org]:
        """Create an org; the creator becomes owner.

        Expected failure: ``OrgSlugConflictError``.
        """
        org = await (
            self._db()
            .insert(AuthOrganization)
            .values(name=name, slug=slug)
            .ignore_conflicts(target=AuthOrganization.slug)
            .returning(AuthOrganization)
            .execute()
        )
        if org is None:
            return AuthResult(error=OrgSlugConflictError(slug))
        await (
            self._db()
            .insert(GCIPOrgMember)
            .values(org_id=org.id, user_id=creator_id, role="owner")
            .execute()
        )
        return AuthResult(value=self._to_org(org))

    async def get_org(
        self, *, org_id: str | None = None, slug: str | None = None
    ) -> AuthResult[Org]:
        """Get an org by id or slug (pass exactly one).

        Expected failure: ``OrgNotFoundError``.
        """
        self._check_org_ref(org_id, slug)
        q = self._db().select(AuthOrganization)
        q = (
            q.where(AuthOrganization.id == org_id)
            if org_id is not None
            else q.where(AuthOrganization.slug == slug)
        )
        row = await q.first_or_none()
        if row is None:
            return AuthResult(
                error=OrgNotFoundError(f"No org matching {org_id or slug!r}")
            )
        return AuthResult(value=self._to_org(row))

    async def list_orgs(
        self, *, user_id: str | None = None, limit: int = 50, cursor: str | None = None
    ) -> Page[Org]:
        """List orgs (cursor is an opaque offset), optionally scoped to a member."""
        db = self._db()
        offset = int(cursor) if cursor else 0
        q = db.select(AuthOrganization).order_by(AuthOrganization.created_at, asc=False)
        if user_id is not None:
            q = q.inner_join(
                GCIPOrgMember, GCIPOrgMember.org_id == AuthOrganization.id
            ).where(GCIPOrgMember.user_id == str(user_id))
        q = q.limit(limit).offset(offset)
        items = [self._to_org(o) for o in await q.execute()]
        has_more = len(items) == limit
        next_cursor = str(offset + limit) if has_more else None
        return Page(items=items, next_cursor=next_cursor, has_more=has_more)

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

        ``name``/``new_slug`` are the new values (``new_slug`` is spelled apart
        from the addressing ``slug``). A slug change is a single Postgres write;
        members holding an org-context credential keep its old slug until they
        next call :meth:`set_active_org`.

        Expected failures: ``OrgNotFoundError``, ``OrgSlugConflictError``.
        """
        self._check_org_ref(org_id, slug)
        q = self._db().select(AuthOrganization)
        q = (
            q.where(AuthOrganization.id == org_id)
            if org_id is not None
            else q.where(AuthOrganization.slug == slug)
        )
        existing = await q.first_or_none()
        if existing is None:
            return AuthResult(
                error=OrgNotFoundError(f"No org matching {org_id or slug!r}")
            )

        updates: dict[str, Any] = {"updated_at": datetime.now(UTC)}
        if name is not None:
            updates["name"] = name
        if new_slug is not None:
            updates["slug"] = new_slug
        try:
            [result] = await (
                self._db()
                .update(AuthOrganization)
                .set(**updates)
                .where(AuthOrganization.id == existing.id)
                .returning(AuthOrganization)
                .execute()
            )
        except Exception as e:
            if new_slug is not None:
                return AuthResult(error=OrgSlugConflictError(new_slug))
            raise AuthBackendError(f"GCIP update_org failed: {e}") from e

        return AuthResult(value=self._to_org(result))

    async def delete_org(
        self, *, org_id: str | None = None, slug: str | None = None
    ) -> bool:
        """Delete an org (by id or slug) and its memberships. ``False`` if absent.

        Memberships are removed by the org delete (FK cascade).
        """
        self._check_org_ref(org_id, slug)
        q = self._db().select(AuthOrganization)
        q = (
            q.where(AuthOrganization.id == org_id)
            if org_id is not None
            else q.where(AuthOrganization.slug == slug)
        )
        existing = await q.first_or_none()
        if existing is None:
            return False
        await (
            self._db()
            .delete(AuthOrganization)
            .where(AuthOrganization.id == existing.id)
            .execute()
        )
        return True

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
        self._check_org_ref(org_id, slug)
        db = self._db()
        canonical = org_id
        if canonical is None:
            org = await (
                db.select(AuthOrganization)
                .where(AuthOrganization.slug == slug)
                .first_or_none()
            )
            if org is None:
                return AuthResult(error=OrgNotFoundError(f"No org matching {slug!r}"))
            canonical = org.id

        member = await (
            db.insert(GCIPOrgMember)
            .values(org_id=canonical, user_id=str(user_id), role=role)
            .ignore_conflicts(target=(GCIPOrgMember.org_id, GCIPOrgMember.user_id))
            .returning(GCIPOrgMember)
            .execute()
        )
        if member is None:
            return AuthResult(error=MemberAlreadyExistsError())
        return AuthResult(value=self._to_member(member))

    async def update_member(
        self,
        *,
        user_id: str,
        role: str,
        org_id: str | None = None,
        slug: str | None = None,
    ) -> AuthResult[OrgMember]:
        """Change a member's role (org by id or slug).

        Expected failure: ``OrgMemberNotFoundError`` (not a member or no such org).
        """
        self._check_org_ref(org_id, slug)
        db = self._db()
        sel = db.select(GCIPOrgMember).where(GCIPOrgMember.user_id == str(user_id))
        if org_id is not None:
            sel = sel.where(GCIPOrgMember.org_id == org_id)
        else:
            sel = sel.inner_join(
                AuthOrganization, AuthOrganization.id == GCIPOrgMember.org_id
            ).where(AuthOrganization.slug == slug)
        existing = await sel.first_or_none()
        if existing is None:
            return AuthResult(
                error=OrgMemberNotFoundError(
                    f"User {user_id!r} is not a member of org {org_id or slug!r}"
                )
            )
        # existing.org_id is the canonical id — reuse it, no extra lookup.
        [result] = await (
            db.update(GCIPOrgMember)
            .set(role=role, updated_at=datetime.now(UTC))
            .where(GCIPOrgMember.org_id == existing.org_id)
            .where(GCIPOrgMember.user_id == str(user_id))
            .returning(GCIPOrgMember)
            .execute()
        )
        return AuthResult(value=self._to_member(result))

    async def remove_member(
        self, *, user_id: str, org_id: str | None = None, slug: str | None = None
    ) -> AuthResult[bool]:
        """Remove a member (org by id or slug).

        ``result.value`` is ``True`` if removed, ``False`` if the user wasn't a
        member. Expected failure: ``LastOwnerError``.
        """
        self._check_org_ref(org_id, slug)
        db = self._db()
        sel = db.select(GCIPOrgMember).where(GCIPOrgMember.user_id == str(user_id))
        if org_id is not None:
            sel = sel.where(GCIPOrgMember.org_id == org_id)
        else:
            sel = sel.inner_join(
                AuthOrganization, AuthOrganization.id == GCIPOrgMember.org_id
            ).where(AuthOrganization.slug == slug)
        existing = await sel.first_or_none()
        if existing is None:
            return AuthResult(value=False)
        if existing.role == "owner":
            owner_count = await (
                db.select(GCIPOrgMember)
                .where(GCIPOrgMember.org_id == existing.org_id)
                .where(GCIPOrgMember.role == "owner")
                .count()
            )
            if owner_count <= 1:
                return AuthResult(
                    error=LastOwnerError(
                        f"Cannot remove the last owner of org {existing.org_id!r}"
                    )
                )
        await (
            db.delete(GCIPOrgMember)
            .where(GCIPOrgMember.org_id == existing.org_id)
            .where(GCIPOrgMember.user_id == str(user_id))
            .execute()
        )
        return AuthResult(value=True)

    async def list_members(
        self,
        *,
        org_id: str | None = None,
        slug: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> Page[OrgMember]:
        """List an org's members (by id or slug; cursor is an opaque offset).

        An unknown org yields an empty page (no existence query needed).
        """
        self._check_org_ref(org_id, slug)
        db = self._db()
        offset = int(cursor) if cursor else 0
        q = db.select(GCIPOrgMember)
        if org_id is not None:
            q = q.where(GCIPOrgMember.org_id == org_id)
        else:
            q = q.inner_join(
                AuthOrganization, AuthOrganization.id == GCIPOrgMember.org_id
            ).where(AuthOrganization.slug == slug)
        q = q.order_by(GCIPOrgMember.created_at, asc=True).limit(limit).offset(offset)
        items = [self._to_member(m) for m in await q.execute()]
        has_more = len(items) == limit
        next_cursor = str(offset + limit) if has_more else None
        return Page(items=items, next_cursor=next_cursor, has_more=has_more)

    # ------------------------------------------------------------------
    # Invitations
    # ------------------------------------------------------------------

    async def invite_to_org(
        self,
        *,
        email: str,
        org_id: str | None = None,
        slug: str | None = None,
        role: str = "member",
    ) -> AuthResult[Invitation]:
        """Create a pending invite for an org (by id or slug) with an opaque token.

        The invite expires after ``GCIPConfig.invitation_ttl_hours``; an expired
        invite is rejected by :meth:`accept_invitation`.

        Expected failure: ``OrgNotFoundError`` when a given slug matches no org.
        """
        self._check_org_ref(org_id, slug)
        canonical = org_id
        if canonical is None:
            org = await (
                self._db()
                .select(AuthOrganization)
                .where(AuthOrganization.slug == slug)
                .first_or_none()
            )
            if org is None:
                return AuthResult(error=OrgNotFoundError(f"No org matching {slug!r}"))
            canonical = org.id

        expires_at = datetime.now(UTC) + timedelta(
            hours=self._config.invitation_ttl_hours
        )
        inv = await (
            self._db()
            .insert(AuthInvitation)
            .values(
                org_id=canonical,
                email=email,
                role=role,
                token=secrets.token_urlsafe(32),
                expires_at=expires_at,
            )
            .returning(AuthInvitation)
            .execute()
        )
        return AuthResult(value=self._to_invitation(inv))

    async def accept_invitation(self, *, invitation_token: str) -> AuthOutcome:
        """Accept an invite: add the invitee as a member.

        GCIP issues its own tokens, so this does not mint one — the invitee
        continues with their existing GCIP session and switches to the new org
        via :meth:`set_active_org` when they want to act in it. The returned
        ``AuthOutcome`` is ``COMPLETE`` with the ``identity`` but no ``tokens``.

        Raises:
            UserNotFoundError: No GCIP account exists for the invited email yet.
        """
        db = self._db()
        now = datetime.now(UTC)
        inv = await (
            db.select(AuthInvitation)
            .where(AuthInvitation.token == invitation_token)
            .first_or_none()
        )
        if inv is None or inv.state != InvitationState.PENDING:
            return AuthOutcome(
                status=AuthStatus.INVALID_TOKEN,
                error=InvalidTokenError("Invitation is invalid or already used"),
            )
        if inv.expires_at is not None and now >= inv.expires_at:
            # Mark it expired so it stops showing as pending, then reject.
            await (
                db.update(AuthInvitation)
                .set(state=InvitationState.EXPIRED, updated_at=now)
                .where(AuthInvitation.id == inv.id)
                .execute()
            )
            return AuthOutcome(
                status=AuthStatus.INVALID_TOKEN,
                error=InvalidTokenError("Invitation has expired"),
            )
        user = await self.find_user(email=inv.email)
        if user is None:
            raise UserNotFoundError(
                "Invitee must create an account before accepting the invitation"
            )
        await (
            db.insert(GCIPOrgMember)
            .values(
                org_id=inv.org_id,
                user_id=user.id,
                role=inv.role,
                created_at=now,
                updated_at=now,
            )
            .ignore_conflicts(target=(GCIPOrgMember.org_id, GCIPOrgMember.user_id))
            .execute()
        )
        await (
            db.update(AuthInvitation)
            .set(state=InvitationState.ACCEPTED, updated_at=now)
            .where(AuthInvitation.id == inv.id)
            .execute()
        )
        return AuthOutcome(status=AuthStatus.COMPLETE, identity=user)

    async def revoke_invitation(self, *, invitation_id: str) -> bool:
        """Revoke a pending invite. Returns ``False`` if not found."""
        db = self._db()
        existing = await (
            db.select(AuthInvitation)
            .where(AuthInvitation.id == str(invitation_id))
            .first_or_none()
        )
        if existing is None:
            return False
        await (
            db.update(AuthInvitation)
            .set(state=InvitationState.REVOKED, updated_at=datetime.now(UTC))
            .where(AuthInvitation.id == str(invitation_id))
            .execute()
        )
        return True

    async def list_invitations(
        self,
        *,
        org_id: str | None = None,
        slug: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> Page[Invitation]:
        """List an org's invitations (by id or slug; cursor is an opaque offset).

        An unknown org yields an empty page (no existence query needed).
        """
        self._check_org_ref(org_id, slug)
        db = self._db()
        offset = int(cursor) if cursor else 0
        q = db.select(AuthInvitation)
        if org_id is not None:
            q = q.where(AuthInvitation.org_id == org_id)
        else:
            q = q.inner_join(
                AuthOrganization, AuthOrganization.id == AuthInvitation.org_id
            ).where(AuthOrganization.slug == slug)
        q = q.order_by(AuthInvitation.created_at, asc=True).limit(limit).offset(offset)
        items = [self._to_invitation(i) for i in await q.execute()]
        has_more = len(items) == limit
        next_cursor = str(offset + limit) if has_more else None
        return Page(items=items, next_cursor=next_cursor, has_more=has_more)
