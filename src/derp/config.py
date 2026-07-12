"""Central configuration for Derp."""

from __future__ import annotations

import os
import tomllib
import warnings
from collections.abc import Sequence
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

CONFIG_FILE = "derp.toml"
MIGRATIONS_TABLE = "derp_migrations"
DEFAULT_MIGRATIONS_DIR = "./migrations"
DEFAULT_CH_MIGRATIONS_DIR = "./ch_migrations"


class ConfigError(Exception):
    """Configuration error."""


class ConfigWarning(UserWarning):
    """Emitted when a non-strict load leaves ``$VAR`` references unresolved."""


def _format_missing(missing: dict[str, list[str]]) -> str:
    """Render unresolved env vars grouped by the section they appear under."""
    return "; ".join(
        f"[{section}] {', '.join('$' + name for name in names)}"
        for section, names in sorted(missing.items())
    )


def _resolve_env_value(
    value: Any,
    *,
    _path: tuple[str, ...] = (),
    _env_vars: dict[tuple[str, ...], str] | None = None,
    _missing: dict[str, list[str]] | None = None,
) -> Any:
    """Substitute ``$VAR`` references, recording — never raising on — misses.

    Unset variables are left as their literal ``$VAR`` text and recorded in
    ``_missing``, keyed by the top-level section they appear under. Whether a
    miss is fatal is :meth:`DerpConfig.load`'s decision, because an offline
    command has no business failing over an unset key in a section it never
    reads.
    """
    if _env_vars is None:
        _env_vars = {}
    if _missing is None:
        _missing = {}
    if isinstance(value, str):
        if value.startswith("$"):
            env_name = value[1:]
            if not env_name:
                raise ConfigError("Invalid environment variable reference: '$'")
            env_value = os.environ.get(env_name)
            if env_value is None:
                section = _path[0] if _path else ""
                _missing.setdefault(section, []).append(env_name)
                return value
            _env_vars[_path] = env_name
            return env_value
        return value
    if isinstance(value, list):
        return [
            _resolve_env_value(
                item, _path=(*_path, str(i)), _env_vars=_env_vars, _missing=_missing
            )
            for i, item in enumerate(value)
        ]
    if isinstance(value, tuple):
        return tuple(
            _resolve_env_value(
                item, _path=(*_path, str(i)), _env_vars=_env_vars, _missing=_missing
            )
            for i, item in enumerate(value)
        )
    if isinstance(value, dict):
        return {
            key: _resolve_env_value(
                val, _path=(*_path, key), _env_vars=_env_vars, _missing=_missing
            )
            for key, val in value.items()
        }
    return value


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DatabaseConfig(_StrictModel):
    """Database configuration."""

    db_url: str
    replica_url: str | None = None
    schema_path: str

    migrations_dir: str = DEFAULT_MIGRATIONS_DIR
    introspect_schemas: Sequence[str] = ("public",)
    introspect_exclude_tables: Sequence[str] = (MIGRATIONS_TABLE,)

    ignore_rls: bool = False

    pool_min_size: int = 2
    pool_max_size: int = 5
    # Default to 0, for PgBouncer compatibility
    statement_cache_size: int = 0

    replica_pool_min_size: int | None = None
    replica_pool_max_size: int | None = None
    # Default to asyncpg's default since replicas don't often use PgBouncer
    replica_statement_cache_size: int | None = None

    replica_max_lag_bytes: int = 1_048_576
    replica_write_fence_seconds: float = 2.0
    replica_lag_check_interval_seconds: float = 5.0


class ClickHouseConfig(_StrictModel):
    """ClickHouse configuration."""

    url: str | None = None
    host: str | None = None
    port: int | None = None
    username: str = "default"
    password: str = ""
    database: str = "default"
    secure: bool = False

    # Migration tooling (used by the `derp ch ...` CLI commands)
    schema_path: str | None = None
    migrations_dir: str = DEFAULT_CH_MIGRATIONS_DIR
    introspect_database: str = "default"


class EmailConfig(_StrictModel):
    """Configuration for email sending via SMTP."""

    site_name: str
    site_url: str
    from_email: str
    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_password: str

    templates_dir: str | None = None

    use_tls: bool = True
    start_tls: bool = False


class JWTConfig(_StrictModel):
    """Configuration for JWT tokens."""

    secret: str
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 15
    refresh_token_expire_days: int = 7
    issuer: str | None = None
    audience: str | None = None


class PasswordConfig(_StrictModel):
    """Configuration for password validation."""

    min_length: int = 8
    max_length: int = 128
    require_uppercase: bool = False
    require_lowercase: bool = False
    require_digit: bool = False
    require_special: bool = False


class GoogleOAuthConfig(_StrictModel):
    """Configuration for Google OAuth."""

    client_id: str
    client_secret: str
    redirect_uri: str
    scopes: Sequence[str] = ("openid", "email", "profile")


class GitHubOAuthConfig(_StrictModel):
    """Configuration for GitHub OAuth."""

    client_id: str
    client_secret: str
    redirect_uri: str
    scopes: Sequence[str] = ("user:email",)


class NativeAuthConfig(_StrictModel):
    """Configuration for native authentication (email/password, magic link, OAuth)."""

    jwt: JWTConfig
    password: PasswordConfig = Field(default_factory=PasswordConfig)

    google_oauth: GoogleOAuthConfig | None = None
    github_oauth: GitHubOAuthConfig | None = None

    enable_signup: bool = True
    enable_confirmation: bool = True
    enable_magic_link: bool = False

    magic_link_expire_minutes: int = 60
    recovery_token_expire_minutes: int = 60
    confirmation_token_expire_hours: int = 24
    session_expire_days: int = 30

    use_kv_cache: bool = True
    cache_prefix: str = "derp:auth"
    cache_session_ttl_seconds: int = 300
    cache_user_ttl_seconds: int = 300


class SupabaseConfig(_StrictModel):
    """Configuration for Supabase GoTrue authentication."""

    url: str
    anon_key: str
    service_role_key: str
    jwt_secret: str
    redirect_uri: str | None = None


class WorkOSConfig(_StrictModel):
    """Configuration for WorkOS authentication."""

    api_key: str
    client_id: str
    redirect_uri: str | None = None


class GCIPConfig(_StrictModel):
    """Configuration for Google Cloud Identity Platform.

    Only ``project_id`` is always required — it fixes the expected issuer and
    audience for token verification, which is all :meth:`~derp.auth.gcip_client.
    GCIPAuthClient.authenticate` (the per-request hot path) needs. The three
    credentials below are each required only by the feature that consumes them,
    and are validated at the point of use with a clear error, so a verify-only
    deployment (sign-in handled client-side by the Firebase JS SDK) configures a
    single field.
    """

    project_id: str
    # Required only for server-side sign-in / sign-up / OAuth / magic-link /
    # password-reset / token-refresh — the API-key-authenticated Identity Toolkit
    # endpoints. Omit when sign-in happens client-side via the Firebase JS SDK.
    public_api_key: str | None = None
    # Required only for user administration (get/find/list/update/delete user,
    # revoke_all_sessions) — the service-account-authenticated admin endpoints.
    # JSON string or $ENV reference.
    service_account_json: str | None = None
    # Required only for active-org features (set_active_org and X-Org-Context
    # resolution). HMAC key for the derp-signed active-org pointer — a critical
    # secret; store in env / secret manager, never source-control. Rotating it
    # invalidates every active-org pointer (clients must call set_active_org
    # again). Must be >= 32 chars when set.
    org_context_secret: str | None = Field(default=None, min_length=32)
    redirect_uri: str | None = None
    invitation_ttl_hours: int = 7 * 24  # 7 days
    # Active-org role → permission grants. Populated into ``Session.permissions``
    # on every request so ``has`` / ``require_permission`` answer zero-IO; empty
    # by default, configure per app to enable RBAC.
    role_permissions: dict[str, list[str]] = Field(default_factory=dict)
    # TTL for the in-KV ``(user_id, slug) → role`` cache that fronts the
    # per-request membership lookup in ``authenticate``. Doubles as the
    # revocation-latency cap (a removed/demoted member loses their old role
    # within this many seconds). Zero disables the cache and goes straight to PG.
    role_cache_ttl_seconds: int = 30


class AuthConfig(_StrictModel):
    """Auth configuration — exactly one backend must be set."""

    native: NativeAuthConfig | None = None
    supabase: SupabaseConfig | None = None
    workos: WorkOSConfig | None = None
    gcip: GCIPConfig | None = None

    @model_validator(mode="after")
    def _check_single_backend(self) -> AuthConfig:
        backends = [self.native, self.supabase, self.workos, self.gcip]
        configured = sum(1 for b in backends if b is not None)
        if configured > 1:
            raise ValueError(
                "Only one auth backend can be configured at a time. "
                "Set exactly one of [auth.native], [auth.supabase], "
                "[auth.workos], or [auth.gcip]."
            )
        if configured == 0:
            raise ValueError("At least one auth backend must be configured.")
        return self


class StorageConfig(_StrictModel):
    """Storage configuration."""

    endpoint_url: str | None = None
    public_urls: dict[str, str] = Field(default_factory=dict)
    service_name: str = "s3"
    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None
    region: str = "auto"
    use_ssl: bool = True
    verify: bool | str = True


class PaymentsConfig(_StrictModel):
    """Payments configuration."""

    api_key: str
    webhook_secret: str | None = None
    max_network_retries: int = 2
    timeout_seconds: float = 30.0


class ValkeyMode(StrEnum):
    """Valkey deployment mode."""

    STANDALONE = "standalone"
    CLUSTER = "cluster"


class ValkeyConfig(_StrictModel):
    """Configuration for Valkey GLIDE connections."""

    addresses: Sequence[tuple[str, int]] = (("localhost", 6379),)
    username: str | None = None
    password: str | None = None
    use_tls: bool = False
    mode: ValkeyMode = ValkeyMode.STANDALONE


class KVConfig(_StrictModel):
    """KV configuration."""

    valkey: ValkeyConfig | None = None


class CeleryConfig(_StrictModel):
    """Configuration for Celery task queue."""

    broker_url: str
    result_backend: str | None = None
    task_serializer: str = "json"
    result_serializer: str = "json"
    task_default_queue: str = "default"


class VercelQueueConfig(_StrictModel):
    """Configuration for Vercel queue (REST-based)."""

    api_token: str
    team_id: str | None = None
    project_id: str | None = None
    default_queue: str = "default"


class ScheduleConfig(_StrictModel):
    """A single recurring task schedule."""

    name: str
    task: str
    cron: str | None = None
    interval_seconds: float | None = None
    payload: dict[str, Any] | None = None
    queue: str | None = None
    path: str | None = None

    @model_validator(mode="after")
    def _check_schedule_type(self) -> ScheduleConfig:
        if self.cron is not None and self.interval_seconds is not None:
            raise ValueError(
                f"Schedule '{self.name}': set either 'cron' or "
                "'interval_seconds', not both."
            )
        if self.cron is None and self.interval_seconds is None:
            raise ValueError(
                f"Schedule '{self.name}': must set either 'cron' or 'interval_seconds'."
            )
        return self


class QueueConfig(_StrictModel):
    """Queue configuration."""

    celery: CeleryConfig | None = None
    vercel: VercelQueueConfig | None = None
    schedules: Sequence[ScheduleConfig] = ()

    @model_validator(mode="after")
    def _check_single_backend(self) -> QueueConfig:
        if self.celery is not None and self.vercel is not None:
            raise ValueError(
                "Only one queue backend can be configured at a time. "
                "Set either [queue.celery] or [queue.vercel], not both."
            )
        return self


class ModalConfig(_StrictModel):
    """Configuration for Modal."""

    token_id: str
    token_secret: str
    endpoint_url: str | None = None


class AIConfig(_StrictModel):
    """AI configuration for OpenAI-compatible providers."""

    api_key: str
    base_url: str | None = None
    fal_api_key: str | None = None
    modal: ModalConfig | None = None


class DerpConfig(_StrictModel):
    """Derp configuration."""

    database: DatabaseConfig
    clickhouse: ClickHouseConfig | None = None
    email: EmailConfig | None = None
    storage: StorageConfig | None = None
    auth: AuthConfig | None = None
    kv: KVConfig | None = None
    payments: PaymentsConfig | None = None
    queue: QueueConfig | None = None
    ai: AIConfig | None = None

    _env_vars: dict[tuple[str, ...], str] = {}
    _missing_env: dict[str, list[str]] = {}

    @property
    def missing_env(self) -> dict[str, list[str]]:
        """Env vars referenced by ``derp.toml`` that were unset at load time.

        Keyed by top-level section. Only ever non-empty after a non-strict
        load, since a strict one raises instead. Those values hold the literal
        ``$VAR`` text rather than a resolved value.
        """
        return self._missing_env

    @classmethod
    def load(
        cls,
        path: str | Path = CONFIG_FILE,
        *,
        strict: bool = True,
    ) -> DerpConfig:
        """Load ``derp.toml``, substituting ``$VAR`` references.

        Args:
            path: Path to the config file.
            strict: When True (the default), an unset ``$VAR`` anywhere in the
              file raises. That is the right behaviour at runtime, where any
              section may be read. Pass False from commands that never touch
              the sections in question — ``derp db generate`` has no business
              failing over an unset ``$FAL_KEY`` in ``[ai]``. Unresolved values
              keep their literal ``$VAR`` text, a :class:`ConfigWarning` names
              them, and :attr:`missing_env` records them.
        """
        config_path = Path(path)

        if not config_path.exists():
            raise ConfigError(
                f"{CONFIG_FILE} not found in current directory. "
                "Run 'derp init' to create one."
            )

        with open(config_path, "rb") as f:
            raw = tomllib.load(f)

        env_vars: dict[tuple[str, ...], str] = {}
        missing: dict[str, list[str]] = {}
        data = _resolve_env_value(raw, _env_vars=env_vars, _missing=missing)

        if missing:
            detail = _format_missing(missing)
            if strict:
                raise ConfigError(f"Missing environment variables: {detail}")
            warnings.warn(
                f"Unresolved environment variables: {detail}. Those values are "
                "left as literal text and are not usable.",
                ConfigWarning,
                stacklevel=2,
            )

        try:
            config = cls(**data)
        except ValidationError as e:
            raise ConfigError("Failed to load configuration.") from e

        config._env_vars = env_vars
        config._missing_env = missing
        return config

    def redacted_dump(self) -> dict:
        """Return config as a dict with environment variable values redacted."""
        data = self.model_dump(mode="json")
        for path, env_name in self._env_vars.items():
            target = data
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = f"${env_name}"
        return data


def create_default_config() -> str:
    """Return default configuration file content."""
    return f"""\\
[database]
db_url = "$DATABASE_URL"  # Environment variable containing the database URL
schema_path = "src/schema.py"  # Path to your schema module(s)
# replica_url = "$REPLICA_DATABASE_URL"  # Optional replica database URL
migrations_dir = "{DEFAULT_MIGRATIONS_DIR}"      # Directory for migration files
# introspect_schemas = ["public"]   # Schemas to introspect
# introspect_exclude_tables = ["{MIGRATIONS_TABLE}"]  # Tables to exclude
# ignore_rls = false  # Ignore RLS and policy changes in migrations

# [clickhouse]
# host = "localhost"
# port = 8123
# username = "default"
# password = "$CLICKHOUSE_PASSWORD"
# database = "default"
# schema_path = "src/ch_schema.py"  # Path to your schema module(s)
# migrations_dir = "{DEFAULT_CH_MIGRATIONS_DIR}"  # Directory for migration files

# [email]
# site_name = "My App"  # Site name for email templates
# site_url = "https://example.com"  # Site URL for email templates
# from_email = "noreply@example.com"  # From email for sending emails
# smtp_host = "smtp.example.com"
# smtp_port = 587
# smtp_user = "$SMTP_USER"
# smtp_password = "$SMTP_PASSWORD"

# [storage]
# endpoint_url = "https://s3.amazonaws.com"
# public_urls = {{ assets = "https://assets.example.com" }}
# access_key_id = "$AWS_ACCESS_KEY_ID"
# secret_access_key = "$AWS_SECRET_ACCESS_KEY"
# region = "us-east-1"

# [auth.native.jwt]
# secret = "$JWT_SECRET"

# [auth.workos]
# api_key = "$WORKOS_API_KEY"
# client_id = "$WORKOS_CLIENT_ID"
# redirect_uri = "https://yourapp.com/callback"

# [auth.supabase]
# url = "$SUPABASE_URL"
# anon_key = "$SUPABASE_ANON_KEY"
# service_role_key = "$SUPABASE_SERVICE_ROLE_KEY"
# jwt_secret = "$SUPABASE_JWT_SECRET"

# [auth.gcip]
# project_id = "$GCIP_PROJECT_ID"  # the only always-required field
# # The rest are needed only by the feature that uses them:
# public_api_key = "$GCIP_API_KEY"  # server-side sign-in / refresh (Web API key)
# service_account_json = "$GCIP_SERVICE_ACCOUNT_JSON"  # user administration
# org_context_secret = "$GCIP_ORG_CONTEXT_SECRET"  # active orgs (>= 32 chars)
# redirect_uri = "https://yourapp.com/callback"

# [kv.valkey]
# addresses = [["localhost", 6379]]
# # username = "$VALKEY_USERNAME"
# # password = "$VALKEY_PASSWORD"
# # use_tls = false

# [payments]
# api_key = "$STRIPE_SECRET_KEY"
# webhook_secret = "$STRIPE_WEBHOOK_SECRET"
# max_network_retries = 2
# timeout_seconds = 30.0

# [queue.celery]
# broker_url = "$CELERY_BROKER_URL"
# result_backend = "$CELERY_RESULT_BACKEND"
# task_default_queue = "default"

# [queue.vercel]
# api_token = "$VERCEL_QUEUE_TOKEN"
# team_id = "team_xxx"
# project_id = "prj_xxx"
# default_queue = "default"

# [ai]
# api_key = "$OPENAI_API_KEY"
# base_url = "https://api.openai.com/v1"  # Optional, for OpenAI-compatible providers
"""
