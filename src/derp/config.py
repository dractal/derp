"""Central configuration for Derp."""

from __future__ import annotations

import os
import tomllib
from collections.abc import Sequence
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

CONFIG_FILE = "derp.toml"
MIGRATIONS_TABLE = "derp_migrations"
DEFAULT_MIGRATIONS_DIR = "./migrations"


class ConfigError(Exception):
    """Configuration error."""


def _resolve_env_value(
    value: Any,
    *,
    _path: tuple[str, ...] = (),
    _env_vars: dict[tuple[str, ...], str] | None = None,
    _missing: list[str] | None = None,
    _root: bool = True,
) -> Any:
    if _env_vars is None:
        _env_vars = {}
    if _missing is None:
        _missing = []
    if isinstance(value, str):
        if value.startswith("$"):
            env_name = value[1:]
            if not env_name:
                raise ConfigError("Invalid environment variable reference: '$'")
            env_value = os.environ.get(env_name)
            if env_value is None:
                _missing.append(env_name)
                return value
            _env_vars[_path] = env_name
            return env_value
        return value
    if isinstance(value, list):
        result = [
            _resolve_env_value(
                item,
                _path=(*_path, str(i)),
                _env_vars=_env_vars,
                _missing=_missing,
                _root=False,
            )
            for i, item in enumerate(value)
        ]
    elif isinstance(value, tuple):
        result = tuple(
            _resolve_env_value(
                item,
                _path=(*_path, str(i)),
                _env_vars=_env_vars,
                _missing=_missing,
                _root=False,
            )
            for i, item in enumerate(value)
        )
    elif isinstance(value, dict):
        result = {
            key: _resolve_env_value(
                val,
                _path=(*_path, key),
                _env_vars=_env_vars,
                _missing=_missing,
                _root=False,
            )
            for key, val in value.items()
        }
    else:
        return value
    if _root and _missing:
        names = ", ".join(f"${v}" for v in _missing)
        raise ConfigError(f"Missing environment variables: {names}")
    return result


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


class ClerkConfig(_StrictModel):
    """Configuration for Clerk authentication."""

    secret_key: str
    jwt_key: str | None = None
    authorized_parties: Sequence[str] = ()


class CognitoConfig(_StrictModel):
    """Configuration for AWS Cognito authentication."""

    user_pool_id: str
    client_id: str
    region: str
    client_secret: str
    access_key_id: str | None = None
    secret_access_key: str | None = None
    domain: str | None = None
    redirect_uri: str | None = None


class SupabaseConfig(_StrictModel):
    """Configuration for Supabase GoTrue authentication."""

    url: str
    anon_key: str
    service_role_key: str
    jwt_secret: str
    redirect_uri: str | None = None


class AuthConfig(_StrictModel):
    """Auth configuration — exactly one backend must be set."""

    native: NativeAuthConfig | None = None
    clerk: ClerkConfig | None = None
    cognito: CognitoConfig | None = None
    supabase: SupabaseConfig | None = None

    @model_validator(mode="after")
    def _check_single_backend(self) -> AuthConfig:
        backends = [self.native, self.clerk, self.cognito, self.supabase]
        configured = sum(1 for b in backends if b is not None)
        if configured > 1:
            raise ValueError(
                "Only one auth backend can be configured at a time. "
                "Set exactly one of [auth.native], [auth.clerk], "
                "[auth.cognito], or [auth.supabase]."
            )
        if configured == 0:
            raise ValueError("At least one auth backend must be configured.")
        return self


class StorageConfig(_StrictModel):
    """Storage configuration."""

    endpoint_url: str | None = None
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
    email: EmailConfig | None = None
    storage: StorageConfig | None = None
    auth: AuthConfig | None = None
    kv: KVConfig | None = None
    payments: PaymentsConfig | None = None
    queue: QueueConfig | None = None
    ai: AIConfig | None = None

    _env_vars: dict[tuple[str, ...], str] = {}

    @classmethod
    def load(cls, path: str | Path = CONFIG_FILE) -> DerpConfig:
        config_path = Path(path)

        if not config_path.exists():
            raise ConfigError(
                f"{CONFIG_FILE} not found in current directory. "
                "Run 'derp init' to create one."
            )

        with open(config_path, "rb") as f:
            raw = tomllib.load(f)

        env_vars: dict[tuple[str, ...], str] = {}
        data = _resolve_env_value(raw, _env_vars=env_vars)

        try:
            config = cls(**data)
        except ValidationError as e:
            raise ConfigError("Failed to load configuration.") from e

        config._env_vars = env_vars
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
schema_path = "src/schema.py"  # Path to your schema module
# replica_url = "$REPLICA_DATABASE_URL"  # Optional replica database URL
migrations_dir = "{DEFAULT_MIGRATIONS_DIR}"      # Directory for migration files
# introspect_schemas = ["public"]   # Schemas to introspect
# introspect_exclude_tables = ["{MIGRATIONS_TABLE}"]  # Tables to exclude
# ignore_rls = false  # Ignore RLS and policy changes in migrations

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
# access_key_id = "$AWS_ACCESS_KEY_ID"
# secret_access_key = "$AWS_SECRET_ACCESS_KEY"
# region = "us-east-1"

# [auth.native.jwt]
# secret = "$JWT_SECRET"

# [auth.clerk]
# secret_key = "$CLERK_SECRET_KEY"

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
