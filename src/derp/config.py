"""Central configuration for Derp."""

from __future__ import annotations

import os
import tomllib
from collections.abc import Sequence
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, ValidationError

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
) -> Any:
    if _env_vars is None:
        _env_vars = {}
    if isinstance(value, str):
        if value.startswith("$"):
            env_name = value[1:]
            if not env_name:
                raise ConfigError("Invalid environment variable reference: '$'")
            env_value = os.environ.get(env_name)
            if not env_value:
                raise ConfigError(
                    f"Environment variable '{env_name}' is not set or empty."
                )
            _env_vars[_path] = env_name
            return env_value
        return value
    if isinstance(value, list):
        return [
            _resolve_env_value(item, _path=(*_path, str(i)), _env_vars=_env_vars)
            for i, item in enumerate(value)
        ]
    if isinstance(value, tuple):
        return tuple(
            _resolve_env_value(item, _path=(*_path, str(i)), _env_vars=_env_vars)
            for i, item in enumerate(value)
        )
    if isinstance(value, dict):
        return {
            key: _resolve_env_value(val, _path=(*_path, key), _env_vars=_env_vars)
            for key, val in value.items()
        }
    return value


class DatabaseConfig(BaseModel):
    """Database configuration."""

    db_url: str
    replica_url: str | None = None
    schema_path: str

    migrations_dir: str = DEFAULT_MIGRATIONS_DIR
    introspect_schemas: Sequence[str] = ("public",)
    introspect_exclude_tables: Sequence[str] = (MIGRATIONS_TABLE,)

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


class EmailConfig(BaseModel):
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


class JWTConfig(BaseModel):
    """Configuration for JWT tokens."""

    secret: str
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 15
    refresh_token_expire_days: int = 7
    issuer: str | None = None
    audience: str | None = None


class PasswordConfig(BaseModel):
    """Configuration for password validation."""

    min_length: int = 8
    max_length: int = 128
    require_uppercase: bool = False
    require_lowercase: bool = False
    require_digit: bool = False
    require_special: bool = False


class GoogleOAuthConfig(BaseModel):
    """Configuration for Google OAuth."""

    client_id: str
    client_secret: str
    redirect_uri: str
    scopes: Sequence[str] = ("openid", "email", "profile")


class GitHubOAuthConfig(BaseModel):
    """Configuration for GitHub OAuth."""

    client_id: str
    client_secret: str
    redirect_uri: str
    scopes: Sequence[str] = ("user:email",)


class AuthConfig(BaseModel):
    """Main configuration for the auth module."""

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


class StorageConfig(BaseModel):
    """Storage configuration."""

    endpoint_url: str | None = None
    service_name: str = "s3"
    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None
    region: str = "auto"
    use_ssl: bool = True
    verify: bool | str = True


class PaymentsConfig(BaseModel):
    """Payments configuration."""

    api_key: str
    webhook_secret: str | None = None
    max_network_retries: int = 2
    timeout_seconds: float = 30.0


class ValkeyMode(StrEnum):
    """Valkey deployment mode."""

    STANDALONE = "standalone"
    CLUSTER = "cluster"


class ValkeyConfig(BaseModel):
    """Configuration for Valkey GLIDE connections."""

    addresses: Sequence[tuple[str, int]] = (("localhost", 6379),)
    username: str | None = None
    password: str | None = None
    use_tls: bool = False
    mode: ValkeyMode = ValkeyMode.STANDALONE


class KVConfig(BaseModel):
    """KV configuration."""

    valkey: ValkeyConfig | None = None


class DerpConfig(BaseModel):
    """Derp configuration."""

    database: DatabaseConfig
    email: EmailConfig | None = None
    storage: StorageConfig | None = None
    auth: AuthConfig | None = None
    kv: KVConfig | None = None
    payments: PaymentsConfig | None = None

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

# [auth]
# user_table_name = "users"

# [auth.jwt]
# secret = "$JWT_SECRET"

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
"""
