"""Configuration for the auth module."""

from __future__ import annotations

import dataclasses

from derp.orm.table import Table


@dataclasses.dataclass(kw_only=True)
class JWTConfig:
    """Configuration for JWT tokens."""

    secret: str
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 15
    refresh_token_expire_days: int = 7
    issuer: str | None = None
    audience: str | None = None


@dataclasses.dataclass(kw_only=True)
class PasswordConfig:
    """Configuration for password validation."""

    min_length: int = 8
    max_length: int = 128
    require_uppercase: bool = False
    require_lowercase: bool = False
    require_digit: bool = False
    require_special: bool = False


@dataclasses.dataclass(kw_only=True)
class GoogleOAuthConfig:
    """Configuration for Google OAuth."""

    client_id: str
    client_secret: str
    redirect_uri: str
    scopes: list[str] = dataclasses.field(
        default_factory=lambda: ["openid", "email", "profile"]
    )


@dataclasses.dataclass(kw_only=True)
class GitHubOAuthConfig:
    """Configuration for GitHub OAuth."""

    client_id: str
    client_secret: str
    redirect_uri: str
    scopes: list[str] = dataclasses.field(default_factory=lambda: ["user:email"])


@dataclasses.dataclass(kw_only=True)
class EmailConfig:
    """Configuration for email sending."""

    templates_dir: str | None = None

    site_name: str
    site_url: str
    confirm_email_url: str = "{site_url}/auth/confirm"
    recovery_url: str = "{site_url}/auth/recovery"
    magic_link_url: str = "{site_url}/auth/magic-link"

    enable_signup: bool = True
    enable_confirmation: bool = True
    enable_magic_link: bool = False

    from_email: str
    from_name: str
    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_password: str
    use_tls: bool = True
    start_tls: bool = False

@dataclasses.dataclass(kw_only=True)
class AuthConfig[UserT: Table]:
    """Main configuration for the auth module."""

    user_table: type[Table]
    email: EmailConfig
    jwt: JWTConfig
    password: PasswordConfig = dataclasses.field(default_factory=PasswordConfig)

    google_oauth: GoogleOAuthConfig | None = None
    github_oauth: GitHubOAuthConfig | None = None

    # Timing
    magic_link_expire_minutes: int = 60
    recovery_token_expire_minutes: int = 60
    confirmation_token_expire_hours: int = 24
    session_expire_days: int = 30
