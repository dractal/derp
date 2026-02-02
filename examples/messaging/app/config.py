"""Application configuration."""

from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment."""

    # Database
    database_url: str = "postgresql://localhost:5432/messaging"

    # Storage (S3-compatible)
    storage_endpoint_url: str = "http://localhost:9000"
    storage_access_key_id: str = "minioadmin"
    storage_secret_access_key: str = "minioadmin"
    storage_region: str = "us-east-1"
    storage_bucket: str = "avatars"

    # JWT
    jwt_secret: str = "change-me-in-production"

    # Site
    site_name: str = "Messaging App"
    site_url: str = "http://localhost:8000"

    # SMTP
    smtp_host: str = "localhost"
    smtp_port: int = 1025
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_from_email: str = "noreply@localhost"
    smtp_from_name: str = "Messaging App"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
