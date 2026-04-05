"""Tests for Derp configuration loading."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from derp.config import (
    CeleryConfig,
    ConfigError,
    DerpConfig,
    QueueConfig,
    VercelQueueConfig,
)


def _write_config(path: Path, content: str) -> None:
    path.write_text(content)


def test_env_resolution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "derp.toml"
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://example")

    _write_config(
        config_path,
        """
[database]
db_url = "$TEST_DATABASE_URL"
schema_path = "src/schema.py"
migrations_dir = "./migrations"
introspect_schemas = ["public"]
""",
    )

    config = DerpConfig.load(config_path)
    assert config.database.db_url == "postgresql://example"
    assert config.database.schema_path == "src/schema.py"
    assert config.database.migrations_dir == "./migrations"
    assert config.database.introspect_schemas == ["public"]
    assert config.auth is None
    assert config.storage is None
    assert config.payments is None


def test_missing_env_raises(tmp_path: Path) -> None:
    config_path = tmp_path / "derp.toml"
    _write_config(
        config_path,
        """
[database]
db_url = "$MISSING_DATABASE_URL"
schema_path = "src/schema.py"
""",
    )

    with pytest.raises(ConfigError):
        DerpConfig.load(config_path)


def test_auth_schema(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "derp.toml"
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://example")
    monkeypatch.setenv("SMTP_PASSWORD", "secret")
    monkeypatch.setenv("JWT_SECRET", "jwt-secret")

    _write_config(
        config_path,
        """
[database]
db_url = "$TEST_DATABASE_URL"
schema_path = "src/schema.py"

[email]
site_name = "Test"
site_url = "https://example.com"
from_email = "noreply@example.com"
smtp_host = "smtp.example.com"
smtp_port = 587
smtp_user = "smtp_user"
smtp_password = "$SMTP_PASSWORD"

[auth.native.jwt]
secret = "$JWT_SECRET"
""",
    )

    config = DerpConfig.load(config_path)
    assert config.auth is not None
    assert config.auth.native is not None


def test_payments_schema(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "derp.toml"
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://example")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_123")
    monkeypatch.setenv("STRIPE_WEBHOOK_SECRET", "whsec_123")

    _write_config(
        config_path,
        """
[database]
db_url = "$TEST_DATABASE_URL"
schema_path = "src/schema.py"

[payments]
api_key = "$STRIPE_SECRET_KEY"
webhook_secret = "$STRIPE_WEBHOOK_SECRET"
max_network_retries = 3
timeout_seconds = 45.5
""",
    )

    config = DerpConfig.load(config_path)
    assert config.payments is not None
    assert config.payments.api_key == "sk_test_123"
    assert config.payments.webhook_secret == "whsec_123"
    assert config.payments.max_network_retries == 3
    assert config.payments.timeout_seconds == 45.5


def test_empty_env_var_resolves_to_empty_string(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "derp.toml"
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://example")
    monkeypatch.setenv("TEST_REPLICA_URL", "")

    _write_config(
        config_path,
        """
[database]
db_url = "$TEST_DATABASE_URL"
schema_path = "src/schema.py"
replica_url = "$TEST_REPLICA_URL"
""",
    )

    config = DerpConfig.load(config_path)
    assert config.database.replica_url == ""


def test_queue_config_rejects_both_backends() -> None:
    with pytest.raises(ValidationError, match="Only one queue backend"):
        QueueConfig(
            celery=CeleryConfig(broker_url="redis://localhost:6379/0"),
            vercel=VercelQueueConfig(api_token="tok_test"),
        )


def test_queue_config_accepts_single_backend() -> None:
    config = QueueConfig(celery=CeleryConfig(broker_url="redis://localhost:6379/0"))
    assert config.celery is not None
    assert config.vercel is None

    config = QueueConfig(vercel=VercelQueueConfig(api_token="tok_test"))
    assert config.vercel is not None
    assert config.celery is None
