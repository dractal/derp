"""Tests for Derp configuration loading."""

from __future__ import annotations

from pathlib import Path

import pytest

from derp.config import ConfigError, DerpConfig


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

[database.migrations]
dir = "./migrations"

[database.introspect]
schemas = ["public"]
""",
    )

    config = DerpConfig.load(config_path)
    assert config.database.db_url == "postgresql://example"
    assert config.database.schema_path == "src/schema.py"
    assert config.database.migrations.dir == "./migrations"
    assert config.database.introspect.schemas == ["public"]
    assert config.auth is None
    assert config.storage is None


def test_missing_env_raises(tmp_path: Path) -> None:
    config_path = tmp_path / "derp.toml"
    _write_config(
        config_path,
        """
[database]
db_url = "$MISSING_DATABASE_URL"

[database.migrations]
dir = "./migrations"
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

[database.migrations]
dir = "./migrations"

[database.introspect]
schemas = ["public"]

[auth]
user_table_name = "users"

[auth.email]
site_name = "Test"
site_url = "https://example.com"
from_email = "noreply@example.com"
smtp_host = "smtp.example.com"
smtp_port = 587
smtp_user = "smtp_user"
smtp_password = "$SMTP_PASSWORD"

[auth.jwt]
secret = "$JWT_SECRET"
""",
    )

    config = DerpConfig.load(config_path)
    assert config.auth is not None
