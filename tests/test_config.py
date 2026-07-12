"""Tests for Derp configuration loading."""

from __future__ import annotations

import warnings
from pathlib import Path

import pytest
from pydantic import ValidationError

from derp.config import (
    CeleryConfig,
    ConfigError,
    ConfigWarning,
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


def test_storage_public_urls_schema(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "derp.toml"
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://example")
    monkeypatch.setenv("ASSETS_PUBLIC_URL", "https://assets.example.com")

    _write_config(
        config_path,
        """
[database]
db_url = "$TEST_DATABASE_URL"
schema_path = "src/schema.py"

[storage]
endpoint_url = "https://s3.amazonaws.com"

[storage.public_urls]
assets = "$ASSETS_PUBLIC_URL"
avatars = "https://avatars.example.com"
""",
    )

    config = DerpConfig.load(config_path)
    assert config.storage is not None
    assert config.storage.public_urls == {
        "assets": "https://assets.example.com",
        "avatars": "https://avatars.example.com",
    }


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


def test_extra_fields_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "derp.toml"
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://example")
    monkeypatch.setenv("TEST_AI_KEY", "sk-test")

    _write_config(
        config_path,
        """
[database]
db_url = "$TEST_DATABASE_URL"
schema_path = "src/schema.py"

[ai]
api_key = "$TEST_AI_KEY"

[ai.model]
token_id = "tok"
token_secret = "sec"
""",
    )

    with pytest.raises(ConfigError):
        DerpConfig.load(config_path)


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


_OFFLINE_CONFIG = """
[database]
db_url = "$TEST_DATABASE_URL"
schema_path = "src/schema.py"

[ai]
api_key = "sk-test"
fal_api_key = "$FAL_KEY"

[storage]
region = "us-east-1"
access_key_id = "$R2_KEY"
secret_access_key = "$R2_SECRET"
"""


class TestStrictEnvResolution:
    """``derp db generate`` and ``check`` never read [ai] or [storage], so an
    unset $FAL_KEY must not stop them from running offline. Everything that
    touches the database keeps the strict default.
    """

    @pytest.fixture
    def offline_config(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
        monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://example")
        for name in ("FAL_KEY", "R2_KEY", "R2_SECRET"):
            monkeypatch.delenv(name, raising=False)
        config_path = tmp_path / "derp.toml"
        _write_config(config_path, _OFFLINE_CONFIG)
        return config_path

    def test_non_strict_load_succeeds(self, offline_config: Path) -> None:
        with pytest.warns(ConfigWarning):
            config = DerpConfig.load(offline_config, strict=False)
        assert config.database.db_url == "postgresql://example"

    def test_non_strict_load_warns_naming_each_section(
        self, offline_config: Path
    ) -> None:
        with pytest.warns(ConfigWarning) as record:
            DerpConfig.load(offline_config, strict=False)
        message = str(record[0].message)
        assert "[ai] $FAL_KEY" in message
        assert "[storage] $R2_KEY, $R2_SECRET" in message

    def test_unresolved_values_keep_their_literal(self, offline_config: Path) -> None:
        with pytest.warns(ConfigWarning):
            config = DerpConfig.load(offline_config, strict=False)
        assert config.ai is not None
        assert config.ai.fal_api_key == "$FAL_KEY"

    def test_missing_env_is_recorded_per_section(self, offline_config: Path) -> None:
        with pytest.warns(ConfigWarning):
            config = DerpConfig.load(offline_config, strict=False)
        assert config.missing_env == {
            "ai": ["FAL_KEY"],
            "storage": ["R2_KEY", "R2_SECRET"],
        }

    def test_non_strict_load_still_warns_for_the_database_section(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A non-strict load tolerates *everything*, database included. The
        caller is asserting it won't read those values."""
        for name in ("TEST_DATABASE_URL", "FAL_KEY", "R2_KEY", "R2_SECRET"):
            monkeypatch.delenv(name, raising=False)
        config_path = tmp_path / "derp.toml"
        _write_config(config_path, _OFFLINE_CONFIG)

        with pytest.warns(ConfigWarning, match=r"\[database\] \$TEST_DATABASE_URL"):
            config = DerpConfig.load(config_path, strict=False)
        assert config.database.db_url == "$TEST_DATABASE_URL"

    def test_default_load_is_strict(self, offline_config: Path) -> None:
        """Runtime paths (DerpClient) must keep the eager, strict behaviour."""
        with pytest.raises(ConfigError, match="FAL_KEY"):
            DerpConfig.load(offline_config)

    def test_strict_error_names_every_section(self, offline_config: Path) -> None:
        with pytest.raises(ConfigError) as exc:
            DerpConfig.load(offline_config)
        assert "[ai] $FAL_KEY" in str(exc.value)
        assert "[storage] $R2_KEY, $R2_SECRET" in str(exc.value)

    def test_fully_resolved_config_does_not_warn(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://example")
        config_path = tmp_path / "derp.toml"
        _write_config(
            config_path,
            '[database]\ndb_url = "$TEST_DATABASE_URL"\nschema_path = "s.py"\n',
        )

        with warnings.catch_warnings():
            warnings.simplefilter("error", ConfigWarning)
            config = DerpConfig.load(config_path, strict=False)
        assert config.missing_env == {}
