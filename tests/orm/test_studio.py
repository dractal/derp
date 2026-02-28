"""Tests for Derp Studio command and app factory."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from derp.cli.main import app
from derp.studio.server import create_app

runner = CliRunner()


class _FakeProcess:
    def __init__(self, poll_result: int | None = None) -> None:
        self._poll_result = poll_result
        self.terminated = False
        self.killed = False
        self.wait_calls: list[float | None] = []

    def poll(self) -> int | None:
        return self._poll_result

    def terminate(self) -> None:
        self.terminated = True
        self._poll_result = 0

    def wait(self, timeout: float | None = None) -> int | None:
        self.wait_calls.append(timeout)
        return self._poll_result

    def kill(self) -> None:
        self.killed = True
        self._poll_result = -9


def _write_studio_config(path: Path) -> None:
    path.write_text(
        """\
[database]
db_url = "$TEST_DATABASE_URL"
schema_path = "src/schema.py"
"""
    )


def test_studio_errors_when_config_missing(temp_dir: Path) -> None:
    """Studio should fail with config error when derp.toml is missing."""
    os.chdir(temp_dir)

    result = runner.invoke(app, ["studio"])

    assert result.exit_code == 1
    assert "Error: derp.toml not found in current directory" in result.output


def test_studio_errors_when_env_missing(temp_dir: Path) -> None:
    """Studio should fail when env-backed config values are not set."""
    os.chdir(temp_dir)
    _write_studio_config(temp_dir / "derp.toml")

    result = runner.invoke(app, ["studio"])

    assert result.exit_code == 1
    assert "Error: Environment variable 'TEST_DATABASE_URL'" in result.output


def test_studio_runs_uvicorn_with_host_port(
    temp_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Studio should load config and pass host/port into uvicorn."""
    os.chdir(temp_dir)
    _write_studio_config(temp_dir / "derp.toml")
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://example")

    captured: dict[str, object] = {}

    def fake_run(app: FastAPI, host: str, port: int) -> None:
        captured["app"] = app
        captured["host"] = host
        captured["port"] = port

    monkeypatch.setattr("derp.cli.commands.studio.uvicorn.run", fake_run)

    result = runner.invoke(app, ["studio", "--host", "0.0.0.0", "--port", "9001"])

    assert result.exit_code == 0
    assert isinstance(captured.get("app"), FastAPI)
    assert captured["host"] == "0.0.0.0"
    assert captured["port"] == 9001


def test_studio_dev_errors_when_config_missing(temp_dir: Path) -> None:
    """Studio dev should fail with config error when derp.toml is missing."""
    os.chdir(temp_dir)

    result = runner.invoke(app, ["studio-dev"])

    assert result.exit_code == 1
    assert "Error: derp.toml not found in current directory" in result.output


def test_studio_dev_errors_when_bun_missing(
    temp_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Studio dev should fail when bun is unavailable."""
    os.chdir(temp_dir)
    _write_studio_config(temp_dir / "derp.toml")
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://example")
    monkeypatch.setattr("derp.cli.commands.studio.shutil.which", lambda _: None)
    result = runner.invoke(app, ["studio-dev"])

    assert result.exit_code == 1
    assert "`bun` is required" in result.output


def test_studio_dev_runs_frontend_and_backend(
    temp_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Studio dev should start frontend process and backend reload server."""
    os.chdir(temp_dir)
    _write_studio_config(temp_dir / "derp.toml")
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://example")
    monkeypatch.setattr(
        "derp.cli.commands.studio.shutil.which", lambda _: "/usr/local/bin/bun"
    )
    monkeypatch.setattr("derp.cli.commands.studio.time.sleep", lambda _: None)

    fake_process = _FakeProcess()
    captured: dict[str, Any] = {}

    def fake_popen(cmd: list[str], cwd: Path, env: dict[str, str]) -> _FakeProcess:
        captured["cmd"] = cmd
        captured["cwd"] = cwd
        captured["env"] = env
        return fake_process

    def fake_run(app: FastAPI, host: str, port: int) -> None:
        captured["app"] = app
        captured["host"] = host
        captured["port"] = port

    monkeypatch.setattr("derp.cli.commands.studio.subprocess.Popen", fake_popen)
    monkeypatch.setattr("derp.cli.commands.studio.uvicorn.run", fake_run)

    result = runner.invoke(
        app,
        [
            "studio-dev",
            "--host",
            "0.0.0.0",
            "--backend-port",
            "9001",
            "--frontend-port",
            "5174",
        ],
    )

    assert result.exit_code == 0
    assert captured["cmd"] == [
        "/usr/local/bin/bun",
        "run",
        "dev",
        "--",
        "--host",
        "0.0.0.0",
        "--port",
        "5174",
        "--strictPort",
    ]
    assert Path(captured["cwd"]).as_posix().endswith("/src/derp/studio/ui")
    env = captured["env"]
    assert isinstance(env, dict)
    assert env["PUBLIC_API_URL"] == "http://0.0.0.0:9001"
    assert env["NODE_ENV"] == "development"
    assert isinstance(captured.get("app"), FastAPI)
    assert captured["host"] == "0.0.0.0"
    assert captured["port"] == 9001
    assert fake_process.terminated is True
    assert fake_process.wait_calls == [5]


def test_studio_dev_errors_when_frontend_exits_early(
    temp_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Studio dev should fail fast when frontend process exits early."""
    os.chdir(temp_dir)
    _write_studio_config(temp_dir / "derp.toml")
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://example")
    monkeypatch.setattr(
        "derp.cli.commands.studio.shutil.which", lambda _: "/usr/local/bin/bun"
    )
    monkeypatch.setattr("derp.cli.commands.studio.time.sleep", lambda _: None)

    fake_process = _FakeProcess(poll_result=1)
    captured: dict[str, Any] = {"uvicorn_called": False}

    def fake_run(*_: object, **__: object) -> None:
        captured["uvicorn_called"] = True

    monkeypatch.setattr(
        "derp.cli.commands.studio.subprocess.Popen",
        lambda *args, **kwargs: fake_process,
    )
    monkeypatch.setattr("derp.cli.commands.studio.uvicorn.run", fake_run)

    result = runner.invoke(app, ["studio-dev"])

    assert result.exit_code == 1
    assert "Frontend dev server exited early" in result.output
    assert captured["uvicorn_called"] is False
    assert fake_process.terminated is False


def _write_index(path: Path) -> None:
    path.write_text(
        """\
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Derp Studio</title>
  </head>
  <body>
    <div id="root"></div>
  </body>
</html>
"""
    )


def test_studio_serves_index_for_root(temp_dir: Path) -> None:
    """Studio should serve index.html at root."""
    static_dir = temp_dir / "static"
    static_dir.mkdir()
    _write_index(static_dir / "index.html")

    client = TestClient(create_app(static_dir=static_dir, enable_lifespan=False))
    response = client.get("/")

    assert response.status_code == 200
    assert '<div id="root"></div>' in response.text


def test_studio_serves_index_for_deep_spa_route(temp_dir: Path) -> None:
    """Studio should serve index.html for BrowserRouter deep links."""
    static_dir = temp_dir / "static"
    static_dir.mkdir()
    _write_index(static_dir / "index.html")

    client = TestClient(create_app(static_dir=static_dir, enable_lifespan=False))
    response = client.get("/tables/users")

    assert response.status_code == 200
    assert "<!doctype html>" in response.text


def test_studio_unknown_api_route_returns_404(temp_dir: Path) -> None:
    """Unknown API routes should stay 404s."""
    static_dir = temp_dir / "static"
    static_dir.mkdir()
    _write_index(static_dir / "index.html")

    client = TestClient(create_app(static_dir=static_dir, enable_lifespan=False))
    response = client.get("/api/missing")

    assert response.status_code == 404


def test_studio_file_like_path_returns_404(temp_dir: Path) -> None:
    """File-like paths should not be routed to the SPA fallback."""
    static_dir = temp_dir / "static"
    static_dir.mkdir()
    _write_index(static_dir / "index.html")

    client = TestClient(create_app(static_dir=static_dir, enable_lifespan=False))
    response = client.get("/favicon.ico")

    assert response.status_code == 404


def test_studio_missing_build_returns_503(temp_dir: Path) -> None:
    """Studio should return actionable error when frontend build is missing."""
    static_dir = temp_dir / "static"
    static_dir.mkdir()

    client = TestClient(create_app(static_dir=static_dir, enable_lifespan=False))
    root_response = client.get("/")
    deep_response = client.get("/tables/users")

    assert root_response.status_code == 503
    assert deep_response.status_code == 503
    assert "Run `./scripts/build_studio.sh`" in root_response.text
    assert "Run `./scripts/build_studio.sh`" in deep_response.text


def test_studio_missing_static_asset_returns_404(temp_dir: Path) -> None:
    """Missing static assets should not be routed to the SPA fallback."""
    static_dir = temp_dir / "static"
    static_dir.mkdir()
    _write_index(static_dir / "index.html")

    client = TestClient(create_app(static_dir=static_dir, enable_lifespan=False))
    response = client.get("/static/missing.js")

    assert response.status_code == 404


# --- Database API endpoint tests ---


def _make_mock_derp() -> MagicMock:
    """Create a mock DerpClient with database engine."""
    mock = MagicMock()
    mock.db = MagicMock()
    mock.db.pool = MagicMock()
    mock.db.execute = AsyncMock(return_value=[])
    mock._storage = None
    mock._email = None
    mock.config = MagicMock()
    mock.config.database.introspect_schemas = ("public",)
    mock.config.database.introspect_exclude_tables = ("derp_migrations",)
    mock.config.email = None
    mock.config.model_dump.return_value = {"database": {"db_url": "test"}}
    return mock


def _create_app_with_mock_derp(
    temp_dir: Path, mock_derp: MagicMock
) -> TestClient:
    static_dir = temp_dir / "static"
    static_dir.mkdir(exist_ok=True)
    _write_index(static_dir / "index.html")
    studio_app = create_app(static_dir=static_dir, enable_lifespan=False)
    studio_app.state.derp_client = mock_derp
    return TestClient(studio_app)


def test_email_templates_endpoint_requires_config(temp_dir: Path) -> None:
    """Email templates endpoint should fail when email is not configured."""
    mock_derp = _make_mock_derp()

    client = _create_app_with_mock_derp(temp_dir, mock_derp)
    response = client.get("/api/email/templates")

    assert response.status_code == 400
    assert response.json()["detail"] == "Email is not configured."


def test_database_tables_endpoint(temp_dir: Path) -> None:
    """Database tables endpoint should return introspected tables with row counts."""
    from derp.orm.migrations.snapshot.models import (
        ColumnSnapshot,
        SchemaSnapshot,
        TableSnapshot,
    )

    mock_derp = _make_mock_derp()

    snapshot = SchemaSnapshot(schemas=["public"])
    snapshot.tables = {
        "users": TableSnapshot(
            name="users",
            schema_name="public",
            columns={
                "id": ColumnSnapshot(
                    name="id", type="serial", primary_key=True, not_null=True
                ),
                "name": ColumnSnapshot(
                    name="name", type="varchar(255)", not_null=True
                ),
            },
        )
    }

    mock_derp.db.execute = AsyncMock(return_value=[{"cnt": 42}])

    with patch(
        "derp.studio.server.PostgresIntrospector"
    ) as mock_introspector_cls:
        mock_introspector = MagicMock()
        mock_introspector.introspect = AsyncMock(return_value=snapshot)
        mock_introspector_cls.return_value = mock_introspector

        client = _create_app_with_mock_derp(temp_dir, mock_derp)
        response = client.get("/api/database/tables")

    assert response.status_code == 200
    data = response.json()
    assert len(data["tables"]) == 1

    table = data["tables"][0]
    assert table["name"] == "users"
    assert table["schema"] == "public"
    assert table["row_count"] == 42
    assert len(table["columns"]) == 2

    id_col = table["columns"][0]
    assert id_col["name"] == "id"
    assert id_col["type"] == "serial"
    assert id_col["primary_key"] is True
    assert id_col["not_null"] is True


def test_database_table_rows_endpoint(temp_dir: Path) -> None:
    """Database rows endpoint should return paginated rows."""
    mock_derp = _make_mock_derp()
    mock_derp.db.execute = AsyncMock(
        side_effect=[
            [{"cnt": 100}],  # count query
            [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],  # rows
        ]
    )

    client = _create_app_with_mock_derp(temp_dir, mock_derp)
    response = client.get("/api/database/tables/users/rows?limit=2&offset=0")

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 100
    assert data["limit"] == 2
    assert data["offset"] == 0
    assert len(data["rows"]) == 2
    assert data["rows"][0]["name"] == "Alice"


def test_database_table_rows_default_limit(temp_dir: Path) -> None:
    """Database rows endpoint should default to limit=50, offset=0."""
    mock_derp = _make_mock_derp()
    mock_derp.db.execute = AsyncMock(
        side_effect=[
            [{"cnt": 0}],  # count query
            [],  # rows
        ]
    )

    client = _create_app_with_mock_derp(temp_dir, mock_derp)
    response = client.get("/api/database/tables/users/rows")

    assert response.status_code == 200
    data = response.json()
    assert data["limit"] == 50
    assert data["offset"] == 0


def test_database_table_rows_clamps_limit(temp_dir: Path) -> None:
    """Database rows endpoint should clamp limit to 1-500 range."""
    mock_derp = _make_mock_derp()
    mock_derp.db.execute = AsyncMock(
        side_effect=[
            [{"cnt": 0}],
            [],
        ]
    )

    client = _create_app_with_mock_derp(temp_dir, mock_derp)
    response = client.get("/api/database/tables/users/rows?limit=9999")

    assert response.status_code == 200
    assert response.json()["limit"] == 500
