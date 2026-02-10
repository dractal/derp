"""Tests for Derp Studio command and app factory."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from fastapi import FastAPI
from typer.testing import CliRunner

from derp.cli.main import app

runner = CliRunner()


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
