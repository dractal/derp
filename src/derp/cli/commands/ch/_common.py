"""Shared helpers for the ``derp ch ...`` ClickHouse commands."""

from __future__ import annotations

from pathlib import Path

import typer

from derp.chorm.engine import ClickHouseEngine
from derp.chorm.loader import discover_tables
from derp.chorm.table import Table
from derp.config import ClickHouseConfig, ConfigError, DerpConfig


def load_config() -> DerpConfig:
    """Load ``derp.toml`` or exit with a friendly error."""
    try:
        return DerpConfig.load()
    except ConfigError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


def require_clickhouse(config: DerpConfig) -> ClickHouseConfig:
    """Return the ClickHouse config section or exit if it is missing."""
    if config.clickhouse is None:
        typer.echo("Error: no [clickhouse] section in derp.toml.", err=True)
        raise typer.Exit(1)
    return config.clickhouse


def make_engine(cfg: ClickHouseConfig) -> ClickHouseEngine:
    """Construct a ClickHouseEngine from config (not yet connected)."""
    return ClickHouseEngine(
        cfg.url,
        host=cfg.host,
        port=cfg.port,
        username=cfg.username,
        password=cfg.password,
        database=cfg.database,
        secure=cfg.secure,
    )


def load_tables(cfg: ClickHouseConfig) -> list[type[Table]]:
    """Discover chorm tables from ``clickhouse.schema_path`` or exit."""
    if cfg.schema_path is None:
        typer.echo(
            "Error: set `schema_path` under [clickhouse] in derp.toml.", err=True
        )
        raise typer.Exit(1)
    try:
        tables = discover_tables(cfg.schema_path)
    except FileNotFoundError:
        typer.echo(f"Error: schema path not found: {cfg.schema_path}", err=True)
        raise typer.Exit(1)
    if not tables:
        typer.echo(f"No chorm Table classes found in {cfg.schema_path}", err=True)
        raise typer.Exit(1)
    return tables


def migrations_dir(cfg: ClickHouseConfig) -> Path:
    return Path(cfg.migrations_dir)
