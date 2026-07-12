"""Shared config loading for the ``derp db`` commands."""

from __future__ import annotations

import warnings
from collections.abc import Iterator
from contextlib import contextmanager

import typer

from derp.config import ConfigWarning, DerpConfig
from derp.orm.migrations.errors import SchemaError


def load_offline_config() -> DerpConfig:
    """Load ``derp.toml`` for a command that never opens a database connection.

    Unset ``$VAR`` references are tolerated and reported rather than fatal:
    ``derp db generate`` reads only ``[database]``'s paths, so an unset
    ``$FAL_KEY`` in ``[ai]`` is none of its business. The warning is rendered
    as CLI output — the file and line of the ``warnings.warn`` call inside
    derp would tell the reader nothing.

    Raises:
        ConfigError: if the file is missing or fails validation. Callers
            handle it; only the unset-variable case is downgraded here.
    """
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ConfigWarning)
        config = DerpConfig.load(strict=False)

    for warning in caught:
        typer.echo(f"Warning: {warning.message}", err=True)

    return config


@contextmanager
def schema_errors() -> Iterator[None]:
    """Report a bad schema as a CLI error rather than a traceback.

    ``SchemaError`` means the user's ``Table`` definitions cannot be turned
    into a migration — a duplicate constraint name, a collation change
    PostgreSQL cannot apply in place. That is not a crash, and a stack trace
    (which Typer renders with local variables, snapshot contents included)
    tells the reader nothing useful.
    """
    try:
        yield
    except SchemaError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc
