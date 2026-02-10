"""Studio command - launch the Derp Studio web interface."""

from __future__ import annotations

from typing import Annotated

import typer

from derp.config import ConfigError, DerpConfig


def studio(
    host: Annotated[
        str, typer.Option("--host", "-h", help="Host to bind to")
    ] = "127.0.0.1",
    port: Annotated[int, typer.Option("--port", "-p", help="Port to bind to")] = 4983,
) -> None:
    """Launch Derp Studio - a web UI for browsing your database."""
    try:
        config = DerpConfig.load()
    except ConfigError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)

    del config

    raise NotImplementedError("Studio is not implemented yet")
