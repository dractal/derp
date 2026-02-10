"""Studio command - launch the Derp Studio web interface."""

from __future__ import annotations

from typing import Annotated

import typer
import uvicorn

from derp.config import ConfigError, DerpConfig


def studio(
    host: Annotated[
        str, typer.Option("--host", "-h", help="Host to bind to")
    ] = "127.0.0.1",
    port: Annotated[int, typer.Option("--port", "-p", help="Port to bind to")] = 4983,
) -> None:
    """Launch Derp Studio - a web UI for browsing your database."""

    # Validate that the config file can be loaded.
    try:
        DerpConfig.load()
    except ConfigError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)

    from derp.studio.server import app

    uvicorn.run(app, host=host, port=port)
