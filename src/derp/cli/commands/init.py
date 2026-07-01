"""Init command - create a Derp configuration file."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from derp.config import CONFIG_FILE, create_default_config


def init(
    force: Annotated[
        bool, typer.Option("--force", "-f", help="Overwrite existing config")
    ] = False,
) -> None:
    """Initialize a new derp.toml configuration file.

    Creates a configuration file with sensible defaults for your project.
    """
    config_path = Path(CONFIG_FILE)

    if config_path.exists() and not force:
        typer.echo(f"{CONFIG_FILE} already exists. Use --force to overwrite.")
        raise typer.Exit(1)

    config_path.write_text(create_default_config())
    typer.echo(f"Created {CONFIG_FILE}")
    typer.echo("")
    typer.echo("Next steps:")
    typer.echo("")
    typer.echo("  1. Edit derp.toml: set `db_url` and `schema_path`.")
    typer.echo("  2. Define your tables as derp.orm.Table subclasses in that module.")
    typer.echo("  3. derp db generate --name initial   # diff schema -> migration")
    typer.echo("  4. derp db migrate                   # apply pending migrations")
    typer.echo("")
    typer.echo("Using ClickHouse? Add a [clickhouse] section and use `derp ch`")
    typer.echo("(generate / migrate / push / pull / status), e.g. `derp ch --help`.")
