"""Derp CLI - Drizzle-style migration management for Derp ORM.

This CLI provides PostgreSQL migration tools matching Drizzle's approach:
- Snapshot-based schema diffing
- JSON statement intermediate representation
- Forward-only migrations
- Interactive safety prompts for destructive operations
"""

from __future__ import annotations

import typer
from dotenv import load_dotenv

from derp.cli.commands.ch import ch_app
from derp.cli.commands.db import db_app
from derp.cli.commands.init import init
from derp.cli.commands.studio import studio, studio_dev

app = typer.Typer(
    name="derp",
    help="Derp - A strongly-typed async backend library",
    no_args_is_help=True,
)


load_dotenv(".env")

# Register commands
app.command()(init)
app.command()(studio)
app.command()(studio_dev)

# PostgreSQL migration commands: `derp db <command>`
app.add_typer(db_app)
# ClickHouse migration commands: `derp ch <command>`
app.add_typer(ch_app)


@app.command()
def version() -> None:
    """Show version information."""
    try:
        from importlib.metadata import version as get_version

        ver = get_version("derp-py")
    except Exception:
        ver = "unknown"

    typer.echo(f"derp-py {ver}")


if __name__ == "__main__":
    app()
