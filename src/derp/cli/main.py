"""Derp CLI - Drizzle-style migration management for Derp ORM.

This CLI provides PostgreSQL migration tools matching Drizzle's approach:
- Snapshot-based schema diffing
- JSON statement intermediate representation
- Forward-only migrations
- Interactive safety prompts for destructive operations
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from dotenv import load_dotenv

from derp.cli.commands.check import check
from derp.cli.commands.drop import drop
from derp.cli.commands.generate import generate
from derp.cli.commands.migrate import migrate
from derp.cli.commands.pull import pull
from derp.cli.commands.push import push
from derp.cli.commands.rollback import rollback
from derp.cli.commands.status import status
from derp.cli.commands.studio import studio, studio_dev
from derp.config import CONFIG_FILE, create_default_config

app = typer.Typer(
    name="derp",
    help="Derp ORM - A strongly-typed async Python ORM for PostgreSQL",
    no_args_is_help=True,
)


load_dotenv(".env")

# Register commands
app.command()(generate)
app.command()(migrate)
app.command()(push)
app.command()(pull)
app.command()(status)
app.command()(check)
app.command()(drop)
app.command()(rollback)
app.command()(studio)
app.command()(studio_dev)


@app.command()
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
    typer.echo("1. Set your database URL:")
    typer.echo(
        "   export DATABASE_URL=postgresql://user:pass@localhost:5432/dbname",
    )
    typer.echo("")
    typer.echo("2. Update derp.toml with your schema path:")
    typer.echo('   schema_path = "app/*"')
    typer.echo("")
    typer.echo("3. Generate your first migration:")
    typer.echo("   derp generate --name initial")
    typer.echo("")
    typer.echo("4. Apply migrations:")
    typer.echo("   derp migrate")


@app.command()
def version() -> None:
    """Show version information."""
    try:
        from importlib.metadata import version as get_version

        ver = get_version("derp-py")
    except Exception:
        ver = "unknown"

    typer.echo(f"derp version {ver}")


if __name__ == "__main__":
    app()
