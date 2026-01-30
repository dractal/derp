"""Drop command - remove migration files."""

from __future__ import annotations

import shutil
from typing import Annotated

import typer

from derp.cli.config import Config
from derp.orm.migrations.journal import (
    get_migration_folders,
    load_journal,
    save_journal,
)


def drop(
    version: Annotated[
        str | None, typer.Argument(help="Version to drop (e.g., '0001')")
    ] = None,
    all_migrations: Annotated[
        bool, typer.Option("--all", help="Drop all migrations")
    ] = False,
    force: Annotated[
        bool, typer.Option("--force", "-f", help="Skip confirmation")
    ] = False,
) -> None:
    """Drop migration files (does not affect database).

    Removes migration folders and updates the journal. This does NOT
    reverse any changes in the database - it only removes the files.

    Use this to:
    - Remove a migration you haven't applied yet
    - Clean up and regenerate migrations during development
    """
    config = Config.load()
    migrations_dir = config.migrations.directory

    if not migrations_dir.exists():
        typer.echo("No migrations directory found.")
        return

    journal = load_journal(migrations_dir)
    folders = get_migration_folders(migrations_dir)

    if not folders:
        typer.echo("No migration folders found.")
        return

    if all_migrations:
        _drop_all(migrations_dir, journal, folders, force)
    elif version:
        _drop_version(migrations_dir, journal, folders, version, force)
    else:
        # Interactive selection
        _drop_interactive(migrations_dir, journal, folders, force)


def _drop_all(migrations_dir, journal, folders, force: bool) -> None:
    """Drop all migrations."""
    typer.echo(f"Found {len(folders)} migration(s):")
    for version, folder in folders:
        typer.echo(f"  - {folder.name}")

    if not force:
        typer.echo("")
        typer.echo("This will delete all migration files and reset the journal.")
        typer.echo("The database will NOT be affected.")
        if not typer.confirm("Are you sure?"):
            raise typer.Abort()

    # Remove all folders
    for version, folder in folders:
        shutil.rmtree(folder)
        typer.echo(f"Deleted: {folder.name}")

    # Clear journal
    journal.entries = []
    save_journal(journal, migrations_dir)

    typer.echo("")
    typer.echo("All migrations dropped. Journal reset.")


def _drop_version(migrations_dir, journal, folders, version: str, force: bool) -> None:
    """Drop a specific version."""
    # Find folder
    target_folder = None
    for v, folder in folders:
        if v == version:
            target_folder = folder
            break

    if not target_folder:
        typer.echo(f"Migration version '{version}' not found.", err=True)
        typer.echo("Available versions:")
        for v, folder in folders:
            typer.echo(f"  - {v} ({folder.name})")
        raise typer.Exit(1)

    if not force:
        typer.echo(f"Will delete: {target_folder.name}")
        typer.echo("The database will NOT be affected.")
        if not typer.confirm("Are you sure?"):
            raise typer.Abort()

    # Remove folder
    shutil.rmtree(target_folder)
    typer.echo(f"Deleted: {target_folder.name}")

    # Update journal
    journal.remove_entry(version)
    save_journal(journal, migrations_dir)

    typer.echo("Migration dropped. Journal updated.")


def _drop_interactive(migrations_dir, journal, folders, force: bool) -> None:
    """Interactive migration selection."""
    typer.echo("Select migration to drop:")
    typer.echo("")

    for i, (version, folder) in enumerate(folders, 1):
        entry = journal.get_entry(version)
        tag = entry.tag if entry else "unknown"
        typer.echo(f"  {i}. {version} - {tag}")

    typer.echo(f"  {len(folders) + 1}. Drop all")
    typer.echo("  0. Cancel")
    typer.echo("")

    choice = typer.prompt("Enter number", type=int)

    if choice == 0:
        raise typer.Abort()
    elif choice == len(folders) + 1:
        _drop_all(migrations_dir, journal, folders, force)
    elif 1 <= choice <= len(folders):
        version, folder = folders[choice - 1]
        _drop_version(migrations_dir, journal, folders, version, force)
    else:
        typer.echo("Invalid choice.", err=True)
        raise typer.Exit(1)
