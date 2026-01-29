"""Generate migration command."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated

import typer

from derp.cli.config import Config

# Import all convertors to register them
from derp.migrations.convertors import (  # noqa: F401
    column,
    constraint,
    enum,
    index,
    policy,
    role,
    schema,
    sequence,
    table,
)
from derp.migrations.convertors.base import ConvertorRegistry
from derp.migrations.journal import (
    get_next_version,
    load_journal,
    load_latest_snapshot,
    save_journal,
    save_migration_sql,
    save_snapshot,
)
from derp.migrations.safety import (
    detect_destructive_operations,
    format_destructive_warnings,
    has_high_risk_operations,
)
from derp.migrations.snapshot.differ import SnapshotDiffer
from derp.migrations.snapshot.models import SchemaSnapshot
from derp.migrations.snapshot.serializer import serialize_schema
from derp.orm.schema import load_tables_from_module


def generate(
    name: Annotated[
        str, typer.Option("--name", "-n", help="Migration name")
    ] = "migration",
    force: Annotated[
        bool,
        typer.Option("--force", "-f", help="Skip confirmation for destructive changes"),
    ] = False,
    custom: Annotated[
        bool, typer.Option("--custom", help="Generate empty migration for custom SQL")
    ] = False,
) -> None:
    """Generate a new migration from schema changes.

    Compares your current schema definition against the latest snapshot
    and generates migration SQL with a new snapshot file.
    """
    config = Config.load()
    migrations_dir = config.migrations.directory
    schema_path = config.migrations.get_schema_path()

    if custom:
        _generate_custom_migration(config, name)
        return

    # Load tables from schema module
    try:
        tables = load_tables_from_module(schema_path)
    except FileNotFoundError:
        typer.echo(f"Error: Schema file not found: {schema_path}", err=True)
        raise typer.Exit(1)

    if not tables:
        typer.echo(f"No Table classes found in {schema_path}", err=True)
        raise typer.Exit(1)

    typer.echo(f"Found {len(tables)} table(s): {', '.join(t.__name__ for t in tables)}")

    # Load journal and previous snapshot
    journal = load_journal(migrations_dir)
    prev_snapshot_data = load_latest_snapshot(migrations_dir, journal)

    if prev_snapshot_data:
        prev_snapshot = SchemaSnapshot.model_validate(prev_snapshot_data)
        typer.echo(f"Previous snapshot: {journal.get_latest_version()}")
    else:
        prev_snapshot = SchemaSnapshot()
        typer.echo("No previous snapshot found, creating initial migration")

    # Serialize current schema to snapshot
    next_version = get_next_version(journal)
    current_snapshot = serialize_schema(
        tables,
        schema="public",
        snapshot_id=next_version,
        prev_id=prev_snapshot.id if prev_snapshot.id else None,
    )

    # Diff snapshots
    differ = SnapshotDiffer(prev_snapshot, current_snapshot)
    statements = differ.diff()

    if not statements:
        typer.echo("No changes detected. Schema is up to date.")
        return

    typer.echo(f"Detected {len(statements)} change(s)")

    # Check for destructive operations
    destructive = detect_destructive_operations(statements)
    if destructive:
        typer.echo("")
        typer.echo(format_destructive_warnings(destructive), err=True)

        if has_high_risk_operations(destructive) and not force:
            if not typer.confirm("Continue with potentially destructive changes?"):
                raise typer.Abort()

    # Generate SQL
    sql = ConvertorRegistry.convert_all(statements)

    # Add header
    header = f"""\
-- Migration: {name}
-- Version: {next_version}
-- Generated at: {datetime.now(UTC).isoformat()}
-- Previous: {prev_snapshot.id or "none"}

"""
    full_sql = header + sql

    # Save files
    migrations_dir.mkdir(parents=True, exist_ok=True)

    # Save snapshot and SQL
    folder_path = save_snapshot(
        migrations_dir,
        next_version,
        name,
        current_snapshot.model_dump(mode="json", by_alias=True),
    )
    sql_path = save_migration_sql(folder_path, full_sql)

    # Update journal
    journal.add_entry(version=next_version, tag=name)
    save_journal(journal, migrations_dir)

    typer.echo("")
    typer.echo(f"Created migration: {folder_path.name}/")
    typer.echo(f"  - {sql_path.name}")
    typer.echo("  - snapshot.json")
    typer.echo("")
    typer.echo("Review the migration and run 'derp migrate' to apply.")


def _generate_custom_migration(config: Config, name: str) -> None:
    """Generate an empty migration for custom SQL."""
    migrations_dir = config.migrations.directory
    journal = load_journal(migrations_dir)

    next_version = get_next_version(journal)

    # Create empty migration
    header = f"""\
-- Migration: {name}
-- Version: {next_version}
-- Generated at: {datetime.now(UTC).isoformat()}
-- Custom migration - add your SQL below

"""

    migrations_dir.mkdir(parents=True, exist_ok=True)

    # Load or create snapshot
    prev_snapshot_data = load_latest_snapshot(migrations_dir, journal)
    if prev_snapshot_data:
        snapshot = prev_snapshot_data
        snapshot["id"] = next_version
        snapshot["prev_id"] = prev_snapshot_data.get("id")
    else:
        snapshot = SchemaSnapshot(id=next_version).model_dump(
            mode="json", by_alias=True
        )

    folder_path = save_snapshot(migrations_dir, next_version, name, snapshot)
    sql_path = save_migration_sql(folder_path, header)

    journal.add_entry(version=next_version, tag=name)
    save_journal(journal, migrations_dir)

    typer.echo(f"Created custom migration: {folder_path.name}/")
    typer.echo(f"Edit {sql_path} to add your SQL statements.")
