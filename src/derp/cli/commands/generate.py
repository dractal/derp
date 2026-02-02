"""Generate migration command."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Annotated

import typer

from derp.cli.config import Config
from derp.orm.loader import load_tables

# Import all convertors to register them
from derp.orm.migrations.convertors import (  # noqa: F401
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
from derp.orm.migrations.convertors.base import ConvertorRegistry
from derp.orm.migrations.journal import (
    get_next_version,
    load_journal,
    load_latest_snapshot,
    save_journal,
    save_migration_sql,
    save_snapshot,
)
from derp.orm.migrations.safety import (
    detect_destructive_operations,
    format_destructive_warnings,
    has_high_risk_operations,
)
from derp.orm.migrations.snapshot.differ import SnapshotDiffer
from derp.orm.migrations.snapshot.models import ColumnSnapshot, SchemaSnapshot
from derp.orm.migrations.snapshot.serializer import serialize_schema


def _columns_match(old_col: ColumnSnapshot, new_col: ColumnSnapshot) -> bool:
    """Check if two columns are similar enough to be a rename candidate."""
    return (
        old_col.type == new_col.type
        and old_col.not_null == new_col.not_null
        and old_col.default == new_col.default
    )


def _find_rename_candidates(
    old_snapshot: SchemaSnapshot, new_snapshot: SchemaSnapshot
) -> list[tuple[str, str, str, str]]:
    """Find potential column renames between snapshots.

    Returns list of (table_name, old_col, new_col, col_type) tuples.
    """
    candidates: list[tuple[str, str, str, str]] = []

    # Find tables that exist in both snapshots
    common_tables = set(old_snapshot.tables.keys()) & set(new_snapshot.tables.keys())

    for table_key in common_tables:
        old_table = old_snapshot.tables[table_key]
        new_table = new_snapshot.tables[table_key]

        old_cols = set(old_table.columns.keys())
        new_cols = set(new_table.columns.keys())

        dropped = old_cols - new_cols
        added = new_cols - old_cols

        # Find matching pairs
        for old_name in sorted(dropped):
            old_col = old_table.columns[old_name]
            for new_name in sorted(added):
                new_col = new_table.columns[new_name]
                if _columns_match(old_col, new_col):
                    candidates.append(
                        (old_table.name, old_name, new_name, old_col.type)
                    )

    return candidates


def create_rename_resolver(
    old_snapshot: SchemaSnapshot,
    new_snapshot: SchemaSnapshot,
    force: bool = False,
) -> dict[str, str]:
    """Prompt user for potential renames and return decisions.

    Returns a dict mapping "table.old_col" -> "new_col" for confirmed renames.
    """
    if force:
        # In force mode, skip prompts and treat as drop+add (safe default)
        return {}

    candidates = _find_rename_candidates(old_snapshot, new_snapshot)
    if not candidates:
        return {}

    decisions: dict[str, str] = {}
    used_pairs: set[tuple[str, str, str]] = set()  # (table, old, new) already decided

    for table_name, old_name, new_name, col_type in candidates:
        # Skip if either column is already part of a confirmed rename
        key = f"{table_name}.{old_name}"
        if key in decisions:
            continue

        # Check if either column already matched in a different pair
        skip = False
        for t, o, n in used_pairs:
            if t == table_name and (o == old_name or n == new_name):
                skip = True
                break
        if skip:
            continue

        typer.echo("")
        typer.echo(f"Potential rename detected in table '{table_name}':")
        typer.echo(f"  Column '{old_name}' ({col_type}) was removed")
        typer.echo(f"  Column '{new_name}' ({col_type}) was added")

        prompt = f"Did you rename '{old_name}' to '{new_name}'?"
        if typer.confirm(prompt, default=False):
            decisions[key] = new_name
            used_pairs.add((table_name, old_name, new_name))

    return decisions


def make_rename_callback(
    decisions: dict[str, str],
) -> Callable[[str, str, str], bool]:
    """Create a callback function for the differ from user decisions."""

    def resolver(object_type: str, old_name: str, new_name: str) -> bool:
        if object_type != "column":
            return False
        # old_name is "table.column" format
        return decisions.get(old_name) == new_name

    return resolver


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
        tables = load_tables(schema_path)
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

    # Prompt for potential column renames before diffing
    rename_decisions = create_rename_resolver(prev_snapshot, current_snapshot, force)
    rename_callback = (
        make_rename_callback(rename_decisions) if rename_decisions else None
    )

    # Diff snapshots
    differ = SnapshotDiffer(prev_snapshot, current_snapshot, rename_callback)
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
