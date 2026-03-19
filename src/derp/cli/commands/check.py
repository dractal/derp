"""Check command - validate schema is synced with snapshots."""

from __future__ import annotations

from pathlib import Path

import typer

from derp.config import ConfigError, DerpConfig
from derp.orm.loader import discover_tables
from derp.orm.migrations.journal import load_journal, load_latest_snapshot
from derp.orm.migrations.snapshot.differ import SnapshotDiffer
from derp.orm.migrations.snapshot.models import SchemaSnapshot
from derp.orm.migrations.snapshot.normalize import get_normalizer
from derp.orm.migrations.snapshot.serializer import serialize_schema
from derp.orm.migrations.statements.types import (
    AddColumnStatement,
    AlterColumnDefaultStatement,
    AlterColumnNullableStatement,
    AlterColumnTypeStatement,
    AlterEnumAddValueStatement,
    CreateEnumStatement,
    CreateForeignKeyStatement,
    CreateIndexStatement,
    CreatePolicyStatement,
    CreateSchemaStatement,
    CreateSequenceStatement,
    CreateTableStatement,
    CreateUniqueConstraintStatement,
    DisableRLSStatement,
    DropColumnStatement,
    DropEnumStatement,
    DropForeignKeyStatement,
    DropIndexStatement,
    DropPolicyStatement,
    DropSchemaStatement,
    DropSequenceStatement,
    DropTableStatement,
    DropUniqueConstraintStatement,
    EnableRLSStatement,
    RenameColumnStatement,
    RenameTableStatement,
)


def check() -> None:
    """Check for schema changes without generating migration.

    Compares your current schema definition against the latest snapshot
    and reports any differences. Useful for CI/CD pipelines to verify
    schema is in sync before deployment.

    Exit codes:
      0 - Schema is up to date
      1 - Schema changes detected (migration needed)
    """
    try:
        config = DerpConfig.load()
    except ConfigError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)

    migrations_dir = Path(config.database.migrations_dir)
    schema_path = config.database.schema_path

    # Load tables from schema module
    try:
        tables = discover_tables(
            schema_path,
            include_auth=config.auth is not None and config.auth.native is not None,
        )
    except FileNotFoundError:
        typer.echo(f"Error: Schema file not found: {schema_path}", err=True)
        raise typer.Exit(1)

    if not tables:
        typer.echo(f"No Table classes found in {schema_path}", err=True)
        raise typer.Exit(1)

    # Load journal and previous snapshot
    journal = load_journal(migrations_dir)
    prev_snapshot_data = load_latest_snapshot(migrations_dir, journal)

    if prev_snapshot_data is None:
        typer.echo(
            "No snapshots found. Run 'derp generate' to create initial migration.",
            err=True,
        )
        raise typer.Exit(1)

    prev_snapshot = SchemaSnapshot.model_validate(prev_snapshot_data)

    # Serialize current schema
    current_snapshot = serialize_schema(tables, schema="public")

    # Normalize for comparison
    normalizer = get_normalizer("postgresql")
    prev_norm = normalizer.normalize(prev_snapshot)
    current_norm = normalizer.normalize(current_snapshot)

    # Diff
    differ = SnapshotDiffer(prev_norm, current_norm)
    statements = differ.diff()

    if not statements:
        typer.echo("Schema is up to date. No changes detected.")
        raise typer.Exit(0)

    # Report changes
    typer.echo("Schema changes detected:", err=True)
    typer.echo("", err=True)

    for stmt in statements:
        description = _describe_statement(stmt)
        typer.echo(f"  - {description}", err=True)

    typer.echo("", err=True)
    typer.echo(f"Total: {len(statements)} change(s)", err=True)
    typer.echo("", err=True)
    typer.echo("Run 'derp generate' to create a migration.", err=True)

    raise typer.Exit(1)


def _describe_statement(stmt) -> str:
    """Generate human-readable description of a statement."""
    match stmt:
        case CreateTableStatement():
            return f"CREATE TABLE {stmt.table_name}"
        case DropTableStatement():
            return f"DROP TABLE {stmt.table_name}"
        case RenameTableStatement():
            return f"RENAME TABLE {stmt.from_table} -> {stmt.to_table}"
        case AddColumnStatement():
            return f"ADD COLUMN {stmt.table_name}.{stmt.column.name}"
        case DropColumnStatement():
            return f"DROP COLUMN {stmt.table_name}.{stmt.column_name}"
        case RenameColumnStatement():
            return (
                f"RENAME COLUMN {stmt.table_name}.{stmt.from_column} "
                f"-> {stmt.to_column}"
            )
        case AlterColumnTypeStatement():
            return (
                f"ALTER COLUMN {stmt.table_name}.{stmt.column_name} "
                f"TYPE {stmt.new_type}"
            )
        case AlterColumnNullableStatement():
            nullable = "NULL" if stmt.nullable else "NOT NULL"
            return f"ALTER COLUMN {stmt.table_name}.{stmt.column_name} SET {nullable}"
        case AlterColumnDefaultStatement():
            if stmt.default is None:
                return f"ALTER COLUMN {stmt.table_name}.{stmt.column_name} DROP DEFAULT"
            return f"ALTER COLUMN {stmt.table_name}.{stmt.column_name} SET DEFAULT"
        case CreateForeignKeyStatement():
            return f"ADD FOREIGN KEY {stmt.name}"
        case DropForeignKeyStatement():
            return f"DROP FOREIGN KEY {stmt.name}"
        case CreateUniqueConstraintStatement():
            return f"ADD UNIQUE CONSTRAINT {stmt.name}"
        case DropUniqueConstraintStatement():
            return f"DROP UNIQUE CONSTRAINT {stmt.name}"
        case CreateIndexStatement():
            return f"CREATE INDEX {stmt.name}"
        case DropIndexStatement():
            return f"DROP INDEX {stmt.name}"
        case CreateEnumStatement():
            return f"CREATE ENUM {stmt.name}"
        case DropEnumStatement():
            return f"DROP ENUM {stmt.name}"
        case AlterEnumAddValueStatement():
            return f"ALTER ENUM {stmt.name} ADD VALUE '{stmt.value}'"
        case CreateSequenceStatement():
            return f"CREATE SEQUENCE {stmt.name}"
        case DropSequenceStatement():
            return f"DROP SEQUENCE {stmt.name}"
        case CreateSchemaStatement():
            return f"CREATE SCHEMA {stmt.name}"
        case DropSchemaStatement():
            return f"DROP SCHEMA {stmt.name}"
        case EnableRLSStatement():
            return f"ENABLE RLS ON {stmt.table_name}"
        case DisableRLSStatement():
            return f"DISABLE RLS ON {stmt.table_name}"
        case CreatePolicyStatement():
            return f"CREATE POLICY {stmt.name} ON {stmt.table_name}"
        case DropPolicyStatement():
            return f"DROP POLICY {stmt.name} ON {stmt.table_name}"
        case _:
            return f"{stmt.type}"
