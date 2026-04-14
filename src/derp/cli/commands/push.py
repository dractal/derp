"""Push command - direct schema push without migration files."""

from __future__ import annotations

import asyncio
from typing import Annotated

import asyncpg
import typer

from derp.cli.commands.generate import create_rename_resolver, make_rename_callback
from derp.config import ConfigError, DerpConfig
from derp.orm.loader import discover_tables

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
from derp.orm.migrations.filters import filter_rls_statements
from derp.orm.migrations.introspect.postgres import PostgresIntrospector
from derp.orm.migrations.safety import (
    detect_destructive_operations,
    format_destructive_warnings,
    has_high_risk_operations,
)
from derp.orm.migrations.snapshot.differ import SnapshotDiffer
from derp.orm.migrations.snapshot.normalize import get_normalizer
from derp.orm.migrations.snapshot.serializer import serialize_schema


def push(
    force: Annotated[
        bool, typer.Option("--force", "-f", help="Skip confirmation prompts")
    ] = False,
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Show SQL without executing")
    ] = False,
) -> None:
    """Push schema changes directly to the database (dev mode).

    This command is for development only. It diffs your schema against
    the live database and applies changes directly without creating
    migration files.

    For production, use 'derp generate' + 'derp migrate' instead.
    """
    try:
        config = DerpConfig.load()
    except ConfigError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)

    db_url = config.database.db_url
    schema_path = config.database.schema_path
    if not schema_path:
        typer.echo(
            "Error: database.schema_path not configured in derp.toml",
            err=True,
        )
        raise typer.Exit(1)

    # Load tables from schema module
    try:
        tables = discover_tables(schema_path, auth_config=config.auth)
    except FileNotFoundError:
        typer.echo(f"Error: Schema file not found: {schema_path}", err=True)
        raise typer.Exit(1)

    if not tables:
        typer.echo(f"No Table classes found in {schema_path}", err=True)
        raise typer.Exit(1)

    typer.echo(f"Found {len(tables)} table(s): {', '.join(t.__name__ for t in tables)}")

    async def _push() -> None:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=2)

        try:
            # Introspect current database
            introspector = PostgresIntrospector(pool)
            db_snapshot = await introspector.introspect(
                schemas=config.database.introspect_schemas,
                exclude_tables=config.database.introspect_exclude_tables,
            )

            typer.echo(f"Introspected {len(db_snapshot.tables)} existing table(s)")

            # Serialize desired schema
            desired_snapshot = serialize_schema(tables, schema="public")

            # Normalize both snapshots for comparison
            normalizer = get_normalizer(desired_snapshot.dialect)
            db_norm = normalizer.normalize(db_snapshot)
            desired_norm = normalizer.normalize(desired_snapshot)

            # Prompt for potential column renames before diffing
            rename_decisions = create_rename_resolver(db_norm, desired_norm, force)
            rename_callback = (
                make_rename_callback(rename_decisions) if rename_decisions else None
            )

            # Diff
            differ = SnapshotDiffer(db_norm, desired_norm, rename_callback)
            statements = differ.diff()

            # Filter out RLS/policy changes when ignore_rls is enabled
            if config.database.ignore_rls:
                statements = filter_rls_statements(statements)

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
                    if not typer.confirm(
                        "Continue with potentially destructive changes?"
                    ):
                        raise typer.Abort()

            # Generate SQL
            sql = ConvertorRegistry.convert_all(statements)

            typer.echo("")
            typer.echo("SQL to execute:")
            typer.echo("-" * 40)
            typer.echo(sql)
            typer.echo("-" * 40)
            typer.echo("")

            if dry_run:
                typer.echo("Dry run complete. No changes were made.")
                return

            if not force:
                if not typer.confirm("Apply these changes?"):
                    raise typer.Abort()

            # Execute
            async with pool.acquire() as conn:
                await conn.execute(sql)

            typer.echo("Schema pushed successfully.")

        finally:
            await pool.close()

    asyncio.run(_push())
