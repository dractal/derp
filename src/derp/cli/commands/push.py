"""Push command - direct schema push without migration files."""

from __future__ import annotations

import asyncio
from typing import Annotated

import asyncpg
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
from derp.migrations.introspect.postgres import PostgresIntrospector
from derp.migrations.safety import (
    detect_destructive_operations,
    format_destructive_warnings,
    has_high_risk_operations,
)
from derp.migrations.snapshot.differ import SnapshotDiffer
from derp.migrations.snapshot.serializer import serialize_schema
from derp.orm.loader import load_tables


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
    config = Config.load()
    db_url = config.database.get_url()
    schema_path = config.migrations.get_schema_path()

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

    async def _push() -> None:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=2)

        try:
            # Introspect current database
            introspector = PostgresIntrospector(pool)
            db_snapshot = await introspector.introspect(
                schemas=config.introspect.schemas,
                exclude_tables=config.introspect.exclude_tables,
            )

            typer.echo(f"Introspected {len(db_snapshot.tables)} existing table(s)")

            # Serialize desired schema
            desired_snapshot = serialize_schema(tables, schema="public")

            # Diff
            differ = SnapshotDiffer(db_snapshot, desired_snapshot)
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
