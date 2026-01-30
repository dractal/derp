"""Pull command - introspect database and create snapshot."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Annotated

import asyncpg
import typer

from derp.cli.config import Config
from derp.orm.migrations.introspect.postgres import PostgresIntrospector
from derp.orm.migrations.journal import (
    get_next_version,
    load_journal,
    save_journal,
    save_snapshot,
)


def pull(
    output: Annotated[
        str | None, typer.Option("--out", "-o", help="Output path for snapshot")
    ] = None,
    create_migration: Annotated[
        bool, typer.Option("--migration", "-m", help="Create as migration in journal")
    ] = False,
    name: Annotated[
        str, typer.Option("--name", "-n", help="Migration name (if --migration)")
    ] = "introspect",
) -> None:
    """Introspect database and generate a schema snapshot.

    This command queries your database and creates a snapshot of its
    current state. Useful for:

    - Starting migrations on an existing database
    - Syncing schema from production
    - Debugging schema differences
    """
    config = Config.load()
    db_url = config.database.get_url()
    migrations_dir = config.migrations.directory

    async def _pull() -> dict:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=2)

        try:
            introspector = PostgresIntrospector(pool)
            snapshot = await introspector.introspect(
                schemas=config.introspect.schemas,
                exclude_tables=config.introspect.exclude_tables,
            )

            return snapshot.model_dump(mode="json", by_alias=True)

        finally:
            await pool.close()

    snapshot_data = asyncio.run(_pull())

    # Count objects
    table_count = len(snapshot_data.get("tables", {}))
    enum_count = len(snapshot_data.get("enums", {}))
    seq_count = len(snapshot_data.get("sequences", {}))
    policy_count = len(snapshot_data.get("policies", {}))

    typer.echo("Introspected database:")
    typer.echo(f"  - {table_count} table(s)")
    typer.echo(f"  - {enum_count} enum(s)")
    typer.echo(f"  - {seq_count} sequence(s)")
    typer.echo(f"  - {policy_count} policy/policies")

    if create_migration:
        # Create as a migration entry
        journal = load_journal(migrations_dir)
        version = get_next_version(journal)
        snapshot_data["id"] = version

        folder_path = save_snapshot(migrations_dir, version, name, snapshot_data)

        # Create empty SQL file (no changes needed for introspection)
        sql_path = folder_path / "migration.sql"
        sql_path.write_text(
            f"-- Migration: {name}\n"
            f"-- Version: {version}\n"
            f"-- Introspected from database\n"
            f"-- No SQL - snapshot only\n"
        )

        journal.add_entry(version=version, tag=name)
        save_journal(journal, migrations_dir)

        typer.echo("")
        typer.echo(f"Created migration: {folder_path.name}/")
        typer.echo("  - snapshot.json")
        typer.echo("  - migration.sql (empty)")

    elif output:
        # Write to specified path
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(snapshot_data, f, indent=2)
        typer.echo("")
        typer.echo(f"Snapshot saved to: {output_path}")

    else:
        # Write to default location
        output_dir = migrations_dir / "_introspected"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "snapshot.json"

        with open(output_path, "w") as f:
            json.dump(snapshot_data, f, indent=2)

        typer.echo("")
        typer.echo(f"Snapshot saved to: {output_path}")
        typer.echo("")
        typer.echo("To use this as your baseline for migrations, run:")
        typer.echo("  derp pull --migration --name baseline")
