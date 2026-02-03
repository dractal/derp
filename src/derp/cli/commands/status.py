"""Status command - show migration status."""

from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path

import asyncpg
import typer

from derp.config import MIGRATIONS_TABLE, ConfigError, DerpConfig
from derp.orm.migrations.journal import load_journal


def status() -> None:
    """Show migration status.

    Displays which migrations have been applied and which are pending,
    comparing the journal against the database's _derp_migrations table.
    """
    try:
        config = DerpConfig.load()
    except ConfigError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)

    db_url = config.database.db_url
    migrations_dir = Path(config.database.migrations.dir)

    # Load journal
    journal = load_journal(migrations_dir)

    if not journal.entries:
        typer.echo("No migrations found in journal.")
        typer.echo("Run 'derp generate --name initial' to create your first migration.")
        return

    async def _status() -> dict[str, tuple[str, datetime | None]]:
        """Get applied migrations with their timestamps."""
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=2)
        try:
            # Ensure table exists
            async with pool.acquire() as conn:
                await conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
                        id SERIAL PRIMARY KEY,
                        version VARCHAR(255) NOT NULL UNIQUE,
                        tag VARCHAR(255) NOT NULL,
                        hash VARCHAR(64) NOT NULL,
                        applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                    """
                )

            # Get applied migrations
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    f"""
                    SELECT version, tag, applied_at
                    FROM {MIGRATIONS_TABLE}
                    ORDER BY id
                    """
                )
                return {row["version"]: (row["tag"], row["applied_at"]) for row in rows}
        finally:
            await pool.close()

    applied = asyncio.run(_status())

    typer.echo("Migration status:")
    typer.echo("")

    applied_count = 0
    pending_count = 0

    for entry in journal.entries:
        if entry.version in applied:
            tag, applied_at = applied[entry.version]
            time_str = applied_at.strftime("%Y-%m-%d %H:%M") if applied_at else ""
            typer.echo(f"  [x] {entry.version} - {entry.tag}  ({time_str})")
            applied_count += 1
        else:
            typer.echo(f"  [ ] {entry.version} - {entry.tag}  (pending)")
            pending_count += 1

    typer.echo("")
    typer.echo(f"{applied_count} applied, {pending_count} pending")

    if pending_count > 0:
        typer.echo("")
        typer.echo("Run 'derp migrate' to apply pending migrations.")
