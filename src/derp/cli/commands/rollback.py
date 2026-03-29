"""Rollback command - revert the most recently applied migration."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Annotated

import asyncpg
import typer

from derp.config import MIGRATIONS_TABLE, ConfigError, DerpConfig
from derp.orm.migrations.journal import (
    get_migration_down_sql,
    get_migration_folders,
    load_journal,
)

from .migrate import _LOCK_KEY, _LOCK_NAMESPACE, _ensure_migrations_table


def rollback(
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Show SQL without executing")
    ] = False,
    all_: Annotated[
        bool,
        typer.Option("--all", help="Rollback all applied migrations"),
    ] = False,
    to: Annotated[
        str | None,
        typer.Option("--to", help="Rollback to a specific version (exclusive)"),
    ] = None,
) -> None:
    """Rollback the most recently applied migration.

    Executes the down.sql for the latest applied migration and removes
    it from the tracking table. Use --all to rollback every applied
    migration, or --to <version> to rollback down to (but not including)
    that version.
    """
    try:
        config = DerpConfig.load()
    except ConfigError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)

    db_url = config.database.db_url
    migrations_dir = Path(config.database.migrations_dir)

    journal = load_journal(migrations_dir)
    if not journal.entries:
        typer.echo("No migrations found in journal.")
        return

    folders = get_migration_folders(migrations_dir)
    folder_map = {version: folder for version, folder in folders}

    async def _rollback() -> int:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=2)
        rolled_back = 0

        try:
            async with pool.acquire() as lock_conn:
                await lock_conn.execute(
                    "SELECT pg_advisory_lock($1, $2)",
                    _LOCK_NAMESPACE,
                    _LOCK_KEY,
                )

                try:
                    await _ensure_migrations_table(pool)

                    # Get applied migrations in order
                    async with pool.acquire() as conn:
                        rows = await conn.fetch(
                            f"SELECT version, tag FROM {MIGRATIONS_TABLE}"
                            " ORDER BY id DESC"
                        )

                    if not rows:
                        typer.echo("No applied migrations to rollback.")
                        return 0

                    # Determine which migrations to rollback
                    targets: list[tuple[str, str]] = []  # (version, tag)
                    if all_:
                        targets = [(r["version"], r["tag"]) for r in rows]
                    elif to is not None:
                        for row in rows:
                            if row["version"] == to:
                                break
                            targets.append((row["version"], row["tag"]))
                        if not targets:
                            typer.echo(
                                f"Version {to} is already the latest applied "
                                "migration (or not found)."
                            )
                            return 0
                    else:
                        targets = [(rows[0]["version"], rows[0]["tag"])]

                    typer.echo(f"Rolling back {len(targets)} migration(s)")
                    typer.echo("")

                    for version, tag in targets:
                        folder = folder_map.get(version)
                        if folder is None:
                            typer.echo(
                                f"Error: Migration folder not found for {version}",
                                err=True,
                            )
                            raise typer.Exit(1)

                        down_sql = get_migration_down_sql(folder)
                        if not down_sql or not down_sql.strip():
                            typer.echo(
                                f"Error: No down.sql found for {version} ({tag}). "
                                "Cannot rollback.",
                                err=True,
                            )
                            raise typer.Exit(1)

                        typer.echo(f"Rolling back: {version} ({tag})")

                        if dry_run:
                            typer.echo("  SQL:")
                            for line in down_sql.strip().split("\n")[:10]:
                                typer.echo(f"    {line}")
                            if down_sql.count("\n") > 10:
                                typer.echo(
                                    f"    ... ({down_sql.count(chr(10)) - 10}"
                                    " more lines)"
                                )
                            typer.echo("")
                            continue

                        async with pool.acquire() as conn:
                            async with conn.transaction():
                                await conn.execute(down_sql)

                        # Remove from tracking table
                        async with pool.acquire() as conn:
                            await conn.execute(
                                f"DELETE FROM {MIGRATIONS_TABLE} WHERE version = $1",
                                version,
                            )
                        rolled_back += 1
                        typer.echo("  Rolled back successfully")

                    return rolled_back

                finally:
                    await lock_conn.execute(
                        "SELECT pg_advisory_unlock($1, $2)",
                        _LOCK_NAMESPACE,
                        _LOCK_KEY,
                    )
        finally:
            await pool.close()

    count = asyncio.run(_rollback())

    if dry_run:
        typer.echo("Dry run complete. No changes were made.")
    elif count > 0:
        typer.echo("")
        typer.echo(f"Rolled back {count} migration(s).")
