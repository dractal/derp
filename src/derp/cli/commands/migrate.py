"""Migrate command - apply pending migrations."""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
from typing import Annotated

import asyncpg
import typer

from derp.config import MIGRATIONS_TABLE, ConfigError, DerpConfig
from derp.orm.migrations.journal import (
    get_migration_folders,
    get_migration_sql,
    load_journal,
)

# PostgreSQL advisory lock key for migration serialization.
# Chosen as a fixed, arbitrary 64-bit integer unlikely to collide with
# application locks. Both halves are used with pg_advisory_lock(int, int).
_LOCK_NAMESPACE = 7_283_946
_LOCK_KEY = 1


class MigrationHashMismatchError(Exception):
    """Raised when a previously applied migration's file has been modified."""


def migrate(
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Show SQL without executing")
    ] = False,
) -> None:
    """Apply pending migrations to the database.

    Reads the migration journal and applies any migrations that haven't
    been recorded in the database's _derp_migrations table.
    """
    try:
        config = DerpConfig.load()
    except ConfigError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)

    db_url = config.database.db_url
    migrations_dir = Path(config.database.migrations_dir)

    # Load journal
    journal = load_journal(migrations_dir)
    if not journal.entries:
        typer.echo("No migrations found in journal.")
        return

    # Get migration folders
    folders = get_migration_folders(migrations_dir)
    if not folders:
        typer.echo("No migration folders found.")
        return

    async def _migrate() -> int:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=2)
        applied_count = 0

        try:
            # Acquire advisory lock to prevent concurrent migrations
            async with pool.acquire() as lock_conn:
                await lock_conn.execute(
                    "SELECT pg_advisory_lock($1, $2)",
                    _LOCK_NAMESPACE,
                    _LOCK_KEY,
                )

                try:
                    # Ensure migrations table exists
                    await _ensure_migrations_table(pool)

                    # Get already applied migrations
                    applied = await _get_applied_migrations(pool)

                    # Validate hashes of applied migrations
                    _validate_applied_hashes(applied, folders)

                    # Find pending migrations
                    pending = []
                    for version, folder in folders:
                        if version not in applied:
                            entry = journal.get_entry(version)
                            if entry:
                                pending.append((version, folder, entry.tag))

                    if not pending:
                        typer.echo("No pending migrations.")
                        return 0

                    typer.echo(f"Found {len(pending)} pending migration(s)")
                    typer.echo("")

                    for version, folder, tag in pending:
                        sql = get_migration_sql(folder)
                        if not sql:
                            typer.echo(
                                f"Warning: No SQL found in {folder.name}, skipping",
                                err=True,
                            )
                            continue

                        typer.echo(f"Applying: {version} ({tag})")

                        if dry_run:
                            typer.echo("  SQL:")
                            for line in sql.strip().split("\n")[:10]:
                                typer.echo(f"    {line}")
                            if sql.count("\n") > 10:
                                typer.echo(
                                    f"    ... ({sql.count(chr(10)) - 10} more lines)"
                                )
                            typer.echo("")
                            continue

                        # Compute hash
                        hash_ = _compute_hash(sql)

                        # Execute migration
                        async with pool.acquire() as conn:
                            async with conn.transaction():
                                await conn.execute(sql)

                        # Record migration
                        await _record_migration(pool, version, tag, hash_)
                        applied_count += 1
                        typer.echo("  Applied successfully")

                    return applied_count

                finally:
                    await lock_conn.execute(
                        "SELECT pg_advisory_unlock($1, $2)",
                        _LOCK_NAMESPACE,
                        _LOCK_KEY,
                    )
        finally:
            await pool.close()

    try:
        count = asyncio.run(_migrate())
    except MigrationHashMismatchError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)

    if dry_run:
        typer.echo("Dry run complete. No changes were made.")
    elif count > 0:
        typer.echo("")
        typer.echo(f"Applied {count} migration(s).")


async def _ensure_migrations_table(pool: asyncpg.Pool) -> None:
    """Ensure the migrations tracking table exists."""
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


async def _get_applied_migrations(pool: asyncpg.Pool) -> dict[str, str]:
    """Get dict of applied migration versions to their hashes."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT version, hash FROM {MIGRATIONS_TABLE} ORDER BY id"
        )
        return {row["version"]: row["hash"] for row in rows}


async def _record_migration(
    pool: asyncpg.Pool, version: str, tag: str, hash_: str
) -> None:
    """Record a migration as applied."""
    async with pool.acquire() as conn:
        await conn.execute(
            f"INSERT INTO {MIGRATIONS_TABLE} (version, tag, hash) VALUES ($1, $2, $3)",
            version,
            tag,
            hash_,
        )


def _compute_hash(content: str) -> str:
    """Compute SHA256 hash of migration content."""
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def _validate_applied_hashes(
    applied: dict[str, str],
    folders: list[tuple[str, Path]],
) -> None:
    """Verify that on-disk migration files match their recorded hashes.

    Raises MigrationHashMismatchError if any applied migration has been
    modified since it was originally run.
    """
    folder_map = {version: folder for version, folder in folders}

    for version, stored_hash in applied.items():
        folder = folder_map.get(version)
        if folder is None:
            continue

        sql = get_migration_sql(folder)
        if sql is None:
            continue

        current_hash = _compute_hash(sql)
        if current_hash != stored_hash:
            raise MigrationHashMismatchError(
                f"Migration {version} has been modified after it was applied. "
                f"Expected hash {stored_hash}, got {current_hash}. "
                "Do not edit migrations that have already been applied."
            )
