"""`derp ch migrate` — apply pending ClickHouse migrations."""

from __future__ import annotations

import asyncio
import hashlib
from typing import Annotated

import typer

from derp.chorm.migrations.files import FileJournal, read_up_sql, split_sql
from derp.chorm.migrations.journal import MigrationJournal
from derp.cli.commands.ch._common import (
    load_config,
    make_engine,
    migrations_dir,
    require_clickhouse,
)


class MigrationHashMismatchError(Exception):
    """An already-applied migration was edited on disk after it was applied."""


def migrate(
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Show SQL without executing")
    ] = False,
) -> None:
    """Apply migrations not yet recorded in ``_derp_chorm_migrations``.

    ClickHouse has no advisory locks, so run this from a single place
    (CI or one operator) to avoid concurrent applies.
    """
    config = load_config()
    cfg = require_clickhouse(config)
    out_dir = migrations_dir(cfg)

    journal = FileJournal.load(out_dir)
    if not journal.entries:
        typer.echo("No migrations found.")
        return

    async def _run() -> int:
        engine = make_engine(cfg)
        await engine.connect()
        try:
            mj = MigrationJournal(engine, database=cfg.database)
            await mj.ensure()
            applied_hashes = await mj.applied_with_hashes()

            # Guard against editing a migration that has already run: its on-disk
            # SQL must still hash to what was recorded when it was applied.
            for entry in journal.entries:
                stored = applied_hashes.get(entry.version)
                if not stored:
                    continue
                current = hashlib.sha256(
                    read_up_sql(out_dir, entry).encode()
                ).hexdigest()
                if current != stored:
                    raise MigrationHashMismatchError(
                        f"Migration {entry.version} ({entry.name}) was modified "
                        "after it was applied. Do not edit applied migrations."
                    )

            pending = [e for e in journal.entries if e.version not in applied_hashes]
            if not pending:
                typer.echo("No pending migrations.")
                return 0

            for entry in pending:
                up_sql = read_up_sql(out_dir, entry)
                statements = split_sql(up_sql)
                typer.echo(f"Applying {entry.version} ({entry.name})...")
                if dry_run:
                    for stmt in statements:
                        typer.echo(f"  {stmt}")
                    continue
                for stmt in statements:
                    await engine.command(stmt)
                digest = hashlib.sha256(up_sql.encode()).hexdigest()
                await mj.record(entry.version, hash=digest, notes=entry.name)
            return len(pending)
        finally:
            await engine.disconnect()

    try:
        count = asyncio.run(_run())
    except MigrationHashMismatchError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)
    if dry_run:
        typer.echo("Dry run — nothing applied.")
    elif count:
        typer.echo(f"Applied {count} migration(s).")
