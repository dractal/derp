"""`derp ch rollback` — revert the most recently applied migration(s)."""

from __future__ import annotations

import asyncio
from typing import Annotated

import typer

from derp.chorm.migrations.files import FileJournal, read_down_sql, split_sql
from derp.chorm.migrations.journal import MigrationJournal
from derp.cli.commands.ch._common import (
    load_config,
    make_engine,
    migrations_dir,
    require_clickhouse,
)


def rollback(
    all_: Annotated[
        bool, typer.Option("--all", help="Roll back every applied migration")
    ] = False,
    force: Annotated[
        bool, typer.Option("--force", "-f", help="Skip confirmation")
    ] = False,
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Show SQL without executing")
    ] = False,
) -> None:
    """Apply the ``down.sql`` of applied migrations, newest first.

    Rollback recreates structure but cannot restore data dropped by the
    forward migration — treat it as destructive.
    """
    config = load_config()
    cfg = require_clickhouse(config)
    out_dir = migrations_dir(cfg)

    journal = FileJournal.load(out_dir)
    if not journal.entries:
        typer.echo("No migrations found.")
        return

    if (
        not force
        and not dry_run
        and not typer.confirm("Rollback may permanently drop data. Continue?")
    ):
        typer.echo("Aborted.")
        raise typer.Exit(1)

    async def _run() -> int:
        engine = make_engine(cfg)
        await engine.connect()
        try:
            mj = MigrationJournal(engine, database=cfg.database)
            await mj.ensure()
            applied = await mj.applied()

            # Newest applied first.
            ordered = [e for e in reversed(journal.entries) if e.version in applied]
            if not ordered:
                typer.echo("Nothing to roll back.")
                return 0
            targets = ordered if all_ else ordered[:1]

            for entry in targets:
                down_sql = read_down_sql(out_dir, entry)
                statements = split_sql(down_sql)
                typer.echo(f"Rolling back {entry.version} ({entry.name})...")
                if not statements:
                    typer.echo("  (no down SQL — skipping)")
                    continue
                if dry_run:
                    for stmt in statements:
                        typer.echo(f"  {stmt}")
                    continue
                for stmt in statements:
                    await engine.command(stmt)
                await mj.remove(entry.version)
            return len(targets)
        finally:
            await engine.disconnect()

    count = asyncio.run(_run())
    if dry_run:
        typer.echo("Dry run — nothing rolled back.")
    elif count:
        typer.echo(f"Rolled back {count} migration(s).")
