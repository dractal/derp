"""`derp ch push` — diff live schema and apply directly (dev workflow)."""

from __future__ import annotations

import asyncio
from typing import Annotated

import typer

from derp.chorm.migrations import diff_snapshots, snapshot_from_tables
from derp.chorm.migrations.introspect import introspect
from derp.cli.commands.ch._common import (
    load_config,
    load_tables,
    make_engine,
    require_clickhouse,
)


def push(
    force: Annotated[
        bool,
        typer.Option("--force", "-f", help="Skip confirmation for destructive changes"),
    ] = False,
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Show SQL without executing")
    ] = False,
) -> None:
    """Diff the live ClickHouse schema against your tables and apply changes.

    Skips migration files entirely — intended for fast local iteration.
    """
    config = load_config()
    cfg = require_clickhouse(config)
    tables = load_tables(cfg)
    new_snapshot = snapshot_from_tables(tables)

    async def _run() -> int:
        engine = make_engine(cfg)
        await engine.connect()
        try:
            live = await introspect(engine, database=cfg.introspect_database)
            forward = diff_snapshots(
                live,
                new_snapshot,
                default_database=cfg.introspect_database,
                from_introspection=True,
            )
            if not forward:
                typer.echo("Schema is up to date.")
                return 0

            typer.echo(f"Detected {len(forward)} change(s):")
            for s in forward:
                typer.echo(f"  - {s.to_sql()}")

            if dry_run:
                return 0

            destructive = [s for s in forward if s.is_destructive()]
            if (
                destructive
                and not force
                and not typer.confirm("Apply destructive changes?")
            ):
                typer.echo("Aborted.")
                raise typer.Exit(1)

            for s in forward:
                await engine.command(s.to_sql())
            return len(forward)
        finally:
            await engine.disconnect()

    count = asyncio.run(_run())
    if dry_run:
        typer.echo("Dry run — nothing applied.")
    elif count:
        typer.echo(f"Applied {count} change(s).")
