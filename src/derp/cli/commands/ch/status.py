"""`derp ch status` — show applied vs pending migrations."""

from __future__ import annotations

import asyncio

import typer

from derp.chorm.migrations.files import FileJournal
from derp.chorm.migrations.journal import MigrationJournal
from derp.cli.commands.ch._common import (
    load_config,
    make_engine,
    migrations_dir,
    require_clickhouse,
)


def status() -> None:
    """List each migration and whether it has been applied to the server."""
    config = load_config()
    cfg = require_clickhouse(config)
    out_dir = migrations_dir(cfg)

    journal = FileJournal.load(out_dir)
    if not journal.entries:
        typer.echo("No migrations found.")
        return

    async def _run() -> set[str]:
        engine = make_engine(cfg)
        await engine.connect()
        try:
            mj = MigrationJournal(engine, database=cfg.database)
            await mj.ensure()
            return await mj.applied()
        finally:
            await engine.disconnect()

    applied = asyncio.run(_run())

    pending = 0
    for entry in journal.entries:
        if entry.version in applied:
            typer.echo(f"  [applied] {entry.version}  {entry.name}")
        else:
            pending += 1
            typer.echo(f"  [pending] {entry.version}  {entry.name}")
    typer.echo("")
    typer.echo(f"{len(applied)} applied, {pending} pending.")
