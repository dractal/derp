"""`derp ch drop` — delete migration folders from disk."""

from __future__ import annotations

import shutil
from typing import Annotated

import typer

from derp.chorm.migrations.files import FileJournal
from derp.cli.commands.ch._common import (
    load_config,
    migrations_dir,
    require_clickhouse,
)


def drop(
    all_: Annotated[
        bool, typer.Option("--all", help="Drop every migration, not just the last")
    ] = False,
    force: Annotated[
        bool, typer.Option("--force", "-f", help="Skip confirmation")
    ] = False,
) -> None:
    """Delete the most recent migration folder (or all with ``--all``).

    Only touches local files — it does not roll back an applied migration.
    Use ``derp ch rollback`` to revert changes already applied to a server.
    """
    config = load_config()
    cfg = require_clickhouse(config)
    out_dir = migrations_dir(cfg)

    journal = FileJournal.load(out_dir)
    if not journal.entries:
        typer.echo("No migrations to drop.")
        return

    targets = list(journal.entries) if all_ else [journal.entries[-1]]
    typer.echo("About to delete:")
    for e in targets:
        typer.echo(f"  - {e.dirname}")
    if not force and not typer.confirm("Continue?"):
        typer.echo("Aborted.")
        raise typer.Exit(1)

    for e in targets:
        folder = out_dir / e.dirname
        if folder.exists():
            shutil.rmtree(folder)

    journal.entries = [e for e in journal.entries if e not in targets]
    journal.save(out_dir)
    typer.echo(f"Dropped {len(targets)} migration(s).")
