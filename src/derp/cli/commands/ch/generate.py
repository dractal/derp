"""`derp ch generate` — create a ClickHouse migration from schema changes."""

from __future__ import annotations

from typing import Annotated

import typer

from derp.chorm.migrations import diff_down, diff_snapshots, snapshot_from_tables
from derp.chorm.migrations.files import (
    FileJournal,
    read_latest_snapshot,
    render_sql,
    slugify,
    write_migration,
)
from derp.cli.commands.ch._common import (
    load_config,
    load_tables,
    migrations_dir,
    require_clickhouse,
)


def generate(
    name: Annotated[
        str, typer.Option("--name", "-n", help="Migration name")
    ] = "migration",
    force: Annotated[
        bool,
        typer.Option("--force", "-f", help="Skip confirmation for destructive changes"),
    ] = False,
) -> None:
    """Generate a ClickHouse migration from schema changes.

    Diffs the chorm tables at ``clickhouse.schema_path`` against the latest
    snapshot and writes a migration folder (``migration.sql`` / ``down.sql``
    / ``snapshot.json``).
    """
    config = load_config()
    cfg = require_clickhouse(config)
    tables = load_tables(cfg)
    out_dir = migrations_dir(cfg)

    typer.echo(f"Found {len(tables)} table(s): {', '.join(t.__name__ for t in tables)}")

    journal = FileJournal.load(out_dir)
    old_snapshot = read_latest_snapshot(out_dir, journal)
    new_snapshot = snapshot_from_tables(tables)

    forward = diff_snapshots(old_snapshot, new_snapshot)
    if not forward:
        typer.echo("No changes detected. Schema is up to date.")
        return

    typer.echo(f"Detected {len(forward)} change(s)")

    destructive = [s for s in forward if s.is_destructive()]
    if destructive:
        typer.echo("")
        typer.echo("Destructive changes detected:")
        for s in destructive:
            typer.echo(f"  - {s.to_sql()}")
        typer.echo("")
        if not force and not typer.confirm("Continue?"):
            typer.echo("Aborted.")
            raise typer.Exit(1)

    down = diff_down(old_snapshot, new_snapshot)

    entry = journal.append(slugify(name))
    folder = write_migration(
        out_dir,
        entry,
        up_sql=render_sql(forward),
        down_sql=render_sql(down),
        snapshot=new_snapshot,
    )
    journal.save(out_dir)

    typer.echo(f"Created migration {entry.version}: {folder}")
    if down:
        typer.echo(
            "Note: down.sql recreates structure but cannot restore dropped data."
        )
