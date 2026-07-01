"""`derp ch check` — verify the schema matches the latest snapshot."""

from __future__ import annotations

import typer

from derp.chorm.migrations import diff_snapshots, snapshot_from_tables
from derp.chorm.migrations.files import FileJournal, read_latest_snapshot
from derp.cli.commands.ch._common import (
    load_config,
    load_tables,
    migrations_dir,
    require_clickhouse,
)


def check() -> None:
    """Exit 0 if the schema is in sync with the latest snapshot, else 1.

    Requires no database connection — purely compares the chorm tables
    against the most recent migration snapshot. Useful in CI.
    """
    config = load_config()
    cfg = require_clickhouse(config)
    tables = load_tables(cfg)
    out_dir = migrations_dir(cfg)

    journal = FileJournal.load(out_dir)
    old_snapshot = read_latest_snapshot(out_dir, journal)
    new_snapshot = snapshot_from_tables(tables)

    forward = diff_snapshots(old_snapshot, new_snapshot)
    if not forward:
        typer.echo("Schema is up to date.")
        return

    typer.echo(f"Schema drift: {len(forward)} pending change(s).")
    for s in forward:
        typer.echo(f"  - {s.to_sql()}")
    raise typer.Exit(1)
