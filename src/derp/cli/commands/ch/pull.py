"""`derp ch pull` — introspect a live ClickHouse server into a snapshot."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Annotated

import typer

from derp.chorm.migrations.files import SNAPSHOT_FILE
from derp.chorm.migrations.introspect import introspect
from derp.chorm.migrations.snapshot import SchemaSnapshot
from derp.cli.commands.ch._common import (
    load_config,
    make_engine,
    migrations_dir,
    require_clickhouse,
)


def pull(
    out: Annotated[
        str | None,
        typer.Option("--out", "-o", help="Where to write the snapshot JSON"),
    ] = None,
) -> None:
    """Introspect the live database and write a schema snapshot."""
    config = load_config()
    cfg = require_clickhouse(config)

    out_path = (
        Path(out)
        if out is not None
        else migrations_dir(cfg) / "_introspected" / SNAPSHOT_FILE
    )

    async def _run() -> SchemaSnapshot:
        engine = make_engine(cfg)
        await engine.connect()
        try:
            return await introspect(engine, database=cfg.introspect_database)
        finally:
            await engine.disconnect()

    snapshot = asyncio.run(_run())
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(snapshot.to_json() + "\n")
    typer.echo(f"Wrote {len(snapshot.tables)} table(s) to {out_path}")
