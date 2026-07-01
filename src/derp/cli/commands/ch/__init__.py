"""`derp ch ...` — ClickHouse (chorm) migration commands."""

from __future__ import annotations

import typer

from derp.cli.commands.ch.check import check
from derp.cli.commands.ch.drop import drop
from derp.cli.commands.ch.generate import generate
from derp.cli.commands.ch.migrate import migrate
from derp.cli.commands.ch.pull import pull
from derp.cli.commands.ch.push import push
from derp.cli.commands.ch.rollback import rollback
from derp.cli.commands.ch.status import status

ch_app = typer.Typer(
    name="ch",
    help="ClickHouse migration commands.",
    no_args_is_help=True,
)

ch_app.command()(generate)
ch_app.command()(migrate)
ch_app.command()(push)
ch_app.command()(pull)
ch_app.command()(status)
ch_app.command()(check)
ch_app.command()(drop)
ch_app.command()(rollback)

__all__ = ["ch_app"]
