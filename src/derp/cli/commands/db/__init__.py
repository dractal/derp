"""`derp db ...` — PostgreSQL (ORM) migration commands."""

from __future__ import annotations

import typer

from derp.cli.commands.db.check import check
from derp.cli.commands.db.drop import drop
from derp.cli.commands.db.generate import generate
from derp.cli.commands.db.migrate import migrate
from derp.cli.commands.db.pull import pull
from derp.cli.commands.db.push import push
from derp.cli.commands.db.rollback import rollback
from derp.cli.commands.db.status import status

db_app = typer.Typer(
    name="db",
    help="PostgreSQL migration commands.",
    no_args_is_help=True,
)

db_app.command()(generate)
db_app.command()(migrate)
db_app.command()(push)
db_app.command()(pull)
db_app.command()(status)
db_app.command()(check)
db_app.command()(drop)
db_app.command()(rollback)

__all__ = ["db_app"]
