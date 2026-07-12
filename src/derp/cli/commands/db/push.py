"""Push command - direct schema push without migration files."""

from __future__ import annotations

import asyncio
import enum
from typing import Annotated

import asyncpg
import typer

from derp.cli.commands.db._common import schema_errors
from derp.cli.commands.db.generate import (
    create_rename_resolver,
    make_rename_callback,
)
from derp.config import ConfigError, DerpConfig
from derp.orm.loader import discover_tables
from derp.orm.migrations.categorize import (
    StatementCategory,
    classify_statements,
    describe,
    filter_drops,
)

# Import all convertors to register them
from derp.orm.migrations.convertors import (  # noqa: F401
    column,
    constraint,
    index,
    policy,
    role,
    schema,
    sequence,
    table,
)
from derp.orm.migrations.convertors import enum as enum_convertors  # noqa: F401
from derp.orm.migrations.convertors.base import ConvertorRegistry
from derp.orm.migrations.filters import filter_rls_statements
from derp.orm.migrations.introspect.postgres import (
    PostgresIntrospector,
    canonicalize_check_expressions,
)
from derp.orm.migrations.snapshot.differ import SnapshotDiffer
from derp.orm.migrations.snapshot.normalize import get_normalizer
from derp.orm.migrations.snapshot.serializer import serialize_schema
from derp.orm.migrations.statements import Statement


class PushAction(enum.StrEnum):
    """Outcome of the categorized push prompt."""

    APPLY_ALL = "apply_all"
    SKIP_DROPS = "skip_drops"
    REVIEW_EACH = "review_each"
    CANCEL = "cancel"


def push(
    apply_all: Annotated[
        bool,
        typer.Option(
            "--apply-all",
            help="Apply all changes (including drops) without prompting",
        ),
    ] = False,
    skip_drops: Annotated[
        bool,
        typer.Option(
            "--skip-drops",
            help="Apply non-destructive changes only, skip all drops",
        ),
    ] = False,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Alias for --apply-all (kept for back-compat)",
            hidden=True,
        ),
    ] = False,
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Show SQL without executing")
    ] = False,
) -> None:
    """Push schema changes directly to the database (dev mode).

    This command is for development only. It diffs your schema against
    the live database and applies changes directly without creating
    migration files.

    For production, use 'derp generate' + 'derp migrate' instead.
    """
    # --force is an alias for --apply-all; collapse before any other logic.
    if force:
        apply_all = True

    if apply_all and skip_drops:
        typer.echo(
            "Error: --apply-all and --skip-drops are mutually exclusive", err=True
        )
        raise typer.Exit(2)

    try:
        config = DerpConfig.load()
    except ConfigError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)

    db_url = config.database.db_url
    schema_path = config.database.schema_path
    if not schema_path:
        typer.echo(
            "Error: database.schema_path not configured in derp.toml",
            err=True,
        )
        raise typer.Exit(1)

    # Load tables from schema module
    try:
        tables = discover_tables(schema_path, auth_config=config.auth)
    except FileNotFoundError:
        typer.echo(f"Error: Schema file not found: {schema_path}", err=True)
        raise typer.Exit(1)

    if not tables:
        typer.echo(f"No Table classes found in {schema_path}", err=True)
        raise typer.Exit(1)

    typer.echo(f"Found {len(tables)} table(s): {', '.join(t.__name__ for t in tables)}")

    async def _push() -> None:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=2)

        try:
            # Introspect current database
            introspector = PostgresIntrospector(pool)
            db_snapshot = await introspector.introspect(
                schemas=config.database.introspect_schemas,
                exclude_tables=config.database.introspect_exclude_tables,
            )

            typer.echo(f"Introspected {len(db_snapshot.tables)} existing table(s)")

            # Serialize desired schema
            with schema_errors():
                desired_snapshot = serialize_schema(tables, schema="public")

            # PostgreSQL re-deparses CHECK expressions, so the authored text
            # never matches what the catalog reports. Ask the server to spell
            # them its way before diffing, or every push re-plans a DROP + ADD.
            desired_snapshot = await canonicalize_check_expressions(
                pool, desired_snapshot, db_snapshot
            )

            # Normalize both snapshots for comparison
            normalizer = get_normalizer(desired_snapshot.dialect)
            db_norm = normalizer.normalize(db_snapshot)
            desired_norm = normalizer.normalize(desired_snapshot)

            # Prompt for potential column renames before diffing. Treat any
            # non-interactive flag as "force" for the rename resolver — there's
            # no human there to answer.
            non_interactive = apply_all or skip_drops or dry_run
            rename_decisions = create_rename_resolver(
                db_norm, desired_norm, non_interactive
            )
            rename_callback = (
                make_rename_callback(rename_decisions) if rename_decisions else None
            )

            # Diff
            differ = SnapshotDiffer(db_norm, desired_norm, rename_callback)
            with schema_errors():
                statements = differ.diff()

            # Filter out RLS/policy changes when ignore_rls is enabled
            if config.database.ignore_rls:
                statements = filter_rls_statements(statements)

            if not statements:
                typer.echo("No changes detected. Schema is up to date.")
                return

            # Classify statements for the summary
            buckets = classify_statements(statements)
            _print_summary(statements, buckets)

            # Decide what to apply: explicit flags > interactive prompt
            if apply_all:
                to_apply = statements
            elif skip_drops:
                to_apply = filter_drops(statements)
                if not to_apply:
                    typer.echo(
                        "Only drop statements detected — nothing to apply with "
                        "--skip-drops."
                    )
                    return
            elif dry_run:
                # Dry-run alone: show all SQL, don't prompt, don't execute.
                to_apply = statements
            else:
                action = _prompt_action(buckets)
                if action == PushAction.CANCEL:
                    raise typer.Abort()
                if action == PushAction.APPLY_ALL:
                    to_apply = statements
                elif action == PushAction.SKIP_DROPS:
                    to_apply = filter_drops(statements)
                    if not to_apply:
                        typer.echo("Only drop statements detected — nothing to apply.")
                        return
                else:  # REVIEW_EACH
                    to_apply = _review_each(statements)
                    if not to_apply:
                        typer.echo("No statements selected. Nothing to apply.")
                        return

            sql = ConvertorRegistry.convert_all(to_apply)

            typer.echo("")
            typer.echo("SQL to execute:")
            typer.echo("-" * 40)
            typer.echo(sql)
            typer.echo("-" * 40)
            typer.echo("")

            if dry_run:
                typer.echo("Dry run complete. No changes were made.")
                return

            # Execute
            async with pool.acquire() as conn:
                await conn.execute(sql)

            applied = len(to_apply)
            skipped = len(statements) - applied
            if skipped:
                typer.echo(
                    f"Schema pushed successfully ({applied} applied, "
                    f"{skipped} skipped)."
                )
            else:
                typer.echo("Schema pushed successfully.")

        finally:
            await pool.close()

    asyncio.run(_push())


def _print_summary(
    statements: list[Statement],
    buckets: dict[StatementCategory, list[Statement]],
) -> None:
    """Drizzle-style summary: total + per-category counts with sample labels."""
    creates = buckets[StatementCategory.CREATE]
    alters = buckets[StatementCategory.ALTER]
    drops = buckets[StatementCategory.DROP]

    typer.echo("")
    typer.echo(f"Push will apply {len(statements)} change(s):")
    if creates:
        typer.echo(f"  [+] {len(creates)} create(s)   {_sample(creates)}")
    if alters:
        typer.echo(f"  [~] {len(alters)} alter(s)    {_sample(alters)}")
    if drops:
        typer.echo(f"  [-] {len(drops)} drop(s)     {_sample(drops)}")


def _sample(stmts: list[Statement], limit: int = 2) -> str:
    """Comma-separated preview of the first ``limit`` statements."""
    head = [describe(s) for s in stmts[:limit]]
    suffix = f", ... (+{len(stmts) - limit} more)" if len(stmts) > limit else ""
    return f"({', '.join(head)}{suffix})"


def _prompt_action(
    buckets: dict[StatementCategory, list[Statement]],
) -> PushAction:
    """Render the 4-option menu and return the user's choice.

    Default is "Skip drops" when drops exist, else "Apply all".
    """
    has_drops = bool(buckets[StatementCategory.DROP])
    default_choice = "2" if has_drops else "1"

    typer.echo("")
    typer.echo("What would you like to do?")
    typer.echo("  1) Apply all")
    typer.echo("  2) Skip drops, apply rest")
    typer.echo("  3) Review each")
    typer.echo("  4) Cancel")

    choice = typer.prompt(
        f"Choice [{default_choice}]",
        default=default_choice,
        show_default=False,
    ).strip()

    mapping = {
        "1": PushAction.APPLY_ALL,
        "apply": PushAction.APPLY_ALL,
        "a": PushAction.APPLY_ALL,
        "2": PushAction.SKIP_DROPS,
        "skip": PushAction.SKIP_DROPS,
        "s": PushAction.SKIP_DROPS,
        "3": PushAction.REVIEW_EACH,
        "review": PushAction.REVIEW_EACH,
        "r": PushAction.REVIEW_EACH,
        "4": PushAction.CANCEL,
        "cancel": PushAction.CANCEL,
        "c": PushAction.CANCEL,
        "q": PushAction.CANCEL,
    }
    action = mapping.get(choice.lower())
    if action is None:
        typer.echo(f"Unrecognized choice: {choice!r}. Cancelling.", err=True)
        return PushAction.CANCEL
    return action


def _review_each(statements: list[Statement]) -> list[Statement]:
    """Walk every statement with [y/N/q]; return the ones the user accepted.

    `q` aborts review and returns whatever was already accepted.
    """
    typer.echo("")
    typer.echo("Reviewing each statement. y=apply, n=skip, q=stop reviewing.")
    selected: list[Statement] = []
    for i, stmt in enumerate(statements, start=1):
        category = classify_statements([stmt])
        marker = (
            "[+]"
            if category[StatementCategory.CREATE]
            else "[-]"
            if category[StatementCategory.DROP]
            else "[~]"
        )
        prompt = f"  ({i}/{len(statements)}) {marker} {describe(stmt)} [y/N/q]"
        answer = typer.prompt(prompt, default="n", show_default=False).strip().lower()
        if answer in ("q", "quit", "stop"):
            break
        if answer in ("y", "yes"):
            selected.append(stmt)
        # any other answer → skip
    return selected
