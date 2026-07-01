"""Snapshot differ — emits migration statements.

The differ compares an "old" snapshot (e.g. introspected from the
live ClickHouse server) against a "new" snapshot (e.g. derived from
the Python ``Table`` classes) and emits a list of
:class:`derp.chorm.migrations.statements.Statement` objects that
transform old → new.

ClickHouse limits what ALTER can do online (no ORDER BY change on
existing data, no PARTITION BY change), so unsupported structural engine
changes are rejected instead of being silently converted into migrations.
"""

from __future__ import annotations

import re

from derp.chorm.ddl import _render_default, build_create_table  # noqa: F401
from derp.chorm.migrations.snapshot import (
    ColumnSnapshot,
    EngineSnapshot,
    SchemaSnapshot,
    TableSnapshot,
)
from derp.chorm.migrations.statements import (
    AddColumn,
    AddIndex,
    AddProjection,
    AlterModifySetting,
    AlterModifyTTL,
    AlterRemoveTTL,
    AlterResetSetting,
    CommentColumn,
    CreateTable,
    DropColumn,
    DropIndex,
    DropProjection,
    DropTable,
    ModifyColumn,
    RenameColumn,
    Statement,
)


class UnsupportedSchemaChange(ValueError):
    """A schema change was detected that ClickHouse cannot alter in place."""


def diff_snapshots(
    old: SchemaSnapshot,
    new: SchemaSnapshot,
    *,
    rename_hints: dict[str, str] | None = None,
    default_database: str | None = None,
    from_introspection: bool = False,
) -> list[Statement]:
    """Diff *old* and *new* schemas, returning the migration statements.

    *rename_hints* maps ``"table.old_name" -> "new_name"`` to signal
    column renames (otherwise the differ treats them as drop + add).

    *default_database* resolves the database a table lives in when its own
    ``database`` is unset. Live introspection always stamps a concrete
    database (e.g. ``default``) while model snapshots leave it unset, so
    without this an unchanged table keys differently on each side and the
    differ emits a spurious DROP + CREATE — silently destroying data on
    ``ch push``. Pass the introspected database here to align the two sides.

    Set *from_introspection* when *old* came from a live server (the ``ch push``
    case). ClickHouse resolves omitted clauses to defaults (``PRIMARY KEY`` ←
    ``ORDER BY``, ``index_granularity`` ← 8192) that a model legitimately leaves
    blank, so without this an unchanged table reports spurious engine changes.

    Statement order is deterministic (every set difference is sorted) so the
    rendered migration — and any hash computed over it — is stable across runs.
    """
    rename_hints = rename_hints or {}
    statements: list[Statement] = []

    old_tables = _table_map(old, default_database)
    new_tables = _table_map(new, default_database)

    # Dropped tables
    for name in sorted(old_tables.keys() - new_tables.keys()):
        statements.append(DropTable(name=name, if_exists=True))

    # Created tables
    for name in sorted(new_tables.keys() - old_tables.keys()):
        t = new_tables[name]
        sql = _build_create_from_snapshot(t)
        statements.append(CreateTable(name=name, sql=sql))

    # Modified tables
    for name in sorted(old_tables.keys() & new_tables.keys()):
        statements.extend(
            _diff_table(
                old_tables[name],
                new_tables[name],
                rename_hints,
                from_introspection=from_introspection,
            )
        )

    return statements


def _table_map(
    snapshot: SchemaSnapshot, default_database: str | None
) -> dict[str, TableSnapshot]:
    """Key a snapshot's tables, treating an unset database as *default_database*.

    Mirrors :pyattr:`TableSnapshot.key` but lets an unqualified model table
    match a live introspected table stamped with the default database.
    """
    out: dict[str, TableSnapshot] = {}
    for t in snapshot.tables:
        db = t.database or default_database
        out[f"{db}.{t.name}" if db else t.name] = t
    return out


def _diff_table(
    old: TableSnapshot,
    new: TableSnapshot,
    rename_hints: dict[str, str],
    *,
    from_introspection: bool = False,
) -> list[Statement]:
    out: list[Statement] = []
    table_name = new.key

    old_cols = old.column_map()
    new_cols = new.column_map()
    rename_targets: set[str] = set()

    # Apply rename hints first.
    renamed_old: set[str] = set()
    for hint_key, target in rename_hints.items():
        if not hint_key.startswith(f"{table_name}."):
            continue
        src = hint_key[len(table_name) + 1 :]
        if src in old_cols and target in new_cols:
            out.append(RenameColumn(table=table_name, old_name=src, new_name=target))
            renamed_old.add(src)
            rename_targets.add(target)

    # Dropped columns
    for name in sorted((old_cols.keys() - new_cols.keys()) - renamed_old):
        out.append(DropColumn(table=table_name, name=name))

    # Added columns
    for name in sorted(new_cols.keys() - old_cols.keys() - rename_targets):
        col = new_cols[name]
        out.append(
            AddColumn(
                table=table_name,
                name=name,
                column_sql=_render_column(col),
            )
        )

    # Modified columns (same name, different type or attributes)
    for name in sorted((old_cols.keys() & new_cols.keys()) - renamed_old):
        a, b = old_cols[name], new_cols[name]
        if _column_differs(a, b):
            out.append(
                ModifyColumn(
                    table=table_name,
                    name=name,
                    column_sql=_render_column(b),
                )
            )
        elif a.comment != b.comment and b.comment is not None:
            out.append(CommentColumn(table=table_name, name=name, comment=b.comment))

    # Indexes — by name
    old_idx = {i.name: i for i in old.indexes}
    new_idx = {i.name: i for i in new.indexes}
    for name in sorted(old_idx.keys() - new_idx.keys()):
        out.append(DropIndex(table=table_name, name=name))
    for name, i in new_idx.items():
        if name not in old_idx or _idx_differs(old_idx[name], i):
            if name in old_idx:
                out.append(DropIndex(table=table_name, name=name))
            out.append(
                AddIndex(
                    table=table_name,
                    index_sql=(
                        f"INDEX `{i.name}` {i.expression} "
                        f"TYPE {i.type} GRANULARITY {i.granularity}"
                    ),
                )
            )

    # Projections — by name
    old_proj = old.projection_map()
    new_proj = new.projection_map()
    for name in sorted(old_proj.keys() - new_proj.keys()):
        out.append(DropProjection(table=table_name, name=name))
    for name, p in new_proj.items():
        if name not in old_proj or _projection_differs(old_proj[name], p):
            if name in old_proj:
                out.append(DropProjection(table=table_name, name=name))
            out.append(
                AddProjection(table=table_name, projection_sql=_render_projection(p))
            )

    _reject_unsupported_engine_changes(old, new, from_introspection=from_introspection)
    _diff_engine_settings(
        old, new, table_name, out, from_introspection=from_introspection
    )

    # Engine TTL
    old_ttl = old.engine.ttl if old.engine else None
    new_ttl = new.engine.ttl if new.engine else None
    if old_ttl != new_ttl:
        if new_ttl is None:
            out.append(AlterRemoveTTL(table=table_name))
        else:
            out.append(AlterModifyTTL(table=table_name, ttl=new_ttl))

    return out


def _column_differs(a: ColumnSnapshot, b: ColumnSnapshot) -> bool:
    return (
        a.type != b.type
        or a.default != b.default
        or a.materialized != b.materialized
        or a.alias != b.alias
        or a.codec != b.codec
        or a.ttl != b.ttl
    )


def _idx_differs(a: Any, b: Any) -> bool:
    return (
        a.expression != b.expression
        or a.type != b.type
        or a.granularity != b.granularity
    )


def _projection_differs(a: Any, b: Any) -> bool:
    return a.select != b.select or a.order_by != b.order_by


def _normalize_clause(text: str | None) -> str | None:
    """Normalize an engine clause expression for COMPARISON only.

    ClickHouse normalizes clause text on its side — ``ORDER BY (id, ts)`` is
    stored as ``id, ts`` in ``system.tables`` — so the model and the
    introspected form of an identical clause differ textually. Collapse
    whitespace, strip backticks, standardize comma spacing, and unwrap one
    layer of all-enclosing parentheses so both sides compare equal. Never used
    for rendering.
    """
    if text is None:
        return None
    s = re.sub(r"\s*,\s*", ", ", " ".join(text.split())).replace("`", "")
    if s.startswith("(") and s.endswith(")"):
        # Strip the outer parens only when they wrap the WHOLE expression —
        # "(a), (b)" keeps its parens, "(a, b)" loses them.
        depth = 0
        for i, ch in enumerate(s):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    if i == len(s) - 1:
                        s = s[1:-1].strip()
                    break
    return s


def _normalize_args(args: tuple[str, ...]) -> tuple[str, ...]:
    """Normalize engine args for COMPARISON only (quoting differs by source)."""
    return tuple(a.strip().strip("'\"").replace("`", "") for a in args)


def _reject_unsupported_engine_changes(
    old: TableSnapshot,
    new: TableSnapshot,
    *,
    from_introspection: bool = False,
) -> None:
    if old.engine is None or new.engine is None:
        return

    def primary_key(e: EngineSnapshot) -> str | None:
        # ClickHouse defaults PRIMARY KEY to ORDER BY; a model that omits the
        # primary key matches a live table whose primary key is its sort key.
        return e.primary_key or e.order_by

    n = _normalize_clause
    checks = [
        ("engine", old.engine.name, new.engine.name),
        (
            "engine args",
            _normalize_args(old.engine.args),
            _normalize_args(new.engine.args),
        ),
        ("ORDER BY", n(old.engine.order_by), n(new.engine.order_by)),
        ("PARTITION BY", n(old.engine.partition_by), n(new.engine.partition_by)),
        ("PRIMARY KEY", n(primary_key(old.engine)), n(primary_key(new.engine))),
        ("SAMPLE BY", n(old.engine.sample_by), n(new.engine.sample_by)),
    ]
    if from_introspection:
        # The live (old) side carries clauses ClickHouse resolved from defaults
        # that the model legitimately leaves blank — only flag a clause the model
        # explicitly set to a different value.
        checks = [c for c in checks if c[2] not in (None, ())]
    changed = [label for label, old_v, new_v in checks if old_v != new_v]
    if not changed:
        return

    joined = ", ".join(changed)
    raise UnsupportedSchemaChange(
        f"Table {new.key}: cannot alter {joined} in place. "
        "Create a replacement table and backfill/swap it manually."
    )


def _diff_engine_settings(
    old: TableSnapshot,
    new: TableSnapshot,
    table_name: str,
    out: list[Statement],
    *,
    from_introspection: bool = False,
) -> None:
    if old.engine is None or new.engine is None:
        return

    old_settings = old.engine.settings
    new_settings = new.engine.settings
    changed = {
        k: v
        for k, v in new_settings.items()
        if k not in old_settings or old_settings[k] != v
    }
    # On push, the live side reports ClickHouse-injected defaults (e.g.
    # index_granularity) the model never declared; resetting those is noise and
    # makes push non-idempotent, so only reset settings the model manages.
    removed: tuple[str, ...] = (
        ()
        if from_introspection
        else tuple(sorted(old_settings.keys() - new_settings.keys()))
    )

    if changed:
        out.append(AlterModifySetting(table=table_name, settings=changed))
    if removed:
        out.append(AlterResetSetting(table=table_name, names=removed))


def _render_column(col: ColumnSnapshot) -> str:
    parts = [f"`{col.name}`", col.type]
    if col.materialized:
        parts.append(f"MATERIALIZED {col.materialized}")
    elif col.alias:
        parts.append(f"ALIAS {col.alias}")
    elif col.ephemeral:
        parts.append("EPHEMERAL")
    elif col.default is not None:
        parts.append(f"DEFAULT {col.default}")
    if col.codec:
        parts.append(f"CODEC({col.codec})")
    if col.ttl:
        parts.append(f"TTL {col.ttl}")
    if col.comment:
        escaped = col.comment.replace("'", "\\'")
        parts.append(f"COMMENT '{escaped}'")
    return " ".join(parts)


def _render_projection(proj: Any) -> str:
    body = f"SELECT {proj.select}"
    if proj.order_by:
        body += f" ORDER BY {proj.order_by}"
    return f"PROJECTION `{proj.name}` ({body})"


def _build_create_from_snapshot(t: TableSnapshot) -> str:
    """Render a CREATE TABLE statement from a snapshot."""
    head = "CREATE TABLE "
    if t.database:
        head += f"{t.database}.{t.name}"
    else:
        head += t.name
    if t.cluster:
        head += f" ON CLUSTER {t.cluster}"

    body_parts = [_render_column(c) for c in t.columns]
    for i in t.indexes:
        body_parts.append(
            f"INDEX `{i.name}` {i.expression} TYPE {i.type} GRANULARITY {i.granularity}"
        )
    for p in t.projections:
        body_parts.append(_render_projection(p))

    body = ",\n    ".join(body_parts)
    sql = f"{head}\n(\n    {body}\n)\n"

    if t.engine:
        sql += _render_engine(t.engine)

    if t.comment:
        escaped = t.comment.replace("'", "\\'")
        sql += f"\nCOMMENT '{escaped}'"
    return sql


def _render_engine(e: EngineSnapshot) -> str:
    if e.args:
        sql = f"ENGINE = {e.name}({', '.join(e.args)})"
    else:
        sql = f"ENGINE = {e.name}()"
    if e.partition_by:
        sql += f"\nPARTITION BY {e.partition_by}"
    if e.primary_key:
        sql += f"\nPRIMARY KEY {e.primary_key}"
    if e.order_by:
        sql += f"\nORDER BY {e.order_by}"
    if e.sample_by:
        sql += f"\nSAMPLE BY {e.sample_by}"
    if e.ttl:
        sql += f"\nTTL {e.ttl}"
    if e.settings:
        parts = [f"{k} = {v}" for k, v in e.settings.items()]
        sql += f"\nSETTINGS {', '.join(parts)}"
    return sql


# Local import alias to avoid unused warning
from typing import Any  # noqa: E402


def invert_rename_hints(rename_hints: dict[str, str]) -> dict[str, str]:
    """Flip ``"table.old" -> "new"`` hints into ``"table.new" -> "old"``."""
    inverted: dict[str, str] = {}
    for key, new_name in rename_hints.items():
        table, _, old_name = key.rpartition(".")
        if table:
            inverted[f"{table}.{new_name}"] = old_name
    return inverted


def diff_down(
    old: SchemaSnapshot,
    new: SchemaSnapshot,
    *,
    rename_hints: dict[str, str] | None = None,
    default_database: str | None = None,
) -> list[Statement]:
    """Statements that revert a forward ``diff_snapshots(old, new)``.

    Computed as the reverse diff (``new -> old``).  Structurally this
    recreates dropped tables/columns, but ClickHouse cannot restore the
    *data* in a column that a forward migration dropped — callers should
    treat any destructive statement here as a non-recoverable rollback.
    """
    inverted = invert_rename_hints(rename_hints or {})
    return diff_snapshots(
        new, old, rename_hints=inverted, default_database=default_database
    )
