"""DDL generation for ClickHouse: CREATE TABLE / VIEW / DICTIONARY / etc."""

from __future__ import annotations

import dataclasses
import re
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from derp.chorm.column.base import Codec, Column

if TYPE_CHECKING:
    from derp.chorm.table import Table


def build_create_table(
    cls: type[Table],
    *,
    if_not_exists: bool = False,
    on_cluster: str | None = None,
) -> str:
    """Generate ``CREATE TABLE`` DDL for a :class:`Table` subclass."""
    columns = cls.get_columns()
    if not columns:
        raise ValueError(f"Table {cls.__name__} has no columns defined")

    engine = cls.get_engine()

    name = cls.get_full_name()
    head = "CREATE TABLE "
    if if_not_exists:
        head += "IF NOT EXISTS "
    head += name
    cluster = on_cluster or cls.__cluster__
    if cluster:
        head += f" ON CLUSTER {cluster}"

    column_defs: list[str] = []
    for col_name, col in columns.items():
        column_defs.append(render_column(col_name, col))

    # Inline data-skipping indexes and projections
    for idx in cls._resolved_indexes:
        column_defs.append(idx.to_ddl())
    for proj in cls._resolved_projections:
        column_defs.append(proj.to_ddl())

    body = ",\n    ".join(column_defs)
    ddl = f"{head}\n(\n    {body}\n)\n"

    # Engine clauses
    ddl += engine.engine_clause()
    for clause in (
        engine.partition_by_clause(),
        engine.primary_key_clause(),
        engine.order_by_clause(),
        engine.sample_by_clause(),
        engine.ttl_clause(),
        engine.settings_clause(),
    ):
        if clause:
            ddl += f"\n{clause}"

    if cls.__comment__:
        escaped = cls.__comment__.replace("'", "\\'")
        ddl += f"\nCOMMENT '{escaped}'"

    return ddl


def render_column(name: str, col: Column[Any]) -> str:
    """Render a single column definition."""
    parts: list[str] = [f"`{name}`", col.sql_type()]

    if col.is_materialized:
        parts.append(f"MATERIALIZED {col._materialized}")
    elif col.is_alias:
        parts.append(f"ALIAS {col._alias_expr}")
    elif col.is_ephemeral:
        if col._ephemeral_default is not None:
            parts.append(f"EPHEMERAL {_render_default(col._ephemeral_default)}")
        else:
            parts.append("EPHEMERAL")
    elif col.has_default and col.default is not dataclasses.MISSING:
        parts.append(f"DEFAULT {_render_default(col.default)}")

    if col.codec:
        codec_sql = ", ".join(c.to_sql() for c in col.codec)
        parts.append(f"CODEC({codec_sql})")

    if col.ttl:
        parts.append(f"TTL {col.ttl}")

    if col.comment:
        escaped = col.comment.replace("'", "\\'")
        parts.append(f"COMMENT '{escaped}'")

    if col._settings:
        rendered = ", ".join(
            f"{k} = {_render_setting_value(v)}" for k, v in col._settings.items()
        )
        parts.append(f"SETTINGS ({rendered})")

    return " ".join(parts)


def _render_setting_value(value: Any) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int | float):
        return str(value)
    escaped = str(value).replace("'", "\\'")
    return f"'{escaped}'"


# A bare string is treated as a SQL function call only when it is exactly
# ``identifier(...)`` — an identifier immediately followed by parentheses. This
# keeps ``now()``/``toDateTime('x')`` unquoted while rendering a literal that
# merely contains parentheses (e.g. ``"hello (world)"``) as a quoted string.
_FUNCTION_DEFAULT_RE = re.compile(r"[A-Za-z_]\w*\(.*\)", re.DOTALL)


def _render_default(value: Any) -> str:
    """Render a column DEFAULT clause."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    if isinstance(value, str):
        if value.upper() in (
            "CURRENT_TIMESTAMP",
            "CURRENT_DATE",
        ) or _FUNCTION_DEFAULT_RE.fullmatch(value):
            return value
        escaped = value.replace("'", "\\'")
        return f"'{escaped}'"
    if isinstance(value, Codec):
        return value.to_sql()
    return str(value)


# =============================================================================
# CREATE VIEW / MATERIALIZED VIEW
# =============================================================================


def build_create_view(
    name: str,
    select_sql: str,
    *,
    if_not_exists: bool = False,
    on_cluster: str | None = None,
) -> str:
    head = "CREATE VIEW "
    if if_not_exists:
        head += "IF NOT EXISTS "
    head += name
    if on_cluster:
        head += f" ON CLUSTER {on_cluster}"
    return f"{head} AS {select_sql}"


def build_create_materialized_view(
    name: str,
    select_sql: str,
    *,
    to: str | None = None,
    engine: str | None = None,
    populate: bool = False,
    if_not_exists: bool = False,
    on_cluster: str | None = None,
) -> str:
    """Render ``CREATE MATERIALIZED VIEW``.

    Pass either *to* (an existing target table) or *engine* (an inline
    engine clause).
    """
    head = "CREATE MATERIALIZED VIEW "
    if if_not_exists:
        head += "IF NOT EXISTS "
    head += name
    if on_cluster:
        head += f" ON CLUSTER {on_cluster}"
    if to is not None:
        head += f" TO {to}"
    if engine is not None:
        head += f" {engine}"
    if populate:
        head += " POPULATE"
    return f"{head} AS {select_sql}"


# =============================================================================
# CREATE DICTIONARY
# =============================================================================


def build_create_dictionary(
    name: str,
    columns: dict[str, str],
    *,
    primary_key: str,
    source: str,
    layout: str,
    lifetime: int | tuple[int, int],
    settings: dict[str, Any] | None = None,
    if_not_exists: bool = False,
    on_cluster: str | None = None,
) -> str:
    """Render ``CREATE DICTIONARY``.

    *source* should be the full ``SOURCE(...)`` body, e.g.
    ``SOURCE(HTTP(url 'http://...' format 'JSON'))``.
    *layout* should be e.g. ``LAYOUT(FLAT())`` or ``LAYOUT(HASHED())``.
    """
    head = "CREATE DICTIONARY "
    if if_not_exists:
        head += "IF NOT EXISTS "
    head += name
    if on_cluster:
        head += f" ON CLUSTER {on_cluster}"

    cols_sql = ",\n    ".join(f"`{n}` {t}" for n, t in columns.items())
    lifetime_sql = (
        f"LIFETIME(MIN {lifetime[0]} MAX {lifetime[1]})"
        if isinstance(lifetime, tuple)
        else f"LIFETIME({lifetime})"
    )

    ddl = (
        f"{head}\n(\n    {cols_sql}\n)\n"
        f"PRIMARY KEY {primary_key}\n"
        f"SOURCE({source})\n"
        f"{layout}\n"
        f"{lifetime_sql}"
    )
    if settings:
        parts = [f"{k} = {v}" for k, v in settings.items()]
        ddl += f"\nSETTINGS({', '.join(parts)})"
    return ddl


# =============================================================================
# DROP / TRUNCATE / RENAME
# =============================================================================


def build_drop_table(
    name: str,
    *,
    if_exists: bool = False,
    on_cluster: str | None = None,
    sync: bool = False,
) -> str:
    sql = "DROP TABLE "
    if if_exists:
        sql += "IF EXISTS "
    sql += name
    if on_cluster:
        sql += f" ON CLUSTER {on_cluster}"
    if sync:
        sql += " SYNC"
    return sql


def build_drop_database(
    name: str,
    *,
    if_exists: bool = False,
    on_cluster: str | None = None,
    sync: bool = False,
) -> str:
    sql = "DROP DATABASE "
    if if_exists:
        sql += "IF EXISTS "
    sql += name
    if on_cluster:
        sql += f" ON CLUSTER {on_cluster}"
    if sync:
        sql += " SYNC"
    return sql


def build_truncate_table(
    name: str,
    *,
    if_exists: bool = False,
    on_cluster: str | None = None,
) -> str:
    sql = "TRUNCATE TABLE "
    if if_exists:
        sql += "IF EXISTS "
    sql += name
    if on_cluster:
        sql += f" ON CLUSTER {on_cluster}"
    return sql


def build_rename_table(old: str, new: str, *, on_cluster: str | None = None) -> str:
    sql = f"RENAME TABLE {old} TO {new}"
    if on_cluster:
        sql += f" ON CLUSTER {on_cluster}"
    return sql


def build_exchange_tables(a: str, b: str, *, on_cluster: str | None = None) -> str:
    sql = f"EXCHANGE TABLES {a} AND {b}"
    if on_cluster:
        sql += f" ON CLUSTER {on_cluster}"
    return sql


def build_optimize_table(
    name: str,
    *,
    on_cluster: str | None = None,
    partition: str | None = None,
    final: bool = False,
    deduplicate: bool = False,
    deduplicate_by: Sequence[str] | None = None,
) -> str:
    sql = f"OPTIMIZE TABLE {name}"
    if on_cluster:
        sql += f" ON CLUSTER {on_cluster}"
    if partition:
        sql += f" PARTITION {partition}"
    if final:
        sql += " FINAL"
    if deduplicate or deduplicate_by:
        sql += " DEDUPLICATE"
        if deduplicate_by:
            sql += f" BY {', '.join(deduplicate_by)}"
    return sql


def build_create_database(
    name: str,
    *,
    if_not_exists: bool = False,
    engine: str | None = None,
    on_cluster: str | None = None,
) -> str:
    sql = "CREATE DATABASE "
    if if_not_exists:
        sql += "IF NOT EXISTS "
    sql += name
    if on_cluster:
        sql += f" ON CLUSTER {on_cluster}"
    if engine:
        sql += f" ENGINE = {engine}"
    return sql
