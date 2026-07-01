"""Schema snapshot models for migration diffing.

A snapshot is a JSON-serializable representation of one or more
ClickHouse tables: name, engine, columns, indexes, projections, etc.
Snapshots are the source of truth that the differ compares.
"""

from __future__ import annotations

import dataclasses
import json
from typing import TYPE_CHECKING, Any

from derp.chorm.ddl import _render_default as _render_sql_default

if TYPE_CHECKING:
    from derp.chorm.table import Table


@dataclasses.dataclass
class ColumnSnapshot:
    name: str
    type: str
    default: str | None = None
    materialized: str | None = None
    alias: str | None = None
    ephemeral: bool = False
    codec: str | None = None
    ttl: str | None = None
    comment: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class EngineSnapshot:
    name: str
    args: tuple[str, ...] = ()
    order_by: str | None = None
    partition_by: str | None = None
    primary_key: str | None = None
    sample_by: str | None = None
    ttl: str | None = None
    settings: dict[str, str] = dataclasses.field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "args": list(self.args),
            "order_by": self.order_by,
            "partition_by": self.partition_by,
            "primary_key": self.primary_key,
            "sample_by": self.sample_by,
            "ttl": self.ttl,
            "settings": dict(self.settings),
        }


@dataclasses.dataclass
class IndexSnapshot:
    name: str
    expression: str
    type: str
    granularity: int = 1

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class ProjectionSnapshot:
    name: str
    select: str
    order_by: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class TableSnapshot:
    name: str
    database: str | None = None
    cluster: str | None = None
    columns: list[ColumnSnapshot] = dataclasses.field(default_factory=list)
    indexes: list[IndexSnapshot] = dataclasses.field(default_factory=list)
    projections: list[ProjectionSnapshot] = dataclasses.field(default_factory=list)
    engine: EngineSnapshot | None = None
    comment: str | None = None

    @property
    def key(self) -> str:
        if self.database:
            return f"{self.database}.{self.name}"
        return self.name

    def column_map(self) -> dict[str, ColumnSnapshot]:
        return {c.name: c for c in self.columns}

    def projection_map(self) -> dict[str, ProjectionSnapshot]:
        return {p.name: p for p in self.projections}

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "database": self.database,
            "cluster": self.cluster,
            "columns": [c.to_dict() for c in self.columns],
            "indexes": [i.to_dict() for i in self.indexes],
            "projections": [p.to_dict() for p in self.projections],
            "engine": self.engine.to_dict() if self.engine else None,
            "comment": self.comment,
        }


@dataclasses.dataclass
class SchemaSnapshot:
    tables: list[TableSnapshot] = dataclasses.field(default_factory=list)

    def table_map(self) -> dict[str, TableSnapshot]:
        return {t.key: t for t in self.tables}

    def to_dict(self) -> dict[str, Any]:
        return {"tables": [t.to_dict() for t in self.tables]}

    def to_json(self, *, indent: int | None = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    @classmethod
    def from_json(cls, data: str | bytes) -> SchemaSnapshot:
        return cls.from_dict(json.loads(data))

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SchemaSnapshot:
        tables = []
        for t in data.get("tables", []):
            cols = [ColumnSnapshot(**c) for c in t.get("columns", [])]
            idxs = [IndexSnapshot(**i) for i in t.get("indexes", [])]
            projs = [ProjectionSnapshot(**p) for p in t.get("projections", [])]
            eng_d = t.get("engine")
            eng = None
            if eng_d:
                eng = EngineSnapshot(
                    name=eng_d["name"],
                    args=tuple(eng_d.get("args", ())),
                    order_by=eng_d.get("order_by"),
                    partition_by=eng_d.get("partition_by"),
                    primary_key=eng_d.get("primary_key"),
                    sample_by=eng_d.get("sample_by"),
                    ttl=eng_d.get("ttl"),
                    settings=eng_d.get("settings", {}),
                )
            tables.append(
                TableSnapshot(
                    name=t["name"],
                    database=t.get("database"),
                    cluster=t.get("cluster"),
                    columns=cols,
                    indexes=idxs,
                    projections=projs,
                    engine=eng,
                    comment=t.get("comment"),
                )
            )
        return SchemaSnapshot(tables=tables)


def snapshot_from_tables(tables: list[type[Table]]) -> SchemaSnapshot:
    """Build a :class:`SchemaSnapshot` from a list of Table subclasses."""
    out: list[TableSnapshot] = []
    for t in tables:
        out.append(_snapshot_one(t))
    return SchemaSnapshot(tables=out)


def _snapshot_one(t: type[Table]) -> TableSnapshot:
    cols: list[ColumnSnapshot] = []
    for name, col in t.get_columns().items():
        cols.append(
            ColumnSnapshot(
                name=name,
                type=col.sql_type(),
                default=_render_default(col),
                materialized=col._materialized,
                alias=col._alias_expr,
                ephemeral=col.is_ephemeral,
                codec=_render_codec(col.codec),
                ttl=col.ttl,
                comment=col.comment,
            )
        )

    indexes: list[IndexSnapshot] = []
    for i in t._resolved_indexes:
        indexes.append(
            IndexSnapshot(
                name=i.name,
                expression=i.expression,
                type=str(i.type),
                granularity=i.granularity,
            )
        )

    projections: list[ProjectionSnapshot] = []
    for p in t._resolved_projections:
        projections.append(
            ProjectionSnapshot(
                name=p.name,
                select=p.select,
                order_by=p.order_by,
            )
        )

    engine = _snapshot_engine(t.get_engine())

    return TableSnapshot(
        name=t.get_table_name(),
        database=t.__database__,
        cluster=t.__cluster__,
        columns=cols,
        indexes=indexes,
        projections=projections,
        engine=engine,
        comment=t.__comment__,
    )


def _render_default(col: Any) -> str | None:
    if not col.has_default:
        return None
    d = col.default
    if d is None or d is dataclasses.MISSING:
        return None
    # Reuse the DDL renderer so the snapshot stores SQL-ready defaults
    # (quoted string literals, pass-through function calls) that match what
    # introspection reads back from ClickHouse. Storing the raw value here
    # produces invalid DDL like `DEFAULT active` instead of `DEFAULT 'active'`.
    return _render_sql_default(d)


def _render_codec(codec: Any) -> str | None:
    if not codec:
        return None
    return ", ".join(c.to_sql() for c in codec)


def _snapshot_engine(engine: Any) -> EngineSnapshot:
    """Snapshot a TableEngine instance.

    Captures the engine name, args, and trailing clauses by inspecting
    the rendered clauses (so this is engine-agnostic).
    """
    name = getattr(engine, "_name", type(engine).__name__)
    args = getattr(engine, "_args", ())
    args_str = tuple(_render_engine_arg(a) for a in args)

    def _strip_clause(clause: str | None, prefix: str) -> str | None:
        if clause is None:
            return None
        if clause.upper().startswith(prefix.upper()):
            return clause[len(prefix) :].strip()
        return clause

    settings: dict[str, str] = {}
    s_clause = engine.settings_clause() if hasattr(engine, "settings_clause") else None
    if s_clause:
        body = s_clause[len("SETTINGS") :].strip()
        for part in _split_settings(body):
            if "=" in part:
                k, v = part.split("=", 1)
                settings[k.strip()] = v.strip()

    return EngineSnapshot(
        name=name,
        args=args_str,
        order_by=_strip_clause(
            engine.order_by_clause() if hasattr(engine, "order_by_clause") else None,
            "ORDER BY",
        ),
        partition_by=_strip_clause(
            engine.partition_by_clause()
            if hasattr(engine, "partition_by_clause")
            else None,
            "PARTITION BY",
        ),
        primary_key=_strip_clause(
            engine.primary_key_clause()
            if hasattr(engine, "primary_key_clause")
            else None,
            "PRIMARY KEY",
        ),
        sample_by=_strip_clause(
            engine.sample_by_clause() if hasattr(engine, "sample_by_clause") else None,
            "SAMPLE BY",
        ),
        ttl=_strip_clause(
            engine.ttl_clause() if hasattr(engine, "ttl_clause") else None,
            "TTL",
        ),
        settings=settings,
    )


def _render_engine_arg(a: Any) -> str:
    return str(a)


def _split_settings(body: str) -> list[str]:
    """Split a SETTINGS body on commas not inside quoted strings."""
    out: list[str] = []
    buf = []
    in_quote = False
    quote_char = ""
    for ch in body:
        if in_quote:
            buf.append(ch)
            if ch == quote_char:
                in_quote = False
        else:
            if ch in ("'", '"'):
                in_quote = True
                quote_char = ch
                buf.append(ch)
            elif ch == ",":
                out.append("".join(buf).strip())
                buf = []
            else:
                buf.append(ch)
    if buf:
        out.append("".join(buf).strip())
    return out
