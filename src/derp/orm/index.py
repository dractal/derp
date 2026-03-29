"""Index definitions for Derp ORM — full PostgreSQL CREATE INDEX coverage."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, NamedTuple


class IndexMethod(StrEnum):
    """PostgreSQL index access methods."""

    BTREE = "btree"
    HASH = "hash"
    GIN = "gin"
    GIST = "gist"
    SPGIST = "spgist"
    BRIN = "brin"
    HNSW = "hnsw"
    IVFFLAT = "ivfflat"


class SortOrder(StrEnum):
    """Column sort order within an index."""

    ASC = "ASC"
    DESC = "DESC"


class NullsPosition(StrEnum):
    """NULLS FIRST / LAST within an index column."""

    FIRST = "FIRST"
    LAST = "LAST"


class IndexColumn(NamedTuple):
    """Per-column configuration within an index.

    Either *name* or *expression* must be provided::

        IndexColumn("email")
        IndexColumn("email", order=SortOrder.DESC, nulls=NullsPosition.LAST)
        IndexColumn(expression="lower(email)")
        IndexColumn("embedding", opclass="vector_cosine_ops")
    """

    name: str | None = None
    expression: str | None = None
    opclass: str | None = None
    order: SortOrder | None = None
    nulls: NullsPosition | None = None
    collation: str | None = None

    def to_ddl(self) -> str:
        """Generate the DDL fragment for this column."""
        if self.expression is not None:
            parts = [f"({self.expression})"]
        elif self.name is not None:
            parts = [self.name]
        else:
            raise ValueError("IndexColumn requires either 'name' or 'expression'.")

        if self.collation is not None:
            parts.append(f'COLLATE "{self.collation}"')
        if self.opclass is not None:
            parts.append(self.opclass)
        if self.order is not None:
            parts.append(self.order.value)
        if self.nulls is not None:
            parts.append(f"NULLS {self.nulls.value}")

        return " ".join(parts)


def _expression_to_literal_sql(expr: Any) -> str:
    """Compile a derp Expression to literal SQL (no $N parameters).

    Used for DDL contexts like partial index WHERE clauses where
    parameterized queries aren't supported.
    """
    from derp.orm.expression_base import Expression

    if isinstance(expr, str):
        return expr
    if not isinstance(expr, Expression):
        raise TypeError(f"Index 'where' must be an Expression or str, got {type(expr)}")
    params: list[Any] = []
    sql = expr.to_sql(params)

    # Substitute $N placeholders with inline literal values.
    for i, val in enumerate(params, 1):
        placeholder = f"${i}"
        if isinstance(val, bool):
            literal = "true" if val else "false"
        elif isinstance(val, str):
            escaped = val.replace("'", "''")
            literal = f"'{escaped}'"
        elif val is None:
            literal = "NULL"
        else:
            literal = str(val)
        sql = sql.replace(placeholder, literal, 1)

    return sql


def _resolve_column_name(col: Any) -> str:
    """Extract a column name from a ``str`` or ``Column`` descriptor."""
    if isinstance(col, str):
        return col
    # Column descriptor — has _field_name set by __set_name__
    field_name = getattr(col, "_field_name", None)
    if field_name is not None:
        return field_name
    raise TypeError(
        f"Expected a column name (str) or Column descriptor, got {type(col)}"
    )


class Index:
    """Full PostgreSQL index definition.

    Accepts flexible column input::

        Index("email")
        Index("created_at", "name")
        Index(IndexColumn("email", order=SortOrder.DESC))

    Index-level options::

        Index("embedding", method=IndexMethod.HNSW, opclass="vector_cosine_ops")
        Index("status", where=MyTable.is_active == True)
        Index("id", include=("name",))
        Index(expression="lower(email)", unique=True)
    """

    __slots__ = (
        "_columns",
        "method",
        "unique",
        "where",
        "include",
        "nulls_distinct",
        "with_params",
        "tablespace",
        "concurrently",
        "name",
        "opclass",
    )

    def __init__(
        self,
        *columns: str | IndexColumn | Any,  # Any covers Column[T]
        method: IndexMethod | str = IndexMethod.BTREE,
        unique: bool = False,
        where: Any | None = None,
        include: tuple[str, ...] | list[str] = (),
        nulls_distinct: bool = True,
        with_params: dict[str, str] | None = None,
        tablespace: str | None = None,
        concurrently: bool = False,
        name: str | None = None,
        opclass: str | None = None,
        # Per-column shortcuts when passing a single column str
        order: SortOrder | str | None = None,
        nulls: NullsPosition | str | None = None,
        collation: str | None = None,
        expression: str | None = None,
    ) -> None:
        if expression is not None and not columns:
            self._columns = (
                IndexColumn(
                    expression=expression,
                    opclass=opclass,
                    order=SortOrder(order) if order else None,
                    nulls=NullsPosition(nulls) if nulls else None,
                    collation=collation,
                ),
            )
        else:
            normalized: list[IndexColumn] = []
            single = len(columns) == 1
            for c in columns:
                if isinstance(c, IndexColumn):
                    normalized.append(c)
                else:
                    col_name = _resolve_column_name(c)
                    normalized.append(
                        IndexColumn(
                            name=col_name,
                            opclass=opclass if single else None,
                            order=SortOrder(order) if order and single else None,
                            nulls=NullsPosition(nulls) if nulls and single else None,
                            collation=collation if single else None,
                        )
                    )
            self._columns = tuple(normalized)

        self.method = IndexMethod(method)
        self.unique = unique
        self.where = where
        self.include = tuple(include)
        self.nulls_distinct = nulls_distinct
        self.with_params = with_params or {}
        self.tablespace = tablespace
        self.concurrently = concurrently
        self.name = name
        self.opclass = opclass

    @property
    def columns(self) -> tuple[IndexColumn, ...]:
        return self._columns

    @property
    def column_names(self) -> list[str]:
        """Plain column names for snapshot compat."""
        return [c.name if c.name else f"({c.expression})" for c in self._columns]

    def auto_name(self, table_name: str) -> str:
        """Generate a conventional index name."""
        if self.name:
            return self.name
        prefix = "uniq" if self.unique else "idx"
        parts: list[str] = []
        for c in self._columns:
            if c.name:
                parts.append(c.name)
            elif c.expression:
                clean = (
                    c.expression.replace("(", "")
                    .replace(")", "")
                    .replace("'", "")
                    .replace(",", "")
                    .replace(" ", "_")
                    .lower()
                )
                parts.append(clean)
        return f"{prefix}_{table_name}_{'_'.join(parts)}"

    def to_ddl(self, table_name: str) -> str:
        """Generate the full ``CREATE INDEX`` statement."""
        idx_name = self.auto_name(table_name)

        parts: list[str] = ["CREATE"]
        if self.unique:
            parts.append("UNIQUE")
        parts.append("INDEX")
        if self.concurrently:
            parts.append("CONCURRENTLY")
        parts.append(idx_name)
        parts.append("ON")
        parts.append(table_name)

        if self.method != IndexMethod.BTREE:
            parts.append(f"USING {self.method}")

        col_ddl = ", ".join(c.to_ddl() for c in self._columns)
        parts.append(f"({col_ddl})")

        if self.include:
            parts.append(f"INCLUDE ({', '.join(self.include)})")

        if not self.nulls_distinct:
            parts.append("NULLS NOT DISTINCT")

        if self.with_params:
            param_str = ", ".join(f"{k} = {v}" for k, v in self.with_params.items())
            parts.append(f"WITH ({param_str})")

        if self.tablespace:
            parts.append(f"TABLESPACE {self.tablespace}")

        if self.where is not None:
            parts.append(f"WHERE {_expression_to_literal_sql(self.where)}")

        return " ".join(parts)

    def __repr__(self) -> str:
        col_repr = ", ".join(c.name or f"expr={c.expression!r}" for c in self._columns)
        return f"Index({col_repr}, method={self.method})"


def normalize_indexes(
    raw: list[tuple[str, ...] | Index],
) -> list[Index]:
    """Convert a mixed ``__indexes__`` list to ``Index`` objects.

    Tuples of strings are converted to simple BTREE indexes.
    """
    result: list[Index] = []
    for item in raw:
        if isinstance(item, Index):
            result.append(item)
        else:
            result.append(Index(*item))
    return result
