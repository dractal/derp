"""Query builders for ClickHouse: SELECT, INSERT, ALTER, mutations."""

from __future__ import annotations

import dataclasses
from collections.abc import Sequence
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Self

from derp.chorm.column.base import Column
from derp.chorm.exceptions import ChormNotConnectedError, NoRowsError
from derp.chorm.expression_base import Params
from derp.chorm.query.expressions import (
    ColumnRef,
    ExistsExpr,
    Expression,
    FunctionCall,
    RawSQL,
    SubqueryExpr,
)
from derp.chorm.table import Table

if TYPE_CHECKING:
    from derp.chorm.engine import ClickHouseEngine


# =============================================================================
# Enums
# =============================================================================


class JoinType(StrEnum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"


class JoinStrictness(StrEnum):
    """ClickHouse join strictness."""

    ALL = "ALL"
    ANY = "ANY"
    ASOF = "ASOF"
    SEMI = "SEMI"
    ANTI = "ANTI"


class OrderDirection(StrEnum):
    ASC = "ASC"
    DESC = "DESC"


# =============================================================================
# Clause dataclasses
# =============================================================================


@dataclasses.dataclass
class JoinClause:
    type: JoinType
    table: Any  # Table class | str | SubqueryExpr
    on: Expression | None = None
    using: tuple[str, ...] | None = None
    strictness: JoinStrictness | None = None
    global_: bool = False
    alias: str | None = None


@dataclasses.dataclass
class OrderByClause:
    expr: Any
    direction: OrderDirection = OrderDirection.ASC
    fill: bool = False
    fill_from: Any = None
    fill_to: Any = None
    fill_step: Any = None
    nulls: str | None = None  # "FIRST" or "LAST"
    collate: str | None = None


@dataclasses.dataclass
class SampleClause:
    coefficient: float | int
    offset: float | int | None = None


@dataclasses.dataclass
class ArrayJoinClause:
    expressions: tuple[Any, ...]
    left: bool = False


@dataclasses.dataclass
class LimitByClause:
    n: int
    offset: int | None
    columns: tuple[Any, ...]


@dataclasses.dataclass
class WithFill:
    enabled: bool = False
    from_: Any = None
    to: Any = None
    step: Any = None


# =============================================================================
# Utilities
# =============================================================================


def _render_col(c: Any, params: Params) -> str:
    if isinstance(c, Column):
        return c.to_sql(params)
    if isinstance(c, Expression):
        return c.to_sql(params)
    if isinstance(c, str):
        # Treat as raw column / expression.
        return c
    return str(c)


def _table_ref(t: Any) -> str:
    if isinstance(t, type) and issubclass(t, Table):
        return t.get_full_name()
    if isinstance(t, str):
        return t
    if isinstance(t, SubqueryExpr):
        return ""  # rendered inline elsewhere
    raise TypeError(f"Cannot resolve table reference from {type(t).__name__}")


def _add_alias(aliases: dict[str, str], table: type[Table], alias: str) -> None:
    aliases[table.get_table_name()] = alias
    aliases[table.get_full_name()] = alias


def _table_ref_for_columns(table: type[Table], params: Params) -> str:
    return params.table_aliases.get(
        table.get_full_name(),
        params.table_aliases.get(table.get_table_name(), table.get_full_name()),
    )


def _quote_table_ref(name: str) -> str:
    return ".".join(f"`{part.replace('`', '')}`" for part in name.split("."))


# =============================================================================
# WHERE shorthand mixin
# =============================================================================


class _WhereShorthand:
    """Shorthand helpers usable by SELECT / UPDATE / DELETE builders."""

    def where(self, cond: Expression) -> Self:
        raise NotImplementedError

    def _resolve_col(self, column: Column[Any] | str) -> Column[Any] | ColumnRef:
        if isinstance(column, Column):
            return column
        if "." in column:
            t, c = column.split(".", 1)
            return ColumnRef(t, c)
        return ColumnRef(None, column)

    def eq(self, column: Column[Any] | str, value: Any) -> Self:
        return self.where(self._resolve_col(column) == value)

    def neq(self, column: Column[Any] | str, value: Any) -> Self:
        return self.where(self._resolve_col(column) != value)

    def gt(self, column: Column[Any] | str, value: Any) -> Self:
        return self.where(self._resolve_col(column) > value)

    def gte(self, column: Column[Any] | str, value: Any) -> Self:
        return self.where(self._resolve_col(column) >= value)

    def lt(self, column: Column[Any] | str, value: Any) -> Self:
        return self.where(self._resolve_col(column) < value)

    def lte(self, column: Column[Any] | str, value: Any) -> Self:
        return self.where(self._resolve_col(column) <= value)

    def is_null(self, column: Column[Any] | str) -> Self:
        return self.where(self._resolve_col(column).is_null())

    def is_not_null(self, column: Column[Any] | str) -> Self:
        return self.where(self._resolve_col(column).is_not_null())

    def in_(self, column: Column[Any] | str, values: Sequence[Any]) -> Self:
        return self.where(self._resolve_col(column).in_(values))

    def not_in(self, column: Column[Any] | str, values: Sequence[Any]) -> Self:
        return self.where(self._resolve_col(column).not_in(values))

    def like(self, column: Column[Any] | str, pattern: str) -> Self:
        return self.where(self._resolve_col(column).like(pattern))


# =============================================================================
# SELECT
# =============================================================================


class SelectQuery[T](_WhereShorthand):
    """Strongly-typed SELECT builder for ClickHouse.

    ``T`` is the result element type: a Table subclass, a Python scalar,
    or a tuple, depending on which columns are projected.
    """

    def __init__(
        self,
        engine: ClickHouseEngine | None,
        columns: tuple[Any, ...],
    ) -> None:
        self._engine = engine
        self._columns: tuple[Any, ...] = columns
        self._from_table: Any = None
        self._from_alias: str | None = None
        self._final: bool = False
        self._sample: SampleClause | None = None
        self._array_join: ArrayJoinClause | None = None
        self._prewhere_clause: Expression | None = None
        self._where_clause: Expression | None = None
        self._group_by: list[Any] = []
        self._group_with: str | None = None  # ROLLUP / CUBE / TOTALS
        self._having_clause: Expression | None = None
        self._order_by: list[OrderByClause] = []
        self._limit_value: int | None = None
        self._offset_value: int | None = None
        self._limit_by: LimitByClause | None = None
        self._distinct: bool = False
        self._distinct_on: list[Any] = []
        self._ctes: list[tuple[str, SelectQuery[Any]]] = []
        self._joins: list[JoinClause] = []
        self._settings: dict[str, Any] = {}
        self._format: str | None = None
        self._with_totals: bool = False

        if columns and isinstance(columns[0], type) and issubclass(columns[0], Table):
            self._from_table = columns[0]
        else:
            # Try to infer FROM from a Column descriptor's owning table.
            for c in columns:
                if isinstance(c, Column) and c._table_name:
                    # Look up the Table subclass by walking Table's
                    # subclasses for a matching name.  Best-effort —
                    # falls back to a string ref.
                    inferred = _find_table_by_name(c._table_name)
                    if inferred is not None:
                        self._from_table = inferred
                        break
                    self._from_table = c._table_name
                    break

    # -- Building chain methods ----------------------------------------------

    def from_(self, table: Any, *, alias: str | None = None) -> Self:
        self._from_table = table
        self._from_alias = alias
        return self

    def final(self) -> Self:
        """Add the ``FINAL`` modifier (forces merge on read)."""
        self._final = True
        return self

    def sample(self, k: float | int, offset: float | int | None = None) -> Self:
        """Add a ``SAMPLE`` clause."""
        self._sample = SampleClause(k, offset)
        return self

    def array_join(self, *exprs: Any, left: bool = False) -> Self:
        """Add ``ARRAY JOIN`` (or ``LEFT ARRAY JOIN``)."""
        self._array_join = ArrayJoinClause(tuple(exprs), left=left)
        return self

    def prewhere(self, cond: Expression) -> Self:
        if self._prewhere_clause is not None:
            self._prewhere_clause = self._prewhere_clause & cond
        else:
            self._prewhere_clause = cond
        return self

    def where(self, cond: Expression) -> Self:
        if self._where_clause is not None:
            self._where_clause = self._where_clause & cond
        else:
            self._where_clause = cond
        return self

    def group_by(self, *cols: Any) -> Self:
        self._group_by.extend(cols)
        return self

    def with_rollup(self) -> Self:
        self._group_with = "ROLLUP"
        return self

    def with_cube(self) -> Self:
        self._group_with = "CUBE"
        return self

    def with_totals(self) -> Self:
        self._with_totals = True
        return self

    def having(self, cond: Expression) -> Self:
        if self._having_clause is not None:
            self._having_clause = self._having_clause & cond
        else:
            self._having_clause = cond
        return self

    def order_by(
        self,
        expr: Any,
        *,
        asc: bool = True,
        desc: bool = False,
        nulls: str | None = None,
        collate: str | None = None,
        with_fill: bool = False,
        fill_from: Any = None,
        fill_to: Any = None,
        fill_step: Any = None,
    ) -> Self:
        # `asc=` mirrors the postgres ORM; `desc=` is the ClickHouse-native flag.
        # Either expresses descending order (desc=True or asc=False).
        direction = OrderDirection.DESC if (desc or not asc) else OrderDirection.ASC
        self._order_by.append(
            OrderByClause(
                expr=expr,
                direction=direction,
                fill=with_fill,
                fill_from=fill_from,
                fill_to=fill_to,
                fill_step=fill_step,
                nulls=nulls,
                collate=collate,
            )
        )
        return self

    def limit(self, n: int) -> Self:
        self._limit_value = n
        return self

    def offset(self, n: int) -> Self:
        self._offset_value = n
        return self

    def limit_by(self, n: int, *cols: Any, offset: int | None = None) -> Self:
        self._limit_by = LimitByClause(n, offset, tuple(cols))
        return self

    def distinct(self, *cols: Any) -> Self:
        if cols:
            self._distinct_on.extend(cols)
        else:
            self._distinct = True
        return self

    def with_cte(self, name: str, query: SelectQuery[Any]) -> Self:
        self._ctes.append((name, query))
        return self

    def settings(self, **kwargs: Any) -> Self:
        self._settings.update(kwargs)
        return self

    def format(self, fmt: str) -> Self:
        self._format = fmt
        return self

    # -- Joins ---------------------------------------------------------------

    def _join(
        self,
        type_: JoinType,
        table: Any,
        *,
        on: Expression | None = None,
        using: tuple[str, ...] | str | None = None,
        strictness: JoinStrictness | None = None,
        global_: bool = False,
        alias: str | None = None,
    ) -> Self:
        if isinstance(using, str):
            using = (using,)
        self._joins.append(
            JoinClause(
                type=type_,
                table=table,
                on=on,
                using=using,
                strictness=strictness,
                global_=global_,
                alias=alias,
            )
        )
        return self

    def inner_join(
        self,
        table: Any,
        *,
        on: Expression | None = None,
        using: tuple[str, ...] | str | None = None,
        strictness: JoinStrictness | None = None,
        global_: bool = False,
        alias: str | None = None,
    ) -> Self:
        return self._join(
            JoinType.INNER,
            table,
            on=on,
            using=using,
            strictness=strictness,
            global_=global_,
            alias=alias,
        )

    def left_join(
        self,
        table: Any,
        *,
        on: Expression | None = None,
        using: tuple[str, ...] | str | None = None,
        strictness: JoinStrictness | None = None,
        global_: bool = False,
        alias: str | None = None,
    ) -> Self:
        return self._join(
            JoinType.LEFT,
            table,
            on=on,
            using=using,
            strictness=strictness,
            global_=global_,
            alias=alias,
        )

    def right_join(
        self,
        table: Any,
        *,
        on: Expression | None = None,
        using: tuple[str, ...] | str | None = None,
        strictness: JoinStrictness | None = None,
        global_: bool = False,
        alias: str | None = None,
    ) -> Self:
        return self._join(
            JoinType.RIGHT,
            table,
            on=on,
            using=using,
            strictness=strictness,
            global_=global_,
            alias=alias,
        )

    def full_join(
        self,
        table: Any,
        *,
        on: Expression | None = None,
        using: tuple[str, ...] | str | None = None,
        strictness: JoinStrictness | None = None,
        global_: bool = False,
        alias: str | None = None,
    ) -> Self:
        return self._join(
            JoinType.FULL,
            table,
            on=on,
            using=using,
            strictness=strictness,
            global_=global_,
            alias=alias,
        )

    def cross_join(self, table: Any, *, alias: str | None = None) -> Self:
        return self._join(JoinType.CROSS, table, alias=alias)

    def asof_join(
        self,
        table: Any,
        *,
        on: Expression | None = None,
        using: tuple[str, ...] | str | None = None,
        alias: str | None = None,
    ) -> Self:
        """Add an ``ASOF`` join (last-match join on inequality keys)."""
        return self._join(
            JoinType.INNER,
            table,
            on=on,
            using=using,
            strictness=JoinStrictness.ASOF,
            alias=alias,
        )

    # -- Subquery wrappers ---------------------------------------------------

    def as_(self, alias: str) -> SubqueryExpr:
        return SubqueryExpr(self, alias)

    def exists(self) -> ExistsExpr:
        return ExistsExpr(SubqueryExpr(self))

    def union(self, other: SelectQuery[Any]) -> SetOperationQuery[T]:
        return SetOperationQuery(self, "UNION", other)

    def union_all(self, other: SelectQuery[Any]) -> SetOperationQuery[T]:
        return SetOperationQuery(self, "UNION ALL", other)

    def union_distinct(self, other: SelectQuery[Any]) -> SetOperationQuery[T]:
        return SetOperationQuery(self, "UNION DISTINCT", other)

    def intersect(self, other: SelectQuery[Any]) -> SetOperationQuery[T]:
        return SetOperationQuery(self, "INTERSECT", other)

    def except_(self, other: SelectQuery[Any]) -> SetOperationQuery[T]:
        return SetOperationQuery(self, "EXCEPT", other)

    # -- Build / execute -----------------------------------------------------

    def build(self) -> tuple[str, dict[str, Any]]:
        """Return ``(sql, parameter_values)``."""
        params = Params()
        sql = self.build_into(params)
        return sql, params.values

    def build_into(self, params: Params) -> str:
        """Build SQL using an existing parameter accumulator (used by subqueries)."""
        alias_scope = self._collect_table_aliases()
        previous_aliases = {k: params.table_aliases.get(k) for k in alias_scope}
        params.table_aliases.update(alias_scope)

        parts: list[str] = []

        if self._ctes:
            cte_parts = []
            for cte_name, cte_q in self._ctes:
                sub_sql = cte_q.build_into(params)
                cte_parts.append(f"{cte_name} AS ({sub_sql})")
            parts.append(f"WITH {', '.join(cte_parts)}")

        # SELECT
        select_parts: list[str] = []
        for col in self._columns:
            if isinstance(col, type) and issubclass(col, Table):
                # SELECT cols of table — render explicit names to match
                # Table column order on result hydration.
                t_name = _table_ref_for_columns(col, params)
                for c_name in col.__columns__:
                    select_parts.append(f"{_quote_table_ref(t_name)}.`{c_name}`")
            elif isinstance(col, Column):
                select_parts.append(col.to_sql(params))
            elif isinstance(col, Expression):
                select_parts.append(col.to_sql(params))
            elif isinstance(col, str):
                select_parts.append(col)
            else:
                select_parts.append(str(col))

        # DISTINCT
        if self._distinct_on:
            d_parts = [_render_col(c, params) for c in self._distinct_on]
            distinct = f"DISTINCT ON ({', '.join(d_parts)}) "
        elif self._distinct:
            distinct = "DISTINCT "
        else:
            distinct = ""

        parts.append(f"SELECT {distinct}{', '.join(select_parts)}")

        # FROM
        if self._from_table is not None:
            from_sql = self._from_clause(params)
            parts.append(from_sql)

        # JOINs
        for join in self._joins:
            parts.append(self._join_clause(join, params))

        # PREWHERE
        if self._prewhere_clause is not None:
            parts.append(f"PREWHERE {self._prewhere_clause.to_sql(params)}")

        # WHERE
        if self._where_clause is not None:
            parts.append(f"WHERE {self._where_clause.to_sql(params)}")

        # GROUP BY
        if self._group_by:
            g_parts = [_render_col(c, params) for c in self._group_by]
            group_clause = f"GROUP BY {', '.join(g_parts)}"
            if self._group_with == "ROLLUP":
                group_clause += " WITH ROLLUP"
            elif self._group_with == "CUBE":
                group_clause += " WITH CUBE"
            if self._with_totals:
                group_clause += " WITH TOTALS"
            parts.append(group_clause)
        elif self._with_totals:
            parts.append("WITH TOTALS")

        # HAVING
        if self._having_clause is not None:
            parts.append(f"HAVING {self._having_clause.to_sql(params)}")

        # ORDER BY
        if self._order_by:
            order_parts: list[str] = []
            for o in self._order_by:
                p = _render_col(o.expr, params)
                p += f" {o.direction}"
                if o.nulls:
                    p += f" NULLS {o.nulls}"
                if o.collate:
                    p += f" COLLATE '{o.collate}'"
                if o.fill:
                    p += " WITH FILL"
                    if o.fill_from is not None:
                        p += f" FROM {_render_col(o.fill_from, params)}"
                    if o.fill_to is not None:
                        p += f" TO {_render_col(o.fill_to, params)}"
                    if o.fill_step is not None:
                        p += f" STEP {_render_col(o.fill_step, params)}"
                order_parts.append(p)
            parts.append(f"ORDER BY {', '.join(order_parts)}")

        # LIMIT BY
        if self._limit_by is not None:
            lb = self._limit_by
            lb_cols = [_render_col(c, params) for c in lb.columns]
            if lb.offset is not None:
                parts.append(f"LIMIT {lb.offset}, {lb.n} BY {', '.join(lb_cols)}")
            else:
                parts.append(f"LIMIT {lb.n} BY {', '.join(lb_cols)}")

        # LIMIT / OFFSET
        if self._limit_value is not None:
            if self._offset_value is not None:
                parts.append(f"LIMIT {self._offset_value}, {self._limit_value}")
            else:
                parts.append(f"LIMIT {self._limit_value}")
        elif self._offset_value is not None:
            parts.append(f"OFFSET {self._offset_value}")

        # SETTINGS
        if self._settings:
            s_parts = [
                f"{k} = {_render_setting_value(v)}" for k, v in self._settings.items()
            ]
            parts.append(f"SETTINGS {', '.join(s_parts)}")

        # FORMAT
        if self._format:
            parts.append(f"FORMAT {self._format}")

        sql = " ".join(parts)
        for key, value in previous_aliases.items():
            if value is None:
                params.table_aliases.pop(key, None)
            else:
                params.table_aliases[key] = value
        return sql

    def _collect_table_aliases(self) -> dict[str, str]:
        aliases: dict[str, str] = {}
        if (
            self._from_alias
            and isinstance(self._from_table, type)
            and issubclass(self._from_table, Table)
        ):
            _add_alias(aliases, self._from_table, self._from_alias)
        for join in self._joins:
            if (
                join.alias
                and isinstance(join.table, type)
                and issubclass(join.table, Table)
            ):
                _add_alias(aliases, join.table, join.alias)
        return aliases

    def _from_clause(self, params: Params) -> str:
        t = self._from_table
        if isinstance(t, type) and issubclass(t, Table):
            sql = f"FROM {t.get_full_name()}"
        elif isinstance(t, SubqueryExpr):
            sql = f"FROM {t.to_sql(params)}"
        elif isinstance(t, str):
            sql = f"FROM {t}"
        else:
            raise TypeError(f"Bad FROM table: {type(t).__name__}")

        if self._from_alias:
            sql += f" AS {self._from_alias}"
        if self._final:
            sql += " FINAL"
        if self._sample:
            s = self._sample
            if s.offset is not None:
                sql += f" SAMPLE {s.coefficient} OFFSET {s.offset}"
            else:
                sql += f" SAMPLE {s.coefficient}"
        if self._array_join:
            aj = self._array_join
            keyword = "LEFT ARRAY JOIN" if aj.left else "ARRAY JOIN"
            aj_parts = [_render_col(e, params) for e in aj.expressions]
            sql += f" {keyword} {', '.join(aj_parts)}"
        return sql

    def _join_clause(self, j: JoinClause, params: Params) -> str:
        parts: list[str] = []
        if j.global_:
            parts.append("GLOBAL")
        if j.strictness:
            parts.append(str(j.strictness))
        parts.append(str(j.type))
        parts.append("JOIN")
        if isinstance(j.table, type) and issubclass(j.table, Table):
            parts.append(j.table.get_full_name())
        elif isinstance(j.table, SubqueryExpr):
            parts.append(j.table.to_sql(params))
        else:
            parts.append(str(j.table))
        if j.alias:
            parts.append(f"AS {j.alias}")
        if j.using:
            parts.append(f"USING ({', '.join(j.using)})")
        elif j.on is not None:
            parts.append(f"ON {j.on.to_sql(params)}")
        return " ".join(parts)

    async def execute(self) -> list[T]:
        if self._engine is None:
            raise ChormNotConnectedError(
                "SelectQuery has no engine bound — call db.select(...)."
            )
        sql, values = self.build()
        rows = await self._engine.fetch(sql, parameters=values)
        return self._hydrate(rows)

    async def first(self) -> T:
        result = await self.first_or_none()
        if result is None:
            raise NoRowsError("SELECT returned no rows")
        return result

    async def first_or_none(self) -> T | None:
        self.limit(1)
        results = await self.execute()
        return results[0] if results else None

    async def count(self) -> int:
        sub = SelectQuery(self._engine, (FunctionCall("count", ()),))
        sub._from_table = SubqueryExpr(self)
        sql, values = sub.build()
        rows = await self._engine.fetch(sql, parameters=values)  # type: ignore[union-attr]
        if not rows:
            return 0
        first = rows[0]
        if isinstance(first, dict):
            return int(next(iter(first.values())))
        return int(first[0])

    def _hydrate(self, rows: list[Any]) -> list[Any]:
        model = self._single_table_model()
        if model is not None:
            return [model._from_row(r) for r in rows]
        if self._is_single_scalar():
            out: list[Any] = []
            for r in rows:
                if isinstance(r, dict):
                    out.append(next(iter(r.values())))
                else:
                    out.append(r[0])
            return out
        if self._is_multi_column():
            out2: list[Any] = []
            for r in rows:
                if isinstance(r, dict):
                    out2.append(tuple(r.values()))
                else:
                    out2.append(tuple(r))
            return out2
        return rows

    def _single_table_model(self) -> type[Table] | None:
        if (
            len(self._columns) == 1
            and isinstance(self._columns[0], type)
            and issubclass(self._columns[0], Table)
        ):
            return self._columns[0]
        return None

    def _is_single_scalar(self) -> bool:
        return len(self._columns) == 1 and not isinstance(self._columns[0], type)

    def _is_multi_column(self) -> bool:
        return len(self._columns) > 1


def _find_table_by_name(name: str) -> type[Table] | None:
    """Walk Table subclasses to find one with matching ``__table_name__``."""
    seen: set[type[Table]] = set()
    stack: list[type[Table]] = list(Table.__subclasses__())
    while stack:
        cls = stack.pop()
        if cls in seen:
            continue
        seen.add(cls)
        if getattr(cls, "__table_name__", None) == name:
            return cls
        stack.extend(cls.__subclasses__())
    return None


def _render_setting_value(v: Any) -> str:
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, int | float):
        return str(v)
    escaped = str(v).replace("'", "\\'")
    return f"'{escaped}'"


# =============================================================================
# Set operations (UNION / INTERSECT / EXCEPT)
# =============================================================================


class SetOperationQuery[T]:
    """``a UNION ALL b`` and friends.

    Supports trailing ``order_by``/``limit``/``offset`` over the combined
    result, plus chaining further set operations.
    """

    def __init__(
        self,
        left: SelectQuery[T] | SetOperationQuery[T],
        op: str,
        right: SelectQuery[Any],
    ) -> None:
        self._left = left
        self._op = op
        self._right = right
        self._order_by: list[OrderByClause] = []
        self._limit_value: int | None = None
        self._offset_value: int | None = None

    def order_by(self, expr: Any, *, asc: bool = True, desc: bool = False) -> Self:
        descending = desc or not asc
        self._order_by.append(
            OrderByClause(
                expr=expr,
                direction=OrderDirection.DESC if descending else OrderDirection.ASC,
            )
        )
        return self

    def limit(self, n: int) -> Self:
        self._limit_value = n
        return self

    def offset(self, n: int) -> Self:
        self._offset_value = n
        return self

    def union(self, other: SelectQuery[Any]) -> SetOperationQuery[T]:
        return SetOperationQuery(self, "UNION", other)

    def union_all(self, other: SelectQuery[Any]) -> SetOperationQuery[T]:
        return SetOperationQuery(self, "UNION ALL", other)

    def intersect(self, other: SelectQuery[Any]) -> SetOperationQuery[T]:
        return SetOperationQuery(self, "INTERSECT", other)

    def except_(self, other: SelectQuery[Any]) -> SetOperationQuery[T]:
        return SetOperationQuery(self, "EXCEPT", other)

    def build_into(self, params: Params) -> str:
        if isinstance(self._left, SelectQuery):
            left_sql = self._left.build_into(params)
        else:
            left_sql = self._left.build_into(params)
        right_sql = self._right.build_into(params)
        sql = f"{left_sql} {self._op} {right_sql}"
        if self._order_by:
            order_parts: list[str] = []
            for o in self._order_by:
                order_parts.append(f"{_render_col(o.expr, params)} {o.direction}")
            sql += f" ORDER BY {', '.join(order_parts)}"
        if self._limit_value is not None:
            sql += f" LIMIT {self._limit_value}"
        if self._offset_value is not None:
            sql += f" OFFSET {self._offset_value}"
        return sql

    def build(self) -> tuple[str, dict[str, Any]]:
        params = Params()
        sql = self.build_into(params)
        return sql, params.values


# =============================================================================
# INSERT
# =============================================================================


class InsertQuery[T: Table]:
    """``INSERT INTO ... VALUES (...)`` / ``INSERT INTO ... SELECT ...``.

    ClickHouse INSERT does not support RETURNING; ``execute()`` returns
    ``None``.  For bulk inserts, ``execute()`` accepts an iterable of
    rows internally.
    """

    def __init__(
        self,
        engine: ClickHouseEngine | None,
        table: type[T],
    ) -> None:
        self._engine = engine
        self._table = table
        self._values: dict[str, Any] | None = None
        self._values_list: list[dict[str, Any]] | None = None
        self._insert_columns: list[str] | None = None
        self._from_select: SelectQuery[Any] | None = None
        self._settings: dict[str, Any] = {}

    def values(self, **kwargs: Any) -> Self:
        """Insert a single row."""
        self._values = kwargs
        return self

    def values_list(self, rows: Sequence[dict[str, Any]]) -> Self:
        """Insert multiple rows."""
        self._values_list = list(rows)
        return self

    def columns(self, *cols: Any) -> Self:
        resolved: list[str] = []
        for c in cols:
            if isinstance(c, Column) and c._field_name:
                resolved.append(c._field_name)
            else:
                resolved.append(str(c))
        self._insert_columns = resolved
        return self

    def from_select(self, query: SelectQuery[Any]) -> Self:
        self._from_select = query
        return self

    def settings(self, **kwargs: Any) -> Self:
        self._settings.update(kwargs)
        return self

    def build(self) -> tuple[str, dict[str, Any]]:
        params = Params()
        sql = self._build(params)
        return sql, params.values

    def _build(self, params: Params) -> str:
        table_name = self._table.get_full_name()
        settings_suffix = ""
        if self._settings:
            s_parts = [
                f"{k} = {_render_setting_value(v)}" for k, v in self._settings.items()
            ]
            settings_suffix = f" SETTINGS {', '.join(s_parts)}"

        if self._from_select is not None:
            cols = self._insert_columns or list(self._table.__columns__.keys())
            col_sql = ", ".join(f"`{c}`" for c in cols)
            sub_sql = self._from_select.build_into(params)
            return f"INSERT INTO {table_name} ({col_sql}){settings_suffix} {sub_sql}"

        if self._values_list is not None:
            if not self._values_list:
                raise ValueError("values_list() requires at least one row.")
            columns = list(self._values_list[0].keys())
            col_sql = ", ".join(f"`{c}`" for c in columns)
            rows_sql: list[str] = []
            for row in self._values_list:
                placeholders = [params.add(row[c]) for c in columns]
                rows_sql.append(f"({', '.join(placeholders)})")
            return (
                f"INSERT INTO {table_name} ({col_sql}){settings_suffix} "
                f"VALUES {', '.join(rows_sql)}"
            )

        if self._values is None:
            raise ValueError(
                "InsertQuery requires values(), values_list(), or from_select()."
            )

        columns = list(self._values.keys())
        col_sql = ", ".join(f"`{c}`" for c in columns)
        placeholders = [params.add(self._values[c]) for c in columns]
        return (
            f"INSERT INTO {table_name} ({col_sql}){settings_suffix} "
            f"VALUES ({', '.join(placeholders)})"
        )

    async def execute(self) -> None:
        if self._engine is None:
            raise ChormNotConnectedError(
                "InsertQuery has no engine bound — call db.insert(...)."
            )
        sql, values = self.build()
        await self._engine.command(sql, parameters=values)


# =============================================================================
# ALTER (DDL + mutations)
# =============================================================================


class _AlterAction:
    """Base for a single ALTER action."""

    def to_sql(self, params: Params) -> str:  # pragma: no cover
        raise NotImplementedError


@dataclasses.dataclass
class _AddColumn(_AlterAction):
    name: str
    type_sql: str
    if_not_exists: bool = False
    after: str | None = None
    first: bool = False
    default: Any = None
    codec_sql: str | None = None
    ttl: str | None = None
    comment: str | None = None

    def to_sql(self, params: Params) -> str:
        sql = "ADD COLUMN "
        if self.if_not_exists:
            sql += "IF NOT EXISTS "
        sql += f"`{self.name}` {self.type_sql}"
        if self.default is not None:
            sql += f" DEFAULT {self.default}"
        if self.codec_sql:
            sql += f" CODEC({self.codec_sql})"
        if self.ttl:
            sql += f" TTL {self.ttl}"
        if self.comment:
            escaped = self.comment.replace("'", "\\'")
            sql += f" COMMENT '{escaped}'"
        if self.first:
            sql += " FIRST"
        elif self.after:
            sql += f" AFTER `{self.after}`"
        return sql


@dataclasses.dataclass
class _DropColumn(_AlterAction):
    name: str
    if_exists: bool = False

    def to_sql(self, params: Params) -> str:
        if self.if_exists:
            return f"DROP COLUMN IF EXISTS `{self.name}`"
        return f"DROP COLUMN `{self.name}`"


@dataclasses.dataclass
class _ModifyColumn(_AlterAction):
    name: str
    type_sql: str
    default: Any = None
    codec_sql: str | None = None
    ttl: str | None = None
    comment: str | None = None

    def to_sql(self, params: Params) -> str:
        sql = f"MODIFY COLUMN `{self.name}` {self.type_sql}"
        if self.default is not None:
            sql += f" DEFAULT {self.default}"
        if self.codec_sql:
            sql += f" CODEC({self.codec_sql})"
        if self.ttl:
            sql += f" TTL {self.ttl}"
        if self.comment:
            escaped = self.comment.replace("'", "\\'")
            sql += f" COMMENT '{escaped}'"
        return sql


@dataclasses.dataclass
class _RenameColumn(_AlterAction):
    old: str
    new: str

    def to_sql(self, params: Params) -> str:
        return f"RENAME COLUMN `{self.old}` TO `{self.new}`"


@dataclasses.dataclass
class _CommentColumn(_AlterAction):
    name: str
    comment: str

    def to_sql(self, params: Params) -> str:
        escaped = self.comment.replace("'", "\\'")
        return f"COMMENT COLUMN `{self.name}` '{escaped}'"


@dataclasses.dataclass
class _ClearColumn(_AlterAction):
    name: str
    in_partition: str | None = None

    def to_sql(self, params: Params) -> str:
        sql = f"CLEAR COLUMN `{self.name}`"
        if self.in_partition:
            sql += f" IN PARTITION {self.in_partition}"
        return sql


@dataclasses.dataclass
class _AddIndex(_AlterAction):
    index_sql: str

    def to_sql(self, params: Params) -> str:
        return f"ADD {self.index_sql}"


@dataclasses.dataclass
class _DropIndex(_AlterAction):
    name: str
    if_exists: bool = False

    def to_sql(self, params: Params) -> str:
        if self.if_exists:
            return f"DROP INDEX IF EXISTS `{self.name}`"
        return f"DROP INDEX `{self.name}`"


@dataclasses.dataclass
class _MaterializeIndex(_AlterAction):
    name: str
    in_partition: str | None = None

    def to_sql(self, params: Params) -> str:
        sql = f"MATERIALIZE INDEX `{self.name}`"
        if self.in_partition:
            sql += f" IN PARTITION {self.in_partition}"
        return sql


@dataclasses.dataclass
class _MaterializeColumn(_AlterAction):
    name: str
    in_partition: str | None = None

    def to_sql(self, params: Params) -> str:
        sql = f"MATERIALIZE COLUMN `{self.name}`"
        if self.in_partition:
            sql += f" IN PARTITION {self.in_partition}"
        return sql


@dataclasses.dataclass
class _ModifyTTL(_AlterAction):
    ttl: str

    def to_sql(self, params: Params) -> str:
        return f"MODIFY TTL {self.ttl}"


@dataclasses.dataclass
class _MaterializeTTL(_AlterAction):
    in_partition: str | None = None

    def to_sql(self, params: Params) -> str:
        sql = "MATERIALIZE TTL"
        if self.in_partition:
            sql += f" IN PARTITION {self.in_partition}"
        return sql


@dataclasses.dataclass
class _RemoveTTL(_AlterAction):
    def to_sql(self, params: Params) -> str:
        return "REMOVE TTL"


@dataclasses.dataclass
class _ModifySetting(_AlterAction):
    settings: dict[str, Any]

    def to_sql(self, params: Params) -> str:
        parts = [f"{k} = {_render_setting_value(v)}" for k, v in self.settings.items()]
        return f"MODIFY SETTING {', '.join(parts)}"


@dataclasses.dataclass
class _ResetSetting(_AlterAction):
    names: tuple[str, ...]

    def to_sql(self, params: Params) -> str:
        return f"RESET SETTING {', '.join(self.names)}"


@dataclasses.dataclass
class _AttachPartition(_AlterAction):
    partition: str

    def to_sql(self, params: Params) -> str:
        return f"ATTACH PARTITION {self.partition}"


@dataclasses.dataclass
class _DetachPartition(_AlterAction):
    partition: str

    def to_sql(self, params: Params) -> str:
        return f"DETACH PARTITION {self.partition}"


@dataclasses.dataclass
class _DropPartition(_AlterAction):
    partition: str

    def to_sql(self, params: Params) -> str:
        return f"DROP PARTITION {self.partition}"


@dataclasses.dataclass
class _FreezePartition(_AlterAction):
    partition: str | None
    name: str | None = None

    def to_sql(self, params: Params) -> str:
        sql = "FREEZE"
        if self.partition:
            sql += f" PARTITION {self.partition}"
        if self.name:
            sql += f" WITH NAME '{self.name}'"
        return sql


@dataclasses.dataclass
class _MovePartition(_AlterAction):
    partition: str
    to: str  # e.g. "DISK 'cold'", "VOLUME 'main'", or "TABLE other"

    def to_sql(self, params: Params) -> str:
        return f"MOVE PARTITION {self.partition} TO {self.to}"


class AlterQuery:
    """Builder for ``ALTER TABLE`` statements.

    ClickHouse allows many ALTER actions, including online schema
    changes and partition manipulation.  Mutations (``UPDATE`` /
    ``DELETE``) are surfaced separately via
    :meth:`UpdateMutation` / :meth:`DeleteMutation`.
    """

    def __init__(
        self,
        engine: ClickHouseEngine | None,
        table: type[Table] | str,
        *,
        on_cluster: str | None = None,
    ) -> None:
        self._engine = engine
        self._table = table
        self._on_cluster = on_cluster
        self._actions: list[_AlterAction] = []
        self._settings: dict[str, Any] = {}

    def _resolve_table_name(self) -> str:
        if isinstance(self._table, type) and issubclass(self._table, Table):
            return self._table.get_full_name()
        return self._table

    # -- Column actions ------------------------------------------------------

    def add_column(
        self,
        name: str,
        type_sql: str,
        *,
        if_not_exists: bool = False,
        after: str | None = None,
        first: bool = False,
        default: Any = None,
        codec: Any = None,
        ttl: str | None = None,
        comment: str | None = None,
    ) -> Self:
        codec_sql = None
        if codec is not None:
            from derp.chorm.column.base import Codec

            if isinstance(codec, Codec):
                codec_sql = codec.to_sql()
            elif isinstance(codec, tuple | list):
                codec_sql = ", ".join(c.to_sql() for c in codec)
            else:
                codec_sql = str(codec)
        self._actions.append(
            _AddColumn(
                name=name,
                type_sql=type_sql,
                if_not_exists=if_not_exists,
                after=after,
                first=first,
                default=default,
                codec_sql=codec_sql,
                ttl=ttl,
                comment=comment,
            )
        )
        return self

    def drop_column(self, name: str, *, if_exists: bool = False) -> Self:
        self._actions.append(_DropColumn(name=name, if_exists=if_exists))
        return self

    def modify_column(
        self,
        name: str,
        type_sql: str,
        *,
        default: Any = None,
        codec: Any = None,
        ttl: str | None = None,
        comment: str | None = None,
    ) -> Self:
        codec_sql = None
        if codec is not None:
            from derp.chorm.column.base import Codec

            if isinstance(codec, Codec):
                codec_sql = codec.to_sql()
            elif isinstance(codec, tuple | list):
                codec_sql = ", ".join(c.to_sql() for c in codec)
        self._actions.append(
            _ModifyColumn(
                name=name,
                type_sql=type_sql,
                default=default,
                codec_sql=codec_sql,
                ttl=ttl,
                comment=comment,
            )
        )
        return self

    def rename_column(self, old: str, new: str) -> Self:
        self._actions.append(_RenameColumn(old=old, new=new))
        return self

    def comment_column(self, name: str, comment: str) -> Self:
        self._actions.append(_CommentColumn(name=name, comment=comment))
        return self

    def clear_column(self, name: str, *, in_partition: str | None = None) -> Self:
        self._actions.append(_ClearColumn(name=name, in_partition=in_partition))
        return self

    def materialize_column(self, name: str, *, in_partition: str | None = None) -> Self:
        self._actions.append(_MaterializeColumn(name=name, in_partition=in_partition))
        return self

    # -- Index actions -------------------------------------------------------

    def add_index(self, index_ddl: str) -> Self:
        self._actions.append(_AddIndex(index_sql=index_ddl))
        return self

    def drop_index(self, name: str, *, if_exists: bool = False) -> Self:
        self._actions.append(_DropIndex(name=name, if_exists=if_exists))
        return self

    def materialize_index(self, name: str, *, in_partition: str | None = None) -> Self:
        self._actions.append(_MaterializeIndex(name=name, in_partition=in_partition))
        return self

    # -- TTL / settings -------------------------------------------------------

    def modify_ttl(self, ttl: str) -> Self:
        self._actions.append(_ModifyTTL(ttl=ttl))
        return self

    def materialize_ttl(self, *, in_partition: str | None = None) -> Self:
        self._actions.append(_MaterializeTTL(in_partition=in_partition))
        return self

    def remove_ttl(self) -> Self:
        self._actions.append(_RemoveTTL())
        return self

    def modify_setting(self, **kwargs: Any) -> Self:
        self._actions.append(_ModifySetting(settings=kwargs))
        return self

    def reset_setting(self, *names: str) -> Self:
        self._actions.append(_ResetSetting(names=names))
        return self

    # -- Partition actions ---------------------------------------------------

    def attach_partition(self, partition: str) -> Self:
        self._actions.append(_AttachPartition(partition=partition))
        return self

    def detach_partition(self, partition: str) -> Self:
        self._actions.append(_DetachPartition(partition=partition))
        return self

    def drop_partition(self, partition: str) -> Self:
        self._actions.append(_DropPartition(partition=partition))
        return self

    def freeze_partition(
        self, partition: str | None = None, *, name: str | None = None
    ) -> Self:
        self._actions.append(_FreezePartition(partition=partition, name=name))
        return self

    def move_partition(self, partition: str, *, to: str) -> Self:
        self._actions.append(_MovePartition(partition=partition, to=to))
        return self

    # -- Settings / build ----------------------------------------------------

    def settings(self, **kwargs: Any) -> Self:
        self._settings.update(kwargs)
        return self

    def build(self) -> tuple[str, dict[str, Any]]:
        params = Params()
        if not self._actions:
            raise ValueError("ALTER requires at least one action.")
        head = f"ALTER TABLE {self._resolve_table_name()}"
        if self._on_cluster:
            head += f" ON CLUSTER {self._on_cluster}"
        action_sql = ", ".join(a.to_sql(params) for a in self._actions)
        sql = f"{head} {action_sql}"
        if self._settings:
            s_parts = [
                f"{k} = {_render_setting_value(v)}" for k, v in self._settings.items()
            ]
            sql += f" SETTINGS {', '.join(s_parts)}"
        return sql, params.values

    async def execute(self) -> None:
        if self._engine is None:
            raise ChormNotConnectedError("AlterQuery has no engine bound.")
        sql, values = self.build()
        await self._engine.command(sql, parameters=values)


# =============================================================================
# Mutations: UPDATE / DELETE
# =============================================================================


class UpdateMutation[T: Table](_WhereShorthand):
    """``ALTER TABLE … UPDATE … WHERE …`` — a mutation, not transactional."""

    def __init__(
        self,
        engine: ClickHouseEngine | None,
        table: type[T],
        *,
        on_cluster: str | None = None,
    ) -> None:
        self._engine = engine
        self._table = table
        self._on_cluster = on_cluster
        self._sets: dict[str, Any] = {}
        self._where_clause: Expression | None = None
        self._in_partition: str | None = None
        self._settings: dict[str, Any] = {}

    def set(self, **kwargs: Any) -> Self:
        self._sets.update(kwargs)
        return self

    def where(self, cond: Expression) -> Self:
        if self._where_clause is not None:
            self._where_clause = self._where_clause & cond
        else:
            self._where_clause = cond
        return self

    def in_partition(self, partition: str) -> Self:
        self._in_partition = partition
        return self

    def settings(self, **kwargs: Any) -> Self:
        self._settings.update(kwargs)
        return self

    def build(self) -> tuple[str, dict[str, Any]]:
        if not self._sets:
            raise ValueError("UpdateMutation requires set() values.")
        params = Params()
        table_name = self._table.get_full_name()
        head = f"ALTER TABLE {table_name}"
        if self._on_cluster:
            head += f" ON CLUSTER {self._on_cluster}"

        set_parts: list[str] = []
        for col, val in self._sets.items():
            if isinstance(val, Expression):
                set_parts.append(f"`{col}` = {val.to_sql(params)}")
            elif isinstance(val, RawSQL):
                set_parts.append(f"`{col}` = {val.to_sql(params)}")
            else:
                set_parts.append(f"`{col}` = {params.add(val)}")

        sql = f"{head} UPDATE {', '.join(set_parts)}"
        if self._in_partition:
            sql += f" IN PARTITION {self._in_partition}"
        if self._where_clause is None:
            raise ValueError(
                "UpdateMutation requires a WHERE clause "
                "(use where(literal(True)) to update all rows)."
            )
        sql += f" WHERE {self._where_clause.to_sql(params)}"
        if self._settings:
            s_parts = [
                f"{k} = {_render_setting_value(v)}" for k, v in self._settings.items()
            ]
            sql += f" SETTINGS {', '.join(s_parts)}"
        return sql, params.values

    async def execute(self) -> None:
        if self._engine is None:
            raise ChormNotConnectedError("UpdateMutation has no engine bound.")
        sql, values = self.build()
        await self._engine.command(sql, parameters=values)


class DeleteMutation[T: Table](_WhereShorthand):
    """``ALTER TABLE ... DELETE WHERE ...`` (or ``DELETE FROM``)."""

    def __init__(
        self,
        engine: ClickHouseEngine | None,
        table: type[T],
        *,
        lightweight: bool = False,
        on_cluster: str | None = None,
    ) -> None:
        self._engine = engine
        self._table = table
        self._lightweight = lightweight
        self._on_cluster = on_cluster
        self._where_clause: Expression | None = None
        self._in_partition: str | None = None
        self._settings: dict[str, Any] = {}

    def where(self, cond: Expression) -> Self:
        if self._where_clause is not None:
            self._where_clause = self._where_clause & cond
        else:
            self._where_clause = cond
        return self

    def in_partition(self, partition: str) -> Self:
        self._in_partition = partition
        return self

    def settings(self, **kwargs: Any) -> Self:
        self._settings.update(kwargs)
        return self

    def build(self) -> tuple[str, dict[str, Any]]:
        params = Params()
        if self._where_clause is None:
            raise ValueError("DeleteMutation requires a WHERE clause.")
        table_name = self._table.get_full_name()
        if self._lightweight:
            sql = f"DELETE FROM {table_name}"
            if self._on_cluster:
                sql += f" ON CLUSTER {self._on_cluster}"
            sql += f" WHERE {self._where_clause.to_sql(params)}"
        else:
            head = f"ALTER TABLE {table_name}"
            if self._on_cluster:
                head += f" ON CLUSTER {self._on_cluster}"
            sql = f"{head} DELETE"
            if self._in_partition:
                sql += f" IN PARTITION {self._in_partition}"
            sql += f" WHERE {self._where_clause.to_sql(params)}"
        if self._settings:
            s_parts = [
                f"{k} = {_render_setting_value(v)}" for k, v in self._settings.items()
            ]
            sql += f" SETTINGS {', '.join(s_parts)}"
        return sql, params.values

    async def execute(self) -> None:
        if self._engine is None:
            raise ChormNotConnectedError("DeleteMutation has no engine bound.")
        sql, values = self.build()
        await self._engine.command(sql, parameters=values)
