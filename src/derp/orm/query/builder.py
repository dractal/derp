"""Query builder for select, insert, update, delete operations."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Self, overload

if TYPE_CHECKING:
    from derp.orm.query.returning import (
        RMT2,
        RMT3,
        RMT4,
        RMT5,
        RMT6,
        RMT7,
        RMT8,
        RMT9,
        RMT10,
        ROT2,
        ROT3,
        ROT4,
        ROT5,
        ROT6,
        ROT7,
        ROT8,
        ROT9,
        ROT10,
        ROTO2,
        ROTO3,
        ROTO4,
        ROTO5,
        ROTO6,
        ROTO7,
        ROTO8,
        ROTO9,
        ROTO10,
    )

import asyncpg

from derp.kv.base import KVClient
from derp.orm.column.base import Column
from derp.orm.query.expressions import (
    ColumnRef,
    ExistsExpr,
    Expression,
    RawSQL,
    SubqueryExpr,
    _renumber_params,
)
from derp.orm.router import ReplicaRouter
from derp.orm.table import Table


@asynccontextmanager
async def _acquire(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
) -> AsyncIterator[asyncpg.Connection]:
    """Resolve a connection: acquire from pool or use directly."""
    if isinstance(pool_or_conn, asyncpg.Pool):
        async with pool_or_conn.acquire() as conn:
            yield conn
    else:
        yield pool_or_conn


# PostgreSQL supports max 65535 parameters per query.  Keep a safety
# margin so callers never need to think about the limit.
_PG_MAX_PARAMS = 65535


class JoinType(StrEnum):
    """SQL JOIN types."""

    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL OUTER"
    CROSS = "CROSS"


class SortOrder(StrEnum):
    """SQL ORDER BY directions."""

    ASC = "ASC"
    DESC = "DESC"


@dataclass
class JoinClause:
    """Represents a JOIN clause."""

    join_type: JoinType
    table: type[Table]
    condition: Expression | None


@dataclass
class OrderByClause:
    """Represents an ORDER BY clause."""

    column: Column[Any] | str
    direction: SortOrder = SortOrder.ASC


class LockMode(StrEnum):
    """SQL row-level locking modes."""

    UPDATE = "FOR UPDATE"
    NO_KEY_UPDATE = "FOR NO KEY UPDATE"
    SHARE = "FOR SHARE"
    KEY_SHARE = "FOR KEY SHARE"


@dataclass
class LockClause:
    """Represents a row-level lock clause."""

    mode: LockMode
    nowait: bool = False
    skip_locked: bool = False


@dataclass
class OnConflictClause:
    """Represents an ON CONFLICT clause for upsert."""

    target: tuple[str, ...]
    action: str  # "nothing" or "update"
    set_values: dict[str, Any] | None = None


class _WhereShorthandMixin:
    """Shorthand filter methods that accept string column names or Column."""

    def where(self, cond: Expression) -> Self:
        """Add WHERE clause. Implemented by subclasses."""
        raise NotImplementedError

    def _resolve_column(self, column: Column[Any] | str) -> Column[Any] | ColumnRef:
        """Resolve a column reference from a string or Column."""
        if isinstance(column, Column):
            return column
        table_name: str | None = None
        from_table = getattr(self, "_from_table", None)
        table = getattr(self, "_table", None)
        if from_table is not None:
            table_name = (
                from_table
                if isinstance(from_table, str)
                else from_table.get_table_name()
            )
        elif table is not None:
            table_name = table if isinstance(table, str) else table.get_table_name()
        if "." in column:
            t, c = column.split(".", 1)
            return ColumnRef(t, c)
        if table_name:
            return ColumnRef(table_name, column)
        raise ValueError(
            f"Cannot resolve column '{column}': no table context. "
            "Use 'table.column' format or set a FROM table."
        )

    def not_(self, column: Column[Any] | str) -> Self:
        """WHERE column == FALSE."""
        return self.where(~self._resolve_column(column))

    def eq(self, column: Column[Any] | str, value: Any) -> Self:
        """WHERE column = value."""
        return self.where(self._resolve_column(column) == value)

    def neq(self, column: Column[Any] | str, value: Any) -> Self:
        """WHERE column <> value."""
        return self.where(self._resolve_column(column) != value)

    def gt(self, column: Column[Any] | str, value: Any) -> Self:
        """WHERE column > value."""
        return self.where(self._resolve_column(column) > value)

    def gte(self, column: Column[Any] | str, value: Any) -> Self:
        """WHERE column >= value."""
        return self.where(self._resolve_column(column) >= value)

    def lt(self, column: Column[Any] | str, value: Any) -> Self:
        """WHERE column < value."""
        return self.where(self._resolve_column(column) < value)

    def lte(self, column: Column[Any] | str, value: Any) -> Self:
        """WHERE column <= value."""
        return self.where(self._resolve_column(column) <= value)

    def is_null(self, column: Column[Any] | str) -> Self:
        """WHERE column IS NULL."""
        return self.where(self._resolve_column(column).is_null())

    def is_not_null(self, column: Column[Any] | str) -> Self:
        """WHERE column IS NOT NULL."""
        return self.where(self._resolve_column(column).is_not_null())

    def in_(self, column: Column[Any] | str, values: Sequence[Any]) -> Self:
        """WHERE column IN (values)."""
        return self.where(self._resolve_column(column).in_(values))

    def not_in(self, column: Column[Any] | str, values: Sequence[Any]) -> Self:
        """WHERE column NOT IN (values)."""
        return self.where(self._resolve_column(column).not_in(values))

    def like(self, column: Column[Any] | str, pattern: str) -> Self:
        """WHERE column LIKE pattern."""
        return self.where(self._resolve_column(column).like(pattern))

    def ilike(self, column: Column[Any] | str, pattern: str) -> Self:
        """WHERE column ILIKE pattern."""
        return self.where(self._resolve_column(column).ilike(pattern))

    def between(self, column: Column[Any] | str, low: Any, high: Any) -> Self:
        """WHERE column BETWEEN low AND high."""
        return self.where(self._resolve_column(column).between(low, high))


# =============================================================================
# SELECT Query
# =============================================================================


class SelectQuery[T](_WhereShorthandMixin):
    """SELECT query - T is the result element type (Table subclass or dict)."""

    def __init__(
        self,
        pool: asyncpg.Pool | asyncpg.Connection | None,
        columns: tuple[type[Table] | Column[Any] | Expression, ...],
        *,
        cache_store: KVClient | None = None,
        router: ReplicaRouter | None = None,
    ):
        self._pool = pool
        self._columns = columns
        self._from_table: type[Table] | str | SubqueryExpr | None = None
        self._ctes: list[tuple[str, SelectQuery[Any]]] = []
        self._joins: list[JoinClause] = []
        self._where_clause: Expression | None = None
        self._order_by: list[OrderByClause] = []
        self._limit_value: int | None = None
        self._offset_value: int | None = None
        self._group_by: list[Column[Any] | str] = []
        self._having_clause: Expression | None = None
        self._distinct: bool = False
        self._distinct_on: list[Column[Any]] = []
        self._lock: LockClause | None = None
        self._cache_store: KVClient | None = cache_store
        self._cache_ttl: float | None = None
        self._cache_lock_ttl: float | None = None
        self._cache_retry_delay: float | None = None
        self._router: ReplicaRouter | None = router
        self._force_primary: bool = False

        # Infer from table if first column is a Table class
        if columns and isinstance(columns[0], type) and issubclass(columns[0], Table):
            self._from_table = columns[0]

    def from_(self, table: type[Table] | str | SubqueryExpr) -> Self:
        """Set the FROM table. Accepts a Table class, string, or subquery."""
        self._from_table = table
        return self

    def where(self, cond: Expression) -> Self:
        """Add WHERE clause. Multiple calls combine with AND."""
        if self._where_clause is not None:
            self._where_clause = self._where_clause & cond
        else:
            self._where_clause = cond
        return self

    def inner_join(self, table: type[Table], condition: Expression) -> Self:
        """Add INNER JOIN."""
        self._joins.append(JoinClause(JoinType.INNER, table, condition))
        return self

    def left_join(self, table: type[Table], condition: Expression) -> Self:
        """Add LEFT JOIN."""
        self._joins.append(JoinClause(JoinType.LEFT, table, condition))
        return self

    def right_join(self, table: type[Table], condition: Expression) -> Self:
        """Add RIGHT JOIN."""
        self._joins.append(JoinClause(JoinType.RIGHT, table, condition))
        return self

    def full_join(self, table: type[Table], condition: Expression) -> Self:
        """Add FULL OUTER JOIN."""
        self._joins.append(JoinClause(JoinType.FULL, table, condition))
        return self

    def cross_join(self, table: type[Table]) -> Self:
        """Add CROSS JOIN."""
        self._joins.append(JoinClause(JoinType.CROSS, table, None))
        return self

    def order_by(self, column: Column[Any] | str, *, asc: bool = True) -> Self:
        """Add ORDER BY clause."""
        self._order_by.append(
            OrderByClause(column, SortOrder.ASC if asc else SortOrder.DESC)
        )
        return self

    def limit(self, n: int) -> Self:
        """Add LIMIT clause."""
        self._limit_value = n
        return self

    def offset(self, n: int) -> Self:
        """Add OFFSET clause."""
        self._offset_value = n
        return self

    def group_by(self, *columns: Column[Any] | str) -> Self:
        """Add GROUP BY clause."""
        self._group_by.extend(columns)
        return self

    def having(self, cond: Expression) -> Self:
        """Add HAVING clause. Multiple calls combine with AND."""
        if self._having_clause is not None:
            self._having_clause = self._having_clause & cond
        else:
            self._having_clause = cond
        return self

    def cache(
        self,
        ttl: float,
        *,
        lock_ttl: float | None = None,
        retry_delay: float | None = None,
    ) -> Self:
        """Cache this query's results for ``ttl`` seconds."""
        self._cache_ttl = ttl
        self._cache_lock_ttl = lock_ttl
        self._cache_retry_delay = retry_delay
        return self

    def use_primary(self) -> Self:
        """Force this query to run against the primary database."""
        self._force_primary = True
        return self

    def distinct(self) -> Self:
        """Add DISTINCT to SELECT."""
        self._distinct = True
        return self

    def distinct_on(self, *columns: Column[Any]) -> Self:
        """Add DISTINCT ON to SELECT (PostgreSQL-specific)."""
        self._distinct_on.extend(columns)
        return self

    def for_update(self, *, nowait: bool = False, skip_locked: bool = False) -> Self:
        """Add FOR UPDATE row lock."""
        self._lock = LockClause(LockMode.UPDATE, nowait=nowait, skip_locked=skip_locked)
        return self

    def for_share(self, *, nowait: bool = False, skip_locked: bool = False) -> Self:
        """Add FOR SHARE row lock."""
        self._lock = LockClause(LockMode.SHARE, nowait=nowait, skip_locked=skip_locked)
        return self

    def as_(self, alias: str) -> SubqueryExpr:
        """Wrap this query as a subquery expression with an alias."""
        return SubqueryExpr(self, _alias=alias)

    def exists(self) -> ExistsExpr:
        """Wrap this query as an EXISTS expression."""
        return ExistsExpr(SubqueryExpr(self))

    def with_cte(self, name: str, query: SelectQuery[Any]) -> Self:
        """Add a Common Table Expression (WITH clause)."""
        self._ctes.append((name, query))
        return self

    def union(self, other: SelectQuery[Any]) -> SetOperationQuery[T]:
        """Combine with another query using UNION."""
        return SetOperationQuery(self, "UNION", other)

    def union_all(self, other: SelectQuery[Any]) -> SetOperationQuery[T]:
        """Combine with another query using UNION ALL."""
        return SetOperationQuery(self, "UNION ALL", other)

    def intersect(self, other: SelectQuery[Any]) -> SetOperationQuery[T]:
        """Combine with another query using INTERSECT."""
        return SetOperationQuery(self, "INTERSECT", other)

    def except_(self, other: SelectQuery[Any]) -> SetOperationQuery[T]:
        """Combine with another query using EXCEPT."""
        return SetOperationQuery(self, "EXCEPT", other)

    def build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
        params: list[Any] = []

        # CTE (WITH) clause
        cte_prefix = ""
        if self._ctes:
            cte_parts = []
            for cte_name, cte_query in self._ctes:
                cte_sql, cte_params = cte_query.build()
                offset = len(params)
                params.extend(cte_params)
                renumbered = _renumber_params(cte_sql, offset)
                cte_parts.append(f"{cte_name} AS ({renumbered})")
            cte_prefix = f"WITH {', '.join(cte_parts)} "

        # SELECT clause
        select_parts: list[str] = []
        for col in self._columns:
            if isinstance(col, type) and issubclass(col, Table):
                table_name = col.get_table_name()
                select_parts.append(f"{table_name}.*")
            elif isinstance(col, Expression):
                select_parts.append(col.to_sql(params))
            elif isinstance(col, Column):
                if col._table_name and col._field_name:
                    select_parts.append(f"{col._table_name}.{col._field_name}")
                elif col._field_name:
                    select_parts.append(col._field_name)
            else:
                select_parts.append(str(col))

        # DISTINCT / DISTINCT ON
        distinct_prefix = ""
        if self._distinct_on:
            on_parts = []
            for dc in self._distinct_on:
                if dc._table_name and dc._field_name:
                    on_parts.append(f"{dc._table_name}.{dc._field_name}")
                elif dc._field_name:
                    on_parts.append(dc._field_name)
            distinct_prefix = f"DISTINCT ON ({', '.join(on_parts)}) "
        elif self._distinct:
            distinct_prefix = "DISTINCT "

        sql = f"{cte_prefix}SELECT {distinct_prefix}{', '.join(select_parts)}"

        # FROM clause
        if self._from_table is not None:
            if isinstance(self._from_table, SubqueryExpr):
                sql += f" FROM {self._from_table.to_sql(params)}"
            elif isinstance(self._from_table, str):
                sql += f" FROM {self._from_table}"
            else:
                sql += f" FROM {self._from_table.get_table_name()}"

        # JOIN clauses
        for join in self._joins:
            join_table = join.table.get_table_name()
            if join.join_type == JoinType.CROSS or join.condition is None:
                sql += f" {join.join_type} JOIN {join_table}"
            else:
                condition_sql = join.condition.to_sql(params)
                sql += f" {join.join_type} JOIN {join_table} ON {condition_sql}"

        # WHERE clause
        if self._where_clause:
            where_sql = self._where_clause.to_sql(params)
            sql += f" WHERE {where_sql}"

        # GROUP BY clause
        if self._group_by:
            group_parts = []
            for col in self._group_by:
                if isinstance(col, Column) and col._table_name and col._field_name:
                    group_parts.append(f"{col._table_name}.{col._field_name}")
                elif isinstance(col, Column) and col._field_name:
                    group_parts.append(col._field_name)
                else:
                    group_parts.append(str(col))
            sql += f" GROUP BY {', '.join(group_parts)}"

        # HAVING clause
        if self._having_clause is not None:
            having_sql = self._having_clause.to_sql(params)
            sql += f" HAVING {having_sql}"

        # ORDER BY clause
        if self._order_by:
            order_parts = []
            for ob in self._order_by:
                if (
                    isinstance(ob.column, Column)
                    and ob.column._table_name
                    and ob.column._field_name
                ):
                    order_parts.append(
                        f"{ob.column._table_name}.{ob.column._field_name} "
                        f"{ob.direction}"
                    )
                elif isinstance(ob.column, Column) and ob.column._field_name:
                    order_parts.append(f"{ob.column._field_name} {ob.direction}")
                else:
                    order_parts.append(f"{ob.column} {ob.direction}")
            sql += f" ORDER BY {', '.join(order_parts)}"

        # LIMIT/OFFSET
        if self._limit_value is not None:
            sql += f" LIMIT {self._limit_value}"
        if self._offset_value is not None:
            sql += f" OFFSET {self._offset_value}"

        # Row locking
        if self._lock is not None:
            sql += f" {self._lock.mode}"
            if self._lock.nowait:
                sql += " NOWAIT"
            elif self._lock.skip_locked:
                sql += " SKIP LOCKED"

        return sql, params

    def build_count(self) -> tuple[str, list[Any]]:
        """Build a COUNT(*) SQL query and parameters."""
        params: list[Any] = []
        sql = "SELECT COUNT(*)"

        if self._from_table is not None:
            if isinstance(self._from_table, SubqueryExpr):
                sql += f" FROM {self._from_table.to_sql(params)}"
            elif isinstance(self._from_table, str):
                sql += f" FROM {self._from_table}"
            else:
                sql += f" FROM {self._from_table.get_table_name()}"

        for join in self._joins:
            join_table = join.table.get_table_name()
            if join.join_type == JoinType.CROSS or join.condition is None:
                sql += f" {join.join_type} JOIN {join_table}"
            else:
                condition_sql = join.condition.to_sql(params)
                sql += f" {join.join_type} JOIN {join_table} ON {condition_sql}"

        if self._where_clause:
            where_sql = self._where_clause.to_sql(params)
            sql += f" WHERE {where_sql}"

        return sql, params

    def _cache_key(self, sql: str, params: list[Any]) -> str:
        """Derive a cache key from SQL and parameters."""
        raw = sql + json.dumps(params, default=str)
        digest = hashlib.sha256(raw.encode()).hexdigest()
        return f"derp:query:{digest}"

    def _effective_pool(self) -> asyncpg.Pool | asyncpg.Connection:
        """Return the pool to use, considering the replica router."""
        if self._pool is None:
            raise RuntimeError("No database connection. Call db.connect() first.")
        if (
            self._router is not None
            and not self._force_primary
            and isinstance(self._pool, asyncpg.Pool)
        ):
            return self._router.get_read_pool()
        return self._pool

    async def execute(self) -> list[T]:
        """Execute the query and return results."""
        pool = self._effective_pool()
        sql, params = self.build()

        if self._cache_store is not None and self._cache_ttl is not None:
            cache_key = self._cache_key(sql, params).encode()

            async def _compute() -> bytes:
                async with _acquire(pool) as conn:
                    rows = await conn.fetch(sql, *params)
                return json.dumps(self._rows_to_dicts(rows), default=str).encode()

            guard_kwargs: dict[str, Any] = {}
            if self._cache_lock_ttl is not None:
                guard_kwargs["lock_ttl"] = self._cache_lock_ttl
            if self._cache_retry_delay is not None:
                guard_kwargs["retry_delay"] = self._cache_retry_delay
            cached = await self._cache_store.guarded_get(
                cache_key, compute=_compute, ttl=self._cache_ttl, **guard_kwargs
            )
            rows_data: list[dict[str, Any]] = json.loads(cached)
            return self._hydrate(rows_data)

        async with _acquire(pool) as conn:
            rows = await conn.fetch(sql, *params)

        # Fast path: single-Table select with no JSON columns — pass
        # asyncpg Records straight to _from_row(), skipping the
        # intermediate dict(row) conversion.
        model_class = self._single_table_model()
        if model_class is not None and not _json_columns(model_class):
            return [  # type: ignore[return-value]
                model_class._from_row(row) for row in rows
            ]

        return self._hydrate(self._rows_to_dicts(rows))

    def _single_table_model(self) -> type[Table] | None:
        """Return the Table class when selecting a single table."""
        if (
            len(self._columns) == 1
            and isinstance(self._columns[0], type)
            and issubclass(self._columns[0], Table)
        ):
            return self._columns[0]
        return None

    def _rows_to_dicts(self, rows: list[asyncpg.Record]) -> list[dict[str, Any]]:
        """Convert asyncpg Records to plain dicts with JSON deserialization."""
        table = None
        if self._from_table is not None and not isinstance(
            self._from_table, str | SubqueryExpr
        ):
            table = self._from_table
        elif (
            len(self._columns) == 1
            and isinstance(self._columns[0], type)
            and issubclass(self._columns[0], Table)
        ):
            table = self._columns[0]

        if table is not None:
            json_cols = _json_columns(table)
            if json_cols:
                return [_deserialize_row(table, dict(row)) for row in rows]
            return [dict(row) for row in rows]
        return [dict(row) for row in rows]

    def _is_single_column(self) -> bool:
        """True when selecting exactly one Column descriptor."""
        return len(self._columns) == 1 and isinstance(self._columns[0], Column)

    def _is_multi_column(self) -> bool:
        """True when selecting multiple Column descriptors."""
        return len(self._columns) > 1 and all(
            isinstance(c, Column) for c in self._columns
        )

    def _hydrate(self, rows_data: list[dict[str, Any]]) -> list[T]:
        """Hydrate dicts into model instances or return as-is."""
        model_class = self._single_table_model()
        if model_class is not None:
            return [  # type: ignore[return-value]
                model_class._from_row(row) for row in rows_data
            ]
        if self._is_single_column():
            return [next(iter(row.values())) for row in rows_data]
        if self._is_multi_column():
            return [  # type: ignore[return-value]
                tuple(row.values()) for row in rows_data
            ]
        return rows_data  # type: ignore[return-value]

    async def first_or_none(self) -> T | None:
        """Execute and return first result or None."""
        self._limit_value = 1
        results = await self.execute()
        return results[0] if results else None

    async def first(self) -> T:
        """Execute and return first result."""
        result = await self.first_or_none()
        if result is None:
            raise RuntimeError("SELECT query returned no results")
        return result

    async def count(self) -> int:
        """Execute a COUNT(*) query and return the count."""
        pool = self._effective_pool()
        sql, params = self.build_count()

        async with _acquire(pool) as conn:
            row = await conn.fetchrow(sql, *params)

        return row[0] if row else 0


# =============================================================================
# Set Operation Query (UNION, INTERSECT, EXCEPT)
# =============================================================================


class SetOperationQuery[T]:
    """Combined query from UNION, INTERSECT, or EXCEPT."""

    def __init__(
        self,
        left: SelectQuery[T],
        op: str,
        right: SelectQuery[Any],
    ):
        self._left = left
        self._op = op
        self._right = right
        self._order_by: list[OrderByClause] = []
        self._limit_value: int | None = None
        self._offset_value: int | None = None

    def order_by(self, column: Column[Any] | str, *, asc: bool = True) -> Self:
        """Add ORDER BY to the combined result."""
        self._order_by.append(
            OrderByClause(column, SortOrder.ASC if asc else SortOrder.DESC)
        )
        return self

    def limit(self, n: int) -> Self:
        """Add LIMIT to the combined result."""
        self._limit_value = n
        return self

    def offset(self, n: int) -> Self:
        """Add OFFSET to the combined result."""
        self._offset_value = n
        return self

    def build(self) -> tuple[str, list[Any]]:
        """Build the combined SQL query."""
        left_sql, left_params = self._left.build()
        right_sql, right_params = self._right.build()

        params = list(left_params)
        offset = len(params)
        params.extend(right_params)
        renumbered_right = _renumber_params(right_sql, offset)

        sql = f"{left_sql} {self._op} {renumbered_right}"

        if self._order_by:
            order_parts = []
            for ob in self._order_by:
                if (
                    isinstance(ob.column, Column)
                    and ob.column._table_name
                    and ob.column._field_name
                ):
                    order_parts.append(
                        f"{ob.column._table_name}.{ob.column._field_name} "
                        f"{ob.direction}"
                    )
                elif isinstance(ob.column, Column) and ob.column._field_name:
                    order_parts.append(f"{ob.column._field_name} {ob.direction}")
                else:
                    order_parts.append(f"{ob.column} {ob.direction}")
            sql += f" ORDER BY {', '.join(order_parts)}"

        if self._limit_value is not None:
            sql += f" LIMIT {self._limit_value}"
        if self._offset_value is not None:
            sql += f" OFFSET {self._offset_value}"

        return sql, params


# =============================================================================
# INSERT Query with typed returning()
# =============================================================================


class _InsertQueryBase[T: Table]:
    """Base class for INSERT queries with shared implementation."""

    def __init__(
        self,
        pool: asyncpg.Pool | asyncpg.Connection | None,
        table: type[T],
        *,
        router: ReplicaRouter | None = None,
    ):
        self._pool = pool
        self._table = table
        self._values: dict[str, Any] = {}
        self._values_list: list[dict[str, Any]] | None = None
        self._on_conflict: OnConflictClause | None = None
        self._insert_columns: list[str] | None = None
        self._from_select: SelectQuery[Any] | None = None
        self._returning: tuple[type[Table] | Column[Any], ...] | None = None
        self._router: ReplicaRouter | None = router

    def _chunk_values_list(
        self,
    ) -> list[list[dict[str, Any]]] | None:
        """Split ``_values_list`` into chunks that fit within PG's
        parameter limit.  Returns ``None`` when no chunking is needed.
        """
        if self._values_list is None or not self._values_list:
            return None
        num_cols = len(self._values_list[0])
        if num_cols == 0:
            return None
        total_params = num_cols * len(self._values_list)
        if total_params <= _PG_MAX_PARAMS:
            return None
        rows_per_chunk = _PG_MAX_PARAMS // num_cols
        return [
            self._values_list[i : i + rows_per_chunk]
            for i in range(0, len(self._values_list), rows_per_chunk)
        ]

    def _build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
        table_name = self._table.get_table_name()
        params: list[Any] = []

        # INSERT ... SELECT
        if self._from_select is not None:
            cols = ", ".join(self._insert_columns or [])
            sub_sql, sub_params = self._from_select.build()
            params.extend(sub_params)
            sql = f"INSERT INTO {table_name} ({cols}) {sub_sql}"

            if self._returning:
                return_parts = []
                for col in self._returning:
                    if isinstance(col, type) and issubclass(col, Table):
                        return_parts.append("*")
                    elif isinstance(col, Column) and col._field_name:
                        return_parts.append(col._field_name)
                sql += f" RETURNING {', '.join(return_parts)}"

            return sql, params

        if self._values_list is not None:
            # Multi-row insert
            if not self._values_list:
                raise ValueError("values_list() requires at least one row.")
            columns = list(self._values_list[0].keys())
            all_placeholders = []
            for row in self._values_list:
                row_ph = []
                for col in columns:
                    params.append(_serialize_value(self._table, col, row[col]))
                    row_ph.append(f"${len(params)}")
                all_placeholders.append(f"({', '.join(row_ph)})")
            sql = (
                f"INSERT INTO {table_name} ({', '.join(columns)}) "
                f"VALUES {', '.join(all_placeholders)}"
            )
        else:
            # Single-row insert
            columns = list(self._values.keys())
            json_cols = _json_columns(self._table)
            if json_cols:
                for col, val in self._values.items():
                    params.append(_serialize_value(self._table, col, val))
            else:
                params.extend(self._values.values())
            placeholders = [f"${i + 1}" for i in range(len(params))]
            sql = (
                f"INSERT INTO {table_name} ({', '.join(columns)}) "
                f"VALUES ({', '.join(placeholders)})"
            )

        # ON CONFLICT
        if self._on_conflict is not None:
            target_cols = ", ".join(self._on_conflict.target)
            sql += f" ON CONFLICT ({target_cols})"
            if self._on_conflict.action == "nothing":
                sql += " DO NOTHING"
            else:
                set_parts = []
                for col, val in (self._on_conflict.set_values or {}).items():
                    params.append(_serialize_value(self._table, col, val))
                    set_parts.append(f"{col} = ${len(params)}")
                sql += f" DO UPDATE SET {', '.join(set_parts)}"

        if self._returning:
            return_parts = []
            for col in self._returning:
                if isinstance(col, type) and issubclass(col, Table):
                    return_parts.append("*")
                elif isinstance(col, Column) and col._field_name:
                    return_parts.append(col._field_name)
            sql += f" RETURNING {', '.join(return_parts)}"

        return sql, params

    def _resolve_target(
        self,
        target: Column[Any] | tuple[Column[Any], ...],
    ) -> tuple[str, ...]:
        """Resolve conflict target to column name strings."""
        if isinstance(target, Column):
            return (target._field_name or "",)
        return tuple(f._field_name or "" for f in target)


class InsertQuery[T: Table](_InsertQueryBase[T]):
    """INSERT query without RETURNING - execute() returns None."""

    def values(self, **kwargs: Any) -> InsertQuery[T]:
        """Set values to insert."""
        self._values = kwargs
        return self

    def values_list(self, rows: list[dict[str, Any]]) -> InsertBulkQuery[T]:
        """Set multiple rows to insert. Returns a bulk query."""
        query: InsertBulkQuery[T] = InsertBulkQuery(
            self._pool, self._table, router=self._router
        )
        query._values_list = rows
        query._on_conflict = self._on_conflict
        query._insert_columns = self._insert_columns
        query._from_select = self._from_select
        return query

    def columns(self, *cols: Column[Any] | str) -> InsertQuery[T]:
        """Set column names for INSERT ... SELECT."""
        resolved: list[str] = []
        for c in cols:
            if isinstance(c, Column) and c._field_name:
                resolved.append(c._field_name)
            else:
                resolved.append(str(c))
        self._insert_columns = resolved
        return self

    def from_select(self, query: SelectQuery[Any]) -> InsertQuery[T]:
        """Set the SELECT query for INSERT ... SELECT."""
        self._from_select = query
        return self

    def ignore_conflicts(
        self,
        *,
        target: Column[Any] | tuple[Column[Any], ...],
    ) -> InsertQueryIgnoreConflicts[T]:
        """Add ON CONFLICT DO NOTHING.

        Returns a query whose ``returning().execute()`` yields
        ``T | None`` instead of ``T``, since the conflict may suppress
        the insert.
        """
        self._on_conflict = OnConflictClause(
            target=self._resolve_target(target),
            action="nothing",
        )
        query: InsertQueryIgnoreConflicts[T] = InsertQueryIgnoreConflicts(
            self._pool, self._table, router=self._router
        )
        query._values = self._values
        query._values_list = self._values_list
        query._on_conflict = self._on_conflict
        query._insert_columns = self._insert_columns
        query._from_select = self._from_select
        return query

    def upsert(
        self,
        *,
        target: Column[Any] | tuple[Column[Any], ...],
        **kwargs: Any,
    ) -> InsertQuery[T]:
        """Add ON CONFLICT DO UPDATE SET (upsert).

        Pass the columns to update as keyword arguments::

            .upsert(target=User.email, name="Updated")
        """
        self._on_conflict = OnConflictClause(
            target=self._resolve_target(target),
            action="update",
            set_values=kwargs,
        )
        return self

    # fmt: off
    @overload
    def returning(self, table: type[T], /) -> ReturningOne[T]: ...
    @overload
    def returning[V](self, c1: Column[V], /) -> ReturningOneScalar[T, V]: ...
    @overload
    def returning[A, B](self, c1: Column[A], c2: Column[B], /) -> ROT2[T, A, B]: ...
    @overload
    def returning[A, B, C](self, c1: Column[A], c2: Column[B], c3: Column[C], /) -> ROT3[T, A, B, C]: ...
    @overload
    def returning[A, B, C, D](self, c1: Column[A], c2: Column[B], c3: Column[C], c4: Column[D], /) -> ROT4[T, A, B, C, D]: ...
    @overload
    def returning[A, B, C, D, E](self, c1: Column[A], c2: Column[B], c3: Column[C], c4: Column[D], c5: Column[E], /) -> ROT5[T, A, B, C, D, E]: ...
    @overload
    def returning[A, B, C, D, E, F](self, c1: Column[A], c2: Column[B], c3: Column[C], c4: Column[D], c5: Column[E], c6: Column[F], /) -> ROT6[T, A, B, C, D, E, F]: ...
    @overload
    def returning[A, B, C, D, E, F, G](self, c1: Column[A], c2: Column[B], c3: Column[C], c4: Column[D], c5: Column[E], c6: Column[F], c7: Column[G], /) -> ROT7[T, A, B, C, D, E, F, G]: ...
    @overload
    def returning[A, B, C, D, E, F, G, H](self, c1: Column[A], c2: Column[B], c3: Column[C], c4: Column[D], c5: Column[E], c6: Column[F], c7: Column[G], c8: Column[H], /) -> ROT8[T, A, B, C, D, E, F, G, H]: ...
    @overload
    def returning[A, B, C, D, E, F, G, H, I](self, c1: Column[A], c2: Column[B], c3: Column[C], c4: Column[D], c5: Column[E], c6: Column[F], c7: Column[G], c8: Column[H], c9: Column[I], /) -> ROT9[T, A, B, C, D, E, F, G, H, I]: ...
    @overload
    def returning[A, B, C, D, E, F, G, H, I, J](self, c1: Column[A], c2: Column[B], c3: Column[C], c4: Column[D], c5: Column[E], c6: Column[F], c7: Column[G], c8: Column[H], c9: Column[I], c10: Column[J], /) -> ROT10[T, A, B, C, D, E, F, G, H, I, J]: ...
    # fmt: on

    def returning(
        self, *columns: Any
    ) -> ReturningOne[T] | ReturningOneScalar[T, Any] | ReturningOneTuple[T]:
        """Add RETURNING clause."""
        if (
            len(columns) == 1
            and isinstance(columns[0], type)
            and issubclass(columns[0], Table)
        ):
            return ReturningOne(self, columns)
        if len(columns) == 1:
            return ReturningOneScalar(self, columns)
        return ReturningOneTuple(self, columns)

    def build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
        return self._build()

    async def execute(self) -> None:
        """Execute the insert."""
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")

        sql, params = self.build()
        async with _acquire(self._pool) as conn:
            await conn.execute(sql, *params)

        if self._router is not None:
            self._router.record_write()


# =============================================================================
# Shared RETURNING executors
# =============================================================================


class _ReturningBase[T: Table]:
    """Shared state for all RETURNING query executors."""

    __slots__ = ("_parent", "_columns")

    def __init__(
        self,
        parent: Any,
        columns: tuple[type[Table] | Column[Any], ...],
    ) -> None:
        self._parent = parent
        # Set _returning on the parent so _build() generates the clause.
        parent._returning = columns
        self._columns = columns

    def build(self) -> tuple[str, list[Any]]:
        return self._parent._build()

    def _is_table_return(self) -> bool:
        return (
            len(self._columns) == 1
            and isinstance(self._columns[0], type)
            and issubclass(self._columns[0], Table)
        )

    def _row_to_model(self, row: Any) -> Any:
        return self._parent._table._from_row(_deserialize_row(self._parent._table, row))

    def _row_to_scalar(self, row: Any) -> Any:
        deserialized = _deserialize_row(self._parent._table, row)
        col = self._columns[0]
        if isinstance(col, Column) and col._field_name:
            return deserialized[col._field_name]
        raise RuntimeError("Expected a single Column for scalar return")

    def _row_to_tuple(self, row: Any) -> tuple[Any, ...]:
        deserialized = _deserialize_row(self._parent._table, row)
        return tuple(
            deserialized[col._field_name]
            for col in self._columns
            if isinstance(col, Column) and col._field_name
        )

    def _record_write(self) -> None:
        router = self._parent._router
        if router is not None:
            router.record_write()

    def _check_pool(self) -> Any:
        pool = self._parent._pool
        if not pool:
            raise RuntimeError("No database connection. Call db.connect() first.")
        return pool


class ReturningOne[T: Table](_ReturningBase[T]):
    """Single-row RETURNING (INSERT) → ``T``."""

    async def execute(self) -> T:
        pool = self._check_pool()
        sql, params = self.build()
        async with _acquire(pool) as conn:
            row = await conn.fetchrow(sql, *params)
            if row is None:
                raise RuntimeError("INSERT RETURNING returned no rows")
        self._record_write()
        return self._row_to_model(row)


class ReturningOneScalar[T: Table, V](_ReturningBase[T]):
    """Single-row RETURNING one column (INSERT) → scalar ``V``."""

    async def execute(self) -> V:
        pool = self._check_pool()
        sql, params = self.build()
        async with _acquire(pool) as conn:
            row = await conn.fetchrow(sql, *params)
            if row is None:
                raise RuntimeError("INSERT RETURNING returned no rows")
        self._record_write()
        return self._row_to_scalar(row)


class ReturningOneTuple[T: Table](_ReturningBase[T]):
    """Single-row RETURNING 2+ columns (INSERT) → ``tuple[Any, ...]``."""

    async def execute(self) -> tuple[Any, ...]:
        pool = self._check_pool()
        sql, params = self.build()
        async with _acquire(pool) as conn:
            row = await conn.fetchrow(sql, *params)
            if row is None:
                raise RuntimeError("INSERT RETURNING returned no rows")
        self._record_write()
        return self._row_to_tuple(row)


class ReturningOneOptional[T: Table](_ReturningBase[T]):
    """Single-row RETURNING with ON CONFLICT → ``T | None``."""

    async def execute(self) -> T | None:
        pool = self._check_pool()
        sql, params = self.build()
        async with _acquire(pool) as conn:
            row = await conn.fetchrow(sql, *params)
            if row is None:
                return None
        self._record_write()
        return self._row_to_model(row)


class ReturningOneScalarOptional[T: Table, V](_ReturningBase[T]):
    """Single-row RETURNING one column with ON CONFLICT → ``V | None``."""

    async def execute(self) -> V | None:
        pool = self._check_pool()
        sql, params = self.build()
        async with _acquire(pool) as conn:
            row = await conn.fetchrow(sql, *params)
            if row is None:
                return None
        self._record_write()
        return self._row_to_scalar(row)


class ReturningOneTupleOptional[T: Table](_ReturningBase[T]):
    """Single-row RETURNING 2+ columns with ON CONFLICT → ``tuple[Any, ...] | None``."""

    async def execute(self) -> tuple[Any, ...] | None:
        pool = self._check_pool()
        sql, params = self.build()
        async with _acquire(pool) as conn:
            row = await conn.fetchrow(sql, *params)
            if row is None:
                return None
        self._record_write()
        return self._row_to_tuple(row)


class ReturningMany[T: Table](_ReturningBase[T]):
    """Multi-row RETURNING (UPDATE/DELETE) → ``list[T]``."""

    async def execute(self) -> list[T]:
        pool = self._check_pool()
        sql, params = self.build()
        async with _acquire(pool) as conn:
            rows = await conn.fetch(sql, *params)
        self._record_write()
        return [self._row_to_model(row) for row in rows]


class ReturningManyScalar[T: Table, V](_ReturningBase[T]):
    """Multi-row RETURNING one column (UPDATE/DELETE) → ``list[V]``."""

    async def execute(self) -> list[V]:
        pool = self._check_pool()
        sql, params = self.build()
        async with _acquire(pool) as conn:
            rows = await conn.fetch(sql, *params)
        self._record_write()
        return [self._row_to_scalar(row) for row in rows]


class ReturningManyTuple[T: Table](_ReturningBase[T]):
    """Multi-row RETURNING 2+ columns (UPDATE/DELETE) → ``list[tuple[Any, ...]]``."""

    async def execute(self) -> list[tuple[Any, ...]]:
        pool = self._check_pool()
        sql, params = self.build()
        async with _acquire(pool) as conn:
            rows = await conn.fetch(sql, *params)
        self._record_write()
        return [self._row_to_tuple(row) for row in rows]


# =============================================================================
# Shared returning mixin for multi-row queries
# =============================================================================


class _ReturningManyMixin[T: Table]:
    """``returning()`` overloads for multi-row queries.

    Used by ``InsertBulkQuery``, ``UpdateQuery``, and ``DeleteQuery``.
    """

    # fmt: off
    @overload
    def returning(self, table: type[T], /) -> ReturningMany[T]: ...
    @overload
    def returning[V](self, c1: Column[V], /) -> ReturningManyScalar[T, V]: ...
    @overload
    def returning[A, B](self, c1: Column[A], c2: Column[B], /) -> RMT2[T, A, B]: ...
    @overload
    def returning[A, B, C](self, c1: Column[A], c2: Column[B], c3: Column[C], /) -> RMT3[T, A, B, C]: ...
    @overload
    def returning[A, B, C, D](self, c1: Column[A], c2: Column[B], c3: Column[C], c4: Column[D], /) -> RMT4[T, A, B, C, D]: ...
    @overload
    def returning[A, B, C, D, E](self, c1: Column[A], c2: Column[B], c3: Column[C], c4: Column[D], c5: Column[E], /) -> RMT5[T, A, B, C, D, E]: ...
    @overload
    def returning[A, B, C, D, E, F](self, c1: Column[A], c2: Column[B], c3: Column[C], c4: Column[D], c5: Column[E], c6: Column[F], /) -> RMT6[T, A, B, C, D, E, F]: ...
    @overload
    def returning[A, B, C, D, E, F, G](self, c1: Column[A], c2: Column[B], c3: Column[C], c4: Column[D], c5: Column[E], c6: Column[F], c7: Column[G], /) -> RMT7[T, A, B, C, D, E, F, G]: ...
    @overload
    def returning[A, B, C, D, E, F, G, H](self, c1: Column[A], c2: Column[B], c3: Column[C], c4: Column[D], c5: Column[E], c6: Column[F], c7: Column[G], c8: Column[H], /) -> RMT8[T, A, B, C, D, E, F, G, H]: ...
    @overload
    def returning[A, B, C, D, E, F, G, H, I](self, c1: Column[A], c2: Column[B], c3: Column[C], c4: Column[D], c5: Column[E], c6: Column[F], c7: Column[G], c8: Column[H], c9: Column[I], /) -> RMT9[T, A, B, C, D, E, F, G, H, I]: ...
    @overload
    def returning[A, B, C, D, E, F, G, H, I, J](self, c1: Column[A], c2: Column[B], c3: Column[C], c4: Column[D], c5: Column[E], c6: Column[F], c7: Column[G], c8: Column[H], c9: Column[I], c10: Column[J], /) -> RMT10[T, A, B, C, D, E, F, G, H, I, J]: ...
    # fmt: on

    def returning(
        self, *columns: Any
    ) -> ReturningMany[T] | ReturningManyScalar[T, Any] | ReturningManyTuple[T]:
        """Add RETURNING clause."""
        if (
            len(columns) == 1
            and isinstance(columns[0], type)
            and issubclass(columns[0], Table)
        ):
            return ReturningMany(self, columns)
        if len(columns) == 1:
            return ReturningManyScalar(self, columns)
        return ReturningManyTuple(self, columns)


# =============================================================================
# INSERT returning() + ignore_conflicts
# =============================================================================


class InsertQueryIgnoreConflicts[T: Table](_InsertQueryBase[T]):
    """INSERT … ON CONFLICT DO NOTHING query."""

    # fmt: off
    @overload
    def returning(self, table: type[T], /) -> ReturningOneOptional[T]: ...
    @overload
    def returning[V](self, c1: Column[V], /) -> ReturningOneScalarOptional[T, V]: ...
    @overload
    def returning[A, B](self, c1: Column[A], c2: Column[B], /) -> ROTO2[T, A, B]: ...
    @overload
    def returning[A, B, C](self, c1: Column[A], c2: Column[B], c3: Column[C], /) -> ROTO3[T, A, B, C]: ...
    @overload
    def returning[A, B, C, D](self, c1: Column[A], c2: Column[B], c3: Column[C], c4: Column[D], /) -> ROTO4[T, A, B, C, D]: ...
    @overload
    def returning[A, B, C, D, E](self, c1: Column[A], c2: Column[B], c3: Column[C], c4: Column[D], c5: Column[E], /) -> ROTO5[T, A, B, C, D, E]: ...
    @overload
    def returning[A, B, C, D, E, F](self, c1: Column[A], c2: Column[B], c3: Column[C], c4: Column[D], c5: Column[E], c6: Column[F], /) -> ROTO6[T, A, B, C, D, E, F]: ...
    @overload
    def returning[A, B, C, D, E, F, G](self, c1: Column[A], c2: Column[B], c3: Column[C], c4: Column[D], c5: Column[E], c6: Column[F], c7: Column[G], /) -> ROTO7[T, A, B, C, D, E, F, G]: ...
    @overload
    def returning[A, B, C, D, E, F, G, H](self, c1: Column[A], c2: Column[B], c3: Column[C], c4: Column[D], c5: Column[E], c6: Column[F], c7: Column[G], c8: Column[H], /) -> ROTO8[T, A, B, C, D, E, F, G, H]: ...
    @overload
    def returning[A, B, C, D, E, F, G, H, I](self, c1: Column[A], c2: Column[B], c3: Column[C], c4: Column[D], c5: Column[E], c6: Column[F], c7: Column[G], c8: Column[H], c9: Column[I], /) -> ROTO9[T, A, B, C, D, E, F, G, H, I]: ...
    @overload
    def returning[A, B, C, D, E, F, G, H, I, J](self, c1: Column[A], c2: Column[B], c3: Column[C], c4: Column[D], c5: Column[E], c6: Column[F], c7: Column[G], c8: Column[H], c9: Column[I], c10: Column[J], /) -> ROTO10[T, A, B, C, D, E, F, G, H, I, J]: ...
    # fmt: on

    def returning(
        self, *columns: Any
    ) -> (
        ReturningOneOptional[T]
        | ReturningOneScalarOptional[T, Any]
        | ReturningOneTupleOptional[T]
    ):
        """Add RETURNING clause (optional result due to ON CONFLICT)."""
        if (
            len(columns) == 1
            and isinstance(columns[0], type)
            and issubclass(columns[0], Table)
        ):
            return ReturningOneOptional(self, columns)
        if len(columns) == 1:
            return ReturningOneScalarOptional(self, columns)
        return ReturningOneTupleOptional(self, columns)

    def build(self) -> tuple[str, list[Any]]:
        return self._build()

    async def execute(self) -> None:
        """Execute the insert (no RETURNING)."""
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")
        sql, params = self.build()
        async with _acquire(self._pool) as conn:
            await conn.execute(sql, *params)
        if self._router is not None:
            self._router.record_write()


class InsertBulkQuery[T: Table](_InsertQueryBase[T], _ReturningManyMixin[T]):
    """INSERT with multiple rows via ``values_list()``.

    ``returning()`` uses multi-row fetch and returns ``list[T]``,
    ``list[V]``, or ``list[tuple]``.
    """

    def ignore_conflicts(
        self,
        *,
        target: Column[Any] | tuple[Column[Any], ...],
    ) -> InsertBulkQuery[T]:
        """Add ON CONFLICT DO NOTHING."""
        self._on_conflict = OnConflictClause(
            target=self._resolve_target(target),
            action="nothing",
        )
        return self

    def upsert(
        self,
        *,
        target: Column[Any] | tuple[Column[Any], ...],
        **kwargs: Any,
    ) -> InsertBulkQuery[T]:
        """Add ON CONFLICT DO UPDATE SET (upsert)."""
        self._on_conflict = OnConflictClause(
            target=self._resolve_target(target),
            action="update",
            set_values=kwargs,
        )
        return self

    def build(self) -> tuple[str, list[Any]]:
        return self._build()

    async def execute(self) -> None:
        """Execute the bulk insert.

        Rows are automatically split into chunks when the total
        parameter count would exceed PostgreSQL's 65 535 limit.
        Chunks run inside a single transaction so the operation
        is atomic.
        """
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")

        chunks = self._chunk_values_list()
        if chunks is not None:
            saved = self._values_list
            async with _acquire(self._pool) as conn:
                async with conn.transaction():
                    for chunk in chunks:
                        self._values_list = chunk
                        sql, params = self.build()
                        await conn.execute(sql, *params)
            self._values_list = saved
        else:
            sql, params = self.build()
            async with _acquire(self._pool) as conn:
                await conn.execute(sql, *params)

        if self._router is not None:
            self._router.record_write()


# =============================================================================
# UPDATE Query with typed returning()
# =============================================================================


class UpdateQuery[T: Table](_WhereShorthandMixin, _ReturningManyMixin[T]):
    """UPDATE query — execute() returns None, returning() for results."""

    def __init__(
        self,
        pool: asyncpg.Pool | asyncpg.Connection | None,
        table: type[T],
        *,
        router: ReplicaRouter | None = None,
    ):
        self._pool = pool
        self._table = table
        self._set_values: dict[str, Any] = {}
        self._where_clause: Expression | None = None
        self._returning: tuple[type[Table] | Column[Any], ...] | None = None
        self._router: ReplicaRouter | None = router

    def set(self, **kwargs: Any) -> UpdateQuery[T]:
        """Set values to update."""
        self._set_values = kwargs
        return self

    def where(self, cond: Expression) -> UpdateQuery[T]:
        """Add WHERE clause. Multiple calls combine with AND."""
        if self._where_clause is not None:
            self._where_clause = self._where_clause & cond
        else:
            self._where_clause = cond
        return self

    def _build(self) -> tuple[str, list[Any]]:
        table_name = self._table.get_table_name()
        params: list[Any] = []

        set_parts = []
        json_cols = _json_columns(self._table)
        for col, val in self._set_values.items():
            if isinstance(val, RawSQL):
                set_parts.append(f"{col} = {val.to_sql(params)}")
            else:
                if json_cols and col in json_cols:
                    val = _serialize_value(self._table, col, val)
                params.append(val)
                set_parts.append(f"{col} = ${len(params)}")

        sql = f"UPDATE {table_name} SET {', '.join(set_parts)}"

        if self._where_clause:
            where_sql = self._where_clause.to_sql(params)
            sql += f" WHERE {where_sql}"

        if self._returning:
            return_parts = []
            for col in self._returning:
                if isinstance(col, type) and issubclass(col, Table):
                    return_parts.append("*")
                elif isinstance(col, Column) and col._field_name:
                    return_parts.append(col._field_name)
            sql += f" RETURNING {', '.join(return_parts)}"

        return sql, params

    def build(self) -> tuple[str, list[Any]]:
        return self._build()

    async def execute(self) -> None:
        """Execute the update."""
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")

        sql, params = self.build()
        async with _acquire(self._pool) as conn:
            await conn.execute(sql, *params)
        if self._router is not None:
            self._router.record_write()


# =============================================================================
# DELETE Query
# =============================================================================


class DeleteQuery[T: Table](_WhereShorthandMixin, _ReturningManyMixin[T]):
    """DELETE query — execute() returns None, returning() for results."""

    def __init__(
        self,
        pool: asyncpg.Pool | asyncpg.Connection | None,
        table: type[T],
        *,
        router: ReplicaRouter | None = None,
    ):
        self._pool = pool
        self._table = table
        self._where_clause: Expression | None = None
        self._returning: tuple[type[Table] | Column[Any], ...] | None = None
        self._router: ReplicaRouter | None = router

    def where(self, cond: Expression) -> DeleteQuery[T]:
        """Add WHERE clause. Multiple calls combine with AND."""
        if self._where_clause is not None:
            self._where_clause = self._where_clause & cond
        else:
            self._where_clause = cond
        return self

    def _build(self) -> tuple[str, list[Any]]:
        table_name = self._table.get_table_name()
        params: list[Any] = []

        sql = f"DELETE FROM {table_name}"

        if self._where_clause:
            where_sql = self._where_clause.to_sql(params)
            sql += f" WHERE {where_sql}"

        if self._returning:
            return_parts = []
            for col in self._returning:
                if isinstance(col, type) and issubclass(col, Table):
                    return_parts.append("*")
                elif isinstance(col, Column) and col._field_name:
                    return_parts.append(col._field_name)
            sql += f" RETURNING {', '.join(return_parts)}"

        return sql, params

    def build(self) -> tuple[str, list[Any]]:
        return self._build()

    async def execute(self) -> None:
        """Execute the delete."""
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")

        sql, params = self.build()
        async with _acquire(self._pool) as conn:
            await conn.execute(sql, *params)
        if self._router is not None:
            self._router.record_write()


# =============================================================================
# Helpers
# =============================================================================


def _serialize_value(table: type[Table], column: str, value: Any) -> Any:
    """Serialize value for database insertion (handles JSONB)."""
    if column in _json_columns(table):
        if isinstance(value, dict | list):
            return json.dumps(value)
    return value


_JSON_COLUMNS_CACHE: dict[type, frozenset[str]] = {}


def _json_columns(table: type[Table]) -> frozenset[str]:
    """Get the set of JSON/JSONB column names for a table (cached)."""
    if table not in _JSON_COLUMNS_CACHE:
        cols = frozenset(
            name
            for name, col in table.get_columns().items()
            if col.sql_type() in ("JSON", "JSONB")
        )
        _JSON_COLUMNS_CACHE[table] = cols
    return _JSON_COLUMNS_CACHE[table]


def _deserialize_row(table: type[Table], row: dict[str, Any]) -> dict[str, Any]:
    """Deserialize row data from database (handles JSONB)."""
    json_cols = _json_columns(table)
    if not json_cols:
        return row  # fast path: no JSON columns, skip entirely
    result = dict(row)
    for col_name in json_cols:
        val = result.get(col_name)
        if isinstance(val, str):
            result[col_name] = json.loads(val)
    return result
