"""Query builder for select, insert, update, delete operations."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Self, overload

import asyncpg

from derp.kv.base import KVClient
from derp.orm.fields import JSON, JSONB, FieldInfo
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

    column: FieldInfo[Any] | str
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
    """Shorthand filter methods that accept string column names or FieldInfo."""

    def where(self, cond: Expression) -> Self:
        """Add WHERE clause. Implemented by subclasses."""
        raise NotImplementedError

    def _resolve_column(
        self, column: FieldInfo[Any] | str
    ) -> FieldInfo[Any] | ColumnRef:
        """Resolve a column reference from a string or FieldInfo."""
        if isinstance(column, FieldInfo):
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

    def not_(self, column: FieldInfo[Any] | str) -> Self:
        """WHERE column == FALSE."""
        return self.where(~self._resolve_column(column))

    def eq(self, column: FieldInfo[Any] | str, value: Any) -> Self:
        """WHERE column = value."""
        return self.where(self._resolve_column(column) == value)

    def neq(self, column: FieldInfo[Any] | str, value: Any) -> Self:
        """WHERE column <> value."""
        return self.where(self._resolve_column(column) != value)

    def gt(self, column: FieldInfo[Any] | str, value: Any) -> Self:
        """WHERE column > value."""
        return self.where(self._resolve_column(column) > value)

    def gte(self, column: FieldInfo[Any] | str, value: Any) -> Self:
        """WHERE column >= value."""
        return self.where(self._resolve_column(column) >= value)

    def lt(self, column: FieldInfo[Any] | str, value: Any) -> Self:
        """WHERE column < value."""
        return self.where(self._resolve_column(column) < value)

    def lte(self, column: FieldInfo[Any] | str, value: Any) -> Self:
        """WHERE column <= value."""
        return self.where(self._resolve_column(column) <= value)

    def is_null(self, column: FieldInfo[Any] | str) -> Self:
        """WHERE column IS NULL."""
        return self.where(self._resolve_column(column).is_null())

    def is_not_null(self, column: FieldInfo[Any] | str) -> Self:
        """WHERE column IS NOT NULL."""
        return self.where(self._resolve_column(column).is_not_null())

    def in_(self, column: FieldInfo[Any] | str, values: Sequence[Any]) -> Self:
        """WHERE column IN (values)."""
        return self.where(self._resolve_column(column).in_(values))

    def not_in(self, column: FieldInfo[Any] | str, values: Sequence[Any]) -> Self:
        """WHERE column NOT IN (values)."""
        return self.where(self._resolve_column(column).not_in(values))

    def like(self, column: FieldInfo[Any] | str, pattern: str) -> Self:
        """WHERE column LIKE pattern."""
        return self.where(self._resolve_column(column).like(pattern))

    def ilike(self, column: FieldInfo[Any] | str, pattern: str) -> Self:
        """WHERE column ILIKE pattern."""
        return self.where(self._resolve_column(column).ilike(pattern))

    def between(self, column: FieldInfo[Any] | str, low: Any, high: Any) -> Self:
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
        columns: tuple[type[Table] | FieldInfo[Any] | Expression, ...],
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
        self._group_by: list[FieldInfo[Any] | str] = []
        self._having_clause: Expression | None = None
        self._distinct: bool = False
        self._distinct_on: list[FieldInfo[Any]] = []
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

    def order_by(self, column: FieldInfo[Any] | str, *, asc: bool = True) -> Self:
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

    def group_by(self, *columns: FieldInfo[Any] | str) -> Self:
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

    def distinct_on(self, *columns: FieldInfo[Any]) -> Self:
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
            elif isinstance(col, FieldInfo):
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
                if isinstance(col, FieldInfo) and col._table_name and col._field_name:
                    group_parts.append(f"{col._table_name}.{col._field_name}")
                elif isinstance(col, FieldInfo) and col._field_name:
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
                    isinstance(ob.column, FieldInfo)
                    and ob.column._table_name
                    and ob.column._field_name
                ):
                    order_parts.append(
                        f"{ob.column._table_name}.{ob.column._field_name} "
                        f"{ob.direction}"
                    )
                elif isinstance(ob.column, FieldInfo) and ob.column._field_name:
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

        return self._hydrate(self._rows_to_dicts(rows))

    def _rows_to_dicts(self, rows: list[asyncpg.Record]) -> list[dict[str, Any]]:
        """Convert asyncpg Records to plain dicts with JSON deserialization."""
        if self._from_table is not None and not isinstance(
            self._from_table, str | SubqueryExpr
        ):
            return [_deserialize_row(self._from_table, dict(row)) for row in rows]
        if (
            len(self._columns) == 1
            and isinstance(self._columns[0], type)
            and issubclass(self._columns[0], Table)
        ):
            return [_deserialize_row(self._columns[0], dict(row)) for row in rows]
        return [dict(row) for row in rows]

    def _hydrate(self, rows_data: list[dict[str, Any]]) -> list[T]:
        """Hydrate dicts into model instances or return as-is."""
        if (
            len(self._columns) == 1
            and isinstance(self._columns[0], type)
            and issubclass(self._columns[0], Table)
        ):
            model_class = self._columns[0]
            return [  # type: ignore[return-value]
                model_class.model_validate(row) for row in rows_data
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

    def order_by(self, column: FieldInfo[Any] | str, *, asc: bool = True) -> Self:
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
                    isinstance(ob.column, FieldInfo)
                    and ob.column._table_name
                    and ob.column._field_name
                ):
                    order_parts.append(
                        f"{ob.column._table_name}.{ob.column._field_name} "
                        f"{ob.direction}"
                    )
                elif isinstance(ob.column, FieldInfo) and ob.column._field_name:
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
        self._returning: tuple[type[Table] | FieldInfo[Any], ...] | None = None
        self._router: ReplicaRouter | None = router

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
                    elif isinstance(col, FieldInfo) and col._field_name:
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
            for col, val in self._values.items():
                params.append(_serialize_value(self._table, col, val))
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
                elif isinstance(col, FieldInfo) and col._field_name:
                    return_parts.append(col._field_name)
            sql += f" RETURNING {', '.join(return_parts)}"

        return sql, params


class InsertQuery[T: Table](_InsertQueryBase[T]):
    """INSERT query without RETURNING - execute() returns None."""

    def values(self, **kwargs: Any) -> InsertQuery[T]:
        """Set values to insert."""
        self._values = kwargs
        return self

    def values_list(self, rows: list[dict[str, Any]]) -> InsertQuery[T]:
        """Set multiple rows to insert."""
        self._values_list = rows
        return self

    def columns(self, *cols: FieldInfo[Any] | str) -> InsertQuery[T]:
        """Set column names for INSERT ... SELECT."""
        resolved: list[str] = []
        for c in cols:
            if isinstance(c, FieldInfo) and c._field_name:
                resolved.append(c._field_name)
            else:
                resolved.append(str(c))
        self._insert_columns = resolved
        return self

    def from_select(self, query: SelectQuery[Any]) -> InsertQuery[T]:
        """Set the SELECT query for INSERT ... SELECT."""
        self._from_select = query
        return self

    def _resolve_target(
        self,
        target: FieldInfo[Any] | tuple[FieldInfo[Any], ...],
    ) -> tuple[str, ...]:
        """Resolve conflict target to column name strings."""
        if isinstance(target, FieldInfo):
            return (target._field_name or "",)
        return tuple(f._field_name or "" for f in target)

    def ignore_conflicts(
        self,
        *,
        target: FieldInfo[Any] | tuple[FieldInfo[Any], ...],
    ) -> InsertQuery[T]:
        """Add ON CONFLICT DO NOTHING."""
        self._on_conflict = OnConflictClause(
            target=self._resolve_target(target),
            action="nothing",
        )
        return self

    def upsert(
        self,
        *,
        target: FieldInfo[Any] | tuple[FieldInfo[Any], ...],
        **kwargs: Any,
    ) -> InsertQuery[T]:
        """Add ON CONFLICT DO UPDATE SET (upsert).

        Pass the columns to update as keyword arguments::

            .upsert(target=User.c.email, name="Updated")
        """
        self._on_conflict = OnConflictClause(
            target=self._resolve_target(target),
            action="update",
            set_values=kwargs,
        )
        return self

    @overload
    def returning(self, table: type[T], /) -> InsertQueryReturning[T]: ...

    @overload
    def returning(self, *columns: FieldInfo[Any]) -> InsertQueryReturningDict[T]: ...

    def returning(
        self, *columns: type[Table] | FieldInfo[Any]
    ) -> InsertQueryReturning[T] | InsertQueryReturningDict[T]:
        """Add RETURNING clause."""
        if (
            len(columns) == 1
            and isinstance(columns[0], type)
            and issubclass(columns[0], Table)
        ):
            query: InsertQueryReturning[T] = InsertQueryReturning(
                self._pool, self._table, router=self._router
            )
            query._values = self._values
            query._values_list = self._values_list
            query._on_conflict = self._on_conflict
            query._insert_columns = self._insert_columns
            query._from_select = self._from_select
            query._returning = columns
            return query
        else:
            query_dict: InsertQueryReturningDict[T] = InsertQueryReturningDict(
                self._pool, self._table, router=self._router
            )
            query_dict._values = self._values
            query_dict._values_list = self._values_list
            query_dict._on_conflict = self._on_conflict
            query_dict._insert_columns = self._insert_columns
            query_dict._from_select = self._from_select
            query_dict._returning = columns
            return query_dict

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


class InsertQueryReturning[T: Table](_InsertQueryBase[T]):
    """INSERT query with RETURNING table - execute() returns T."""

    def values(self, **kwargs: Any) -> InsertQueryReturning[T]:
        """Set values to insert."""
        self._values = kwargs
        return self

    def build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
        return self._build()

    async def execute(self) -> T:
        """Execute the insert and return model instance."""
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")

        sql, params = self.build()
        async with _acquire(self._pool) as conn:
            row = await conn.fetchrow(sql, *params)
            if row is None:
                raise RuntimeError("INSERT RETURNING returned no rows")
            result = self._table.model_validate(_deserialize_row(self._table, row))
        if self._router is not None:
            self._router.record_write()
        return result


class InsertQueryReturningDict[T: Table](_InsertQueryBase[T]):
    """INSERT query with RETURNING columns - execute() returns dict."""

    def values(self, **kwargs: Any) -> InsertQueryReturningDict[T]:
        """Set values to insert."""
        self._values = kwargs
        return self

    def build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
        return self._build()

    async def execute(self) -> dict[str, Any]:
        """Execute the insert and return dict."""
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")

        sql, params = self.build()
        async with _acquire(self._pool) as conn:
            row = await conn.fetchrow(sql, *params)
            if row is None:
                raise RuntimeError("INSERT RETURNING returned no rows")
            result = _deserialize_row(self._table, row)
        if self._router is not None:
            self._router.record_write()
        return result


# =============================================================================
# UPDATE Query with typed returning()
# =============================================================================


class _UpdateQueryBase[T: Table](_WhereShorthandMixin):
    """Base class for UPDATE queries with shared implementation."""

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
        self._returning: tuple[type[Table] | FieldInfo[Any], ...] | None = None
        self._router: ReplicaRouter | None = router

    def _build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
        table_name = self._table.get_table_name()
        params: list[Any] = []

        set_parts = []
        for col, val in self._set_values.items():
            if isinstance(val, RawSQL):
                set_parts.append(f"{col} = {val.to_sql(params)}")
            else:
                params.append(_serialize_value(self._table, col, val))
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
                elif isinstance(col, FieldInfo) and col._field_name:
                    return_parts.append(col._field_name)
            sql += f" RETURNING {', '.join(return_parts)}"

        return sql, params


class UpdateQuery[T: Table](_UpdateQueryBase[T]):
    """UPDATE query without RETURNING - execute() returns None."""

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

    @overload
    def returning(self, table: type[T], /) -> UpdateQueryReturning[T]: ...

    @overload
    def returning(self, *columns: FieldInfo[Any]) -> UpdateQueryReturningDict[T]: ...

    def returning(
        self, *columns: type[Table] | FieldInfo[Any]
    ) -> UpdateQueryReturning[T] | UpdateQueryReturningDict[T]:
        """Add RETURNING clause."""
        if (
            len(columns) == 1
            and isinstance(columns[0], type)
            and issubclass(columns[0], Table)
        ):
            query: UpdateQueryReturning[T] = UpdateQueryReturning(
                self._pool, self._table, router=self._router
            )
            query._set_values = self._set_values
            query._where_clause = self._where_clause
            query._returning = columns
            return query
        else:
            query_dict: UpdateQueryReturningDict[T] = UpdateQueryReturningDict(
                self._pool, self._table, router=self._router
            )
            query_dict._set_values = self._set_values
            query_dict._where_clause = self._where_clause
            query_dict._returning = columns
            return query_dict

    def build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
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


class UpdateQueryReturning[T: Table](_UpdateQueryBase[T]):
    """UPDATE query with RETURNING table - execute() returns list[T]."""

    def set(self, **kwargs: Any) -> UpdateQueryReturning[T]:
        """Set values to update."""
        self._set_values = kwargs
        return self

    def where(self, cond: Expression) -> UpdateQueryReturning[T]:
        """Add WHERE clause. Multiple calls combine with AND."""
        if self._where_clause is not None:
            self._where_clause = self._where_clause & cond
        else:
            self._where_clause = cond
        return self

    def build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
        return self._build()

    async def execute(self) -> list[T]:
        """Execute the update and return model instances."""
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")

        sql, params = self.build()
        async with _acquire(self._pool) as conn:
            rows = await conn.fetch(sql, *params)
            result = [
                self._table.model_validate(_deserialize_row(self._table, row))
                for row in rows
            ]
        if self._router is not None:
            self._router.record_write()
        return result


class UpdateQueryReturningDict[T: Table](_UpdateQueryBase[T]):
    """UPDATE query with RETURNING columns - execute() returns list[dict]."""

    def set(self, **kwargs: Any) -> UpdateQueryReturningDict[T]:
        """Set values to update."""
        self._set_values = kwargs
        return self

    def where(self, cond: Expression) -> UpdateQueryReturningDict[T]:
        """Add WHERE clause. Multiple calls combine with AND."""
        if self._where_clause is not None:
            self._where_clause = self._where_clause & cond
        else:
            self._where_clause = cond
        return self

    def build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
        return self._build()

    async def execute(self) -> list[dict[str, Any]]:
        """Execute the update and return dicts."""
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")

        sql, params = self.build()
        async with _acquire(self._pool) as conn:
            rows = await conn.fetch(sql, *params)
            result = [_deserialize_row(self._table, row) for row in rows]
        if self._router is not None:
            self._router.record_write()
        return result


# =============================================================================
# DELETE Query with typed returning()
# =============================================================================


class _DeleteQueryBase[T: Table](_WhereShorthandMixin):
    """Base class for DELETE queries with shared implementation."""

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
        self._returning: tuple[type[Table] | FieldInfo[Any], ...] | None = None
        self._router: ReplicaRouter | None = router

    def _build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
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
                elif isinstance(col, FieldInfo) and col._field_name:
                    return_parts.append(col._field_name)
            sql += f" RETURNING {', '.join(return_parts)}"

        return sql, params


class DeleteQuery[T: Table](_DeleteQueryBase[T]):
    """DELETE query without RETURNING - execute() returns None."""

    def where(self, cond: Expression) -> DeleteQuery[T]:
        """Add WHERE clause. Multiple calls combine with AND."""
        if self._where_clause is not None:
            self._where_clause = self._where_clause & cond
        else:
            self._where_clause = cond
        return self

    @overload
    def returning(self, table: type[T], /) -> DeleteQueryReturning[T]: ...

    @overload
    def returning(self, *columns: FieldInfo[Any]) -> DeleteQueryReturningDict[T]: ...

    def returning(
        self, *columns: type[Table] | FieldInfo[Any]
    ) -> DeleteQueryReturning[T] | DeleteQueryReturningDict[T]:
        """Add RETURNING clause."""
        if (
            len(columns) == 1
            and isinstance(columns[0], type)
            and issubclass(columns[0], Table)
        ):
            query: DeleteQueryReturning[T] = DeleteQueryReturning(
                self._pool, self._table, router=self._router
            )
            query._where_clause = self._where_clause
            query._returning = columns
            return query
        else:
            query_dict: DeleteQueryReturningDict[T] = DeleteQueryReturningDict(
                self._pool, self._table, router=self._router
            )
            query_dict._where_clause = self._where_clause
            query_dict._returning = columns
            return query_dict

    def build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
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


class DeleteQueryReturning[T: Table](_DeleteQueryBase[T]):
    """DELETE query with RETURNING table - execute() returns list[T]."""

    def where(self, cond: Expression) -> DeleteQueryReturning[T]:
        """Add WHERE clause. Multiple calls combine with AND."""
        if self._where_clause is not None:
            self._where_clause = self._where_clause & cond
        else:
            self._where_clause = cond
        return self

    def build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
        return self._build()

    async def execute(self) -> list[T]:
        """Execute the delete and return model instances."""
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")

        sql, params = self.build()
        async with _acquire(self._pool) as conn:
            rows = await conn.fetch(sql, *params)
            result = [
                self._table.model_validate(_deserialize_row(self._table, row))
                for row in rows
            ]
        if self._router is not None:
            self._router.record_write()
        return result


class DeleteQueryReturningDict[T: Table](_DeleteQueryBase[T]):
    """DELETE query with RETURNING columns - execute() returns list[dict]."""

    def where(self, cond: Expression) -> DeleteQueryReturningDict[T]:
        """Add WHERE clause. Multiple calls combine with AND."""
        if self._where_clause is not None:
            self._where_clause = self._where_clause & cond
        else:
            self._where_clause = cond
        return self

    def build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
        return self._build()

    async def execute(self) -> list[dict[str, Any]]:
        """Execute the delete and return dicts."""
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")

        sql, params = self.build()
        async with _acquire(self._pool) as conn:
            rows = await conn.fetch(sql, *params)
            result = [_deserialize_row(self._table, row) for row in rows]
        if self._router is not None:
            self._router.record_write()
        return result


# =============================================================================
# Helpers
# =============================================================================


def _serialize_value(table: type[Table], column: str, value: Any) -> Any:
    """Serialize value for database insertion (handles JSONB)."""
    columns = table.get_columns()
    if column in columns:
        field_info = columns[column]
        if isinstance(field_info.field_type, (JSON, JSONB)):
            if isinstance(value, (dict, list)):
                return json.dumps(value)
    return value


def _deserialize_row(table: type[Table], row: dict[str, Any]) -> dict[str, Any]:
    """Deserialize row data from database (handles JSONB)."""
    columns = table.get_columns()
    result = dict(row)
    for col, val in result.items():
        if col in columns:
            field_info = columns[col]
            if isinstance(field_info.field_type, (JSON, JSONB)):
                if isinstance(val, str):
                    result[col] = json.loads(val)
    return result
