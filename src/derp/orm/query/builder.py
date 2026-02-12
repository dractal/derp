"""Query builder for select, insert, update, delete operations."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Self, overload

import asyncpg

from derp.kv.base import KVClient
from derp.orm.fields import JSON, JSONB, FieldInfo
from derp.orm.query.expressions import Expression
from derp.orm.table import Table


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
    condition: Expression


@dataclass
class OrderByClause:
    """Represents an ORDER BY clause."""

    column: FieldInfo[Any] | str
    direction: SortOrder = SortOrder.ASC


class SelectQuery[T]:
    """SELECT query - T is the result element type (Table subclass or dict)."""

    def __init__(
        self,
        pool: asyncpg.Pool | None,
        columns: tuple[type[Table] | FieldInfo[Any], ...],
        *,
        cache_store: KVClient | None = None,
    ):
        self._pool = pool
        self._columns = columns
        self._from_table: type[Table] | None = None
        self._joins: list[JoinClause] = []
        self._where_clause: Expression | None = None
        self._order_by: list[OrderByClause] = []
        self._limit_value: int | None = None
        self._offset_value: int | None = None
        self._group_by: list[FieldInfo[Any] | str] = []
        self._cache_store: KVClient | None = cache_store
        self._cache_ttl: float | None = None

        # Infer from table if first column is a Table class
        if columns and isinstance(columns[0], type) and issubclass(columns[0], Table):
            self._from_table = columns[0]

    def from_(self, table: type[Table]) -> Self:
        """Set the FROM table."""
        self._from_table = table
        return self

    def where(self, condition: Expression) -> Self:
        """Add WHERE clause."""
        self._where_clause = condition
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

    def cross_join(self, table: type[Table], condition: Expression) -> Self:
        """Add CROSS JOIN."""
        self._joins.append(JoinClause(JoinType.CROSS, table, condition))
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

    def cache(self, ttl: float) -> Self:
        """Cache this query's results for ``ttl`` seconds."""
        self._cache_ttl = ttl
        return self

    def build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
        params: list[Any] = []

        # SELECT clause
        select_parts: list[str] = []
        for col in self._columns:
            if isinstance(col, type) and issubclass(col, Table):
                # Select all columns from table
                table_name = col.get_table_name()
                select_parts.append(f"{table_name}.*")
            elif isinstance(col, FieldInfo):
                if col._table_name and col._field_name:
                    select_parts.append(f"{col._table_name}.{col._field_name}")
                elif col._field_name:
                    select_parts.append(col._field_name)
            else:
                # Raw string column name
                select_parts.append(str(col))

        sql = f"SELECT {', '.join(select_parts)}"

        # FROM clause
        if self._from_table:
            sql += f" FROM {self._from_table.get_table_name()}"

        # JOIN clauses
        for join in self._joins:
            join_table = join.table.get_table_name()
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

        return sql, params

    def _cache_key(self, sql: str, params: list[Any]) -> str:
        """Derive a cache key from SQL and parameters."""
        raw = sql + json.dumps(params, default=str)
        digest = hashlib.sha256(raw.encode()).hexdigest()
        return f"derp:query:{digest}"

    async def execute(self) -> list[T]:
        """Execute the query and return results."""
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")

        sql, params = self.build()

        # Check cache
        if self._cache_store is not None and self._cache_ttl is not None:
            cache_key = self._cache_key(sql, params).encode()
            cached = await self._cache_store.get(cache_key)
            if cached is not None:
                rows_data: list[dict[str, Any]] = json.loads(cached)
                return self._hydrate(rows_data)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        rows_data = self._rows_to_dicts(rows)

        # Populate cache
        if self._cache_store is not None and self._cache_ttl is not None:
            await self._cache_store.set(
                cache_key,
                json.dumps(rows_data, default=str).encode(),
                ttl=self._cache_ttl,
            )

        return self._hydrate(rows_data)

    def _rows_to_dicts(self, rows: list[asyncpg.Record]) -> list[dict[str, Any]]:
        """Convert asyncpg Records to plain dicts with JSON deserialization."""
        if self._from_table:
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


# =============================================================================
# INSERT Query with typed returning()
# =============================================================================


class _InsertQueryBase[T: Table]:
    """Base class for INSERT queries with shared implementation."""

    def __init__(self, pool: asyncpg.Pool | None, table: type[T]):
        self._pool = pool
        self._table = table
        self._values: dict[str, Any] = {}
        self._returning: tuple[type[Table] | FieldInfo[Any], ...] | None = None

    def _build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
        table_name = self._table.get_table_name()
        columns = list(self._values.keys())
        params = [
            _serialize_value(self._table, col, val) for col, val in self._values.items()
        ]

        placeholders = [f"${i + 1}" for i in range(len(params))]

        sql = (
            f"INSERT INTO {table_name} ({', '.join(columns)}) "
            f"VALUES ({', '.join(placeholders)})"
        )

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
                self._pool, self._table
            )
            query._values = self._values
            query._returning = columns
            return query
        else:
            query_dict: InsertQueryReturningDict[T] = InsertQueryReturningDict(
                self._pool, self._table
            )
            query_dict._values = self._values
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
        async with self._pool.acquire() as conn:
            await conn.execute(sql, *params)


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
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, *params)
            if row is None:
                raise RuntimeError("INSERT RETURNING returned no rows")
            return self._table.model_validate(_deserialize_row(self._table, row))


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
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, *params)
            if row is None:
                raise RuntimeError("INSERT RETURNING returned no rows")
            return _deserialize_row(self._table, row)


# =============================================================================
# UPDATE Query with typed returning()
# =============================================================================


class _UpdateQueryBase[T: Table]:
    """Base class for UPDATE queries with shared implementation."""

    def __init__(self, pool: asyncpg.Pool | None, table: type[T]):
        self._pool = pool
        self._table = table
        self._set_values: dict[str, Any] = {}
        self._where_clause: Expression | None = None
        self._returning: tuple[type[Table] | FieldInfo[Any], ...] | None = None

    def _build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
        table_name = self._table.get_table_name()
        params: list[Any] = []

        set_parts = []
        for col, val in self._set_values.items():
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

    def where(self, condition: Expression) -> UpdateQuery[T]:
        """Add WHERE clause."""
        self._where_clause = condition
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
                self._pool, self._table
            )
            query._set_values = self._set_values
            query._where_clause = self._where_clause
            query._returning = columns
            return query
        else:
            query_dict: UpdateQueryReturningDict[T] = UpdateQueryReturningDict(
                self._pool, self._table
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
        async with self._pool.acquire() as conn:
            await conn.execute(sql, *params)


class UpdateQueryReturning[T: Table](_UpdateQueryBase[T]):
    """UPDATE query with RETURNING table - execute() returns list[T]."""

    def set(self, **kwargs: Any) -> UpdateQueryReturning[T]:
        """Set values to update."""
        self._set_values = kwargs
        return self

    def where(self, condition: Expression) -> UpdateQueryReturning[T]:
        """Add WHERE clause."""
        self._where_clause = condition
        return self

    def build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
        return self._build()

    async def execute(self) -> list[T]:
        """Execute the update and return model instances."""
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")

        sql, params = self.build()
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
            return [
                self._table.model_validate(_deserialize_row(self._table, row))
                for row in rows
            ]


class UpdateQueryReturningDict[T: Table](_UpdateQueryBase[T]):
    """UPDATE query with RETURNING columns - execute() returns list[dict]."""

    def set(self, **kwargs: Any) -> UpdateQueryReturningDict[T]:
        """Set values to update."""
        self._set_values = kwargs
        return self

    def where(self, condition: Expression) -> UpdateQueryReturningDict[T]:
        """Add WHERE clause."""
        self._where_clause = condition
        return self

    def build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
        return self._build()

    async def execute(self) -> list[dict[str, Any]]:
        """Execute the update and return dicts."""
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")

        sql, params = self.build()
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
            return [_deserialize_row(self._table, row) for row in rows]


# =============================================================================
# DELETE Query with typed returning()
# =============================================================================


class _DeleteQueryBase[T: Table]:
    """Base class for DELETE queries with shared implementation."""

    def __init__(self, pool: asyncpg.Pool | None, table: type[T]):
        self._pool = pool
        self._table = table
        self._where_clause: Expression | None = None
        self._returning: tuple[type[Table] | FieldInfo[Any], ...] | None = None

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

    def where(self, condition: Expression) -> DeleteQuery[T]:
        """Add WHERE clause."""
        self._where_clause = condition
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
                self._pool, self._table
            )
            query._where_clause = self._where_clause
            query._returning = columns
            return query
        else:
            query_dict: DeleteQueryReturningDict[T] = DeleteQueryReturningDict(
                self._pool, self._table
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
        async with self._pool.acquire() as conn:
            await conn.execute(sql, *params)


class DeleteQueryReturning[T: Table](_DeleteQueryBase[T]):
    """DELETE query with RETURNING table - execute() returns list[T]."""

    def where(self, condition: Expression) -> DeleteQueryReturning[T]:
        """Add WHERE clause."""
        self._where_clause = condition
        return self

    def build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
        return self._build()

    async def execute(self) -> list[T]:
        """Execute the delete and return model instances."""
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")

        sql, params = self.build()
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
            return [
                self._table.model_validate(_deserialize_row(self._table, row))
                for row in rows
            ]


class DeleteQueryReturningDict[T: Table](_DeleteQueryBase[T]):
    """DELETE query with RETURNING columns - execute() returns list[dict]."""

    def where(self, condition: Expression) -> DeleteQueryReturningDict[T]:
        """Add WHERE clause."""
        self._where_clause = condition
        return self

    def build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
        return self._build()

    async def execute(self) -> list[dict[str, Any]]:
        """Execute the delete and return dicts."""
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")

        sql, params = self.build()
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
            return [_deserialize_row(self._table, row) for row in rows]


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
