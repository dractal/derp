"""Query builder for SELECT, INSERT, UPDATE, DELETE operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dribble.fields import FieldInfo
from dribble.query.expressions import Expression
from dribble.table import Table

if TYPE_CHECKING:
    import asyncpg


@dataclass
class JoinClause:
    """Represents a JOIN clause."""

    join_type: str  # INNER, LEFT, RIGHT, FULL
    table: type[Table]
    condition: Expression


@dataclass
class OrderByClause:
    """Represents an ORDER BY clause."""

    column: FieldInfo | str
    direction: str = "ASC"  # ASC or DESC


class SelectQuery[T: Table]:
    """Builder for SELECT queries."""

    def __init__(
        self,
        pool: asyncpg.Pool | None,
        columns: tuple[type[Table] | FieldInfo, ...],
    ):
        self._pool = pool
        self._columns = columns
        self._from_table: type[Table] | None = None
        self._joins: list[JoinClause] = []
        self._where_clause: Expression | None = None
        self._order_by: list[OrderByClause] = []
        self._limit_value: int | None = None
        self._offset_value: int | None = None
        self._group_by: list[FieldInfo | str] = []

        # Infer from table if first column is a Table class
        if columns and isinstance(columns[0], type) and issubclass(columns[0], Table):
            self._from_table = columns[0]

    def from_(self, table: type[Table]) -> SelectQuery[T]:
        """Set the FROM table."""
        self._from_table = table
        return self

    def where(self, condition: Expression) -> SelectQuery[T]:
        """Add WHERE clause."""
        self._where_clause = condition
        return self

    def inner_join(self, table: type[Table], condition: Expression) -> SelectQuery[T]:
        """Add INNER JOIN."""
        self._joins.append(JoinClause("INNER", table, condition))
        return self

    def left_join(self, table: type[Table], condition: Expression) -> SelectQuery[T]:
        """Add LEFT JOIN."""
        self._joins.append(JoinClause("LEFT", table, condition))
        return self

    def right_join(self, table: type[Table], condition: Expression) -> SelectQuery[T]:
        """Add RIGHT JOIN."""
        self._joins.append(JoinClause("RIGHT", table, condition))
        return self

    def full_join(self, table: type[Table], condition: Expression) -> SelectQuery[T]:
        """Add FULL OUTER JOIN."""
        self._joins.append(JoinClause("FULL OUTER", table, condition))
        return self

    def order_by(self, column: FieldInfo | str, direction: str = "ASC") -> SelectQuery[T]:
        """Add ORDER BY clause."""
        self._order_by.append(OrderByClause(column, direction.upper()))
        return self

    def limit(self, n: int) -> SelectQuery[T]:
        """Add LIMIT clause."""
        self._limit_value = n
        return self

    def offset(self, n: int) -> SelectQuery[T]:
        """Add OFFSET clause."""
        self._offset_value = n
        return self

    def group_by(self, *columns: FieldInfo | str) -> SelectQuery[T]:
        """Add GROUP BY clause."""
        self._group_by.extend(columns)
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
                        f"{ob.column._table_name}.{ob.column._field_name} {ob.direction}"
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

    async def execute(self) -> list[T]:
        """Execute the query and return results."""
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")

        sql, params = self.build()
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        # Map rows to model instances if selecting a single table
        if (
            len(self._columns) == 1
            and isinstance(self._columns[0], type)
            and issubclass(self._columns[0], Table)
        ):
            model_class = self._columns[0]
            return [model_class.model_validate(dict(row)) for row in rows]  # type: ignore

        # Return as list of dicts for partial/multi-table selects
        return [dict(row) for row in rows]  # type: ignore

    async def first(self) -> T | None:
        """Execute and return first result or None."""
        self._limit_value = 1
        results = await self.execute()
        return results[0] if results else None


class InsertQuery[T: Table]:
    """Builder for INSERT queries."""

    def __init__(self, pool: asyncpg.Pool | None, table: type[T]):
        self._pool = pool
        self._table = table
        self._values: dict[str, Any] = {}
        self._returning: tuple[type[Table] | FieldInfo, ...] | None = None

    def values(self, **kwargs: Any) -> InsertQuery[T]:
        """Set values to insert."""
        self._values = kwargs
        return self

    def returning(self, *columns: type[Table] | FieldInfo) -> InsertQuery[T]:
        """Add RETURNING clause."""
        self._returning = columns
        return self

    def build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
        table_name = self._table.get_table_name()
        columns = list(self._values.keys())
        params = list(self._values.values())

        placeholders = [f"${i + 1}" for i in range(len(params))]

        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

        if self._returning:
            return_parts = []
            for col in self._returning:
                if isinstance(col, type) and issubclass(col, Table):
                    return_parts.append("*")
                elif isinstance(col, FieldInfo) and col._field_name:
                    return_parts.append(col._field_name)
            sql += f" RETURNING {', '.join(return_parts)}"

        return sql, params

    async def execute(self) -> T | dict[str, Any] | None:
        """Execute the insert."""
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")

        sql, params = self.build()
        async with self._pool.acquire() as conn:
            if self._returning:
                row = await conn.fetchrow(sql, *params)
                if row:
                    if (
                        self._returning
                        and isinstance(self._returning[0], type)
                        and issubclass(self._returning[0], Table)
                    ):
                        return self._table.model_validate(dict(row))
                    return dict(row)
                return None
            else:
                await conn.execute(sql, *params)
                return None


class UpdateQuery[T: Table]:
    """Builder for UPDATE queries."""

    def __init__(self, pool: asyncpg.Pool | None, table: type[T]):
        self._pool = pool
        self._table = table
        self._set_values: dict[str, Any] = {}
        self._where_clause: Expression | None = None
        self._returning: tuple[type[Table] | FieldInfo, ...] | None = None

    def set(self, **kwargs: Any) -> UpdateQuery[T]:
        """Set values to update."""
        self._set_values = kwargs
        return self

    def where(self, condition: Expression) -> UpdateQuery[T]:
        """Add WHERE clause."""
        self._where_clause = condition
        return self

    def returning(self, *columns: type[Table] | FieldInfo) -> UpdateQuery[T]:
        """Add RETURNING clause."""
        self._returning = columns
        return self

    def build(self) -> tuple[str, list[Any]]:
        """Build the SQL query and parameters."""
        table_name = self._table.get_table_name()
        params: list[Any] = []

        set_parts = []
        for col, val in self._set_values.items():
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
                elif isinstance(col, FieldInfo) and col._field_name:
                    return_parts.append(col._field_name)
            sql += f" RETURNING {', '.join(return_parts)}"

        return sql, params

    async def execute(self) -> list[T] | None:
        """Execute the update."""
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")

        sql, params = self.build()
        async with self._pool.acquire() as conn:
            if self._returning:
                rows = await conn.fetch(sql, *params)
                if (
                    self._returning
                    and isinstance(self._returning[0], type)
                    and issubclass(self._returning[0], Table)
                ):
                    return [self._table.model_validate(dict(row)) for row in rows]
                return [dict(row) for row in rows]  # type: ignore[return-value]
            else:
                await conn.execute(sql, *params)
                return None


class DeleteQuery[T: Table]:
    """Builder for DELETE queries."""

    def __init__(self, pool: asyncpg.Pool | None, table: type[T]):
        self._pool = pool
        self._table = table
        self._where_clause: Expression | None = None
        self._returning: tuple[type[Table] | FieldInfo, ...] | None = None

    def where(self, condition: Expression) -> DeleteQuery[T]:
        """Add WHERE clause."""
        self._where_clause = condition
        return self

    def returning(self, *columns: type[Table] | FieldInfo) -> DeleteQuery[T]:
        """Add RETURNING clause."""
        self._returning = columns
        return self

    def build(self) -> tuple[str, list[Any]]:
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

    async def execute(self) -> list[T] | None:
        """Execute the delete."""
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")

        sql, params = self.build()
        async with self._pool.acquire() as conn:
            if self._returning:
                rows = await conn.fetch(sql, *params)
                if (
                    self._returning
                    and isinstance(self._returning[0], type)
                    and issubclass(self._returning[0], Table)
                ):
                    return [self._table.model_validate(dict(row)) for row in rows]
                return [dict(row) for row in rows]  # type: ignore[return-value]
            else:
                await conn.execute(sql, *params)
                return None
