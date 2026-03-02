"""Non ORM string-based query builders for Derp."""

from __future__ import annotations

from typing import Any, Self

import asyncpg

from derp.kv.base import KVClient
from derp.orm.query.builder import SelectQuery, _acquire, _WhereShorthandMixin
from derp.orm.query.expressions import Expression
from derp.orm.router import ReplicaRouter

# =============================================================================
# Untyped INSERT
# =============================================================================


class _UntypedInsertBase:
    """Shared INSERT implementation for string-based queries."""

    def __init__(
        self,
        pool: asyncpg.Pool | asyncpg.Connection | None,
        table_name: str,
        *,
        router: ReplicaRouter | None = None,
    ):
        self._pool = pool
        self._table_name = table_name
        self._values: dict[str, Any] = {}
        self._returning_cols: tuple[str, ...] | None = None
        self._router: ReplicaRouter | None = router

    def _build(self) -> tuple[str, list[Any]]:
        columns = list(self._values.keys())
        params = list(self._values.values())
        placeholders = [f"${i + 1}" for i in range(len(params))]

        sql = (
            f"INSERT INTO {self._table_name} ({', '.join(columns)}) "
            f"VALUES ({', '.join(placeholders)})"
        )

        if self._returning_cols:
            sql += f" RETURNING {', '.join(self._returning_cols)}"

        return sql, params


class UntypedInsertQuery(_UntypedInsertBase):
    """INSERT query using string table name — execute() returns None."""

    def returning(self, *columns: str) -> UntypedInsertQueryReturning:
        """Add RETURNING clause. Use '*' for all columns."""
        query = UntypedInsertQueryReturning(
            self._pool, self._table_name, router=self._router
        )
        query._values = self._values
        query._returning_cols = columns
        return query

    def build(self) -> tuple[str, list[Any]]:
        return self._build()

    async def execute(self) -> None:
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")
        sql, params = self.build()
        async with _acquire(self._pool) as conn:
            await conn.execute(sql, *params)
        if self._router is not None:
            self._router.record_write()


class UntypedInsertQueryReturning(_UntypedInsertBase):
    """INSERT with RETURNING — execute() returns dict[str, Any]."""

    def build(self) -> tuple[str, list[Any]]:
        return self._build()

    async def execute(self) -> dict[str, Any]:
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")
        sql, params = self.build()
        async with _acquire(self._pool) as conn:
            row = await conn.fetchrow(sql, *params)
            if row is None:
                raise RuntimeError("INSERT RETURNING returned no rows")
        if self._router is not None:
            self._router.record_write()
        return dict(row)


# =============================================================================
# Untyped UPDATE
# =============================================================================


class _UntypedUpdateBase(_WhereShorthandMixin):
    """Shared UPDATE implementation for string-based queries."""

    def __init__(
        self,
        pool: asyncpg.Pool | asyncpg.Connection | None,
        table_name: str,
        *,
        router: ReplicaRouter | None = None,
    ):
        self._pool = pool
        self._table = table_name
        self._set_values: dict[str, Any] = {}
        self._where_clause: Expression | None = None
        self._returning_cols: tuple[str, ...] | None = None
        self._router: ReplicaRouter | None = router

    def _build(self) -> tuple[str, list[Any]]:
        params: list[Any] = []

        set_parts = []
        for col, val in self._set_values.items():
            params.append(val)
            set_parts.append(f"{col} = ${len(params)}")

        sql = f"UPDATE {self._table} SET {', '.join(set_parts)}"

        if self._where_clause:
            where_sql = self._where_clause.to_sql(params)
            sql += f" WHERE {where_sql}"

        if self._returning_cols:
            sql += f" RETURNING {', '.join(self._returning_cols)}"

        return sql, params


class UntypedUpdateQuery(_UntypedUpdateBase):
    """UPDATE query using string table name — execute() returns None."""

    def set(self, **kwargs: Any) -> Self:
        """Set values to update."""
        self._set_values = kwargs
        return self

    def where(self, cond: Expression) -> Self:
        """Add WHERE clause. Multiple calls combine with AND."""
        if self._where_clause is not None:
            self._where_clause = self._where_clause & cond
        else:
            self._where_clause = cond
        return self

    def returning(self, *columns: str) -> UntypedUpdateQueryReturning:
        """Add RETURNING clause. Use '*' for all columns."""
        query = UntypedUpdateQueryReturning(
            self._pool, self._table, router=self._router
        )
        query._set_values = self._set_values
        query._where_clause = self._where_clause
        query._returning_cols = columns
        return query

    def build(self) -> tuple[str, list[Any]]:
        return self._build()

    async def execute(self) -> None:
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")
        sql, params = self.build()
        async with _acquire(self._pool) as conn:
            await conn.execute(sql, *params)
        if self._router is not None:
            self._router.record_write()


class UntypedUpdateQueryReturning(_UntypedUpdateBase):
    """UPDATE with RETURNING — execute() returns list[dict[str, Any]]."""

    def where(self, cond: Expression) -> Self:
        """Add WHERE clause. Multiple calls combine with AND."""
        if self._where_clause is not None:
            self._where_clause = self._where_clause & cond
        else:
            self._where_clause = cond
        return self

    def build(self) -> tuple[str, list[Any]]:
        return self._build()

    async def execute(self) -> list[dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")
        sql, params = self.build()
        async with _acquire(self._pool) as conn:
            rows = await conn.fetch(sql, *params)
        if self._router is not None:
            self._router.record_write()
        return [dict(row) for row in rows]


# =============================================================================
# Untyped DELETE
# =============================================================================


class _UntypedDeleteBase(_WhereShorthandMixin):
    """Shared DELETE implementation for string-based queries."""

    def __init__(
        self,
        pool: asyncpg.Pool | asyncpg.Connection | None,
        table_name: str,
        *,
        router: ReplicaRouter | None = None,
    ):
        self._pool = pool
        self._table = table_name
        self._where_clause: Expression | None = None
        self._returning_cols: tuple[str, ...] | None = None
        self._router: ReplicaRouter | None = router

    def _build(self) -> tuple[str, list[Any]]:
        params: list[Any] = []
        sql = f"DELETE FROM {self._table}"

        if self._where_clause:
            where_sql = self._where_clause.to_sql(params)
            sql += f" WHERE {where_sql}"

        if self._returning_cols:
            sql += f" RETURNING {', '.join(self._returning_cols)}"

        return sql, params


class UntypedDeleteQuery(_UntypedDeleteBase):
    """DELETE query using string table name — execute() returns None."""

    def where(self, cond: Expression) -> Self:
        """Add WHERE clause. Multiple calls combine with AND."""
        if self._where_clause is not None:
            self._where_clause = self._where_clause & cond
        else:
            self._where_clause = cond
        return self

    def returning(self, *columns: str) -> UntypedDeleteQueryReturning:
        """Add RETURNING clause. Use '*' for all columns."""
        query = UntypedDeleteQueryReturning(
            self._pool, self._table, router=self._router
        )
        query._where_clause = self._where_clause
        query._returning_cols = columns
        return query

    def build(self) -> tuple[str, list[Any]]:
        return self._build()

    async def execute(self) -> None:
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")
        sql, params = self.build()
        async with _acquire(self._pool) as conn:
            await conn.execute(sql, *params)
        if self._router is not None:
            self._router.record_write()


class UntypedDeleteQueryReturning(_UntypedDeleteBase):
    """DELETE with RETURNING — execute() returns list[dict[str, Any]]."""

    def where(self, cond: Expression) -> Self:
        """Add WHERE clause. Multiple calls combine with AND."""
        if self._where_clause is not None:
            self._where_clause = self._where_clause & cond
        else:
            self._where_clause = cond
        return self

    def build(self) -> tuple[str, list[Any]]:
        return self._build()

    async def execute(self) -> list[dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("No database connection. Call db.connect() first.")
        sql, params = self.build()
        async with _acquire(self._pool) as conn:
            rows = await conn.fetch(sql, *params)
        if self._router is not None:
            self._router.record_write()
        return [dict(row) for row in rows]


# =============================================================================
# TableRef — non ORM entry point
# =============================================================================


class TableRef:
    """Non ORM table reference for string-based queries.

    Example::

        db.table("users").select("*").eq("id", 1).execute()
        db.table("users").insert({"name": "Bob"}).execute()
        db.table("users").update({"name": "Robert"}).eq("id", 1).execute()
        db.table("users").delete().eq("id", 1).execute()
    """

    def __init__(
        self,
        table_name: str,
        pool: asyncpg.Pool | asyncpg.Connection | None,
        *,
        cache_store: KVClient | None = None,
        router: ReplicaRouter | None = None,
    ):
        self._table_name = table_name
        self._pool = pool
        self._cache_store = cache_store
        self._router = router

    def select(self, *columns: str) -> SelectQuery[dict[str, Any]]:
        """Start a SELECT query.

        Args:
            *columns: Column names to select. Use ``"*"`` or omit for all
                columns. A single comma-separated string like
                ``"name, email"`` is also accepted.

        Returns:
            SelectQuery[dict[str, Any]]
        """
        if not columns:
            resolved: tuple[str, ...] = ("*",)
        elif len(columns) == 1 and "," in columns[0]:
            resolved = tuple(c.strip() for c in columns[0].split(","))
        else:
            resolved = columns

        query: SelectQuery[dict[str, Any]] = SelectQuery(
            self._pool,
            resolved,  # type: ignore[arg-type]
            cache_store=self._cache_store,
            router=self._router,
        )
        query._from_table = self._table_name
        return query

    def insert(self, values: dict[str, Any]) -> UntypedInsertQuery:
        """Start an INSERT query.

        Args:
            values: Column-value mapping to insert.

        Returns:
            UntypedInsertQuery
        """
        query = UntypedInsertQuery(self._pool, self._table_name, router=self._router)
        query._values = values
        return query

    def update(self, values: dict[str, Any]) -> UntypedUpdateQuery:
        """Start an UPDATE query.

        Args:
            values: Column-value mapping of SET assignments.

        Returns:
            UntypedUpdateQuery
        """
        query = UntypedUpdateQuery(self._pool, self._table_name, router=self._router)
        query._set_values = values
        return query

    def delete(self) -> UntypedDeleteQuery:
        """Start a DELETE query.

        Returns:
            UntypedDeleteQuery
        """
        return UntypedDeleteQuery(self._pool, self._table_name, router=self._router)
