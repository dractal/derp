"""Async query engine (asyncpg wrapper) for Derp ORM."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import TracebackType
from typing import Any, overload

import asyncpg

from derp.kv.base import KVClient
from derp.orm.column.base import Column
from derp.orm.query.builder import DeleteQuery, InsertQuery, SelectQuery, UpdateQuery
from derp.orm.query.expressions import Expression
from derp.orm.query.table_ref import TableRef
from derp.orm.router import ReplicaRouter
from derp.orm.table import Table


class _QueryBase:
    """Shared typed query methods for Transaction and DatabaseEngine.

    Subclasses must set ``_pool`` and may set ``_cache_store`` / ``_router``.
    """

    _pool: asyncpg.Pool | asyncpg.Connection | None
    _cache_store: KVClient | None
    _router: ReplicaRouter | None

    # -- select overloads (1 table, 1–10 columns, fallback) --------

    @overload
    def select[T: Table](self, table: type[T], /) -> SelectQuery[T]: ...

    @overload
    def select[A](self, c1: Column[A], /) -> SelectQuery[A]: ...

    @overload
    def select[A, B](
        self,
        c1: Column[A],
        c2: Column[B],
        /,
    ) -> SelectQuery[tuple[A, B]]: ...

    @overload
    def select[A, B, C](
        self,
        c1: Column[A],
        c2: Column[B],
        c3: Column[C],
        /,
    ) -> SelectQuery[tuple[A, B, C]]: ...

    @overload
    def select[A, B, C, D](
        self,
        c1: Column[A],
        c2: Column[B],
        c3: Column[C],
        c4: Column[D],
        /,
    ) -> SelectQuery[tuple[A, B, C, D]]: ...

    @overload
    def select[A, B, C, D, E](
        self,
        c1: Column[A],
        c2: Column[B],
        c3: Column[C],
        c4: Column[D],
        c5: Column[E],
        /,
    ) -> SelectQuery[tuple[A, B, C, D, E]]: ...

    @overload
    def select[A, B, C, D, E, F](
        self,
        c1: Column[A],
        c2: Column[B],
        c3: Column[C],
        c4: Column[D],
        c5: Column[E],
        c6: Column[F],
        /,
    ) -> SelectQuery[tuple[A, B, C, D, E, F]]: ...

    @overload
    def select[A, B, C, D, E, F, G](
        self,
        c1: Column[A],
        c2: Column[B],
        c3: Column[C],
        c4: Column[D],
        c5: Column[E],
        c6: Column[F],
        c7: Column[G],
        /,
    ) -> SelectQuery[tuple[A, B, C, D, E, F, G]]: ...

    @overload
    def select[A, B, C, D, E, F, G, H](
        self,
        c1: Column[A],
        c2: Column[B],
        c3: Column[C],
        c4: Column[D],
        c5: Column[E],
        c6: Column[F],
        c7: Column[G],
        c8: Column[H],
        /,
    ) -> SelectQuery[tuple[A, B, C, D, E, F, G, H]]: ...

    @overload
    def select[A, B, C, D, E, F, G, H, I](
        self,
        c1: Column[A],
        c2: Column[B],
        c3: Column[C],
        c4: Column[D],
        c5: Column[E],
        c6: Column[F],
        c7: Column[G],
        c8: Column[H],
        c9: Column[I],
        /,
    ) -> SelectQuery[tuple[A, B, C, D, E, F, G, H, I]]: ...

    @overload
    def select[A, B, C, D, E, F, G, H, I, J](
        self,
        c1: Column[A],
        c2: Column[B],
        c3: Column[C],
        c4: Column[D],
        c5: Column[E],
        c6: Column[F],
        c7: Column[G],
        c8: Column[H],
        c9: Column[I],
        c10: Column[J],
        /,
    ) -> SelectQuery[tuple[A, B, C, D, E, F, G, H, I, J]]: ...

    @overload
    def select(
        self, *columns: type[Table] | Column[Any] | Expression
    ) -> SelectQuery[dict[str, Any]]: ...

    def select(
        self, *columns: type[Table] | Column[Any] | Expression
    ) -> SelectQuery[Any]:
        """Start a SELECT query.

        Args:
            *columns: Table classes, Column columns, or Expressions to select

        Returns:
            Typed SelectQuery builder:
            - ``SelectQuery[T]`` for a single Table class
            - ``SelectQuery[A]`` for a single Column
            - ``SelectQuery[tuple[A, B, ...]]`` for multiple Columns
            - ``SelectQuery[dict]`` for mixed/untyped selections
        """
        return SelectQuery(
            self._pool,
            columns,
            cache_store=getattr(self, "_cache_store", None),
            router=getattr(self, "_router", None),
        )

    def insert[T: Table](self, table: type[T]) -> InsertQuery[T]:
        """Start an INSERT query."""
        return InsertQuery(
            self._pool,
            table,
            router=getattr(self, "_router", None),
        )

    def update[T: Table](self, table: type[T]) -> UpdateQuery[T]:
        """Start an UPDATE query."""
        return UpdateQuery(
            self._pool,
            table,
            router=getattr(self, "_router", None),
        )

    def delete[T: Table](self, table: type[T]) -> DeleteQuery[T]:
        """Start a DELETE query."""
        return DeleteQuery(
            self._pool,
            table,
            router=getattr(self, "_router", None),
        )

    def table(self, table_name: str) -> TableRef:
        """Start a non-ORM query from a string table name."""
        return TableRef(table_name, self._pool)


class Transaction(_QueryBase):
    """Transaction context manager with query builder support.

    Queries created via this transaction's ``select``, ``insert``,
    ``update``, and ``delete`` methods reuse the transaction's
    connection instead of acquiring a new one from the pool.

    Example::

        async with db.transaction() as txn:
            user = await txn.insert(User).values(name="Alice").returning(User).execute()
            await txn.update(Profile).set(user_id=user.id).execute()
    """

    def __init__(self, connection: asyncpg.Connection):
        self._pool: asyncpg.Connection = connection
        self._cache_store: KVClient | None = None
        self._router: ReplicaRouter | None = None
        self._txn: asyncpg.connection.transaction.Transaction | None = None

    async def __aenter__(self) -> Transaction:
        self._txn = self._pool.transaction()  # type: ignore[union-attr]
        await self._txn.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._txn is None:
            return
        if exc_type is not None:
            await self._txn.rollback()
        else:
            await self._txn.commit()


class DatabaseEngine(_QueryBase):
    """Main async database engine for Derp ORM.

    Example:
        db = DatabaseEngine("postgresql://user:pass@localhost:5432/mydb")

        async with db:
            users = await db.select(User).where(User.name == "Alice").execute()

        # Or manual lifecycle
        await db.connect()
        users = await db.select(User).execute()
        await db.disconnect()
    """

    def __init__(
        self,
        dsn: str,
        *,
        min_size: int = 2,
        max_size: int = 10,
        statement_cache_size: int | None = None,
    ):
        """Initialize Derp engine.

        Args:
            dsn: PostgreSQL connection string
            min_size: Minimum connection pool size
            max_size: Maximum connection pool size
            statement_cache_size: Size of the prepared statement cache per
                connection. Set to 0 to disable, which is required when
                connecting through PgBouncer in transaction mode. None
                uses asyncpg's default.
        """
        self._dsn = dsn
        self._min_size = min_size
        self._max_size = max_size
        self._statement_cache_size = statement_cache_size
        self._pool: asyncpg.Pool | None = None
        self._cache_store: KVClient | None = None
        self._router: ReplicaRouter | None = None

    async def connect(self) -> None:
        """Establish connection pool."""
        if self._pool is not None:
            return
        kwargs: dict[str, Any] = {
            "min_size": self._min_size,
            "max_size": self._max_size,
        }
        if self._statement_cache_size is not None:
            kwargs["statement_cache_size"] = self._statement_cache_size
        self._pool = await asyncpg.create_pool(self._dsn, **kwargs)

    async def disconnect(self) -> None:
        """Close connection pool."""
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def __aenter__(self) -> DatabaseEngine:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.disconnect()

    def set_cache(self, store: KVClient | None) -> None:
        """Set the KV store for query result caching."""
        self._cache_store = store

    def set_router(self, router: ReplicaRouter | None) -> None:
        """Set the replica router for automatic read routing."""
        self._router = router

    @property
    def pool(self) -> asyncpg.Pool:
        """Get the connection pool."""
        if self._pool is None:
            raise RuntimeError(
                "Database not connected. Call connect() or use async context manager."
            )
        return self._pool  # type: ignore[return-value]

    def table(self, table_name: Table | str) -> TableRef:
        """Start a non ORM query from a table name or Table class."""
        name = (
            table_name if isinstance(table_name, str) else table_name.get_table_name()
        )
        return TableRef(
            name,
            self._pool,
            cache_store=self._cache_store,
            router=self._router,
        )

    async def execute(
        self, query: str, params: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        """Execute a raw SQL query.

        Args:
            query: SQL query string with $N placeholders
            params: Parameter values

        Returns:
            List of row dicts

        Example:
            result = await db.execute("SELECT * FROM users WHERE id = $1", [user_id])
        """
        async with self.pool.acquire() as conn:
            if params:
                rows = await conn.fetch(query, *params)
            else:
                rows = await conn.fetch(query)
            return [dict(row) for row in rows]

    async def execute_many(self, query: str, params_list: list[list[Any]]) -> None:
        """Execute a query with multiple parameter sets.

        Args:
            query: SQL query string
            params_list: List of parameter lists
        """
        async with self.pool.acquire() as conn:
            await conn.executemany(query, params_list)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Transaction]:
        """Start a transaction.

        Example:
            async with db.transaction():
                await db.insert(User).values(...).execute()
                await db.update(Post).set(...).execute()
        """
        async with self.pool.acquire() as conn:
            txn = Transaction(conn)
            async with txn:
                yield txn

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[asyncpg.Connection]:
        """Acquire a connection from the pool.

        Example:
            async with db.acquire() as conn:
                await conn.execute("...")
        """
        async with self.pool.acquire() as conn:
            yield conn
