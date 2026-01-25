"""Async query engine (asyncpg wrapper) for Dribble ORM."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, TypeVar

import asyncpg

from dribble.fields import FieldInfo
from dribble.query.builder import DeleteQuery, InsertQuery, SelectQuery, UpdateQuery
from dribble.table import Table

if TYPE_CHECKING:
    from types import TracebackType

T = TypeVar("T", bound=Table)


class Transaction:
    """Transaction context manager."""

    def __init__(self, connection: asyncpg.Connection):
        self._connection = connection
        self._transaction: asyncpg.connection.transaction.Transaction | None = None

    async def __aenter__(self) -> Transaction:
        self._transaction = self._connection.transaction()
        await self._transaction.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._transaction is None:
            return
        if exc_type is not None:
            await self._transaction.rollback()
        else:
            await self._transaction.commit()


class Dribble:
    """Main async database engine for Dribble ORM.

    Example:
        db = Dribble("postgresql://user:pass@localhost:5432/mydb")

        async with db:
            users = await db.select(User).where(eq(User.name, "Alice")).execute()

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
    ):
        """Initialize Dribble engine.

        Args:
            dsn: PostgreSQL connection string
            min_size: Minimum connection pool size
            max_size: Maximum connection pool size
        """
        self._dsn = dsn
        self._min_size = min_size
        self._max_size = max_size
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        """Establish connection pool."""
        if self._pool is not None:
            return
        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=self._min_size,
            max_size=self._max_size,
        )

    async def disconnect(self) -> None:
        """Close connection pool."""
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def __aenter__(self) -> Dribble:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.disconnect()

    @property
    def pool(self) -> asyncpg.Pool:
        """Get the connection pool."""
        if self._pool is None:
            raise RuntimeError(
                "Database not connected. Call connect() or use async context manager."
            )
        return self._pool

    def select(self, *columns: type[Table] | FieldInfo) -> SelectQuery[Any]:
        """Start a SELECT query.

        Args:
            *columns: Table classes or FieldInfo columns to select

        Returns:
            SelectQuery builder

        Examples:
            # Select all columns from User
            db.select(User).where(eq(User.id, 1))

            # Select specific columns
            db.select(User.id, User.name)

            # Join queries
            db.select(Post, User.name).from_(Post).inner_join(User, eq(Post.author_id, User.id))
        """
        return SelectQuery(self._pool, columns)

    def insert(self, table: type[T]) -> InsertQuery[T]:
        """Start an INSERT query.

        Args:
            table: Table class to insert into

        Returns:
            InsertQuery builder

        Example:
            await (
                db.insert(User)
                .values(name="Bob", email="bob@example.com")
                .returning(User)
                .execute()
            )
        """
        return InsertQuery(self._pool, table)

    def update(self, table: type[T]) -> UpdateQuery[T]:
        """Start an UPDATE query.

        Args:
            table: Table class to update

        Returns:
            UpdateQuery builder

        Example:
            await db.update(User).set(name="Robert").where(eq(User.id, 1)).execute()
        """
        return UpdateQuery(self._pool, table)

    def delete(self, table: type[T]) -> DeleteQuery[T]:
        """Start a DELETE query.

        Args:
            table: Table class to delete from

        Returns:
            DeleteQuery builder

        Example:
            await db.delete(User).where(eq(User.id, 1)).execute()
        """
        return DeleteQuery(self._pool, table)

    async def execute(self, query: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
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
