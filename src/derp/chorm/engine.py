"""ClickHouse connection engine.

Thin async wrapper over ``clickhouse_connect.get_async_client`` that
exposes the same surface as the rest of the chorm package: ``fetch``
for SELECT, ``command`` for DDL/DML, plus ``select`` / ``insert`` /
``alter`` / ``update`` / ``delete`` builder factories.
"""

from collections.abc import Sequence
from typing import Any, Self

# Plain import: clickhouse-connect is a hard dependency (pyproject), so a lazy
# wrapper would only mislead readers into thinking it is optional.
import clickhouse_connect
from clickhouse_connect import driver

from derp.chorm.exceptions import ChormBackendError, ChormNotConnectedError
from derp.chorm.query.builder import (
    AlterQuery,
    DeleteMutation,
    InsertQuery,
    SelectQuery,
    UpdateMutation,
)
from derp.chorm.table import Table


class ClickHouseEngine:
    """Async ClickHouse client."""

    def __init__(
        self,
        url: str | None = None,
        *,
        host: str | None = None,
        port: int | None = None,
        username: str = "default",
        password: str = "",
        database: str = "default",
        secure: bool = False,
        **kwargs: Any,
    ) -> None:
        self._url = url
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._database = database
        self._secure = secure
        self._extra_kwargs = kwargs
        self._client: driver.AsyncClient | None = None

    async def connect(self) -> None:
        """Open the underlying clickhouse-connect client."""
        if self._client is not None:
            return
        kwargs: dict[str, Any] = {
            "username": self._username,
            "password": self._password,
            "database": self._database,
            "secure": self._secure,
        }
        if self._url is not None:
            kwargs["dsn"] = self._url
        else:
            if self._host is not None:
                kwargs["host"] = self._host
            if self._port is not None:
                kwargs["port"] = self._port
        kwargs.update(self._extra_kwargs)
        self._client = await clickhouse_connect.get_async_client(**kwargs)

    async def disconnect(self) -> None:
        """Close the underlying client."""
        if self._client is not None:
            await self._client.close()
            self._client = None

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.disconnect()

    # -- Raw access ----------------------------------------------------------

    async def fetch(
        self,
        sql: str,
        *,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute *sql* and return rows as a list of column-name dicts."""
        kwargs: dict[str, Any] = {}
        if parameters:
            kwargs["parameters"] = parameters
        client = self.client
        try:
            result = await client.query(sql, **kwargs)
        except Exception as exc:
            raise ChormBackendError(str(exc)) from exc
        return list(result.named_results())

    async def command(
        self,
        sql: str,
        *,
        parameters: dict[str, Any] | None = None,
    ) -> Any:
        """Execute a non-SELECT statement (DDL/DML)."""
        kwargs: dict[str, Any] = {}
        if parameters:
            kwargs["parameters"] = parameters
        client = self.client
        try:
            return await client.command(sql, **kwargs)
        except Exception as exc:
            raise ChormBackendError(str(exc)) from exc

    @property
    def client(self) -> "driver.AsyncClient":
        """The underlying clickhouse-connect async client.

        Use for native operations not covered by the builder API, such as
        bulk ``insert`` with explicit column types or streaming formats.
        """
        if self._client is None:
            raise ChormNotConnectedError("Not connected — call connect() first.")
        return self._client

    # -- Builder factories ---------------------------------------------------

    def select[T](self, *columns: Any) -> SelectQuery[T]:
        """Start a SELECT query."""
        return SelectQuery(self, columns)

    def insert[T: Table](self, table: type[T]) -> InsertQuery[T]:
        """Start an INSERT into *table*."""
        return InsertQuery(self, table)

    def alter(
        self,
        table: type[Table] | str,
        *,
        on_cluster: str | None = None,
    ) -> AlterQuery:
        """Start an ALTER TABLE statement."""
        return AlterQuery(self, table, on_cluster=on_cluster)

    def update[T: Table](
        self,
        table: type[T],
        *,
        on_cluster: str | None = None,
    ) -> UpdateMutation[T]:
        """Start an ``ALTER TABLE … UPDATE`` mutation."""
        return UpdateMutation(self, table, on_cluster=on_cluster)

    def delete[T: Table](
        self,
        table: type[T],
        *,
        lightweight: bool = False,
        on_cluster: str | None = None,
    ) -> DeleteMutation[T]:
        """Start a DELETE.

        When *lightweight* is True, emits a ``DELETE FROM ... WHERE ...``
        statement (lightweight delete).  Otherwise emits an
        ``ALTER TABLE ... DELETE WHERE ...`` mutation.
        """
        return DeleteMutation(
            self, table, lightweight=lightweight, on_cluster=on_cluster
        )

    # -- Convenience DDL helpers ---------------------------------------------

    async def create_table(
        self,
        table: type[Table],
        *,
        if_not_exists: bool = False,
        on_cluster: str | None = None,
    ) -> None:
        from derp.chorm.ddl import build_create_table

        sql = build_create_table(
            table, if_not_exists=if_not_exists, on_cluster=on_cluster
        )
        await self.command(sql)

    async def drop_table(
        self,
        table: type[Table] | str,
        *,
        if_exists: bool = False,
        on_cluster: str | None = None,
        sync: bool = False,
    ) -> None:
        from derp.chorm.ddl import build_drop_table

        name = table.get_full_name() if isinstance(table, type) else table
        sql = build_drop_table(
            name, if_exists=if_exists, on_cluster=on_cluster, sync=sync
        )
        await self.command(sql)

    async def truncate_table(
        self,
        table: type[Table] | str,
        *,
        if_exists: bool = False,
        on_cluster: str | None = None,
    ) -> None:
        from derp.chorm.ddl import build_truncate_table

        name = table.get_full_name() if isinstance(table, type) else table
        sql = build_truncate_table(name, if_exists=if_exists, on_cluster=on_cluster)
        await self.command(sql)

    async def optimize_table(
        self,
        table: type[Table] | str,
        *,
        on_cluster: str | None = None,
        partition: str | None = None,
        final: bool = False,
        deduplicate: bool = False,
        deduplicate_by: Sequence[str] | None = None,
    ) -> None:
        from derp.chorm.ddl import build_optimize_table

        name = table.get_full_name() if isinstance(table, type) else table
        sql = build_optimize_table(
            name,
            on_cluster=on_cluster,
            partition=partition,
            final=final,
            deduplicate=deduplicate,
            deduplicate_by=deduplicate_by,
        )
        await self.command(sql)

    async def exchange_tables(
        self,
        a: type[Table] | str,
        b: type[Table] | str,
        *,
        on_cluster: str | None = None,
    ) -> None:
        from derp.chorm.ddl import build_exchange_tables

        name_a = a.get_full_name() if isinstance(a, type) else a
        name_b = b.get_full_name() if isinstance(b, type) else b
        sql = build_exchange_tables(name_a, name_b, on_cluster=on_cluster)
        await self.command(sql)
