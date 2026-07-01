"""Tests for the ClickHouseEngine client surface, using a fake client."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from derp.chorm import (
    ChormBackendError,
    ChormNotConnectedError,
    ClickHouseEngine,
    Field,
    Int64,
    MergeTree,
    NoRowsError,
    String,
    Table,
    UInt64,
)


class Item(Table, table="items"):
    id: UInt64 = Field()
    name: String = Field()
    qty: Int64 = Field()
    __engine__ = MergeTree(order_by="id")


@dataclass
class FakeQueryResult:
    column_names: list[str]
    result_rows: list[list[Any]]

    def named_results(self):
        for row in self.result_rows:
            yield dict(zip(self.column_names, row, strict=False))


class FakeClient:
    """Minimal stub of ``clickhouse_connect.AsyncClient`` for unit tests."""

    def __init__(
        self, *, rows: list[list[Any]] | None = None, columns: list[str] | None = None
    ) -> None:
        self.query_calls: list[tuple[str, dict[str, Any]]] = []
        self.command_calls: list[tuple[str, dict[str, Any]]] = []
        self.insert_calls: list[tuple[Any, ...]] = []
        self._rows = rows or []
        self._columns = columns or []
        self.closed = False

    async def query(self, sql: str, **kwargs: Any) -> FakeQueryResult:
        self.query_calls.append((sql, kwargs))
        return FakeQueryResult(self._columns, self._rows)

    async def command(self, sql: str, **kwargs: Any) -> str:
        self.command_calls.append((sql, kwargs))
        return "ok"

    async def insert(
        self, table: str, data: Any, *, column_names: list[str] | None = None
    ) -> str:
        self.insert_calls.append((table, data, column_names))
        return "ok"

    async def close(self) -> None:
        self.closed = True


def _make_engine(client: FakeClient) -> ClickHouseEngine:
    e = ClickHouseEngine(host="localhost")
    e._client = client  # ty: ignore[invalid-assignment]
    return e


@pytest.mark.asyncio
async def test_fetch_query_returns_dicts():
    fake = FakeClient(columns=["id", "name", "qty"], rows=[[1, "a", 5], [2, "b", 7]])
    db = _make_engine(fake)
    rows = await db.fetch("SELECT id, name, qty FROM items")
    assert rows == [
        {"id": 1, "name": "a", "qty": 5},
        {"id": 2, "name": "b", "qty": 7},
    ]
    assert fake.query_calls[0][0] == "SELECT id, name, qty FROM items"


@pytest.mark.asyncio
async def test_fetch_with_parameters():
    fake = FakeClient(columns=["id"], rows=[[1]])
    db = _make_engine(fake)
    await db.fetch(
        "SELECT id WHERE x = {x:UInt32}",
        parameters={"x": 5},
    )
    _, kwargs = fake.query_calls[0]
    assert kwargs["parameters"] == {"x": 5}


@pytest.mark.asyncio
async def test_command_executes_ddl():
    fake = FakeClient()
    db = _make_engine(fake)
    await db.command("DROP TABLE foo")
    assert fake.command_calls[0][0] == "DROP TABLE foo"


@pytest.mark.asyncio
async def test_create_table_helper():
    fake = FakeClient()
    db = _make_engine(fake)
    await db.create_table(Item, if_not_exists=True)
    sql = fake.command_calls[0][0]
    assert "CREATE TABLE IF NOT EXISTS items" in sql


@pytest.mark.asyncio
async def test_drop_table_helper():
    fake = FakeClient()
    db = _make_engine(fake)
    await db.drop_table(Item, if_exists=True, sync=True)
    sql = fake.command_calls[0][0]
    assert "DROP TABLE IF EXISTS items" in sql
    assert "SYNC" in sql


@pytest.mark.asyncio
async def test_truncate_table_helper():
    fake = FakeClient()
    db = _make_engine(fake)
    await db.truncate_table(Item)
    assert "TRUNCATE TABLE items" in fake.command_calls[0][0]


@pytest.mark.asyncio
async def test_select_executes_via_builder():
    fake = FakeClient(columns=["id", "name", "qty"], rows=[[1, "a", 5]])
    db = _make_engine(fake)
    rows = await db.select(Item).where(Item.id == 1).execute()
    assert len(rows) == 1
    assert rows[0].id == 1
    assert rows[0].name == "a"
    # The SQL submitted was the SelectQuery output
    sql, _ = fake.query_calls[0]
    assert "FROM items" in sql


@pytest.mark.asyncio
async def test_select_first_or_none():
    fake = FakeClient(columns=["id", "name", "qty"], rows=[])
    db = _make_engine(fake)
    result = await db.select(Item).first_or_none()
    assert result is None


@pytest.mark.asyncio
async def test_select_first_raises_when_empty():
    fake = FakeClient(columns=["id", "name", "qty"], rows=[])
    db = _make_engine(fake)
    with pytest.raises(NoRowsError):
        await db.select(Item).first()


@pytest.mark.asyncio
async def test_insert_via_builder():
    fake = FakeClient()
    db = _make_engine(fake)
    await db.insert(Item).values(id=1, name="x", qty=5).execute()
    sql = fake.command_calls[0][0]
    assert sql.startswith("INSERT INTO items")


def test_client_property_exposes_underlying_client():
    fake = FakeClient()
    db = _make_engine(fake)
    assert db.client is fake


def test_client_property_requires_connect():
    db = ClickHouseEngine(host="localhost")
    with pytest.raises(ChormNotConnectedError, match="connect"):
        _ = db.client


@pytest.mark.asyncio
async def test_client_native_insert():
    fake = FakeClient()
    db = _make_engine(fake)
    await db.client.insert("items", [[1, "x", 5]], column_names=["id", "name", "qty"])
    assert fake.insert_calls[0] == ("items", [[1, "x", 5]], ["id", "name", "qty"])


@pytest.mark.asyncio
async def test_alter_via_builder():
    fake = FakeClient()
    db = _make_engine(fake)
    await db.alter(Item).add_column("extra", "Int64").execute()
    sql = fake.command_calls[0][0]
    assert "ADD COLUMN" in sql


@pytest.mark.asyncio
async def test_update_mutation_via_builder():
    fake = FakeClient()
    db = _make_engine(fake)
    await db.update(Item).set(name="z").where(Item.id == 1).execute()
    sql = fake.command_calls[0][0]
    assert "ALTER TABLE items UPDATE" in sql


@pytest.mark.asyncio
async def test_delete_mutation_via_builder():
    fake = FakeClient()
    db = _make_engine(fake)
    await db.delete(Item, lightweight=True).where(Item.id == 1).execute()
    sql = fake.command_calls[0][0]
    assert "DELETE FROM items" in sql


@pytest.mark.asyncio
async def test_disconnect_closes_client():
    fake = FakeClient()
    db = _make_engine(fake)
    await db.disconnect()
    assert fake.closed is True
    assert db._client is None


@pytest.mark.asyncio
async def test_disconnect_is_idempotent():
    db = ClickHouseEngine(host="localhost")
    # Never connected — should not raise
    await db.disconnect()


@pytest.mark.asyncio
async def test_connect_passes_url_as_dsn(monkeypatch: pytest.MonkeyPatch):
    calls: list[dict[str, Any]] = []
    fake = FakeClient()

    async def fake_get_async_client(**kwargs: Any) -> FakeClient:
        calls.append(kwargs)
        return fake

    import derp.chorm.engine as engine_module

    monkeypatch.setattr(
        engine_module.clickhouse_connect,
        "get_async_client",
        fake_get_async_client,
    )

    db = ClickHouseEngine(url="https://example.test:8443/default", secure=True)
    await db.connect()

    assert calls[0]["dsn"] == "https://example.test:8443/default"
    assert "host" not in calls[0]
    assert db.client is fake


@pytest.mark.asyncio
async def test_command_without_connect_raises():
    db = ClickHouseEngine(host="localhost")
    with pytest.raises(ChormNotConnectedError, match="connect"):
        await db.command("SELECT 1")


@pytest.mark.asyncio
async def test_fetch_without_connect_raises():
    db = ClickHouseEngine(host="localhost")
    with pytest.raises(ChormNotConnectedError, match="connect"):
        await db.fetch("SELECT 1")


@pytest.mark.asyncio
async def test_command_runs_arbitrary_sql():
    fake = FakeClient()
    db = _make_engine(fake)
    await db.command("OPTIMIZE TABLE items FINAL")
    assert "OPTIMIZE" in fake.command_calls[0][0]


@pytest.mark.asyncio
async def test_optimize_table_helper():
    fake = FakeClient()
    db = _make_engine(fake)
    await db.optimize_table(Item, final=True)
    assert fake.command_calls[0][0] == "OPTIMIZE TABLE items FINAL"


@pytest.mark.asyncio
async def test_exchange_tables_helper():
    fake = FakeClient()
    db = _make_engine(fake)
    await db.exchange_tables(Item, "items_new")
    assert fake.command_calls[0][0] == "EXCHANGE TABLES items AND items_new"


@pytest.mark.asyncio
async def test_select_count_via_subquery():
    fake = FakeClient(columns=["count()"], rows=[[42]])
    db = _make_engine(fake)
    n = await db.select(Item).count()
    assert n == 42


@pytest.mark.asyncio
async def test_select_query_unbound_engine_raises():
    from derp.chorm import SelectQuery

    q = SelectQuery(None, (Item,))
    with pytest.raises(ChormNotConnectedError, match="engine"):
        await q.execute()


@pytest.mark.asyncio
async def test_insert_query_unbound_engine_raises():
    from derp.chorm import InsertQuery

    q = InsertQuery(None, Item).values(id=1, name="x", qty=5)
    with pytest.raises(ChormNotConnectedError, match="engine"):
        await q.execute()


@pytest.mark.asyncio
async def test_async_context_manager_connects_and_disconnects(
    monkeypatch: pytest.MonkeyPatch,
):
    fake = FakeClient()

    async def fake_get_async_client(**_: Any) -> FakeClient:
        return fake

    import derp.chorm.engine as engine_module

    monkeypatch.setattr(
        engine_module.clickhouse_connect, "get_async_client", fake_get_async_client
    )

    async with ClickHouseEngine(host="localhost") as db:
        assert db.client is fake
    assert fake.closed is True


@pytest.mark.asyncio
async def test_driver_error_is_wrapped_as_backend_error():
    class Boom(FakeClient):
        async def command(self, sql: str, **kwargs: Any) -> str:
            raise ValueError("driver exploded")

    db = _make_engine(Boom())
    with pytest.raises(ChormBackendError):
        await db.command("OPTIMIZE TABLE items FINAL")
