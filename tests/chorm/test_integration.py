"""Integration tests: generated SQL executed against a real ClickHouse engine.

Backed by chdb (embedded ClickHouse) via the ``ch_engine`` fixture, these prove
properties the string-assertion suite cannot — that emitted DDL/DML actually
parses and runs, and that the migration introspect→diff round-trip is a true
no-op (the gap that let the data-wipe and unquoted-default blockers ship).

Skipped automatically when chdb is not installed.
"""

from __future__ import annotations

from derp.chorm import (
    Array,
    ClickHouseEngine,
    DateTime,
    Field,
    LowCardinality,
    MergeTree,
    Nullable,
    ReplacingMergeTree,
    String,
    Table,
    UInt64,
)
from derp.chorm.migrations import diff_snapshots, snapshot_from_tables
from derp.chorm.migrations.introspect import introspect
from derp.chorm.migrations.statements import CreateTable, DropTable


async def test_create_insert_select_roundtrip(ch_engine: ClickHouseEngine) -> None:
    class Event(Table, table="events"):
        id: UInt64 = Field()
        name: String = Field()
        __engine__ = MergeTree(order_by="id")

    await ch_engine.create_table(Event)
    await ch_engine.insert(Event).values(id=1, name="a").execute()
    await ch_engine.insert(Event).values(id=2, name="b").execute()

    rows = await ch_engine.select(Event).where(Event.id == 2).execute()
    assert len(rows) == 1
    assert rows[0].id == 2
    assert rows[0].name == "b"


async def test_migration_roundtrip_is_noop(ch_engine: ClickHouseEngine) -> None:
    """Blocker #1: pushing an unchanged schema must NOT drop+recreate the table."""

    class Event(Table, table="events"):
        id: UInt64 = Field()
        ts: DateTime = Field()
        __engine__ = MergeTree(order_by="id")

    await ch_engine.create_table(Event)
    live = await introspect(ch_engine, database="default")
    model = snapshot_from_tables([Event])

    stmts = diff_snapshots(
        live, model, default_database="default", from_introspection=True
    )
    # The precise blocker invariant: no destructive DROP + CREATE for an
    # unchanged table — and, with engine-round-trip normalisation, a full no-op.
    assert not any(isinstance(s, DropTable | CreateTable) for s in stmts)
    assert stmts == []


async def test_engine_clause_normalization_roundtrip(
    ch_engine: ClickHouseEngine,
) -> None:
    """Composite clauses survive the introspect→diff round-trip.

    ClickHouse normalizes clause text server-side — the model's
    ``ORDER BY (id, ts)`` is stored as ``id, ts``, PRIMARY KEY is resolved from
    ORDER BY, and ``index_granularity`` is injected — none of which may read as
    a schema change (previously a spurious ``UnsupportedSchemaChange``).
    """

    class Event(Table, table="events"):
        id: UInt64 = Field()
        ts: DateTime = Field()
        v: UInt64 = Field()
        __engine__ = ReplacingMergeTree(
            "v", order_by=("id", "ts"), partition_by="toYYYYMM(ts)"
        )

    await ch_engine.create_table(Event)
    live = await introspect(ch_engine, database="default")
    model = snapshot_from_tables([Event])
    assert (
        diff_snapshots(live, model, default_database="default", from_introspection=True)
        == []
    )


async def test_string_default_roundtrip_is_noop(ch_engine: ClickHouseEngine) -> None:
    """Blocker #2: a string default round-trips clean (stored as DEFAULT 'active')."""

    class Account(Table, table="accounts"):
        id: UInt64 = Field()
        status: String = Field(default="active")
        __engine__ = MergeTree(order_by="id")

    await ch_engine.create_table(Account)
    live = await introspect(ch_engine, database="default")
    model = snapshot_from_tables([Account])

    # The introspected default_expression is "'active'", matching the snapshot.
    col = live.table_map()["default.accounts"].column_map()["status"]
    assert col.default == "'active'"
    assert (
        diff_snapshots(live, model, default_database="default", from_introspection=True)
        == []
    )


async def test_nullable_and_lowcardinality_ddl_executes(
    ch_engine: ClickHouseEngine,
) -> None:
    """Fixes #3/#4: composite + ordering modifiers produce valid, executable DDL."""

    class Doc(Table, table="docs"):
        id: UInt64 = Field()
        tags: Array[Nullable[String]] = Field()
        kind: LowCardinality[Nullable[String]] = Field()
        __engine__ = MergeTree(order_by="id")

    await ch_engine.create_table(Doc)  # raises if the DDL is invalid CH
    live = await introspect(ch_engine, database="default")
    cols = {c.name: c.type for c in live.table_map()["default.docs"].columns}
    assert cols["tags"] == "Array(Nullable(String))"
    assert cols["kind"] == "LowCardinality(Nullable(String))"


async def test_alter_add_column_executes(ch_engine: ClickHouseEngine) -> None:
    class Widget(Table, table="widgets"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

    await ch_engine.create_table(Widget)
    await ch_engine.alter(Widget).add_column("label", "String").execute()

    live = await introspect(ch_engine, database="default")
    cols = {c.name for c in live.table_map()["default.widgets"].columns}
    assert "label" in cols


async def test_column_level_settings_ddl_executes(ch_engine: ClickHouseEngine) -> None:
    class Blob(Table, table="blobs"):
        id: UInt64 = Field()
        data: String = Field(settings={"max_compress_block_size": 1024})
        __engine__ = MergeTree(order_by="id")

    await ch_engine.create_table(Blob)  # raises if the SETTINGS clause is invalid
    live = await introspect(ch_engine, database="default")
    assert "data" in {c.name for c in live.table_map()["default.blobs"].columns}
