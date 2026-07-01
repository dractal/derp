"""Tests for ``Table`` definitions and DDL generation."""

from __future__ import annotations

import enum

import pytest

from derp.chorm import (
    UUID,
    Array,
    Codec,
    DateTime,
    Enum8,
    Field,
    Fn,
    Index,
    IndexType,
    Int64,
    LowCardinality,
    Map,
    Materialized,
    MergeTree,
    Nullable,
    Projection,
    ReplicatedMergeTree,
    String,
    Table,
    UInt64,
)
from derp.chorm.column.base import Alias as ColumnAlias
from derp.chorm.column.base import Ephemeral
from derp.chorm.ddl import (
    build_create_database,
    build_create_dictionary,
    build_create_materialized_view,
    build_create_view,
    build_drop_table,
    build_exchange_tables,
    build_optimize_table,
    build_rename_table,
    build_truncate_table,
)


def test_simple_table_ddl():
    class Event(Table, table="events"):
        id: UInt64 = Field()
        name: String = Field()
        __engine__ = MergeTree(order_by="id")

    ddl = Event.to_ddl()
    assert "CREATE TABLE events" in ddl
    assert "`id` UInt64" in ddl
    assert "`name` String" in ddl
    assert "ENGINE = MergeTree()" in ddl
    assert "ORDER BY id" in ddl


def test_if_not_exists():
    class T(Table, table="t"):
        x: Int64 = Field()
        __engine__ = MergeTree(order_by="x")

    assert "CREATE TABLE IF NOT EXISTS t" in T.to_ddl(if_not_exists=True)


def test_database_qualified_table():
    class T(Table, table="t", database="mydb"):
        x: Int64 = Field()
        __engine__ = MergeTree(order_by="x")

    assert T.get_full_name() == "mydb.t"
    assert "CREATE TABLE mydb.t" in T.to_ddl()


def test_on_cluster():
    class T(Table, table="t", cluster="my_cluster"):
        x: Int64 = Field()
        __engine__ = MergeTree(order_by="x")

    assert "ON CLUSTER my_cluster" in T.to_ddl()


def test_nullable_marker_applied():
    class T(Table, table="t"):
        x: Nullable[String] = Field()
        __engine__ = MergeTree(order_by="tuple()")

    ddl = T.to_ddl()
    assert "Nullable(String)" in ddl


def test_low_cardinality_marker_applied():
    class T(Table, table="t"):
        country: LowCardinality[String] = Field()
        __engine__ = MergeTree(order_by="tuple()")

    ddl = T.to_ddl()
    assert "LowCardinality(String)" in ddl


def test_nullable_and_low_cardinality():
    class T(Table, table="t"):
        x: Nullable[LowCardinality[String]] = Field()
        __engine__ = MergeTree(order_by="tuple()")

    ddl = T.to_ddl()
    # ClickHouse only accepts LowCardinality(Nullable(T)); the reverse nesting
    # is rejected, so both Python orderings must render this way.
    assert "LowCardinality(Nullable(String))" in ddl


def test_low_cardinality_of_nullable_table_path():
    class T(Table, table="t"):
        x: LowCardinality[Nullable[String]] = Field()
        __engine__ = MergeTree(order_by="tuple()")

    assert "LowCardinality(Nullable(String))" in T.to_ddl()


def test_array_and_map_columns():
    class T(Table, table="t"):
        tags: Array[String] = Field()
        attrs: Map[String, String] = Field()
        __engine__ = MergeTree(order_by="tuple()")

    ddl = T.to_ddl()
    assert "`tags` Array(String)" in ddl
    assert "`attrs` Map(String, String)" in ddl


def test_default_function_call():
    class T(Table, table="t"):
        ts: DateTime = Field(default=Fn.now())
        id: UUID = Field(default=Fn.generate_uuidv4())
        __engine__ = MergeTree(order_by="tuple()")

    ddl = T.to_ddl()
    assert "DEFAULT now()" in ddl
    assert "DEFAULT generateUUIDv4()" in ddl


def test_default_string_literal():
    class T(Table, table="t"):
        msg: String = Field(default="hello")
        __engine__ = MergeTree(order_by="tuple()")

    ddl = T.to_ddl()
    assert "DEFAULT 'hello'" in ddl


def test_default_int_literal():
    class T(Table, table="t"):
        n: Int64 = Field(default=42)
        __engine__ = MergeTree(order_by="tuple()")

    assert "DEFAULT 42" in T.to_ddl()


def test_default_string_literal_with_parentheses_is_quoted():
    # A literal that merely contains parentheses must NOT be treated as a SQL
    # function call (which would emit invalid unquoted DDL).
    class T(Table, table="t"):
        label: String = Field(default="hello (world)")
        __engine__ = MergeTree(order_by="tuple()")

    ddl = T.to_ddl()
    assert "DEFAULT 'hello (world)'" in ddl
    assert "DEFAULT hello (world)" not in ddl


def test_column_level_settings_are_emitted():
    class T(Table, table="t"):
        id: UInt64 = Field()
        blob: String = Field(settings={"max_compress_block_size": 1024})
        __engine__ = MergeTree(order_by="id")

    assert "SETTINGS (max_compress_block_size = 1024)" in T.to_ddl()


def test_materialized_column():
    class T(Table, table="t"):
        a: Int64 = Field()
        b: Int64 = Field(default=Materialized("a * 2"))
        __engine__ = MergeTree(order_by="a")

    ddl = T.to_ddl()
    assert "`b` Int64 MATERIALIZED a * 2" in ddl


def test_alias_column():
    class T(Table, table="t"):
        a: Int64 = Field()
        b: Int64 = Field(default=ColumnAlias("a + 1"))
        __engine__ = MergeTree(order_by="a")

    assert "`b` Int64 ALIAS a + 1" in T.to_ddl()


def test_ephemeral_column():
    class T(Table, table="t"):
        a: Int64 = Field()
        b: Int64 = Field(default=Ephemeral(0))
        __engine__ = MergeTree(order_by="a")

    ddl = T.to_ddl()
    assert "`b` Int64 EPHEMERAL 0" in ddl


def test_codec_column():
    class T(Table, table="t"):
        x: Int64 = Field(codec=(Codec("Delta"), Codec("ZSTD", 3)))
        __engine__ = MergeTree(order_by="x")

    assert "CODEC(Delta, ZSTD(3))" in T.to_ddl()


def test_codec_single():
    class T(Table, table="t"):
        x: Int64 = Field(codec=Codec("LZ4HC", 9))
        __engine__ = MergeTree(order_by="x")

    assert "CODEC(LZ4HC(9))" in T.to_ddl()


def test_column_ttl():
    class T(Table, table="t"):
        ts: DateTime = Field()
        msg: String = Field(ttl="ts + INTERVAL 7 DAY")
        __engine__ = MergeTree(order_by="ts")

    assert "TTL ts + INTERVAL 7 DAY" in T.to_ddl()


def test_column_comment():
    class T(Table, table="t"):
        x: Int64 = Field(comment="primary key")
        __engine__ = MergeTree(order_by="x")

    assert "COMMENT 'primary key'" in T.to_ddl()


def test_table_comment():
    class T(Table, table="t", comment="user activity log"):
        x: Int64 = Field()
        __engine__ = MergeTree(order_by="x")

    assert "COMMENT 'user activity log'" in T.to_ddl()


def test_indexes_inline():
    class T(Table, table="t"):
        x: Int64 = Field()
        y: String = Field()
        __engine__ = MergeTree(order_by="x")

        @classmethod
        def indexes(cls):
            return [
                Index(name="x_minmax", expression="x", type=IndexType.MINMAX),
                Index(
                    name="y_bloom",
                    expression="y",
                    type=IndexType.BLOOM_FILTER,
                    type_args=(0.01,),
                    granularity=4,
                ),
            ]

    ddl = T.to_ddl()
    assert "INDEX x_minmax x TYPE minmax GRANULARITY 1" in ddl
    assert "INDEX y_bloom y TYPE bloom_filter(0.01) GRANULARITY 4" in ddl


def test_projection_inline():
    class T(Table, table="t"):
        x: Int64 = Field()
        y: String = Field()
        __engine__ = MergeTree(order_by="x")

        @classmethod
        def projections(cls):
            return [Projection(name="by_y", select="y, x", order_by="y")]

    assert "PROJECTION by_y (SELECT y, x ORDER BY y)" in T.to_ddl()


def test_nullable_union_rejected():
    with pytest.raises(TypeError, match="Nullable"):

        class T(Table, table="t"):
            x: String | None = Field()
            __engine__ = MergeTree(order_by="tuple()")


class _Color(enum.IntEnum):
    RED = 1
    GREEN = 2


def test_enum_column():
    class T(Table, table="t"):
        c: Enum8[_Color] = Field()
        __engine__ = MergeTree(order_by="tuple()")

    ddl = T.to_ddl()
    assert "Enum8" in ddl
    assert "'RED' = 1" in ddl
    assert "'GREEN' = 2" in ddl


def test_table_instance_kwargs():
    class T(Table, table="t"):
        id: UInt64 = Field()
        name: String = Field(default="anon")
        __engine__ = MergeTree(order_by="id")

    row = T(id=42)
    assert row.id == 42
    assert row.name == "anon"


def test_table_missing_required():
    class T(Table, table="t"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

    with pytest.raises(TypeError, match="missing required"):
        T()  # ty: ignore[missing-argument]


def test_table_unknown_kwarg():
    class T(Table, table="t"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

    with pytest.raises(TypeError, match="unexpected keyword"):
        T(id=1, junk=2)  # ty: ignore[unknown-argument]


def test_from_dict_filters_unknown_keys():
    class T(Table, table="t"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

    row = T.from_dict({"id": 1, "extra": "ignored"})
    assert row.id == 1


def test_to_dict_roundtrip():
    class T(Table, table="t"):
        id: UInt64 = Field()
        name: String = Field()
        __engine__ = MergeTree(order_by="id")

    row = T(id=1, name="x")
    assert row.to_dict() == {"id": 1, "name": "x"}


# =============================================================================
# DDL helpers
# =============================================================================


def test_drop_table_ddl():
    assert build_drop_table("t") == "DROP TABLE t"
    assert build_drop_table("t", if_exists=True) == "DROP TABLE IF EXISTS t"
    assert "ON CLUSTER c" in build_drop_table("t", on_cluster="c")
    assert build_drop_table("t", sync=True).endswith(" SYNC")


def test_truncate_table_ddl():
    assert build_truncate_table("t") == "TRUNCATE TABLE t"


def test_rename_table_ddl():
    assert build_rename_table("a", "b") == "RENAME TABLE a TO b"
    assert build_rename_table("a", "b", on_cluster="c").endswith("ON CLUSTER c")


def test_exchange_tables_ddl():
    assert build_exchange_tables("a", "b") == "EXCHANGE TABLES a AND b"
    assert build_exchange_tables("a", "b", on_cluster="c").endswith("ON CLUSTER c")


def test_optimize_table_ddl():
    assert build_optimize_table("t") == "OPTIMIZE TABLE t"
    assert build_optimize_table("t", final=True) == "OPTIMIZE TABLE t FINAL"
    assert (
        build_optimize_table("t", partition="202401")
        == "OPTIMIZE TABLE t PARTITION 202401"
    )
    assert build_optimize_table("t", deduplicate=True).endswith("DEDUPLICATE")
    assert build_optimize_table(
        "t", deduplicate=True, deduplicate_by=["a", "b"]
    ).endswith("DEDUPLICATE BY a, b")
    assert "ON CLUSTER c" in build_optimize_table("t", on_cluster="c")
    assert (
        build_optimize_table("t", on_cluster="c", partition="202401", final=True)
        == "OPTIMIZE TABLE t ON CLUSTER c PARTITION 202401 FINAL"
    )


def test_create_database_ddl():
    assert build_create_database("db") == "CREATE DATABASE db"
    assert (
        build_create_database("db", if_not_exists=True, engine="Atomic")
        == "CREATE DATABASE IF NOT EXISTS db ENGINE = Atomic"
    )


def test_create_view():
    assert build_create_view("v", "SELECT 1") == "CREATE VIEW v AS SELECT 1"
    assert "IF NOT EXISTS" in build_create_view("v", "SELECT 1", if_not_exists=True)


def test_create_materialized_view_with_to():
    sql = build_create_materialized_view(
        "mv",
        "SELECT count() FROM src",
        to="dst_table",
    )
    assert sql == "CREATE MATERIALIZED VIEW mv TO dst_table AS SELECT count() FROM src"


def test_create_materialized_view_inline_engine():
    sql = build_create_materialized_view(
        "mv",
        "SELECT count() FROM src",
        engine="ENGINE = MergeTree() ORDER BY tuple()",
        populate=True,
    )
    expected_prefix = (
        "CREATE MATERIALIZED VIEW mv "
        "ENGINE = MergeTree() ORDER BY tuple() POPULATE AS SELECT"
    )
    assert expected_prefix in sql


def test_create_dictionary():
    sql = build_create_dictionary(
        "users_dict",
        {"id": "UInt64", "name": "String"},
        primary_key="id",
        source="HTTP(url 'http://x' format 'JSON')",
        layout="LAYOUT(HASHED())",
        lifetime=(300, 600),
    )
    assert "CREATE DICTIONARY users_dict" in sql
    assert "PRIMARY KEY id" in sql
    assert "SOURCE(HTTP" in sql
    assert "LAYOUT(HASHED())" in sql
    assert "LIFETIME(MIN 300 MAX 600)" in sql


def test_replicated_engine_render_in_ddl():
    class T(
        Table,
        table="t",
        engine=ReplicatedMergeTree("/clickhouse/{shard}/t", "{replica}"),
    ):
        x: Int64 = Field()

    ddl = T.to_ddl()
    assert "ReplicatedMergeTree" in ddl
    assert "{shard}" in ddl


def test_table_engine_via_kwarg():
    class T(Table, table="t", engine=MergeTree(order_by="id")):
        id: Int64 = Field()

    assert "ENGINE = MergeTree" in T.to_ddl()
    assert "ORDER BY id" in T.to_ddl()
