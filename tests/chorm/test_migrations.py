"""Tests for migration snapshot, diff, and JSON roundtrip."""

from __future__ import annotations

import pytest

from derp.chorm import (
    Codec,
    DateTime,
    Field,
    Index,
    IndexType,
    Int64,
    MergeTree,
    Nullable,
    Projection,
    ReplacingMergeTree,
    String,
    Table,
    UInt64,
)
from derp.chorm.migrations import (
    SchemaSnapshot,
    UnsupportedSchemaChange,
    diff_snapshots,
    snapshot_from_tables,
)
from derp.chorm.migrations.introspect import _parse_engine_full
from derp.chorm.migrations.statements import (
    AddColumn,
    AddProjection,
    AlterModifyTTL,
    AlterRemoveTTL,
    CreateTable,
    DropColumn,
    DropProjection,
    DropTable,
    ModifyColumn,
    RenameColumn,
)

# =============================================================================
# Snapshot building
# =============================================================================


def test_snapshot_basic():
    class T(Table, table="t"):
        id: UInt64 = Field()
        name: String = Field()
        __engine__ = MergeTree(order_by="id")

    snap = snapshot_from_tables([T])
    assert len(snap.tables) == 1
    t = snap.tables[0]
    assert t.name == "t"
    assert {c.name for c in t.columns} == {"id", "name"}
    assert t.engine is not None
    assert t.engine.name == "MergeTree"
    assert t.engine.order_by == "id"


def test_snapshot_with_codec_and_ttl():
    class T(Table, table="t"):
        ts: DateTime = Field()
        x: Int64 = Field(codec=Codec("ZSTD", 3), ttl="ts + INTERVAL 7 DAY")
        __engine__ = MergeTree(order_by="ts", ttl="ts + INTERVAL 30 DAY")

    snap = snapshot_from_tables([T])
    col = snap.table_map()["t"].column_map()["x"]
    assert col.codec == "ZSTD(3)"
    assert col.ttl == "ts + INTERVAL 7 DAY"
    t = snap.table_map()["t"]
    assert t.engine is not None
    assert t.engine.ttl == "ts + INTERVAL 30 DAY"


def test_snapshot_indexes():
    class T(Table, table="t"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

        @classmethod
        def indexes(cls):
            return [Index(name="i1", expression="id", type=IndexType.MINMAX)]

    snap = snapshot_from_tables([T])
    idx = snap.table_map()["t"].indexes
    assert len(idx) == 1
    assert idx[0].name == "i1"


def test_snapshot_projections():
    class T(Table, table="t"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

        @classmethod
        def projections(cls):
            return [Projection(name="by_id", select="id", order_by="id")]

    snap = snapshot_from_tables([T])
    projections = snap.table_map()["t"].projections
    assert len(projections) == 1
    assert projections[0].name == "by_id"


def test_snapshot_engine_with_args():
    class T(Table, table="t"):
        id: UInt64 = Field()
        v: Int64 = Field()
        __engine__ = ReplacingMergeTree("v", order_by="id")

    snap = snapshot_from_tables([T])
    eng = snap.table_map()["t"].engine
    assert eng is not None
    assert eng.name == "ReplacingMergeTree"
    assert "v" in eng.args


def test_parse_engine_full_includes_ttl_and_settings():
    parsed = _parse_engine_full(
        "ReplacingMergeTree(ver) "
        "PARTITION BY toYYYYMM(ts) "
        "ORDER BY (id, ts) "
        "TTL ts + INTERVAL 7 DAY "
        "SETTINGS index_granularity = 8192"
    )
    assert parsed["name"] == "ReplacingMergeTree"
    assert parsed["args"] == ("ver",)
    assert parsed["ttl"] == "ts + INTERVAL 7 DAY"
    assert parsed["settings"] == {"index_granularity": "8192"}


# =============================================================================
# JSON roundtrip
# =============================================================================


def test_snapshot_json_roundtrip():
    class T(Table, table="t"):
        id: UInt64 = Field()
        name: Nullable[String] = Field()
        __engine__ = MergeTree(
            order_by="id",
            partition_by="toYYYYMM(id)",
            settings={"index_granularity": 8192},
        )

    snap = snapshot_from_tables([T])
    js = snap.to_json()
    reloaded = SchemaSnapshot.from_json(js)
    assert reloaded.to_dict() == snap.to_dict()


# =============================================================================
# Diff: table-level
# =============================================================================


def test_diff_create_table():
    empty = SchemaSnapshot(tables=[])

    class T(Table, table="t"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

    new = snapshot_from_tables([T])
    stmts = diff_snapshots(empty, new)
    assert len(stmts) == 1
    assert isinstance(stmts[0], CreateTable)
    assert "CREATE TABLE t" in stmts[0].to_sql()


def test_diff_drop_table():
    class T(Table, table="t"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

    old = snapshot_from_tables([T])
    new = SchemaSnapshot(tables=[])
    stmts = diff_snapshots(old, new)
    assert len(stmts) == 1
    assert isinstance(stmts[0], DropTable)
    assert stmts[0].is_destructive()


def test_diff_no_changes():
    class T(Table, table="t"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

    snap = snapshot_from_tables([T])
    assert diff_snapshots(snap, snap) == []


# =============================================================================
# Diff: column-level
# =============================================================================


def test_diff_add_column():
    class Old(Table, table="t"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

    class New(Table, table="t"):
        id: UInt64 = Field()
        name: String = Field()
        __engine__ = MergeTree(order_by="id")

    old = snapshot_from_tables([Old])
    new = snapshot_from_tables([New])
    stmts = diff_snapshots(old, new)
    assert len(stmts) == 1
    assert isinstance(stmts[0], AddColumn)
    assert stmts[0].name == "name"


def test_diff_drop_column():
    class Old(Table, table="t"):
        id: UInt64 = Field()
        name: String = Field()
        __engine__ = MergeTree(order_by="id")

    class New(Table, table="t"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

    old = snapshot_from_tables([Old])
    new = snapshot_from_tables([New])
    stmts = diff_snapshots(old, new)
    assert any(isinstance(s, DropColumn) and s.name == "name" for s in stmts)


def test_diff_modify_column_type():
    class Old(Table, table="t"):
        id: UInt64 = Field()
        v: Int64 = Field()
        __engine__ = MergeTree(order_by="id")

    class New(Table, table="t"):
        id: UInt64 = Field()
        v: String = Field()
        __engine__ = MergeTree(order_by="id")

    old = snapshot_from_tables([Old])
    new = snapshot_from_tables([New])
    stmts = diff_snapshots(old, new)
    assert any(isinstance(s, ModifyColumn) for s in stmts)


def test_diff_rename_column_via_hint():
    class Old(Table, table="t"):
        id: UInt64 = Field()
        name: String = Field()
        __engine__ = MergeTree(order_by="id")

    class New(Table, table="t"):
        id: UInt64 = Field()
        full_name: String = Field()
        __engine__ = MergeTree(order_by="id")

    old = snapshot_from_tables([Old])
    new = snapshot_from_tables([New])
    stmts = diff_snapshots(old, new, rename_hints={"t.name": "full_name"})
    assert any(isinstance(s, RenameColumn) and s.old_name == "name" for s in stmts)


# =============================================================================
# Diff: indexes
# =============================================================================


def test_diff_add_drop_index():
    class Old(Table, table="t"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

        @classmethod
        def indexes(cls):
            return [Index(name="i1", expression="id", type=IndexType.MINMAX)]

    class New(Table, table="t"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

        @classmethod
        def indexes(cls):
            return [Index(name="i2", expression="id", type=IndexType.MINMAX)]

    old = snapshot_from_tables([Old])
    new = snapshot_from_tables([New])
    stmts = diff_snapshots(old, new)
    kinds = [type(s).__name__ for s in stmts]
    assert "DropIndex" in kinds
    assert "AddIndex" in kinds


def test_diff_add_drop_projection():
    class Old(Table, table="t"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

        @classmethod
        def projections(cls):
            return [Projection(name="old_p", select="id", order_by="id")]

    class New(Table, table="t"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

        @classmethod
        def projections(cls):
            return [Projection(name="new_p", select="id", order_by="id")]

    old = snapshot_from_tables([Old])
    new = snapshot_from_tables([New])
    stmts = diff_snapshots(old, new)
    assert any(isinstance(s, DropProjection) and s.name == "old_p" for s in stmts)
    assert any(isinstance(s, AddProjection) for s in stmts)


def test_diff_database_qualified_alter():
    class Old(Table, table="t", database="analytics"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

    class New(Table, table="t", database="analytics"):
        id: UInt64 = Field()
        name: String = Field()
        __engine__ = MergeTree(order_by="id")

    stmts = diff_snapshots(snapshot_from_tables([Old]), snapshot_from_tables([New]))
    assert stmts[0].to_sql().startswith("ALTER TABLE analytics.t")


def test_diff_statement_order_is_deterministic():
    # Set-difference iteration order depends on PYTHONHASHSEED; the differ must
    # sort so the rendered migration (and its hash) is stable across processes.
    class Old(Table, table="t"):
        id: UInt64 = Field()
        zulu: String = Field()
        alpha: String = Field()
        mike: String = Field()
        bravo: String = Field()
        __engine__ = MergeTree(order_by="id")

    class New(Table, table="t"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

    stmts = diff_snapshots(snapshot_from_tables([Old]), snapshot_from_tables([New]))
    dropped = [s.name for s in stmts if isinstance(s, DropColumn)]
    assert dropped == sorted(dropped)
    assert dropped == ["alpha", "bravo", "mike", "zulu"]


def test_diff_rejects_unsupported_engine_changes():
    class Old(Table, table="t"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

    class New(Table, table="t"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="tuple()")

    with pytest.raises(UnsupportedSchemaChange, match="ORDER BY"):
        diff_snapshots(snapshot_from_tables([Old]), snapshot_from_tables([New]))


# =============================================================================
# Diff: TTL
# =============================================================================


def test_diff_modify_ttl():
    class Old(Table, table="t"):
        ts: DateTime = Field()
        __engine__ = MergeTree(order_by="ts")

    class New(Table, table="t"):
        ts: DateTime = Field()
        __engine__ = MergeTree(order_by="ts", ttl="ts + INTERVAL 1 DAY")

    old = snapshot_from_tables([Old])
    new = snapshot_from_tables([New])
    stmts = diff_snapshots(old, new)
    assert any(isinstance(s, AlterModifyTTL) for s in stmts)


def test_diff_remove_ttl():
    class Old(Table, table="t"):
        ts: DateTime = Field()
        __engine__ = MergeTree(order_by="ts", ttl="ts + INTERVAL 1 DAY")

    class New(Table, table="t"):
        ts: DateTime = Field()
        __engine__ = MergeTree(order_by="ts")

    old = snapshot_from_tables([Old])
    new = snapshot_from_tables([New])
    stmts = diff_snapshots(old, new)
    assert any(isinstance(s, AlterRemoveTTL) for s in stmts)


# =============================================================================
# Statement SQL rendering
# =============================================================================


def test_add_column_sql():
    s = AddColumn(table="t", name="n", column_sql="`n` Int64")
    assert s.to_sql() == "ALTER TABLE t ADD COLUMN `n` Int64"


def test_drop_column_sql():
    assert (
        DropColumn(table="t", name="n").to_sql()
        == "ALTER TABLE t DROP COLUMN IF EXISTS `n`"
    )


def test_modify_column_sql():
    s = ModifyColumn(table="t", name="n", column_sql="`n` String")
    assert s.to_sql() == "ALTER TABLE t MODIFY COLUMN `n` String"


def test_rename_column_sql():
    s = RenameColumn(table="t", old_name="a", new_name="b")
    assert s.to_sql() == "ALTER TABLE t RENAME COLUMN `a` TO `b`"


def test_alter_modify_ttl_sql():
    s = AlterModifyTTL(table="t", ttl="ts + INTERVAL 1 DAY")
    assert "MODIFY TTL ts + INTERVAL 1 DAY" in s.to_sql()


def test_alter_remove_ttl_sql():
    assert AlterRemoveTTL(table="t").to_sql() == "ALTER TABLE t REMOVE TTL"
