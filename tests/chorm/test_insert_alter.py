"""Tests for INSERT, ALTER, UPDATE and DELETE builders."""

from __future__ import annotations

import pytest

from derp.chorm import (
    AlterQuery,
    Codec,
    DateTime,
    DeleteMutation,
    Field,
    Fn,
    InsertQuery,
    MergeTree,
    SelectQuery,
    String,
    Table,
    UInt64,
    UpdateMutation,
)


class Event(Table, table="events"):
    id: UInt64 = Field()
    user_id: UInt64 = Field()
    type: String = Field()
    ts: DateTime = Field(default=Fn.now())
    __engine__ = MergeTree(order_by=("user_id", "ts"))


# =============================================================================
# INSERT
# =============================================================================


def test_insert_single_row():
    q = InsertQuery(None, Event).values(id=1, user_id=42, type="click", ts="2024-01-01")
    sql, vals = q.build()
    assert sql.startswith("INSERT INTO events (")
    assert "VALUES (" in sql
    assert vals == {
        "p1": 1,
        "p2": 42,
        "p3": "click",
        "p4": "2024-01-01",
    }


def test_insert_multi_row():
    q = InsertQuery(None, Event).values_list(
        [
            {"id": 1, "user_id": 1, "type": "click", "ts": "2024-01-01"},
            {"id": 2, "user_id": 2, "type": "view", "ts": "2024-01-02"},
        ]
    )
    sql, vals = q.build()
    assert sql.count("(") >= 3  # column list + 2 row tuples
    assert len(vals) == 8


def test_insert_from_select():
    sub = SelectQuery(None, (Event.id, Event.user_id, Event.type, Event.ts))
    q = (
        InsertQuery(None, Event)
        .columns(Event.id, Event.user_id, Event.type, Event.ts)
        .from_select(sub)
    )
    sql, _ = q.build()
    assert "INSERT INTO events" in sql
    assert " SELECT " in sql


def test_insert_with_settings():
    q = (
        InsertQuery(None, Event)
        .values(id=1, user_id=1, type="x", ts="t")
        .settings(
            async_insert=1,
            wait_for_async_insert=1,
        )
    )
    sql, _ = q.build()
    assert "SETTINGS async_insert = 1, wait_for_async_insert = 1" in sql


def test_insert_requires_values():
    with pytest.raises(ValueError, match="requires values"):
        InsertQuery(None, Event).build()


# =============================================================================
# ALTER
# =============================================================================


def test_alter_add_column():
    q = AlterQuery(None, Event).add_column("extra", "String", default="''")
    sql, _ = q.build()
    assert "ADD COLUMN `extra` String DEFAULT ''" in sql


def test_alter_add_column_if_not_exists_with_position():
    q = AlterQuery(None, Event).add_column(
        "extra", "Int64", if_not_exists=True, after="user_id"
    )
    sql, _ = q.build()
    assert "IF NOT EXISTS" in sql
    assert "AFTER `user_id`" in sql


def test_alter_add_column_first():
    q = AlterQuery(None, Event).add_column("extra", "Int64", first=True)
    sql, _ = q.build()
    assert sql.endswith(" FIRST")


def test_alter_drop_column():
    q = AlterQuery(None, Event).drop_column("extra")
    sql, _ = q.build()
    assert sql == "ALTER TABLE events DROP COLUMN `extra`"


def test_alter_drop_column_if_exists():
    q = AlterQuery(None, Event).drop_column("x", if_exists=True)
    sql, _ = q.build()
    assert "IF EXISTS" in sql


def test_alter_modify_column():
    q = AlterQuery(None, Event).modify_column("type", "LowCardinality(String)")
    sql, _ = q.build()
    assert "MODIFY COLUMN `type` LowCardinality(String)" in sql


def test_alter_rename_column():
    q = AlterQuery(None, Event).rename_column("type", "kind")
    sql, _ = q.build()
    assert "RENAME COLUMN `type` TO `kind`" in sql


def test_alter_comment_column():
    q = AlterQuery(None, Event).comment_column("type", "the event kind")
    sql, _ = q.build()
    assert "COMMENT COLUMN `type` 'the event kind'" in sql


def test_alter_clear_column():
    q = AlterQuery(None, Event).clear_column("type", in_partition="202401")
    sql, _ = q.build()
    assert "CLEAR COLUMN `type` IN PARTITION 202401" in sql


def test_alter_add_index():
    q = AlterQuery(None, Event).add_index(
        "INDEX `t_idx` type TYPE set(0) GRANULARITY 1"
    )
    sql, _ = q.build()
    assert "ADD INDEX `t_idx`" in sql


def test_alter_drop_index():
    q = AlterQuery(None, Event).drop_index("t_idx", if_exists=True)
    sql, _ = q.build()
    assert "DROP INDEX IF EXISTS `t_idx`" in sql


def test_alter_materialize_index():
    q = AlterQuery(None, Event).materialize_index("t_idx")
    sql, _ = q.build()
    assert "MATERIALIZE INDEX `t_idx`" in sql


def test_alter_materialize_ttl():
    q = AlterQuery(None, Event).materialize_ttl()
    sql, _ = q.build()
    assert "MATERIALIZE TTL" in sql


def test_alter_materialize_ttl_in_partition():
    q = AlterQuery(None, Event).materialize_ttl(in_partition="202401")
    sql, _ = q.build()
    assert "MATERIALIZE TTL IN PARTITION 202401" in sql


def test_alter_materialize_column():
    q = AlterQuery(None, Event).materialize_column("type")
    sql, _ = q.build()
    assert "MATERIALIZE COLUMN `type`" in sql


def test_alter_materialize_column_in_partition():
    q = AlterQuery(None, Event).materialize_column("type", in_partition="202401")
    sql, _ = q.build()
    assert "MATERIALIZE COLUMN `type` IN PARTITION 202401" in sql


def test_alter_modify_ttl():
    q = AlterQuery(None, Event).modify_ttl("ts + INTERVAL 7 DAY")
    sql, _ = q.build()
    assert "MODIFY TTL ts + INTERVAL 7 DAY" in sql


def test_alter_remove_ttl():
    q = AlterQuery(None, Event).remove_ttl()
    sql, _ = q.build()
    assert "REMOVE TTL" in sql


def test_alter_modify_setting():
    q = AlterQuery(None, Event).modify_setting(index_granularity=4096)
    sql, _ = q.build()
    assert "MODIFY SETTING index_granularity = 4096" in sql


def test_alter_reset_setting():
    q = AlterQuery(None, Event).reset_setting("merge_with_ttl_timeout")
    sql, _ = q.build()
    assert "RESET SETTING merge_with_ttl_timeout" in sql


def test_alter_partition_operations():
    q = (
        AlterQuery(None, Event)
        .detach_partition("202401")
        .attach_partition("202401")
        .drop_partition("202312")
    )
    sql, _ = q.build()
    assert "DETACH PARTITION" in sql
    assert "ATTACH PARTITION" in sql
    assert "DROP PARTITION" in sql


def test_alter_freeze_partition():
    q = AlterQuery(None, Event).freeze_partition("202401", name="backup1")
    sql, _ = q.build()
    assert "FREEZE PARTITION 202401 WITH NAME 'backup1'" in sql


def test_alter_move_partition():
    q = AlterQuery(None, Event).move_partition("202401", to="DISK 'cold'")
    sql, _ = q.build()
    assert "MOVE PARTITION 202401 TO DISK 'cold'" in sql


def test_alter_with_codec():
    q = AlterQuery(None, Event).modify_column("type", "String", codec=Codec("ZSTD", 9))
    sql, _ = q.build()
    assert "CODEC(ZSTD(9))" in sql


def test_alter_on_cluster():
    q = AlterQuery(None, Event, on_cluster="c").drop_column("type")
    sql, _ = q.build()
    assert "ON CLUSTER c" in sql


def test_alter_multiple_actions():
    q = (
        AlterQuery(None, Event)
        .add_column("a", "Int64")
        .add_column("b", "String")
        .drop_column("type")
    )
    sql, _ = q.build()
    # All three actions are comma-joined on a single ALTER TABLE.
    assert sql.startswith("ALTER TABLE events ADD COLUMN")
    assert sql.count("ADD COLUMN") == 2
    assert "DROP COLUMN" in sql


def test_alter_requires_actions():
    with pytest.raises(ValueError, match="at least one action"):
        AlterQuery(None, Event).build()


# =============================================================================
# UPDATE mutation
# =============================================================================


def test_update_mutation():
    q = UpdateMutation(None, Event).set(type="archived").where(Event.id == 1)
    sql, vals = q.build()
    assert sql.startswith("ALTER TABLE events UPDATE `type` = ")
    assert "WHERE" in sql
    assert vals["p1"] == "archived"


def test_update_in_partition():
    q = (
        UpdateMutation(None, Event)
        .set(type="x")
        .in_partition("202401")
        .where(Event.id == 1)
    )
    sql, _ = q.build()
    assert "IN PARTITION 202401" in sql


def test_update_requires_where():
    q = UpdateMutation(None, Event).set(type="x")
    with pytest.raises(ValueError, match="WHERE"):
        q.build()


def test_update_requires_set():
    q = UpdateMutation(None, Event).where(Event.id == 1)
    with pytest.raises(ValueError, match="set"):
        q.build()


def test_update_with_settings():
    q = (
        UpdateMutation(None, Event)
        .set(type="x")
        .where(Event.id == 1)
        .settings(mutations_sync=2)
    )
    sql, _ = q.build()
    assert "SETTINGS mutations_sync = 2" in sql


# =============================================================================
# DELETE mutation (and lightweight)
# =============================================================================


def test_delete_mutation_default():
    q = DeleteMutation(None, Event).where(Event.id == 1)
    sql, _ = q.build()
    assert sql.startswith("ALTER TABLE events DELETE WHERE")


def test_delete_lightweight():
    q = DeleteMutation(None, Event, lightweight=True).where(Event.id == 1)
    sql, _ = q.build()
    assert sql.startswith("DELETE FROM events WHERE")


def test_delete_in_partition():
    q = DeleteMutation(None, Event).in_partition("202401").where(Event.id == 1)
    sql, _ = q.build()
    assert "IN PARTITION 202401" in sql


def test_delete_requires_where():
    with pytest.raises(ValueError, match="WHERE"):
        DeleteMutation(None, Event).build()


def test_delete_on_cluster():
    q = DeleteMutation(None, Event, on_cluster="c", lightweight=True).where(
        Event.id == 1
    )
    sql, _ = q.build()
    assert "ON CLUSTER c" in sql
