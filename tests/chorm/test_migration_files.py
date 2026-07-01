"""Tests for chorm migration file layout, loader, and reverse diff."""

from __future__ import annotations

import sys
from pathlib import Path

from derp.chorm import Field, MergeTree, String, Table, UInt64
from derp.chorm.loader import discover_tables
from derp.chorm.migrations import diff_down, diff_snapshots, snapshot_from_tables
from derp.chorm.migrations.files import (
    FileJournal,
    read_down_sql,
    read_latest_snapshot,
    read_up_sql,
    render_sql,
    slugify,
    split_sql,
    write_migration,
)


class Event(Table, table="events"):
    id: UInt64 = Field()
    name: String = Field()
    __engine__ = MergeTree(order_by="id")


# -- loader ------------------------------------------------------------------


def test_chorm_loader_finds_only_chorm_tables(tmp_path: Path) -> None:
    schema = tmp_path / "ch_loader_schema.py"
    schema.write_text(
        "from derp.chorm import Field, MergeTree, String, Table, UInt64\n"
        "from derp.orm import Field as PgField, Serial, Text\n"
        "from derp.orm import Table as PgTable\n"
        "\n"
        "class Hit(Table, table='hits'):\n"
        "    id: UInt64 = Field()\n"
        "    path: String = Field()\n"
        "    __engine__ = MergeTree(order_by='id')\n"
        "\n"
        "class PgUser(PgTable, table='users'):\n"
        "    id: Serial = PgField(primary=True)\n"
        "    name: Text = PgField()\n"
    )
    try:
        tables = discover_tables(str(schema))
    finally:
        sys.modules.pop("ch_loader_schema", None)

    assert [t.__name__ for t in tables] == ["Hit"]


# -- render / split ----------------------------------------------------------


def test_render_split_round_trip() -> None:
    new = snapshot_from_tables([Event])
    statements = diff_snapshots(snapshot_from_tables([]), new)
    text = render_sql(statements)
    assert split_sql(text) == [s.to_sql() for s in statements]


def test_slugify() -> None:
    assert slugify("Add Events Table!") == "add_events_table"
    assert slugify("  ") == "migration"


# -- reverse diff ------------------------------------------------------------


def test_diff_down_reverses_create() -> None:
    new = snapshot_from_tables([Event])
    empty = snapshot_from_tables([])

    forward = diff_snapshots(empty, new)
    assert [s.kind() for s in forward] == ["create"]

    down = diff_down(empty, new)
    assert [s.kind() for s in down] == ["drop"]
    assert "events" in down[0].to_sql()


# -- file journal + folders --------------------------------------------------


def test_file_journal_round_trip(tmp_path: Path) -> None:
    journal = FileJournal.load(tmp_path)
    assert journal.entries == []
    assert journal.next_version() == "0000"

    entry = journal.append("initial")
    assert entry.version == "0000"
    assert entry.dirname == "0000_initial"
    journal.save(tmp_path)

    reloaded = FileJournal.load(tmp_path)
    assert len(reloaded.entries) == 1
    latest = reloaded.latest()
    assert latest is not None
    assert latest.name == "initial"
    assert reloaded.next_version() == "0001"


def test_write_and_read_migration(tmp_path: Path) -> None:
    new = snapshot_from_tables([Event])
    forward = diff_snapshots(snapshot_from_tables([]), new)
    down = diff_down(snapshot_from_tables([]), new)

    journal = FileJournal.load(tmp_path)
    entry = journal.append("initial")
    write_migration(
        tmp_path,
        entry,
        up_sql=render_sql(forward),
        down_sql=render_sql(down),
        snapshot=new,
    )
    journal.save(tmp_path)

    assert "CREATE TABLE" in read_up_sql(tmp_path, entry).upper()
    assert "DROP TABLE" in read_down_sql(tmp_path, entry).upper()

    latest = read_latest_snapshot(tmp_path, FileJournal.load(tmp_path))
    assert latest.table_map().keys() == {"events"}


def test_read_latest_snapshot_empty_when_no_journal(tmp_path: Path) -> None:
    snap = read_latest_snapshot(tmp_path, FileJournal.load(tmp_path))
    assert snap.tables == []
