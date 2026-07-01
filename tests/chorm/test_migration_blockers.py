"""Regression tests for two migration data-safety blockers.

1. ``ch push`` against a live (introspected) database stamped every table with
   a concrete database (``default``) while model snapshots left it unset, so an
   unchanged table keyed differently on each side and the differ emitted a
   spurious DROP + CREATE — silently destroying data on every push.

2. String column defaults were stored verbatim in the snapshot, so a default of
   ``"active"`` rendered as ``DEFAULT active`` (an identifier/expression) instead
   of ``DEFAULT 'active'``, corrupting generated migrations and breaking
   introspection round-trips.
"""

from __future__ import annotations

import copy

from derp.chorm import (
    DateTime,
    Field,
    MergeTree,
    String,
    Table,
    UInt64,
)
from derp.chorm.migrations import (
    SchemaSnapshot,
    diff_snapshots,
    snapshot_from_tables,
)
from derp.chorm.migrations.snapshot import TableSnapshot
from derp.chorm.migrations.statements import AddColumn, CreateTable, DropTable


def _as_introspected(snap: SchemaSnapshot, database: str) -> SchemaSnapshot:
    """Mimic live introspection, which stamps every table with a concrete
    database (model snapshots leave ``database`` unset)."""
    out = copy.deepcopy(snap)
    for t in out.tables:
        t.database = database
    return out


# =============================================================================
# Blocker 1 — push must not drop + recreate an unchanged table
# =============================================================================


def test_push_against_introspected_schema_is_a_noop_when_unchanged():
    class Event(Table, table="events"):
        id: UInt64 = Field()
        status: String = Field(default="active")
        ts: DateTime = Field(default="now()")
        __engine__ = MergeTree(order_by="id")

    model = snapshot_from_tables([Event])
    live = _as_introspected(model, "default")  # what `introspect()` produces

    # Aligning the default database makes an identical schema a true no-op.
    assert diff_snapshots(live, model, default_database="default") == []


def test_unaligned_database_qualifier_would_drop_and_recreate():
    # Pins the hazard the fix addresses: without database normalisation the
    # live (`default.events`) and model (`events`) keys diverge and the differ
    # emits a destructive DropTable + CreateTable for a byte-identical schema.
    class Event(Table, table="events"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

    model = snapshot_from_tables([Event])
    live = _as_introspected(model, "default")

    naive = diff_snapshots(live, model)  # default_database not supplied
    assert any(isinstance(s, DropTable) for s in naive)
    assert any(isinstance(s, CreateTable) for s in naive)


def test_explicit_database_still_distinguishes_tables():
    # The normalisation must not collapse genuinely different databases: a model
    # table in `analytics` is not the same as a live table in `default`.
    live = SchemaSnapshot(tables=[TableSnapshot(name="events", database="default")])

    class Event(Table, table="events", database="analytics"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

    model = snapshot_from_tables([Event])
    stmts = diff_snapshots(live, model, default_database="default")
    assert any(isinstance(s, DropTable) for s in stmts)  # default.events
    assert any(isinstance(s, CreateTable) for s in stmts)  # analytics.events


# =============================================================================
# Blocker 2 — string defaults must be quoted (SQL-ready) in the snapshot
# =============================================================================


def test_string_default_is_quoted_in_snapshot():
    class T(Table, table="t"):
        id: UInt64 = Field()
        status: String = Field(default="active")
        __engine__ = MergeTree(order_by="id")

    col = snapshot_from_tables([T]).table_map()["t"].column_map()["status"]
    assert col.default == "'active'"


def test_function_and_numeric_defaults_pass_through_unquoted():
    class T(Table, table="t"):
        id: UInt64 = Field()
        n: UInt64 = Field(default=42)
        ts: DateTime = Field(default="now()")
        __engine__ = MergeTree(order_by="id")

    cols = snapshot_from_tables([T]).table_map()["t"].column_map()
    assert cols["n"].default == "42"
    assert cols["ts"].default == "now()"


def test_create_from_snapshot_emits_quoted_default():
    class T(Table, table="t"):
        id: UInt64 = Field()
        status: String = Field(default="active")
        __engine__ = MergeTree(order_by="id")

    stmts = diff_snapshots(SchemaSnapshot(tables=[]), snapshot_from_tables([T]))
    sql = stmts[0].to_sql()
    assert "DEFAULT 'active'" in sql
    assert "DEFAULT active" not in sql


def test_add_column_with_string_default_is_quoted():
    class Old(Table, table="t"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

    class New(Table, table="t"):
        id: UInt64 = Field()
        status: String = Field(default="active")
        __engine__ = MergeTree(order_by="id")

    stmts = diff_snapshots(snapshot_from_tables([Old]), snapshot_from_tables([New]))
    add_sql = next(s.to_sql() for s in stmts if isinstance(s, AddColumn))
    assert "DEFAULT 'active'" in add_sql


def test_string_default_round_trips_clean_against_introspected_form():
    # Introspection reads ClickHouse's `default_expression` back already quoted
    # (`'active'`); the model snapshot now stores the same form, so a table with
    # a string default no longer perpetually diffs dirty.
    class T(Table, table="t"):
        id: UInt64 = Field()
        status: String = Field(default="active")
        __engine__ = MergeTree(order_by="id")

    model = snapshot_from_tables([T])
    live = _as_introspected(model, "default")
    assert diff_snapshots(live, model, default_database="default") == []
