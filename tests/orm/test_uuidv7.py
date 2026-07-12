"""Tests for UUIDv7 server-side defaults."""

from __future__ import annotations

from derp.orm import UUID, Field, Fn, Table
from derp.orm.migrations.snapshot.normalize import PostgresNormalizer
from derp.orm.migrations.snapshot.serializer import serialize_table


class Event(Table, table="events"):
    id: UUID = Field(primary=True, default=Fn.uuidv7())


class TestUuidv7Fn:
    def test_emits_bare_sql_call(self):
        assert Fn.uuidv7() == "uuidv7()"

    def test_column_type_is_plain_uuid(self):
        """v7 is a property of the value, not a distinct PostgreSQL type."""
        assert Event.get_columns()["id"].sql_type() == "UUID"


class TestUuidv7DDL:
    def test_default_is_not_quoted(self):
        ddl = Event.to_ddl()
        assert "id UUID PRIMARY KEY DEFAULT uuidv7()" in ddl
        assert "'uuidv7()'" not in ddl


class TestUuidv7Snapshot:
    def test_default_round_trips_as_sql_expression(self):
        col = serialize_table(Event).columns["id"]
        assert col.type == "uuid"
        assert col.default == "uuidv7()"

    def test_normalizes_to_match_live_introspection(self):
        """PostgreSQL 18 reports the default via ``pg_get_expr`` as
        ``uuidv7()``; the serialized form must normalize to the same string
        or every ``derp push`` would re-plan an ALTER COLUMN SET DEFAULT."""
        normalized = PostgresNormalizer().normalize_column(
            serialize_table(Event).columns["id"]
        )
        assert normalized.default == "uuidv7()"
