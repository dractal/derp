"""Column collation: emitted at create time, never altered in place.

``COLLATE "C"`` gives byte-wise comparison on opaque text columns. Changing a
column's collation after the fact rewrites it under an ACCESS EXCLUSIVE lock,
rebuilds every index whose key includes it, and silently changes comparison
semantics for existing rows. derp therefore emits collation in CREATE TABLE and
ADD COLUMN, and refuses to diff a change onto an existing column.
"""

from __future__ import annotations

import shutil

import pytest

from derp.orm import UUID, Field, Table, Text
from derp.orm.migrations.errors import SchemaError
from derp.orm.migrations.snapshot.differ import SnapshotDiffer, columns_match
from derp.orm.migrations.snapshot.models import (
    ColumnSnapshot,
    PrimaryKeySnapshot,
    SchemaSnapshot,
    TableSnapshot,
)
from derp.orm.migrations.snapshot.normalize import PostgresNormalizer
from derp.orm.migrations.snapshot.serializer import serialize_schema, serialize_table
from derp.orm.migrations.statements.types import (
    AddColumnStatement,
    CreateTableStatement,
)

_needs_postgres = pytest.mark.skipif(
    shutil.which("initdb") is None, reason="requires a local PostgreSQL install"
)


class Doc(Table, table="docs"):
    id: UUID = Field(primary=True)
    key: Text = Field(collate="C")
    title: Text = Field()


def _table(name: str, collation: str | None) -> TableSnapshot:
    return TableSnapshot(
        name=name,
        columns={
            "id": ColumnSnapshot(name="id", type="uuid", primary_key=True),
            "key": ColumnSnapshot(name="key", type="text", collation=collation),
        },
        primary_key=PrimaryKeySnapshot(columns=["id"]),
    )


def _schema(name: str, collation: str | None) -> SchemaSnapshot:
    return SchemaSnapshot(tables={name: _table(name, collation)})


class TestDeclaration:
    def test_column_exposes_collation(self):
        assert Doc.get_columns()["key"].collation == "C"

    def test_absent_collation_is_none(self):
        assert Doc.get_columns()["title"].collation is None

    def test_empty_collation_rejected(self):
        with pytest.raises(ValueError, match="non-empty collation"):
            Field(collate="  ")


class TestSerialization:
    def test_collation_reaches_the_snapshot(self):
        assert serialize_table(Doc).columns["key"].collation == "C"

    def test_uncollated_column_is_none(self):
        assert serialize_table(Doc).columns["title"].collation is None

    def test_create_table_statement_carries_collation(self):
        stmts = SnapshotDiffer(SchemaSnapshot(), serialize_schema([Doc])).diff()
        [create] = [s for s in stmts if isinstance(s, CreateTableStatement)]
        by_name = {c.name: c for c in create.columns}
        assert by_name["key"].collation == "C"
        assert by_name["title"].collation is None


class TestDDL:
    def test_create_table_emits_collate_after_the_type(self):
        assert 'key TEXT COLLATE "C" NOT NULL' in Doc.to_ddl()

    def test_uncollated_column_has_no_collate(self):
        assert "title TEXT NOT NULL" in Doc.to_ddl()

    def test_create_table_convertor_emits_collate(self):
        from derp.orm.migrations.convertors import ConvertorRegistry

        stmts = SnapshotDiffer(SchemaSnapshot(), serialize_schema([Doc])).diff()
        [create] = [s for s in stmts if isinstance(s, CreateTableStatement)]
        sql = ConvertorRegistry().convert(create)
        assert '"key" TEXT COLLATE "C" NOT NULL' in sql

    def test_add_column_emits_collate(self):
        """Adding a collated column to an existing table is free — no rewrite."""
        from derp.orm.migrations.convertors import ConvertorRegistry

        old = SchemaSnapshot(
            tables={
                "docs": TableSnapshot(
                    name="docs",
                    columns={
                        "id": ColumnSnapshot(name="id", type="uuid", primary_key=True)
                    },
                    primary_key=PrimaryKeySnapshot(columns=["id"]),
                )
            }
        )
        stmts = SnapshotDiffer(old, _schema("docs", "C")).diff()
        [add] = [s for s in stmts if isinstance(s, AddColumnStatement)]
        sql = ConvertorRegistry().convert(add)
        assert 'ADD COLUMN "key" TEXT COLLATE "C"' in sql


class TestNoInPlaceAlter:
    def test_adding_collation_to_existing_column_raises(self):
        with pytest.raises(SchemaError, match=r"docs\.key.*collation"):
            SnapshotDiffer(_schema("docs", None), _schema("docs", "C")).diff()

    def test_changing_collation_raises(self):
        with pytest.raises(SchemaError, match=r"docs\.key.*collation"):
            SnapshotDiffer(_schema("docs", "C"), _schema("docs", "POSIX")).diff()

    def test_error_explains_the_remedy(self):
        with pytest.raises(SchemaError, match="cannot be changed in place"):
            SnapshotDiffer(_schema("docs", None), _schema("docs", "C")).diff()

    def test_undeclared_collation_leaves_the_column_alone(self):
        """A column the schema says nothing about is not derp's to manage.

        Otherwise adopting derp on a database with any hand-collated column
        would block every unrelated migration.
        """
        assert SnapshotDiffer(_schema("docs", "C"), _schema("docs", None)).diff() == []

    def test_unchanged_collation_plans_nothing(self):
        assert SnapshotDiffer(_schema("docs", "C"), _schema("docs", "C")).diff() == []

    def test_new_table_with_collation_is_fine(self):
        stmts = SnapshotDiffer(SchemaSnapshot(), _schema("docs", "C")).diff()
        assert any(isinstance(s, CreateTableStatement) for s in stmts)

    def test_collation_difference_blocks_a_rename_match(self):
        """Otherwise a rename would smuggle a collation change past the guard."""
        old = ColumnSnapshot(name="a", type="text", collation=None)
        new = ColumnSnapshot(name="b", type="text", collation="C")
        assert columns_match(old, new) is False


class TestNormalizerTreatsDefaultAsAbsent:
    def test_default_collation_normalizes_to_none(self):
        col = ColumnSnapshot(name="k", type="text", collation="default")
        assert PostgresNormalizer().normalize_column(col).collation is None

    def test_real_collation_survives(self):
        col = ColumnSnapshot(name="k", type="text", collation="C")
        assert PostgresNormalizer().normalize_column(col).collation == "C"


@_needs_postgres
class TestLivePostgres:
    async def _live(self, pool):
        from derp.orm.migrations.introspect.postgres import PostgresIntrospector

        return await PostgresIntrospector(pool).introspect(
            schemas=["public"], exclude_tables=[]
        )

    async def test_collation_is_introspected(self, database_url: str):
        import asyncpg

        pool = await asyncpg.create_pool(database_url, min_size=1, max_size=2)
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    "CREATE TABLE docs (id uuid primary key, "
                    'key text COLLATE "C", title text)'
                )
            cols = (await self._live(pool)).tables["docs"].columns
            assert cols["key"].collation == "C"
            # An uncollated text column must not report "default", or the
            # serializer (which emits None) would churn against it.
            assert cols["title"].collation is None
            # A non-collatable type must not report a collation either.
            assert cols["id"].collation is None
        finally:
            async with pool.acquire() as conn:
                await conn.execute("DROP TABLE IF EXISTS docs CASCADE")
            await pool.close()

    async def test_push_is_idempotent(self, database_url: str):
        import asyncpg

        from derp.orm.migrations.convertors import ConvertorRegistry
        from derp.orm.migrations.snapshot.normalize import get_normalizer

        pool = await asyncpg.create_pool(database_url, min_size=1, max_size=2)
        registry = ConvertorRegistry()
        n = get_normalizer("postgresql")

        async def plan():
            live = await self._live(pool)
            return SnapshotDiffer(
                n.normalize(live), n.normalize(serialize_schema([Doc]))
            ).diff()

        try:
            for stmt in await plan():
                async with pool.acquire() as conn:
                    await conn.execute(registry.convert(stmt))
            replan = await plan()
            assert replan == [], [registry.convert(s) for s in replan]
        finally:
            async with pool.acquire() as conn:
                await conn.execute("DROP TABLE IF EXISTS docs CASCADE")
            await pool.close()

    async def test_emitted_ddl_actually_collates(self, database_url: str):
        """Not just syntactically valid — the column really sorts byte-wise.

        The throwaway test cluster is initialized with ``C.UTF-8``, so an
        uncollated column already sorts byte-wise there and makes a useless
        control. Contrast against an explicit ICU collation instead, which
        orders ``'a'`` before ``'B'`` in any locale.
        """
        import asyncpg

        from derp.orm.migrations.convertors import ConvertorRegistry

        pool = await asyncpg.create_pool(database_url, min_size=1, max_size=2)
        registry = ConvertorRegistry()
        try:
            for stmt in SnapshotDiffer(
                SchemaSnapshot(), serialize_schema([Doc])
            ).diff():
                async with pool.acquire() as conn:
                    await conn.execute(registry.convert(stmt))

            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO docs (id, key, title) VALUES "
                    "(gen_random_uuid(), 'B', 'B'), (gen_random_uuid(), 'a', 'a')"
                )
                # The column's own collation drives ORDER BY with no COLLATE
                # clause. 'B' (0x42) precedes 'a' (0x61) only under "C".
                byte_order = [
                    r["key"]
                    for r in await conn.fetch("SELECT key FROM docs ORDER BY key")
                ]
                icu_order = [
                    r["key"]
                    for r in await conn.fetch(
                        'SELECT key FROM docs ORDER BY key COLLATE "und-x-icu"'
                    )
                ]
                declared = await conn.fetchval(
                    """
                    SELECT co.collname FROM pg_attribute a
                    LEFT JOIN pg_collation co ON co.oid = a.attcollation
                    WHERE a.attrelid = 'docs'::regclass AND a.attname = 'key'
                    """
                )
            assert byte_order == ["B", "a"]
            assert icu_order == ["a", "B"]
            assert declared == "C"
        finally:
            async with pool.acquire() as conn:
                await conn.execute("DROP TABLE IF EXISTS docs CASCADE")
            await pool.close()
