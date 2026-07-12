"""Indexes must survive introspection unchanged, or every push rebuilds them.

``serialize_table`` always emits ``column_specs``; introspection never did, so
``_index_definitions_equal`` saw a difference for *every* declared index and
planned DROP + CREATE on each push. Expression indexes were missing from
introspection entirely, INCLUDE columns were reported as key columns, and
collation / opclass / NULLS NOT DISTINCT were dropped on the floor.
"""

from __future__ import annotations

import shutil

import pytest

from derp.orm.migrations.snapshot.normalize import PostgresNormalizer

_needs_postgres = pytest.mark.skipif(
    shutil.which("initdb") is None, reason="requires a local PostgreSQL install"
)

_DDL = [
    "CREATE TABLE users (id text primary key, email text, age int, bio text)",
    "CREATE INDEX i_plain ON users (email)",
    "CREATE INDEX i_desc ON users (age DESC)",
    "CREATE INDEX i_nulls_first ON users (age NULLS FIRST)",
    "CREATE INDEX i_desc_nulls_last ON users (age DESC NULLS LAST)",
    'CREATE INDEX i_coll ON users (id COLLATE "C")',
    "CREATE INDEX i_opclass ON users (email text_pattern_ops)",
    "CREATE INDEX i_expr ON users (lower(email))",
    "CREATE INDEX i_multi ON users (email, age DESC)",
    "CREATE INDEX i_include ON users (email) INCLUDE (bio)",
    "CREATE INDEX i_partial ON users (age) WHERE age > 18",
    "CREATE UNIQUE INDEX i_unique ON users (bio)",
    "CREATE UNIQUE INDEX i_nnd ON users (age) NULLS NOT DISTINCT",
    "CREATE INDEX i_hash ON users USING hash (email)",
]

_ALL = {
    "i_plain",
    "i_desc",
    "i_nulls_first",
    "i_desc_nulls_last",
    "i_coll",
    "i_opclass",
    "i_expr",
    "i_multi",
    "i_include",
    "i_partial",
    "i_unique",
    "i_nnd",
    "i_hash",
}


async def _introspect(database_url: str):
    import asyncpg

    from derp.orm.migrations.introspect.postgres import PostgresIntrospector

    pool = await asyncpg.create_pool(database_url, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            for sql in _DDL:
                await conn.execute(sql)
        snapshot = await PostgresIntrospector(pool).introspect(
            schemas=["public"], exclude_tables=[]
        )
        return snapshot.tables["users"].indexes
    finally:
        async with pool.acquire() as conn:
            await conn.execute("DROP TABLE IF EXISTS users CASCADE")
        await pool.close()


@_needs_postgres
class TestIntrospectsEveryIndex:
    async def test_no_index_is_missing(self, database_url: str):
        indexes = await _introspect(database_url)
        assert _ALL - set(indexes) == set()

    async def test_expression_index_is_captured(self, database_url: str):
        idx = (await _introspect(database_url))["i_expr"]
        [spec] = idx.column_specs
        assert spec.name is None
        assert spec.expression == "lower(email)"

    async def test_include_columns_are_not_key_columns(self, database_url: str):
        idx = (await _introspect(database_url))["i_include"]
        assert idx.columns == ["email"]
        assert idx.include == ["bio"]

    async def test_collation_is_captured(self, database_url: str):
        [spec] = (await _introspect(database_url))["i_coll"].column_specs
        assert spec.collation == "C"

    async def test_opclass_is_captured(self, database_url: str):
        [spec] = (await _introspect(database_url))["i_opclass"].column_specs
        assert spec.opclass == "text_pattern_ops"

    async def test_default_opclass_is_not_captured(self, database_url: str):
        """Otherwise the ORM (which omits it) would never compare equal."""
        [spec] = (await _introspect(database_url))["i_plain"].column_specs
        assert spec.opclass is None

    async def test_default_collation_is_not_captured(self, database_url: str):
        [spec] = (await _introspect(database_url))["i_plain"].column_specs
        assert spec.collation is None

    async def test_nulls_not_distinct_is_captured(self, database_url: str):
        indexes = await _introspect(database_url)
        assert indexes["i_nnd"].nulls_not_distinct is True
        assert indexes["i_unique"].nulls_not_distinct is False

    async def test_method_is_captured(self, database_url: str):
        assert (await _introspect(database_url))["i_hash"].method.value == "hash"

    async def test_partial_predicate_is_captured(self, database_url: str):
        assert (await _introspect(database_url))["i_partial"].where is not None

    async def test_multi_column_order_is_preserved(self, database_url: str):
        idx = (await _introspect(database_url))["i_multi"]
        assert idx.columns == ["email", "age"]
        assert [s.name for s in idx.column_specs] == ["email", "age"]


@_needs_postgres
class TestNormalizedSortDefaults:
    """ASC and the collation-implied NULLS position are defaults. The ORM omits
    them; PostgreSQL always reports them. They must canonicalize to the same
    thing or an index declared ``Index("age", order="DESC")`` churns forever.
    """

    @staticmethod
    def _spec(indexes, name):
        normalizer = PostgresNormalizer()
        [spec] = normalizer.normalize_index(indexes[name]).column_specs
        return spec

    async def test_ascending_normalizes_to_none(self, database_url: str):
        spec = self._spec(await _introspect(database_url), "i_plain")
        assert spec.order is None
        assert spec.nulls is None

    async def test_desc_keeps_order_but_drops_implied_nulls_first(
        self, database_url: str
    ):
        spec = self._spec(await _introspect(database_url), "i_desc")
        assert spec.order == "DESC"
        assert spec.nulls is None

    async def test_explicit_nulls_first_on_asc_is_kept(self, database_url: str):
        spec = self._spec(await _introspect(database_url), "i_nulls_first")
        assert spec.order is None
        assert spec.nulls == "FIRST"

    async def test_explicit_nulls_last_on_desc_is_kept(self, database_url: str):
        spec = self._spec(await _introspect(database_url), "i_desc_nulls_last")
        assert spec.order == "DESC"
        assert spec.nulls == "LAST"


@_needs_postgres
class TestPushIsIdempotent:
    """The bug that started this: a second push must plan nothing."""

    @staticmethod
    def _schema():
        from derp.orm import UUID, Field, Index, IndexColumn, Table, Text

        class Doc(Table, table="docs"):
            id: UUID = Field(primary=True)
            slug: Text = Field()
            title: Text = Field()
            body: Text = Field()

            @classmethod
            def indexes(cls):
                return [
                    Index("slug"),
                    Index("title", "slug", unique=True),
                    Index(IndexColumn("slug", collation="C"), name="idx_docs_slug_c"),
                    Index(
                        IndexColumn("title", opclass="text_pattern_ops"),
                        name="idx_docs_title_pat",
                    ),
                    Index(
                        IndexColumn("title", order="DESC"), name="idx_docs_title_desc"
                    ),
                    Index("body", include=("title",), name="idx_docs_body_incl"),
                    Index(expression="lower(slug)", name="idx_docs_lower_slug"),
                ]

        return Doc

    async def _plan(self, pool, table):
        from derp.orm.migrations.introspect.postgres import PostgresIntrospector
        from derp.orm.migrations.snapshot.differ import SnapshotDiffer
        from derp.orm.migrations.snapshot.normalize import get_normalizer
        from derp.orm.migrations.snapshot.serializer import serialize_schema

        live = await PostgresIntrospector(pool).introspect(
            schemas=["public"], exclude_tables=[]
        )
        n = get_normalizer("postgresql")
        return SnapshotDiffer(
            n.normalize(live), n.normalize(serialize_schema([table]))
        ).diff()

    async def test_second_push_plans_nothing(self, database_url: str):
        import asyncpg

        from derp.orm.migrations.convertors import ConvertorRegistry

        table = self._schema()
        pool = await asyncpg.create_pool(database_url, min_size=1, max_size=2)
        registry = ConvertorRegistry()
        try:
            for stmt in await self._plan(pool, table):
                async with pool.acquire() as conn:
                    await conn.execute(registry.convert(stmt))

            replan = await self._plan(pool, table)
            assert replan == [], [registry.convert(s) for s in replan]
        finally:
            async with pool.acquire() as conn:
                await conn.execute("DROP TABLE IF EXISTS docs CASCADE")
            await pool.close()


class TestIndexColumnCoercion:
    """``IndexColumn`` takes the same loose input ``Index`` does."""

    def test_order_string_is_coerced(self):
        from derp.orm import IndexColumn, SortOrder

        assert IndexColumn("a", order="DESC").order is SortOrder.DESC

    def test_nulls_string_is_coerced(self):
        from derp.orm import IndexColumn, NullsPosition

        assert IndexColumn("a", nulls="FIRST").nulls is NullsPosition.FIRST

    def test_invalid_order_raises_at_the_call_site(self):
        from derp.orm import IndexColumn

        with pytest.raises(ValueError):
            IndexColumn("a", order="SIDEWAYS")  # ty: ignore[invalid-argument-type]

    def test_enum_members_pass_through(self):
        from derp.orm import IndexColumn, SortOrder

        assert IndexColumn("a", order=SortOrder.ASC).order is SortOrder.ASC
