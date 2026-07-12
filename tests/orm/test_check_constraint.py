"""Tests for CHECK constraints: declaration, DDL, snapshot, and push idempotence."""

from __future__ import annotations

import shutil

import pytest

from derp.orm import UUID, Check, Constraint, Field, Integer, Table, Text, Varchar
from derp.orm.migrations.snapshot.differ import SnapshotDiffer
from derp.orm.migrations.snapshot.models import SchemaSnapshot
from derp.orm.migrations.snapshot.serializer import serialize_schema, serialize_table
from derp.orm.migrations.statements.types import CreateTableStatement


class Order(Table, table="orders"):
    id: UUID = Field(primary=True)
    status: Text = Field(check="status IN ('draft', 'live', 'archived')")
    qty: Integer = Field()

    @classmethod
    def constraints(cls):
        return [Check("qty >= 0 AND qty <= 100", name="orders_qty_range")]


class Product(Table, table="products"):
    id: UUID = Field(primary=True)
    tier: Varchar[20] = Field()
    price: Integer = Field()

    @classmethod
    def constraints(cls):
        return [Check("price > 0")]


class TestCheckDeclaration:
    def test_check_is_a_constraint(self):
        assert isinstance(Check("x > 0"), Constraint)

    def test_requires_an_expression(self):
        with pytest.raises(ValueError, match="non-empty expression"):
            Check("   ")

    def test_auto_name_follows_postgres_convention(self):
        assert Check("price > 0").auto_name("products") == "products_check"

    def test_explicit_name_wins(self):
        assert Check("price > 0", name="pos_price").auto_name("products") == "pos_price"

    def test_to_ddl(self):
        assert (
            Check("price > 0", name="pos").to_ddl("products")
            == "CONSTRAINT pos CHECK (price > 0)"
        )


class TestCheckSerialization:
    def test_column_level_check_emitted(self):
        table = serialize_table(Order)
        cc = table.check_constraints["orders_status_check"]
        assert cc.expression == "status IN ('draft', 'live', 'archived')"

    def test_table_level_check_emitted(self):
        table = serialize_table(Order)
        cc = table.check_constraints["orders_qty_range"]
        assert cc.expression == "qty >= 0 AND qty <= 100"

    def test_auto_named_table_check(self):
        assert "products_check" in serialize_table(Product).check_constraints

    def test_column_check_does_not_become_an_index_or_unique(self):
        table = serialize_table(Order)
        assert table.indexes == {}
        assert table.unique_constraints == {}

    def test_no_check_means_empty_dict(self):
        class Plain(Table, table="plain"):
            id: UUID = Field(primary=True)

        assert serialize_table(Plain).check_constraints == {}

    def test_duplicate_check_name_raises(self):
        class Dupe(Table, table="dupes"):
            id: UUID = Field(primary=True)
            status: Text = Field(check="status <> ''")

            @classmethod
            def constraints(cls):
                return [Check("status <> 'x'", name="dupes_status_check")]

        with pytest.raises(ValueError, match="duplicate check constraint"):
            serialize_table(Dupe)


class TestCheckDDL:
    def test_column_check_in_create_table(self):
        assert (
            "CONSTRAINT orders_status_check CHECK "
            "(status IN ('draft', 'live', 'archived'))" in Order.to_ddl()
        )

    def test_table_check_in_create_table(self):
        assert (
            "CONSTRAINT orders_qty_range CHECK (qty >= 0 AND qty <= 100)"
            in Order.to_ddl()
        )

    def test_check_reaches_the_create_table_statement(self):
        """The differ must carry checks into CREATE TABLE, not drop them."""
        stmts = SnapshotDiffer(SchemaSnapshot(), serialize_schema([Order])).diff()
        [create] = [s for s in stmts if isinstance(s, CreateTableStatement)]
        assert {cc.name for cc in create.check_constraints} == {
            "orders_status_check",
            "orders_qty_range",
        }


@pytest.mark.skipif(
    shutil.which("initdb") is None, reason="requires a local PostgreSQL install"
)
class TestPushIdempotence:
    """PostgreSQL rewrites CHECK expressions (``IN`` becomes ``= ANY (ARRAY[..])``,
    casts get injected). Without canonicalizing the authored expression against
    the server, every ``derp push`` re-plans a DROP + ADD of the same constraint.
    """

    @staticmethod
    async def _live_snapshot(pool):
        from derp.orm.migrations.introspect.postgres import PostgresIntrospector

        return await PostgresIntrospector(pool).introspect(
            schemas=["public"], exclude_tables=[]
        )

    async def _plan(self, pool):
        from derp.orm.migrations.introspect.postgres import (
            canonicalize_check_expressions,
        )
        from derp.orm.migrations.snapshot.normalize import get_normalizer

        live = await self._live_snapshot(pool)
        desired = serialize_schema([Order, Product])
        desired = await canonicalize_check_expressions(pool, desired, live)

        n = get_normalizer("postgresql")
        return SnapshotDiffer(n.normalize(live), n.normalize(desired)).diff()

    async def test_second_push_plans_nothing(self, database_url: str):
        import asyncpg

        from derp.orm.migrations.convertors import ConvertorRegistry

        pool = await asyncpg.create_pool(database_url, min_size=1, max_size=2)
        registry = ConvertorRegistry()
        try:
            # First push: creates both tables with their checks.
            for stmt in await self._plan(pool):
                async with pool.acquire() as c:
                    await c.execute(registry.convert(stmt))

            # Second push against the now-matching database must be a no-op.
            assert await self._plan(pool) == []
        finally:
            async with pool.acquire() as c:
                await c.execute("DROP TABLE IF EXISTS orders, products CASCADE")
            await pool.close()

    async def test_probe_leaves_no_trace(self, database_url: str):
        import asyncpg

        from derp.orm.migrations.introspect.postgres import (
            canonicalize_check_expressions,
        )

        pool = await asyncpg.create_pool(database_url, min_size=1, max_size=2)
        try:
            async with pool.acquire() as c:
                await c.execute(
                    "CREATE TABLE orders (id uuid primary key, status text, qty int)"
                )
            live = await self._live_snapshot(pool)
            await canonicalize_check_expressions(pool, serialize_schema([Order]), live)

            async with pool.acquire() as c:
                assert (
                    await c.fetchval("SELECT to_regclass('_derp_check_probe')") is None
                )
                # The real table was never modified.
                assert (
                    await c.fetchval(
                        "SELECT count(*) FROM pg_constraint "
                        "WHERE conrelid = 'orders'::regclass AND contype = 'c'"
                    )
                    == 0
                )
        finally:
            async with pool.acquire() as c:
                await c.execute("DROP TABLE IF EXISTS orders CASCADE")
            await pool.close()

    async def test_check_on_a_column_added_in_the_same_migration(
        self, database_url: str
    ):
        """The probe copies the *live* table, which has no such column yet.

        Only constraints that already exist in the database get canonicalized,
        so adding a column together with its CHECK must plan normally rather
        than fail with a bogus "column does not exist".
        """
        import asyncpg

        from derp.orm.migrations.statements.types import (
            AddColumnStatement,
            CreateCheckConstraintStatement,
        )

        pool = await asyncpg.create_pool(database_url, min_size=1, max_size=2)
        try:
            async with pool.acquire() as c:
                await c.execute(
                    "CREATE TABLE orders (id uuid primary key, status text NOT NULL)"
                )
            stmts = await self._plan(pool)

            assert any(isinstance(s, AddColumnStatement) for s in stmts)
            created = {
                s.name for s in stmts if isinstance(s, CreateCheckConstraintStatement)
            }
            assert "orders_qty_range" in created
        finally:
            async with pool.acquire() as c:
                await c.execute("DROP TABLE IF EXISTS orders, products CASCADE")
            await pool.close()

    async def test_probe_failure_degrades_instead_of_raising(self, database_url: str):
        """A read-only replica or a role without TEMP must not fail the push.

        The authored expression stands; the cost is a redundant DROP + ADD of
        an identical constraint, not a broken migration.
        """
        import asyncpg

        from derp.orm.migrations.introspect.postgres import (
            canonicalize_check_expressions,
        )

        pool = await asyncpg.create_pool(database_url, min_size=1, max_size=2)
        try:
            async with pool.acquire() as c:
                await c.execute(
                    "CREATE TABLE orders (id uuid primary key, "
                    "status text, qty int);"
                    "ALTER TABLE orders ADD CONSTRAINT orders_status_check "
                    "CHECK (status IN ('draft'));"
                )
            live = await self._live_snapshot(pool)
            desired = serialize_schema([Order])
            # Aim the probe at a table it cannot copy.
            unprobeable = desired.model_copy(
                update={
                    "tables": {
                        "orders": desired.tables["orders"].model_copy(
                            update={"name": "no_such_table"}
                        )
                    }
                }
            )
            result = await canonicalize_check_expressions(pool, unprobeable, live)
            expr = (
                result.tables["orders"]
                .check_constraints["orders_status_check"]
                .expression
            )
            assert expr == "status IN ('draft', 'live', 'archived')"
        finally:
            async with pool.acquire() as c:
                await c.execute("DROP TABLE IF EXISTS orders CASCADE")
            await pool.close()

    async def test_live_check_is_not_dropped_when_declared(self, database_url: str):
        """Regression: the serializer used to emit no checks at all, so push
        planned a DROP for every CHECK constraint in the database."""
        import asyncpg

        from derp.orm.migrations.statements.types import DropCheckConstraintStatement

        pool = await asyncpg.create_pool(database_url, min_size=1, max_size=2)
        try:
            async with pool.acquire() as c:
                await c.execute(
                    "CREATE TABLE orders (id uuid primary key, status text, qty int);"
                    "ALTER TABLE orders ADD CONSTRAINT orders_status_check "
                    "CHECK (status IN ('draft', 'live', 'archived'));"
                    "ALTER TABLE orders ADD CONSTRAINT orders_qty_range "
                    "CHECK (qty >= 0 AND qty <= 100);"
                )
            stmts = await self._plan(pool)
            assert not any(
                isinstance(s, DropCheckConstraintStatement) for s in stmts
            ), [type(s).__name__ for s in stmts]
        finally:
            async with pool.acquire() as c:
                await c.execute("DROP TABLE IF EXISTS orders, products CASCADE")
            await pool.close()
