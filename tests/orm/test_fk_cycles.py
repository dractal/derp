"""Foreign keys are emitted as ALTER TABLE, never inline.

Like drizzle-kit, the differ creates every table without its foreign keys, then
adds all of them with ``ALTER TABLE ... ADD CONSTRAINT`` once the tables exist.
Nothing in the CREATE TABLE phase references another table, so a cycle between
two tables — or a self-reference, or any depth of chain — needs no dependency
ordering and no special case.
"""

from __future__ import annotations

import shutil

import pytest

from derp.orm.migrations.snapshot.differ import SnapshotDiffer
from derp.orm.migrations.snapshot.models import (
    ColumnSnapshot,
    ForeignKeyAction,
    ForeignKeySnapshot,
    PrimaryKeySnapshot,
    SchemaSnapshot,
    TableSnapshot,
)
from derp.orm.migrations.statements.types import (
    CreateForeignKeyStatement,
    CreateTableStatement,
)


def _table(name: str, *refs: tuple[str, str]) -> TableSnapshot:
    """Build a table with an ``id`` PK and one FK column per ``(column, table)``."""
    columns = {"id": ColumnSnapshot(name="id", type="uuid", primary_key=True)}
    foreign_keys = {}
    for col, ref_table in refs:
        columns[col] = ColumnSnapshot(name=col, type="uuid", not_null=False)
        fk_name = f"{name}_{col}_fkey"
        foreign_keys[fk_name] = ForeignKeySnapshot(
            name=fk_name,
            columns=[col],
            references_table=ref_table,
            references_columns=["id"],
        )
    return TableSnapshot(
        name=name,
        columns=columns,
        primary_key=PrimaryKeySnapshot(columns=["id"]),
        foreign_keys=foreign_keys,
    )


def _diff(*tables: TableSnapshot):
    new = SchemaSnapshot(tables={t.name: t for t in tables})
    return SnapshotDiffer(SchemaSnapshot(), new).diff()


def _creates(stmts) -> list[str]:
    return [s.table_name for s in stmts if isinstance(s, CreateTableStatement)]


def _inline_fk_count(stmts) -> int:
    return sum(
        len(s.foreign_keys) for s in stmts if isinstance(s, CreateTableStatement)
    )


def _fk_constraints(stmts) -> list[str]:
    return [s.name for s in stmts if isinstance(s, CreateForeignKeyStatement)]


class TestForeignKeysAreNeverInline:
    def test_no_create_table_carries_a_foreign_key(self):
        stmts = _diff(_table("posts", ("author_id", "users")), _table("users"))
        assert _inline_fk_count(stmts) == 0

    def test_fk_becomes_an_add_constraint(self):
        stmts = _diff(_table("posts", ("author_id", "users")), _table("users"))
        assert _fk_constraints(stmts) == ["posts_author_id_fkey"]

    def test_fk_to_a_preexisting_table_is_still_an_add_constraint(self):
        old = SchemaSnapshot(tables={"users": _table("users")})
        new = SchemaSnapshot(
            tables={
                "users": _table("users"),
                "posts": _table("posts", ("uid", "users")),
            }
        )
        stmts = SnapshotDiffer(old, new).diff()
        assert _creates(stmts) == ["posts"]
        assert _fk_constraints(stmts) == ["posts_uid_fkey"]

    def test_referential_actions_are_preserved(self):
        posts = _table("posts", ("author_id", "users"))
        for fk in posts.foreign_keys.values():
            fk.on_delete = ForeignKeyAction.CASCADE
            fk.on_update = ForeignKeyAction.RESTRICT

        stmts = _diff(posts, _table("users"))
        [fk_stmt] = [s for s in stmts if isinstance(s, CreateForeignKeyStatement)]
        assert fk_stmt.on_delete == ForeignKeyAction.CASCADE.value
        assert fk_stmt.on_update == ForeignKeyAction.RESTRICT.value


class TestPhaseOrdering:
    def test_every_create_precedes_every_add_constraint(self):
        stmts = _diff(
            _table("a", ("b_id", "b")),
            _table("b", ("a_id", "a")),
            _table("c", ("a_id", "a")),
        )
        last_create = max(
            i for i, s in enumerate(stmts) if isinstance(s, CreateTableStatement)
        )
        first_fk = min(
            i for i, s in enumerate(stmts) if isinstance(s, CreateForeignKeyStatement)
        )
        assert last_create < first_fk

    def test_creation_order_is_deterministic_and_sorted(self):
        def plan():
            stmts = _diff(
                _table("c", ("b_id", "b")),
                _table("a"),
                _table("b", ("a_id", "a")),
            )
            return _creates(stmts), _fk_constraints(stmts)

        first = plan()
        assert first[0] == ["a", "b", "c"]
        assert all(plan() == first for _ in range(5))


class TestCyclesNeedNoSpecialCase:
    def test_two_table_cycle_emits_both_fks(self):
        stmts = _diff(
            _table("staging_requests", ("accepted_quote_id", "quotes")),
            _table("quotes", ("staging_request_id", "staging_requests")),
        )
        assert sorted(_creates(stmts)) == ["quotes", "staging_requests"]
        assert _inline_fk_count(stmts) == 0
        assert sorted(_fk_constraints(stmts)) == [
            "quotes_staging_request_id_fkey",
            "staging_requests_accepted_quote_id_fkey",
        ]

    def test_three_table_cycle_emits_all_three_fks(self):
        stmts = _diff(
            _table("a", ("b_id", "b")),
            _table("b", ("c_id", "c")),
            _table("c", ("a_id", "a")),
        )
        assert len(_creates(stmts)) == 3
        assert len(_fk_constraints(stmts)) == 3

    def test_self_reference_emits_one_add_constraint(self):
        stmts = _diff(_table("nodes", ("parent_id", "nodes")))
        assert _inline_fk_count(stmts) == 0
        assert _fk_constraints(stmts) == ["nodes_parent_id_fkey"]

    def test_each_fk_is_emitted_exactly_once(self):
        stmts = _diff(
            _table("a", ("b_id", "b")),
            _table("b", ("a_id", "a"), ("c_id", "c")),
            _table("c", ("a_id", "a")),
        )
        fks = _fk_constraints(stmts)
        assert sorted(fks) == [
            "a_b_id_fkey",
            "b_a_id_fkey",
            "b_c_id_fkey",
            "c_a_id_fkey",
        ]
        assert len(fks) == len(set(fks))


@pytest.mark.skipif(
    shutil.which("initdb") is None, reason="requires a local PostgreSQL install"
)
class TestAppliesToRealPostgres:
    """The whole point: the generated baseline must actually execute."""

    @staticmethod
    def _sql(stmts) -> list[str]:
        from derp.orm.migrations.convertors import ConvertorRegistry

        registry = ConvertorRegistry()
        return [registry.convert(s) for s in stmts]

    async def test_cyclic_baseline_applies(self, database_url: str):
        import asyncpg

        stmts = _diff(
            _table("staging_requests", ("accepted_quote_id", "quotes")),
            _table("quotes", ("staging_request_id", "staging_requests")),
        )
        conn = await asyncpg.connect(database_url)
        try:
            for sql in self._sql(stmts):
                await conn.execute(sql)
            n = await conn.fetchval(
                "SELECT count(*) FROM pg_constraint WHERE contype = 'f'"
            )
            assert n == 2
        finally:
            await conn.execute("DROP TABLE IF EXISTS quotes, staging_requests CASCADE")
            await conn.close()

    async def test_self_referential_baseline_applies(self, database_url: str):
        import asyncpg

        stmts = _diff(_table("nodes", ("parent_id", "nodes")))
        conn = await asyncpg.connect(database_url)
        try:
            for sql in self._sql(stmts):
                await conn.execute(sql)
            n = await conn.fetchval(
                "SELECT count(*) FROM pg_constraint WHERE contype = 'f'"
            )
            assert n == 1
        finally:
            await conn.execute("DROP TABLE IF EXISTS nodes CASCADE")
            await conn.close()


class TestThroughTheNormalizer:
    """``generate`` and ``push`` both normalize before diffing. The FK plan must
    survive the normalizer re-keying ``TableSnapshot.foreign_keys``."""

    @staticmethod
    def _normalized(*tables: TableSnapshot):
        from derp.orm.migrations.snapshot.normalize import get_normalizer

        normalizer = get_normalizer("postgresql")
        new = SchemaSnapshot(tables={t.name: t for t in tables})
        return SnapshotDiffer(
            normalizer.normalize(SchemaSnapshot()), normalizer.normalize(new)
        ).diff()

    def test_cycle_survives_normalization(self):
        stmts = self._normalized(
            _table("staging_requests", ("accepted_quote_id", "quotes")),
            _table("quotes", ("staging_request_id", "staging_requests")),
        )
        assert len(_creates(stmts)) == 2
        assert len(_fk_constraints(stmts)) == 2

    def test_fk_keeps_its_constraint_name(self):
        stmts = self._normalized(
            _table("posts", ("author_id", "users")), _table("users")
        )
        assert _fk_constraints(stmts) == ["posts_author_id_fkey"]
