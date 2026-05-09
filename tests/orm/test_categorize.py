"""Tests for derp.orm.migrations.categorize."""

from __future__ import annotations

import pytest

from derp.orm.migrations.categorize import (
    StatementCategory,
    classify,
    classify_statements,
    describe,
    filter_drops,
    is_drop,
)
from derp.orm.migrations.statements import (
    AddColumnStatement,
    AlterColumnNullableStatement,
    AlterColumnTypeStatement,
    CreateIndexStatement,
    CreateTableStatement,
    DropColumnStatement,
    DropTableStatement,
    EnableRLSStatement,
    GrantStatement,
    RenameColumnStatement,
    RenameTableStatement,
)
from derp.orm.migrations.statements.types import ColumnDefinition


def _column(name: str = "id", type_: str = "integer") -> ColumnDefinition:
    return ColumnDefinition(name=name, type=type_)


class TestClassify:
    """classify() buckets a single statement into create/alter/drop."""

    def test_create_table_is_create(self):
        stmt = CreateTableStatement(table_name="users", columns=[_column()])
        assert classify(stmt) == StatementCategory.CREATE

    def test_add_column_is_create(self):
        stmt = AddColumnStatement(table_name="users", column=_column("email"))
        assert classify(stmt) == StatementCategory.CREATE

    def test_drop_table_is_drop(self):
        stmt = DropTableStatement(table_name="users")
        assert classify(stmt) == StatementCategory.DROP

    def test_drop_column_is_drop(self):
        stmt = DropColumnStatement(table_name="users", column_name="legacy")
        assert classify(stmt) == StatementCategory.DROP

    def test_alter_column_type_is_alter(self):
        stmt = AlterColumnTypeStatement(
            table_name="users",
            column_name="email",
            old_type="varchar(100)",
            new_type="varchar(255)",
        )
        assert classify(stmt) == StatementCategory.ALTER

    def test_alter_column_nullable_is_alter(self):
        stmt = AlterColumnNullableStatement(
            table_name="users", column_name="email", nullable=True
        )
        assert classify(stmt) == StatementCategory.ALTER

    def test_rename_column_is_alter(self):
        stmt = RenameColumnStatement(
            table_name="users", from_column="old", to_column="new"
        )
        assert classify(stmt) == StatementCategory.ALTER

    def test_rename_table_is_alter(self):
        stmt = RenameTableStatement(from_table="users", to_table="people")
        assert classify(stmt) == StatementCategory.ALTER

    def test_create_index_is_create(self):
        stmt = CreateIndexStatement(
            name="users_email_idx", table_name="users", columns=["email"]
        )
        assert classify(stmt) == StatementCategory.CREATE

    def test_enable_rls_is_create(self):
        stmt = EnableRLSStatement(table_name="users")
        assert classify(stmt) == StatementCategory.CREATE


class TestClassifyStatements:
    """classify_statements() groups a mixed list, preserving order per bucket."""

    def test_empty_list(self):
        buckets = classify_statements([])
        assert buckets == {
            StatementCategory.CREATE: [],
            StatementCategory.ALTER: [],
            StatementCategory.DROP: [],
        }

    def test_mixed_list_preserves_order(self):
        a = CreateTableStatement(table_name="a", columns=[_column()])
        b = DropColumnStatement(table_name="b", column_name="x")
        c = AddColumnStatement(table_name="a", column=_column("y"))
        d = AlterColumnTypeStatement(
            table_name="a", column_name="y", old_type="int", new_type="bigint"
        )
        e = DropTableStatement(table_name="legacy")

        buckets = classify_statements([a, b, c, d, e])

        # Within a bucket, order matches the original input order
        assert buckets[StatementCategory.CREATE] == [a, c]
        assert buckets[StatementCategory.ALTER] == [d]
        assert buckets[StatementCategory.DROP] == [b, e]

    def test_all_categories_keys_present_even_when_empty(self):
        stmt = CreateTableStatement(table_name="a", columns=[_column()])
        buckets = classify_statements([stmt])
        assert buckets[StatementCategory.ALTER] == []
        assert buckets[StatementCategory.DROP] == []


class TestFilterDrops:
    """filter_drops() removes every drop-class statement, preserving order."""

    def test_filter_drops_removes_drops(self):
        a = CreateTableStatement(table_name="a", columns=[_column()])
        b = DropTableStatement(table_name="b")
        c = AddColumnStatement(table_name="a", column=_column("y"))
        d = DropColumnStatement(table_name="a", column_name="z")

        result = filter_drops([a, b, c, d])

        assert result == [a, c]

    def test_filter_drops_keeps_alters(self):
        stmt = AlterColumnTypeStatement(
            table_name="t", column_name="c", old_type="int", new_type="bigint"
        )
        assert filter_drops([stmt]) == [stmt]

    def test_filter_drops_empty_input(self):
        assert filter_drops([]) == []

    def test_filter_drops_only_drops(self):
        stmts = [
            DropTableStatement(table_name="a"),
            DropColumnStatement(table_name="b", column_name="c"),
        ]
        assert filter_drops(stmts) == []


class TestIsDrop:
    def test_drop_table_is_drop(self):
        assert is_drop(DropTableStatement(table_name="x"))

    def test_create_table_is_not_drop(self):
        stmt = CreateTableStatement(table_name="x", columns=[_column()])
        assert not is_drop(stmt)

    def test_alter_is_not_drop(self):
        stmt = AlterColumnNullableStatement(
            table_name="x", column_name="y", nullable=True
        )
        assert not is_drop(stmt)


class TestDescribe:
    """describe() returns short human-readable labels."""

    def test_create_table(self):
        stmt = CreateTableStatement(table_name="users", columns=[_column()])
        assert describe(stmt) == "CREATE TABLE users"

    def test_drop_table(self):
        assert describe(DropTableStatement(table_name="legacy")) == "DROP TABLE legacy"

    def test_add_column_includes_table_and_name(self):
        stmt = AddColumnStatement(
            table_name="users", column=_column("avatar_url", "text")
        )
        assert describe(stmt) == "ADD COLUMN users.avatar_url"

    def test_drop_column_includes_table_and_name(self):
        stmt = DropColumnStatement(table_name="users", column_name="legacy")
        assert describe(stmt) == "DROP COLUMN users.legacy"

    def test_rename_column_shows_arrow(self):
        stmt = RenameColumnStatement(
            table_name="users", from_column="username", to_column="handle"
        )
        assert describe(stmt) == "RENAME COLUMN users.username -> handle"

    def test_rename_table_shows_arrow(self):
        stmt = RenameTableStatement(from_table="users", to_table="people")
        assert describe(stmt) == "ALTER TABLE users RENAME TO people"

    def test_alter_column_type_shows_new_type(self):
        stmt = AlterColumnTypeStatement(
            table_name="users",
            column_name="email",
            old_type="varchar(100)",
            new_type="varchar(255)",
        )
        result = describe(stmt)
        assert "users.email" in result
        assert "varchar(255)" in result

    def test_alter_column_nullable_drop_not_null(self):
        stmt = AlterColumnNullableStatement(
            table_name="users", column_name="email", nullable=True
        )
        assert "DROP NOT NULL" in describe(stmt)

    def test_alter_column_nullable_set_not_null(self):
        stmt = AlterColumnNullableStatement(
            table_name="users", column_name="email", nullable=False
        )
        assert "SET NOT NULL" in describe(stmt)

    def test_grant_includes_privileges_and_grantee(self):
        stmt = GrantStatement(
            privileges=["SELECT", "INSERT"],
            object_type="TABLE",
            object_name="users",
            grantee="readonly",
        )
        result = describe(stmt)
        assert "SELECT" in result
        assert "INSERT" in result
        assert "users" in result
        assert "readonly" in result


class TestUnknownStatement:
    """Unknown statement types raise ValueError so we notice when types are added."""

    def test_classify_raises_on_unknown(self):
        # Construct a JsonStatement directly that isn't in any bucket
        from derp.orm.migrations.statements import JsonStatement

        class FakeStatement(JsonStatement):
            type: str = "fake"

        with pytest.raises(ValueError, match="Unclassified statement type"):
            classify(FakeStatement())  # type: ignore
