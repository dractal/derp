"""Tests for statement convertors."""

from __future__ import annotations

import pytest

# Import convertors to register them
from derp.orm.migrations import convertors  # noqa: F401
from derp.orm.migrations.convertors.base import ConvertorRegistry
from derp.orm.migrations.statements.types import (
    AddColumnStatement,
    AlterColumnDefaultStatement,
    AlterColumnNullableStatement,
    AlterColumnTypeStatement,
    AlterEnumAddValueStatement,
    AlterEnumRenameValueStatement,
    CheckConstraintDefinition,
    ColumnDefinition,
    CreateCheckConstraintStatement,
    CreateEnumStatement,
    CreateForeignKeyStatement,
    CreateIndexStatement,
    CreatePrimaryKeyStatement,
    CreateTableStatement,
    CreateUniqueConstraintStatement,
    DropCheckConstraintStatement,
    DropColumnStatement,
    DropEnumStatement,
    DropForeignKeyStatement,
    DropIndexStatement,
    DropPrimaryKeyStatement,
    DropTableStatement,
    DropUniqueConstraintStatement,
    ForeignKeyDefinition,
    PrimaryKeyDefinition,
    RenameColumnStatement,
    RenameTableStatement,
    UniqueConstraintDefinition,
)


class TestCreateTableConvertor:
    """Tests for CREATE TABLE convertor."""

    def test_simple_table(self):
        """Test creating a simple table."""
        stmt = CreateTableStatement(
            table_name="users",
            columns=[
                ColumnDefinition(name="id", type="serial", primary_key=True),
                ColumnDefinition(name="name", type="varchar(255)", not_null=True),
            ],
        )

        sql = ConvertorRegistry.convert(stmt)

        assert "CREATE TABLE" in sql
        assert '"users"' in sql
        assert '"id" SERIAL PRIMARY KEY' in sql
        assert '"name" VARCHAR(255) NOT NULL' in sql

    def test_table_with_schema(self):
        """Test creating a table in a non-public schema."""
        stmt = CreateTableStatement(
            table_name="users",
            schema_name="auth",
            columns=[
                ColumnDefinition(name="id", type="serial", primary_key=True),
            ],
        )

        sql = ConvertorRegistry.convert(stmt)

        assert '"auth"."users"' in sql

    def test_table_with_composite_primary_key(self):
        """Test creating a table with composite primary key."""
        stmt = CreateTableStatement(
            table_name="user_roles",
            columns=[
                ColumnDefinition(name="user_id", type="integer"),
                ColumnDefinition(name="role_id", type="integer"),
            ],
            primary_key=PrimaryKeyDefinition(
                name="user_roles_pkey",
                columns=["user_id", "role_id"],
            ),
        )

        sql = ConvertorRegistry.convert(stmt)

        assert 'CONSTRAINT "user_roles_pkey" PRIMARY KEY ("user_id", "role_id")' in sql

    def test_table_with_unique_constraint(self):
        """Test creating a table with unique constraint."""
        stmt = CreateTableStatement(
            table_name="users",
            columns=[
                ColumnDefinition(name="id", type="serial", primary_key=True),
                ColumnDefinition(name="email", type="varchar(255)"),
            ],
            unique_constraints=[
                UniqueConstraintDefinition(
                    name="users_email_unique", columns=["email"]
                ),
            ],
        )

        sql = ConvertorRegistry.convert(stmt)

        assert 'CONSTRAINT "users_email_unique" UNIQUE ("email")' in sql

    def test_table_with_unique_nulls_not_distinct(self):
        """Test unique constraint with NULLS NOT DISTINCT."""
        stmt = CreateTableStatement(
            table_name="users",
            columns=[
                ColumnDefinition(name="id", type="serial", primary_key=True),
                ColumnDefinition(name="external_id", type="varchar(255)"),
            ],
            unique_constraints=[
                UniqueConstraintDefinition(
                    name="users_external_id_unique",
                    columns=["external_id"],
                    nulls_not_distinct=True,
                ),
            ],
        )

        sql = ConvertorRegistry.convert(stmt)

        assert "UNIQUE NULLS NOT DISTINCT" in sql

    def test_table_with_check_constraint(self):
        """Test creating a table with check constraint."""
        stmt = CreateTableStatement(
            table_name="products",
            columns=[
                ColumnDefinition(name="id", type="serial", primary_key=True),
                ColumnDefinition(name="price", type="numeric(10,2)"),
            ],
            check_constraints=[
                CheckConstraintDefinition(
                    name="products_price_positive",
                    expression="price > 0",
                ),
            ],
        )

        sql = ConvertorRegistry.convert(stmt)

        assert 'CONSTRAINT "products_price_positive" CHECK (price > 0)' in sql

    def test_table_with_foreign_key(self):
        """Test creating a table with foreign key."""
        stmt = CreateTableStatement(
            table_name="posts",
            columns=[
                ColumnDefinition(name="id", type="serial", primary_key=True),
                ColumnDefinition(name="author_id", type="integer"),
            ],
            foreign_keys=[
                ForeignKeyDefinition(
                    name="posts_author_fkey",
                    columns=["author_id"],
                    references_table="users",
                    references_columns=["id"],
                    on_delete="CASCADE",
                ),
            ],
        )

        sql = ConvertorRegistry.convert(stmt)

        assert 'CONSTRAINT "posts_author_fkey" FOREIGN KEY ("author_id")' in sql
        assert 'REFERENCES "users"("id")' in sql
        assert "ON DELETE CASCADE" in sql

    def test_table_with_deferrable_foreign_key(self):
        """Test creating a table with deferrable foreign key."""
        stmt = CreateTableStatement(
            table_name="posts",
            columns=[
                ColumnDefinition(name="id", type="serial", primary_key=True),
                ColumnDefinition(name="author_id", type="integer"),
            ],
            foreign_keys=[
                ForeignKeyDefinition(
                    name="posts_author_fkey",
                    columns=["author_id"],
                    references_table="users",
                    references_columns=["id"],
                    deferrable=True,
                    initially_deferred=True,
                ),
            ],
        )

        sql = ConvertorRegistry.convert(stmt)

        assert "DEFERRABLE INITIALLY DEFERRED" in sql

    def test_column_with_default(self):
        """Test column with default value."""
        stmt = CreateTableStatement(
            table_name="users",
            columns=[
                ColumnDefinition(name="id", type="serial", primary_key=True),
                ColumnDefinition(name="is_active", type="boolean", default="TRUE"),
            ],
        )

        sql = ConvertorRegistry.convert(stmt)

        assert "DEFAULT TRUE" in sql

    def test_column_with_generated(self):
        """Test column with generated expression."""
        stmt = CreateTableStatement(
            table_name="users",
            columns=[
                ColumnDefinition(name="id", type="serial", primary_key=True),
                ColumnDefinition(name="first_name", type="varchar(100)"),
                ColumnDefinition(name="last_name", type="varchar(100)"),
                ColumnDefinition(
                    name="full_name",
                    type="varchar(201)",
                    generated="first_name || ' ' || last_name",
                ),
            ],
        )

        sql = ConvertorRegistry.convert(stmt)

        assert "GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED" in sql

    def test_column_with_array(self):
        """Test column with array type."""
        stmt = CreateTableStatement(
            table_name="users",
            columns=[
                ColumnDefinition(name="id", type="serial", primary_key=True),
                ColumnDefinition(name="tags", type="text", array_dimensions=1),
            ],
        )

        sql = ConvertorRegistry.convert(stmt)

        assert "TEXT[]" in sql


class TestDropTableConvertor:
    """Tests for DROP TABLE convertor."""

    def test_simple_drop(self):
        """Test dropping a table."""
        stmt = DropTableStatement(table_name="users")

        sql = ConvertorRegistry.convert(stmt)

        assert sql == 'DROP TABLE IF EXISTS "users";'

    def test_drop_with_schema(self):
        """Test dropping a table in a schema."""
        stmt = DropTableStatement(table_name="users", schema_name="auth")

        sql = ConvertorRegistry.convert(stmt)

        assert 'DROP TABLE IF EXISTS "auth"."users"' in sql

    def test_drop_with_cascade(self):
        """Test dropping a table with CASCADE."""
        stmt = DropTableStatement(table_name="users", cascade=True)

        sql = ConvertorRegistry.convert(stmt)

        assert "CASCADE" in sql


class TestRenameTableConvertor:
    """Tests for RENAME TABLE convertor."""

    def test_rename_table(self):
        """Test renaming a table."""
        stmt = RenameTableStatement(from_table="users", to_table="accounts")

        sql = ConvertorRegistry.convert(stmt)

        assert 'ALTER TABLE "users" RENAME TO "accounts"' in sql


class TestAddColumnConvertor:
    """Tests for ADD COLUMN convertor."""

    def test_add_simple_column(self):
        """Test adding a simple column."""
        stmt = AddColumnStatement(
            table_name="users",
            column=ColumnDefinition(name="email", type="varchar(255)", not_null=True),
        )

        sql = ConvertorRegistry.convert(stmt)

        assert 'ALTER TABLE "users" ADD COLUMN "email" VARCHAR(255) NOT NULL' in sql

    def test_add_column_with_default(self):
        """Test adding a column with default."""
        stmt = AddColumnStatement(
            table_name="users",
            column=ColumnDefinition(name="is_active", type="boolean", default="TRUE"),
        )

        sql = ConvertorRegistry.convert(stmt)

        assert "DEFAULT TRUE" in sql


class TestDropColumnConvertor:
    """Tests for DROP COLUMN convertor."""

    def test_drop_column(self):
        """Test dropping a column."""
        stmt = DropColumnStatement(table_name="users", column_name="email")

        sql = ConvertorRegistry.convert(stmt)

        assert 'ALTER TABLE "users" DROP COLUMN "email"' in sql

    def test_drop_column_cascade(self):
        """Test dropping a column with CASCADE."""
        stmt = DropColumnStatement(
            table_name="users", column_name="email", cascade=True
        )

        sql = ConvertorRegistry.convert(stmt)

        assert "CASCADE" in sql


class TestRenameColumnConvertor:
    """Tests for RENAME COLUMN convertor."""

    def test_rename_column(self):
        """Test renaming a column."""
        stmt = RenameColumnStatement(
            table_name="users",
            from_column="name",
            to_column="full_name",
        )

        sql = ConvertorRegistry.convert(stmt)

        assert 'ALTER TABLE "users" RENAME COLUMN "name" TO "full_name"' in sql


class TestAlterColumnTypeConvertor:
    """Tests for ALTER COLUMN TYPE convertor."""

    def test_alter_type(self):
        """Test changing column type."""
        stmt = AlterColumnTypeStatement(
            table_name="users",
            column_name="age",
            old_type="integer",
            new_type="bigint",
        )

        sql = ConvertorRegistry.convert(stmt)

        assert 'ALTER TABLE "users" ALTER COLUMN "age" SET DATA TYPE BIGINT' in sql

    def test_alter_type_with_using(self):
        """Test changing column type with USING clause."""
        stmt = AlterColumnTypeStatement(
            table_name="users",
            column_name="amount",
            old_type="varchar(50)",
            new_type="numeric(10,2)",
            using="amount::numeric(10,2)",
        )

        sql = ConvertorRegistry.convert(stmt)

        assert "USING amount::numeric(10,2)" in sql


class TestAlterColumnNullableConvertor:
    """Tests for ALTER COLUMN nullable convertor."""

    def test_set_not_null(self):
        """Test setting NOT NULL."""
        stmt = AlterColumnNullableStatement(
            table_name="users",
            column_name="email",
            nullable=False,
        )

        sql = ConvertorRegistry.convert(stmt)

        assert 'ALTER TABLE "users" ALTER COLUMN "email" SET NOT NULL' in sql

    def test_drop_not_null(self):
        """Test dropping NOT NULL."""
        stmt = AlterColumnNullableStatement(
            table_name="users",
            column_name="email",
            nullable=True,
        )

        sql = ConvertorRegistry.convert(stmt)

        assert 'ALTER TABLE "users" ALTER COLUMN "email" DROP NOT NULL' in sql


class TestAlterColumnDefaultConvertor:
    """Tests for ALTER COLUMN default convertor."""

    def test_set_default(self):
        """Test setting default value."""
        stmt = AlterColumnDefaultStatement(
            table_name="users",
            column_name="is_active",
            default="TRUE",
        )

        sql = ConvertorRegistry.convert(stmt)

        assert 'ALTER TABLE "users" ALTER COLUMN "is_active" SET DEFAULT TRUE' in sql

    def test_drop_default(self):
        """Test dropping default value."""
        stmt = AlterColumnDefaultStatement(
            table_name="users",
            column_name="is_active",
            default=None,
        )

        sql = ConvertorRegistry.convert(stmt)

        assert 'ALTER TABLE "users" ALTER COLUMN "is_active" DROP DEFAULT' in sql


class TestForeignKeyConvertors:
    """Tests for foreign key convertors."""

    def test_create_foreign_key(self):
        """Test creating a foreign key."""
        stmt = CreateForeignKeyStatement(
            name="posts_author_fkey",
            table_name="posts",
            columns=["author_id"],
            references_table="users",
            references_columns=["id"],
            on_delete="CASCADE",
        )

        sql = ConvertorRegistry.convert(stmt)

        assert 'ALTER TABLE "posts" ADD CONSTRAINT "posts_author_fkey"' in sql
        assert 'FOREIGN KEY ("author_id") REFERENCES "users"("id")' in sql
        assert "ON DELETE CASCADE" in sql

    def test_drop_foreign_key(self):
        """Test dropping a foreign key."""
        stmt = DropForeignKeyStatement(
            name="posts_author_fkey",
            table_name="posts",
        )

        sql = ConvertorRegistry.convert(stmt)

        assert 'ALTER TABLE "posts" DROP CONSTRAINT "posts_author_fkey"' in sql


class TestUniqueConstraintConvertors:
    """Tests for unique constraint convertors."""

    def test_create_unique_constraint(self):
        """Test creating a unique constraint."""
        stmt = CreateUniqueConstraintStatement(
            name="users_email_unique",
            table_name="users",
            columns=["email"],
        )

        sql = ConvertorRegistry.convert(stmt)

        assert (
            'ALTER TABLE "users" ADD CONSTRAINT "users_email_unique" UNIQUE ("email")'
            in sql
        )

    def test_create_unique_constraint_nulls_not_distinct(self):
        """Test creating a unique constraint with NULLS NOT DISTINCT."""
        stmt = CreateUniqueConstraintStatement(
            name="users_external_id_unique",
            table_name="users",
            columns=["external_id"],
            nulls_not_distinct=True,
        )

        sql = ConvertorRegistry.convert(stmt)

        assert "UNIQUE NULLS NOT DISTINCT" in sql

    def test_drop_unique_constraint(self):
        """Test dropping a unique constraint."""
        stmt = DropUniqueConstraintStatement(
            name="users_email_unique",
            table_name="users",
        )

        sql = ConvertorRegistry.convert(stmt)

        assert 'ALTER TABLE "users" DROP CONSTRAINT "users_email_unique"' in sql


class TestCheckConstraintConvertors:
    """Tests for check constraint convertors."""

    def test_create_check_constraint(self):
        """Test creating a check constraint."""
        stmt = CreateCheckConstraintStatement(
            name="products_price_positive",
            table_name="products",
            expression="price > 0",
        )

        sql = ConvertorRegistry.convert(stmt)

        assert (
            'ALTER TABLE "products" ADD CONSTRAINT "products_price_positive" '
            'CHECK (price > 0)'
            in sql
        )

    def test_drop_check_constraint(self):
        """Test dropping a check constraint."""
        stmt = DropCheckConstraintStatement(
            name="products_price_positive",
            table_name="products",
        )

        sql = ConvertorRegistry.convert(stmt)

        assert 'ALTER TABLE "products" DROP CONSTRAINT "products_price_positive"' in sql


class TestPrimaryKeyConvertors:
    """Tests for primary key convertors."""

    def test_create_primary_key(self):
        """Test creating a primary key."""
        stmt = CreatePrimaryKeyStatement(
            name="users_pkey",
            table_name="users",
            columns=["id"],
        )

        sql = ConvertorRegistry.convert(stmt)

        assert (
            'ALTER TABLE "users" ADD CONSTRAINT "users_pkey" PRIMARY KEY ("id")' in sql
        )

    def test_create_primary_key_without_name(self):
        """Test creating a primary key without explicit name."""
        stmt = CreatePrimaryKeyStatement(
            table_name="users",
            columns=["id"],
        )

        sql = ConvertorRegistry.convert(stmt)

        assert 'ALTER TABLE "users" ADD PRIMARY KEY ("id")' in sql

    def test_drop_primary_key(self):
        """Test dropping a primary key."""
        stmt = DropPrimaryKeyStatement(
            name="users_pkey",
            table_name="users",
        )

        sql = ConvertorRegistry.convert(stmt)

        assert 'ALTER TABLE "users" DROP CONSTRAINT "users_pkey"' in sql


class TestIndexConvertors:
    """Tests for index convertors."""

    def test_create_simple_index(self):
        """Test creating a simple index."""
        stmt = CreateIndexStatement(
            name="users_email_idx",
            table_name="users",
            columns=["email"],
        )

        sql = ConvertorRegistry.convert(stmt)

        assert 'CREATE INDEX "users_email_idx" ON "users" ("email")' in sql

    def test_create_unique_index(self):
        """Test creating a unique index."""
        stmt = CreateIndexStatement(
            name="users_email_idx",
            table_name="users",
            columns=["email"],
            unique=True,
        )

        sql = ConvertorRegistry.convert(stmt)

        assert "CREATE UNIQUE INDEX" in sql

    def test_create_index_concurrently(self):
        """Test creating an index concurrently."""
        stmt = CreateIndexStatement(
            name="users_email_idx",
            table_name="users",
            columns=["email"],
            concurrently=True,
        )

        sql = ConvertorRegistry.convert(stmt)

        assert "INDEX CONCURRENTLY" in sql

    def test_create_index_with_method(self):
        """Test creating an index with non-btree method."""
        stmt = CreateIndexStatement(
            name="users_data_idx",
            table_name="users",
            columns=["data"],
            method="gin",
        )

        sql = ConvertorRegistry.convert(stmt)

        assert "USING GIN" in sql

    def test_create_partial_index(self):
        """Test creating a partial index."""
        stmt = CreateIndexStatement(
            name="users_active_idx",
            table_name="users",
            columns=["email"],
            where="is_active = true",
        )

        sql = ConvertorRegistry.convert(stmt)

        assert "WHERE is_active = true" in sql

    def test_create_index_with_include(self):
        """Test creating an index with INCLUDE columns."""
        stmt = CreateIndexStatement(
            name="users_email_idx",
            table_name="users",
            columns=["email"],
            include=["name", "created_at"],
        )

        sql = ConvertorRegistry.convert(stmt)

        assert 'INCLUDE ("name", "created_at")' in sql

    def test_drop_index(self):
        """Test dropping an index."""
        stmt = DropIndexStatement(name="users_email_idx")

        sql = ConvertorRegistry.convert(stmt)

        assert 'DROP INDEX IF EXISTS "users_email_idx"' in sql

    def test_drop_index_with_schema(self):
        """Test dropping an index with schema."""
        stmt = DropIndexStatement(name="users_email_idx", schema_name="auth")

        sql = ConvertorRegistry.convert(stmt)

        assert '"auth"."users_email_idx"' in sql

    def test_drop_index_concurrently(self):
        """Test dropping an index concurrently."""
        stmt = DropIndexStatement(name="users_email_idx", concurrently=True)

        sql = ConvertorRegistry.convert(stmt)

        assert "DROP INDEX CONCURRENTLY" in sql


class TestEnumConvertors:
    """Tests for enum convertors."""

    def test_create_enum(self):
        """Test creating an enum."""
        stmt = CreateEnumStatement(
            name="status",
            values=["pending", "active", "inactive"],
        )

        sql = ConvertorRegistry.convert(stmt)

        assert 'CREATE TYPE "status" AS ENUM' in sql
        assert "'pending'" in sql
        assert "'active'" in sql
        assert "'inactive'" in sql

    def test_create_enum_with_schema(self):
        """Test creating an enum in a schema."""
        stmt = CreateEnumStatement(
            name="status",
            schema_name="custom",
            values=["pending", "active"],
        )

        sql = ConvertorRegistry.convert(stmt)

        assert '"custom"."status"' in sql

    def test_drop_enum(self):
        """Test dropping an enum."""
        stmt = DropEnumStatement(name="status")

        sql = ConvertorRegistry.convert(stmt)

        assert 'DROP TYPE IF EXISTS "status"' in sql

    def test_drop_enum_cascade(self):
        """Test dropping an enum with CASCADE."""
        stmt = DropEnumStatement(name="status", cascade=True)

        sql = ConvertorRegistry.convert(stmt)

        assert "CASCADE" in sql

    def test_add_enum_value(self):
        """Test adding an enum value."""
        stmt = AlterEnumAddValueStatement(
            name="status",
            value="suspended",
        )

        sql = ConvertorRegistry.convert(stmt)

        assert "ALTER TYPE \"status\" ADD VALUE 'suspended'" in sql

    def test_add_enum_value_after(self):
        """Test adding an enum value after another."""
        stmt = AlterEnumAddValueStatement(
            name="status",
            value="suspended",
            after="active",
        )

        sql = ConvertorRegistry.convert(stmt)

        assert "AFTER 'active'" in sql

    def test_add_enum_value_before(self):
        """Test adding an enum value before another."""
        stmt = AlterEnumAddValueStatement(
            name="status",
            value="pending",
            before="active",
        )

        sql = ConvertorRegistry.convert(stmt)

        assert "BEFORE 'active'" in sql

    def test_rename_enum_value(self):
        """Test renaming an enum value."""
        stmt = AlterEnumRenameValueStatement(
            name="status",
            old_value="inactive",
            new_value="disabled",
        )

        sql = ConvertorRegistry.convert(stmt)

        assert "ALTER TYPE \"status\" RENAME VALUE 'inactive' TO 'disabled'" in sql


class TestConvertorRegistry:
    """Tests for the convertor registry."""

    def test_convert_unregistered_type(self):
        """Test that converting an unregistered type raises an error."""

        class UnknownStatement:
            type: str = "unknown_type"

        with pytest.raises(ValueError):
            ConvertorRegistry.convert(UnknownStatement())  # type: ignore

    def test_all_convertors_registered(self):
        """Test that all expected convertors are registered."""
        expected_types = [
            "create_table",
            "drop_table",
            "rename_table",
            "alter_table_add_column",
            "alter_table_drop_column",
            "alter_table_rename_column",
            "alter_table_alter_column_set_type",
            "alter_table_alter_column_set_nullable",
            "alter_table_alter_column_set_default",
            "create_foreign_key",
            "drop_foreign_key",
            "create_unique_constraint",
            "drop_unique_constraint",
            "create_check_constraint",
            "drop_check_constraint",
            "create_pk",
            "drop_pk",
            "create_index",
            "drop_index",
            "create_enum",
            "drop_enum",
            "alter_enum_add_value",
            "alter_enum_rename_value",
        ]

        for stmt_type in expected_types:
            assert stmt_type in ConvertorRegistry._convertors, (
                f"Missing convertor for {stmt_type}"
            )
