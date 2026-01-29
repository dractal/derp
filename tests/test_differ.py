"""Tests for snapshot differ."""

from __future__ import annotations

from derp.migrations.snapshot.differ import SnapshotDiffer
from derp.migrations.snapshot.models import (
    ColumnSnapshot,
    EnumSnapshot,
    ForeignKeyAction,
    ForeignKeySnapshot,
    IndexMethod,
    IndexSnapshot,
    PolicyCommand,
    PolicySnapshot,
    PrimaryKeySnapshot,
    SchemaSnapshot,
    SequenceSnapshot,
    TableSnapshot,
    UniqueConstraintSnapshot,
)
from derp.migrations.statements.types import (
    AddColumnStatement,
    AlterColumnDefaultStatement,
    AlterColumnNullableStatement,
    AlterColumnTypeStatement,
    AlterEnumAddValueStatement,
    CreateEnumStatement,
    CreateForeignKeyStatement,
    CreateIndexStatement,
    CreatePolicyStatement,
    CreateSchemaStatement,
    CreateSequenceStatement,
    CreateTableStatement,
    CreateUniqueConstraintStatement,
    DisableRLSStatement,
    DropColumnStatement,
    DropEnumStatement,
    DropForeignKeyStatement,
    DropIndexStatement,
    DropPolicyStatement,
    DropSchemaStatement,
    DropSequenceStatement,
    DropTableStatement,
    DropUniqueConstraintStatement,
    EnableRLSStatement,
)


class TestSnapshotDifferTables:
    """Tests for table diffing."""

    def test_create_new_table(self):
        """Test detecting a new table."""
        old = SchemaSnapshot(id="0000")
        new = SchemaSnapshot(
            id="0001",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "id": ColumnSnapshot(
                            name="id", type="serial", primary_key=True
                        ),
                        "name": ColumnSnapshot(name="name", type="varchar(255)"),
                    },
                    primary_key=PrimaryKeySnapshot(columns=["id"]),
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        assert len(statements) >= 1
        create_table = [s for s in statements if isinstance(s, CreateTableStatement)]
        assert len(create_table) == 1
        assert create_table[0].table_name == "users"
        assert len(create_table[0].columns) == 2

    def test_drop_table(self):
        """Test detecting a dropped table."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={"id": ColumnSnapshot(name="id", type="serial")},
                ),
            },
        )
        new = SchemaSnapshot(id="0001")

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        drop_table = [s for s in statements if isinstance(s, DropTableStatement)]
        assert len(drop_table) == 1
        assert drop_table[0].table_name == "users"

    def test_no_changes(self):
        """Test when tables are identical."""
        table = TableSnapshot(
            name="users",
            columns={"id": ColumnSnapshot(name="id", type="serial")},
        )
        old = SchemaSnapshot(id="0000", tables={"users": table})
        new = SchemaSnapshot(id="0001", tables={"users": table})

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        assert len(statements) == 0


class TestSnapshotDifferColumns:
    """Tests for column diffing."""

    def test_add_column(self):
        """Test detecting a new column."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={"id": ColumnSnapshot(name="id", type="serial")},
                ),
            },
        )
        new = SchemaSnapshot(
            id="0001",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "id": ColumnSnapshot(name="id", type="serial"),
                        "email": ColumnSnapshot(
                            name="email", type="varchar(255)", not_null=True
                        ),
                    },
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        add_column = [s for s in statements if isinstance(s, AddColumnStatement)]
        assert len(add_column) == 1
        assert add_column[0].column.name == "email"
        assert add_column[0].column.type == "varchar(255)"

    def test_drop_column(self):
        """Test detecting a dropped column."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "id": ColumnSnapshot(name="id", type="serial"),
                        "email": ColumnSnapshot(name="email", type="varchar(255)"),
                    },
                ),
            },
        )
        new = SchemaSnapshot(
            id="0001",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={"id": ColumnSnapshot(name="id", type="serial")},
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        drop_column = [s for s in statements if isinstance(s, DropColumnStatement)]
        assert len(drop_column) == 1
        assert drop_column[0].column_name == "email"

    def test_alter_column_type(self):
        """Test detecting column type change."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={"age": ColumnSnapshot(name="age", type="integer")},
                ),
            },
        )
        new = SchemaSnapshot(
            id="0001",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={"age": ColumnSnapshot(name="age", type="bigint")},
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        alter_type = [s for s in statements if isinstance(s, AlterColumnTypeStatement)]
        assert len(alter_type) == 1
        assert alter_type[0].column_name == "age"
        assert alter_type[0].old_type == "integer"
        assert alter_type[0].new_type == "bigint"

    def test_alter_column_nullable(self):
        """Test detecting nullable change."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "email": ColumnSnapshot(
                            name="email", type="varchar(255)", not_null=False
                        )
                    },
                ),
            },
        )
        new = SchemaSnapshot(
            id="0001",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "email": ColumnSnapshot(
                            name="email", type="varchar(255)", not_null=True
                        )
                    },
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        alter_nullable = [
            s for s in statements if isinstance(s, AlterColumnNullableStatement)
        ]
        assert len(alter_nullable) == 1
        assert alter_nullable[0].column_name == "email"
        assert alter_nullable[0].nullable is False

    def test_alter_column_default(self):
        """Test detecting default value change."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "is_active": ColumnSnapshot(
                            name="is_active", type="boolean", default="FALSE"
                        )
                    },
                ),
            },
        )
        new = SchemaSnapshot(
            id="0001",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "is_active": ColumnSnapshot(
                            name="is_active", type="boolean", default="TRUE"
                        )
                    },
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        alter_default = [
            s for s in statements if isinstance(s, AlterColumnDefaultStatement)
        ]
        assert len(alter_default) == 1
        assert alter_default[0].column_name == "is_active"
        assert alter_default[0].default == "TRUE"


class TestSnapshotDifferForeignKeys:
    """Tests for foreign key diffing."""

    def test_create_foreign_key(self):
        """Test detecting a new foreign key."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "posts": TableSnapshot(
                    name="posts",
                    columns={
                        "id": ColumnSnapshot(name="id", type="serial"),
                        "author_id": ColumnSnapshot(name="author_id", type="integer"),
                    },
                ),
            },
        )
        new = SchemaSnapshot(
            id="0001",
            tables={
                "posts": TableSnapshot(
                    name="posts",
                    columns={
                        "id": ColumnSnapshot(name="id", type="serial"),
                        "author_id": ColumnSnapshot(name="author_id", type="integer"),
                    },
                    foreign_keys={
                        "posts_author_id_fkey": ForeignKeySnapshot(
                            name="posts_author_id_fkey",
                            columns=["author_id"],
                            references_table="users",
                            references_columns=["id"],
                            on_delete=ForeignKeyAction.CASCADE,
                        ),
                    },
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        create_fk = [s for s in statements if isinstance(s, CreateForeignKeyStatement)]
        assert len(create_fk) == 1
        assert create_fk[0].name == "posts_author_id_fkey"
        assert create_fk[0].references_table == "users"

    def test_drop_foreign_key(self):
        """Test detecting a dropped foreign key."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "posts": TableSnapshot(
                    name="posts",
                    columns={
                        "author_id": ColumnSnapshot(name="author_id", type="integer")
                    },
                    foreign_keys={
                        "posts_author_id_fkey": ForeignKeySnapshot(
                            name="posts_author_id_fkey",
                            columns=["author_id"],
                            references_table="users",
                            references_columns=["id"],
                        ),
                    },
                ),
            },
        )
        new = SchemaSnapshot(
            id="0001",
            tables={
                "posts": TableSnapshot(
                    name="posts",
                    columns={
                        "author_id": ColumnSnapshot(name="author_id", type="integer")
                    },
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        drop_fk = [s for s in statements if isinstance(s, DropForeignKeyStatement)]
        assert len(drop_fk) == 1
        assert drop_fk[0].name == "posts_author_id_fkey"


class TestSnapshotDifferIndexes:
    """Tests for index diffing."""

    def test_create_index(self):
        """Test detecting a new index."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "email": ColumnSnapshot(name="email", type="varchar(255)")
                    },
                ),
            },
        )
        new = SchemaSnapshot(
            id="0001",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "email": ColumnSnapshot(name="email", type="varchar(255)")
                    },
                    indexes={
                        "users_email_idx": IndexSnapshot(
                            name="users_email_idx",
                            columns=["email"],
                            method=IndexMethod.BTREE,
                        ),
                    },
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        create_idx = [s for s in statements if isinstance(s, CreateIndexStatement)]
        assert len(create_idx) == 1
        assert create_idx[0].name == "users_email_idx"

    def test_drop_index(self):
        """Test detecting a dropped index."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "email": ColumnSnapshot(name="email", type="varchar(255)")
                    },
                    indexes={
                        "users_email_idx": IndexSnapshot(
                            name="users_email_idx",
                            columns=["email"],
                        ),
                    },
                ),
            },
        )
        new = SchemaSnapshot(
            id="0001",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "email": ColumnSnapshot(name="email", type="varchar(255)")
                    },
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        drop_idx = [s for s in statements if isinstance(s, DropIndexStatement)]
        assert len(drop_idx) == 1
        assert drop_idx[0].name == "users_email_idx"


class TestSnapshotDifferUniqueConstraints:
    """Tests for unique constraint diffing."""

    def test_create_unique_constraint(self):
        """Test detecting a new unique constraint."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "email": ColumnSnapshot(name="email", type="varchar(255)")
                    },
                ),
            },
        )
        new = SchemaSnapshot(
            id="0001",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "email": ColumnSnapshot(name="email", type="varchar(255)")
                    },
                    unique_constraints={
                        "users_email_unique": UniqueConstraintSnapshot(
                            name="users_email_unique",
                            columns=["email"],
                        ),
                    },
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        create_uc = [
            s for s in statements if isinstance(s, CreateUniqueConstraintStatement)
        ]
        assert len(create_uc) == 1
        assert create_uc[0].name == "users_email_unique"

    def test_drop_unique_constraint(self):
        """Test detecting a dropped unique constraint."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "email": ColumnSnapshot(name="email", type="varchar(255)")
                    },
                    unique_constraints={
                        "users_email_unique": UniqueConstraintSnapshot(
                            name="users_email_unique",
                            columns=["email"],
                        ),
                    },
                ),
            },
        )
        new = SchemaSnapshot(
            id="0001",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "email": ColumnSnapshot(name="email", type="varchar(255)")
                    },
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        drop_uc = [
            s for s in statements if isinstance(s, DropUniqueConstraintStatement)
        ]
        assert len(drop_uc) == 1
        assert drop_uc[0].name == "users_email_unique"


class TestSnapshotDifferEnums:
    """Tests for enum diffing."""

    def test_create_enum(self):
        """Test detecting a new enum."""
        old = SchemaSnapshot(id="0000")
        new = SchemaSnapshot(
            id="0001",
            enums={
                "status": EnumSnapshot(
                    name="status",
                    values=["pending", "active", "inactive"],
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        create_enum = [s for s in statements if isinstance(s, CreateEnumStatement)]
        assert len(create_enum) == 1
        assert create_enum[0].name == "status"
        assert create_enum[0].values == ["pending", "active", "inactive"]

    def test_drop_enum(self):
        """Test detecting a dropped enum."""
        old = SchemaSnapshot(
            id="0000",
            enums={
                "status": EnumSnapshot(
                    name="status",
                    values=["pending", "active"],
                ),
            },
        )
        new = SchemaSnapshot(id="0001")

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        drop_enum = [s for s in statements if isinstance(s, DropEnumStatement)]
        assert len(drop_enum) == 1
        assert drop_enum[0].name == "status"

    def test_add_enum_value(self):
        """Test detecting added enum values."""
        old = SchemaSnapshot(
            id="0000",
            enums={
                "status": EnumSnapshot(
                    name="status",
                    values=["pending", "active"],
                ),
            },
        )
        new = SchemaSnapshot(
            id="0001",
            enums={
                "status": EnumSnapshot(
                    name="status",
                    values=["pending", "active", "suspended"],
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        add_value = [s for s in statements if isinstance(s, AlterEnumAddValueStatement)]
        assert len(add_value) == 1
        assert add_value[0].value == "suspended"
        assert add_value[0].after == "active"

    def test_remove_enum_value_generates_warning(self):
        """Test that removing enum values generates a warning."""
        old = SchemaSnapshot(
            id="0000",
            enums={
                "status": EnumSnapshot(
                    name="status",
                    values=["pending", "active", "removed"],
                ),
            },
        )
        new = SchemaSnapshot(
            id="0001",
            enums={
                "status": EnumSnapshot(
                    name="status",
                    values=["pending", "active"],
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        differ.diff()
        warnings = differ.get_warnings()

        assert len(warnings) == 1
        assert "removed values" in warnings[0]


class TestSnapshotDifferSequences:
    """Tests for sequence diffing."""

    def test_create_sequence(self):
        """Test detecting a new sequence."""
        old = SchemaSnapshot(id="0000")
        new = SchemaSnapshot(
            id="0001",
            sequences={
                "order_id_seq": SequenceSnapshot(
                    name="order_id_seq",
                    start=1000,
                    increment=1,
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        create_seq = [s for s in statements if isinstance(s, CreateSequenceStatement)]
        assert len(create_seq) == 1
        assert create_seq[0].name == "order_id_seq"
        assert create_seq[0].start == 1000

    def test_drop_sequence(self):
        """Test detecting a dropped sequence."""
        old = SchemaSnapshot(
            id="0000",
            sequences={
                "order_id_seq": SequenceSnapshot(
                    name="order_id_seq",
                ),
            },
        )
        new = SchemaSnapshot(id="0001")

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        drop_seq = [s for s in statements if isinstance(s, DropSequenceStatement)]
        assert len(drop_seq) == 1
        assert drop_seq[0].name == "order_id_seq"


class TestSnapshotDifferSchemas:
    """Tests for database schema namespace diffing."""

    def test_create_schema(self):
        """Test detecting a new schema."""
        old = SchemaSnapshot(id="0000", schemas=["public"])
        new = SchemaSnapshot(id="0001", schemas=["public", "audit"])

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        create_schema = [s for s in statements if isinstance(s, CreateSchemaStatement)]
        assert len(create_schema) == 1
        assert create_schema[0].name == "audit"

    def test_drop_schema(self):
        """Test detecting a dropped schema."""
        old = SchemaSnapshot(id="0000", schemas=["public", "audit"])
        new = SchemaSnapshot(id="0001", schemas=["public"])

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        drop_schema = [s for s in statements if isinstance(s, DropSchemaStatement)]
        assert len(drop_schema) == 1
        assert drop_schema[0].name == "audit"

    def test_public_schema_not_touched(self):
        """Test that public schema is never created or dropped."""
        old = SchemaSnapshot(id="0000", schemas=["public"])
        new = SchemaSnapshot(id="0001", schemas=["public"])

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        create_schema = [s for s in statements if isinstance(s, CreateSchemaStatement)]
        drop_schema = [s for s in statements if isinstance(s, DropSchemaStatement)]

        assert len(create_schema) == 0
        assert len(drop_schema) == 0


class TestSnapshotDifferRLS:
    """Tests for Row-Level Security diffing."""

    def test_enable_rls(self):
        """Test detecting RLS being enabled."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={"id": ColumnSnapshot(name="id", type="serial")},
                    rls_enabled=False,
                ),
            },
        )
        new = SchemaSnapshot(
            id="0001",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={"id": ColumnSnapshot(name="id", type="serial")},
                    rls_enabled=True,
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        enable_rls = [s for s in statements if isinstance(s, EnableRLSStatement)]
        assert len(enable_rls) == 1
        assert enable_rls[0].table_name == "users"

    def test_disable_rls(self):
        """Test detecting RLS being disabled."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={"id": ColumnSnapshot(name="id", type="serial")},
                    rls_enabled=True,
                ),
            },
        )
        new = SchemaSnapshot(
            id="0001",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={"id": ColumnSnapshot(name="id", type="serial")},
                    rls_enabled=False,
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        disable_rls = [s for s in statements if isinstance(s, DisableRLSStatement)]
        assert len(disable_rls) == 1
        assert disable_rls[0].table_name == "users"


class TestSnapshotDifferPolicies:
    """Tests for RLS policy diffing."""

    def test_create_policy(self):
        """Test detecting a new policy."""
        old = SchemaSnapshot(id="0000")
        new = SchemaSnapshot(
            id="0001",
            policies={
                "users.user_access": PolicySnapshot(
                    name="user_access",
                    table="users",
                    command=PolicyCommand.SELECT,
                    using="id = current_user_id()",
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        create_policy = [s for s in statements if isinstance(s, CreatePolicyStatement)]
        assert len(create_policy) == 1
        assert create_policy[0].name == "user_access"
        assert create_policy[0].table_name == "users"

    def test_drop_policy(self):
        """Test detecting a dropped policy."""
        old = SchemaSnapshot(
            id="0000",
            policies={
                "users.user_access": PolicySnapshot(
                    name="user_access",
                    table="users",
                    command=PolicyCommand.ALL,
                    using="true",
                ),
            },
        )
        new = SchemaSnapshot(id="0001")

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        drop_policy = [s for s in statements if isinstance(s, DropPolicyStatement)]
        assert len(drop_policy) == 1
        assert drop_policy[0].name == "user_access"


class TestSnapshotDifferComplexScenarios:
    """Tests for complex diffing scenarios."""

    def test_multiple_changes_in_single_table(self):
        """Test detecting multiple changes within a single table."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "id": ColumnSnapshot(name="id", type="serial"),
                        "old_field": ColumnSnapshot(name="old_field", type="text"),
                    },
                ),
            },
        )
        new = SchemaSnapshot(
            id="0001",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "id": ColumnSnapshot(name="id", type="serial"),
                        "new_field": ColumnSnapshot(
                            name="new_field", type="varchar(100)"
                        ),
                    },
                    indexes={
                        "users_new_field_idx": IndexSnapshot(
                            name="users_new_field_idx",
                            columns=["new_field"],
                        ),
                    },
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        drop_cols = [s for s in statements if isinstance(s, DropColumnStatement)]
        add_cols = [s for s in statements if isinstance(s, AddColumnStatement)]
        create_idx = [s for s in statements if isinstance(s, CreateIndexStatement)]

        assert len(drop_cols) == 1
        assert drop_cols[0].column_name == "old_field"
        assert len(add_cols) == 1
        assert add_cols[0].column.name == "new_field"
        assert len(create_idx) == 1

    def test_create_table_with_all_constraints(self):
        """Test creating a table with all constraint types."""
        old = SchemaSnapshot(id="0000")
        new = SchemaSnapshot(
            id="0001",
            tables={
                "posts": TableSnapshot(
                    name="posts",
                    columns={
                        "id": ColumnSnapshot(
                            name="id", type="serial", primary_key=True
                        ),
                        "title": ColumnSnapshot(
                            name="title", type="varchar(255)", not_null=True
                        ),
                        "slug": ColumnSnapshot(name="slug", type="varchar(255)"),
                        "author_id": ColumnSnapshot(name="author_id", type="integer"),
                    },
                    primary_key=PrimaryKeySnapshot(columns=["id"]),
                    unique_constraints={
                        "posts_slug_unique": UniqueConstraintSnapshot(
                            name="posts_slug_unique",
                            columns=["slug"],
                        ),
                    },
                    foreign_keys={
                        "posts_author_fkey": ForeignKeySnapshot(
                            name="posts_author_fkey",
                            columns=["author_id"],
                            references_table="users",
                            references_columns=["id"],
                        ),
                    },
                    indexes={
                        "posts_title_idx": IndexSnapshot(
                            name="posts_title_idx",
                            columns=["title"],
                        ),
                    },
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        create_table = [s for s in statements if isinstance(s, CreateTableStatement)]
        create_idx = [s for s in statements if isinstance(s, CreateIndexStatement)]

        assert len(create_table) == 1
        table_stmt = create_table[0]
        assert table_stmt.table_name == "posts"
        assert len(table_stmt.columns) == 4
        assert table_stmt.primary_key is not None
        assert len(table_stmt.unique_constraints) == 1
        assert len(table_stmt.foreign_keys) == 1
        assert len(create_idx) == 1

    def test_statement_ordering(self):
        """Test that statements are ordered correctly for dependencies."""
        old = SchemaSnapshot(id="0000", schemas=["public"])
        new = SchemaSnapshot(
            id="0001",
            schemas=["public", "custom"],
            enums={
                "custom.status": EnumSnapshot(
                    name="status",
                    schema_name="custom",
                    values=["active", "inactive"],
                ),
            },
            tables={
                "custom.items": TableSnapshot(
                    name="items",
                    schema_name="custom",
                    columns={
                        "id": ColumnSnapshot(name="id", type="serial"),
                        "status": ColumnSnapshot(name="status", type="custom.status"),
                    },
                ),
            },
        )

        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        # Schema creation should come before enum
        schema_idx = next(
            i for i, s in enumerate(statements) if isinstance(s, CreateSchemaStatement)
        )
        enum_idx = next(
            i for i, s in enumerate(statements) if isinstance(s, CreateEnumStatement)
        )
        table_idx = next(
            i for i, s in enumerate(statements) if isinstance(s, CreateTableStatement)
        )

        assert schema_idx < enum_idx
        assert enum_idx < table_idx
