"""Tests for snapshot differ."""

from __future__ import annotations

from derp.orm.migrations.snapshot.differ import SnapshotDiffer
from derp.orm.migrations.snapshot.models import (
    CheckConstraintSnapshot,
    ColumnSnapshot,
    EnumSnapshot,
    ForeignKeyAction,
    ForeignKeySnapshot,
    IndexColumnSnapshot,
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
from derp.orm.migrations.statements.types import (
    AddColumnStatement,
    AlterColumnDefaultStatement,
    AlterColumnNullableStatement,
    AlterColumnTypeStatement,
    AlterEnumAddValueStatement,
    CreateCheckConstraintStatement,
    CreateEnumStatement,
    CreateForeignKeyStatement,
    CreateIndexStatement,
    CreatePolicyStatement,
    CreateSchemaStatement,
    CreateSequenceStatement,
    CreateTableStatement,
    CreateUniqueConstraintStatement,
    DisableRLSStatement,
    DropCheckConstraintStatement,
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
    RenameColumnStatement,
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
        create_fk = [s for s in statements if isinstance(s, CreateForeignKeyStatement)]

        assert len(create_table) == 1
        table_stmt = create_table[0]
        assert table_stmt.table_name == "posts"
        assert len(table_stmt.columns) == 4
        assert table_stmt.primary_key is not None
        assert len(table_stmt.unique_constraints) == 1
        # FKs are emitted as ALTER TABLE after the CREATE, never inline.
        assert table_stmt.foreign_keys == []
        assert [s.name for s in create_fk] == ["posts_author_fkey"]
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


class TestSnapshotDifferColumnRename:
    """Tests for column rename detection."""

    def test_rename_column_with_resolver_confirming(self):
        """Test that rename is detected when resolver confirms it."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "id": ColumnSnapshot(name="id", type="serial"),
                        "username": ColumnSnapshot(
                            name="username", type="varchar(255)", not_null=True
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
                        "id": ColumnSnapshot(name="id", type="serial"),
                        "user_name": ColumnSnapshot(
                            name="user_name", type="varchar(255)", not_null=True
                        ),
                    },
                ),
            },
        )

        # Resolver that confirms the rename
        def resolver(obj_type: str, old_name: str, new_name: str) -> bool:
            return (
                obj_type == "column"
                and old_name == "users.username"
                and new_name == "user_name"
            )

        differ = SnapshotDiffer(old, new, rename_resolver=resolver)
        statements = differ.diff()

        rename_stmts = [s for s in statements if isinstance(s, RenameColumnStatement)]
        drop_stmts = [s for s in statements if isinstance(s, DropColumnStatement)]
        add_stmts = [s for s in statements if isinstance(s, AddColumnStatement)]

        assert len(rename_stmts) == 1
        assert rename_stmts[0].from_column == "username"
        assert rename_stmts[0].to_column == "user_name"
        assert rename_stmts[0].table_name == "users"
        assert len(drop_stmts) == 0
        assert len(add_stmts) == 0

    def test_no_rename_without_resolver(self):
        """Test that without resolver, drop+add is generated."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "username": ColumnSnapshot(
                            name="username", type="varchar(255)", not_null=True
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
                        "user_name": ColumnSnapshot(
                            name="user_name", type="varchar(255)", not_null=True
                        ),
                    },
                ),
            },
        )

        # No resolver passed
        differ = SnapshotDiffer(old, new)
        statements = differ.diff()

        rename_stmts = [s for s in statements if isinstance(s, RenameColumnStatement)]
        drop_stmts = [s for s in statements if isinstance(s, DropColumnStatement)]
        add_stmts = [s for s in statements if isinstance(s, AddColumnStatement)]

        assert len(rename_stmts) == 0
        assert len(drop_stmts) == 1
        assert len(add_stmts) == 1
        assert drop_stmts[0].column_name == "username"
        assert add_stmts[0].column.name == "user_name"

    def test_no_rename_when_resolver_rejects(self):
        """Test that drop+add is generated when resolver rejects rename."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "username": ColumnSnapshot(
                            name="username", type="varchar(255)", not_null=True
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
                        "user_name": ColumnSnapshot(
                            name="user_name", type="varchar(255)", not_null=True
                        ),
                    },
                ),
            },
        )

        # Resolver that rejects all renames
        def resolver(obj_type: str, old_name: str, new_name: str) -> bool:
            return False

        differ = SnapshotDiffer(old, new, rename_resolver=resolver)
        statements = differ.diff()

        rename_stmts = [s for s in statements if isinstance(s, RenameColumnStatement)]
        drop_stmts = [s for s in statements if isinstance(s, DropColumnStatement)]
        add_stmts = [s for s in statements if isinstance(s, AddColumnStatement)]

        assert len(rename_stmts) == 0
        assert len(drop_stmts) == 1
        assert len(add_stmts) == 1

    def test_no_rename_candidate_with_different_types(self):
        """Test that columns with different types are not rename candidates."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "old_col": ColumnSnapshot(name="old_col", type="varchar(255)"),
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
                        "new_col": ColumnSnapshot(
                            name="new_col",
                            type="integer",  # Different type!
                        ),
                    },
                ),
            },
        )

        # Resolver that would accept any rename
        def resolver(obj_type: str, old_name: str, new_name: str) -> bool:
            return True

        differ = SnapshotDiffer(old, new, rename_resolver=resolver)
        statements = differ.diff()

        # Should NOT detect a rename because types differ
        rename_stmts = [s for s in statements if isinstance(s, RenameColumnStatement)]
        drop_stmts = [s for s in statements if isinstance(s, DropColumnStatement)]
        add_stmts = [s for s in statements if isinstance(s, AddColumnStatement)]

        assert len(rename_stmts) == 0
        assert len(drop_stmts) == 1
        assert len(add_stmts) == 1

    def test_no_rename_candidate_with_different_nullability(self):
        """Test that columns with different nullability are not rename candidates."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "old_col": ColumnSnapshot(
                            name="old_col", type="varchar(255)", not_null=True
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
                        "new_col": ColumnSnapshot(
                            name="new_col", type="varchar(255)", not_null=False
                        ),
                    },
                ),
            },
        )

        # Resolver that would accept any rename
        def resolver(obj_type: str, old_name: str, new_name: str) -> bool:
            return True

        differ = SnapshotDiffer(old, new, rename_resolver=resolver)
        statements = differ.diff()

        # Should NOT detect a rename because nullability differs
        rename_stmts = [s for s in statements if isinstance(s, RenameColumnStatement)]
        assert len(rename_stmts) == 0

    def test_ambiguous_rename_first_match_wins(self):
        """Test that with multiple potential matches, first confirmed wins."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "col_a": ColumnSnapshot(
                            name="col_a", type="varchar(255)", not_null=True
                        ),
                        "col_b": ColumnSnapshot(
                            name="col_b", type="varchar(255)", not_null=True
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
                        "col_x": ColumnSnapshot(
                            name="col_x", type="varchar(255)", not_null=True
                        ),
                        "col_y": ColumnSnapshot(
                            name="col_y", type="varchar(255)", not_null=True
                        ),
                    },
                ),
            },
        )

        # Resolver that confirms col_a->col_x and col_b->col_y
        confirmed = {
            ("users.col_a", "col_x"): True,
            ("users.col_b", "col_y"): True,
        }

        def resolver(obj_type: str, old_name: str, new_name: str) -> bool:
            return confirmed.get((old_name, new_name), False)

        differ = SnapshotDiffer(old, new, rename_resolver=resolver)
        statements = differ.diff()

        rename_stmts = [s for s in statements if isinstance(s, RenameColumnStatement)]
        drop_stmts = [s for s in statements if isinstance(s, DropColumnStatement)]
        add_stmts = [s for s in statements if isinstance(s, AddColumnStatement)]

        # Both renames should be confirmed
        assert len(rename_stmts) == 2
        assert len(drop_stmts) == 0
        assert len(add_stmts) == 0

        # Verify the rename mappings
        renames = {s.from_column: s.to_column for s in rename_stmts}
        assert renames == {"col_a": "col_x", "col_b": "col_y"}


# =============================================================================
# Definition-change diffing
#
# Regression coverage for the bug Codex flagged: when an index, FK, unique
# constraint, or check constraint exists in BOTH snapshots under the same name
# but the definition differs (opclass, columns, sort order, ON DELETE,
# expression, etc.), the differ historically emitted no statements at all.
# These tests assert the same-name-changed-definition path emits DROP + CREATE.
# =============================================================================


def _users_table_with_email() -> dict[str, ColumnSnapshot]:
    return {
        "id": ColumnSnapshot(name="id", type="serial", primary_key=True),
        "email": ColumnSnapshot(name="email", type="varchar(255)"),
    }


class TestSnapshotDifferIndexDefinitionChange:
    """Same-name index whose definition changed must emit DROP + CREATE."""

    def _diff_with_index(self, old_idx: IndexSnapshot, new_idx: IndexSnapshot) -> list:
        """Build two snapshots that differ only in one index definition."""
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns=_users_table_with_email(),
                    indexes={old_idx.name: old_idx},
                ),
            },
        )
        new = SchemaSnapshot(
            id="0001",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns=_users_table_with_email(),
                    indexes={new_idx.name: new_idx},
                ),
            },
        )
        return SnapshotDiffer(old, new).diff()

    def test_opclass_change_emits_drop_and_create(self):
        """Adding an opclass on an existing index → DROP + CREATE."""
        old_idx = IndexSnapshot(
            name="users_email_idx",
            columns=["email"],
            column_specs=[IndexColumnSnapshot(name="email")],
        )
        new_idx = IndexSnapshot(
            name="users_email_idx",
            columns=["email"],
            column_specs=[
                IndexColumnSnapshot(name="email", opclass="text_pattern_ops")
            ],
        )

        statements = self._diff_with_index(old_idx, new_idx)

        drops = [s for s in statements if isinstance(s, DropIndexStatement)]
        creates = [s for s in statements if isinstance(s, CreateIndexStatement)]
        assert len(drops) == 1
        assert drops[0].name == "users_email_idx"
        assert len(creates) == 1
        assert creates[0].name == "users_email_idx"
        assert creates[0].column_specs[0].opclass == "text_pattern_ops"

    def test_sort_order_change_emits_drop_and_create(self):
        """Flipping ASC → DESC → DROP + CREATE."""
        old_idx = IndexSnapshot(
            name="users_email_idx",
            columns=["email"],
            column_specs=[IndexColumnSnapshot(name="email", order="ASC")],
        )
        new_idx = IndexSnapshot(
            name="users_email_idx",
            columns=["email"],
            column_specs=[IndexColumnSnapshot(name="email", order="DESC")],
        )

        statements = self._diff_with_index(old_idx, new_idx)

        assert any(isinstance(s, DropIndexStatement) for s in statements)
        creates = [s for s in statements if isinstance(s, CreateIndexStatement)]
        assert len(creates) == 1
        assert creates[0].column_specs[0].order == "DESC"

    def test_with_options_change_emits_drop_and_create(self):
        """HNSW with_options change → DROP + CREATE (the codex example)."""
        old_idx = IndexSnapshot(
            name="items_embedding_idx",
            columns=["embedding"],
            method=IndexMethod.HNSW,
            with_options={"m": "16", "ef_construction": "64"},
        )
        new_idx = IndexSnapshot(
            name="items_embedding_idx",
            columns=["embedding"],
            method=IndexMethod.HNSW,
            with_options={"m": "32", "ef_construction": "128"},
        )

        statements = self._diff_with_index(old_idx, new_idx)

        assert any(isinstance(s, DropIndexStatement) for s in statements)
        creates = [s for s in statements if isinstance(s, CreateIndexStatement)]
        assert len(creates) == 1
        assert creates[0].with_options == {"m": "32", "ef_construction": "128"}

    def test_method_change_emits_drop_and_create(self):
        """Switching method (BTREE → GIN) → DROP + CREATE."""
        old_idx = IndexSnapshot(
            name="users_email_idx", columns=["email"], method=IndexMethod.BTREE
        )
        new_idx = IndexSnapshot(
            name="users_email_idx", columns=["email"], method=IndexMethod.GIN
        )

        statements = self._diff_with_index(old_idx, new_idx)

        creates = [s for s in statements if isinstance(s, CreateIndexStatement)]
        assert len(creates) == 1
        assert creates[0].method == IndexMethod.GIN

    def test_where_clause_change_emits_drop_and_create(self):
        """Partial-index WHERE clause edit → DROP + CREATE."""
        old_idx = IndexSnapshot(
            name="users_email_idx",
            columns=["email"],
            where="email IS NOT NULL",
        )
        new_idx = IndexSnapshot(
            name="users_email_idx",
            columns=["email"],
            where="email IS NOT NULL AND email != ''",
        )

        statements = self._diff_with_index(old_idx, new_idx)

        creates = [s for s in statements if isinstance(s, CreateIndexStatement)]
        assert len(creates) == 1
        assert creates[0].where == "email IS NOT NULL AND email != ''"

    def test_unchanged_index_emits_nothing(self):
        """Identical same-name index → no statements at all."""
        idx = IndexSnapshot(
            name="users_email_idx",
            columns=["email"],
            column_specs=[
                IndexColumnSnapshot(name="email", opclass="text_pattern_ops")
            ],
            method=IndexMethod.BTREE,
            where="email IS NOT NULL",
            with_options={"fillfactor": "70"},
        )
        statements = self._diff_with_index(idx, idx.model_copy())

        assert not any(isinstance(s, DropIndexStatement) for s in statements)
        assert not any(isinstance(s, CreateIndexStatement) for s in statements)

    def test_concurrently_change_alone_emits_nothing(self):
        """``concurrently`` is a build-time hint, not part of index identity."""
        old_idx = IndexSnapshot(
            name="users_email_idx", columns=["email"], concurrently=False
        )
        new_idx = IndexSnapshot(
            name="users_email_idx", columns=["email"], concurrently=True
        )
        statements = self._diff_with_index(old_idx, new_idx)
        assert not any(isinstance(s, DropIndexStatement) for s in statements)
        assert not any(isinstance(s, CreateIndexStatement) for s in statements)


class TestSnapshotDifferForeignKeyDefinitionChange:
    """Same-name FK whose definition changed must emit DROP + CREATE."""

    def _diff_with_fk(
        self, old_fk: ForeignKeySnapshot, new_fk: ForeignKeySnapshot
    ) -> list:
        cols = {
            "id": ColumnSnapshot(name="id", type="serial", primary_key=True),
            "owner_id": ColumnSnapshot(name="owner_id", type="integer"),
        }
        old = SchemaSnapshot(
            id="0000",
            tables={
                "posts": TableSnapshot(
                    name="posts", columns=cols, foreign_keys={old_fk.name: old_fk}
                ),
            },
        )
        new = SchemaSnapshot(
            id="0001",
            tables={
                "posts": TableSnapshot(
                    name="posts", columns=cols, foreign_keys={new_fk.name: new_fk}
                ),
            },
        )
        return SnapshotDiffer(old, new).diff()

    def test_on_delete_change_emits_drop_and_create(self):
        """Flipping ON DELETE CASCADE → SET NULL → DROP + CREATE."""
        old_fk = ForeignKeySnapshot(
            name="posts_owner_id_fkey",
            columns=["owner_id"],
            references_table="users",
            references_columns=["id"],
            on_delete=ForeignKeyAction.CASCADE,
        )
        new_fk = ForeignKeySnapshot(
            name="posts_owner_id_fkey",
            columns=["owner_id"],
            references_table="users",
            references_columns=["id"],
            on_delete=ForeignKeyAction.SET_NULL,
        )

        statements = self._diff_with_fk(old_fk, new_fk)

        drops = [s for s in statements if isinstance(s, DropForeignKeyStatement)]
        creates = [s for s in statements if isinstance(s, CreateForeignKeyStatement)]
        assert len(drops) == 1
        assert len(creates) == 1
        # Pydantic coerces StrEnum to its value when storing on the str-typed field
        assert creates[0].on_delete == ForeignKeyAction.SET_NULL

    def test_referenced_table_change_emits_drop_and_create(self):
        """Repointing FK to a different table → DROP + CREATE."""
        old_fk = ForeignKeySnapshot(
            name="posts_owner_id_fkey",
            columns=["owner_id"],
            references_table="users",
            references_columns=["id"],
        )
        new_fk = ForeignKeySnapshot(
            name="posts_owner_id_fkey",
            columns=["owner_id"],
            references_table="accounts",
            references_columns=["id"],
        )

        statements = self._diff_with_fk(old_fk, new_fk)

        creates = [s for s in statements if isinstance(s, CreateForeignKeyStatement)]
        assert len(creates) == 1
        assert creates[0].references_table == "accounts"

    def test_unchanged_fk_emits_nothing(self):
        fk = ForeignKeySnapshot(
            name="posts_owner_id_fkey",
            columns=["owner_id"],
            references_table="users",
            references_columns=["id"],
            on_delete=ForeignKeyAction.CASCADE,
        )
        statements = self._diff_with_fk(fk, fk.model_copy())
        assert not any(isinstance(s, DropForeignKeyStatement) for s in statements)
        assert not any(isinstance(s, CreateForeignKeyStatement) for s in statements)


class TestSnapshotDifferUniqueConstraintDefinitionChange:
    """Same-name UC whose columns changed must emit DROP + CREATE."""

    def _diff_with_uc(
        self, old_uc: UniqueConstraintSnapshot, new_uc: UniqueConstraintSnapshot
    ) -> list:
        cols = {
            "id": ColumnSnapshot(name="id", type="serial", primary_key=True),
            "tenant_id": ColumnSnapshot(name="tenant_id", type="integer"),
            "email": ColumnSnapshot(name="email", type="varchar(255)"),
        }
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns=cols,
                    unique_constraints={old_uc.name: old_uc},
                ),
            },
        )
        new = SchemaSnapshot(
            id="0001",
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns=cols,
                    unique_constraints={new_uc.name: new_uc},
                ),
            },
        )
        return SnapshotDiffer(old, new).diff()

    def test_columns_change_emits_drop_and_create(self):
        """Adding a column to the UC → DROP + CREATE."""
        old_uc = UniqueConstraintSnapshot(name="users_email_unique", columns=["email"])
        new_uc = UniqueConstraintSnapshot(
            name="users_email_unique", columns=["tenant_id", "email"]
        )

        statements = self._diff_with_uc(old_uc, new_uc)

        drops = [s for s in statements if isinstance(s, DropUniqueConstraintStatement)]
        creates = [
            s for s in statements if isinstance(s, CreateUniqueConstraintStatement)
        ]
        assert len(drops) == 1
        assert len(creates) == 1
        assert creates[0].columns == ["tenant_id", "email"]

    def test_nulls_not_distinct_change_emits_drop_and_create(self):
        old_uc = UniqueConstraintSnapshot(
            name="users_email_unique", columns=["email"], nulls_not_distinct=False
        )
        new_uc = UniqueConstraintSnapshot(
            name="users_email_unique", columns=["email"], nulls_not_distinct=True
        )

        statements = self._diff_with_uc(old_uc, new_uc)

        creates = [
            s for s in statements if isinstance(s, CreateUniqueConstraintStatement)
        ]
        assert len(creates) == 1
        assert creates[0].nulls_not_distinct is True

    def test_unchanged_uc_emits_nothing(self):
        uc = UniqueConstraintSnapshot(
            name="users_email_unique", columns=["tenant_id", "email"]
        )
        statements = self._diff_with_uc(uc, uc.model_copy())
        assert not any(isinstance(s, DropUniqueConstraintStatement) for s in statements)
        assert not any(
            isinstance(s, CreateUniqueConstraintStatement) for s in statements
        )


class TestSnapshotDifferCheckConstraints:
    """Check constraints were previously not diffed at all on ALTER paths."""

    def _diff_with_check(
        self,
        old_checks: dict[str, CheckConstraintSnapshot],
        new_checks: dict[str, CheckConstraintSnapshot],
    ) -> list:
        cols = {
            "id": ColumnSnapshot(name="id", type="serial", primary_key=True),
            "age": ColumnSnapshot(name="age", type="integer"),
        }
        old = SchemaSnapshot(
            id="0000",
            tables={
                "users": TableSnapshot(
                    name="users", columns=cols, check_constraints=old_checks
                ),
            },
        )
        new = SchemaSnapshot(
            id="0001",
            tables={
                "users": TableSnapshot(
                    name="users", columns=cols, check_constraints=new_checks
                ),
            },
        )
        return SnapshotDiffer(old, new).diff()

    def test_create_check_constraint(self):
        """Adding a check constraint to an existing table → CREATE."""
        statements = self._diff_with_check(
            {},
            {
                "users_age_positive": CheckConstraintSnapshot(
                    name="users_age_positive", expression="age > 0"
                ),
            },
        )
        creates = [
            s for s in statements if isinstance(s, CreateCheckConstraintStatement)
        ]
        assert len(creates) == 1
        assert creates[0].name == "users_age_positive"
        assert creates[0].expression == "age > 0"

    def test_drop_check_constraint(self):
        """Removing a check constraint → DROP."""
        statements = self._diff_with_check(
            {
                "users_age_positive": CheckConstraintSnapshot(
                    name="users_age_positive", expression="age > 0"
                ),
            },
            {},
        )
        drops = [s for s in statements if isinstance(s, DropCheckConstraintStatement)]
        assert len(drops) == 1
        assert drops[0].name == "users_age_positive"

    def test_expression_change_emits_drop_and_create(self):
        """Tightening the predicate → DROP + CREATE (same name, new expression)."""
        statements = self._diff_with_check(
            {
                "users_age_positive": CheckConstraintSnapshot(
                    name="users_age_positive", expression="age > 0"
                ),
            },
            {
                "users_age_positive": CheckConstraintSnapshot(
                    name="users_age_positive", expression="age >= 18"
                ),
            },
        )
        drops = [s for s in statements if isinstance(s, DropCheckConstraintStatement)]
        creates = [
            s for s in statements if isinstance(s, CreateCheckConstraintStatement)
        ]
        assert len(drops) == 1
        assert len(creates) == 1
        assert creates[0].expression == "age >= 18"

    def test_unchanged_check_emits_nothing(self):
        cc = {
            "users_age_positive": CheckConstraintSnapshot(
                name="users_age_positive", expression="age > 0"
            ),
        }
        statements = self._diff_with_check(cc, dict(cc))
        assert not any(
            isinstance(
                s, (DropCheckConstraintStatement, CreateCheckConstraintStatement)
            )
            for s in statements
        )
