"""Tests for snapshot models and serialization."""

from __future__ import annotations

from enum import StrEnum

from derp.orm import (
    Boolean,
    Enum,
    Field,
    Index,
    IndexColumn,
    IndexMethod,
    Integer,
    Nullable,
    Serial,
    SortOrder,
    Table,
    Text,
    Timestamp,
    Varchar,
)
from derp.orm.migrations.snapshot.models import (
    ColumnSnapshot,
    EnumSnapshot,
    ForeignKeySnapshot,
    PrimaryKeySnapshot,
    SchemaSnapshot,
    TableSnapshot,
)
from derp.orm.migrations.snapshot.models import (
    ForeignKeyAction as SnapshotFKAction,
)
from derp.orm.migrations.snapshot.serializer import (
    extract_enums,
    serialize_column,
    serialize_schema,
    serialize_table,
)


# Test tables
class User(Table, table="users"):
    id: Serial = Field(primary=True)
    name: Varchar[255] = Field()
    email: Varchar[255] = Field(unique=True)
    is_active: Boolean = Field(default=True)
    created_at: Timestamp = Field(default="now()")


class Post(Table, table="posts"):
    id: Serial = Field(primary=True)
    title: Varchar[255] = Field()
    content: Nullable[Text] = Field()
    author_id: Integer = Field(
        foreign_key="users.id",
        on_delete="cascade",
    )

    @classmethod
    def indexes(cls) -> list[Index]:
        return [Index(cls.author_id)]


class AuditLog(Table, table="audit_logs"):
    id: Serial = Field(primary=True)
    actor_id: Integer = Field()

    @classmethod
    def indexes(cls) -> list[Index]:
        return [Index(IndexColumn(cls.actor_id))]


class TestColumnSnapshot:
    """Tests for ColumnSnapshot model."""

    def test_column_snapshot_creation(self):
        """Test creating a ColumnSnapshot."""
        col = ColumnSnapshot(
            name="id",
            type="serial",
            primary_key=True,
            not_null=True,
        )

        assert col.name == "id"
        assert col.type == "serial"
        assert col.primary_key is True
        assert col.not_null is True
        assert col.unique is False
        assert col.default is None

    def test_column_snapshot_with_default(self):
        """Test ColumnSnapshot with default value."""
        col = ColumnSnapshot(
            name="created_at",
            type="timestamp",
            default="now()",
        )

        assert col.default == "now()"

    def test_column_snapshot_serialization(self):
        """Test ColumnSnapshot JSON serialization."""
        col = ColumnSnapshot(
            name="email",
            type="varchar(255)",
            unique=True,
            not_null=True,
        )

        data = col.model_dump()

        assert data["name"] == "email"
        assert data["type"] == "varchar(255)"
        assert data["unique"] is True


class TestTableSnapshot:
    """Tests for TableSnapshot model."""

    def test_table_snapshot_creation(self):
        """Test creating a TableSnapshot."""
        table = TableSnapshot(
            name="users",
            columns={
                "id": ColumnSnapshot(name="id", type="serial", primary_key=True),
                "name": ColumnSnapshot(name="name", type="varchar(255)"),
            },
            primary_key=PrimaryKeySnapshot(columns=["id"]),
        )

        assert table.name == "users"
        assert "id" in table.columns
        assert "name" in table.columns
        assert table.primary_key is not None
        assert table.primary_key.columns == ["id"]

    def test_table_snapshot_with_foreign_key(self):
        """Test TableSnapshot with foreign key."""
        fk = ForeignKeySnapshot(
            name="posts_author_id_fkey",
            columns=["author_id"],
            references_table="users",
            references_columns=["id"],
            on_delete=SnapshotFKAction.CASCADE,
        )

        table = TableSnapshot(
            name="posts",
            columns={
                "id": ColumnSnapshot(name="id", type="serial", primary_key=True),
                "author_id": ColumnSnapshot(name="author_id", type="integer"),
            },
            foreign_keys={"posts_author_id_fkey": fk},
        )

        assert "posts_author_id_fkey" in table.foreign_keys
        assert (
            table.foreign_keys["posts_author_id_fkey"].on_delete
            == SnapshotFKAction.CASCADE
        )


class TestSchemaSnapshot:
    """Tests for SchemaSnapshot model."""

    def test_schema_snapshot_creation(self):
        """Test creating a SchemaSnapshot."""
        schema = SchemaSnapshot(
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={"id": ColumnSnapshot(name="id", type="serial")},
                ),
            },
            id="0001",
        )

        assert "users" in schema.tables
        assert schema.id == "0001"
        assert schema.dialect == "postgresql"

    def test_schema_snapshot_serialization(self):
        """Test SchemaSnapshot JSON serialization."""
        schema = SchemaSnapshot(id="0001")
        data = schema.model_dump(mode="json")

        assert data["version"] == "1"
        assert data["dialect"] == "postgresql"
        assert data["id"] == "0001"


class TestSerializeColumn:
    """Tests for serialize_column function."""

    def test_serialize_serial_column(self):
        """Test serializing a serial primary key column."""
        field_info = User.get_columns()["id"]
        col = serialize_column("id", field_info)

        assert col.name == "id"
        assert col.type == "serial"
        assert col.primary_key is True
        assert col.not_null is True

    def test_serialize_varchar_column(self):
        """Test serializing a varchar column."""
        field_info = User.get_columns()["name"]
        col = serialize_column("name", field_info)

        assert col.name == "name"
        assert col.type == "varchar(255)"
        assert col.not_null is True
        assert col.primary_key is False

    def test_serialize_unique_column(self):
        """Test serializing a unique column."""
        field_info = User.get_columns()["email"]
        col = serialize_column("email", field_info)

        assert col.unique is True

    def test_serialize_nullable_column(self):
        """Test serializing a nullable column."""
        field_info = Post.get_columns()["content"]
        col = serialize_column("content", field_info)

        assert col.not_null is False

    def test_serialize_column_with_default(self):
        """Test serializing a column with default value."""
        field_info = User.get_columns()["is_active"]
        col = serialize_column("is_active", field_info)

        assert col.default == "TRUE"


class TestSerializeTable:
    """Tests for serialize_table function."""

    def test_serialize_user_table(self):
        """Test serializing the User table."""
        table = serialize_table(User)

        assert table.name == "users"
        assert "id" in table.columns
        assert "name" in table.columns
        assert "email" in table.columns
        assert table.primary_key is not None
        assert table.primary_key.columns == ["id"]

    def test_serialize_table_with_foreign_key(self):
        """Test serializing a table with foreign key."""
        table = serialize_table(Post)

        assert table.name == "posts"
        assert len(table.foreign_keys) == 1

        fk_name = list(table.foreign_keys.keys())[0]
        fk = table.foreign_keys[fk_name]

        assert fk.columns == ["author_id"]
        assert fk.references_table == "users"
        assert fk.references_columns == ["id"]

    def test_serialize_table_with_index(self):
        """Test serializing a table with index."""
        table = serialize_table(Post)

        # author_id has an index via __indexes__
        assert len(table.indexes) >= 1

    def test_serialize_table_with_indexcolumn_column(self):
        """Test serializing table index built from IndexColumn(Column)."""
        table = serialize_table(AuditLog)

        assert len(table.indexes) == 1
        index = next(iter(table.indexes.values()))
        assert index.columns == ["actor_id"]

    def test_serialize_table_with_unique_constraint(self):
        """Test serializing a table with unique constraint."""
        table = serialize_table(User)

        # email has unique=True
        assert len(table.unique_constraints) == 1


class TestSerializeSchema:
    """Tests for serialize_schema function."""

    def test_serialize_schema_single_table(self):
        """Test serializing a schema with one table."""
        schema = serialize_schema([User])

        assert "users" in schema.tables
        assert schema.dialect == "postgresql"

    def test_serialize_schema_multiple_tables(self):
        """Test serializing a schema with multiple tables."""
        schema = serialize_schema([User, Post])

        assert "users" in schema.tables
        assert "posts" in schema.tables
        assert len(schema.tables) == 2

    def test_serialize_schema_with_id(self):
        """Test serializing a schema with snapshot ID."""
        schema = serialize_schema([User], snapshot_id="0001", prev_id="0000")

        assert schema.id == "0001"
        assert schema.prev_id == "0000"

    def test_serialize_schema_roundtrip(self):
        """Test that schema can be serialized and deserialized."""
        original = serialize_schema([User, Post], snapshot_id="0001")
        data = original.model_dump(mode="json", by_alias=True)

        restored = SchemaSnapshot.model_validate(data)

        assert restored.id == original.id
        assert set(restored.tables.keys()) == set(original.tables.keys())
        assert restored.tables["users"].name == original.tables["users"].name


# =============================================================================
# Enum extraction
# =============================================================================


class _Plan(StrEnum):
    STARTER = "starter"
    PRO = "pro"
    ENTERPRISE = "enterprise"


class _Account(Table, table="accounts"):
    id: Serial = Field(primary=True)
    plan: Enum[_Plan] = Field(default="starter")


class TestExtractEnums:
    """``extract_enums`` should pick up Enum columns and their values.

    Without this, every snapshot's ``enums`` is empty and ``derp generate``
    re-emits ``CREATE TYPE`` on each run, which fails against any DB that
    already has the type.
    """

    def test_extracts_enum_with_values(self):
        enums = extract_enums([_Account])

        assert "_plan" in enums
        snap = enums["_plan"]
        assert isinstance(snap, EnumSnapshot)
        assert snap.values == ["starter", "pro", "enterprise"]
        assert snap.schema_name == "public"

    def test_no_enum_columns_yields_empty(self):
        # User has no Enum columns
        assert extract_enums([User]) == {}

    def test_serialize_schema_includes_enums(self):
        """End-to-end: ``serialize_schema`` populates ``enums`` from Enum cols."""
        schema = serialize_schema([_Account])
        assert "_plan" in schema.enums
        assert schema.enums["_plan"].values == ["starter", "pro", "enterprise"]


# =============================================================================
# Index column metadata (opclass / sort order / WITH options)
# =============================================================================


class _Items(Table, table="items"):
    id: Serial = Field(primary=True)
    user_id: Integer = Field()
    created_at: Timestamp = Field()
    embedding: Text = Field()  # Stand-in for vector(N) — opclass is what matters

    @classmethod
    def indexes(cls) -> list[Index]:
        return [
            Index(
                cls.user_id,
                IndexColumn(cls.created_at, order=SortOrder.DESC),
                name="items_user_recent_idx",
            ),
            Index(
                IndexColumn(cls.embedding, opclass="vector_cosine_ops"),
                method=IndexMethod.HNSW,
                with_params={"m": "16", "ef_construction": "64"},
                name="items_embedding_idx",
            ),
        ]


class TestIndexColumnSpecs:
    """``serialize_table`` must capture per-column index metadata so
    opclass / ASC|DESC / NULLS FIRST|LAST / collation aren't lost."""

    def test_per_column_sort_order_captured(self):
        snap = serialize_table(_Items)
        idx = snap.indexes["items_user_recent_idx"]

        # Flat ``columns`` is preserved for backwards compat
        assert idx.columns == ["user_id", "created_at"]

        # Richer ``column_specs`` carries the order info
        assert len(idx.column_specs) == 2
        assert idx.column_specs[0].name == "user_id"
        assert idx.column_specs[0].order is None
        assert idx.column_specs[1].name == "created_at"
        assert idx.column_specs[1].order == "DESC"

    def test_opclass_captured(self):
        snap = serialize_table(_Items)
        idx = snap.indexes["items_embedding_idx"]

        assert idx.method.value == "hnsw"
        assert len(idx.column_specs) == 1
        assert idx.column_specs[0].name == "embedding"
        assert idx.column_specs[0].opclass == "vector_cosine_ops"

    def test_with_options_captured(self):
        snap = serialize_table(_Items)
        idx = snap.indexes["items_embedding_idx"]

        assert idx.with_options == {"m": "16", "ef_construction": "64"}
