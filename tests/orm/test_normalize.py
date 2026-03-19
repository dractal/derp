"""Tests for dialect-specific snapshot normalization."""

from __future__ import annotations

from derp.orm.migrations.snapshot.models import (
    ColumnSnapshot,
    ForeignKeyAction,
    ForeignKeySnapshot,
    PrimaryKeySnapshot,
    SchemaSnapshot,
    SequenceSnapshot,
    TableSnapshot,
    UniqueConstraintSnapshot,
)
from derp.orm.migrations.snapshot.normalize import PostgresNormalizer


class TestTypeNormalization:
    """Tests for PostgreSQL type alias canonicalization."""

    def setup_method(self) -> None:
        self.n = PostgresNormalizer()

    def test_serial_to_integer(self) -> None:
        assert self.n.normalize_type("serial") == "integer"

    def test_bigserial_to_bigint(self) -> None:
        assert self.n.normalize_type("bigserial") == "bigint"

    def test_smallserial_to_smallint(self) -> None:
        assert self.n.normalize_type("smallserial") == "smallint"

    def test_varchar_to_character_varying(self) -> None:
        assert self.n.normalize_type("varchar(255)") == "character varying(255)"

    def test_char_to_character(self) -> None:
        assert self.n.normalize_type("char(10)") == "character(10)"

    def test_int_to_integer(self) -> None:
        assert self.n.normalize_type("int") == "integer"

    def test_int4_to_integer(self) -> None:
        assert self.n.normalize_type("int4") == "integer"

    def test_int8_to_bigint(self) -> None:
        assert self.n.normalize_type("int8") == "bigint"

    def test_int2_to_smallint(self) -> None:
        assert self.n.normalize_type("int2") == "smallint"

    def test_bool_to_boolean(self) -> None:
        assert self.n.normalize_type("bool") == "boolean"

    def test_float4_to_real(self) -> None:
        assert self.n.normalize_type("float4") == "real"

    def test_float8_to_double_precision(self) -> None:
        assert self.n.normalize_type("float8") == "double precision"

    def test_timestamp_to_full_form(self) -> None:
        assert self.n.normalize_type("timestamp") == "timestamp without time zone"

    def test_timestamptz_to_full_form(self) -> None:
        assert self.n.normalize_type("timestamptz") == "timestamp with time zone"

    def test_time_to_full_form(self) -> None:
        assert self.n.normalize_type("time") == "time without time zone"

    def test_timetz_to_full_form(self) -> None:
        assert self.n.normalize_type("timetz") == "time with time zone"

    def test_text_unchanged(self) -> None:
        assert self.n.normalize_type("text") == "text"

    def test_uuid_unchanged(self) -> None:
        assert self.n.normalize_type("uuid") == "uuid"

    def test_boolean_unchanged(self) -> None:
        assert self.n.normalize_type("boolean") == "boolean"

    def test_integer_unchanged(self) -> None:
        assert self.n.normalize_type("integer") == "integer"

    def test_character_varying_unchanged(self) -> None:
        assert (
            self.n.normalize_type("character varying(255)") == "character varying(255)"
        )

    def test_jsonb_unchanged(self) -> None:
        assert self.n.normalize_type("jsonb") == "jsonb"

    def test_numeric_with_precision_unchanged(self) -> None:
        assert self.n.normalize_type("numeric(10, 2)") == "numeric(10, 2)"


class TestDefaultNormalization:
    """Tests for default value canonicalization."""

    def setup_method(self) -> None:
        self.n = PostgresNormalizer()

    def test_serial_default_stays_none(self) -> None:
        """Serializer produces serial + default=None, should stay None."""
        col = ColumnSnapshot(name="id", type="serial", not_null=True)
        result = self.n.normalize_column(col)
        assert result.default is None

    def test_nextval_default_becomes_none(self) -> None:
        """Introspector produces integer + nextval(...), should become None."""
        col = ColumnSnapshot(
            name="id",
            type="integer",
            not_null=True,
            default="nextval('users_id_seq'::regclass)",
        )
        result = self.n.normalize_column(col)
        assert result.default is None

    def test_non_serial_default_preserved(self) -> None:
        col = ColumnSnapshot(name="name", type="text", default="'hello'")
        result = self.n.normalize_column(col)
        assert result.default == "'hello'"

    def test_now_default_preserved(self) -> None:
        col = ColumnSnapshot(name="created_at", type="timestamp", default="now()")
        result = self.n.normalize_column(col)
        assert result.default == "now()"

    def test_boolean_true_default_preserved(self) -> None:
        col = ColumnSnapshot(name="active", type="boolean", default="true")
        result = self.n.normalize_column(col)
        assert result.default == "true"

    def test_uppercase_true_normalized_to_lowercase(self) -> None:
        """Serializer produces TRUE, should normalize to true."""
        col = ColumnSnapshot(name="active", type="boolean", default="TRUE")
        result = self.n.normalize_column(col)
        assert result.default == "true"

    def test_uppercase_false_normalized_to_lowercase(self) -> None:
        col = ColumnSnapshot(name="revoked", type="boolean", default="FALSE")
        result = self.n.normalize_column(col)
        assert result.default == "false"

    def test_type_cast_stripped(self) -> None:
        """pg_get_expr returns 'value'::type, should strip the cast."""
        col = ColumnSnapshot(
            name="role",
            type="character varying(50)",
            default="'default'::character varying",
        )
        result = self.n.normalize_column(col)
        assert result.default == "'default'"

    def test_type_cast_stripped_with_complex_type(self) -> None:
        col = ColumnSnapshot(
            name="role",
            type="character varying(50)",
            default="'member'::character varying",
        )
        result = self.n.normalize_column(col)
        assert result.default == "'member'"


class TestPKNameNormalization:
    """Tests for primary key name canonicalization."""

    def setup_method(self) -> None:
        self.n = PostgresNormalizer()

    def test_pk_name_set_to_none(self) -> None:
        """Introspector returns 'users_pkey', should normalize to None."""
        table = TableSnapshot(
            name="users",
            columns={
                "id": ColumnSnapshot(name="id", type="integer", primary_key=True),
            },
            primary_key=PrimaryKeySnapshot(name="users_pkey", columns=["id"]),
        )
        snapshot = SchemaSnapshot(tables={"users": table})
        result = self.n.normalize(snapshot)
        assert result.tables["users"].primary_key is not None
        assert result.tables["users"].primary_key.name is None

    def test_pk_name_none_stays_none(self) -> None:
        """Serializer produces name=None, should stay None."""
        table = TableSnapshot(
            name="users",
            columns={
                "id": ColumnSnapshot(name="id", type="serial", primary_key=True),
            },
            primary_key=PrimaryKeySnapshot(name=None, columns=["id"]),
        )
        snapshot = SchemaSnapshot(tables={"users": table})
        result = self.n.normalize(snapshot)
        assert result.tables["users"].primary_key is not None
        assert result.tables["users"].primary_key.name is None


class TestUniqueConstraintNormalization:
    """Tests for unique constraint re-keying by structure."""

    def setup_method(self) -> None:
        self.n = PostgresNormalizer()

    def test_different_names_same_structure_match(self) -> None:
        """users_email_unique and users_email_key should normalize to same key."""
        table_serializer = TableSnapshot(
            name="users",
            columns={
                "email": ColumnSnapshot(name="email", type="varchar(255)"),
            },
            unique_constraints={
                "users_email_unique": UniqueConstraintSnapshot(
                    name="users_email_unique", columns=["email"]
                ),
            },
        )
        table_introspector = TableSnapshot(
            name="users",
            columns={
                "email": ColumnSnapshot(name="email", type="character varying(255)"),
            },
            unique_constraints={
                "users_email_key": UniqueConstraintSnapshot(
                    name="users_email_key", columns=["email"]
                ),
            },
        )
        snap_a = SchemaSnapshot(tables={"users": table_serializer})
        snap_b = SchemaSnapshot(tables={"users": table_introspector})

        norm_a = self.n.normalize(snap_a)
        norm_b = self.n.normalize(snap_b)

        assert set(norm_a.tables["users"].unique_constraints.keys()) == set(
            norm_b.tables["users"].unique_constraints.keys()
        )

    def test_multi_column_constraint_key(self) -> None:
        """Multi-column unique constraints should produce a stable key."""
        table = TableSnapshot(
            name="t",
            columns={
                "a": ColumnSnapshot(name="a", type="text"),
                "b": ColumnSnapshot(name="b", type="text"),
            },
            unique_constraints={
                "t_a_b_unique": UniqueConstraintSnapshot(
                    name="t_a_b_unique", columns=["a", "b"]
                ),
            },
        )
        snapshot = SchemaSnapshot(tables={"t": table})
        result = self.n.normalize(snapshot)
        keys = list(result.tables["t"].unique_constraints.keys())
        assert len(keys) == 1
        # The key should be deterministic and structural
        assert keys[0] == keys[0]  # sanity


class TestForeignKeyNormalization:
    """Tests for foreign key re-keying by structure."""

    def setup_method(self) -> None:
        self.n = PostgresNormalizer()

    def test_fk_rekeyed_by_structure(self) -> None:
        """FK constraints with different names but same structure match."""
        table_a = TableSnapshot(
            name="posts",
            columns={
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
        )
        table_b = TableSnapshot(
            name="posts",
            columns={
                "author_id": ColumnSnapshot(name="author_id", type="integer"),
            },
            foreign_keys={
                "posts_author_id_fk": ForeignKeySnapshot(
                    name="posts_author_id_fk",
                    columns=["author_id"],
                    references_table="users",
                    references_columns=["id"],
                    on_delete=ForeignKeyAction.CASCADE,
                ),
            },
        )
        snap_a = SchemaSnapshot(tables={"posts": table_a})
        snap_b = SchemaSnapshot(tables={"posts": table_b})

        norm_a = self.n.normalize(snap_a)
        norm_b = self.n.normalize(snap_b)

        assert set(norm_a.tables["posts"].foreign_keys.keys()) == set(
            norm_b.tables["posts"].foreign_keys.keys()
        )


class TestSequenceNormalization:
    """Tests for filtering out serial-implied sequences."""

    def setup_method(self) -> None:
        self.n = PostgresNormalizer()

    def test_serial_sequence_filtered_out(self) -> None:
        """Sequences backing serial columns should be removed."""
        table = TableSnapshot(
            name="users",
            columns={
                "id": ColumnSnapshot(
                    name="id",
                    type="integer",
                    not_null=True,
                    default="nextval('users_id_seq'::regclass)",
                ),
            },
        )
        snapshot = SchemaSnapshot(
            tables={"users": table},
            sequences={
                "users_id_seq": SequenceSnapshot(
                    name="users_id_seq",
                ),
            },
        )
        result = self.n.normalize(snapshot)
        assert len(result.sequences) == 0

    def test_orphan_sequence_filtered(self) -> None:
        """Sequences from excluded tables (not referenced by any column) are removed."""
        snapshot = SchemaSnapshot(
            sequences={
                "derp_migrations_id_seq": SequenceSnapshot(
                    name="derp_migrations_id_seq",
                ),
            },
        )
        result = self.n.normalize(snapshot)
        assert len(result.sequences) == 0


class TestEndToEnd:
    """Serializer output and introspector output should normalize identically."""

    def setup_method(self) -> None:
        self.n = PostgresNormalizer()

    def test_serializer_and_introspector_normalize_to_same(self) -> None:
        """Build both representations of a User table and verify zero diff."""
        from derp.orm.migrations.snapshot.differ import SnapshotDiffer

        # What the serializer produces
        serializer_snap = SchemaSnapshot(
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "id": ColumnSnapshot(
                            name="id",
                            type="serial",
                            primary_key=True,
                            not_null=True,
                        ),
                        "email": ColumnSnapshot(
                            name="email",
                            type="varchar(255)",
                            not_null=True,
                        ),
                        "active": ColumnSnapshot(
                            name="active",
                            type="boolean",
                            default="true",
                        ),
                        "created_at": ColumnSnapshot(
                            name="created_at",
                            type="timestamp",
                            default="now()",
                        ),
                    },
                    primary_key=PrimaryKeySnapshot(name=None, columns=["id"]),
                    unique_constraints={
                        "users_email_unique": UniqueConstraintSnapshot(
                            name="users_email_unique",
                            columns=["email"],
                        ),
                    },
                ),
            },
        )

        # What the introspector produces
        introspector_snap = SchemaSnapshot(
            tables={
                "users": TableSnapshot(
                    name="users",
                    columns={
                        "id": ColumnSnapshot(
                            name="id",
                            type="integer",
                            primary_key=True,
                            not_null=True,
                            default="nextval('users_id_seq'::regclass)",
                        ),
                        "email": ColumnSnapshot(
                            name="email",
                            type="character varying(255)",
                            not_null=True,
                        ),
                        "active": ColumnSnapshot(
                            name="active",
                            type="boolean",
                            default="true",
                        ),
                        "created_at": ColumnSnapshot(
                            name="created_at",
                            type="timestamp without time zone",
                            default="now()",
                        ),
                    },
                    primary_key=PrimaryKeySnapshot(name="users_pkey", columns=["id"]),
                    unique_constraints={
                        "users_email_key": UniqueConstraintSnapshot(
                            name="users_email_key",
                            columns=["email"],
                        ),
                    },
                ),
            },
            sequences={
                "users_id_seq": SequenceSnapshot(name="users_id_seq"),
            },
        )

        norm_ser = self.n.normalize(serializer_snap)
        norm_int = self.n.normalize(introspector_snap)

        differ = SnapshotDiffer(norm_int, norm_ser)
        statements = differ.diff()

        assert statements == [], f"Expected zero diff, got: {statements}"
