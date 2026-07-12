"""Tests for table-level UNIQUE constraints and their snapshot round-trip."""

from __future__ import annotations

import pytest

from derp.orm import UUID, Constraint, Field, Index, Table, Unique, Varchar
from derp.orm.column.base import Fn
from derp.orm.migrations.errors import SchemaError
from derp.orm.migrations.snapshot.differ import SnapshotDiffer
from derp.orm.migrations.snapshot.models import UniqueConstraintSnapshot
from derp.orm.migrations.snapshot.normalize import get_normalizer
from derp.orm.migrations.snapshot.serializer import serialize_schema, serialize_table


class VendorConversation(Table, table="vendor_conversations"):
    id: UUID = Field(primary=True, default=Fn.gen_random_uuid())
    staging_request_id: Varchar[64] = Field()
    vendor_id: Varchar[64] = Field()

    @classmethod
    def constraints(cls):
        return [
            Unique(
                "staging_request_id",
                "vendor_id",
                name="vendor_conversations_staging_vendor_unique",
            )
        ]


class Membership(Table, table="memberships"):
    id: UUID = Field(primary=True, default=Fn.gen_random_uuid())
    org_id: Varchar[64] = Field()
    user_id: Varchar[64] = Field()

    @classmethod
    def constraints(cls):
        return [Unique(cls.org_id, cls.user_id)]


class Slot(Table, table="slots"):
    id: UUID = Field(primary=True, default=Fn.gen_random_uuid())
    room: Varchar[64] = Field()
    starts_at: Varchar[64] = Field()

    @classmethod
    def constraints(cls):
        return [Unique("room", "starts_at", nulls_distinct=False)]


class TestConstraintBase:
    def test_unique_is_a_constraint(self):
        assert isinstance(Unique("a"), Constraint)

    def test_base_class_is_abstract(self):
        with pytest.raises(TypeError, match="abstract"):
            Constraint()

    def test_subclass_must_implement_both_hooks(self):
        class Partial(Constraint):
            def auto_name(self, table_name: str) -> str:
                return "partial"

        with pytest.raises(TypeError, match="abstract"):
            Partial()

    def test_constraints_are_slotted(self):
        """The base must declare ``__slots__`` too, or every subclass silently
        regains a ``__dict__``."""
        assert not hasattr(Unique("a"), "__dict__")

    def test_serializer_rejects_unknown_constraint_type(self):
        class Exclude(Constraint):
            def auto_name(self, table_name: str) -> str:
                return "excl"

            def to_ddl(self, table_name: str) -> str:
                return "CONSTRAINT excl EXCLUDE (room WITH =)"

        class Booking(Table, table="bookings"):
            id: UUID = Field(primary=True)
            room: Varchar[8] = Field()

            @classmethod
            def constraints(cls):
                return [Exclude()]

        with pytest.raises(SchemaError, match="cannot serialize constraint"):
            serialize_table(Booking)


class TestUniqueDeclaration:
    def test_columns_accepts_strings(self):
        uc = Unique("a", "b")
        assert uc.columns == ("a", "b")

    def test_columns_accepts_column_descriptors(self):
        uc = Unique(Membership.org_id, Membership.user_id)
        assert uc.columns == ("org_id", "user_id")

    def test_requires_at_least_one_column(self):
        with pytest.raises(ValueError, match="at least one column"):
            Unique()

    def test_auto_name_follows_postgres_convention(self):
        assert Unique("org_id", "user_id").auto_name("memberships") == (
            "memberships_org_id_user_id_key"
        )

    def test_explicit_name_wins(self):
        uc = Unique("a", "b", name="my_constraint")
        assert uc.auto_name("things") == "my_constraint"

    def test_single_column_auto_name_matches_field_unique_naming(self):
        """``Unique("email")`` must produce the same name the column-level
        ``Field(unique=True)`` path emits, or the two would collide."""
        assert Unique("email").auto_name("users") == "users_email_key"


class TestUniqueSerialization:
    def test_emitted_as_constraint_not_index(self):
        table = serialize_table(VendorConversation)
        assert "vendor_conversations_staging_vendor_unique" in table.unique_constraints
        assert table.indexes == {}

    def test_constraint_carries_column_order(self):
        table = serialize_table(VendorConversation)
        uc = table.unique_constraints["vendor_conversations_staging_vendor_unique"]
        assert uc.columns == ["staging_request_id", "vendor_id"]

    def test_auto_named_constraint(self):
        table = serialize_table(Membership)
        assert "memberships_org_id_user_id_key" in table.unique_constraints

    def test_nulls_distinct_false_sets_nulls_not_distinct(self):
        table = serialize_table(Slot)
        uc = table.unique_constraints["slots_room_starts_at_key"]
        assert uc.nulls_not_distinct is True

    def test_duplicate_constraint_name_raises(self):
        class Dupe(Table, table="dupes"):
            id: UUID = Field(primary=True)
            email: Varchar[64] = Field(unique=True)

            @classmethod
            def constraints(cls):
                # Collides with the column-level Field(unique=True) name.
                return [Unique("email")]

        with pytest.raises(SchemaError, match="duplicate unique constraint"):
            serialize_table(Dupe)

    def test_unique_index_still_supported_separately(self):
        class Indexed(Table, table="indexed"):
            id: UUID = Field(primary=True)
            a: Varchar[8] = Field()
            b: Varchar[8] = Field()

            @classmethod
            def indexes(cls):
                return [Index("a", "b", unique=True)]

        table = serialize_table(Indexed)
        assert table.unique_constraints == {}
        assert table.indexes["uniq_indexed_a_b"].unique is True


class TestUniqueDDL:
    def test_create_table_emits_table_constraint(self):
        ddl = VendorConversation.to_ddl()
        assert (
            "CONSTRAINT vendor_conversations_staging_vendor_unique "
            "UNIQUE (staging_request_id, vendor_id)"
        ) in ddl

    def test_create_table_does_not_emit_unique_index(self):
        assert "CREATE UNIQUE INDEX" not in VendorConversation.to_ddl()

    def test_nulls_not_distinct_in_ddl(self):
        assert "UNIQUE NULLS NOT DISTINCT (room, starts_at)" in Slot.to_ddl()


class TestNoDestructiveRewrite:
    """The regression the downstream monkey-patch was working around.

    A live database whose uniqueness is a real table constraint must diff
    clean against the serialized Table, with no DROP CONSTRAINT / CREATE
    UNIQUE INDEX churn.
    """

    @staticmethod
    def _live_snapshot():
        """Shape a snapshot the way PostgresIntrospector would report it:
        the uniqueness lands in ``unique_constraints`` and ``_get_indexes``
        excludes the constraint-backed index.
        """
        live = serialize_schema([VendorConversation])
        table = live.tables["vendor_conversations"]
        table.indexes.clear()
        table.unique_constraints.clear()
        table.unique_constraints["vendor_conversations_staging_vendor_unique"] = (
            UniqueConstraintSnapshot(
                name="vendor_conversations_staging_vendor_unique",
                columns=["staging_request_id", "vendor_id"],
            )
        )
        return live

    def test_push_plans_nothing(self):
        normalizer = get_normalizer("postgresql")
        live = normalizer.normalize(self._live_snapshot())
        desired = normalizer.normalize(serialize_schema([VendorConversation]))

        assert SnapshotDiffer(live, desired).diff() == []

    def test_push_plans_nothing_when_db_named_the_constraint_differently(self):
        """Normalization rekeys unique constraints structurally, so a
        Postgres-assigned name must not trigger a rewrite."""
        live = self._live_snapshot()
        table = live.tables["vendor_conversations"]
        table.unique_constraints.clear()
        table.unique_constraints[
            "vendor_conversations_staging_request_id_vendor_id_key"
        ] = UniqueConstraintSnapshot(
            name="vendor_conversations_staging_request_id_vendor_id_key",
            columns=["staging_request_id", "vendor_id"],
        )

        normalizer = get_normalizer("postgresql")
        desired = normalizer.normalize(serialize_schema([VendorConversation]))

        assert SnapshotDiffer(normalizer.normalize(live), desired).diff() == []
