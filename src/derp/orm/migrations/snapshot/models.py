"""Pydantic models for database schema snapshots.

These models represent the complete state of a database schema at a point in time,
matching Drizzle's snapshot format for PostgreSQL.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, Field, PrivateAttr


class SnapshotVersion(StrEnum):
    """Snapshot format version."""

    V1 = "1"


class ForeignKeyAction(StrEnum):
    """Foreign key referential actions."""

    CASCADE = "cascade"
    SET_NULL = "set null"
    SET_DEFAULT = "set default"
    RESTRICT = "restrict"
    NO_ACTION = "no action"


class IndexMethod(StrEnum):
    """PostgreSQL index methods."""

    BTREE = "btree"
    HASH = "hash"
    GIN = "gin"
    GIST = "gist"
    SPGIST = "spgist"
    BRIN = "brin"


class IdentityGeneration(StrEnum):
    """Identity column generation type."""

    ALWAYS = "always"
    BY_DEFAULT = "by default"


class IdentityConfig(BaseModel):
    """IDENTITY column configuration."""

    generation: IdentityGeneration = IdentityGeneration.BY_DEFAULT
    start: int = 1
    increment: int = 1
    min_value: int | None = None
    max_value: int | None = None
    cycle: bool = False


class ColumnSnapshot(BaseModel):
    """Serialized column definition."""

    name: str
    type: str  # e.g., "varchar(255)", "serial", "integer"
    primary_key: bool = False
    not_null: bool = True
    unique: bool = False
    default: str | None = None  # SQL expression or literal
    generated: str | None = None  # Generated column expression (STORED)
    identity: IdentityConfig | None = None
    array_dimensions: int = 0  # For array types

    class ConfigDict:
        use_enum_values = True


class ForeignKeySnapshot(BaseModel):
    """Serialized foreign key constraint."""

    name: str
    columns: list[str]
    references_schema: str = "public"
    references_table: str
    references_columns: list[str]
    on_delete: ForeignKeyAction | None = None
    on_update: ForeignKeyAction | None = None
    deferrable: bool = False
    initially_deferred: bool = False

    class ConfigDict:
        use_enum_values = True


class IndexSnapshot(BaseModel):
    """Serialized index definition."""

    name: str
    columns: list[str]
    unique: bool = False
    where: str | None = None  # Partial index condition
    method: IndexMethod = IndexMethod.BTREE
    concurrently: bool = False
    nulls_not_distinct: bool = False  # PostgreSQL 15+
    include: list[str] = Field(default_factory=list)  # INCLUDE columns
    with_options: dict[str, str] = Field(default_factory=dict)  # WITH (option=value)

    class ConfigDict:
        use_enum_values = True


class UniqueConstraintSnapshot(BaseModel):
    """Serialized unique constraint."""

    name: str
    columns: list[str]
    nulls_not_distinct: bool = False  # PostgreSQL 15+


class CheckConstraintSnapshot(BaseModel):
    """Serialized check constraint."""

    name: str
    expression: str


class PrimaryKeySnapshot(BaseModel):
    """Serialized primary key constraint."""

    name: str | None = None
    columns: list[str]


class TableSnapshot(BaseModel):
    """Serialized table definition."""

    name: str
    schema_name: str = Field(default="public", alias="schema")
    columns: dict[str, ColumnSnapshot] = Field(default_factory=dict)
    primary_key: PrimaryKeySnapshot | None = None
    foreign_keys: dict[str, ForeignKeySnapshot] = Field(default_factory=dict)
    indexes: dict[str, IndexSnapshot] = Field(default_factory=dict)
    unique_constraints: dict[str, UniqueConstraintSnapshot] = Field(
        default_factory=dict
    )
    check_constraints: dict[str, CheckConstraintSnapshot] = Field(default_factory=dict)
    rls_enabled: bool = False
    rls_forced: bool = False

    class ConfigDict:
        populate_by_name = True


class EnumSnapshot(BaseModel):
    """Serialized PostgreSQL enum type."""

    name: str
    schema_name: str = Field(default="public", alias="schema")
    values: list[str]

    class ConfigDict:
        populate_by_name = True


class SequenceSnapshot(BaseModel):
    """Serialized sequence definition."""

    name: str
    schema_name: str = Field(default="public", alias="schema")
    start: int = 1
    increment: int = 1
    min_value: int | None = None
    max_value: int | None = None
    cache: int = 1
    cycle: bool = False
    owned_by: str | None = None  # "table.column" or None

    class ConfigDict:
        populate_by_name = True


class PolicyCommand(StrEnum):
    """RLS policy commands."""

    ALL = "all"
    SELECT = "select"
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


class PolicySnapshot(BaseModel):
    """Serialized Row-Level Security policy."""

    name: str
    schema_name: str = Field(default="public", alias="schema")
    table: str
    command: PolicyCommand = PolicyCommand.ALL
    permissive: bool = True  # PERMISSIVE vs RESTRICTIVE
    roles: list[str] = Field(default_factory=lambda: ["public"])
    using: str | None = None  # USING expression
    with_check: str | None = None  # WITH CHECK expression

    class ConfigDict:
        populate_by_name = True
        use_enum_values = True


class RoleSnapshot(BaseModel):
    """Serialized database role."""

    name: str
    superuser: bool = False
    create_db: bool = False
    create_role: bool = False
    inherit: bool = True
    login: bool = False
    replication: bool = False
    bypass_rls: bool = False
    connection_limit: int = -1
    password: str | None = None  # Not stored in snapshots, just for creation
    valid_until: str | None = None
    in_roles: list[str] = Field(default_factory=list)


class GrantSnapshot(BaseModel):
    """Serialized grant/permission."""

    grantee: str  # Role name
    object_type: str  # "table", "schema", "sequence", "function"
    object_schema: str = "public"
    object_name: str
    privileges: list[str]  # ["SELECT", "INSERT", "UPDATE", "DELETE", "ALL"]
    with_grant_option: bool = False


class SchemaSnapshot(BaseModel):
    """Complete database schema snapshot.

    This is the root model that captures the entire database state.
    """

    version: SnapshotVersion = SnapshotVersion.V1
    dialect: Literal["postgresql"] = "postgresql"

    # Database objects
    tables: dict[str, TableSnapshot] = Field(default_factory=dict)
    enums: dict[str, EnumSnapshot] = Field(default_factory=dict)
    sequences: dict[str, SequenceSnapshot] = Field(default_factory=dict)
    schemas: list[str] = Field(default_factory=lambda: ["public"])
    policies: dict[str, PolicySnapshot] = Field(default_factory=dict)
    roles: dict[str, RoleSnapshot] = Field(default_factory=dict)
    grants: list[GrantSnapshot] = Field(default_factory=list)

    # Metadata for linking snapshots
    id: str = ""  # Unique snapshot identifier (e.g., "0001")
    prev_id: str | None = None  # Previous snapshot ID for diffing

    # Internal metadata for rename tracking (private attribute)
    _meta: dict = PrivateAttr(
        default_factory=lambda: {"schemas": {}, "tables": {}, "columns": {}}
    )

    class ConfigDict:
        use_enum_values = True

    def get_table_key(self, schema: str, table: str) -> str:
        """Get the dict key for a table."""
        if schema == "public":
            return table
        return f"{schema}.{table}"

    def get_enum_key(self, schema: str, name: str) -> str:
        """Get the dict key for an enum."""
        if schema == "public":
            return name
        return f"{schema}.{name}"

    def get_sequence_key(self, schema: str, name: str) -> str:
        """Get the dict key for a sequence."""
        if schema == "public":
            return name
        return f"{schema}.{name}"

    def get_policy_key(self, schema: str, table: str, name: str) -> str:
        """Get the dict key for a policy."""
        table_key = self.get_table_key(schema, table)
        return f"{table_key}.{name}"
