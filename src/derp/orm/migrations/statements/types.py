"""JSON statement intermediate representation types.

These types represent atomic database operations in a database-agnostic way.
Each statement type maps to one or more SQL statements via convertors.
"""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

# =============================================================================
# Base Statement
# =============================================================================


class JsonStatement(BaseModel):
    """Base class for all JSON statements."""

    type: str

    model_config = ConfigDict(use_enum_values=True)


# =============================================================================
# Column Definition (used in table statements)
# =============================================================================


class ColumnDefinition(BaseModel):
    """Column definition used in CREATE TABLE and ADD COLUMN statements."""

    name: str
    type: str
    primary_key: bool = False
    not_null: bool = True
    unique: bool = False
    default: str | None = None
    generated: str | None = None  # GENERATED ALWAYS AS (expr) STORED
    identity: dict | None = None  # Identity column config
    array_dimensions: int = 0


class UniqueConstraintDefinition(BaseModel):
    """Unique constraint definition."""

    name: str
    columns: list[str]
    nulls_not_distinct: bool = False


class CheckConstraintDefinition(BaseModel):
    """Check constraint definition."""

    name: str
    expression: str


class ForeignKeyDefinition(BaseModel):
    """Foreign key definition."""

    name: str
    columns: list[str]
    references_schema: str = "public"
    references_table: str
    references_columns: list[str]
    on_delete: str | None = None
    on_update: str | None = None
    deferrable: bool = False
    initially_deferred: bool = False


class PrimaryKeyDefinition(BaseModel):
    """Primary key definition."""

    name: str | None = None
    columns: list[str]


# =============================================================================
# Table Statements
# =============================================================================


class CreateTableStatement(JsonStatement):
    """CREATE TABLE statement."""

    type: str = "create_table"
    table_name: str
    schema_name: str = "public"
    columns: list[ColumnDefinition]
    primary_key: PrimaryKeyDefinition | None = None
    unique_constraints: list[UniqueConstraintDefinition] = Field(default_factory=list)
    check_constraints: list[CheckConstraintDefinition] = Field(default_factory=list)
    foreign_keys: list[ForeignKeyDefinition] = Field(default_factory=list)


class DropTableStatement(JsonStatement):
    """DROP TABLE statement."""

    type: str = "drop_table"
    table_name: str
    schema_name: str = "public"
    cascade: bool = False


class RenameTableStatement(JsonStatement):
    """RENAME TABLE statement."""

    type: str = "rename_table"
    from_table: str
    to_table: str
    schema_name: str = "public"


class RecreateTableStatement(JsonStatement):
    """Recreate table (for complex alterations that require recreation)."""

    type: str = "recreate_table"
    table_name: str
    schema_name: str = "public"
    columns: list[ColumnDefinition]
    primary_key: PrimaryKeyDefinition | None = None
    unique_constraints: list[UniqueConstraintDefinition] = Field(default_factory=list)
    check_constraints: list[CheckConstraintDefinition] = Field(default_factory=list)
    foreign_keys: list[ForeignKeyDefinition] = Field(default_factory=list)
    # Data migration: list of (old_column, new_column) mappings
    column_mapping: dict[str, str] = Field(default_factory=dict)


# =============================================================================
# Column Statements
# =============================================================================


class AddColumnStatement(JsonStatement):
    """ALTER TABLE ADD COLUMN statement."""

    type: str = "alter_table_add_column"
    table_name: str
    schema_name: str = "public"
    column: ColumnDefinition


class DropColumnStatement(JsonStatement):
    """ALTER TABLE DROP COLUMN statement."""

    type: str = "alter_table_drop_column"
    table_name: str
    schema_name: str = "public"
    column_name: str
    cascade: bool = False


class RenameColumnStatement(JsonStatement):
    """ALTER TABLE RENAME COLUMN statement."""

    type: str = "alter_table_rename_column"
    table_name: str
    schema_name: str = "public"
    from_column: str
    to_column: str


class AlterColumnTypeStatement(JsonStatement):
    """ALTER TABLE ALTER COLUMN SET DATA TYPE statement."""

    type: str = "alter_table_alter_column_set_type"
    table_name: str
    schema_name: str = "public"
    column_name: str
    old_type: str
    new_type: str
    using: str | None = None  # USING expression for type cast


class AlterColumnNullableStatement(JsonStatement):
    """ALTER TABLE ALTER COLUMN SET/DROP NOT NULL statement."""

    type: str = "alter_table_alter_column_set_nullable"
    table_name: str
    schema_name: str = "public"
    column_name: str
    nullable: bool  # True = DROP NOT NULL, False = SET NOT NULL


class AlterColumnDefaultStatement(JsonStatement):
    """ALTER TABLE ALTER COLUMN SET/DROP DEFAULT statement."""

    type: str = "alter_table_alter_column_set_default"
    table_name: str
    schema_name: str = "public"
    column_name: str
    default: str | None  # None = DROP DEFAULT


# =============================================================================
# Foreign Key Statements
# =============================================================================


class CreateForeignKeyStatement(JsonStatement):
    """ALTER TABLE ADD CONSTRAINT ... FOREIGN KEY statement."""

    type: str = "create_foreign_key"
    name: str
    table_name: str
    schema_name: str = "public"
    columns: list[str]
    references_schema: str = "public"
    references_table: str
    references_columns: list[str]
    on_delete: str | None = None
    on_update: str | None = None
    deferrable: bool = False
    initially_deferred: bool = False


class DropForeignKeyStatement(JsonStatement):
    """ALTER TABLE DROP CONSTRAINT (foreign key) statement."""

    type: str = "drop_foreign_key"
    name: str
    table_name: str
    schema_name: str = "public"


# =============================================================================
# Unique Constraint Statements
# =============================================================================


class CreateUniqueConstraintStatement(JsonStatement):
    """ALTER TABLE ADD CONSTRAINT ... UNIQUE statement."""

    type: str = "create_unique_constraint"
    name: str
    table_name: str
    schema_name: str = "public"
    columns: list[str]
    nulls_not_distinct: bool = False


class DropUniqueConstraintStatement(JsonStatement):
    """ALTER TABLE DROP CONSTRAINT (unique) statement."""

    type: str = "drop_unique_constraint"
    name: str
    table_name: str
    schema_name: str = "public"


# =============================================================================
# Check Constraint Statements
# =============================================================================


class CreateCheckConstraintStatement(JsonStatement):
    """ALTER TABLE ADD CONSTRAINT ... CHECK statement."""

    type: str = "create_check_constraint"
    name: str
    table_name: str
    schema_name: str = "public"
    expression: str


class DropCheckConstraintStatement(JsonStatement):
    """ALTER TABLE DROP CONSTRAINT (check) statement."""

    type: str = "drop_check_constraint"
    name: str
    table_name: str
    schema_name: str = "public"


# =============================================================================
# Primary Key Statements
# =============================================================================


class CreatePrimaryKeyStatement(JsonStatement):
    """ALTER TABLE ADD PRIMARY KEY statement."""

    type: str = "create_pk"
    name: str | None = None
    table_name: str
    schema_name: str = "public"
    columns: list[str]


class DropPrimaryKeyStatement(JsonStatement):
    """ALTER TABLE DROP CONSTRAINT (primary key) statement."""

    type: str = "drop_pk"
    name: str
    table_name: str
    schema_name: str = "public"


# =============================================================================
# Index Statements
# =============================================================================


class CreateIndexStatement(JsonStatement):
    """CREATE INDEX statement."""

    type: str = "create_index"
    name: str
    table_name: str
    schema_name: str = "public"
    columns: list[str]
    unique: bool = False
    where: str | None = None  # Partial index condition
    method: str = "btree"  # btree, hash, gin, gist, etc.
    concurrently: bool = False
    nulls_not_distinct: bool = False
    include: list[str] = Field(default_factory=list)  # INCLUDE columns


class DropIndexStatement(JsonStatement):
    """DROP INDEX statement."""

    type: str = "drop_index"
    name: str
    schema_name: str = "public"
    concurrently: bool = False


# =============================================================================
# Enum Statements
# =============================================================================


class CreateEnumStatement(JsonStatement):
    """CREATE TYPE ... AS ENUM statement."""

    type: str = "create_enum"
    name: str
    schema_name: str = "public"
    values: list[str]


class DropEnumStatement(JsonStatement):
    """DROP TYPE (enum) statement."""

    type: str = "drop_enum"
    name: str
    schema_name: str = "public"
    cascade: bool = False


class AlterEnumAddValueStatement(JsonStatement):
    """ALTER TYPE ... ADD VALUE statement."""

    type: str = "alter_enum_add_value"
    name: str
    schema_name: str = "public"
    value: str
    before: str | None = None  # Add before this value
    after: str | None = None  # Add after this value


class AlterEnumRenameValueStatement(JsonStatement):
    """ALTER TYPE ... RENAME VALUE statement (PostgreSQL 10+)."""

    type: str = "alter_enum_rename_value"
    name: str
    schema_name: str = "public"
    old_value: str
    new_value: str


# =============================================================================
# Sequence Statements
# =============================================================================


class CreateSequenceStatement(JsonStatement):
    """CREATE SEQUENCE statement."""

    type: str = "create_sequence"
    name: str
    schema_name: str = "public"
    start: int = 1
    increment: int = 1
    min_value: int | None = None
    max_value: int | None = None
    cache: int = 1
    cycle: bool = False
    owned_by: str | None = None  # "table.column"


class DropSequenceStatement(JsonStatement):
    """DROP SEQUENCE statement."""

    type: str = "drop_sequence"
    name: str
    schema_name: str = "public"
    cascade: bool = False


class AlterSequenceStatement(JsonStatement):
    """ALTER SEQUENCE statement."""

    type: str = "alter_sequence"
    name: str
    schema_name: str = "public"
    restart: int | None = None
    increment: int | None = None
    min_value: int | None = None
    max_value: int | None = None
    cache: int | None = None
    cycle: bool | None = None
    owned_by: str | None = None


# =============================================================================
# Schema Statements
# =============================================================================


class CreateSchemaStatement(JsonStatement):
    """CREATE SCHEMA statement."""

    type: str = "create_schema"
    name: str
    authorization: str | None = None


class DropSchemaStatement(JsonStatement):
    """DROP SCHEMA statement."""

    type: str = "drop_schema"
    name: str
    cascade: bool = False


# =============================================================================
# Row-Level Security Statements
# =============================================================================


class EnableRLSStatement(JsonStatement):
    """ALTER TABLE ENABLE ROW LEVEL SECURITY statement."""

    type: str = "enable_rls"
    table_name: str
    schema_name: str = "public"
    force: bool = False  # FORCE vs not


class DisableRLSStatement(JsonStatement):
    """ALTER TABLE DISABLE ROW LEVEL SECURITY statement."""

    type: str = "disable_rls"
    table_name: str
    schema_name: str = "public"


class CreatePolicyStatement(JsonStatement):
    """CREATE POLICY statement."""

    type: str = "create_policy"
    name: str
    table_name: str
    schema_name: str = "public"
    command: str = "ALL"  # ALL, SELECT, INSERT, UPDATE, DELETE
    permissive: bool = True  # PERMISSIVE vs RESTRICTIVE
    roles: list[str] = Field(default_factory=lambda: ["public"])
    using: str | None = None  # USING expression
    with_check: str | None = None  # WITH CHECK expression


class DropPolicyStatement(JsonStatement):
    """DROP POLICY statement."""

    type: str = "drop_policy"
    name: str
    table_name: str
    schema_name: str = "public"


class AlterPolicyStatement(JsonStatement):
    """ALTER POLICY statement."""

    type: str = "alter_policy"
    name: str
    table_name: str
    schema_name: str = "public"
    roles: list[str] | None = None
    using: str | None = None
    with_check: str | None = None


# =============================================================================
# Role Statements
# =============================================================================


class CreateRoleStatement(JsonStatement):
    """CREATE ROLE statement."""

    type: str = "create_role"
    name: str
    superuser: bool = False
    create_db: bool = False
    create_role: bool = False
    inherit: bool = True
    login: bool = False
    replication: bool = False
    bypass_rls: bool = False
    connection_limit: int = -1
    password: str | None = None
    valid_until: str | None = None
    in_roles: list[str] = Field(default_factory=list)


class DropRoleStatement(JsonStatement):
    """DROP ROLE statement."""

    type: str = "drop_role"
    name: str


class GrantStatement(JsonStatement):
    """GRANT statement."""

    type: str = "grant"
    privileges: list[str]  # SELECT, INSERT, UPDATE, DELETE, ALL, etc.
    object_type: str  # TABLE, SCHEMA, SEQUENCE, FUNCTION
    object_schema: str = "public"
    object_name: str
    grantee: str  # Role name
    with_grant_option: bool = False


class RevokeStatement(JsonStatement):
    """REVOKE statement."""

    type: str = "revoke"
    privileges: list[str]
    object_type: str
    object_schema: str = "public"
    object_name: str
    grantee: str
    cascade: bool = False


# =============================================================================
# Statement Union Type
# =============================================================================


Statement = Annotated[
    (
        # Table
        CreateTableStatement
        | DropTableStatement
        | RenameTableStatement
        | RecreateTableStatement
        # Column
        | AddColumnStatement
        | DropColumnStatement
        | RenameColumnStatement
        | AlterColumnTypeStatement
        | AlterColumnNullableStatement
        | AlterColumnDefaultStatement
        # Foreign Key
        | CreateForeignKeyStatement
        | DropForeignKeyStatement
        # Unique Constraint
        | CreateUniqueConstraintStatement
        | DropUniqueConstraintStatement
        # Check Constraint
        | CreateCheckConstraintStatement
        | DropCheckConstraintStatement
        # Primary Key
        | CreatePrimaryKeyStatement
        | DropPrimaryKeyStatement
        # Index
        | CreateIndexStatement
        | DropIndexStatement
        # Enum
        | CreateEnumStatement
        | DropEnumStatement
        | AlterEnumAddValueStatement
        | AlterEnumRenameValueStatement
        # Sequence
        | CreateSequenceStatement
        | DropSequenceStatement
        | AlterSequenceStatement
        # Schema
        | CreateSchemaStatement
        | DropSchemaStatement
        # RLS
        | EnableRLSStatement
        | DisableRLSStatement
        | CreatePolicyStatement
        | DropPolicyStatement
        | AlterPolicyStatement
        # Role
        | CreateRoleStatement
        | DropRoleStatement
        | GrantStatement
        | RevokeStatement
    ),
    Field(discriminator="type"),
]
