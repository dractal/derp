"""Categorize migration statements into create / alter / drop buckets.

Used by the push command to surface a Drizzle-style summary and to filter
out destructive statements when the user picks "Skip drops, apply rest".
"""

from __future__ import annotations

import enum

from derp.orm.migrations.statements import (
    AddColumnStatement,
    AlterColumnDefaultStatement,
    AlterColumnNullableStatement,
    AlterColumnTypeStatement,
    AlterEnumAddValueStatement,
    AlterEnumRenameValueStatement,
    AlterPolicyStatement,
    AlterSequenceStatement,
    CreateCheckConstraintStatement,
    CreateEnumStatement,
    CreateForeignKeyStatement,
    CreateIndexStatement,
    CreatePolicyStatement,
    CreatePrimaryKeyStatement,
    CreateRoleStatement,
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
    DropPrimaryKeyStatement,
    DropRoleStatement,
    DropSchemaStatement,
    DropSequenceStatement,
    DropTableStatement,
    DropUniqueConstraintStatement,
    EnableRLSStatement,
    GrantStatement,
    JsonStatement,
    RecreateTableStatement,
    RenameColumnStatement,
    RenameTableStatement,
    RevokeStatement,
    Statement,
)


class StatementCategory(enum.StrEnum):
    """Bucket a Statement falls into for push-time UX."""

    CREATE = "create"
    ALTER = "alter"
    DROP = "drop"


# Tuples of statement classes per category. Kept explicit (rather than a
# name-prefix heuristic) so adding a new statement type is a deliberate
# choice — mypy/ty will flag any unhandled case in classify().
_CREATE_TYPES: tuple[type[JsonStatement], ...] = (
    CreateTableStatement,
    CreateSchemaStatement,
    CreateEnumStatement,
    CreateSequenceStatement,
    CreateIndexStatement,
    CreateForeignKeyStatement,
    CreateUniqueConstraintStatement,
    CreateCheckConstraintStatement,
    CreatePrimaryKeyStatement,
    CreatePolicyStatement,
    CreateRoleStatement,
    AddColumnStatement,
    EnableRLSStatement,
    GrantStatement,
)

_DROP_TYPES: tuple[type[JsonStatement], ...] = (
    DropTableStatement,
    DropSchemaStatement,
    DropEnumStatement,
    DropSequenceStatement,
    DropIndexStatement,
    DropForeignKeyStatement,
    DropUniqueConstraintStatement,
    DropCheckConstraintStatement,
    DropPrimaryKeyStatement,
    DropPolicyStatement,
    DropRoleStatement,
    DropColumnStatement,
    DisableRLSStatement,
    RevokeStatement,
)

_ALTER_TYPES: tuple[type[JsonStatement], ...] = (
    AlterColumnTypeStatement,
    AlterColumnNullableStatement,
    AlterColumnDefaultStatement,
    AlterEnumAddValueStatement,
    AlterEnumRenameValueStatement,
    AlterPolicyStatement,
    AlterSequenceStatement,
    RenameColumnStatement,
    RenameTableStatement,
    # RecreateTable mutates an existing table in place; surface it under
    # alter even though it includes drops internally.
    RecreateTableStatement,
)


def classify(statement: Statement) -> StatementCategory:
    """Bucket a single statement into create / alter / drop."""
    if isinstance(statement, _DROP_TYPES):
        return StatementCategory.DROP
    if isinstance(statement, _CREATE_TYPES):
        return StatementCategory.CREATE
    if isinstance(statement, _ALTER_TYPES):
        return StatementCategory.ALTER
    raise ValueError(
        f"Unclassified statement type: {type(statement).__name__}. "
        "Add it to _CREATE_TYPES, _ALTER_TYPES, or _DROP_TYPES in categorize.py."
    )


def classify_statements(
    statements: list[Statement],
) -> dict[StatementCategory, list[Statement]]:
    """Group statements by category, preserving original order within each bucket."""
    buckets: dict[StatementCategory, list[Statement]] = {
        StatementCategory.CREATE: [],
        StatementCategory.ALTER: [],
        StatementCategory.DROP: [],
    }
    for stmt in statements:
        buckets[classify(stmt)].append(stmt)
    return buckets


def is_drop(statement: Statement) -> bool:
    """True if the statement removes schema objects."""
    return isinstance(statement, _DROP_TYPES)


def filter_drops(statements: list[Statement]) -> list[Statement]:
    """Return statements with all drop-class statements removed.

    Used by `derp push` "Skip drops, apply rest" and `--skip-drops`.
    """
    return [s for s in statements if not is_drop(s)]


def describe(statement: Statement) -> str:
    """Short human-readable label for a single statement.

    Used in the categorized summary and the Review-each prompt.
    """
    match statement:
        case CreateTableStatement():
            return f"CREATE TABLE {statement.table_name}"
        case DropTableStatement():
            return f"DROP TABLE {statement.table_name}"
        case RenameTableStatement():
            return f"ALTER TABLE {statement.from_table} RENAME TO {statement.to_table}"
        case RecreateTableStatement():
            return f"RECREATE TABLE {statement.table_name}"
        case AddColumnStatement():
            return f"ADD COLUMN {statement.table_name}.{statement.column.name}"
        case DropColumnStatement():
            return f"DROP COLUMN {statement.table_name}.{statement.column_name}"
        case RenameColumnStatement():
            return (
                f"RENAME COLUMN {statement.table_name}.{statement.from_column} "
                f"-> {statement.to_column}"
            )
        case AlterColumnTypeStatement():
            return (
                f"ALTER COLUMN {statement.table_name}.{statement.column_name} "
                f"TYPE {statement.new_type}"
            )
        case AlterColumnNullableStatement():
            verb = "DROP NOT NULL" if statement.nullable else "SET NOT NULL"
            return f"ALTER COLUMN {statement.table_name}.{statement.column_name} {verb}"
        case AlterColumnDefaultStatement():
            return (
                f"ALTER COLUMN {statement.table_name}.{statement.column_name} "
                "SET/DROP DEFAULT"
            )
        case CreateForeignKeyStatement():
            return f"ADD FOREIGN KEY {statement.table_name}.{statement.name}"
        case DropForeignKeyStatement():
            return f"DROP FOREIGN KEY {statement.table_name}.{statement.name}"
        case CreateUniqueConstraintStatement():
            return f"ADD UNIQUE {statement.table_name}.{statement.name}"
        case DropUniqueConstraintStatement():
            return f"DROP UNIQUE {statement.table_name}.{statement.name}"
        case CreateCheckConstraintStatement():
            return f"ADD CHECK {statement.table_name}.{statement.name}"
        case DropCheckConstraintStatement():
            return f"DROP CHECK {statement.table_name}.{statement.name}"
        case CreatePrimaryKeyStatement():
            return f"ADD PRIMARY KEY {statement.table_name}"
        case DropPrimaryKeyStatement():
            return f"DROP PRIMARY KEY {statement.table_name}"
        case CreateIndexStatement():
            return f"CREATE INDEX {statement.name} ON {statement.table_name}"
        case DropIndexStatement():
            return f"DROP INDEX {statement.name}"
        case CreateEnumStatement():
            return f"CREATE TYPE {statement.name}"
        case DropEnumStatement():
            return f"DROP TYPE {statement.name}"
        case AlterEnumAddValueStatement():
            return f"ALTER TYPE {statement.name} ADD VALUE {statement.value}"
        case AlterEnumRenameValueStatement():
            return (
                f"ALTER TYPE {statement.name} RENAME VALUE "
                f"{statement.old_value} -> {statement.new_value}"
            )
        case CreateSequenceStatement():
            return f"CREATE SEQUENCE {statement.name}"
        case DropSequenceStatement():
            return f"DROP SEQUENCE {statement.name}"
        case AlterSequenceStatement():
            return f"ALTER SEQUENCE {statement.name}"
        case CreateSchemaStatement():
            return f"CREATE SCHEMA {statement.name}"
        case DropSchemaStatement():
            return f"DROP SCHEMA {statement.name}"
        case EnableRLSStatement():
            return f"ENABLE RLS {statement.table_name}"
        case DisableRLSStatement():
            return f"DISABLE RLS {statement.table_name}"
        case CreatePolicyStatement():
            return f"CREATE POLICY {statement.name} ON {statement.table_name}"
        case DropPolicyStatement():
            return f"DROP POLICY {statement.name} ON {statement.table_name}"
        case AlterPolicyStatement():
            return f"ALTER POLICY {statement.name} ON {statement.table_name}"
        case CreateRoleStatement():
            return f"CREATE ROLE {statement.name}"
        case DropRoleStatement():
            return f"DROP ROLE {statement.name}"
        case GrantStatement():
            privs = ", ".join(statement.privileges)
            return f"GRANT {privs} ON {statement.object_name} TO {statement.grantee}"
        case RevokeStatement():
            privs = ", ".join(statement.privileges)
            return f"REVOKE {privs} ON {statement.object_name}"
        case _:
            return type(statement).__name__
