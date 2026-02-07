"""Constraint operation convertors (FK, UNIQUE, CHECK, PK)."""

from __future__ import annotations

from derp.orm.migrations.convertors.base import (
    ConvertorRegistry,
    StatementConvertor,
    quote_identifier,
    quote_schema_table,
)
from derp.orm.migrations.statements.types import (
    CreateCheckConstraintStatement,
    CreateForeignKeyStatement,
    CreatePrimaryKeyStatement,
    CreateUniqueConstraintStatement,
    DropCheckConstraintStatement,
    DropForeignKeyStatement,
    DropPrimaryKeyStatement,
    DropUniqueConstraintStatement,
)


class CreateForeignKeyConvertor(StatementConvertor[CreateForeignKeyStatement]):
    """Convert CREATE FOREIGN KEY statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "create_foreign_key"

    def convert(self, statement: CreateForeignKeyStatement) -> str:
        table_ref = quote_schema_table(statement.schema_name, statement.table_name)
        constraint_name = quote_identifier(statement.name)

        fk_cols = ", ".join(quote_identifier(c) for c in statement.columns)
        ref_cols = ", ".join(quote_identifier(c) for c in statement.references_columns)
        ref_table = quote_schema_table(
            statement.references_schema, statement.references_table
        )

        sql = (
            f"ALTER TABLE {table_ref} ADD CONSTRAINT {constraint_name} "
            f"FOREIGN KEY ({fk_cols}) REFERENCES {ref_table}({ref_cols})"
        )

        if statement.on_delete:
            sql += f" ON DELETE {statement.on_delete.upper()}"
        if statement.on_update:
            sql += f" ON UPDATE {statement.on_update.upper()}"
        if statement.deferrable:
            sql += " DEFERRABLE"
            if statement.initially_deferred:
                sql += " INITIALLY DEFERRED"

        return sql + ";"


class DropForeignKeyConvertor(StatementConvertor[DropForeignKeyStatement]):
    """Convert DROP FOREIGN KEY statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "drop_foreign_key"

    def convert(self, statement: DropForeignKeyStatement) -> str:
        table_ref = quote_schema_table(statement.schema_name, statement.table_name)
        constraint_name = quote_identifier(statement.name)
        return f"ALTER TABLE {table_ref} DROP CONSTRAINT {constraint_name};"


class CreateUniqueConstraintConvertor(
    StatementConvertor[CreateUniqueConstraintStatement]
):
    """Convert CREATE UNIQUE CONSTRAINT statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "create_unique_constraint"

    def convert(self, statement: CreateUniqueConstraintStatement) -> str:
        table_ref = quote_schema_table(statement.schema_name, statement.table_name)
        constraint_name = quote_identifier(statement.name)
        cols = ", ".join(quote_identifier(c) for c in statement.columns)

        sql = f"ALTER TABLE {table_ref} ADD CONSTRAINT {constraint_name} UNIQUE"
        if statement.nulls_not_distinct:
            sql += " NULLS NOT DISTINCT"
        sql += f" ({cols})"

        return sql + ";"


class DropUniqueConstraintConvertor(StatementConvertor[DropUniqueConstraintStatement]):
    """Convert DROP UNIQUE CONSTRAINT statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "drop_unique_constraint"

    def convert(self, statement: DropUniqueConstraintStatement) -> str:
        table_ref = quote_schema_table(statement.schema_name, statement.table_name)
        constraint_name = quote_identifier(statement.name)
        return f"ALTER TABLE {table_ref} DROP CONSTRAINT {constraint_name};"


class CreateCheckConstraintConvertor(
    StatementConvertor[CreateCheckConstraintStatement]
):
    """Convert CREATE CHECK CONSTRAINT statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "create_check_constraint"

    def convert(self, statement: CreateCheckConstraintStatement) -> str:
        table_ref = quote_schema_table(statement.schema_name, statement.table_name)
        constraint_name = quote_identifier(statement.name)
        return (
            f"ALTER TABLE {table_ref} ADD CONSTRAINT {constraint_name} "
            f"CHECK ({statement.expression});"
        )


class DropCheckConstraintConvertor(StatementConvertor[DropCheckConstraintStatement]):
    """Convert DROP CHECK CONSTRAINT statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "drop_check_constraint"

    def convert(self, statement: DropCheckConstraintStatement) -> str:
        table_ref = quote_schema_table(statement.schema_name, statement.table_name)
        constraint_name = quote_identifier(statement.name)
        return f"ALTER TABLE {table_ref} DROP CONSTRAINT {constraint_name};"


class CreatePrimaryKeyConvertor(StatementConvertor[CreatePrimaryKeyStatement]):
    """Convert CREATE PRIMARY KEY statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "create_pk"

    def convert(self, statement: CreatePrimaryKeyStatement) -> str:
        table_ref = quote_schema_table(statement.schema_name, statement.table_name)
        cols = ", ".join(quote_identifier(c) for c in statement.columns)

        if statement.name:
            constraint_name = quote_identifier(statement.name)
            return (
                f"ALTER TABLE {table_ref} ADD CONSTRAINT {constraint_name} "
                f"PRIMARY KEY ({cols});"
            )
        return f"ALTER TABLE {table_ref} ADD PRIMARY KEY ({cols});"


class DropPrimaryKeyConvertor(StatementConvertor[DropPrimaryKeyStatement]):
    """Convert DROP PRIMARY KEY statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "drop_pk"

    def convert(self, statement: DropPrimaryKeyStatement) -> str:
        table_ref = quote_schema_table(statement.schema_name, statement.table_name)
        constraint_name = quote_identifier(statement.name)
        return f"ALTER TABLE {table_ref} DROP CONSTRAINT {constraint_name};"


# Register convertors
ConvertorRegistry.register(CreateForeignKeyConvertor())
ConvertorRegistry.register(DropForeignKeyConvertor())
ConvertorRegistry.register(CreateUniqueConstraintConvertor())
ConvertorRegistry.register(DropUniqueConstraintConvertor())
ConvertorRegistry.register(CreateCheckConstraintConvertor())
ConvertorRegistry.register(DropCheckConstraintConvertor())
ConvertorRegistry.register(CreatePrimaryKeyConvertor())
ConvertorRegistry.register(DropPrimaryKeyConvertor())
