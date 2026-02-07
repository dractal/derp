"""Enum type operation convertors."""

from __future__ import annotations

from derp.orm.migrations.convertors.base import (
    ConvertorRegistry,
    StatementConvertor,
    quote_identifier,
    quote_value,
)
from derp.orm.migrations.statements.types import (
    AlterEnumAddValueStatement,
    AlterEnumRenameValueStatement,
    CreateEnumStatement,
    DropEnumStatement,
)


def _quote_enum_name(schema: str, name: str) -> str:
    """Quote an enum type name with optional schema."""
    if schema and schema != "public":
        return f"{quote_identifier(schema)}.{quote_identifier(name)}"
    return quote_identifier(name)


class CreateEnumConvertor(StatementConvertor[CreateEnumStatement]):
    """Convert CREATE ENUM statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "create_enum"

    def convert(self, statement: CreateEnumStatement) -> str:
        enum_name = _quote_enum_name(statement.schema_name, statement.name)
        values = ", ".join(quote_value(v) for v in statement.values)
        return f"CREATE TYPE {enum_name} AS ENUM ({values});"


class DropEnumConvertor(StatementConvertor[DropEnumStatement]):
    """Convert DROP ENUM statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "drop_enum"

    def convert(self, statement: DropEnumStatement) -> str:
        enum_name = _quote_enum_name(statement.schema_name, statement.name)
        cascade = " CASCADE" if statement.cascade else ""
        return f"DROP TYPE IF EXISTS {enum_name}{cascade};"


class AlterEnumAddValueConvertor(StatementConvertor[AlterEnumAddValueStatement]):
    """Convert ALTER ENUM ADD VALUE statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "alter_enum_add_value"

    def convert(self, statement: AlterEnumAddValueStatement) -> str:
        enum_name = _quote_enum_name(statement.schema_name, statement.name)
        value = quote_value(statement.value)

        sql = f"ALTER TYPE {enum_name} ADD VALUE {value}"

        if statement.before:
            sql += f" BEFORE {quote_value(statement.before)}"
        elif statement.after:
            sql += f" AFTER {quote_value(statement.after)}"

        return sql + ";"


class AlterEnumRenameValueConvertor(StatementConvertor[AlterEnumRenameValueStatement]):
    """Convert ALTER ENUM RENAME VALUE statements to SQL (PostgreSQL 10+)."""

    @property
    def statement_type(self) -> str:
        return "alter_enum_rename_value"

    def convert(self, statement: AlterEnumRenameValueStatement) -> str:
        enum_name = _quote_enum_name(statement.schema_name, statement.name)
        old_value = quote_value(statement.old_value)
        new_value = quote_value(statement.new_value)
        return f"ALTER TYPE {enum_name} RENAME VALUE {old_value} TO {new_value};"


# Register convertors
ConvertorRegistry.register(CreateEnumConvertor())
ConvertorRegistry.register(DropEnumConvertor())
ConvertorRegistry.register(AlterEnumAddValueConvertor())
ConvertorRegistry.register(AlterEnumRenameValueConvertor())
