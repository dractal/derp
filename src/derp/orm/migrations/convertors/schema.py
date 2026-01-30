"""Database schema (namespace) operation convertors."""

from __future__ import annotations

from derp.orm.migrations.convertors.base import (
    ConvertorRegistry,
    StatementConvertor,
    quote_identifier,
)
from derp.orm.migrations.statements.types import (
    CreateSchemaStatement,
    DropSchemaStatement,
)


class CreateSchemaConvertor(StatementConvertor[CreateSchemaStatement]):
    """Convert CREATE SCHEMA statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "create_schema"

    def convert(self, statement: CreateSchemaStatement) -> str:
        schema_name = quote_identifier(statement.name)
        sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"

        if statement.authorization:
            sql += f" AUTHORIZATION {quote_identifier(statement.authorization)}"

        return sql + ";"


class DropSchemaConvertor(StatementConvertor[DropSchemaStatement]):
    """Convert DROP SCHEMA statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "drop_schema"

    def convert(self, statement: DropSchemaStatement) -> str:
        schema_name = quote_identifier(statement.name)
        cascade = " CASCADE" if statement.cascade else ""
        return f"DROP SCHEMA IF EXISTS {schema_name}{cascade};"


# Register convertors
ConvertorRegistry.register(CreateSchemaConvertor())
ConvertorRegistry.register(DropSchemaConvertor())
