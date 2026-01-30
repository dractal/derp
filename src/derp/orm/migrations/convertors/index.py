"""Index operation convertors."""

from __future__ import annotations

from derp.orm.migrations.convertors.base import (
    ConvertorRegistry,
    StatementConvertor,
    quote_identifier,
    quote_schema_table,
)
from derp.orm.migrations.statements.types import (
    CreateIndexStatement,
    DropIndexStatement,
)


class CreateIndexConvertor(StatementConvertor[CreateIndexStatement]):
    """Convert CREATE INDEX statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "create_index"

    def convert(self, statement: CreateIndexStatement) -> str:
        parts = ["CREATE"]

        if statement.unique:
            parts.append("UNIQUE")

        parts.append("INDEX")

        if statement.concurrently:
            parts.append("CONCURRENTLY")

        parts.append(quote_identifier(statement.name))

        table_ref = quote_schema_table(statement.schema, statement.table_name)
        parts.append(f"ON {table_ref}")

        # Index method
        if statement.method and statement.method.lower() != "btree":
            parts.append(f"USING {statement.method.upper()}")

        # Columns
        cols = ", ".join(quote_identifier(c) for c in statement.columns)
        parts.append(f"({cols})")

        # INCLUDE columns
        if statement.include:
            include_cols = ", ".join(quote_identifier(c) for c in statement.include)
            parts.append(f"INCLUDE ({include_cols})")

        # NULLS NOT DISTINCT (PostgreSQL 15+)
        if statement.nulls_not_distinct:
            parts.append("NULLS NOT DISTINCT")

        # WHERE clause for partial indexes
        if statement.where:
            parts.append(f"WHERE {statement.where}")

        return " ".join(parts) + ";"


class DropIndexConvertor(StatementConvertor[DropIndexStatement]):
    """Convert DROP INDEX statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "drop_index"

    def convert(self, statement: DropIndexStatement) -> str:
        parts = ["DROP INDEX"]

        if statement.concurrently:
            parts.append("CONCURRENTLY")

        parts.append("IF EXISTS")

        # Index with schema
        if statement.schema and statement.schema != "public":
            parts.append(
                f"{quote_identifier(statement.schema)}.{quote_identifier(statement.name)}"
            )
        else:
            parts.append(quote_identifier(statement.name))

        return " ".join(parts) + ";"


# Register convertors
ConvertorRegistry.register(CreateIndexConvertor())
ConvertorRegistry.register(DropIndexConvertor())
