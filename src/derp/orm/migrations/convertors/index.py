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
    IndexColumnSpec,
)


def _column_ddl(spec: IndexColumnSpec) -> str:
    """Emit DDL for one indexed column.

    Format: ``name [COLLATE c] [opclass] [ASC|DESC] [NULLS FIRST|LAST]``.
    Mirrors ``IndexColumn.to_ddl`` so opclass / sort order / nulls position
    survive the snapshot → statement → SQL pipeline. Without this, an HNSW
    index on a ``vector`` column emits ``USING HNSW (embedding)`` and
    PostgreSQL rejects with "data type vector has no default operator class".
    """
    if spec.expression is not None:
        parts = [f"({spec.expression})"]
    elif spec.name is not None:
        parts = [quote_identifier(spec.name)]
    else:
        raise ValueError("IndexColumnSpec requires either 'name' or 'expression'.")
    if spec.collation is not None:
        parts.append(f'COLLATE "{spec.collation}"')
    if spec.opclass is not None:
        parts.append(spec.opclass)
    if spec.order is not None:
        parts.append(spec.order)
    if spec.nulls is not None:
        parts.append(f"NULLS {spec.nulls}")
    return " ".join(parts)


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

        table_ref = quote_schema_table(statement.schema_name, statement.table_name)
        parts.append(f"ON {table_ref}")

        # Index method
        if statement.method and statement.method.lower() != "btree":
            parts.append(f"USING {statement.method.upper()}")

        # Columns: prefer richer ``column_specs`` (carries opclass / sort order /
        # nulls / collation). Fall back to flat ``columns`` for callers that
        # haven't populated specs.
        if statement.column_specs:
            cols = ", ".join(_column_ddl(spec) for spec in statement.column_specs)
        else:
            cols = ", ".join(quote_identifier(c) for c in statement.columns)
        parts.append(f"({cols})")

        # INCLUDE columns
        if statement.include:
            include_cols = ", ".join(quote_identifier(c) for c in statement.include)
            parts.append(f"INCLUDE ({include_cols})")

        # NULLS NOT DISTINCT (PostgreSQL 15+)
        if statement.nulls_not_distinct:
            parts.append("NULLS NOT DISTINCT")

        # WITH (k = v, …) — index method storage parameters (e.g. HNSW's
        # ``m`` and ``ef_construction``).
        if statement.with_options:
            opts = ", ".join(f"{k} = {v}" for k, v in statement.with_options.items())
            parts.append(f"WITH ({opts})")

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
        if statement.schema_name and statement.schema_name != "public":
            parts.append(
                f"{quote_identifier(statement.schema_name)}.{quote_identifier(statement.name)}"
            )
        else:
            parts.append(quote_identifier(statement.name))

        return " ".join(parts) + ";"


# Register convertors
ConvertorRegistry.register(CreateIndexConvertor())
ConvertorRegistry.register(DropIndexConvertor())
