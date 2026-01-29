"""Sequence operation convertors."""

from __future__ import annotations

from derp.migrations.convertors.base import (
    ConvertorRegistry,
    StatementConvertor,
    quote_identifier,
)
from derp.migrations.statements.types import (
    AlterSequenceStatement,
    CreateSequenceStatement,
    DropSequenceStatement,
)


def _quote_sequence_name(schema: str, name: str) -> str:
    """Quote a sequence name with optional schema."""
    if schema and schema != "public":
        return f"{quote_identifier(schema)}.{quote_identifier(name)}"
    return quote_identifier(name)


class CreateSequenceConvertor(StatementConvertor[CreateSequenceStatement]):
    """Convert CREATE SEQUENCE statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "create_sequence"

    def convert(self, statement: CreateSequenceStatement) -> str:
        seq_name = _quote_sequence_name(statement.schema, statement.name)
        parts = [f"CREATE SEQUENCE {seq_name}"]

        if statement.start != 1:
            parts.append(f"START WITH {statement.start}")

        if statement.increment != 1:
            parts.append(f"INCREMENT BY {statement.increment}")

        if statement.min_value is not None:
            parts.append(f"MINVALUE {statement.min_value}")
        else:
            parts.append("NO MINVALUE")

        if statement.max_value is not None:
            parts.append(f"MAXVALUE {statement.max_value}")
        else:
            parts.append("NO MAXVALUE")

        if statement.cache != 1:
            parts.append(f"CACHE {statement.cache}")

        if statement.cycle:
            parts.append("CYCLE")
        else:
            parts.append("NO CYCLE")

        sql = " ".join(parts) + ";"

        # OWNED BY is a separate ALTER SEQUENCE
        if statement.owned_by:
            sql += f"\nALTER SEQUENCE {seq_name} OWNED BY {statement.owned_by};"

        return sql


class DropSequenceConvertor(StatementConvertor[DropSequenceStatement]):
    """Convert DROP SEQUENCE statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "drop_sequence"

    def convert(self, statement: DropSequenceStatement) -> str:
        seq_name = _quote_sequence_name(statement.schema, statement.name)
        cascade = " CASCADE" if statement.cascade else ""
        return f"DROP SEQUENCE IF EXISTS {seq_name}{cascade};"


class AlterSequenceConvertor(StatementConvertor[AlterSequenceStatement]):
    """Convert ALTER SEQUENCE statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "alter_sequence"

    def convert(self, statement: AlterSequenceStatement) -> str:
        seq_name = _quote_sequence_name(statement.schema, statement.name)
        parts = [f"ALTER SEQUENCE {seq_name}"]

        if statement.restart is not None:
            parts.append(f"RESTART WITH {statement.restart}")

        if statement.increment is not None:
            parts.append(f"INCREMENT BY {statement.increment}")

        if statement.min_value is not None:
            parts.append(f"MINVALUE {statement.min_value}")

        if statement.max_value is not None:
            parts.append(f"MAXVALUE {statement.max_value}")

        if statement.cache is not None:
            parts.append(f"CACHE {statement.cache}")

        if statement.cycle is not None:
            parts.append("CYCLE" if statement.cycle else "NO CYCLE")

        sql = " ".join(parts) + ";"

        if statement.owned_by is not None:
            sql += f"\nALTER SEQUENCE {seq_name} OWNED BY {statement.owned_by};"

        return sql


# Register convertors
ConvertorRegistry.register(CreateSequenceConvertor())
ConvertorRegistry.register(DropSequenceConvertor())
ConvertorRegistry.register(AlterSequenceConvertor())
