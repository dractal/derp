"""Base convertor class and registry for SQL generation.

Convertors transform JSON statement objects into PostgreSQL SQL strings.
"""

from __future__ import annotations

import abc

from derp.migrations.statements.types import JsonStatement, Statement


class StatementConvertor[T](abc.ABC):
    """Base class for converting JSON statements to SQL.

    Each convertor handles one type of statement and produces the
    corresponding PostgreSQL DDL.
    """

    @property
    @abc.abstractmethod
    def statement_type(self) -> str:
        """The statement type this convertor handles."""

    @abc.abstractmethod
    def convert(self, statement: T) -> str:
        """Convert statement to SQL string.

        Args:
            statement: The JSON statement to convert

        Returns:
            SQL string (may be multiple statements separated by newlines)
        """


class ConvertorRegistry:
    """Registry of statement convertors.

    This class manages the mapping from statement types to their convertors
    and provides methods for converting statements to SQL.
    """

    _convertors: dict[str, StatementConvertor] = {}

    @classmethod
    def register(cls, convertor: StatementConvertor) -> None:
        """Register a convertor for its statement type.

        Args:
            convertor: The convertor to register
        """
        cls._convertors[convertor.statement_type] = convertor

    @classmethod
    def get_convertor(cls, statement_type: str) -> StatementConvertor | None:
        """Get the convertor for a statement type.

        Args:
            statement_type: The statement type

        Returns:
            The convertor or None if not found
        """
        return cls._convertors.get(statement_type)

    @classmethod
    def convert(cls, statement: JsonStatement) -> str:
        """Convert a statement to SQL.

        Args:
            statement: The statement to convert

        Returns:
            SQL string

        Raises:
            ValueError: If no convertor is registered for the statement type
        """
        convertor = cls._convertors.get(statement.type)
        if not convertor:
            raise ValueError(
                f"No convertor registered for statement type: {statement.type}"
            )
        return convertor.convert(statement)

    @classmethod
    def convert_all(cls, statements: list[Statement]) -> str:
        """Convert multiple statements to SQL.

        Args:
            statements: List of statements to convert

        Returns:
            Combined SQL string with statements separated by newlines
        """
        sql_parts: list[str] = []
        for stmt in statements:
            sql = cls.convert(stmt)
            if sql.strip():
                sql_parts.append(sql)
        return "\n\n".join(sql_parts)


def quote_identifier(name: str) -> str:
    """Quote an identifier (table name, column name, etc.) for PostgreSQL.

    Args:
        name: The identifier to quote

    Returns:
        Quoted identifier
    """
    # Double any existing quotes
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def quote_schema_table(schema: str, table: str) -> str:
    """Quote a schema.table reference.

    Args:
        schema: Schema name
        table: Table name

    Returns:
        Quoted schema.table reference
    """
    if schema == "public":
        return quote_identifier(table)
    return f"{quote_identifier(schema)}.{quote_identifier(table)}"


def quote_value(value: str | None) -> str:
    """Quote a string value for SQL.

    Args:
        value: The value to quote

    Returns:
        Quoted value or NULL
    """
    if value is None:
        return "NULL"
    # Escape single quotes
    escaped = value.replace("'", "''")
    return f"'{escaped}'"
