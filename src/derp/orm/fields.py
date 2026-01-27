"""PostgreSQL field/column type definitions for Derp ORM."""

from __future__ import annotations

import abc
import dataclasses
import enum as enum_lib
import re
from collections.abc import Sequence
from typing import Any

from etils import epy

with epy.lazy_imports():
    from derp.orm.query import expressions


class ForeignKeyAction(enum_lib.StrEnum):
    """Actions for foreign key ON DELETE / ON UPDATE clauses."""

    CASCADE = "CASCADE"
    SET_NULL = "SET NULL"
    SET_DEFAULT = "SET DEFAULT"
    RESTRICT = "RESTRICT"
    NO_ACTION = "NO ACTION"


@dataclasses.dataclass
class FieldType(abc.ABC):
    """Base class for all PostgreSQL field types."""

    @abc.abstractmethod
    def sql_type(self) -> str:
        """Return the SQL type string for this field."""


# Integer Types


@dataclasses.dataclass
class Serial(FieldType):
    """Auto-incrementing 4-byte integer."""

    def sql_type(self) -> str:
        return "SERIAL"


@dataclasses.dataclass
class BigSerial(FieldType):
    """Auto-incrementing 8-byte integer."""

    def sql_type(self) -> str:
        return "BIGSERIAL"


@dataclasses.dataclass
class SmallInt(FieldType):
    """2-byte signed integer."""

    def sql_type(self) -> str:
        return "SMALLINT"


@dataclasses.dataclass
class Integer(FieldType):
    """4-byte signed integer."""

    def sql_type(self) -> str:
        return "INTEGER"


@dataclasses.dataclass
class BigInt(FieldType):
    """8-byte signed integer."""

    def sql_type(self) -> str:
        return "BIGINT"


# String Types


@dataclasses.dataclass
class Varchar(FieldType):
    """Variable-length string with limit."""

    length: int

    def sql_type(self) -> str:
        return f"VARCHAR({self.length})"


@dataclasses.dataclass
class Char(FieldType):
    """Fixed-length string."""

    length: int

    def sql_type(self) -> str:
        return f"CHAR({self.length})"


@dataclasses.dataclass
class Text(FieldType):
    """Variable unlimited length string."""

    def sql_type(self) -> str:
        return "TEXT"


# Boolean


@dataclasses.dataclass
class Boolean(FieldType):
    """Boolean type."""

    def sql_type(self) -> str:
        return "BOOLEAN"


# Temporal Types


@dataclasses.dataclass
class Timestamp(FieldType):
    """Timestamp without timezone."""

    with_timezone: bool = False

    def sql_type(self) -> str:
        if self.with_timezone:
            return "TIMESTAMP WITH TIME ZONE"
        return "TIMESTAMP"


@dataclasses.dataclass
class Date(FieldType):
    """Date type."""

    def sql_type(self) -> str:
        return "DATE"


@dataclasses.dataclass
class Time(FieldType):
    """Time without timezone."""

    with_timezone: bool = False

    def sql_type(self) -> str:
        if self.with_timezone:
            return "TIME WITH TIME ZONE"
        return "TIME"


@dataclasses.dataclass
class Interval(FieldType):
    """Time interval."""

    def sql_type(self) -> str:
        return "INTERVAL"


# Numeric Types


@dataclasses.dataclass
class Numeric(FieldType):
    """Exact numeric with precision and scale."""

    precision: int | None = None
    scale: int | None = None

    def sql_type(self) -> str:
        if self.precision is not None and self.scale is not None:
            return f"NUMERIC({self.precision}, {self.scale})"
        elif self.precision is not None:
            return f"NUMERIC({self.precision})"
        return "NUMERIC"


@dataclasses.dataclass
class Real(FieldType):
    """4-byte floating point."""

    def sql_type(self) -> str:
        return "REAL"


@dataclasses.dataclass
class DoublePrecision(FieldType):
    """8-byte floating point."""

    def sql_type(self) -> str:
        return "DOUBLE PRECISION"


# UUID


@dataclasses.dataclass
class UUID(FieldType):
    """UUID type."""

    def sql_type(self) -> str:
        return "UUID"


# Enum


@dataclasses.dataclass
class Enum(FieldType):
    """Enum type."""

    enum: type[enum_lib.Enum]

    def sql_type(self) -> str:
        return _to_snake_case(self.enum.__name__)


# JSON Types


@dataclasses.dataclass
class JSON(FieldType):
    """JSON type."""

    def sql_type(self) -> str:
        return "JSON"


@dataclasses.dataclass
class JSONB(FieldType):
    """Binary JSON type (more efficient for queries)."""

    def sql_type(self) -> str:
        return "JSONB"


# Array Type


@dataclasses.dataclass
class Array(FieldType):
    """Array of another type."""

    element_type: FieldType

    def sql_type(self) -> str:
        return f"{self.element_type.sql_type()}[]"


# Vector Types

@dataclasses.dataclass
class Vector(FieldType):
    """Vector type."""

    dim: int

    def sql_type(self) -> str:
        return f"VECTOR({self.dim})"


@dataclasses.dataclass
class TSVector(FieldType):
    """TSVector type."""

    language: str = "english"

    def sql_type(self) -> str:
        return "TSVECTOR"
    
    def generated(self) -> str:
        return f"to_tsvector('{self.language}')"



# Foreign Key


@dataclasses.dataclass
class ForeignKey:
    """Foreign key reference to another table.column."""

    reference: str  # e.g., "users.id"
    on_delete: ForeignKeyAction | None = None
    on_update: ForeignKeyAction | None = None

    def to_sql(self) -> str:
        """Generate SQL for foreign key constraint."""
        sql = f"REFERENCES {self.reference.replace('.', '(')}"
        sql += ")"
        if self.on_delete:
            sql += f" ON DELETE {self.on_delete}"
        if self.on_update:
            sql += f" ON UPDATE {self.on_update}"
        return sql


# Field metadata


@dataclasses.dataclass
class FieldInfo:
    """Metadata for a table field/column."""

    field_type: FieldType
    primary_key: bool = False
    unique: bool = False
    nullable: bool = True
    default: Any = None
    foreign_key: ForeignKey | None = None
    index: bool = False
    # Store references to other FieldInfo for comparison expressions
    _table_name: str | None = dataclasses.field(default=None, repr=False)
    _field_name: str | None = dataclasses.field(default=None, repr=False)

    def is_auto_increment(self) -> bool:
        """Check if this field auto-increments."""
        return isinstance(self.field_type, Serial | BigSerial)

    def __eq__(self, other: Any) -> Any:
        """Equal comparison: field == value."""
        return expressions.BinaryOp(
            self,
            expressions.ComparisonOperator.EQ,
            expressions.to_expr(other),
        )

    def __ne__(self, other: Any) -> Any:
        """Not equal comparison: field != value."""
        return expressions.BinaryOp(
            self,
            expressions.ComparisonOperator.NE,
            expressions.to_expr(other),
        )

    def __lt__(self, other: Any) -> Any:
        """Less than comparison: field < value."""
        return expressions.BinaryOp(
            self,
            expressions.ComparisonOperator.LT,
            expressions.to_expr(other),
        )

    def __le__(self, other: Any) -> Any:
        """Less than or equal comparison: field <= value."""
        return expressions.BinaryOp(
            self,
            expressions.ComparisonOperator.LTE,
            expressions.to_expr(other),
        )

    def __gt__(self, other: Any) -> Any:
        """Greater than comparison: field > value."""
        return expressions.BinaryOp(
            self,
            expressions.ComparisonOperator.GT,
            expressions.to_expr(other),
        )

    def __ge__(self, other: Any) -> Any:
        """Greater than or equal comparison: field >= value."""
        return expressions.BinaryOp(
            self,
            expressions.ComparisonOperator.GTE,
            expressions.to_expr(other),
        )

    def in_(self, values: Sequence[Any]) -> Any:
        """IN clause."""
        return expressions.InList(self, tuple(values), negated=False)

    def not_in(self, values: Sequence[Any]) -> Any:
        """NOT IN clause."""
        return expressions.InList(self, tuple(values), negated=True)

    def like(self, pattern: str) -> Any:
        """Case-sensitive LIKE pattern matching."""
        return expressions.Like(self, pattern, case_insensitive=False)

    def ilike(self, pattern: str) -> Any:
        """Case-insensitive ILIKE pattern matching."""
        return expressions.Like(self, pattern, case_insensitive=True)

    def is_null(self) -> Any:
        """IS NULL check."""
        return expressions.NullCheck(self, is_null=True)

    def is_not_null(self) -> Any:
        """IS NOT NULL check."""
        return expressions.NullCheck(self, is_null=False)

    def between(self, low: Any, high: Any) -> Any:
        """BETWEEN range check."""
        return expressions.Between(self, low, high)


def Field(
    field_type: FieldType,
    *,
    primary_key: bool = False,
    unique: bool = False,
    nullable: bool = False,
    default: Any = None,
    foreign_key: ForeignKey | None = None,
    index: bool = False,
) -> Any:
    """Create a field definition for a table column.

    Args:
        field_type: The PostgreSQL type for this field
        primary_key: Whether this is the primary key
        unique: Whether values must be unique
        nullable: Whether NULL is allowed (defaults to False for NOT NULL)
        default: Default value or SQL expression (e.g., "now()")
        foreign_key: Foreign key reference
        index: Whether to create an index on this column

    Returns:
        FieldInfo with Pydantic field default
    """
    return FieldInfo(
        field_type=field_type,
        primary_key=primary_key,
        unique=unique,
        nullable=nullable,
        default=default,
        foreign_key=foreign_key,
        index=index,
    )


def _to_snake_case(name: str) -> str:
    """Converts a string (e.g., CamelCase) to snake case."""
    # Use regex to insert an underscore before any uppercase letter that
    # is either preceded by a lowercase letter, or followed by another
    # uppercase letter which is then followed by a lowercase letter. This
    # helps to handle acronyms correctly (e.g., "HTTPHeader" -> "http_header"
    # instead of "h_t_t_p_header").
    pattern = re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
    snake_case_name = pattern.sub("_", name).lower()
    return snake_case_name
