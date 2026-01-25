"""PostgreSQL field/column type definitions for Dribble ORM."""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any


@dataclass
class FieldType:
    """Base class for all PostgreSQL field types."""

    def sql_type(self) -> str:
        """Return the SQL type string for this field."""
        raise NotImplementedError


# Integer Types


@dataclass
class Serial(FieldType):
    """Auto-incrementing 4-byte integer."""

    def sql_type(self) -> str:
        return "SERIAL"


@dataclass
class BigSerial(FieldType):
    """Auto-incrementing 8-byte integer."""

    def sql_type(self) -> str:
        return "BIGSERIAL"


@dataclass
class SmallInt(FieldType):
    """2-byte signed integer."""

    def sql_type(self) -> str:
        return "SMALLINT"


@dataclass
class Integer(FieldType):
    """4-byte signed integer."""

    def sql_type(self) -> str:
        return "INTEGER"


@dataclass
class BigInt(FieldType):
    """8-byte signed integer."""

    def sql_type(self) -> str:
        return "BIGINT"


# String Types


@dataclass
class Varchar(FieldType):
    """Variable-length string with limit."""

    length: int

    def sql_type(self) -> str:
        return f"VARCHAR({self.length})"


@dataclass
class Char(FieldType):
    """Fixed-length string."""

    length: int

    def sql_type(self) -> str:
        return f"CHAR({self.length})"


@dataclass
class Text(FieldType):
    """Variable unlimited length string."""

    def sql_type(self) -> str:
        return "TEXT"


# Boolean


@dataclass
class Boolean(FieldType):
    """Boolean type."""

    def sql_type(self) -> str:
        return "BOOLEAN"


# Temporal Types


@dataclass
class Timestamp(FieldType):
    """Timestamp without timezone."""

    with_timezone: bool = False

    def sql_type(self) -> str:
        if self.with_timezone:
            return "TIMESTAMP WITH TIME ZONE"
        return "TIMESTAMP"


@dataclass
class Date(FieldType):
    """Date type."""

    def sql_type(self) -> str:
        return "DATE"


@dataclass
class Time(FieldType):
    """Time without timezone."""

    with_timezone: bool = False

    def sql_type(self) -> str:
        if self.with_timezone:
            return "TIME WITH TIME ZONE"
        return "TIME"


@dataclass
class Interval(FieldType):
    """Time interval."""

    def sql_type(self) -> str:
        return "INTERVAL"


# Numeric Types


@dataclass
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


@dataclass
class Real(FieldType):
    """4-byte floating point."""

    def sql_type(self) -> str:
        return "REAL"


@dataclass
class DoublePrecision(FieldType):
    """8-byte floating point."""

    def sql_type(self) -> str:
        return "DOUBLE PRECISION"


# UUID


@dataclass
class UUID(FieldType):
    """UUID type."""

    def sql_type(self) -> str:
        return "UUID"


# JSON Types


@dataclass
class JSON(FieldType):
    """JSON type."""

    def sql_type(self) -> str:
        return "JSON"


@dataclass
class JSONB(FieldType):
    """Binary JSON type (more efficient for queries)."""

    def sql_type(self) -> str:
        return "JSONB"


# Array Type


@dataclass
class Array(FieldType):
    """Array of another type."""

    element_type: FieldType

    def sql_type(self) -> str:
        return f"{self.element_type.sql_type()}[]"


# Foreign Key


@dataclass
class ForeignKey:
    """Foreign key reference to another table.column."""

    reference: str  # e.g., "users.id"
    on_delete: str | None = None  # CASCADE, SET NULL, RESTRICT, etc.
    on_update: str | None = None

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


@dataclass
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
    _table_name: str | None = dataclass_field(default=None, repr=False)
    _field_name: str | None = dataclass_field(default=None, repr=False)

    def is_auto_increment(self) -> bool:
        """Check if this field auto-increments."""
        return isinstance(self.field_type, Serial | BigSerial)


def Field(
    field_type: FieldType,
    *,
    primary_key: bool = False,
    unique: bool = False,
    nullable: bool | None = None,
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
    if nullable is None:
        nullable = False

    return FieldInfo(
        field_type=field_type,
        primary_key=primary_key,
        unique=unique,
        nullable=nullable,
        default=default,
        foreign_key=foreign_key,
        index=index,
    )
