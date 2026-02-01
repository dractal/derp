"""PostgreSQL field/column type definitions for Derp ORM."""

from __future__ import annotations

import abc
import dataclasses
import datetime
import enum as enum_lib
import re
import uuid as uuid_lib
from collections.abc import Sequence
from decimal import Decimal
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
class FieldType[T](abc.ABC):
    """Base class for PostgreSQL field types, parameterized by Python type."""

    @abc.abstractmethod
    def sql_type(self) -> str:
        """Return the SQL type string for this field."""


# Integer Types


@dataclasses.dataclass
class Serial(FieldType[int]):
    """Auto-incrementing 4-byte integer."""

    def sql_type(self) -> str:
        return "SERIAL"


@dataclasses.dataclass
class BigSerial(FieldType[int]):
    """Auto-incrementing 8-byte integer."""

    def sql_type(self) -> str:
        return "BIGSERIAL"


@dataclasses.dataclass
class SmallInt(FieldType[int]):
    """2-byte signed integer."""

    def sql_type(self) -> str:
        return "SMALLINT"


@dataclasses.dataclass
class Integer(FieldType[int]):
    """4-byte signed integer."""

    def sql_type(self) -> str:
        return "INTEGER"


@dataclasses.dataclass
class BigInt(FieldType[int]):
    """8-byte signed integer."""

    def sql_type(self) -> str:
        return "BIGINT"


# String Types


@dataclasses.dataclass
class Varchar(FieldType[str]):
    """Variable-length string with limit."""

    length: int

    def sql_type(self) -> str:
        return f"VARCHAR({self.length})"


@dataclasses.dataclass
class Char(FieldType[str]):
    """Fixed-length string."""

    length: int

    def sql_type(self) -> str:
        return f"CHAR({self.length})"


@dataclasses.dataclass
class Text(FieldType[str]):
    """Variable unlimited length string."""

    def sql_type(self) -> str:
        return "TEXT"


# Boolean


@dataclasses.dataclass
class Boolean(FieldType[bool]):
    """Boolean type."""

    def sql_type(self) -> str:
        return "BOOLEAN"


# Temporal Types


@dataclasses.dataclass
class Timestamp(FieldType[datetime.datetime]):
    """Timestamp without timezone."""

    with_timezone: bool = False

    def sql_type(self) -> str:
        if self.with_timezone:
            return "TIMESTAMP WITH TIME ZONE"
        return "TIMESTAMP"


@dataclasses.dataclass
class Date(FieldType[datetime.date]):
    """Date type."""

    def sql_type(self) -> str:
        return "DATE"


@dataclasses.dataclass
class Time(FieldType[datetime.time]):
    """Time without timezone."""

    with_timezone: bool = False

    def sql_type(self) -> str:
        if self.with_timezone:
            return "TIME WITH TIME ZONE"
        return "TIME"


@dataclasses.dataclass
class Interval(FieldType[datetime.timedelta]):
    """Time interval."""

    def sql_type(self) -> str:
        return "INTERVAL"


# Numeric Types


@dataclasses.dataclass
class Numeric(FieldType[Decimal]):
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
class Real(FieldType[float]):
    """4-byte floating point."""

    def sql_type(self) -> str:
        return "REAL"


@dataclasses.dataclass
class DoublePrecision(FieldType[float]):
    """8-byte floating point."""

    def sql_type(self) -> str:
        return "DOUBLE PRECISION"


# UUID


@dataclasses.dataclass
class UUID(FieldType[uuid_lib.UUID]):
    """UUID type."""

    def sql_type(self) -> str:
        return "UUID"


# Enum


@dataclasses.dataclass
class Enum[E: enum_lib.Enum](FieldType[E]):
    """Enum type."""

    enum: type[E]

    def sql_type(self) -> str:
        return _to_snake_case(self.enum.__name__)


# JSON Types


@dataclasses.dataclass
class JSON(FieldType[Any]):
    """JSON type."""

    def sql_type(self) -> str:
        return "JSON"


@dataclasses.dataclass
class JSONB(FieldType[Any]):
    """Binary JSON type (more efficient for queries)."""

    def sql_type(self) -> str:
        return "JSONB"


# Array Type


@dataclasses.dataclass
class Array[E](FieldType[list[E]]):
    """Array of another type."""

    element_type: FieldType[E]

    def sql_type(self) -> str:
        return f"{self.element_type.sql_type()}[]"


# Vector Types


@dataclasses.dataclass
class Vector(FieldType[list[float]]):
    """Vector type."""

    dim: int

    def sql_type(self) -> str:
        return f"VECTOR({self.dim})"


@dataclasses.dataclass
class TSVector(FieldType[str]):
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

    reference: str | type[Any]
    on_delete: ForeignKeyAction | None = None
    on_update: ForeignKeyAction | None = None

    def to_sql(self) -> str:
        """Generate SQL for foreign key constraint."""
        from derp.orm.table import Table

        if issubclass(self.reference, Table):
            table: type[Table] = self.reference
            primary_key = table.get_primary_key()
            if primary_key is None:
                raise ValueError(f"Table `{table.__name__}` has no primary key.")
            pk_name, _ = primary_key
            reference = f"{table.get_table_name()}({pk_name})"

        elif not isinstance(self.reference, str):
            raise ValueError(f"Invalid foreign key reference: {self.reference}")
        else:
            reference = self.reference.replace(".", "(") + ")"

        sql = f"REFERENCES {reference}"
        if self.on_delete:
            sql += f" ON DELETE {self.on_delete}"
        if self.on_update:
            sql += f" ON UPDATE {self.on_update}"
        return sql


# Field metadata


@dataclasses.dataclass
class FieldInfo[T]:
    """Metadata for a table field/column, parameterized by Python type."""

    field_type: FieldType[T]
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


def Field[T](
    t: FieldType[T],
    *,
    primary_key: bool = False,
    unique: bool = False,
    nullable: bool = False,
    default: Any = None,
    foreign_key: ForeignKey | None = None,
    index: bool = False,
) -> T:  # Fool the type checker into validating the type annotation.
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
        FieldInfo with Pydantic field default (typed as T for compatibility)
    """
    return FieldInfo(  # type: ignore[return-value]
        field_type=t,
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
