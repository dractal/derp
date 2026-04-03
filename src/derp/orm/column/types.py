"""PostgreSQL type classes for Column annotations.

Each class maps 1:1 to a PostgreSQL data type and is used as the type
annotation in table definitions::

    class User(Table, table="users"):
        id: Serial = Field(primary=True)
        name: Varchar[255] = Field()
        email: Text = Field(unique=True)
"""

from __future__ import annotations

import datetime
import enum as enum_lib
import typing
import uuid as uuid_lib
from decimal import Decimal as PyDecimal
from typing import Any

from derp.orm.column.base import Column


def _unwrap_literal(val: Any) -> Any:
    """Unwrap ``Literal[x]`` to ``x`` so both ``Varchar[255]`` and
    ``Varchar[Literal[255]]`` work at runtime."""
    args = typing.get_args(val)
    if args and typing.get_origin(val) is typing.Literal:
        return args[0]
    return val


# =============================================================================
# Integer types
# =============================================================================


class Serial(Column[int]):
    """Auto-incrementing 4-byte integer (SERIAL)."""

    _sql_type = "SERIAL"


class BigSerial(Column[int]):
    """Auto-incrementing 8-byte integer (BIGSERIAL)."""

    _sql_type = "BIGSERIAL"


class SmallInt(Column[int]):
    """2-byte signed integer (SMALLINT)."""

    _sql_type = "SMALLINT"


class Integer(Column[int]):
    """4-byte signed integer (INTEGER)."""

    _sql_type = "INTEGER"


class BigInt(Column[int]):
    """8-byte signed integer (BIGINT)."""

    _sql_type = "BIGINT"


# =============================================================================
# String types
# =============================================================================


class Varchar[L](Column[str]):
    """Variable-length string with limit (VARCHAR).

    Accepts both ``Varchar[255]`` and ``Varchar[Literal[255]]``.
    """

    _sql_type = "VARCHAR"
    _length: int | None = None

    def __class_getitem__(cls, length: int) -> type[Varchar]:
        return type("Varchar", (cls,), {"_length": _unwrap_literal(length)})

    def sql_type(self) -> str:
        if self._length:
            return f"VARCHAR({self._length})"
        return "VARCHAR"


class Char(Column[str]):
    """Fixed-length string (CHAR).

    Accepts both ``Char[10]`` and ``Char[Literal[10]]``.
    """

    _sql_type = "CHAR"
    _length: int | None = None

    def __class_getitem__(cls, length: int) -> type[Char]:
        return type("Char", (cls,), {"_length": _unwrap_literal(length)})

    def sql_type(self) -> str:
        if self._length:
            return f"CHAR({self._length})"
        return "CHAR"


class Text(Column[str]):
    """Variable unlimited length string (TEXT)."""

    _sql_type = "TEXT"


# =============================================================================
# Boolean
# =============================================================================


class Boolean(Column[bool]):
    """Boolean type (BOOLEAN)."""

    _sql_type = "BOOLEAN"


# =============================================================================
# Temporal types
# =============================================================================


class Timestamp(Column[datetime.datetime]):
    """Timestamp without timezone (TIMESTAMP)."""

    _sql_type = "TIMESTAMP"


class TimestampTZ(Column[datetime.datetime]):
    """Timestamp with timezone (TIMESTAMP WITH TIME ZONE)."""

    _sql_type = "TIMESTAMP WITH TIME ZONE"


class Date(Column[datetime.date]):
    """Date type (DATE)."""

    _sql_type = "DATE"


class Time(Column[datetime.time]):
    """Time without timezone (TIME)."""

    _sql_type = "TIME"


class TimeTZ(Column[datetime.time]):
    """Time with timezone (TIME WITH TIME ZONE)."""

    _sql_type = "TIME WITH TIME ZONE"


class Interval(Column[datetime.timedelta]):
    """Time interval (INTERVAL)."""

    _sql_type = "INTERVAL"


# =============================================================================
# UUID
# =============================================================================


class UUID(Column[uuid_lib.UUID]):
    """UUID type."""

    _sql_type = "UUID"


# =============================================================================
# Numeric types
# =============================================================================


class Numeric(Column[PyDecimal]):
    """Exact numeric with precision and scale (NUMERIC)."""

    _sql_type = "NUMERIC"
    _precision: int | None = None
    _scale: int | None = None

    def __class_getitem__(cls, params: int | tuple[int, int]) -> type[Numeric]:
        if isinstance(params, tuple):
            p, s = _unwrap_literal(params[0]), _unwrap_literal(params[1])
            return type("Numeric", (cls,), {"_precision": p, "_scale": s})
        return type("Numeric", (cls,), {"_precision": _unwrap_literal(params)})

    def sql_type(self) -> str:
        if self._precision is not None and self._scale is not None:
            return f"NUMERIC({self._precision}, {self._scale})"
        if self._precision is not None:
            return f"NUMERIC({self._precision})"
        return "NUMERIC"


class Real(Column[float]):
    """4-byte floating point (REAL)."""

    _sql_type = "REAL"


class DoublePrecision(Column[float]):
    """8-byte floating point (DOUBLE PRECISION)."""

    _sql_type = "DOUBLE PRECISION"


# =============================================================================
# JSON types
# =============================================================================


class JSON(Column[Any]):
    """JSON type."""

    _sql_type = "JSON"


class JSONB(Column[Any]):
    """Binary JSON type (JSONB)."""

    _sql_type = "JSONB"


# =============================================================================
# Vector type
# =============================================================================


class Vector(Column[list[float]]):
    """Vector type for embeddings (pgvector).

    Distance methods for use in ORDER BY or WHERE clauses::

        db.select(Doc).order_by(Doc.embedding.cosine_distance(vec)).limit(10)
    """

    _sql_type = "VECTOR"
    _dim: int | None = None

    def __class_getitem__(cls, dim: int) -> type[Vector]:
        return type("Vector", (cls,), {"_dim": _unwrap_literal(dim)})

    def sql_type(self) -> str:
        if self._dim:
            return f"VECTOR({self._dim})"
        return "VECTOR"

    @staticmethod
    def _as_vector(values: Any) -> Any:
        """Convert any iterable of floats to a ``CastLiteral`` with ``::vector``."""
        from derp.orm.query.expressions import CastLiteral

        pg_str = "[" + ",".join(str(float(v)) for v in values) + "]"
        return CastLiteral(pg_str, "vector")

    def cosine_distance(self, other: Any) -> Any:
        """Cosine distance (``<=>``).  Sort ASC for most similar."""
        from derp.orm.query.expressions import BinaryOp

        return BinaryOp(self, "<=>", self._as_vector(other))

    def l2_distance(self, other: Any) -> Any:
        """L2 / Euclidean distance (``<->``).  Sort ASC for most similar."""
        from derp.orm.query.expressions import BinaryOp

        return BinaryOp(self, "<->", self._as_vector(other))

    def inner_product(self, other: Any) -> Any:
        """Negative inner product (``<#>``).  Sort ASC for most similar."""
        from derp.orm.query.expressions import BinaryOp

        return BinaryOp(self, "<#>", self._as_vector(other))


# =============================================================================
# Nullable wrapper
# =============================================================================


class Nullable[C: Column[Any]](Column[Any]):
    """Nullable column wrapper.

    Use as a type annotation to indicate a column that allows NULL::

        age: Nullable[Integer] = Field()

    At the class level ``User.age`` is a Column (supports query operators).
    At the instance level ``user.age`` is ``int | None``.

    ``Nullable[Varchar[255]]`` also works for parameterized types.
    """

    _inner: type[Column[Any]] | None = None

    def __class_getitem__(cls, inner: type[Column[Any]]) -> type[Nullable[Any]]:
        return type(
            f"Nullable_{getattr(inner, '__name__', 'col')}",
            (inner,),
            {"_inner": inner, "_nullable_marker": True},
        )


# =============================================================================
# Enum helper — derive SQL type from Python enum
# =============================================================================

_ENUM_SQL_CACHE: dict[type[enum_lib.Enum], str] = {}


def _enum_sql_name(enum_cls: type[enum_lib.Enum]) -> str:
    """Convert a Python enum class name to a PostgreSQL enum type name."""
    import re

    if enum_cls not in _ENUM_SQL_CACHE:
        pattern = re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
        _ENUM_SQL_CACHE[enum_cls] = pattern.sub("_", enum_cls.__name__).lower()
    return _ENUM_SQL_CACHE[enum_cls]


class Enum[E: enum_lib.Enum](Column[E]):
    """PostgreSQL enum column.

    Usage::

        class Status(StrEnum):
            ACTIVE = "active"
            INACTIVE = "inactive"

        status: Enum[Status] = Field(default="active")
    """

    _enum_cls: type[enum_lib.Enum] | None = None

    def __class_getitem__(cls, enum_cls: type[enum_lib.Enum]) -> type[Enum[Any]]:
        return type("Enum", (cls,), {"_enum_cls": enum_cls})

    def sql_type(self) -> str:
        if self._enum_cls is not None:
            return _enum_sql_name(self._enum_cls)
        return "TEXT"
