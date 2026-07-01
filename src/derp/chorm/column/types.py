"""ClickHouse type classes for Column annotations.

Each class maps 1:1 to a ClickHouse data type and is used as the
type annotation in table definitions::

    class Event(Table, table="events"):
        id: UInt64 = Field()
        name: String = Field()
        ts: DateTime64[6] = Field(default=Fn.now64(6))
        tags: Array[String] = Field()
        props: Map[String, String] = Field()
"""

from __future__ import annotations

import datetime
import enum as enum_lib
import ipaddress as _ip
import typing
import uuid as uuid_lib
from decimal import Decimal as PyDecimal
from typing import Any

from derp.chorm.column.base import Column, _wrap_modifiers


def _unwrap_literal(val: Any) -> Any:
    """Unwrap ``Literal[x]`` to ``x``."""
    args = typing.get_args(val)
    if args and typing.get_origin(val) is typing.Literal:
        return args[0]
    return val


def _type_of(t: Any) -> str:
    """Render the SQL type for *t* — a Column subclass or a Column instance.

    Allows callers like ``Array[String]`` to construct a class and have
    it render correctly without a Field/Column instance.
    """
    if isinstance(t, Column):
        return t.sql_type()
    if isinstance(t, type) and issubclass(t, Column):
        # Build a temporary instance through the class default to invoke
        # ``sql_type()`` (no instance state needed).
        return _class_sql_type(t)
    return str(t)


def _class_sql_type(cls: type[Column[Any]]) -> str:
    """Render a Column subclass's SQL type without instantiating it.

    Uses the class-level ``_sql_type`` plus class-level params set by
    ``__class_getitem__``.
    """
    # Delegate to instance method when a Column subclass exposes
    # parameterized state via class attributes.  Build a thin instance
    # that re-uses the class's class-level attributes. The Nullable[...]/
    # LowCardinality[...] markers must be honoured here too, otherwise a
    # modifier nested inside a composite (e.g. Array[Nullable[String]]) is
    # silently dropped.
    inst = cls.__new__(cls)
    object.__setattr__(inst, "_nullable", bool(getattr(cls, "_nullable_marker", False)))
    object.__setattr__(
        inst, "_low_cardinality", bool(getattr(cls, "_low_cardinality_marker", False))
    )
    return inst.sql_type()


# =============================================================================
# Unsigned integer types
# =============================================================================


class UInt8(Column[int]):
    """8-bit unsigned integer."""

    _sql_type = "UInt8"


class UInt16(Column[int]):
    _sql_type = "UInt16"


class UInt32(Column[int]):
    _sql_type = "UInt32"


class UInt64(Column[int]):
    _sql_type = "UInt64"


class UInt128(Column[int]):
    _sql_type = "UInt128"


class UInt256(Column[int]):
    _sql_type = "UInt256"


# =============================================================================
# Signed integer types
# =============================================================================


class Int8(Column[int]):
    _sql_type = "Int8"


class Int16(Column[int]):
    _sql_type = "Int16"


class Int32(Column[int]):
    _sql_type = "Int32"


class Int64(Column[int]):
    _sql_type = "Int64"


class Int128(Column[int]):
    _sql_type = "Int128"


class Int256(Column[int]):
    _sql_type = "Int256"


# =============================================================================
# Float types
# =============================================================================


class Float32(Column[float]):
    _sql_type = "Float32"


class Float64(Column[float]):
    _sql_type = "Float64"


# =============================================================================
# Decimal types (parameterized)
# =============================================================================


class Decimal(Column[PyDecimal]):
    """Generic ``Decimal(P, S)`` type."""

    _sql_type = "Decimal"
    _precision: int | None = None
    _scale: int | None = None

    def __class_getitem__(cls, params: tuple[int, int]) -> type[Decimal]:
        if not isinstance(params, tuple) or len(params) != 2:
            raise TypeError("Decimal requires (precision, scale)")
        p = _unwrap_literal(params[0])
        s = _unwrap_literal(params[1])
        return type("Decimal", (cls,), {"_precision": p, "_scale": s})

    def sql_type(self) -> str:
        base = "Decimal"
        if self._precision is not None and self._scale is not None:
            base = f"Decimal({self._precision}, {self._scale})"
        elif self._precision is not None:
            base = f"Decimal({self._precision})"
        return _wrap_modifiers(
            base, nullable=self._nullable, low_cardinality=self._low_cardinality
        )


class _DecimalWidth(Decimal):
    """Internal base for ``Decimal32/64/128/256``."""

    _width: int = 0
    _sql_type = "Decimal"

    def __class_getitem__(cls, scale: int) -> type[_DecimalWidth]:  # type: ignore[override]
        s = _unwrap_literal(scale)
        return type(cls.__name__, (cls,), {"_scale": s})

    def sql_type(self) -> str:
        base = f"Decimal{self._width}({self._scale or 0})"
        return _wrap_modifiers(
            base, nullable=self._nullable, low_cardinality=self._low_cardinality
        )


class Decimal32(_DecimalWidth):
    _width = 32


class Decimal64(_DecimalWidth):
    _width = 64


class Decimal128(_DecimalWidth):
    _width = 128


class Decimal256(_DecimalWidth):
    _width = 256


# =============================================================================
# Boolean
# =============================================================================


class Bool(Column[bool]):
    """ClickHouse ``Bool`` type (alias for ``UInt8`` semantically)."""

    _sql_type = "Bool"


# =============================================================================
# String types
# =============================================================================


class String(Column[str]):
    """Variable-length UTF-8 string."""

    _sql_type = "String"


class FixedString(Column[bytes]):
    """Fixed-length byte string (``FixedString(N)``)."""

    _sql_type = "FixedString"
    _length: int | None = None

    def __class_getitem__(cls, length: int) -> type[FixedString]:
        n = _unwrap_literal(length)
        return type("FixedString", (cls,), {"_length": n})

    def sql_type(self) -> str:
        base = f"FixedString({self._length})" if self._length else "FixedString"
        return _wrap_modifiers(
            base, nullable=self._nullable, low_cardinality=self._low_cardinality
        )


# =============================================================================
# Temporal types
# =============================================================================


class Date(Column[datetime.date]):
    """``Date`` — 2-byte unsigned, days since 1970-01-01."""

    _sql_type = "Date"


class Date32(Column[datetime.date]):
    """``Date32`` — 4-byte, extended range."""

    _sql_type = "Date32"


class DateTime(Column[datetime.datetime]):
    """``DateTime`` (optional timezone)."""

    _sql_type = "DateTime"
    _tz: str | None = None

    def __class_getitem__(cls, tz: str) -> type[DateTime]:
        return type("DateTime", (cls,), {"_tz": _unwrap_literal(tz)})

    def sql_type(self) -> str:
        base = f"DateTime('{self._tz}')" if self._tz else "DateTime"
        return _wrap_modifiers(
            base, nullable=self._nullable, low_cardinality=self._low_cardinality
        )


class DateTime64(Column[datetime.datetime]):
    """``DateTime64(precision[, tz])`` with sub-second precision."""

    _sql_type = "DateTime64"
    _precision: int = 3
    _tz: str | None = None

    def __class_getitem__(cls, params: int | tuple[int, str]) -> type[DateTime64]:
        if isinstance(params, tuple):
            p = _unwrap_literal(params[0])
            tz = _unwrap_literal(params[1])
            return type("DateTime64", (cls,), {"_precision": p, "_tz": tz})
        return type("DateTime64", (cls,), {"_precision": _unwrap_literal(params)})

    def sql_type(self) -> str:
        if self._tz:
            base = f"DateTime64({self._precision}, '{self._tz}')"
        else:
            base = f"DateTime64({self._precision})"
        return _wrap_modifiers(
            base, nullable=self._nullable, low_cardinality=self._low_cardinality
        )


# =============================================================================
# UUID
# =============================================================================


class UUID(Column[uuid_lib.UUID]):
    """``UUID``."""

    _sql_type = "UUID"


# =============================================================================
# IP address types
# =============================================================================


class IPv4(Column[_ip.IPv4Address]):
    _sql_type = "IPv4"


class IPv6(Column[_ip.IPv6Address]):
    _sql_type = "IPv6"


# =============================================================================
# JSON / dynamic
# =============================================================================


class JSON(Column[Any]):
    """ClickHouse ``JSON`` type (object)."""

    _sql_type = "JSON"


class Dynamic(Column[Any]):
    """``Dynamic`` — value of any type at runtime."""

    _sql_type = "Dynamic"


class Nothing(Column[None]):
    """``Nothing`` — used internally for ``NULL`` literals."""

    _sql_type = "Nothing"


# =============================================================================
# Enum
# =============================================================================


def _enum_sql(enum_cls: type[enum_lib.Enum]) -> str:
    """Render an enum mapping as ``'name' = id, 'name2' = id2``."""
    parts = []
    for i, member in enumerate(enum_cls):
        val = member.value if isinstance(member.value, int) else i
        name = member.name if not isinstance(member.value, str) else member.value
        parts.append(f"'{name}' = {val}")
    return ", ".join(parts)


class _EnumBase(Column[Any]):
    _bits: int = 8
    _enum_cls: type[enum_lib.Enum] | None = None

    def __class_getitem__(cls, enum_cls: type[enum_lib.Enum]) -> type[_EnumBase]:
        return type(cls.__name__, (cls,), {"_enum_cls": enum_cls})

    def sql_type(self) -> str:
        if self._enum_cls is None:
            base = f"Enum{self._bits}"
        else:
            base = f"Enum{self._bits}({_enum_sql(self._enum_cls)})"
        return _wrap_modifiers(
            base, nullable=self._nullable, low_cardinality=self._low_cardinality
        )


class Enum8(_EnumBase):
    _bits = 8


class Enum16(_EnumBase):
    _bits = 16


# =============================================================================
# LowCardinality wrapper
# =============================================================================


class LowCardinality(Column[Any]):
    """``LowCardinality(T)`` — dictionary-encoded wrapper.

    Use as a type annotation::

        country: LowCardinality[String] = Field()
    """

    def __class_getitem__(cls, inner: type[Column[Any]]) -> type[LowCardinality]:
        # Subclass the inner type and flag it; the marker (not an `_inner`
        # attribute) drives rendering, so wrapping a composite like
        # LowCardinality[Array[String]] does not clobber the composite's
        # own `_inner`.
        return type(
            f"LowCardinality_{getattr(inner, '__name__', 'col')}",
            (inner,),
            {"_low_cardinality_marker": True},
        )


# =============================================================================
# Nullable wrapper
# =============================================================================


class Nullable(Column[Any]):
    """``Nullable(T)`` wrapper.

    Usage::

        bio: Nullable[String] = Field()
    """

    def __class_getitem__(cls, inner: type[Column[Any]]) -> type[Nullable]:
        # See LowCardinality: marker-driven, no `_inner` override (which would
        # clobber a composite inner's own `_inner`).
        return type(
            f"Nullable_{getattr(inner, '__name__', 'col')}",
            (inner,),
            {"_nullable_marker": True},
        )


# =============================================================================
# Composite types: Array, Tuple, Map, Nested, Variant
# =============================================================================


class Array(Column[list[Any]]):
    """``Array(T)``."""

    _sql_type = "Array"
    _inner: Any = None

    def __class_getitem__(cls, inner: Any) -> type[Array]:
        return type("Array", (cls,), {"_inner": inner})

    def sql_type(self) -> str:
        if self._inner is None:
            return "Array"
        base = f"Array({_type_of(self._inner)})"
        return _wrap_modifiers(
            base, nullable=self._nullable, low_cardinality=self._low_cardinality
        )


class Tuple(Column[tuple]):
    """``Tuple(T1, T2, ...)`` — heterogeneous fixed-size tuple."""

    _sql_type = "Tuple"
    _members: tuple[Any, ...] = ()

    def __class_getitem__(cls, members: Any) -> type[Tuple]:
        if not isinstance(members, tuple):
            members = (members,)
        return type("Tuple", (cls,), {"_members": members})

    def sql_type(self) -> str:
        if not self._members:
            return "Tuple"
        parts = []
        for m in self._members:
            # Named tuple member: ``("name", Type)``
            if isinstance(m, tuple) and len(m) == 2 and isinstance(m[0], str):
                parts.append(f"{m[0]} {_type_of(m[1])}")
            else:
                parts.append(_type_of(m))
        base = f"Tuple({', '.join(parts)})"
        return _wrap_modifiers(
            base, nullable=self._nullable, low_cardinality=self._low_cardinality
        )


class Map(Column[dict]):
    """``Map(K, V)``."""

    _sql_type = "Map"
    _key: Any = None
    _value: Any = None

    def __class_getitem__(cls, params: tuple[Any, Any]) -> type[Map]:
        if not isinstance(params, tuple) or len(params) != 2:
            raise TypeError("Map requires (key_type, value_type)")
        return type("Map", (cls,), {"_key": params[0], "_value": params[1]})

    def sql_type(self) -> str:
        if self._key is None or self._value is None:
            return "Map"
        base = f"Map({_type_of(self._key)}, {_type_of(self._value)})"
        return _wrap_modifiers(
            base, nullable=self._nullable, low_cardinality=self._low_cardinality
        )


class Nested(Column[Any]):
    """``Nested(name1 Type1, name2 Type2, ...)``.

    Pass column definitions as ``Nested[("name", Type), ("name2", Type2)]``.
    """

    _sql_type = "Nested"
    _fields: tuple[tuple[str, Any], ...] = ()

    def __class_getitem__(cls, fields: Any) -> type[Nested]:
        if isinstance(fields, tuple) and fields and not isinstance(fields[0], tuple):
            # Single field passed as ``Nested["name", Type]`` won't work in
            # Python — coerce ``Nested[("name", Type)]`` to a 1-tuple.
            fields = (fields,)
        return type("Nested", (cls,), {"_fields": tuple(fields)})

    def sql_type(self) -> str:
        if not self._fields:
            return "Nested()"
        parts = [f"{n} {_type_of(t)}" for n, t in self._fields]
        return f"Nested({', '.join(parts)})"


class Variant(Column[Any]):
    """``Variant(T1, T2, ...)`` — discriminated union."""

    _sql_type = "Variant"
    _variants: tuple[Any, ...] = ()

    def __class_getitem__(cls, variants: Any) -> type[Variant]:
        if not isinstance(variants, tuple):
            variants = (variants,)
        return type("Variant", (cls,), {"_variants": variants})

    def sql_type(self) -> str:
        if not self._variants:
            return "Variant"
        return f"Variant({', '.join(_type_of(v) for v in self._variants)})"


# =============================================================================
# Aggregate function state types
# =============================================================================


class AggregateFunction(Column[Any]):
    """``AggregateFunction(name, ...types)`` — stored aggregate state."""

    _sql_type = "AggregateFunction"
    _func: str | None = None
    _arg_types: tuple[Any, ...] = ()

    def __class_getitem__(cls, params: tuple[Any, ...]) -> type[AggregateFunction]:
        if not isinstance(params, tuple) or len(params) < 2:
            raise TypeError(
                "AggregateFunction requires (name, *arg_types). "
                "Example: AggregateFunction['uniq', String]"
            )
        func = _unwrap_literal(params[0])
        return type(
            "AggregateFunction",
            (cls,),
            {"_func": func, "_arg_types": params[1:]},
        )

    def sql_type(self) -> str:
        if not self._func:
            return "AggregateFunction"
        types_sql = ", ".join(_type_of(t) for t in self._arg_types)
        return f"AggregateFunction({self._func}, {types_sql})"


class SimpleAggregateFunction(AggregateFunction):
    """``SimpleAggregateFunction(name, type)`` — for commutative aggregations."""

    def sql_type(self) -> str:
        if not self._func:
            return "SimpleAggregateFunction"
        types_sql = ", ".join(_type_of(t) for t in self._arg_types)
        return f"SimpleAggregateFunction({self._func}, {types_sql})"


# =============================================================================
# Geo types
# =============================================================================


class Point(Column[tuple[float, float]]):
    """``Point`` — alias for ``Tuple(Float64, Float64)``."""

    _sql_type = "Point"


class Ring(Column[list[tuple[float, float]]]):
    """``Ring`` — alias for ``Array(Point)``."""

    _sql_type = "Ring"


class Polygon(Column[list[list[tuple[float, float]]]]):
    """``Polygon`` — alias for ``Array(Ring)``."""

    _sql_type = "Polygon"


class MultiPolygon(Column[list[list[list[tuple[float, float]]]]]):
    """``MultiPolygon`` — alias for ``Array(Polygon)``."""

    _sql_type = "MultiPolygon"
