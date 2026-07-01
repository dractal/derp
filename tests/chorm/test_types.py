"""Tests for ClickHouse type rendering."""

from __future__ import annotations

import enum

import pytest

from derp.chorm import (
    JSON,
    UUID,
    AggregateFunction,
    Array,
    Bool,
    Date,
    Date32,
    DateTime,
    DateTime64,
    Decimal,
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
    Dynamic,
    Enum8,
    Enum16,
    FixedString,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,
    IPv4,
    IPv6,
    LowCardinality,
    Map,
    MultiPolygon,
    Nested,
    Nothing,
    Nullable,
    Point,
    Polygon,
    Ring,
    SimpleAggregateFunction,
    String,
    Tuple,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    UInt256,
    Variant,
)
from derp.chorm.column.base import FieldSpec


def _sql(cls):
    """Render a Column subclass's SQL type."""
    return cls(FieldSpec()).sql_type()


def test_unsigned_int_types():
    assert _sql(UInt8) == "UInt8"
    assert _sql(UInt16) == "UInt16"
    assert _sql(UInt32) == "UInt32"
    assert _sql(UInt64) == "UInt64"
    assert _sql(UInt128) == "UInt128"
    assert _sql(UInt256) == "UInt256"


def test_signed_int_types():
    assert _sql(Int8) == "Int8"
    assert _sql(Int16) == "Int16"
    assert _sql(Int32) == "Int32"
    assert _sql(Int64) == "Int64"
    assert _sql(Int128) == "Int128"
    assert _sql(Int256) == "Int256"


def test_float_types():
    assert _sql(Float32) == "Float32"
    assert _sql(Float64) == "Float64"


def test_bool_type():
    assert _sql(Bool) == "Bool"


def test_string_type():
    assert _sql(String) == "String"


def test_fixed_string():
    assert _sql(FixedString[16]) == "FixedString(16)"


def test_decimal_types():
    assert _sql(Decimal[10, 2]) == "Decimal(10, 2)"
    assert _sql(Decimal32[4]) == "Decimal32(4)"
    assert _sql(Decimal64[6]) == "Decimal64(6)"
    assert _sql(Decimal128[8]) == "Decimal128(8)"
    assert _sql(Decimal256[10]) == "Decimal256(10)"


def test_temporal_types():
    assert _sql(Date) == "Date"
    assert _sql(Date32) == "Date32"
    assert _sql(DateTime) == "DateTime"
    assert _sql(DateTime["UTC"]) == "DateTime('UTC')"
    assert _sql(DateTime64) == "DateTime64(3)"
    assert _sql(DateTime64[6]) == "DateTime64(6)"
    assert _sql(DateTime64[3, "UTC"]) == "DateTime64(3, 'UTC')"


def test_uuid_and_ip_types():
    assert _sql(UUID) == "UUID"
    assert _sql(IPv4) == "IPv4"
    assert _sql(IPv6) == "IPv6"


def test_json_nothing_dynamic():
    assert _sql(JSON) == "JSON"
    assert _sql(Dynamic) == "Dynamic"
    assert _sql(Nothing) == "Nothing"


def test_nullable_wrapper():
    assert _sql(Nullable[String]) == "Nullable(String)"
    assert _sql(Nullable[UInt64]) == "Nullable(UInt64)"


def test_low_cardinality_wrapper():
    assert _sql(LowCardinality[String]) == "LowCardinality(String)"


def test_low_cardinality_of_nullable_ordering():
    # ClickHouse requires LowCardinality(Nullable(T)) — the reverse is rejected.
    assert _sql(LowCardinality[Nullable[String]]) == "LowCardinality(Nullable(String))"


def test_modifiers_preserved_inside_composites():
    # Modifiers nested inside a composite must not be silently dropped.
    assert _sql(Array[Nullable[String]]) == "Array(Nullable(String))"
    assert _sql(Array[LowCardinality[String]]) == "Array(LowCardinality(String))"
    assert _sql(Map[String, Nullable[UInt64]]) == "Map(String, Nullable(UInt64))"
    assert _sql(Tuple[(String, Nullable[UInt64])]) == "Tuple(String, Nullable(UInt64))"
    assert (
        _sql(Array[LowCardinality[Nullable[String]]])
        == "Array(LowCardinality(Nullable(String)))"
    )


def test_nested_named_members_preserve_modifiers():
    n = Nested[(("a", Nullable[String]), ("b", UInt64))]
    assert _sql(n) == "Nested(a Nullable(String), b UInt64)"
    v = Variant[(String, Nullable[Int64])]
    assert _sql(v) == "Variant(String, Nullable(Int64))"


def test_modifier_wrapping_a_composite():
    # The symmetric case: a modifier wrapping a composite type.
    assert _sql(Nullable[Array[String]]) == "Nullable(Array(String))"
    assert _sql(LowCardinality[Array[String]]) == "LowCardinality(Array(String))"


def test_array_basic():
    assert _sql(Array[String]) == "Array(String)"
    assert _sql(Array[Int64]) == "Array(Int64)"


def test_array_nested():
    inner = Array[String]
    assert _sql(Array[inner]) == "Array(Array(String))"


def test_tuple_anonymous():
    t = Tuple[(String, Int64, Float64)]
    assert _sql(t) == "Tuple(String, Int64, Float64)"


def test_tuple_named():
    t = Tuple[(("name", String), ("count", Int64))]
    assert _sql(t) == "Tuple(name String, count Int64)"


def test_map_type():
    assert _sql(Map[String, Int64]) == "Map(String, Int64)"


def test_nested_type():
    n = Nested[(("k", String), ("v", Int64))]
    assert _sql(n) == "Nested(k String, v Int64)"


def test_variant_type():
    v = Variant[(String, Int64, Float64)]
    assert _sql(v) == "Variant(String, Int64, Float64)"


def test_aggregate_function_state():
    s = AggregateFunction["uniq", String]
    assert _sql(s) == "AggregateFunction(uniq, String)"

    s2 = SimpleAggregateFunction["sum", Int64]
    assert _sql(s2) == "SimpleAggregateFunction(sum, Int64)"


def test_geo_types():
    assert _sql(Point) == "Point"
    assert _sql(Ring) == "Ring"
    assert _sql(Polygon) == "Polygon"
    assert _sql(MultiPolygon) == "MultiPolygon"


def test_enum_types():
    class Color(enum.IntEnum):
        RED = 1
        GREEN = 2
        BLUE = 3

    rendered = _sql(Enum8[Color])
    assert "Enum8" in rendered
    assert "'RED' = 1" in rendered
    assert "'GREEN' = 2" in rendered
    assert "'BLUE' = 3" in rendered

    rendered16 = _sql(Enum16[Color])
    assert "Enum16" in rendered16


def test_decimal_requires_two_args():
    with pytest.raises(TypeError, match="precision, scale"):
        Decimal[(10,)]  # type: ignore[index]


def test_map_requires_two_args():
    with pytest.raises(TypeError, match="key_type, value_type"):
        Map[(String,)]  # type: ignore[index]


def test_aggregate_function_requires_two_args():
    with pytest.raises(TypeError, match="name"):
        AggregateFunction[("uniq",)]
