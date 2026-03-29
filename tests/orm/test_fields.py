"""Tests for field types."""

from derp.orm import (
    JSON,
    JSONB,
    UUID,
    BigInt,
    BigSerial,
    Boolean,
    Char,
    Date,
    DoublePrecision,
    Field,
    Integer,
    Interval,
    Numeric,
    Real,
    Serial,
    SmallInt,
    Text,
    Time,
    Timestamp,
    TimestampTZ,
    TimeTZ,
    Varchar,
    Vector,
)


def test_integer_types():
    """Test integer type SQL generation."""
    assert Serial(Field()).sql_type() == "SERIAL"
    assert BigSerial(Field()).sql_type() == "BIGSERIAL"
    assert SmallInt(Field()).sql_type() == "SMALLINT"
    assert Integer(Field()).sql_type() == "INTEGER"
    assert BigInt(Field()).sql_type() == "BIGINT"


def test_string_types():
    """Test string type SQL generation."""
    assert Varchar[255](Field()).sql_type() == "VARCHAR(255)"
    assert Char[10](Field()).sql_type() == "CHAR(10)"
    assert Text(Field()).sql_type() == "TEXT"


def test_boolean():
    """Test boolean type SQL generation."""
    assert Boolean(Field()).sql_type() == "BOOLEAN"


def test_temporal_types():
    """Test temporal type SQL generation."""
    assert Timestamp(Field()).sql_type() == "TIMESTAMP"
    assert TimestampTZ(Field()).sql_type() == "TIMESTAMP WITH TIME ZONE"
    assert Date(Field()).sql_type() == "DATE"
    assert Time(Field()).sql_type() == "TIME"
    assert TimeTZ(Field()).sql_type() == "TIME WITH TIME ZONE"
    assert Interval(Field()).sql_type() == "INTERVAL"


def test_numeric_types():
    """Test numeric type SQL generation."""
    assert Numeric(Field()).sql_type() == "NUMERIC"
    assert Numeric[10](Field()).sql_type() == "NUMERIC(10)"
    assert Numeric[10, 2](Field()).sql_type() == "NUMERIC(10, 2)"
    assert Real(Field()).sql_type() == "REAL"
    assert DoublePrecision(Field()).sql_type() == "DOUBLE PRECISION"


def test_uuid():
    """Test UUID type SQL generation."""
    assert UUID(Field()).sql_type() == "UUID"


def test_json_types():
    """Test JSON type SQL generation."""
    assert JSON(Field()).sql_type() == "JSON"
    assert JSONB(Field()).sql_type() == "JSONB"


def test_vector_type():
    """Test vector type SQL generation."""
    assert Vector[1536](Field()).sql_type() == "VECTOR(1536)"
    assert Vector(Field()).sql_type() == "VECTOR"
