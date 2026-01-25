"""Tests for field types."""

from dribble.fields import (
    JSON,
    JSONB,
    UUID,
    Array,
    BigInt,
    BigSerial,
    Boolean,
    Char,
    Date,
    DoublePrecision,
    Integer,
    Interval,
    Numeric,
    Real,
    Serial,
    SmallInt,
    Text,
    Time,
    Timestamp,
    Varchar,
)


def test_integer_types():
    """Test integer type SQL generation."""
    assert Serial().sql_type() == "SERIAL"
    assert BigSerial().sql_type() == "BIGSERIAL"
    assert SmallInt().sql_type() == "SMALLINT"
    assert Integer().sql_type() == "INTEGER"
    assert BigInt().sql_type() == "BIGINT"


def test_string_types():
    """Test string type SQL generation."""
    assert Varchar(255).sql_type() == "VARCHAR(255)"
    assert Char(10).sql_type() == "CHAR(10)"
    assert Text().sql_type() == "TEXT"


def test_boolean():
    """Test boolean type SQL generation."""
    assert Boolean().sql_type() == "BOOLEAN"


def test_temporal_types():
    """Test temporal type SQL generation."""
    assert Timestamp().sql_type() == "TIMESTAMP"
    assert Timestamp(with_timezone=True).sql_type() == "TIMESTAMP WITH TIME ZONE"
    assert Date().sql_type() == "DATE"
    assert Time().sql_type() == "TIME"
    assert Time(with_timezone=True).sql_type() == "TIME WITH TIME ZONE"
    assert Interval().sql_type() == "INTERVAL"


def test_numeric_types():
    """Test numeric type SQL generation."""
    assert Numeric().sql_type() == "NUMERIC"
    assert Numeric(10).sql_type() == "NUMERIC(10)"
    assert Numeric(10, 2).sql_type() == "NUMERIC(10, 2)"
    assert Real().sql_type() == "REAL"
    assert DoublePrecision().sql_type() == "DOUBLE PRECISION"


def test_uuid():
    """Test UUID type SQL generation."""
    assert UUID().sql_type() == "UUID"


def test_json_types():
    """Test JSON type SQL generation."""
    assert JSON().sql_type() == "JSON"
    assert JSONB().sql_type() == "JSONB"


def test_array_type():
    """Test array type SQL generation."""
    assert Array(Integer()).sql_type() == "INTEGER[]"
    assert Array(Varchar(255)).sql_type() == "VARCHAR(255)[]"
    assert Array(Array(Integer())).sql_type() == "INTEGER[][]"
