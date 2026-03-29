"""Derp ORM - A strongly-typed async Python ORM for PostgreSQL.

Example usage::

    from derp.orm import Table, Serial, Text, Varchar, Timestamp, Field

    class User(Table, table="users"):
        id: Serial = Field(primary=True)
        name: Text = Field()
        email: Varchar[255] = Field(unique=True)
        created_at: Timestamp = Field(default="now()")

    async with DatabaseEngine("postgresql://...") as db:
        users = await db.select(User).where(User.name == "Alice").execute()
"""

from typing import Literal

from derp.config import DatabaseConfig
from derp.orm.column.base import FK, Column, Field, FieldSpec, Fn
from derp.orm.column.types import (
    JSON,
    JSONB,
    UUID,
    BigInt,
    BigSerial,
    Boolean,
    Char,
    Date,
    DoublePrecision,
    Enum,
    Integer,
    Interval,
    Nullable,
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
from derp.orm.engine import DatabaseEngine
from derp.orm.expression_base import ComparisonOperator, Expression
from derp.orm.index import Index, IndexColumn, IndexMethod, NullsPosition
from derp.orm.query import (
    JoinType,
    LockMode,
    LogicalOperator,
    SortOrder,
    sql,
)
from derp.orm.table import Table

L = Literal

__version__ = "0.2.0"

__all__ = [
    # Main engine
    "DatabaseConfig",
    "DatabaseEngine",
    # Table definition
    "Table",
    "Column",
    "Field",
    "FieldSpec",
    "Fn",
    "FK",
    # PG types
    "Serial",
    "BigSerial",
    "SmallInt",
    "Integer",
    "BigInt",
    "Varchar",
    "Char",
    "Text",
    "Boolean",
    "Timestamp",
    "TimestampTZ",
    "Date",
    "Time",
    "TimeTZ",
    "Interval",
    "UUID",
    "Numeric",
    "Real",
    "DoublePrecision",
    "JSON",
    "JSONB",
    "Vector",
    "Nullable",
    "Enum",
    # Query enums
    "JoinType",
    "LockMode",
    "SortOrder",
    "LogicalOperator",
    "ComparisonOperator",
    # Expressions
    "Expression",
    "sql",
    # Indexes
    "Index",
    "IndexColumn",
    "IndexMethod",
    "NullsPosition",
    # Literal shorthand for strict type checkers
    "L",
]
