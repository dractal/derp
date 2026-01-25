"""Dribble ORM - A strongly-typed async Python ORM for PostgreSQL.

Example usage:

    from dribble import Dribble, Table, Field, eq
    from dribble.fields import Serial, Varchar, Timestamp

    # Define tables
    class User(Table, table_name="users"):
        id: int = Field(Serial(), primary_key=True)
        name: str = Field(Varchar(255))
        email: str = Field(Varchar(255), unique=True)
        created_at: datetime = Field(Timestamp(), default="now()")

    # Query using .c accessor for columns
    async with Dribble("postgresql://...") as db:
        users = await db.select(User).where(eq(User.c.name, "Alice")).execute()
        new_user = await (
            db.insert(User)
            .values(name="Bob", email="bob@example.com")
            .returning(User)
            .execute()
        )
"""

from dribble.engine import Dribble
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
    Field,
    FieldInfo,
    ForeignKey,
    ForeignKeyAction,
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
from dribble.query import (
    ComparisonOperator,
    JoinType,
    LogicalOperator,
    SortOrder,
    and_,
    between,
    eq,
    gt,
    gte,
    ilike,
    in_,
    is_not_null,
    is_null,
    like,
    lt,
    lte,
    ne,
    not_,
    not_in,
    or_,
)
from dribble.table import Table

__version__ = "0.1.0"

__all__ = [
    # Main engine
    "Dribble",
    # Table definition
    "Table",
    "Field",
    "FieldInfo",
    "ForeignKey",
    "ForeignKeyAction",
    # Field types
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
    "Date",
    "Time",
    "Interval",
    "Numeric",
    "Real",
    "DoublePrecision",
    "UUID",
    "JSON",
    "JSONB",
    "Array",
    # Query enums
    "JoinType",
    "SortOrder",
    "LogicalOperator",
    "ComparisonOperator",
    # Query operators
    "eq",
    "ne",
    "gt",
    "gte",
    "lt",
    "lte",
    "and_",
    "or_",
    "not_",
    "like",
    "ilike",
    "in_",
    "not_in",
    "is_null",
    "is_not_null",
    "between",
]
