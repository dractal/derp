"""Derp ORM - A strongly-typed async Python ORM for PostgreSQL.

Example usage:

    from derp import Derp, Table, Field, eq
    from derp.fields import Serial, Varchar, Timestamp

    # Define tables
    class User(Table, table="users"):
        id: int = Field(Serial(), primary_key=True)
        name: str = Field(Varchar(255))
        email: str = Field(Varchar(255), unique=True)
        created_at: datetime = Field(Timestamp(), default="now()")

    # Query using .c accessor for columns
    async with Derp("postgresql://...") as db:
        users = await db.select(User).where(User.c.name == "Alice").execute()
        new_user = await (
            db.insert(User)
            .values(name="Bob", email="bob@example.com")
            .returning(User)
            .execute()
        )
"""

from derp.config import DatabaseConfig
from derp.orm.engine import DatabaseEngine
from derp.orm.fields import (
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
from derp.orm.query import (
    ComparisonOperator,
    JoinType,
    LogicalOperator,
    SortOrder,
)
from derp.orm.table import Table

__version__ = "0.1.0"

__all__ = [
    # Main engine
    "DatabaseConfig",
    "DatabaseEngine",
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
]
