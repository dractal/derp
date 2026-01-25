"""Query builder module for Dribble ORM."""

from dribble.query.builder import DeleteQuery, InsertQuery, SelectQuery, UpdateQuery
from dribble.query.expressions import (
    Expression,
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
from dribble.query.types import Row, T

__all__ = [
    # Query builders
    "SelectQuery",
    "InsertQuery",
    "UpdateQuery",
    "DeleteQuery",
    # Expressions
    "Expression",
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
    # Types
    "Row",
    "T",
]
