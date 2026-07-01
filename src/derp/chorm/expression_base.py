"""Base Expression class and operator enums for the ClickHouse ORM.

Mirrors :mod:`derp.orm.expression_base` but emits ClickHouse-flavoured SQL.
ClickHouse uses ``{name:Type}`` parameter placeholders rather than ``$N``.
"""

from __future__ import annotations

import abc
from enum import StrEnum
from typing import Any


class LogicalOperator(StrEnum):
    """SQL logical operators."""

    AND = "AND"
    OR = "OR"


class ComparisonOperator(StrEnum):
    """SQL comparison operators."""

    EQ = "="
    NE = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="


class Params:
    """Accumulator for parameter substitutions.

    ClickHouse supports parameterized queries with ``{name:Type}``
    syntax.  This class allocates unique names and tracks the inferred
    ClickHouse type for each value.
    """

    __slots__ = ("_values", "_counter", "table_aliases")

    def __init__(self) -> None:
        self._values: dict[str, Any] = {}
        self._counter: int = 0
        self.table_aliases: dict[str, str] = {}

    def add(self, value: Any, ch_type: str | None = None) -> str:
        """Add *value* and return its ``{name:Type}`` placeholder text.

        The ClickHouse type rides inline in the placeholder — there is no
        separate types channel to the driver.
        """
        self._counter += 1
        name = f"p{self._counter}"
        ch_type = ch_type or _infer_ch_type(value)
        self._values[name] = value
        return f"{{{name}:{ch_type}}}"

    @property
    def values(self) -> dict[str, Any]:
        return dict(self._values)

    def __len__(self) -> int:
        return self._counter


def _infer_ch_type(value: Any) -> str:
    """Infer a ClickHouse type tag for a Python value."""
    import datetime
    import uuid as _uuid

    if value is None:
        return "Nullable(String)"
    if isinstance(value, bool):
        return "Bool"
    if isinstance(value, int):
        if value < 0:
            return "Int64"
        return "UInt64" if value > 2_147_483_647 else "Int64"
    if isinstance(value, float):
        return "Float64"
    if isinstance(value, str):
        return "String"
    if isinstance(value, bytes):
        return "String"
    if isinstance(value, _uuid.UUID):
        return "UUID"
    if isinstance(value, datetime.datetime):
        return "DateTime64(6)"
    if isinstance(value, datetime.date):
        return "Date"
    if isinstance(value, list | tuple):
        if not value:
            return "Array(String)"
        inner = _infer_ch_type(value[0])
        return f"Array({inner})"
    if isinstance(value, dict):
        return "String"  # serialized JSON fallback
    return "String"


class Expression(abc.ABC):
    """Base class for ClickHouse SQL expressions.

    Subclasses implement :meth:`to_sql`, returning SQL text and adding
    any parameters via the shared :class:`Params` accumulator.
    """

    @abc.abstractmethod
    def to_sql(self, params: Params) -> str:
        """Generate SQL string, recording any parameters in *params*."""

    # -- Logical combinators --------------------------------------------------

    def __and__(self, other: Expression) -> Expression:
        from derp.chorm.query.expressions import LogicalOp

        return LogicalOp(LogicalOperator.AND, (self, other))

    def __or__(self, other: Expression) -> Expression:
        from derp.chorm.query.expressions import LogicalOp

        return LogicalOp(LogicalOperator.OR, (self, other))

    def __invert__(self) -> Expression:
        from derp.chorm.query.expressions import UnaryOp

        return UnaryOp("NOT", self)

    # -- Comparison -----------------------------------------------------------

    def __eq__(self, other: Any) -> Any:
        from derp.chorm.query.expressions import BinaryOp, to_expr

        return BinaryOp(self, ComparisonOperator.EQ, to_expr(other))

    def __ne__(self, other: Any) -> Any:
        from derp.chorm.query.expressions import BinaryOp, to_expr

        return BinaryOp(self, ComparisonOperator.NE, to_expr(other))

    def __lt__(self, other: Any) -> Any:
        from derp.chorm.query.expressions import BinaryOp, to_expr

        return BinaryOp(self, ComparisonOperator.LT, to_expr(other))

    def __le__(self, other: Any) -> Any:
        from derp.chorm.query.expressions import BinaryOp, to_expr

        return BinaryOp(self, ComparisonOperator.LTE, to_expr(other))

    def __gt__(self, other: Any) -> Any:
        from derp.chorm.query.expressions import BinaryOp, to_expr

        return BinaryOp(self, ComparisonOperator.GT, to_expr(other))

    def __ge__(self, other: Any) -> Any:
        from derp.chorm.query.expressions import BinaryOp, to_expr

        return BinaryOp(self, ComparisonOperator.GTE, to_expr(other))

    # -- Arithmetic (commonly needed in ClickHouse analytics) -----------------

    def __add__(self, other: Any) -> Expression:
        from derp.chorm.query.expressions import BinaryOp, to_expr

        return BinaryOp(self, "+", to_expr(other))

    def __radd__(self, other: Any) -> Expression:
        from derp.chorm.query.expressions import BinaryOp, to_expr

        return BinaryOp(to_expr(other), "+", self)

    def __sub__(self, other: Any) -> Expression:
        from derp.chorm.query.expressions import BinaryOp, to_expr

        return BinaryOp(self, "-", to_expr(other))

    def __rsub__(self, other: Any) -> Expression:
        from derp.chorm.query.expressions import BinaryOp, to_expr

        return BinaryOp(to_expr(other), "-", self)

    def __mul__(self, other: Any) -> Expression:
        from derp.chorm.query.expressions import BinaryOp, to_expr

        return BinaryOp(self, "*", to_expr(other))

    def __rmul__(self, other: Any) -> Expression:
        from derp.chorm.query.expressions import BinaryOp, to_expr

        return BinaryOp(to_expr(other), "*", self)

    def __truediv__(self, other: Any) -> Expression:
        from derp.chorm.query.expressions import BinaryOp, to_expr

        return BinaryOp(self, "/", to_expr(other))

    def __mod__(self, other: Any) -> Expression:
        from derp.chorm.query.expressions import BinaryOp, to_expr

        return BinaryOp(self, "%", to_expr(other))

    # -- IN / LIKE / IS NULL / BETWEEN ----------------------------------------

    def in_(self, values: Any) -> Any:
        from derp.chorm.query.expressions import InList, InSubquery

        if hasattr(values, "build"):
            return InSubquery(self, values, negated=False)
        return InList(self, tuple(values), negated=False)

    def not_in(self, values: Any) -> Any:
        from derp.chorm.query.expressions import InList, InSubquery

        if hasattr(values, "build"):
            return InSubquery(self, values, negated=True)
        return InList(self, tuple(values), negated=True)

    def global_in(self, values: Any) -> Any:
        """``GLOBAL IN`` — for use in Distributed table queries."""
        from derp.chorm.query.expressions import InList, InSubquery

        if hasattr(values, "build"):
            return InSubquery(self, values, negated=False, global_=True)
        return InList(self, tuple(values), negated=False, global_=True)

    def like(self, pattern: str) -> Any:
        from derp.chorm.query.expressions import Like

        return Like(self, pattern, case_insensitive=False)

    def ilike(self, pattern: str) -> Any:
        from derp.chorm.query.expressions import Like

        return Like(self, pattern, case_insensitive=True)

    def not_like(self, pattern: str) -> Any:
        from derp.chorm.query.expressions import Like

        return Like(self, pattern, case_insensitive=False, negated=True)

    def match(self, regex: str) -> Any:
        """ClickHouse regex match (``match()`` function)."""
        from derp.chorm.query.expressions import FunctionCall, to_expr

        return FunctionCall("match", (self, to_expr(regex)))

    def is_null(self) -> Any:
        from derp.chorm.query.expressions import NullCheck

        return NullCheck(self, is_null=True)

    def is_not_null(self) -> Any:
        from derp.chorm.query.expressions import NullCheck

        return NullCheck(self, is_null=False)

    def between(self, low: Any, high: Any) -> Any:
        from derp.chorm.query.expressions import Between

        return Between(self, low, high)

    # -- Aliasing -------------------------------------------------------------

    def as_(self, alias: str) -> Any:
        from derp.chorm.query.expressions import Alias

        return Alias(self, alias)
