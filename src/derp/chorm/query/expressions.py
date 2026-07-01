"""ClickHouse expression tree nodes."""

from __future__ import annotations

import dataclasses
from typing import Any

from derp.chorm.expression_base import (
    ComparisonOperator,
    Expression,
    LogicalOperator,
    Params,
)

# =============================================================================
# Leaf nodes
# =============================================================================


@dataclasses.dataclass(eq=False)
class ColumnRef(Expression):
    """Reference to a table column (used when only the name is known)."""

    table_name: str | None
    column_name: str

    def to_sql(self, params: Params) -> str:
        if self.table_name:
            return f"`{self.table_name}`.`{self.column_name}`"
        return f"`{self.column_name}`"


@dataclasses.dataclass(eq=False)
class Literal(Expression):
    """Literal value substituted via a query parameter."""

    value: Any
    ch_type: str | None = None

    def to_sql(self, params: Params) -> str:
        return params.add(self.value, self.ch_type)


@dataclasses.dataclass(eq=False)
class CastLiteral(Expression):
    """``CAST({p:T} AS Type)`` — emit an explicit cast."""

    value: Any
    cast: str

    def to_sql(self, params: Params) -> str:
        placeholder = params.add(self.value)
        return f"CAST({placeholder} AS {self.cast})"


def to_expr(value: Expression | Any) -> Expression:
    """Coerce *value* into an :class:`Expression`."""
    if isinstance(value, Expression):
        return value
    return Literal(value)


# =============================================================================
# Composite nodes
# =============================================================================


@dataclasses.dataclass(eq=False)
class BinaryOp(Expression):
    """Binary operator expression."""

    left: Expression | Any
    operator: ComparisonOperator | str
    right: Expression | Any

    def to_sql(self, params: Params) -> str:
        left_sql = _expr_to_sql(self.left, params)
        right_sql = _expr_to_sql(self.right, params)
        return f"({left_sql} {self.operator} {right_sql})"


@dataclasses.dataclass(eq=False)
class UnaryOp(Expression):
    """Unary operator expression."""

    operator: str
    operand: Expression

    def to_sql(self, params: Params) -> str:
        return f"({self.operator} {_expr_to_sql(self.operand, params)})"


@dataclasses.dataclass(eq=False)
class LogicalOp(Expression):
    """Logical combinator (AND / OR)."""

    operator: LogicalOperator
    conditions: tuple[Expression, ...]

    def to_sql(self, params: Params) -> str:
        if not self.conditions:
            return "1" if self.operator == LogicalOperator.AND else "0"
        parts = [_expr_to_sql(c, params) for c in self.conditions]
        return f"({f' {self.operator} '.join(parts)})"


@dataclasses.dataclass(eq=False)
class InList(Expression):
    """``IN (...)`` against a literal list."""

    column: Expression
    values: tuple[Any, ...]
    negated: bool = False
    global_: bool = False

    def to_sql(self, params: Params) -> str:
        if not self.values:
            return "0" if not self.negated else "1"
        col_sql = _expr_to_sql(self.column, params)
        placeholders = [params.add(v) for v in self.values]
        op = "NOT IN" if self.negated else "IN"
        if self.global_:
            op = f"GLOBAL {op}"
        return f"({col_sql} {op} ({', '.join(placeholders)}))"


@dataclasses.dataclass(eq=False)
class InSubquery(Expression):
    """``IN (SELECT ...)`` against another query."""

    column: Expression
    query: Any  # SelectQuery — typed as Any to avoid circular import
    negated: bool = False
    global_: bool = False

    def to_sql(self, params: Params) -> str:
        col_sql = _expr_to_sql(self.column, params)
        sub_sql = self.query.build_into(params)
        op = "NOT IN" if self.negated else "IN"
        if self.global_:
            op = f"GLOBAL {op}"
        return f"({col_sql} {op} ({sub_sql}))"


@dataclasses.dataclass(eq=False)
class Between(Expression):
    """``BETWEEN low AND high``."""

    column: Expression
    low: Any
    high: Any

    def to_sql(self, params: Params) -> str:
        col_sql = _expr_to_sql(self.column, params)
        low_ph = params.add(self.low)
        high_ph = params.add(self.high)
        return f"({col_sql} BETWEEN {low_ph} AND {high_ph})"


@dataclasses.dataclass(eq=False)
class NullCheck(Expression):
    """``IS NULL`` / ``IS NOT NULL``."""

    column: Expression
    is_null: bool = True

    def to_sql(self, params: Params) -> str:
        col_sql = _expr_to_sql(self.column, params)
        op = "IS NULL" if self.is_null else "IS NOT NULL"
        return f"({col_sql} {op})"


@dataclasses.dataclass(eq=False)
class Like(Expression):
    """``LIKE`` / ``ILIKE`` / ``NOT LIKE``."""

    column: Expression
    pattern: str
    case_insensitive: bool = False
    negated: bool = False

    def to_sql(self, params: Params) -> str:
        col_sql = _expr_to_sql(self.column, params)
        ph = params.add(self.pattern)
        op = "ILIKE" if self.case_insensitive else "LIKE"
        if self.negated:
            op = f"NOT {op}"
        return f"({col_sql} {op} {ph})"


def _expr_to_sql(expr: Expression | Any, params: Params) -> str:
    if isinstance(expr, Expression):
        return expr.to_sql(params)
    return params.add(expr)


# =============================================================================
# Raw SQL fragment
# =============================================================================


@dataclasses.dataclass(eq=False)
class RawSQL(Expression):
    """Raw SQL fragment with optional parameterized values.

    Use ``sql()`` to construct.  ``{}`` placeholders are interpolated
    positionally::

        sql("toYYYYMM(ts)")
        sql("toYYYYMM({})", some_date)
    """

    template: str
    values: tuple[Any, ...] = ()
    _alias: str | None = None

    def to_sql(self, params: Params) -> str:
        parts = self.template.split("{}")
        result = parts[0]
        for i, val in enumerate(self.values):
            result += params.add(val)
            if i + 1 < len(parts):
                result += parts[i + 1]
        if self._alias is not None:
            result += f" AS {self._alias}"
        return result

    def as_(self, alias: str) -> RawSQL:
        return RawSQL(self.template, self.values, _alias=alias)


def sql(template: str, *values: Any) -> RawSQL:
    """Create a raw SQL expression fragment."""
    return RawSQL(template, values)


def raw(template: str) -> RawSQL:
    """Raw SQL with no parameter interpolation."""
    return RawSQL(template, ())


# =============================================================================
# Function call
# =============================================================================


@dataclasses.dataclass(eq=False)
class FunctionCall(Expression):
    """Call a ClickHouse function with positional args.

    *params_args* are rendered into a ``func(p1, p2)(arg1, arg2)`` form,
    used by parametric aggregate functions (e.g. ``quantile(0.5)(x)``).
    """

    name: str
    args: tuple[Any, ...]
    params_args: tuple[Any, ...] = ()
    _alias: str | None = None

    def to_sql(self, params: Params) -> str:
        arg_sql = ", ".join(_expr_to_sql(a, params) for a in self.args)
        if self.params_args:
            param_sql = ", ".join(_expr_to_sql(p, params) for p in self.params_args)
            result = f"{self.name}({param_sql})({arg_sql})"
        else:
            result = f"{self.name}({arg_sql})"
        if self._alias is not None:
            result += f" AS {self._alias}"
        return result

    def as_(self, alias: str) -> FunctionCall:
        return FunctionCall(self.name, self.args, self.params_args, _alias=alias)


# =============================================================================
# Aggregate function with combinators
# =============================================================================


@dataclasses.dataclass(eq=False)
class AggregateFunc(Expression):
    """Aggregate function with optional combinators (``-If``, ``-Array``, etc.).

    Example::

        AggregateFunc("sum", (col,))                # sum(col)
        AggregateFunc("sum", (col,), combinators=("If",), cond=cond)
        AggregateFunc("quantile", (col,), params=(0.95,))
    """

    name: str
    args: tuple[Any, ...]
    params: tuple[Any, ...] = ()
    combinators: tuple[str, ...] = ()
    cond: Any = None
    distinct: bool = False
    _alias: str | None = None

    def to_sql(self, params: Params) -> str:
        func = self.name
        for c in self.combinators:
            func = func + c

        # Render args
        all_args = list(self.args)
        if "If" in self.combinators and self.cond is not None:
            all_args.append(self.cond)
        arg_sql_parts = [_expr_to_sql(a, params) for a in all_args]
        distinct_prefix = "DISTINCT " if self.distinct else ""

        if self.params:
            param_sql = ", ".join(_expr_to_sql(p, params) for p in self.params)
            result = f"{func}({param_sql})({distinct_prefix}{', '.join(arg_sql_parts)})"
        else:
            result = f"{func}({distinct_prefix}{', '.join(arg_sql_parts)})"

        if self._alias is not None:
            result += f" AS {self._alias}"
        return result

    def as_(self, alias: str) -> AggregateFunc:
        return AggregateFunc(
            self.name,
            self.args,
            self.params,
            self.combinators,
            self.cond,
            self.distinct,
            _alias=alias,
        )

    def if_(self, cond: Any) -> AggregateFunc:
        """Append the ``-If`` combinator with a filter condition."""
        return AggregateFunc(
            self.name,
            self.args,
            self.params,
            self.combinators + ("If",),
            cond,
            self.distinct,
            self._alias,
        )

    def or_null(self) -> AggregateFunc:
        return AggregateFunc(
            self.name,
            self.args,
            self.params,
            self.combinators + ("OrNull",),
            self.cond,
            self.distinct,
            self._alias,
        )

    def or_default(self) -> AggregateFunc:
        return AggregateFunc(
            self.name,
            self.args,
            self.params,
            self.combinators + ("OrDefault",),
            self.cond,
            self.distinct,
            self._alias,
        )

    def state(self) -> AggregateFunc:
        """Append the ``-State`` combinator (returns intermediate state)."""
        return AggregateFunc(
            self.name,
            self.args,
            self.params,
            self.combinators + ("State",),
            self.cond,
            self.distinct,
            self._alias,
        )

    def merge(self) -> AggregateFunc:
        return AggregateFunc(
            self.name,
            self.args,
            self.params,
            self.combinators + ("Merge",),
            self.cond,
            self.distinct,
            self._alias,
        )

    def array(self) -> AggregateFunc:
        """``-Array`` combinator (apply to array argument)."""
        return AggregateFunc(
            self.name,
            self.args,
            self.params,
            self.combinators + ("Array",),
            self.cond,
            self.distinct,
            self._alias,
        )


# =============================================================================
# Window function
# =============================================================================


@dataclasses.dataclass(eq=False)
class WindowFunc(Expression):
    """A window function expression: ``func() OVER (...)``."""

    func: Expression
    partition_by: tuple[Expression, ...] = ()
    order_by: tuple[tuple[Expression, str], ...] = ()
    frame: str | None = None
    _alias: str | None = None

    def to_sql(self, params: Params) -> str:
        func_sql = _expr_to_sql(self.func, params)
        over_parts: list[str] = []
        if self.partition_by:
            parts = [_expr_to_sql(p, params) for p in self.partition_by]
            over_parts.append(f"PARTITION BY {', '.join(parts)}")
        if self.order_by:
            parts = [f"{_expr_to_sql(c, params)} {d}" for c, d in self.order_by]
            over_parts.append(f"ORDER BY {', '.join(parts)}")
        if self.frame:
            over_parts.append(self.frame)
        result = f"{func_sql} OVER ({' '.join(over_parts)})"
        if self._alias is not None:
            result += f" AS {self._alias}"
        return result

    def as_(self, alias: str) -> WindowFunc:
        return WindowFunc(
            self.func, self.partition_by, self.order_by, self.frame, alias
        )


# =============================================================================
# CASE
# =============================================================================


@dataclasses.dataclass(eq=False)
class CaseExpression(Expression):
    """Simple ``CASE operand WHEN val THEN result ... END``."""

    operand: Expression
    whens: list[tuple[Any, Any]]
    else_value: Any | None = None
    _alias: str | None = None

    def to_sql(self, params: Params) -> str:
        operand_sql = _expr_to_sql(self.operand, params)
        result = f"CASE {operand_sql}"
        for cond, val in self.whens:
            result += f" WHEN {params.add(cond)} THEN {params.add(val)}"
        if self.else_value is not None:
            result += f" ELSE {params.add(self.else_value)}"
        result += " END"
        if self._alias is not None:
            result += f" AS {self._alias}"
        return result

    def as_(self, alias: str) -> CaseExpression:
        return CaseExpression(self.operand, self.whens, self.else_value, alias)


@dataclasses.dataclass(eq=False)
class SearchedCase(Expression):
    """Searched ``CASE WHEN cond THEN val ... END``."""

    cases: list[tuple[Expression, Any]]
    else_value: Any | None = None
    _alias: str | None = None

    def to_sql(self, params: Params) -> str:
        result = "CASE"
        for cond, val in self.cases:
            result += (
                f" WHEN {_expr_to_sql(cond, params)} THEN {_expr_to_sql(val, params)}"
            )
        if self.else_value is not None:
            result += f" ELSE {_expr_to_sql(self.else_value, params)}"
        result += " END"
        if self._alias is not None:
            result += f" AS {self._alias}"
        return result

    def as_(self, alias: str) -> SearchedCase:
        return SearchedCase(self.cases, self.else_value, alias)


# =============================================================================
# Alias / subquery
# =============================================================================


@dataclasses.dataclass(eq=False)
class Alias(Expression):
    """``expr AS alias``."""

    expr: Expression
    alias: str

    def to_sql(self, params: Params) -> str:
        return f"{_expr_to_sql(self.expr, params)} AS {self.alias}"


@dataclasses.dataclass(eq=False)
class SubqueryExpr(Expression):
    """A ``SELECT`` query wrapped as an expression."""

    query: Any
    _alias: str | None = None

    def to_sql(self, params: Params) -> str:
        sub_sql = self.query.build_into(params)
        result = f"({sub_sql})"
        if self._alias is not None:
            result += f" AS {self._alias}"
        return result

    def as_(self, alias: str) -> SubqueryExpr:
        return SubqueryExpr(self.query, alias)


@dataclasses.dataclass(eq=False)
class ExistsExpr(Expression):
    """``EXISTS (SELECT ...)``."""

    subquery: SubqueryExpr

    def to_sql(self, params: Params) -> str:
        return f"EXISTS {self.subquery.to_sql(params)}"
