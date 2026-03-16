"""WHERE clause expressions and operators for Derp ORM."""

from __future__ import annotations

import abc
import dataclasses
import re
from collections.abc import Sequence
from enum import StrEnum
from typing import Any

from derp.orm.fields import FieldInfo


class LogicalOperator(StrEnum):
    """SQL logical operators."""

    AND = "AND"
    OR = "OR"


class ComparisonOperator(StrEnum):
    """SQL comparison operators."""

    EQ = "="
    NE = "<>"
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="


@dataclasses.dataclass
class Expression(abc.ABC):
    """Base class for SQL expressions."""

    @abc.abstractmethod
    def to_sql(self, params: list[Any]) -> str:
        """Generate SQL string with parameterized values.

        Args:
            params: List to append parameter values to

        Returns:
            SQL string with $N placeholders
        """

    def __and__(self, other: Expression) -> Expression:
        return LogicalOp(LogicalOperator.AND, (self, other))

    def __or__(self, other: Expression) -> Expression:
        return LogicalOp(LogicalOperator.OR, (self, other))

    def __invert__(self) -> Expression:
        return UnaryOp("NOT", self)

    def __eq__(self, other: Any) -> Any:
        """Equal comparison: field == value."""
        return BinaryOp(self, ComparisonOperator.EQ, to_expr(other))

    def __ne__(self, other: Any) -> Any:
        """Not equal comparison: field != value."""
        return BinaryOp(self, ComparisonOperator.NE, to_expr(other))

    def __lt__(self, other: Any) -> Any:
        """Less than comparison: field < value."""
        return BinaryOp(self, ComparisonOperator.LT, to_expr(other))

    def __le__(self, other: Any) -> Any:
        """Less than or equal comparison: field <= value."""
        return BinaryOp(self, ComparisonOperator.LTE, to_expr(other))

    def __gt__(self, other: Any) -> Any:
        """Greater than comparison: field > value."""
        return BinaryOp(self, ComparisonOperator.GT, to_expr(other))

    def __ge__(self, other: Any) -> Any:
        """Greater than or equal comparison: field >= value."""
        return BinaryOp(self, ComparisonOperator.GTE, to_expr(other))

    def in_(self, values: Sequence[Any] | Any) -> Any:
        """IN clause. Accepts a list of values or a SelectQuery."""
        if hasattr(values, "build"):
            return InSubquery(self, values, negated=False)
        return InList(self, tuple(values), negated=False)

    def not_in(self, values: Sequence[Any] | Any) -> Any:
        """NOT IN clause. Accepts a list of values or a SelectQuery."""
        if hasattr(values, "build"):
            return InSubquery(self, values, negated=True)
        return InList(self, tuple(values), negated=True)

    def like(self, pattern: str) -> Any:
        """LIKE pattern matching."""
        return Like(self, pattern, case_insensitive=False)

    def ilike(self, pattern: str) -> Any:
        """ILIKE pattern matching."""
        return Like(self, pattern, case_insensitive=True)

    def is_null(self) -> Any:
        """IS NULL check."""
        return NullCheck(self, is_null=True)

    def is_not_null(self) -> Any:
        """IS NOT NULL check."""
        return NullCheck(self, is_null=False)

    def between(self, low: Any, high: Any) -> Any:
        """BETWEEN range check."""
        return Between(self, low, high)


@dataclasses.dataclass(eq=False)
class ColumnRef(Expression):
    """Reference to a table column."""

    table_name: str
    column_name: str

    def to_sql(self, params: list[Any]) -> str:
        return f"{self.table_name}.{self.column_name}"


@dataclasses.dataclass
class Literal(Expression):
    """Literal value."""

    value: Any

    def to_sql(self, params: list[Any]) -> str:
        params.append(self.value)
        return f"${len(params)}"


@dataclasses.dataclass
class BinaryOp(Expression):
    """Binary operator expression (e.g., a = b)."""

    left: Expression | FieldInfo | Any
    operator: ComparisonOperator | str
    right: Expression | FieldInfo | Any

    def to_sql(self, params: list[Any]) -> str:
        left_sql = _expr_to_sql(self.left, params)
        right_sql = _expr_to_sql(self.right, params)
        return f"({left_sql} {self.operator} {right_sql})"


@dataclasses.dataclass
class UnaryOp(Expression):
    """Unary operator expression (e.g., NOT a)."""

    operator: str
    operand: Expression

    def to_sql(self, params: list[Any]) -> str:
        operand_sql = _expr_to_sql(self.operand, params)
        return f"({self.operator} {operand_sql})"


@dataclasses.dataclass
class LogicalOp(Expression):
    """Logical combination of expressions (AND/OR)."""

    operator: LogicalOperator
    conditions: tuple[Expression, ...]

    def to_sql(self, params: list[Any]) -> str:
        if not self.conditions:
            return "TRUE" if self.operator == LogicalOperator.AND else "FALSE"
        parts = [_expr_to_sql(c, params) for c in self.conditions]
        return f"({f' {self.operator} '.join(parts)})"


@dataclasses.dataclass
class InList(Expression):
    """IN expression (a IN (1, 2, 3))."""

    column: Expression | FieldInfo
    values: tuple[Any, ...]
    negated: bool = False

    def to_sql(self, params: list[Any]) -> str:
        if not self.values:
            return "FALSE" if not self.negated else "TRUE"
        col_sql = _expr_to_sql(self.column, params)
        placeholders = []
        for v in self.values:
            params.append(v)
            placeholders.append(f"${len(params)}")
        op = "NOT IN" if self.negated else "IN"
        return f"({col_sql} {op} ({', '.join(placeholders)}))"


@dataclasses.dataclass
class InSubquery(Expression):
    """IN (SELECT ...) expression."""

    column: Expression | FieldInfo
    query: Any  # SelectQuery — typed as Any to avoid circular import
    negated: bool = False

    def to_sql(self, params: list[Any]) -> str:
        col_sql = _expr_to_sql(self.column, params)
        sub_sql, sub_params = self.query.build()
        offset = len(params)
        params.extend(sub_params)
        renumbered = _renumber_params(sub_sql, offset)
        op = "NOT IN" if self.negated else "IN"
        return f"({col_sql} {op} ({renumbered}))"


@dataclasses.dataclass
class Between(Expression):
    """BETWEEN expression."""

    column: Expression | FieldInfo
    low: Any
    high: Any

    def to_sql(self, params: list[Any]) -> str:
        col_sql = _expr_to_sql(self.column, params)
        params.append(self.low)
        low_placeholder = f"${len(params)}"
        params.append(self.high)
        high_placeholder = f"${len(params)}"
        return f"({col_sql} BETWEEN {low_placeholder} AND {high_placeholder})"


@dataclasses.dataclass
class NullCheck(Expression):
    """IS NULL / IS NOT NULL expression."""

    column: Expression | FieldInfo
    is_null: bool = True

    def to_sql(self, params: list[Any]) -> str:
        col_sql = _expr_to_sql(self.column, params)
        op = "IS NULL" if self.is_null else "IS NOT NULL"
        return f"({col_sql} {op})"


@dataclasses.dataclass
class Like(Expression):
    """LIKE/ILIKE pattern matching."""

    column: Expression | FieldInfo
    pattern: str
    case_insensitive: bool = False

    def to_sql(self, params: list[Any]) -> str:
        col_sql = _expr_to_sql(self.column, params)
        params.append(self.pattern)
        op = "ILIKE" if self.case_insensitive else "LIKE"
        return f"({col_sql} {op} ${len(params)})"


def _expr_to_sql(expr: Expression | FieldInfo | Any, params: list[Any]) -> str:
    """Convert an expression, FieldInfo, or literal to SQL."""

    if isinstance(expr, Expression):
        return expr.to_sql(params)
    elif isinstance(expr, FieldInfo):
        # FieldInfo stores table and column name
        if expr._table_name and expr._field_name:
            return f"{expr._table_name}.{expr._field_name}"
        elif expr._field_name:
            return expr._field_name
        raise ValueError("FieldInfo missing table/column name metadata")
    else:
        # Literal value
        params.append(expr)
        return f"${len(params)}"


@dataclasses.dataclass
class RawSQL(Expression):
    """Raw SQL fragment with optional parameterized values.

    Use the ``sql()`` factory function to create instances::

        sql("NOW()")
        sql("age > {}", 18)
        sql("age > {} AND name = {}", 18, "Alice")
    """

    template: str
    values: tuple[Any, ...]
    _alias: str | None = dataclasses.field(default=None, repr=False)

    def to_sql(self, params: list[Any]) -> str:
        parts = self.template.split("{}")
        result = parts[0]
        for i, val in enumerate(self.values):
            params.append(val)
            result += f"${len(params)}"
            if i + 1 < len(parts):
                result += parts[i + 1]
        if self._alias is not None:
            result += f" AS {self._alias}"
        return result

    def as_(self, alias: str) -> RawSQL:
        """Return a copy with an AS alias."""
        return RawSQL(self.template, self.values, _alias=alias)


def sql(template: str, *values: Any) -> RawSQL:
    """Create a raw SQL expression fragment.

    Use ``{}`` as placeholder for parameterized values::

        sql("NOW()")
        sql("age > {}", 18)
        sql("CONCAT({}, name)", "Dr. ")
    """
    return RawSQL(template, values)


@dataclasses.dataclass
class AggregateFunc(Expression):
    """SQL aggregate function expression (COUNT, SUM, AVG, MIN, MAX)."""

    func: str
    arg: Expression | FieldInfo
    _alias: str | None = dataclasses.field(default=None, repr=False)

    def to_sql(self, params: list[Any]) -> str:
        arg_sql = _expr_to_sql(self.arg, params)
        result = f"{self.func}({arg_sql})"
        if self._alias is not None:
            result += f" AS {self._alias}"
        return result

    def as_(self, alias: str) -> AggregateFunc:
        """Return a copy with an AS alias."""
        return AggregateFunc(self.func, self.arg, _alias=alias)


@dataclasses.dataclass
class CaseExpression(Expression):
    """SQL CASE expression.

    Simple CASE: ``CASE operand WHEN val THEN result ... END``
    """

    operand: Expression | FieldInfo
    whens: list[tuple[Any, Any]]
    else_value: Any | None = dataclasses.field(default=None)
    _alias: str | None = dataclasses.field(default=None, repr=False)

    def to_sql(self, params: list[Any]) -> str:
        operand_sql = _expr_to_sql(self.operand, params)
        result = f"CASE {operand_sql}"
        for cond, val in self.whens:
            params.append(cond)
            cond_ph = f"${len(params)}"
            params.append(val)
            val_ph = f"${len(params)}"
            result += f" WHEN {cond_ph} THEN {val_ph}"
        if self.else_value is not None:
            params.append(self.else_value)
            result += f" ELSE ${len(params)}"
        result += " END"
        if self._alias is not None:
            result += f" AS {self._alias}"
        return result

    def as_(self, alias: str) -> CaseExpression:
        """Return a copy with an AS alias."""
        return CaseExpression(self.operand, self.whens, self.else_value, _alias=alias)


_PARAM_RE = re.compile(r"\$(\d+)")


def _renumber_params(sql_str: str, offset: int) -> str:
    """Shift all ``$N`` placeholders in *sql_str* by *offset*."""
    if offset == 0:
        return sql_str
    return _PARAM_RE.sub(lambda m: f"${int(m.group(1)) + offset}", sql_str)


@dataclasses.dataclass
class SubqueryExpr(Expression):
    """A SELECT query wrapped as an expression (for use in WHERE/SELECT/FROM)."""

    query: Any  # SelectQuery — typed as Any to avoid circular import
    _alias: str | None = dataclasses.field(default=None, repr=False)

    def to_sql(self, params: list[Any]) -> str:
        sub_sql, sub_params = self.query.build()
        offset = len(params)
        params.extend(sub_params)
        renumbered = _renumber_params(sub_sql, offset)
        result = f"({renumbered})"
        if self._alias is not None:
            result += f" AS {self._alias}"
        return result

    def as_(self, alias: str) -> SubqueryExpr:
        """Return a copy with an AS alias."""
        return SubqueryExpr(self.query, _alias=alias)


@dataclasses.dataclass
class ExistsExpr(Expression):
    """EXISTS (SELECT ...) expression."""

    subquery: SubqueryExpr

    def to_sql(self, params: list[Any]) -> str:
        sub_sql = self.subquery.to_sql(params)
        return f"EXISTS {sub_sql}"


def to_expr(value: Expression | FieldInfo | Any) -> Expression | FieldInfo:
    """Ensure value is an expression or FieldInfo."""
    if isinstance(value, Expression | FieldInfo):
        return value
    return Literal(value)
