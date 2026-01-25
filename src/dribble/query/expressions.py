"""WHERE clause expressions and operators for Dribble ORM."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dribble.fields import FieldInfo


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


@dataclass
class Expression:
    """Base class for SQL expressions."""

    def to_sql(self, params: list[Any]) -> str:
        """Generate SQL string with parameterized values.

        Args:
            params: List to append parameter values to

        Returns:
            SQL string with $N placeholders
        """
        raise NotImplementedError


@dataclass
class ColumnRef(Expression):
    """Reference to a table column."""

    table_name: str
    column_name: str

    def to_sql(self, params: list[Any]) -> str:
        return f"{self.table_name}.{self.column_name}"


@dataclass
class Literal(Expression):
    """Literal value."""

    value: Any

    def to_sql(self, params: list[Any]) -> str:
        params.append(self.value)
        return f"${len(params)}"


@dataclass
class BinaryOp(Expression):
    """Binary operator expression (e.g., a = b)."""

    left: Expression | FieldInfo | Any
    operator: ComparisonOperator | str
    right: Expression | FieldInfo | Any

    def to_sql(self, params: list[Any]) -> str:
        left_sql = _expr_to_sql(self.left, params)
        right_sql = _expr_to_sql(self.right, params)
        return f"({left_sql} {self.operator} {right_sql})"


@dataclass
class UnaryOp(Expression):
    """Unary operator expression (e.g., NOT a)."""

    operator: str
    operand: Expression

    def to_sql(self, params: list[Any]) -> str:
        operand_sql = _expr_to_sql(self.operand, params)
        return f"({self.operator} {operand_sql})"


@dataclass
class LogicalOp(Expression):
    """Logical combination of expressions (AND/OR)."""

    operator: LogicalOperator
    conditions: tuple[Expression, ...]

    def to_sql(self, params: list[Any]) -> str:
        if not self.conditions:
            return "TRUE" if self.operator == LogicalOperator.AND else "FALSE"
        parts = [_expr_to_sql(c, params) for c in self.conditions]
        return f"({f' {self.operator} '.join(parts)})"


@dataclass
class InList(Expression):
    """IN expression (a IN (1, 2, 3))."""

    column: Expression | FieldInfo
    values: tuple[Any, ...]
    negated: bool = False

    def to_sql(self, params: list[Any]) -> str:
        col_sql = _expr_to_sql(self.column, params)
        placeholders = []
        for v in self.values:
            params.append(v)
            placeholders.append(f"${len(params)}")
        op = "NOT IN" if self.negated else "IN"
        return f"({col_sql} {op} ({', '.join(placeholders)}))"


@dataclass
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


@dataclass
class NullCheck(Expression):
    """IS NULL / IS NOT NULL expression."""

    column: Expression | FieldInfo
    is_null: bool = True

    def to_sql(self, params: list[Any]) -> str:
        col_sql = _expr_to_sql(self.column, params)
        op = "IS NULL" if self.is_null else "IS NOT NULL"
        return f"({col_sql} {op})"


@dataclass
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
    from dribble.fields import FieldInfo

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


def _to_expr(value: Expression | FieldInfo | Any) -> Expression | FieldInfo:
    """Ensure value is an expression or FieldInfo."""
    from dribble.fields import FieldInfo

    if isinstance(value, Expression | FieldInfo):
        return value
    return Literal(value)


# Comparison operators


def eq(left: FieldInfo | Expression | Any, right: FieldInfo | Expression | Any) -> BinaryOp:
    """Equal (=) comparison."""
    return BinaryOp(_to_expr(left), ComparisonOperator.EQ, _to_expr(right))


def ne(left: FieldInfo | Expression | Any, right: FieldInfo | Expression | Any) -> BinaryOp:
    """Not equal (<>) comparison."""
    return BinaryOp(_to_expr(left), ComparisonOperator.NE, _to_expr(right))


def gt(left: FieldInfo | Expression | Any, right: FieldInfo | Expression | Any) -> BinaryOp:
    """Greater than (>) comparison."""
    return BinaryOp(_to_expr(left), ComparisonOperator.GT, _to_expr(right))


def gte(left: FieldInfo | Expression | Any, right: FieldInfo | Expression | Any) -> BinaryOp:
    """Greater than or equal (>=) comparison."""
    return BinaryOp(_to_expr(left), ComparisonOperator.GTE, _to_expr(right))


def lt(left: FieldInfo | Expression | Any, right: FieldInfo | Expression | Any) -> BinaryOp:
    """Less than (<) comparison."""
    return BinaryOp(_to_expr(left), ComparisonOperator.LT, _to_expr(right))


def lte(left: FieldInfo | Expression | Any, right: FieldInfo | Expression | Any) -> BinaryOp:
    """Less than or equal (<=) comparison."""
    return BinaryOp(_to_expr(left), ComparisonOperator.LTE, _to_expr(right))


# Logical operators


def and_(*conditions: Expression) -> LogicalOp:
    """Logical AND of conditions."""
    return LogicalOp(LogicalOperator.AND, conditions)


def or_(*conditions: Expression) -> LogicalOp:
    """Logical OR of conditions."""
    return LogicalOp(LogicalOperator.OR, conditions)


def not_(condition: Expression) -> UnaryOp:
    """Logical NOT."""
    return UnaryOp("NOT", condition)


# Pattern matching


def like(column: FieldInfo | Expression, pattern: str) -> Like:
    """Case-sensitive LIKE pattern matching."""
    return Like(_to_expr(column), pattern, case_insensitive=False)


def ilike(column: FieldInfo | Expression, pattern: str) -> Like:
    """Case-insensitive ILIKE pattern matching."""
    return Like(_to_expr(column), pattern, case_insensitive=True)


# Membership


def in_(column: FieldInfo | Expression, values: list[Any] | tuple[Any, ...]) -> InList:
    """IN membership check."""
    return InList(_to_expr(column), tuple(values), negated=False)


def not_in(column: FieldInfo | Expression, values: list[Any] | tuple[Any, ...]) -> InList:
    """NOT IN membership check."""
    return InList(_to_expr(column), tuple(values), negated=True)


# Null checks


def is_null(column: FieldInfo | Expression) -> NullCheck:
    """IS NULL check."""
    return NullCheck(_to_expr(column), is_null=True)


def is_not_null(column: FieldInfo | Expression) -> NullCheck:
    """IS NOT NULL check."""
    return NullCheck(_to_expr(column), is_null=False)


# Range


def between(column: FieldInfo | Expression, low: Any, high: Any) -> Between:
    """BETWEEN range check."""
    return Between(_to_expr(column), low, high)
