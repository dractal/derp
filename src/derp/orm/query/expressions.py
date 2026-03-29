"""WHERE clause expressions and operators for Derp ORM."""

from __future__ import annotations

import dataclasses
import enum
import re
from typing import Any

from derp.orm.expression_base import ComparisonOperator, Expression, LogicalOperator


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
class CastLiteral(Expression):
    """Literal value with an explicit SQL cast (e.g. ``$1::vector``)."""

    value: Any
    cast: str

    def to_sql(self, params: list[Any]) -> str:
        params.append(self.value)
        return f"${len(params)}::{self.cast}"


@dataclasses.dataclass
class BinaryOp(Expression):
    """Binary operator expression (e.g., a = b)."""

    left: Expression | Any
    operator: ComparisonOperator | str
    right: Expression | Any

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

    column: Expression
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

    column: Expression
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

    column: Expression
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

    column: Expression
    is_null: bool = True

    def to_sql(self, params: list[Any]) -> str:
        col_sql = _expr_to_sql(self.column, params)
        op = "IS NULL" if self.is_null else "IS NOT NULL"
        return f"({col_sql} {op})"


@dataclasses.dataclass
class Like(Expression):
    """LIKE/ILIKE pattern matching."""

    column: Expression
    pattern: str
    case_insensitive: bool = False

    def to_sql(self, params: list[Any]) -> str:
        col_sql = _expr_to_sql(self.column, params)
        params.append(self.pattern)
        op = "ILIKE" if self.case_insensitive else "LIKE"
        return f"({col_sql} {op} ${len(params)})"


def _expr_to_sql(expr: Expression | Any, params: list[Any]) -> str:
    """Convert an expression or literal to SQL."""

    if isinstance(expr, Expression):
        return expr.to_sql(params)
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


class _TSQueryStyle(enum.StrEnum):
    """PostgreSQL full-text query parser functions."""

    PLAIN = "plainto_tsquery"
    WEBSEARCH = "websearch_to_tsquery"
    PHRASE = "phraseto_tsquery"

    @classmethod
    def from_short(cls, name: str) -> _TSQueryStyle:
        """Resolve short name (``"websearch"``) or full enum value."""
        _SHORT = {
            "plain": cls.PLAIN,
            "websearch": cls.WEBSEARCH,
            "phrase": cls.PHRASE,
        }
        return _SHORT.get(name) or cls(name)


def _ts_vector_sql(
    column: Expression,
    language: str,
    params: list[Any],
    *,
    stored: bool = False,
) -> str:
    """Emit ``to_tsvector($lang::regconfig, col)`` or just ``col`` when
    *stored* is True (for pre-computed tsvector columns)."""
    col_sql = _expr_to_sql(column, params)
    if stored:
        return col_sql
    params.append(language)
    return f"to_tsvector(${len(params)}::regconfig, {col_sql})"


def _ts_query_sql(
    style: _TSQueryStyle,
    language: str,
    query: str,
    params: list[Any],
) -> str:
    """Emit ``websearch_to_tsquery($lang::regconfig, $query)``."""
    params.append(language)
    lang_ph = f"${len(params)}"
    params.append(query)
    query_ph = f"${len(params)}"
    return f"{style}({lang_ph}::regconfig, {query_ph})"


@dataclasses.dataclass
class TSMatch(Expression):
    """Full-text search match (``@@ websearch_to_tsquery(...)``)."""

    column: Expression
    query: str
    language: str = "english"
    style: _TSQueryStyle = _TSQueryStyle.WEBSEARCH
    stored: bool = False

    def to_sql(self, params: list[Any]) -> str:
        vec = _ts_vector_sql(self.column, self.language, params, stored=self.stored)
        tsq = _ts_query_sql(self.style, self.language, self.query, params)
        return f"({vec} @@ {tsq})"


@dataclasses.dataclass
class TSRank(Expression):
    """Full-text search rank for ORDER BY."""

    column: Expression
    query: str
    language: str = "english"
    style: _TSQueryStyle = _TSQueryStyle.WEBSEARCH
    stored: bool = False
    _alias: str | None = dataclasses.field(default=None, repr=False)

    def to_sql(self, params: list[Any]) -> str:
        vec = _ts_vector_sql(self.column, self.language, params, stored=self.stored)
        tsq = _ts_query_sql(self.style, self.language, self.query, params)
        result = f"ts_rank({vec}, {tsq})"
        if self._alias is not None:
            result += f" AS {self._alias}"
        return result

    def as_(self, alias: str) -> TSRank:
        """Return a copy with an AS alias."""
        return TSRank(
            self.column,
            self.query,
            self.language,
            self.style,
            self.stored,
            _alias=alias,
        )


# Maps Python kwarg names → PostgreSQL ts_headline option names.
_HEADLINE_OPTION_KEYS: dict[str, str] = {
    "max_words": "MaxWords",
    "min_words": "MinWords",
    "max_fragments": "MaxFragments",
    "start_sel": "StartSel",
    "stop_sel": "StopSel",
    "fragment_delimiter": "FragmentDelimiter",
    "highlight_all": "HighlightAll",
    "short_word": "ShortWord",
}


def _headline_options_to_pg(opts: dict[str, Any]) -> str | None:
    """Build the PostgreSQL options string from non-None kwargs."""
    parts: list[str] = []
    for py_key, pg_key in _HEADLINE_OPTION_KEYS.items():
        val = opts.get(py_key)
        if val is None:
            continue
        if isinstance(val, bool):
            parts.append(f"{pg_key}={'true' if val else 'false'}")
        else:
            parts.append(f"{pg_key}={val}")
    return ", ".join(parts) if parts else None


@dataclasses.dataclass
class TSHeadline(Expression):
    """Search result snippet with highlighted matches.

    Produces ``ts_headline(lang::regconfig, col, query[, options])``.
    """

    column: Expression
    query: str
    language: str = "english"
    style: _TSQueryStyle = _TSQueryStyle.WEBSEARCH
    headline_options: dict[str, Any] = dataclasses.field(default_factory=dict)
    _alias: str | None = dataclasses.field(default=None, repr=False)

    def to_sql(self, params: list[Any]) -> str:
        params.append(self.language)
        lang_ph = f"${len(params)}"
        col_sql = _expr_to_sql(self.column, params)
        tsq = _ts_query_sql(self.style, self.language, self.query, params)
        opts_str = _headline_options_to_pg(self.headline_options)
        if opts_str is not None:
            params.append(opts_str)
            opts_ph = f", ${len(params)}"
        else:
            opts_ph = ""
        result = f"ts_headline({lang_ph}::regconfig, {col_sql}, {tsq}{opts_ph})"
        if self._alias is not None:
            result += f" AS {self._alias}"
        return result

    def as_(self, alias: str) -> TSHeadline:
        """Return a copy with an AS alias."""
        return TSHeadline(
            self.column,
            self.query,
            self.language,
            self.style,
            self.headline_options,
            _alias=alias,
        )


@dataclasses.dataclass
class AggregateFunc(Expression):
    """SQL aggregate function expression (COUNT, SUM, AVG, MIN, MAX)."""

    func: str
    arg: Expression
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

    operand: Expression
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


def to_expr(value: Expression | Any) -> Expression:
    """Ensure value is an expression."""
    if isinstance(value, Expression):
        return value
    return Literal(value)
