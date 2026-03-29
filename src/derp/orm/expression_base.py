"""Base Expression class — separated to avoid circular imports with Column."""

from __future__ import annotations

import abc
from enum import StrEnum
from typing import Any, Literal


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


class Expression(abc.ABC):
    """Base class for SQL expressions."""

    @abc.abstractmethod
    def to_sql(self, params: list[Any]) -> str:
        """Generate SQL string with parameterized values."""

    def __and__(self, other: Expression) -> Expression:
        from derp.orm.query.expressions import LogicalOp

        return LogicalOp(LogicalOperator.AND, (self, other))

    def __or__(self, other: Expression) -> Expression:
        from derp.orm.query.expressions import LogicalOp

        return LogicalOp(LogicalOperator.OR, (self, other))

    def __invert__(self) -> Expression:
        from derp.orm.query.expressions import UnaryOp

        return UnaryOp("NOT", self)

    def __eq__(self, other: Any) -> Any:
        from derp.orm.query.expressions import BinaryOp, to_expr

        return BinaryOp(self, ComparisonOperator.EQ, to_expr(other))

    def __ne__(self, other: Any) -> Any:
        from derp.orm.query.expressions import BinaryOp, to_expr

        return BinaryOp(self, ComparisonOperator.NE, to_expr(other))

    def __lt__(self, other: Any) -> Any:
        from derp.orm.query.expressions import BinaryOp, to_expr

        return BinaryOp(self, ComparisonOperator.LT, to_expr(other))

    def __le__(self, other: Any) -> Any:
        from derp.orm.query.expressions import BinaryOp, to_expr

        return BinaryOp(self, ComparisonOperator.LTE, to_expr(other))

    def __gt__(self, other: Any) -> Any:
        from derp.orm.query.expressions import BinaryOp, to_expr

        return BinaryOp(self, ComparisonOperator.GT, to_expr(other))

    def __ge__(self, other: Any) -> Any:
        from derp.orm.query.expressions import BinaryOp, to_expr

        return BinaryOp(self, ComparisonOperator.GTE, to_expr(other))

    def in_(self, values: Any) -> Any:
        from derp.orm.query.expressions import InList, InSubquery

        if hasattr(values, "build"):
            return InSubquery(self, values, negated=False)
        return InList(self, tuple(values), negated=False)

    def not_in(self, values: Any) -> Any:
        from derp.orm.query.expressions import InList, InSubquery

        if hasattr(values, "build"):
            return InSubquery(self, values, negated=True)
        return InList(self, tuple(values), negated=True)

    def like(self, pattern: str) -> Any:
        from derp.orm.query.expressions import Like

        return Like(self, pattern, case_insensitive=False)

    def ilike(self, pattern: str) -> Any:
        from derp.orm.query.expressions import Like

        return Like(self, pattern, case_insensitive=True)

    def is_null(self) -> Any:
        from derp.orm.query.expressions import NullCheck

        return NullCheck(self, is_null=True)

    def is_not_null(self) -> Any:
        from derp.orm.query.expressions import NullCheck

        return NullCheck(self, is_null=False)

    def between(self, low: Any, high: Any) -> Any:
        from derp.orm.query.expressions import Between

        return Between(self, low, high)

    def matches(
        self,
        query: str,
        *,
        language: str = "english",
        style: Literal["websearch", "plain", "phrase"] = "websearch",
        stored: bool = False,
    ) -> Any:
        """Full-text search match using ``@@``.

        *stored*: set to ``True`` when the column is a pre-computed
        tsvector (skips the ``to_tsvector()`` wrapper).
        *style*: ``"websearch"`` (default), ``"plain"``, or ``"phrase"``.
        """
        from derp.orm.query.expressions import TSMatch, _TSQueryStyle

        return TSMatch(self, query, language, _TSQueryStyle.from_short(style), stored)

    def ts_rank(
        self,
        query: str,
        *,
        language: str = "english",
        style: Literal["websearch", "plain", "phrase"] = "websearch",
        stored: bool = False,
    ) -> Any:
        """Full-text search rank for ORDER BY.

        *stored*: set to ``True`` when the column is a pre-computed
        tsvector (skips the ``to_tsvector()`` wrapper).
        """
        from derp.orm.query.expressions import TSRank, _TSQueryStyle

        return TSRank(self, query, language, _TSQueryStyle.from_short(style), stored)

    def ts_headline(
        self,
        query: str,
        *,
        language: str = "english",
        style: Literal["websearch", "plain", "phrase"] = "websearch",
        max_words: int | None = None,
        min_words: int | None = None,
        max_fragments: int | None = None,
        start_sel: str | None = None,
        stop_sel: str | None = None,
        fragment_delimiter: str | None = None,
        highlight_all: bool | None = None,
        short_word: int | None = None,
    ) -> Any:
        """Highlighted search snippet for display in results."""
        from derp.orm.query.expressions import TSHeadline, _TSQueryStyle

        opts = {
            "max_words": max_words,
            "min_words": min_words,
            "max_fragments": max_fragments,
            "start_sel": start_sel,
            "stop_sel": stop_sel,
            "fragment_delimiter": fragment_delimiter,
            "highlight_all": highlight_all,
            "short_word": short_word,
        }
        return TSHeadline(self, query, language, _TSQueryStyle.from_short(style), opts)
