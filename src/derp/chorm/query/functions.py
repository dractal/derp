"""ClickHouse function helpers — a strongly-typed surface for the most
common scalar, array, string, date, math, and aggregate functions.

Use the catch-all ``f`` builder for any function not pre-defined::

    f.toDate(col)       # toDate(col)
    f.lengthUTF8(s)     # lengthUTF8(s)
    f("anyFn", a, b)    # anyFn(a, b)
"""

from __future__ import annotations

from typing import Any

from derp.chorm.query.expressions import (
    AggregateFunc,
    FunctionCall,
    Literal,
    WindowFunc,
    to_expr,
)


def lit(value: Any, ch_type: str | None = None) -> Literal:
    """Wrap a Python value as a typed literal."""
    return Literal(value, ch_type)


class _Functions:
    """Dispatcher for ClickHouse function calls."""

    def __call__(self, name: str, *args: Any) -> FunctionCall:
        return FunctionCall(name, tuple(to_expr(a) for a in args))

    def __getattr__(self, name: str) -> Any:
        # Special: aggregate functions known to support combinators.
        if name in _AGG_FN_NAMES:

            def _builder(*args: Any, **kwargs: Any) -> AggregateFunc:
                expr_args = tuple(to_expr(a) for a in args)
                p = kwargs.pop("params", ())
                if not isinstance(p, tuple):
                    p = (p,)
                return AggregateFunc(
                    name,
                    expr_args,
                    params=tuple(to_expr(x) for x in p),
                )

            return _builder

        def _builder(*args: Any) -> FunctionCall:
            return FunctionCall(name, tuple(to_expr(a) for a in args))

        return _builder


f = _Functions()


# =============================================================================
# Aggregate function name registry
# =============================================================================

_AGG_FN_NAMES: frozenset[str] = frozenset(
    [
        "count",
        "sum",
        "sumKahan",
        "min",
        "max",
        "avg",
        "any",
        "anyLast",
        "anyHeavy",
        "uniq",
        "uniqExact",
        "uniqCombined",
        "uniqCombined64",
        "uniqHLL12",
        "uniqTheta",
        "uniqUpTo",
        "groupArray",
        "groupUniqArray",
        "groupArrayInsertAt",
        "groupArrayMovingSum",
        "groupArrayMovingAvg",
        "groupArraySample",
        "groupBitAnd",
        "groupBitOr",
        "groupBitXor",
        "groupBitmap",
        "groupBitmapAnd",
        "groupBitmapOr",
        "groupBitmapXor",
        "quantile",
        "quantiles",
        "quantileDeterministic",
        "quantileExact",
        "quantileExactWeighted",
        "quantileTiming",
        "quantileTimingWeighted",
        "quantileTDigest",
        "quantileTDigestWeighted",
        "quantileBFloat16",
        "median",
        "medianExact",
        "medianExactWeighted",
        "medianTiming",
        "medianTimingWeighted",
        "medianTDigest",
        "medianTDigestWeighted",
        "stddevPop",
        "stddevSamp",
        "varPop",
        "varSamp",
        "covarPop",
        "covarSamp",
        "corr",
        "corrMatrix",
        "entropy",
        "kurtPop",
        "kurtSamp",
        "skewPop",
        "skewSamp",
        "topK",
        "topKWeighted",
        "argMin",
        "argMax",
        "boundingRatio",
        "first_value",
        "last_value",
        "deltaSum",
        "deltaSumTimestamp",
        "simpleLinearRegression",
        "stochasticLinearRegression",
        "stochasticLogisticRegression",
        "categoricalInformationValue",
        "studentTTest",
        "welchTTest",
        "kolmogorovSmirnovTest",
        "mannWhitneyUTest",
        "rankCorr",
        "meanZTest",
        "intervalLengthSum",
        "sumMap",
        "minMap",
        "maxMap",
        "anyResample",
        "histogram",
        "sequenceMatch",
        "sequenceCount",
        "windowFunnel",
        "retention",
        "uniqLastEqual",
        "sumWithOverflow",
        "sumIf",
        "countIf",
        "avgIf",
        "anyIf",
        "minIf",
        "maxIf",
        "row_number",
        "rank",
        "dense_rank",
        "ntile",
        "lagInFrame",
        "leadInFrame",
        "exponentialMovingAverage",
        "exponentialTimeDecayedAvg",
        "exponentialTimeDecayedCount",
        "exponentialTimeDecayedMax",
        "exponentialTimeDecayedSum",
    ]
)


# =============================================================================
# Window function helper
# =============================================================================


def over(
    func: Any,
    *,
    partition_by: Any = (),
    order_by: Any = (),
    frame: str | None = None,
) -> WindowFunc:
    """Wrap *func* into a window expression: ``func OVER (...)``.

    *partition_by* / *order_by* accept a single expression or a sequence.
    *order_by* items may be plain expressions (ASC) or ``(expr, "DESC")``
    tuples.
    """
    if not isinstance(partition_by, tuple | list):
        partition_by = (partition_by,)
    pb = tuple(to_expr(p) for p in partition_by)

    # Disambiguate (expr, "DESC") as a single pair from a multi-element
    # sequence: if order_by is a 2-tuple whose second element is a
    # direction string, treat it as a single (expr, direction) pair.
    if (
        isinstance(order_by, tuple)
        and len(order_by) == 2
        and isinstance(order_by[1], str)
        and order_by[1].upper() in ("ASC", "DESC")
    ):
        order_by = (order_by,)
    elif not isinstance(order_by, tuple | list):
        order_by = (order_by,)
    ob: list[tuple[Any, str]] = []
    for item in order_by:
        if isinstance(item, tuple) and len(item) == 2 and isinstance(item[1], str):
            ob.append((to_expr(item[0]), item[1].upper()))
        else:
            ob.append((to_expr(item), "ASC"))
    return WindowFunc(to_expr(func), pb, tuple(ob), frame)


# =============================================================================
# Convenience helpers
# =============================================================================


def array(*items: Any) -> FunctionCall:
    """``[a, b, c]`` rendered as ``array(a, b, c)``."""
    return f("array", *items)


def tuple_(*items: Any) -> FunctionCall:
    """``tuple(a, b, c)``."""
    return f("tuple", *items)


def map_(*kv: Any) -> FunctionCall:
    """``map(k1, v1, k2, v2, ...)``."""
    return f("map", *kv)


def if_(cond: Any, then: Any, else_: Any) -> FunctionCall:
    """``if(cond, then, else)``."""
    return f("if", cond, then, else_)


def coalesce(*args: Any) -> FunctionCall:
    return f("coalesce", *args)


def ifnull(a: Any, b: Any) -> FunctionCall:
    return f("ifNull", a, b)


def cast(value: Any, ch_type: str) -> FunctionCall:
    return f("CAST", to_expr(value), Literal(ch_type, "String"))


def to_date(value: Any) -> FunctionCall:
    return f("toDate", value)


def to_datetime(value: Any) -> FunctionCall:
    return f("toDateTime", value)


def to_yyyymm(value: Any) -> FunctionCall:
    return f("toYYYYMM", value)


def to_yyyymmdd(value: Any) -> FunctionCall:
    return f("toYYYYMMDD", value)


def to_start_of_day(value: Any) -> FunctionCall:
    return f("toStartOfDay", value)


def to_start_of_hour(value: Any) -> FunctionCall:
    return f("toStartOfHour", value)


def to_start_of_minute(value: Any) -> FunctionCall:
    return f("toStartOfMinute", value)


def to_start_of_month(value: Any) -> FunctionCall:
    return f("toStartOfMonth", value)


def date_diff(unit: str, a: Any, b: Any) -> FunctionCall:
    return f("dateDiff", Literal(unit, "String"), a, b)


def date_add(unit: str, n: int, value: Any) -> FunctionCall:
    return f("dateAdd", Literal(unit, "String"), n, value)


def array_join(value: Any) -> FunctionCall:
    """``arrayJoin(arr)`` — turn an array into a column."""
    return f("arrayJoin", value)


def length(value: Any) -> FunctionCall:
    return f("length", value)


def lower(value: Any) -> FunctionCall:
    return f("lower", value)


def upper(value: Any) -> FunctionCall:
    return f("upper", value)


def concat(*args: Any) -> FunctionCall:
    return f("concat", *args)


def position(haystack: Any, needle: Any) -> FunctionCall:
    return f("position", haystack, needle)


def substring(value: Any, start: int, length_: int | None = None) -> FunctionCall:
    args: tuple[Any, ...] = (value, start)
    if length_ is not None:
        args = args + (length_,)
    return f("substring", *args)


def replace_all(value: Any, pattern: Any, replacement: Any) -> FunctionCall:
    return f("replaceAll", value, pattern, replacement)


def regex_replace(value: Any, pattern: Any, replacement: Any) -> FunctionCall:
    return f("replaceRegexpAll", value, pattern, replacement)


def hash64(value: Any) -> FunctionCall:
    return f("cityHash64", value)


def round_(value: Any, n: int = 0) -> FunctionCall:
    return f("round", value, n)


def abs_(value: Any) -> FunctionCall:
    return f("abs", value)
