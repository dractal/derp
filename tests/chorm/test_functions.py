"""Tests for ClickHouse function helpers and expression operators."""

from __future__ import annotations

from derp.chorm import (
    Field,
    Float64,
    Int64,
    MergeTree,
    SelectQuery,
    String,
    Table,
    UInt64,
    cast,
    coalesce,
    concat,
    f,
    if_,
    ifnull,
    length,
    lit,
    lower,
    over,
    position,
    raw,
    regex_replace,
    replace_all,
    substring,
    to_date,
    to_datetime,
    to_start_of_day,
    to_start_of_hour,
    to_start_of_minute,
    to_start_of_month,
    to_yyyymm,
    to_yyyymmdd,
    upper,
)
from derp.chorm import (
    sql as raw_sql,
)
from derp.chorm.expression_base import Params
from derp.chorm.query.expressions import (
    AggregateFunc,
    Literal,
    SearchedCase,
    to_expr,
)
from derp.chorm.query.functions import (
    abs_,
    array,
    array_join,
    hash64,
    map_,
    round_,
    tuple_,
)


class Item(Table, table="items"):
    id: UInt64 = Field()
    name: String = Field()
    price: Float64 = Field()
    qty: Int64 = Field()
    __engine__ = MergeTree(order_by="id")


def _sql(expr):
    p = Params()
    return expr.to_sql(p), p.values


def test_f_dispatch_function():
    expr = f.lengthUTF8(Item.name)
    s, _ = _sql(expr)
    assert s == "lengthUTF8(`items`.`name`)"


def test_f_call_explicit():
    expr = f("anyFn", Item.name, 1)
    s, _ = _sql(expr)
    assert s.startswith("anyFn(")


def test_aggregate_dispatch_via_f():
    expr = f.sum(Item.qty)
    assert isinstance(expr, AggregateFunc)
    s, _ = _sql(expr)
    assert s == "sum(`items`.`qty`)"


def test_aggregate_with_params_kwarg():
    expr = f.quantile(Item.price, params=0.95)
    s, _ = _sql(expr)
    assert "quantile" in s
    assert "0.95" in s or "p1:Float64" in s


def test_lit_helper():
    expr = lit(42, "Int32")
    s, vals = _sql(expr)
    assert vals == {"p1": 42}
    # The explicit type rides inline in the placeholder.
    assert s == "{p1:Int32}"


def test_array_helper():
    expr = array(1, 2, 3)
    s, _ = _sql(expr)
    assert s.startswith("array(")


def test_tuple_helper():
    expr = tuple_(1, "x")
    s, _ = _sql(expr)
    assert s.startswith("tuple(")


def test_map_helper():
    expr = map_("k", 1, "k2", 2)
    s, _ = _sql(expr)
    assert s.startswith("map(")


def test_if_helper():
    expr = if_(Item.qty > 0, "yes", "no")
    s, _ = _sql(expr)
    assert s.startswith("if(")


def test_coalesce_helper():
    expr = coalesce(Item.name, "default")
    s, _ = _sql(expr)
    assert s.startswith("coalesce(")


def test_ifnull_helper():
    expr = ifnull(Item.name, "x")
    s, _ = _sql(expr)
    assert s.startswith("ifNull(")


def test_cast_helper():
    expr = cast(Item.qty, "Float64")
    s, vals = _sql(expr)
    assert "CAST(" in s
    assert "Float64" in vals.values()


def test_to_date():
    s, _ = _sql(to_date(Item.qty))
    assert s.startswith("toDate(")


def test_to_datetime():
    s, _ = _sql(to_datetime(Item.qty))
    assert s.startswith("toDateTime(")


def test_to_yyyymm_yyyymmdd():
    assert _sql(to_yyyymm(Item.qty))[0].startswith("toYYYYMM(")
    assert _sql(to_yyyymmdd(Item.qty))[0].startswith("toYYYYMMDD(")


def test_to_start_of_helpers():
    assert _sql(to_start_of_day(Item.qty))[0].startswith("toStartOfDay")
    assert _sql(to_start_of_hour(Item.qty))[0].startswith("toStartOfHour")
    assert _sql(to_start_of_minute(Item.qty))[0].startswith("toStartOfMinute")
    assert _sql(to_start_of_month(Item.qty))[0].startswith("toStartOfMonth")


def test_array_join_helper():
    s, _ = _sql(array_join(Item.name))
    assert s.startswith("arrayJoin(")


def test_string_helpers():
    assert _sql(length(Item.name))[0].startswith("length(")
    assert _sql(lower(Item.name))[0].startswith("lower(")
    assert _sql(upper(Item.name))[0].startswith("upper(")
    assert _sql(concat(Item.name, "x"))[0].startswith("concat(")
    assert _sql(position(Item.name, "x"))[0].startswith("position(")


def test_substring_with_and_without_length():
    assert "substring(" in _sql(substring(Item.name, 1))[0]
    assert "substring(" in _sql(substring(Item.name, 1, 5))[0]


def test_replace_all_and_regex_replace():
    assert _sql(replace_all(Item.name, "x", "y"))[0].startswith("replaceAll(")
    assert _sql(regex_replace(Item.name, "[a]", "z"))[0].startswith("replaceRegexpAll(")


def test_hash64():
    assert _sql(hash64(Item.name))[0].startswith("cityHash64(")


def test_round_and_abs():
    assert _sql(round_(Item.price, 2))[0].startswith("round(")
    assert _sql(abs_(Item.qty))[0].startswith("abs(")


def test_arithmetic_operators():
    expr = (Item.qty + 1) - 2 * Item.price / 3
    s, _ = _sql(expr)
    # Operators show up
    assert "+" in s and "-" in s and "*" in s and "/" in s


def test_radd_rsub_rmul():
    expr = 1 + Item.qty - 2 - 3 * Item.price
    s, _ = _sql(expr)
    assert s  # smoke


def test_searched_case():
    expr = SearchedCase(
        cases=[(Item.qty > 0, "pos"), (Item.qty < 0, "neg")],
        else_value="zero",
    )
    s, _ = _sql(expr)
    assert s.startswith("CASE WHEN")
    assert "ELSE" in s


def test_function_call_alias():
    expr = f.lower(Item.name).as_("l")
    s, _ = _sql(expr)
    assert s.endswith(" AS l")


def test_window_function_no_frame_no_order():
    wf = over(f.count(), partition_by=Item.id)
    s, _ = _sql(wf)
    assert "OVER (PARTITION BY" in s


def test_to_expr_passthrough():
    e = to_expr(Item.id)
    assert e is Item.id


def test_to_expr_wraps_value():
    e = to_expr(42)
    assert isinstance(e, Literal)


def test_raw_no_params():
    s, vals = _sql(raw("toDate(ts)"))
    assert s == "toDate(ts)"
    assert vals == {}


def test_raw_sql_with_alias():
    s, _ = _sql(raw_sql("toDate(ts)").as_("d"))
    assert s.endswith(" AS d")


def test_aggregate_func_aliasing_and_combinators_chain():
    expr = Item.qty.sum().if_(Item.qty > 0).or_null().state().as_("s")
    s, _ = _sql(expr)
    assert "sumIfOrNullState" in s
    assert s.endswith(" AS s")


def test_aggregate_func_merge_and_array():
    expr = Item.qty.uniq().merge().array()
    s, _ = _sql(expr)
    assert "uniqMergeArray" in s


def test_aggregate_or_default():
    expr = Item.qty.uniq().or_default()
    s, _ = _sql(expr)
    assert "uniqOrDefault" in s


def test_column_aggregates_in_select():
    """Smoke-test that all Column aggregate helpers build sane SQL."""
    queries = [
        Item.qty.count(),
        Item.qty.count_distinct(),
        Item.qty.sum(),
        Item.qty.avg(),
        Item.qty.min(),
        Item.qty.max(),
        Item.qty.any(),
        Item.qty.any_last(),
        Item.qty.uniq(),
        Item.qty.uniq_exact(),
        Item.qty.uniq_combined(),
        Item.qty.uniq_hll12(),
        Item.qty.group_array(),
        Item.qty.group_uniq_array(),
        Item.qty.quantile(0.5),
        Item.qty.quantiles(0.5, 0.95, 0.99),
        Item.qty.median(),
        Item.qty.stddev_pop(),
        Item.qty.stddev_samp(),
        Item.qty.var_pop(),
        Item.qty.var_samp(),
        Item.qty.argmin(Item.id),
        Item.qty.argmax(Item.id),
    ]
    for expr in queries:
        s, _ = _sql(expr)
        assert "(" in s and ")" in s


def test_column_case_method():
    expr = Item.qty.case({1: "a", 2: "b"}, else_="c")
    s, _ = _sql(expr)
    assert s.startswith("CASE")


def test_invert_column_eq_false():
    expr = ~Item.qty
    s, _ = _sql(expr)
    # Inverting a column yields col = false (treated as 0 in CH)
    assert "= " in s


def test_unary_not():
    expr = ~(Item.qty > 0)
    s, _ = _sql(expr)
    assert "(NOT " in s


def test_in_subquery_via_select():
    sub = SelectQuery(None, (Item.id,))
    cond = Item.id.in_(sub)
    s, _ = _sql(cond)
    assert "IN (SELECT" in s


def test_modulo_operator():
    expr = Item.qty % 7
    s, _ = _sql(expr)
    assert "%" in s


def test_string_in_select_kwarg():
    """Strings passed in projection survive as raw SQL fragments."""
    q = SelectQuery(None, ("toDate(ts) AS d", Item.id))
    sql, _ = q.build()
    assert "toDate(ts) AS d" in sql
