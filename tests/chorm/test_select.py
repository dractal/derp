"""Tests for the SELECT query builder."""

from __future__ import annotations

from derp.chorm import (
    Array,
    DateTime,
    Field,
    Fn,
    Int64,
    LowCardinality,
    MergeTree,
    Nullable,
    SelectQuery,
    String,
    Table,
    UInt64,
    f,
    over,
    raw,
)
from derp.chorm import (
    sql as raw_sql,
)


class Event(Table, table="events"):
    id: UInt64 = Field()
    user_id: UInt64 = Field()
    ts: DateTime = Field(default=Fn.now())
    type: LowCardinality[String] = Field()
    bio: Nullable[String] = Field()
    tags: Array[String] = Field()
    __engine__ = MergeTree(order_by=("user_id", "ts"))


class User(Table, table="users"):
    id: UInt64 = Field()
    name: String = Field()
    age: Int64 = Field()
    __engine__ = MergeTree(order_by="id")


def _build(q: SelectQuery) -> tuple[str, dict]:
    return q.build()


def test_basic_select_all():
    q = SelectQuery(None, (Event,))
    sql, _ = _build(q)
    assert "SELECT `events`.`id`" in sql
    assert "FROM events" in sql


def test_select_all_uses_from_alias():
    q = SelectQuery(None, (Event,)).from_(Event, alias="e")
    sql, _ = _build(q)
    assert "SELECT `e`.`id`" in sql
    assert "FROM events AS e" in sql


def test_join_condition_uses_join_alias():
    q = SelectQuery(None, (Event,)).left_join(
        User, alias="u", on=Event.user_id == User.id
    )
    sql, _ = _build(q)
    assert "LEFT JOIN users AS u" in sql
    assert "`events`.`user_id` = `u`.`id`" in sql


def test_database_qualified_column_reference():
    class DbEvent(Table, table="events", database="analytics"):
        id: UInt64 = Field()
        __engine__ = MergeTree(order_by="id")

    q = SelectQuery(None, (DbEvent.id,))
    sql, _ = _build(q)
    assert "SELECT `analytics`.`events`.`id`" in sql
    assert "FROM analytics.events" in sql


def test_select_specific_columns():
    q = SelectQuery(None, (Event.id, Event.user_id))
    sql, _ = _build(q)
    assert "SELECT `events`.`id`, `events`.`user_id`" in sql


def test_where_eq():
    q = SelectQuery(None, (Event,)).where(Event.user_id == 42)
    sql, vals = _build(q)
    assert "WHERE (`events`.`user_id` = {p1:Int64})" in sql  # type inline
    assert vals == {"p1": 42}


def test_where_chained_and():
    q = (
        SelectQuery(None, (Event,))
        .where(Event.user_id == 42)
        .where(Event.type == "click")
    )
    sql, _ = _build(q)
    # Chained .where() joins with AND
    assert " AND " in sql


def test_where_or_combinator():
    q = SelectQuery(None, (Event,)).where((Event.user_id == 42) | (Event.user_id == 99))
    sql, _ = _build(q)
    assert " OR " in sql


def test_where_in_list():
    q = SelectQuery(None, (Event,)).where(Event.user_id.in_([1, 2, 3]))
    sql, vals = _build(q)
    assert " IN ({p1:Int64}, {p2:Int64}, {p3:Int64})" in sql
    assert vals == {"p1": 1, "p2": 2, "p3": 3}


def test_where_global_in():
    q = SelectQuery(None, (Event,)).where(Event.user_id.global_in([1, 2]))
    sql, _ = _build(q)
    assert "GLOBAL IN" in sql


def test_where_not_in():
    q = SelectQuery(None, (Event,)).where(Event.id.not_in([1, 2]))
    sql, _ = _build(q)
    assert "NOT IN" in sql


def test_where_like():
    q = SelectQuery(None, (Event,)).where(Event.type.like("%click%"))
    sql, vals = _build(q)
    assert " LIKE " in sql
    assert vals == {"p1": "%click%"}


def test_where_ilike():
    q = SelectQuery(None, (Event,)).where(Event.type.ilike("%X%"))
    sql, _ = _build(q)
    assert " ILIKE " in sql


def test_where_is_null():
    q = SelectQuery(None, (Event,)).where(Event.bio.is_null())
    sql, _ = _build(q)
    assert "IS NULL" in sql


def test_where_between():
    q = SelectQuery(None, (Event,)).where(Event.id.between(1, 100))
    sql, vals = _build(q)
    assert "BETWEEN" in sql
    assert vals == {"p1": 1, "p2": 100}


def test_order_by_asc_desc():
    q = SelectQuery(None, (Event,)).order_by(Event.ts, desc=True).order_by(Event.id)
    sql, _ = _build(q)
    assert "ORDER BY `events`.`ts` DESC, `events`.`id` ASC" in sql


def test_order_by_accepts_asc_kwarg_like_orm():
    # `asc=` mirrors the postgres ORM; asc=False is equivalent to desc=True.
    q = SelectQuery(None, (Event,)).order_by(Event.ts, asc=False).order_by(Event.id)
    sql, _ = _build(q)
    assert "ORDER BY `events`.`ts` DESC, `events`.`id` ASC" in sql


def test_order_by_nulls_collate():
    q = SelectQuery(None, (Event,)).order_by(Event.bio, nulls="LAST", collate="en")
    sql, _ = _build(q)
    assert "NULLS LAST" in sql
    assert "COLLATE 'en'" in sql


def test_order_by_with_fill():
    q = SelectQuery(None, (Event,)).order_by(
        Event.ts, with_fill=True, fill_step=raw("INTERVAL 1 DAY")
    )
    sql, _ = _build(q)
    assert "WITH FILL" in sql
    assert "STEP INTERVAL 1 DAY" in sql


def test_limit_offset():
    q = SelectQuery(None, (Event,)).limit(10).offset(20)
    sql, _ = _build(q)
    assert sql.endswith("LIMIT 20, 10")


def test_offset_only():
    q = SelectQuery(None, (Event,)).offset(5)
    sql, _ = _build(q)
    assert "OFFSET 5" in sql


def test_limit_by():
    q = SelectQuery(None, (Event,)).limit_by(3, Event.user_id)
    sql, _ = _build(q)
    assert "LIMIT 3 BY `events`.`user_id`" in sql


def test_distinct():
    q = SelectQuery(None, (Event.user_id,)).distinct()
    sql, _ = _build(q)
    assert "SELECT DISTINCT" in sql


def test_distinct_on():
    q = SelectQuery(None, (Event,)).distinct(Event.user_id)
    sql, _ = _build(q)
    assert "DISTINCT ON (" in sql


def test_group_by_having():
    q = (
        SelectQuery(None, (Event.user_id, Event.id.count().as_("n")))
        .group_by(Event.user_id)
        .having(Event.id.count() > 5)
    )
    sql, _ = _build(q)
    assert "GROUP BY `events`.`user_id`" in sql
    assert "HAVING" in sql


def test_group_by_with_rollup():
    q = SelectQuery(None, (Event.user_id,)).group_by(Event.user_id).with_rollup()
    sql, _ = _build(q)
    assert "WITH ROLLUP" in sql


def test_group_by_with_cube():
    q = SelectQuery(None, (Event.user_id,)).group_by(Event.user_id).with_cube()
    sql, _ = _build(q)
    assert "WITH CUBE" in sql


def test_group_by_with_totals():
    q = SelectQuery(None, (Event.user_id,)).group_by(Event.user_id).with_totals()
    sql, _ = _build(q)
    assert "WITH TOTALS" in sql


def test_inner_join_on():
    q = SelectQuery(None, (Event,)).inner_join(User, on=Event.user_id == User.id)
    sql, _ = _build(q)
    assert "INNER JOIN users" in sql
    assert "ON (" in sql


def test_left_join_using():
    q = SelectQuery(None, (Event,)).left_join(User, using="id")
    sql, _ = _build(q)
    assert "LEFT JOIN users USING (id)" in sql


def test_asof_join():
    q = SelectQuery(None, (Event,)).asof_join(User, on=Event.user_id == User.id)
    sql, _ = _build(q)
    assert "ASOF" in sql


def test_global_join():
    q = SelectQuery(None, (Event,)).left_join(
        User, on=Event.user_id == User.id, global_=True
    )
    sql, _ = _build(q)
    assert "GLOBAL LEFT JOIN" in sql


def test_final_modifier():
    q = SelectQuery(None, (Event,)).final()
    sql, _ = _build(q)
    assert "FROM events FINAL" in sql


def test_sample_clause():
    q = SelectQuery(None, (Event,)).sample(0.1)
    sql, _ = _build(q)
    assert "SAMPLE 0.1" in sql


def test_sample_with_offset():
    q = SelectQuery(None, (Event,)).sample(0.1, 0.2)
    sql, _ = _build(q)
    assert "SAMPLE 0.1 OFFSET 0.2" in sql


def test_prewhere():
    q = (
        SelectQuery(None, (Event,))
        .prewhere(Event.user_id == 1)
        .where(Event.type == "click")
    )
    sql, _ = _build(q)
    assert "PREWHERE" in sql
    assert "WHERE" in sql


def test_array_join():
    q = SelectQuery(None, (Event.id, Event.tags)).array_join(Event.tags)
    sql, _ = _build(q)
    assert "ARRAY JOIN" in sql


def test_left_array_join():
    q = SelectQuery(None, (Event.id,)).array_join(Event.tags, left=True)
    sql, _ = _build(q)
    assert "LEFT ARRAY JOIN" in sql


def test_settings():
    q = SelectQuery(None, (Event,)).settings(max_threads=4, max_memory_usage=10_000_000)
    sql, _ = _build(q)
    assert "SETTINGS max_threads = 4" in sql
    assert "max_memory_usage = 10000000" in sql


def test_format():
    q = SelectQuery(None, (Event,)).format("JSONEachRow")
    sql, _ = _build(q)
    assert sql.endswith(" FORMAT JSONEachRow")


def test_with_cte():
    sub = SelectQuery(None, (Event.id,)).where(Event.user_id == 1)
    q = SelectQuery(None, (Event,)).with_cte("active_ids", sub).from_("active_ids")
    sql, _ = _build(q)
    assert sql.startswith("WITH active_ids AS (")
    assert "FROM active_ids" in sql


def test_union_all():
    a = SelectQuery(None, (Event.id,))
    b = SelectQuery(None, (Event.id,)).where(Event.user_id == 1)
    combined = a.union_all(b).order_by(Event.id).limit(10)
    sql, _ = combined.build()
    assert "UNION ALL" in sql
    assert "ORDER BY" in sql
    assert "LIMIT 10" in sql


def test_intersect_except():
    a = SelectQuery(None, (Event.id,))
    b = SelectQuery(None, (Event.id,)).where(Event.user_id == 1)
    assert "INTERSECT" in a.intersect(b).build()[0]
    assert "EXCEPT" in a.except_(b).build()[0]


def test_subquery_as_expression():
    sub = SelectQuery(None, (Event.user_id,)).where(Event.type == "click")
    q = SelectQuery(None, (User,)).where(User.id.in_(sub))
    sql, vals = _build(q)
    assert "IN (SELECT" in sql
    # parameters from subquery are pulled into the outer params dict
    assert vals == {"p1": "click"}


def test_subquery_in_from():
    sub = SelectQuery(None, (Event,)).where(Event.user_id == 1).as_("sub")
    q = SelectQuery(None, (Event.id,)).from_(sub)
    sql, _ = _build(q)
    assert "FROM (" in sql
    assert ") AS sub" in sql


def test_aggregate_function_combinators():
    expr = Event.id.sum().if_(Event.type == "click")
    q = SelectQuery(None, (expr.as_("clicks"),)).from_(Event)
    sql, _ = _build(q)
    assert "sumIf" in sql


def test_aggregate_or_null():
    expr = Event.id.uniq().or_null().as_("u")
    sql, _ = SelectQuery(None, (expr,)).from_(Event).build()
    assert "uniqOrNull" in sql


def test_aggregate_state_merge():
    expr = Event.id.uniq().state().as_("s")
    sql, _ = SelectQuery(None, (expr,)).from_(Event).build()
    assert "uniqState" in sql


def test_aggregate_quantile_with_params():
    expr = Event.id.quantile(0.95).as_("p95")
    sql, vals = SelectQuery(None, (expr,)).from_(Event).build()
    assert "quantile(" in sql
    # 0.95 is rendered as a function param, not as `:type` parameter
    assert "0.95" in sql or any(v == 0.95 for v in vals.values())


def test_window_function():
    wf = over(
        f.row_number(),
        partition_by=Event.user_id,
        order_by=(Event.ts, "DESC"),
    )
    q = SelectQuery(None, (Event.id, wf.as_("rn"))).from_(Event)
    sql, _ = _build(q)
    assert "row_number() OVER" in sql
    assert "PARTITION BY" in sql
    assert "ORDER BY" in sql
    assert "DESC" in sql


def test_window_function_with_frame():
    wf = over(
        f.sum(Event.id),
        order_by=Event.ts,
        frame="ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
    )
    q = SelectQuery(None, (wf.as_("running"),)).from_(Event)
    sql, _ = _build(q)
    assert "ROWS BETWEEN" in sql


def test_raw_sql_in_select():
    q = SelectQuery(None, (raw_sql("toDate(ts)").as_("d"), Event.id)).from_(Event)
    sql, _ = _build(q)
    assert "toDate(ts) AS d" in sql


def test_function_dispatch_via_f():
    expr = f.lengthUTF8(Event.type)
    q = SelectQuery(None, (expr.as_("l"),)).from_(Event)
    sql, _ = _build(q)
    assert "lengthUTF8(" in sql


def test_arithmetic_expressions():
    expr = (Event.id + 1) * 2
    q = SelectQuery(None, (expr.as_("calc"),)).from_(Event)
    sql, vals = _build(q)
    assert "+" in sql and "*" in sql
    assert 1 in vals.values() and 2 in vals.values()


def test_case_expression():
    expr = Event.type.case({"click": 1, "view": 2}, else_=0).as_("kind")
    q = SelectQuery(None, (expr,)).from_(Event)
    sql, vals = _build(q)
    assert "CASE" in sql
    assert "ELSE" in sql
    assert "click" in vals.values()


def test_count_helper():
    # Single column .count() aggregate
    expr = Event.id.count_distinct().as_("u")
    sql, _ = SelectQuery(None, (expr,)).from_(Event).build()
    assert "count(DISTINCT" in sql


def test_settings_with_string():
    q = SelectQuery(None, (Event,)).settings(date_time_input_format="best_effort")
    sql, _ = _build(q)
    assert "'best_effort'" in sql


def test_match_function():
    q = SelectQuery(None, (Event,)).where(Event.type.match("^cl.*"))
    sql, _ = _build(q)
    assert "match(" in sql
