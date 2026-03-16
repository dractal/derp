"""Tests for aggregate functions, HAVING, and CASE/WHEN expressions."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from derp.orm import Table
from derp.orm.fields import (
    Boolean,
    Field,
    Integer,
    Serial,
    Timestamp,
    Varchar,
)
from derp.orm.query.builder import SelectQuery


class User(Table, table="users"):
    id: int = Field(Serial(), primary_key=True)
    name: str = Field(Varchar(255))
    email: str = Field(Varchar(255), unique=True)
    age: int = Field(Integer(), nullable=True)
    role: str = Field(Varchar(50))
    created_at: datetime = Field(Timestamp(), default="now()")


class Post(Table, table="posts"):
    id: int = Field(Serial(), primary_key=True)
    user_id: int = Field(Integer())
    title: str = Field(Varchar(255))
    published: bool = Field(Boolean(), default=False)


# =============================================================================
# Aggregate methods on FieldInfo
# =============================================================================


class TestAggregates:
    def test_count(self):
        """FieldInfo.count() produces COUNT(col)."""
        expr = User.c.id.count()
        params: list[Any] = []
        result = expr.to_sql(params)
        assert result == "COUNT(users.id)"
        assert params == []

    def test_sum(self):
        """FieldInfo.sum() produces SUM(col)."""
        expr = User.c.age.sum()
        params: list[Any] = []
        result = expr.to_sql(params)
        assert result == "SUM(users.age)"

    def test_avg(self):
        """FieldInfo.avg() produces AVG(col)."""
        expr = User.c.age.avg()
        params: list[Any] = []
        result = expr.to_sql(params)
        assert result == "AVG(users.age)"

    def test_min(self):
        """FieldInfo.min() produces MIN(col)."""
        expr = User.c.age.min()
        params: list[Any] = []
        result = expr.to_sql(params)
        assert result == "MIN(users.age)"

    def test_max(self):
        """FieldInfo.max() produces MAX(col)."""
        expr = User.c.age.max()
        params: list[Any] = []
        result = expr.to_sql(params)
        assert result == "MAX(users.age)"

    def test_aggregate_with_alias(self):
        """Aggregate .as_() adds AS alias."""
        expr = User.c.id.count().as_("user_count")
        params: list[Any] = []
        result = expr.to_sql(params)
        assert result == "COUNT(users.id) AS user_count"

    def test_aggregate_in_select(self):
        """Aggregate usable as a SELECT column."""
        query = (
            SelectQuery[Any](None, (User.c.role, User.c.id.count().as_("cnt")))
            .from_(User)
            .group_by(User.c.role)
        )
        s, params = query.build()
        assert "SELECT users.role, COUNT(users.id) AS cnt" in s
        assert "GROUP BY users.role" in s

    def test_aggregate_comparison(self):
        """Aggregates support comparison operators for HAVING."""
        expr = User.c.id.count() > 5
        params: list[Any] = []
        result = expr.to_sql(params)
        assert "(COUNT(users.id) > $1)" in result
        assert params == [5]

    def test_sum_with_alias_in_select(self):
        """SUM with alias in a SELECT query."""
        query = (
            SelectQuery[Any](None, (User.c.role, User.c.age.sum().as_("total_age")))
            .from_(User)
            .group_by(User.c.role)
        )
        s, params = query.build()
        assert "SUM(users.age) AS total_age" in s


# =============================================================================
# HAVING clause
# =============================================================================


class TestHaving:
    def test_having_basic(self):
        """HAVING clause with aggregate comparison."""
        query = (
            SelectQuery[Any](None, (User.c.role, User.c.id.count()))
            .from_(User)
            .group_by(User.c.role)
            .having(User.c.id.count() > 5)
        )
        s, params = query.build()
        assert "GROUP BY users.role" in s
        assert "HAVING (COUNT(users.id) > $1)" in s
        assert params == [5]

    def test_having_multiple(self):
        """Multiple .having() calls combine with AND."""
        query = (
            SelectQuery[Any](None, (User.c.role,))
            .from_(User)
            .group_by(User.c.role)
            .having(User.c.id.count() > 2)
            .having(User.c.age.avg() < 50)
        )
        s, params = query.build()
        assert "HAVING" in s
        assert "AND" in s
        assert params == [2, 50]

    def test_having_with_where(self):
        """HAVING comes after GROUP BY, separate from WHERE."""
        query = (
            SelectQuery[Any](None, (User.c.role, User.c.id.count()))
            .from_(User)
            .where(User.c.age > 18)
            .group_by(User.c.role)
            .having(User.c.id.count() >= 3)
        )
        s, params = query.build()
        # WHERE uses $1, HAVING uses $2 and $3
        assert "WHERE (users.age > $1)" in s
        assert "GROUP BY users.role" in s
        assert "HAVING (COUNT(users.id) >= $2)" in s
        assert params == [18, 3]

    def test_having_before_order_by(self):
        """HAVING appears between GROUP BY and ORDER BY."""
        query = (
            SelectQuery[Any](None, (User.c.role,))
            .from_(User)
            .group_by(User.c.role)
            .having(User.c.id.count() > 1)
            .order_by(User.c.role)
        )
        s, params = query.build()
        group_pos = s.index("GROUP BY")
        having_pos = s.index("HAVING")
        order_pos = s.index("ORDER BY")
        assert group_pos < having_pos < order_pos


# =============================================================================
# CASE/WHEN expressions
# =============================================================================


class TestCase:
    def test_simple_case(self):
        """FieldInfo.case() for simple CASE with mapping."""
        expr = User.c.role.case({"admin": 1, "user": 0}, else_=-1)
        params: list[Any] = []
        result = expr.to_sql(params)
        assert "CASE users.role" in result
        assert "WHEN $1 THEN $2" in result
        assert "ELSE" in result
        assert "END" in result
        assert params == ["admin", 1, "user", 0, -1]

    def test_simple_case_no_else(self):
        """CASE without ELSE."""
        expr = User.c.role.case({"admin": 1, "user": 0})
        params: list[Any] = []
        result = expr.to_sql(params)
        assert "CASE users.role" in result
        assert "ELSE" not in result
        assert "END" in result

    def test_case_with_alias(self):
        """CASE with .as_() alias."""
        expr = User.c.role.case({"admin": 1}, else_=0).as_("role_num")
        params: list[Any] = []
        result = expr.to_sql(params)
        assert result.endswith("END AS role_num")

    def test_case_in_select(self):
        """CASE usable as a SELECT column."""
        query = SelectQuery[Any](
            None,
            (
                User.c.name,
                User.c.role.case({"admin": "Admin"}, else_="Member"),
            ),
        ).from_(User)
        s, params = query.build()
        assert "CASE users.role" in s
        assert "FROM users" in s

    def test_case_comparison_operators(self):
        """CASE expression supports comparison operators."""
        expr = User.c.role.case({"admin": 1}, else_=0) > 0
        params: list[Any] = []
        result = expr.to_sql(params)
        assert "CASE users.role" in result
        assert "> $" in result
