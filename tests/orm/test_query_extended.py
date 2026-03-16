"""Tests for Phase 1 query builder features."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest

from derp.orm import Table
from derp.orm.fields import Boolean, Field, Integer, Serial, Timestamp, Varchar
from derp.orm.query.builder import (
    InsertQuery,
    SelectQuery,
    UpdateQuery,
)
from derp.orm.query.expressions import sql


class User(Table, table="users"):
    id: int = Field(Serial(), primary_key=True)
    name: str = Field(Varchar(255))
    email: str = Field(Varchar(255), unique=True)
    age: int = Field(Integer(), nullable=True)
    created_at: datetime = Field(Timestamp(), default="now()")


class Post(Table, table="posts"):
    id: int = Field(Serial(), primary_key=True)
    user_id: int = Field(Integer())
    title: str = Field(Varchar(255))
    content: str = Field(Varchar(1000), nullable=True)
    published: bool = Field(Boolean(), default=False)


# =============================================================================
# sql() template tag
# =============================================================================


class TestSQLTemplate:
    def test_raw_sql_no_params(self):
        """sql() with no parameters produces raw SQL inline."""
        expr = sql("NOW()")
        params: list[Any] = []
        result = expr.to_sql(params)
        assert result == "NOW()"
        assert params == []

    def test_raw_sql_with_params(self):
        """sql() with parameters uses $N placeholders."""
        expr = sql("age > {}", 18)
        params: list[Any] = []
        result = expr.to_sql(params)
        assert result == "age > $1"
        assert params == [18]

    def test_raw_sql_multiple_params(self):
        """sql() with multiple parameters."""
        expr = sql("age > {} AND name = {}", 18, "Alice")
        params: list[Any] = []
        result = expr.to_sql(params)
        assert result == "age > $1 AND name = $2"
        assert params == [18, "Alice"]

    def test_raw_sql_param_offset(self):
        """sql() respects existing params in the list."""
        expr = sql("age > {}", 18)
        params: list[Any] = ["existing"]
        result = expr.to_sql(params)
        assert result == "age > $2"
        assert params == ["existing", 18]

    def test_raw_sql_in_where(self):
        """sql() usable in WHERE clause."""
        query = SelectQuery[User](None, (User,)).where(sql("age > {}", 18))
        s, params = query.build()
        assert "WHERE age > $1" in s
        assert params == [18]

    def test_raw_sql_in_select_columns(self):
        """sql() usable in SELECT column list."""
        query = SelectQuery[Any](None, (User.c.name, sql("COUNT(*)"))).from_(User)
        s, params = query.build()
        assert "SELECT users.name, COUNT(*)" in s

    def test_raw_sql_as_alias(self):
        """sql().as_() adds AS alias."""
        expr = sql("COUNT(*)").as_("total")
        params: list[Any] = []
        result = expr.to_sql(params)
        assert result == "COUNT(*) AS total"

    def test_raw_sql_in_update_set(self):
        """sql() usable in UPDATE SET values."""
        query = (
            UpdateQuery[User](None, User)
            .set(name=sql("UPPER(name)"))
            .where(User.c.id == 1)
        )
        s, params = query.build()
        assert "name = UPPER(name)" in s
        # Only the WHERE param
        assert params == [1]

    def test_raw_sql_in_update_set_with_params(self):
        """sql() with params in UPDATE SET."""
        query = (
            UpdateQuery[User](None, User)
            .set(name=sql("CONCAT({}, name)", "Dr. "))
            .where(User.c.id == 1)
        )
        s, params = query.build()
        assert "name = CONCAT($1, name)" in s
        assert params == ["Dr. ", 1]

    def test_raw_sql_is_expression(self):
        """RawSQL is a proper Expression subclass."""
        expr = sql("1 = 1")
        from derp.orm.query.expressions import Expression

        assert isinstance(expr, Expression)

    def test_raw_sql_combinable_with_and(self):
        """sql() can be combined with & operator."""
        expr = sql("age > {}", 18) & sql("name = {}", "Alice")
        params: list[Any] = []
        result = expr.to_sql(params)
        assert "AND" in result
        assert params == [18, "Alice"]


# =============================================================================
# DISTINCT / DISTINCT ON
# =============================================================================


class TestDistinct:
    def test_distinct(self):
        """SELECT DISTINCT."""
        query = SelectQuery[User](None, (User,)).distinct()
        s, params = query.build()
        assert s.startswith("SELECT DISTINCT users.*")

    def test_distinct_with_where(self):
        """SELECT DISTINCT with WHERE."""
        query = SelectQuery[User](None, (User,)).distinct().where(User.c.age > 18)
        s, params = query.build()
        assert "SELECT DISTINCT users.*" in s
        assert "WHERE" in s
        assert params == [18]

    def test_distinct_on_single_column(self):
        """SELECT DISTINCT ON (col)."""
        query = (
            SelectQuery[User](None, (User,))
            .distinct_on(User.c.email)
            .order_by(User.c.email)
        )
        s, params = query.build()
        assert "SELECT DISTINCT ON (users.email) users.*" in s

    def test_distinct_on_multiple_columns(self):
        """SELECT DISTINCT ON (col1, col2)."""
        query = SelectQuery[User](None, (User,)).distinct_on(User.c.email, User.c.name)
        s, params = query.build()
        assert "DISTINCT ON (users.email, users.name)" in s


# =============================================================================
# Row locking (FOR UPDATE / FOR SHARE)
# =============================================================================


class TestRowLocking:
    def test_for_update(self):
        """SELECT ... FOR UPDATE."""
        query = SelectQuery[User](None, (User,)).where(User.c.id == 1).for_update()
        s, params = query.build()
        assert s.endswith("FOR UPDATE")
        assert params == [1]

    def test_for_share(self):
        """SELECT ... FOR SHARE."""
        query = SelectQuery[User](None, (User,)).where(User.c.id == 1).for_share()
        s, params = query.build()
        assert s.endswith("FOR SHARE")

    def test_for_update_nowait(self):
        """SELECT ... FOR UPDATE NOWAIT."""
        query = (
            SelectQuery[User](None, (User,))
            .where(User.c.id == 1)
            .for_update(nowait=True)
        )
        s, params = query.build()
        assert s.endswith("FOR UPDATE NOWAIT")

    def test_for_update_skip_locked(self):
        """SELECT ... FOR UPDATE SKIP LOCKED."""
        query = (
            SelectQuery[User](None, (User,))
            .where(User.c.id == 1)
            .for_update(skip_locked=True)
        )
        s, params = query.build()
        assert s.endswith("FOR UPDATE SKIP LOCKED")

    def test_for_share_skip_locked(self):
        """SELECT ... FOR SHARE SKIP LOCKED."""
        query = (
            SelectQuery[User](None, (User,))
            .where(User.c.id == 1)
            .for_share(skip_locked=True)
        )
        s, params = query.build()
        assert s.endswith("FOR SHARE SKIP LOCKED")

    def test_lock_after_limit(self):
        """Lock clause comes after LIMIT/OFFSET."""
        query = (
            SelectQuery[User](None, (User,))
            .where(User.c.id == 1)
            .limit(10)
            .offset(5)
            .for_update()
        )
        s, params = query.build()
        assert "LIMIT 10" in s
        assert "OFFSET 5" in s
        assert s.endswith("FOR UPDATE")


# =============================================================================
# Upsert (ignore_conflicts / upsert)
# =============================================================================


class TestUpsert:
    def test_ignore_conflicts_single_target(self):
        """INSERT ... ON CONFLICT (col) DO NOTHING."""
        query = (
            InsertQuery[User](None, User)
            .values(name="Alice", email="alice@example.com")
            .ignore_conflicts(target=User.c.email)
        )
        s, params = query.build()
        assert "ON CONFLICT (email) DO NOTHING" in s
        assert params == ["Alice", "alice@example.com"]

    def test_ignore_conflicts_multiple_targets(self):
        """INSERT ... ON CONFLICT (col1, col2) DO NOTHING."""
        query = (
            InsertQuery[User](None, User)
            .values(name="Alice", email="alice@example.com")
            .ignore_conflicts(target=(User.c.email, User.c.name))
        )
        s, params = query.build()
        assert "ON CONFLICT (email, name) DO NOTHING" in s

    def test_upsert_single_target(self):
        """INSERT ... ON CONFLICT (col) DO UPDATE SET."""
        query = (
            InsertQuery[User](None, User)
            .values(name="Alice", email="alice@example.com")
            .upsert(target=User.c.email, name="Alice Updated")
        )
        s, params = query.build()
        assert "ON CONFLICT (email) DO UPDATE SET" in s
        assert "name = $3" in s
        assert params == ["Alice", "alice@example.com", "Alice Updated"]

    def test_upsert_multiple_set(self):
        """Upsert with multiple SET columns."""
        query = (
            InsertQuery[User](None, User)
            .values(name="Alice", email="alice@example.com", age=25)
            .upsert(target=User.c.email, name="Alice Updated", age=30)
        )
        s, params = query.build()
        assert "ON CONFLICT (email) DO UPDATE SET" in s
        assert "name =" in s
        assert "age =" in s
        assert params == ["Alice", "alice@example.com", 25, "Alice Updated", 30]

    def test_upsert_with_returning(self):
        """Upsert preserves state through returning()."""
        query = (
            InsertQuery[User](None, User)
            .values(name="Alice", email="alice@example.com")
            .upsert(target=User.c.email, name="Alice Updated")
            .returning(User)
        )
        s, params = query.build()
        assert "ON CONFLICT" in s
        assert "RETURNING *" in s

    def test_ignore_conflicts_with_returning(self):
        """ignore_conflicts preserves state through returning()."""
        query = (
            InsertQuery[User](None, User)
            .values(name="Alice", email="alice@example.com")
            .ignore_conflicts(target=User.c.email)
            .returning(User)
        )
        s, params = query.build()
        assert "ON CONFLICT (email) DO NOTHING" in s
        assert "RETURNING *" in s

    def test_upsert_before_values(self):
        """Upsert can be called before values()."""
        query = (
            InsertQuery[User](None, User)
            .upsert(target=User.c.email, name="Updated")
            .values(name="Alice", email="alice@example.com")
        )
        s, params = query.build()
        assert "ON CONFLICT (email) DO UPDATE SET" in s


# =============================================================================
# Multi-row insert
# =============================================================================


class TestMultiRowInsert:
    def test_values_list_basic(self):
        """INSERT with multiple rows."""
        query = InsertQuery[User](None, User).values_list(
            [
                {"name": "Alice", "email": "alice@example.com"},
                {"name": "Bob", "email": "bob@example.com"},
            ]
        )
        s, params = query.build()
        assert "INSERT INTO users (name, email)" in s
        assert "VALUES ($1, $2), ($3, $4)" in s
        assert params == ["Alice", "alice@example.com", "Bob", "bob@example.com"]

    def test_values_list_three_rows(self):
        """INSERT with three rows and correct param numbering."""
        query = InsertQuery[User](None, User).values_list(
            [
                {"name": "Alice", "email": "a@a.com"},
                {"name": "Bob", "email": "b@b.com"},
                {"name": "Carol", "email": "c@c.com"},
            ]
        )
        s, params = query.build()
        assert "($1, $2), ($3, $4), ($5, $6)" in s
        assert len(params) == 6

    def test_values_list_with_returning(self):
        """Multi-row insert with RETURNING."""
        query = (
            InsertQuery[User](None, User)
            .values_list(
                [
                    {"name": "Alice", "email": "a@a.com"},
                    {"name": "Bob", "email": "b@b.com"},
                ]
            )
            .returning(User)
        )
        s, params = query.build()
        assert "VALUES ($1, $2), ($3, $4)" in s
        assert "RETURNING *" in s

    def test_values_list_with_upsert(self):
        """Multi-row insert with upsert."""
        query = (
            InsertQuery[User](None, User)
            .values_list(
                [
                    {"name": "Alice", "email": "a@a.com"},
                    {"name": "Bob", "email": "b@b.com"},
                ]
            )
            .ignore_conflicts(target=User.c.email)
        )
        s, params = query.build()
        assert "VALUES ($1, $2), ($3, $4)" in s
        assert "ON CONFLICT (email) DO NOTHING" in s

    def test_values_list_empty_raises(self):
        """Empty values_list raises ValueError."""
        with pytest.raises(ValueError, match="at least one row"):
            InsertQuery[User](None, User).values_list([]).build()
