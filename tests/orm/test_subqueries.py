"""Tests for subqueries, set operations, EXISTS, CTEs, INSERT...SELECT."""

from __future__ import annotations

from typing import Any

from derp.orm import (
    Boolean,
    Field,
    Integer,
    Nullable,
    Serial,
    Table,
    Timestamp,
    Varchar,
)
from derp.orm.query.builder import InsertQuery, SelectQuery
from derp.orm.query.expressions import _renumber_params, sql


class User(Table, table="users"):
    id: Serial = Field(primary=True)
    name: Varchar[255] = Field()
    email: Varchar[255] = Field(unique=True)
    age: Nullable[Integer] = Field()
    role: Varchar[50] = Field()
    created_at: Timestamp = Field(default="now()")


class Post(Table, table="posts"):
    id: Serial = Field(primary=True)
    user_id: Integer = Field()
    title: Varchar[255] = Field()
    published: Boolean = Field(default=False)


class ArchivedUser(Table, table="archived_users"):
    id: Serial = Field(primary=True)
    name: Varchar[255] = Field()
    email: Varchar[255] = Field()


# =============================================================================
# _renumber_params utility
# =============================================================================


class TestRenumberParams:
    def test_no_offset(self):
        """Zero offset returns unchanged SQL."""
        assert _renumber_params("SELECT * WHERE id = $1", 0) == (
            "SELECT * WHERE id = $1"
        )

    def test_single_param(self):
        """Single param gets shifted."""
        assert _renumber_params("id = $1", 3) == "id = $4"

    def test_multiple_params(self):
        """Multiple params all get shifted."""
        result = _renumber_params("a = $1 AND b = $2 AND c = $3", 5)
        assert result == "a = $6 AND b = $7 AND c = $8"

    def test_double_digit_params(self):
        """Double-digit param numbers work correctly."""
        result = _renumber_params("x = $10 AND y = $11", 2)
        assert result == "x = $12 AND y = $13"

    def test_no_params(self):
        """SQL with no params is unchanged."""
        assert _renumber_params("SELECT 1", 5) == "SELECT 1"


# =============================================================================
# Subqueries in WHERE (IN)
# =============================================================================


class TestSubqueryInWhere:
    def test_in_subquery(self):
        """column.in_(subquery) produces IN (SELECT ...)."""
        sub = SelectQuery[Any](None, (User.id,)).from_(User).where(User.age > 30)
        query = SelectQuery[Post](None, (Post,)).where(Post.user_id.in_(sub))
        s, params = query.build()
        assert "IN (SELECT users.id FROM users WHERE (users.age > $1))" in s
        assert params == [30]

    def test_not_in_subquery(self):
        """column.not_in(subquery) produces NOT IN (SELECT ...)."""
        sub = (
            SelectQuery[Any](None, (User.id,)).from_(User).where(User.role == "banned")
        )
        query = SelectQuery[Post](None, (Post,)).where(Post.user_id.not_in(sub))
        s, params = query.build()
        assert "NOT IN (SELECT" in s
        assert params == ["banned"]

    def test_in_subquery_with_outer_params(self):
        """Subquery params are renumbered after outer params."""
        sub = SelectQuery[Any](None, (User.id,)).from_(User).where(User.age > 30)
        query = (
            SelectQuery[Post](None, (Post,))
            .where(Post.published)
            .where(Post.user_id.in_(sub))
        )
        s, params = query.build()
        assert "posts.published" in s
        assert "users.age > $1" in s
        assert params == [30]


# =============================================================================
# Subqueries in SELECT (scalar subquery)
# =============================================================================


class TestSubqueryInSelect:
    def test_scalar_subquery_in_select(self):
        """Subquery .as_() usable as a SELECT column."""
        sub = (
            SelectQuery[Any](None, (Post.id.count(),))
            .from_(Post)
            .where(Post.user_id == User.id)
        )
        query = SelectQuery[Any](None, (User.name, sub.as_("post_count"))).from_(User)
        s, params = query.build()
        assert "users.name" in s
        assert "(SELECT COUNT(posts.id) FROM posts" in s
        assert "AS post_count" in s


# =============================================================================
# Subqueries in FROM
# =============================================================================


class TestSubqueryInFrom:
    def test_from_subquery(self):
        """from_() accepts a subquery."""
        sub = (
            SelectQuery[Any](None, (User.role, User.id.count().as_("cnt")))
            .from_(User)
            .group_by(User.role)
        )
        query = SelectQuery[Any](None, (sql("*"),)).from_(sub.as_("stats"))
        s, params = query.build()
        assert "FROM (SELECT" in s
        assert "AS stats" in s


# =============================================================================
# EXISTS operator
# =============================================================================


class TestExists:
    def test_exists(self):
        """subquery.exists() produces EXISTS (SELECT ...)."""
        sub = SelectQuery[Any](None, (Post,)).where(Post.user_id == User.id)
        query = SelectQuery[User](None, (User,)).where(sub.exists())
        s, params = query.build()
        assert "WHERE EXISTS (SELECT" in s

    def test_not_exists(self):
        """~subquery.exists() produces NOT EXISTS (SELECT ...)."""
        sub = SelectQuery[Any](None, (Post,)).where(Post.user_id == User.id)
        query = SelectQuery[User](None, (User,)).where(~sub.exists())
        s, params = query.build()
        assert "NOT EXISTS (SELECT" in s

    def test_exists_with_outer_where(self):
        """EXISTS combined with other WHERE conditions."""
        sub = (
            SelectQuery[Any](None, (Post,))
            .where(Post.user_id == User.id)
            .where(Post.published)
        )
        query = (
            SelectQuery[User](None, (User,)).where(User.age > 18).where(sub.exists())
        )
        s, params = query.build()
        assert "(users.age > $1)" in s
        assert "EXISTS (SELECT" in s
        assert "posts.published" in s
        assert params == [18]


# =============================================================================
# Set operations (UNION, INTERSECT, EXCEPT)
# =============================================================================


class TestSetOperations:
    def test_union(self):
        """q1.union(q2) produces UNION."""
        q1 = SelectQuery[User](None, (User,)).where(User.age > 30)
        q2 = SelectQuery[User](None, (User,)).where(User.role == "admin")
        s, params = q1.union(q2).build()
        assert "UNION" in s
        assert "UNION ALL" not in s
        assert params == [30, "admin"]

    def test_union_all(self):
        """q1.union_all(q2) produces UNION ALL."""
        q1 = SelectQuery[User](None, (User,)).where(User.age > 30)
        q2 = SelectQuery[User](None, (User,)).where(User.role == "admin")
        s, params = q1.union_all(q2).build()
        assert "UNION ALL" in s
        assert params == [30, "admin"]

    def test_intersect(self):
        """q1.intersect(q2) produces INTERSECT."""
        q1 = SelectQuery[User](None, (User,)).where(User.age > 30)
        q2 = SelectQuery[User](None, (User,)).where(User.role == "admin")
        s, params = q1.intersect(q2).build()
        assert "INTERSECT" in s

    def test_except(self):
        """q1.except_(q2) produces EXCEPT."""
        q1 = SelectQuery[User](None, (User,)).where(User.age > 30)
        q2 = SelectQuery[User](None, (User,)).where(User.role == "admin")
        s, params = q1.except_(q2).build()
        assert "EXCEPT" in s

    def test_union_param_renumbering(self):
        """Right-side params are renumbered correctly."""
        q1 = SelectQuery[User](None, (User,)).where(
            (User.age > 18) & (User.name == "Alice")
        )
        q2 = SelectQuery[User](None, (User,)).where(
            (User.age < 65) & (User.name == "Bob")
        )
        s, params = q1.union(q2).build()
        # Left uses $1, $2; right should use $3, $4
        assert "$1" in s
        assert "$2" in s
        assert "$3" in s
        assert "$4" in s
        assert params == [18, "Alice", 65, "Bob"]

    def test_set_operation_with_order_by(self):
        """Set operation supports ORDER BY on the result."""
        q1 = SelectQuery[User](None, (User,)).where(User.age > 30)
        q2 = SelectQuery[User](None, (User,)).where(User.role == "admin")
        s, params = q1.union(q2).order_by("name").limit(10).build()
        assert "UNION" in s
        assert "ORDER BY name ASC" in s
        assert "LIMIT 10" in s


# =============================================================================
# CTEs (WITH clause)
# =============================================================================


class TestCTEs:
    def test_basic_cte(self):
        """with_cte() produces WITH ... AS (SELECT ...)."""
        cte = SelectQuery[Any](None, (User,)).where(User.age > 18)
        query = (
            SelectQuery[Any](None, (sql("*"),))
            .from_("active_users")
            .with_cte("active_users", cte)
        )
        s, params = query.build()
        assert s.startswith("WITH active_users AS (")
        assert "FROM active_users" in s
        assert params == [18]

    def test_multiple_ctes(self):
        """Multiple with_cte() calls produce multiple CTEs."""
        cte1 = SelectQuery[Any](None, (User,)).where(User.age > 18)
        cte2 = SelectQuery[Any](None, (Post,)).where(Post.published)
        query = (
            SelectQuery[Any](None, (sql("*"),))
            .from_("active_users")
            .with_cte("active_users", cte1)
            .with_cte("published_posts", cte2)
        )
        s, params = query.build()
        assert "WITH active_users AS (" in s
        assert "published_posts AS (" in s
        assert params == [18]

    def test_cte_param_renumbering(self):
        """CTE params are renumbered before main query params."""
        cte = SelectQuery[Any](None, (User,)).where(User.age > 18)
        query = (
            SelectQuery[Any](None, (sql("*"),))
            .from_("active_users")
            .with_cte("active_users", cte)
            .where(sql("name = {}", "Alice"))
        )
        s, params = query.build()
        assert "age > $1" in s
        assert "name = $2" in s
        assert params == [18, "Alice"]


# =============================================================================
# INSERT ... SELECT
# =============================================================================


class TestInsertSelect:
    def test_insert_from_select(self):
        """INSERT INTO ... SELECT ... FROM ..."""
        sub = (
            SelectQuery[Any](None, (User.name, User.email))
            .from_(User)
            .where(User.age > 65)
        )
        query = (
            InsertQuery[ArchivedUser](None, ArchivedUser)
            .columns("name", "email")
            .from_select(sub)
        )
        s, params = query.build()
        assert "INSERT INTO archived_users (name, email)" in s
        assert "SELECT users.name, users.email FROM users" in s
        assert "WHERE (users.age > $1)" in s
        assert params == [65]

    def test_insert_from_select_with_returning(self):
        """INSERT ... SELECT ... RETURNING."""
        sub = (
            SelectQuery[Any](None, (User.name, User.email))
            .from_(User)
            .where(User.role == "inactive")
        )
        query = (
            InsertQuery[ArchivedUser](None, ArchivedUser)
            .columns("name", "email")
            .from_select(sub)
            .returning(ArchivedUser)
        )
        s, params = query.build()
        assert "INSERT INTO archived_users" in s
        assert "SELECT" in s
        assert "RETURNING *" in s
        assert params == ["inactive"]
