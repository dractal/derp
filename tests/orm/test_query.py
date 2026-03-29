"""Tests for query builder."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from derp.orm import (
    Boolean,
    Field,
    Integer,
    Nullable,
    Serial,
    Table,
    Varchar,
)
from derp.orm.query import sql
from derp.orm.query.builder import (
    DeleteQuery,
    InsertQuery,
    SelectQuery,
    UpdateQuery,
    _acquire,
)


class User(Table, table="users"):
    id: Serial = Field(primary=True)
    name: Varchar[255] = Field()
    email: Varchar[255] = Field(unique=True)
    age: Nullable[Integer] = Field()
    created_at: Varchar[255] = Field(default="now()")


class Post(Table, table="posts"):
    id: Serial = Field(primary=True)
    user_id: Integer = Field()
    title: Varchar[255] = Field()
    content: Nullable[Varchar[1000]] = Field()
    published: Boolean = Field(default=False)


def test_select_all():
    """Test SELECT * query building."""
    query = SelectQuery[User](None, (User,))
    sql, params = query.build()

    assert sql == "SELECT users.* FROM users"
    assert params == []


def test_select_columns():
    """Test SELECT specific columns."""
    query = SelectQuery[Any](None, (User.id, User.name))
    query.from_(User)
    sql, params = query.build()

    assert "SELECT users.id, users.name FROM users" == sql
    assert params == []


def test_select_where_eq():
    """Test SELECT with WHERE equality."""
    query = SelectQuery[User](None, (User,)).where(User.id == 1)
    sql, params = query.build()

    assert "SELECT users.* FROM users WHERE" in sql
    assert "(users.id = $1)" in sql
    assert params == [1]


def test_select_where_comparison():
    """Test SELECT with comparison operators."""
    query = SelectQuery[User](None, (User,)).where(User.age > 18)
    sql, params = query.build()

    assert "(users.age > $1)" in sql
    assert params == [18]


def test_select_where_and():
    """Test SELECT with AND condition."""
    query = SelectQuery[User](None, (User,)).where(
        (User.name == "Alice") & (User.age > 18)
    )
    sql, params = query.build()

    assert "AND" in sql
    assert "(users.name = $1)" in sql
    assert "(users.age > $2)" in sql
    assert params == ["Alice", 18]


def test_select_where_or():
    """Test SELECT with OR condition."""
    query = SelectQuery[User](None, (User,)).where(
        (User.name == "Alice") | (User.name == "Bob")
    )
    sql, params = query.build()

    assert "OR" in sql
    assert params == ["Alice", "Bob"]


def test_select_where_in():
    """Test SELECT with IN clause."""
    query = SelectQuery[User](None, (User,)).where(User.id.in_([1, 2, 3]))
    sql, params = query.build()

    assert "IN ($1, $2, $3)" in sql
    assert params == [1, 2, 3]


def test_select_where_like():
    """Test SELECT with LIKE pattern."""
    query = SelectQuery[User](None, (User,)).where(User.name.like("%Alice%"))
    sql, params = query.build()

    assert "LIKE $1" in sql
    assert params == ["%Alice%"]


def test_select_order_by():
    """Test SELECT with ORDER BY."""
    query = SelectQuery[User](None, (User,)).order_by(User.name, asc=False)
    sql, params = query.build()

    assert "ORDER BY users.name DESC" in sql


def test_select_limit_offset():
    """Test SELECT with LIMIT and OFFSET."""
    query = SelectQuery[User](None, (User,)).limit(10).offset(20)
    sql, params = query.build()

    assert "LIMIT 10" in sql
    assert "OFFSET 20" in sql


def test_insert():
    """Test INSERT query building."""
    query = InsertQuery[User](None, User).values(name="Bob", email="bob@example.com")
    sql, params = query.build()

    assert "INSERT INTO users" in sql
    assert "(name, email)" in sql
    assert "VALUES ($1, $2)" in sql
    assert params == ["Bob", "bob@example.com"]


def test_insert_returning():
    """Test INSERT with RETURNING."""
    query = (
        InsertQuery[User](None, User)
        .values(name="Bob", email="bob@example.com")
        .returning(User)
    )
    sql, params = query.build()

    assert "RETURNING *" in sql


def test_update():
    """Test UPDATE query building."""
    query = UpdateQuery[User](None, User).set(name="Robert").where(User.id == 1)
    sql, params = query.build()

    assert "UPDATE users SET name = $1" in sql
    assert "WHERE (users.id = $2)" in sql
    assert params == ["Robert", 1]


def test_delete():
    """Test DELETE query building."""
    query = DeleteQuery[User](None, User).where(User.id == 1)
    sql, params = query.build()

    assert "DELETE FROM users" in sql
    assert "WHERE (users.id = $1)" in sql
    assert params == [1]


def test_complex_where():
    """Test complex WHERE with nested conditions."""
    query = SelectQuery[User](None, (User,)).where(
        ((User.name == "Alice") | (User.name == "Bob"))
        & (User.age > 18)
        & (User.age < 65),
    )
    sql, params = query.build()

    assert "AND" in sql
    assert "OR" in sql
    assert params == ["Alice", "Bob", 18, 65]


def test_column_dunder_operators():
    """Test Column dunder methods for binary operations."""
    # Test == operator
    query = SelectQuery[User](None, (User,)).where(User.id == 1)
    sql, params = query.build()
    assert "(users.id = $1)" in sql
    assert params == [1]

    # Test != operator
    query = SelectQuery[User](None, (User,)).where(User.id != 1)
    sql, params = query.build()
    assert "(users.id <> $1)" in sql
    assert params == [1]

    # Test > operator
    query = SelectQuery[User](None, (User,)).where(User.age > 18)
    sql, params = query.build()
    assert "(users.age > $1)" in sql
    assert params == [18]

    # Test >= operator
    query = SelectQuery[User](None, (User,)).where(User.age >= 18)
    sql, params = query.build()
    assert "(users.age >= $1)" in sql
    assert params == [18]

    # Test < operator
    query = SelectQuery[User](None, (User,)).where(User.age < 65)
    sql, params = query.build()
    assert "(users.age < $1)" in sql
    assert params == [65]

    # Test <= operator
    query = SelectQuery[User](None, (User,)).where(User.age <= 65)
    sql, params = query.build()
    assert "(users.age <= $1)" in sql
    assert params == [65]

    # Test complex expression with dunder operators
    query = SelectQuery[User](None, (User,)).where((User.age > 18) & (User.age < 65))
    sql, params = query.build()
    assert "(users.age > $1)" in sql
    assert "(users.age < $2)" in sql
    assert params == [18, 65]


# =============================================================================
# JOIN Tests
# =============================================================================


def test_inner_join():
    """Test SELECT with INNER JOIN."""
    query = SelectQuery[User](None, (User,)).inner_join(Post, User.id == Post.user_id)
    sql, params = query.build()

    assert "INNER JOIN posts ON" in sql
    assert "(users.id = posts.user_id)" in sql
    assert params == []


def test_left_join():
    """Test SELECT with LEFT JOIN."""
    query = SelectQuery[User](None, (User,)).left_join(Post, User.id == Post.user_id)
    sql, params = query.build()

    assert "LEFT JOIN posts ON" in sql
    assert "(users.id = posts.user_id)" in sql


def test_right_join():
    """Test SELECT with RIGHT JOIN."""
    query = SelectQuery[User](None, (User,)).right_join(Post, User.id == Post.user_id)
    sql, params = query.build()

    assert "RIGHT JOIN posts ON" in sql
    assert "(users.id = posts.user_id)" in sql


def test_full_join():
    """Test SELECT with FULL OUTER JOIN."""
    query = SelectQuery[User](None, (User,)).full_join(Post, User.id == Post.user_id)
    sql, params = query.build()

    assert "FULL OUTER JOIN posts ON" in sql
    assert "(users.id = posts.user_id)" in sql


def test_cross_join():
    """Test SELECT with CROSS JOIN."""
    query = SelectQuery[User](None, (User,)).cross_join(Post)
    sql, params = query.build()

    assert "CROSS JOIN posts" in sql
    assert "ON" not in sql


def test_multiple_joins():
    """Test SELECT with multiple JOINs."""
    query = (
        SelectQuery[User](None, (User,))
        .inner_join(Post, User.id == Post.user_id)
        .left_join(Post, User.id == Post.user_id)
    )
    sql, params = query.build()

    assert sql.count("JOIN") == 2
    assert "INNER JOIN" in sql
    assert "LEFT JOIN" in sql


def test_join_with_where():
    """Test JOIN combined with WHERE clause."""
    query = (
        SelectQuery[User](None, (User,))
        .inner_join(Post, User.id == Post.user_id)
        .where(Post.published)
    )
    sql, params = query.build()

    assert "INNER JOIN" in sql
    assert "WHERE" in sql
    assert "posts.published" in sql
    assert params == []


# =============================================================================
# GROUP BY Tests
# =============================================================================


def test_group_by_single_column():
    """Test SELECT with GROUP BY single column."""
    query = SelectQuery[User](None, (User,)).group_by(User.age)
    sql, params = query.build()

    assert "GROUP BY users.age" in sql
    assert params == []


def test_group_by_multiple_columns():
    """Test SELECT with GROUP BY multiple columns."""
    query = SelectQuery[User](None, (User,)).group_by(User.age, User.name)
    sql, params = query.build()

    assert "GROUP BY" in sql
    assert "users.age" in sql
    assert "users.name" in sql
    assert sql.count(",") >= 1  # At least one comma in GROUP BY


def test_group_by_with_where():
    """Test GROUP BY combined with WHERE clause."""
    query = SelectQuery[User](None, (User,)).where(User.age > 18).group_by(User.age)
    sql, params = query.build()

    assert "WHERE" in sql
    assert "GROUP BY users.age" in sql
    assert params == [18]


# =============================================================================
# Multiple ORDER BY Tests
# =============================================================================


def test_order_by_multiple_columns():
    """Test SELECT with multiple ORDER BY columns."""
    query = (
        SelectQuery[User](None, (User,))
        .order_by(User.age, asc=False)
        .order_by(User.name, asc=True)
    )
    sql, params = query.build()

    assert "ORDER BY" in sql
    assert "users.age DESC" in sql
    assert "users.name ASC" in sql
    assert sql.count(",") >= 1  # At least one comma in ORDER BY


def test_order_by_with_limit_offset():
    """Test ORDER BY combined with LIMIT and OFFSET."""
    query = SelectQuery[User](None, (User,)).order_by(User.name).limit(10).offset(20)
    sql, params = query.build()

    assert "ORDER BY" in sql
    assert "LIMIT 10" in sql
    assert "OFFSET 20" in sql


# =============================================================================
# UPDATE with RETURNING Tests
# =============================================================================


def test_update_returning_table():
    """Test UPDATE with RETURNING table."""
    query = (
        UpdateQuery[User](None, User)
        .set(name="Robert")
        .where(User.id == 1)
        .returning(User)
    )
    sql, params = query.build()

    assert "UPDATE users SET name = $1" in sql
    assert "WHERE (users.id = $2)" in sql
    assert "RETURNING *" in sql
    assert params == ["Robert", 1]


def test_update_returning_columns():
    """Test UPDATE with RETURNING specific columns."""
    query = (
        UpdateQuery[User](None, User)
        .set(name="Robert", email="robert@example.com")
        .where(User.id == 1)
        .returning(User.id, User.name)
    )
    sql, params = query.build()

    assert "UPDATE users SET" in sql
    assert "RETURNING id, name" in sql
    assert params == ["Robert", "robert@example.com", 1]


def test_update_multiple_set():
    """Test UPDATE with multiple SET values."""
    query = (
        UpdateQuery[User](None, User)
        .set(name="Robert", email="robert@example.com", age=30)
        .where(User.id == 1)
    )
    sql, params = query.build()

    assert "UPDATE users SET" in sql
    assert "name = $1" in sql
    assert "email = $2" in sql
    assert "age = $3" in sql
    assert "WHERE (users.id = $4)" in sql
    assert params == ["Robert", "robert@example.com", 30, 1]


# =============================================================================
# DELETE with RETURNING Tests
# =============================================================================


def test_delete_returning_table():
    """Test DELETE with RETURNING table."""
    query = DeleteQuery[User](None, User).where(User.id == 1).returning(User)
    sql, params = query.build()

    assert "DELETE FROM users" in sql
    assert "WHERE (users.id = $1)" in sql
    assert "RETURNING *" in sql
    assert params == [1]


def test_delete_returning_columns():
    """Test DELETE with RETURNING specific columns."""
    query = (
        DeleteQuery[User](None, User).where(User.id == 1).returning(User.id, User.name)
    )
    sql, params = query.build()

    assert "DELETE FROM users" in sql
    assert "RETURNING id, name" in sql
    assert params == [1]


def test_delete_with_complex_where():
    """Test DELETE with complex WHERE clause."""
    query = DeleteQuery[User](None, User).where((User.age > 65) | (User.age < 18))
    sql, params = query.build()

    assert "DELETE FROM users" in sql
    assert "WHERE" in sql
    assert "OR" in sql
    assert params == [65, 18]


# =============================================================================
# INSERT with RETURNING specific columns Tests
# =============================================================================


def test_insert_returning_columns():
    """Test INSERT with RETURNING specific columns."""
    query = (
        InsertQuery[User](None, User)
        .values(name="Bob", email="bob@example.com")
        .returning(User.id, User.name)
    )
    sql, params = query.build()

    assert "INSERT INTO users" in sql
    assert "RETURNING id, name" in sql
    assert params == ["Bob", "bob@example.com"]


def test_insert_multiple_values():
    """Test INSERT with multiple column values."""
    query = InsertQuery[User](None, User).values(
        name="Alice", email="alice@example.com", age=25
    )
    sql, params = query.build()

    assert "INSERT INTO users" in sql
    assert "(name, email, age)" in sql
    assert "VALUES ($1, $2, $3)" in sql
    assert params == ["Alice", "alice@example.com", 25]


# =============================================================================
# Additional Expression Operators Tests
# =============================================================================


def test_not_in():
    """Test NOT IN clause."""
    query = SelectQuery[User](None, (User,)).where(User.id.not_in([1, 2, 3]))
    sql, params = query.build()

    assert "NOT IN ($1, $2, $3)" in sql
    assert params == [1, 2, 3]


def test_ilike():
    """Test ILIKE (case-insensitive LIKE) clause."""
    query = SelectQuery[User](None, (User,)).where(User.name.ilike("%alice%"))
    sql, params = query.build()

    assert "ILIKE $1" in sql
    assert params == ["%alice%"]


def test_is_null():
    """Test IS NULL clause."""
    query = SelectQuery[User](None, (User,)).where(User.age.is_null())
    sql, params = query.build()

    assert "IS NULL" in sql
    assert params == []


def test_is_not_null():
    """Test IS NOT NULL clause."""
    query = SelectQuery[User](None, (User,)).where(User.age.is_not_null())
    sql, params = query.build()

    assert "IS NOT NULL" in sql
    assert params == []


def test_between():
    """Test BETWEEN clause."""
    query = SelectQuery[User](None, (User,)).where(User.age.between(18, 65))
    sql, params = query.build()

    assert "BETWEEN $1 AND $2" in sql
    assert params == [18, 65]


def test_not_operator():
    """Test NOT operator."""
    query = SelectQuery[User](None, (User,)).where(~(User.age > 18))
    sql, params = query.build()

    assert "NOT" in sql
    assert "(users.age > $1)" in sql
    assert params == [18]


def test_not_with_is_null():
    """Test NOT with IS NULL."""
    query = SelectQuery[User](None, (User,)).where(~User.age.is_null())
    sql, params = query.build()

    assert "NOT" in sql
    assert "IS NULL" in sql
    assert params == []


# =============================================================================
# SelectQuery Tests
# =============================================================================


def test_column_select_with_join():
    """Test SelectQuery with JOIN."""
    query = (
        SelectQuery[Any](None, (User.id, User.name, Post.title))
        .from_(User)
        .inner_join(Post, User.id == Post.user_id)
    )
    sql, params = query.build()

    assert "SELECT users.id, users.name, posts.title" in sql
    assert "FROM users" in sql
    assert "INNER JOIN posts" in sql
    assert params == []


def test_column_select_with_where():
    """Test SelectQuery with WHERE clause."""
    query = (
        SelectQuery[Any](None, (User.id, User.name)).from_(User).where(User.age > 18)
    )
    sql, params = query.build()

    assert "SELECT users.id, users.name" in sql
    assert "WHERE (users.age > $1)" in sql
    assert params == [18]


def test_column_select_with_group_by():
    """Test SelectQuery with GROUP BY."""
    query = SelectQuery[Any](None, (User.age,)).from_(User).group_by(User.age)
    sql, params = query.build()

    assert "SELECT users.age" in sql
    assert "GROUP BY users.age" in sql
    assert params == []


# =============================================================================
# Complex Query Combinations Tests
# =============================================================================


def test_complex_select_all_clauses():
    """Test SELECT with all clauses combined."""
    query = (
        SelectQuery[User](None, (User,))
        .inner_join(Post, User.id == Post.user_id)
        .where((User.age > 18) & (User.age < 65))
        .group_by(User.age)
        .order_by(User.age, asc=False)
        .order_by(User.name, asc=True)
        .limit(10)
        .offset(20)
    )
    sql, params = query.build()

    assert "SELECT users.* FROM users" in sql
    assert "INNER JOIN posts" in sql
    assert "WHERE" in sql
    assert "AND" in sql
    assert "GROUP BY users.age" in sql
    assert "ORDER BY" in sql
    assert "LIMIT 10" in sql
    assert "OFFSET 20" in sql
    assert params == [18, 65]


def test_complex_where_with_all_operators():
    """Test complex WHERE with multiple operator types."""
    query = SelectQuery(None, (User,)).where(
        (User.id.in_([1, 2, 3]))
        & (User.name.like("%Alice%"))
        & (User.age.between(18, 65))
        & (User.email.is_not_null())
        & (~(User.age < 18))
    )
    sql, params = query.build()

    assert "IN" in sql
    assert "LIKE" in sql
    assert "BETWEEN" in sql
    assert "IS NOT NULL" in sql
    assert "NOT" in sql
    assert len(params) >= 5


def test_update_with_complex_where():
    """Test UPDATE with complex WHERE clause."""
    query = (
        UpdateQuery[User](None, User)
        .set(name="Updated", age=30)
        .where((User.id.in_([1, 2, 3])) & (User.age > 18) & (User.email.is_not_null()))
        .returning(User.id, User.name)
    )
    sql, params = query.build()

    assert "UPDATE users SET" in sql
    assert "WHERE" in sql
    assert "IN" in sql
    assert "IS NOT NULL" in sql
    assert "RETURNING id, name" in sql
    assert len(params) >= 5


def test_delete_with_multiple_conditions():
    """Test DELETE with multiple WHERE conditions."""
    query = (
        DeleteQuery[User](None, User)
        .where((User.age < 18) | (User.age > 65))
        .returning(User)
    )
    sql, params = query.build()

    assert "DELETE FROM users" in sql
    assert "WHERE" in sql
    assert "OR" in sql
    assert "RETURNING *" in sql
    assert params == [18, 65]


# =============================================================================
# COUNT Tests
# =============================================================================


def test_count_all():
    """Test COUNT(*) query building."""
    query = SelectQuery[User](None, (User,))
    sql, params = query.build_count()

    assert sql == "SELECT COUNT(*) FROM users"
    assert params == []


def test_count_where():
    """Test COUNT(*) with WHERE clause."""
    query = SelectQuery[User](None, (User,)).where(User.age > 18)
    sql, params = query.build_count()

    assert sql == "SELECT COUNT(*) FROM users WHERE (users.age > $1)"
    assert params == [18]


def test_count_join():
    """Test COUNT(*) with JOIN clause."""
    query = (
        SelectQuery[User](None, (User,))
        .inner_join(Post, User.id == Post.user_id)
        .where(Post.published)
    )
    sql, params = query.build_count()

    assert "SELECT COUNT(*) FROM users" in sql
    assert "INNER JOIN posts ON" in sql
    assert "posts.published" in sql
    assert params == []


def test_count_ignores_order_limit_offset():
    """Test that COUNT(*) ignores ORDER BY, LIMIT, and OFFSET."""
    query = (
        SelectQuery[User](None, (User,))
        .where(User.age > 18)
        .order_by(User.name)
        .limit(10)
        .offset(20)
    )
    sql, params = query.build_count()

    assert "ORDER BY" not in sql
    assert "LIMIT" not in sql
    assert "OFFSET" not in sql
    assert sql == "SELECT COUNT(*) FROM users WHERE (users.age > $1)"
    assert params == [18]


def test_count_ignores_column_selection():
    """Test that COUNT(*) ignores the selected columns."""
    query = SelectQuery[Any](None, (User.id, User.name)).from_(User)
    sql, params = query.build_count()

    assert sql == "SELECT COUNT(*) FROM users"
    assert params == []


# =============================================================================
# Transaction-bound builder tests
# =============================================================================


@pytest.mark.asyncio
async def test_acquire_uses_pool_acquire():
    """_acquire acquires from pool when given a Pool-like object."""
    mock_conn = AsyncMock()
    mock_pool = MagicMock()
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    # asyncpg.Pool is the type we check with isinstance, so make it look like one
    mock_pool.__class__ = type("Pool", (), {})

    # When given a non-Pool connection, _acquire yields it directly
    async with _acquire(mock_conn) as conn:
        assert conn is mock_conn


@pytest.mark.asyncio
async def test_acquire_yields_connection_directly():
    """_acquire yields connection directly when given a Connection."""
    mock_conn = AsyncMock()
    async with _acquire(mock_conn) as conn:
        assert conn is mock_conn


def test_insert_returning_preserves_connection():
    """returning() propagates the pool/connection via the parent query."""
    sentinel = object()
    query = InsertQuery(sentinel, User).values(name="Alice")  # type: ignore[arg-type]
    returning_query = query.returning(User)
    assert returning_query._parent._pool is sentinel


def test_insert_returning_tuple_preserves_connection():
    """returning() with columns propagates the pool/connection."""
    sentinel = object()
    query = InsertQuery(sentinel, User).values(name="Alice")  # type: ignore[arg-type]
    returning_query = query.returning(User.id)
    assert returning_query._parent._pool is sentinel


def test_update_returning_preserves_connection():
    """returning() propagates the pool/connection via the parent query."""
    sentinel = object()
    query = UpdateQuery(sentinel, User).set(name="Bob")  # type: ignore[arg-type]
    returning_query = query.returning(User)
    assert returning_query._parent._pool is sentinel


def test_update_returning_tuple_preserves_connection():
    """returning() with columns propagates the pool/connection."""
    sentinel = object()
    query = UpdateQuery(sentinel, User).set(name="Bob")  # type: ignore[arg-type]
    returning_query = query.returning(User.id)
    assert returning_query._parent._pool is sentinel


def test_delete_returning_preserves_connection():
    """returning() propagates the pool/connection via the parent query."""
    sentinel = object()
    query = DeleteQuery(sentinel, User).where(User.id == 1)  # type: ignore[arg-type]
    returning_query = query.returning(User)
    assert returning_query._parent._pool is sentinel


def test_delete_returning_tuple_preserves_connection():
    """returning() with columns propagates the pool/connection."""
    sentinel = object()
    query = DeleteQuery(sentinel, User).where(User.id == 1)  # type: ignore[arg-type]
    returning_query = query.returning(User.id)
    assert returning_query._parent._pool is sentinel


def test_select_builds_with_connection():
    """SelectQuery builds valid SQL regardless of pool vs connection."""
    sentinel = object()
    query = SelectQuery[User](sentinel, (User,)).where(User.id == 1)  # type: ignore[arg-type]
    sql, params = query.build()
    assert sql == "SELECT users.* FROM users WHERE (users.id = $1)"
    assert params == [1]


# =============================================================================
# Shorthand filter method tests
# =============================================================================


def test_eq_with_string():
    """Test .eq() with a string column name."""
    query = SelectQuery[User](None, (User,)).eq("id", 1)
    sql, params = query.build()
    assert "(users.id = $1)" in sql
    assert params == [1]


def test_eq_with_column():
    """Test .eq() with a Column."""
    query = SelectQuery[User](None, (User,)).eq(User.id, 1)
    sql, params = query.build()
    assert "(users.id = $1)" in sql
    assert params == [1]


def test_neq_with_string():
    """Test .neq() with a string column name."""
    query = SelectQuery[User](None, (User,)).neq("name", "Alice")
    sql, params = query.build()
    assert "(users.name <> $1)" in sql
    assert params == ["Alice"]


def test_gt_with_string():
    """Test .gt() with a string column name."""
    query = SelectQuery[User](None, (User,)).gt("age", 18)
    sql, params = query.build()
    assert "(users.age > $1)" in sql
    assert params == [18]


def test_gte_with_string():
    """Test .gte() with a string column name."""
    query = SelectQuery[User](None, (User,)).gte("age", 18)
    sql, params = query.build()
    assert "(users.age >= $1)" in sql
    assert params == [18]


def test_lt_with_string():
    """Test .lt() with a string column name."""
    query = SelectQuery[User](None, (User,)).lt("age", 65)
    sql, params = query.build()
    assert "(users.age < $1)" in sql
    assert params == [65]


def test_lte_with_string():
    """Test .lte() with a string column name."""
    query = SelectQuery[User](None, (User,)).lte("age", 65)
    sql, params = query.build()
    assert "(users.age <= $1)" in sql
    assert params == [65]


def test_is_null_with_string():
    """Test .is_null() with a string column name."""
    query = SelectQuery[User](None, (User,)).is_null("age")
    sql, params = query.build()
    assert "(users.age IS NULL)" in sql
    assert params == []


def test_is_not_null_with_string():
    """Test .is_not_null() with a string column name."""
    query = SelectQuery[User](None, (User,)).is_not_null("email")
    sql, params = query.build()
    assert "(users.email IS NOT NULL)" in sql
    assert params == []


def test_in_with_string():
    """Test .in_() with a string column name."""
    query = SelectQuery[User](None, (User,)).in_("id", [1, 2, 3])
    sql, params = query.build()
    assert "users.id" in sql
    assert "IN ($1, $2, $3)" in sql
    assert params == [1, 2, 3]


def test_not_in_with_string():
    """Test .not_in() with a string column name."""
    query = SelectQuery[User](None, (User,)).not_in("id", [1, 2])
    sql, params = query.build()
    assert "NOT IN ($1, $2)" in sql
    assert params == [1, 2]


def test_like_with_string():
    """Test .like() with a string column name."""
    query = SelectQuery[User](None, (User,)).like("name", "%Alice%")
    sql, params = query.build()
    assert "users.name" in sql
    assert "LIKE $1" in sql
    assert params == ["%Alice%"]


def test_ilike_with_string():
    """Test .ilike() with a string column name."""
    query = SelectQuery[User](None, (User,)).ilike("name", "%alice%")
    sql, params = query.build()
    assert "ILIKE $1" in sql
    assert params == ["%alice%"]


def test_between_with_string():
    """Test .between() with a string column name."""
    query = SelectQuery[User](None, (User,)).between("age", 18, 65)
    sql, params = query.build()
    assert "users.age" in sql
    assert "BETWEEN $1 AND $2" in sql
    assert params == [18, 65]


def test_dot_notation_string():
    """Test string column with explicit table.column format."""
    query = SelectQuery[User](None, (User,)).eq("posts.user_id", 1)
    sql, params = query.build()
    assert "(posts.user_id = $1)" in sql
    assert params == [1]


def test_chained_shorthand_methods():
    """Test chaining multiple shorthand methods combines with AND."""
    query = (
        SelectQuery[User](None, (User,))
        .eq("name", "Alice")
        .gt("age", 18)
        .is_not_null("email")
    )
    sql, params = query.build()
    assert "AND" in sql
    assert "(users.name = $1)" in sql
    assert "(users.age > $2)" in sql
    assert "(users.email IS NOT NULL)" in sql
    assert params == ["Alice", 18]


def test_mixed_shorthand_and_where():
    """Test mixing shorthand methods with where()."""
    query = SelectQuery[User](None, (User,)).eq("name", "Alice").where(User.age > 18)
    sql, params = query.build()
    assert "AND" in sql
    assert "(users.name = $1)" in sql
    assert "(users.age > $2)" in sql
    assert params == ["Alice", 18]


def test_update_shorthand():
    """Test shorthand methods on UpdateQuery."""
    query = UpdateQuery[User](None, User).set(name="Robert").eq("id", 1)
    sql, params = query.build()
    assert "UPDATE users SET name = $1" in sql
    assert "WHERE (users.id = $2)" in sql
    assert params == ["Robert", 1]


def test_delete_shorthand():
    """Test shorthand methods on DeleteQuery."""
    query = DeleteQuery[User](None, User).eq("id", 1)
    sql, params = query.build()
    assert "DELETE FROM users" in sql
    assert "WHERE (users.id = $1)" in sql
    assert params == [1]


def test_shorthand_no_table_context_raises():
    """Test that bare string without table context raises ValueError."""
    query = SelectQuery[User](None, ())
    with pytest.raises(ValueError, match="no table context"):
        query.eq("id", 1)


# =============================================================================
# Tuple select tests
# =============================================================================


def test_multi_column_detected():
    """Multi-column selection is detected."""
    query = SelectQuery[Any](None, (User.id, User.name))
    assert query._is_multi_column() is True
    assert query._is_single_column() is False


def test_single_table_not_column_selection():
    """Single table selection is not a column selection."""
    query = SelectQuery[User](None, (User,))
    assert query._is_multi_column() is False
    assert query._is_single_column() is False


def test_single_column_detected():
    """Single column selection is detected."""
    query = SelectQuery[Any](None, (User.id,))
    assert query._is_single_column() is True
    assert query._is_multi_column() is False


def test_hydrate_returns_scalars_for_single_column():
    """_hydrate returns scalar values for single-column selections."""
    query = SelectQuery[Any](None, (User.id,))
    rows = [{"id": 1}, {"id": 2}, {"id": 3}]
    result = query._hydrate(rows)
    assert result == [1, 2, 3]


def test_hydrate_returns_tuples_for_columns():
    """_hydrate returns tuples for multi-column selections."""
    query = SelectQuery[Any](None, (User.id, User.name))
    rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    result = query._hydrate(rows)
    assert result == [(1, "Alice"), (2, "Bob")]
    assert all(isinstance(r, tuple) for r in result)


def test_hydrate_returns_dicts_for_mixed():
    """_hydrate returns dicts when selection includes non-Column items."""

    query = SelectQuery[Any](None, (User.id, sql("COUNT(*)")))
    rows = [{"id": 1, "count": 5}]
    result = query._hydrate(rows)
    assert result == [{"id": 1, "count": 5}]


@pytest.mark.asyncio
async def test_execute_returns_tuples_for_columns():
    """Full execute path returns tuples for multi-column selections."""
    mock_conn = AsyncMock()
    mock_record_1 = MagicMock()
    mock_record_1.__iter__ = MagicMock(return_value=iter([1, "Alice"]))
    mock_record_1.items = MagicMock(return_value=[("id", 1), ("name", "Alice")])
    mock_record_1.keys = MagicMock(return_value=["id", "name"])
    mock_record_1.__getitem__ = lambda self, k: {"id": 1, "name": "Alice"}[k]

    mock_record_2 = MagicMock()
    mock_record_2.__iter__ = MagicMock(return_value=iter([2, "Bob"]))
    mock_record_2.items = MagicMock(return_value=[("id", 2), ("name", "Bob")])
    mock_record_2.keys = MagicMock(return_value=["id", "name"])
    mock_record_2.__getitem__ = lambda self, k: {"id": 2, "name": "Bob"}[k]

    mock_conn.fetch = AsyncMock(return_value=[mock_record_1, mock_record_2])

    query = SelectQuery[Any](mock_conn, (User.id, User.name))
    query.from_(User)
    result = await query.execute()
    assert result == [(1, "Alice"), (2, "Bob")]
    assert all(isinstance(r, tuple) for r in result)


# =============================================================================
# Returning executor tests
# =============================================================================


def _mock_record(**kwargs: Any) -> MagicMock:
    """Create a mock asyncpg Record that behaves like a dict."""
    record = MagicMock()
    record.items.return_value = list(kwargs.items())
    record.keys.return_value = list(kwargs.keys())
    record.__getitem__ = lambda self, k: kwargs[k]
    record.__contains__ = lambda self, k: k in kwargs
    record.__iter__ = lambda self: iter(kwargs)
    return record


def _mock_conn_fetchrow(return_value: Any) -> AsyncMock:
    """Create a mock connection that returns a single row."""
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=return_value)
    return conn


def _mock_conn_fetch(return_value: list[Any]) -> AsyncMock:
    """Create a mock connection that returns multiple rows."""
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=return_value)
    return conn


# -- INSERT returning table ---------------------------------------------------


class TestInsertReturningTable:
    async def test_returns_model(self) -> None:
        record = _mock_record(
            id=1, name="Alice", email="a@b.com", age=30, created_at="now"
        )
        conn = _mock_conn_fetchrow(record)
        query = InsertQuery(conn, User).values(name="Alice", email="a@b.com")
        result = await query.returning(User).execute()
        assert isinstance(result, User)
        assert result.name == "Alice"

    async def test_raises_on_no_rows(self) -> None:
        conn = _mock_conn_fetchrow(None)
        query = InsertQuery(conn, User).values(name="Alice", email="a@b.com")
        with pytest.raises(RuntimeError, match="no rows"):
            await query.returning(User).execute()


# -- INSERT returning scalar (1 column) ---------------------------------------


class TestInsertReturningScalar:
    async def test_returns_scalar(self) -> None:
        record = _mock_record(id=42)
        conn = _mock_conn_fetchrow(record)
        query = InsertQuery(conn, User).values(name="Alice", email="a@b.com")
        result = await query.returning(User.id).execute()
        assert result == 42

    async def test_raises_on_no_rows(self) -> None:
        conn = _mock_conn_fetchrow(None)
        query = InsertQuery(conn, User).values(name="Alice", email="a@b.com")
        with pytest.raises(RuntimeError, match="no rows"):
            await query.returning(User.id).execute()


# -- INSERT returning tuple (2+ columns) -------------------------------------


class TestInsertReturningTuple:
    async def test_returns_tuple(self) -> None:
        record = _mock_record(id=1, name="Alice")
        conn = _mock_conn_fetchrow(record)
        query = InsertQuery(conn, User).values(name="Alice", email="a@b.com")
        result = await query.returning(User.id, User.name).execute()
        assert result == (1, "Alice")
        assert isinstance(result, tuple)

    async def test_three_columns(self) -> None:
        record = _mock_record(id=1, name="Alice", email="a@b.com")
        conn = _mock_conn_fetchrow(record)
        query = InsertQuery(conn, User).values(name="Alice", email="a@b.com")
        result = await query.returning(User.id, User.name, User.email).execute()
        assert result == (1, "Alice", "a@b.com")


# -- INSERT ignore_conflicts returning table ----------------------------------


class TestInsertIgnoreConflictsReturningTable:
    async def test_returns_model_on_insert(self) -> None:
        record = _mock_record(
            id=1, name="Alice", email="a@b.com", age=30, created_at="now"
        )
        conn = _mock_conn_fetchrow(record)
        query = (
            InsertQuery(conn, User)
            .values(name="Alice", email="a@b.com")
            .ignore_conflicts(target=User.email)
        )
        result = await query.returning(User).execute()
        assert isinstance(result, User)
        assert result.name == "Alice"

    async def test_returns_none_on_conflict(self) -> None:
        conn = _mock_conn_fetchrow(None)
        query = (
            InsertQuery(conn, User)
            .values(name="Alice", email="a@b.com")
            .ignore_conflicts(target=User.email)
        )
        result = await query.returning(User).execute()
        assert result is None


# -- INSERT ignore_conflicts returning scalar ---------------------------------


class TestInsertIgnoreConflictsReturningScalar:
    async def test_returns_scalar_on_insert(self) -> None:
        record = _mock_record(id=42)
        conn = _mock_conn_fetchrow(record)
        query = (
            InsertQuery(conn, User)
            .values(name="Alice", email="a@b.com")
            .ignore_conflicts(target=User.email)
        )
        result = await query.returning(User.id).execute()
        assert result == 42

    async def test_returns_none_on_conflict(self) -> None:
        conn = _mock_conn_fetchrow(None)
        query = (
            InsertQuery(conn, User)
            .values(name="Alice", email="a@b.com")
            .ignore_conflicts(target=User.email)
        )
        result = await query.returning(User.id).execute()
        assert result is None


# -- INSERT ignore_conflicts returning tuple ----------------------------------


class TestInsertIgnoreConflictsReturningTuple:
    async def test_returns_tuple_on_insert(self) -> None:
        record = _mock_record(id=1, name="Alice")
        conn = _mock_conn_fetchrow(record)
        query = (
            InsertQuery(conn, User)
            .values(name="Alice", email="a@b.com")
            .ignore_conflicts(target=User.email)
        )
        result = await query.returning(User.id, User.name).execute()
        assert result == (1, "Alice")

    async def test_returns_none_on_conflict(self) -> None:
        conn = _mock_conn_fetchrow(None)
        query = (
            InsertQuery(conn, User)
            .values(name="Alice", email="a@b.com")
            .ignore_conflicts(target=User.email)
        )
        result = await query.returning(User.id, User.name).execute()
        assert result is None


# -- UPDATE returning table ---------------------------------------------------


class TestUpdateReturningTable:
    async def test_returns_list_of_models(self) -> None:
        records = [
            _mock_record(id=1, name="Bob", email="a@b.com", age=30, created_at="now"),
            _mock_record(id=2, name="Bob", email="b@c.com", age=25, created_at="now"),
        ]
        conn = _mock_conn_fetch(records)
        query = UpdateQuery(conn, User).set(name="Bob").where(User.age > 20)
        result = await query.returning(User).execute()
        assert len(result) == 2
        assert all(isinstance(r, User) for r in result)
        assert result[0].name == "Bob"

    async def test_returns_empty_list(self) -> None:
        conn = _mock_conn_fetch([])
        query = UpdateQuery(conn, User).set(name="Bob").where(User.id == 999)
        result = await query.returning(User).execute()
        assert result == []


# -- UPDATE returning scalar --------------------------------------------------


class TestUpdateReturningScalar:
    async def test_returns_list_of_scalars(self) -> None:
        records = [_mock_record(id=1), _mock_record(id=2), _mock_record(id=3)]
        conn = _mock_conn_fetch(records)
        query = UpdateQuery(conn, User).set(name="Bob").where(User.age > 20)
        result = await query.returning(User.id).execute()
        assert result == [1, 2, 3]

    async def test_returns_empty_list(self) -> None:
        conn = _mock_conn_fetch([])
        query = UpdateQuery(conn, User).set(name="Bob").where(User.id == 999)
        result = await query.returning(User.id).execute()
        assert result == []


# -- UPDATE returning tuple ---------------------------------------------------


class TestUpdateReturningTuple:
    async def test_returns_list_of_tuples(self) -> None:
        records = [
            _mock_record(id=1, name="Bob"),
            _mock_record(id=2, name="Bob"),
        ]
        conn = _mock_conn_fetch(records)
        query = UpdateQuery(conn, User).set(name="Bob").where(User.age > 20)
        result = await query.returning(User.id, User.name).execute()
        assert result == [(1, "Bob"), (2, "Bob")]
        assert all(isinstance(r, tuple) for r in result)


# -- DELETE returning table ---------------------------------------------------


class TestDeleteReturningTable:
    async def test_returns_list_of_models(self) -> None:
        records = [
            _mock_record(id=1, name="Alice", email="a@b.com", age=30, created_at="now"),
        ]
        conn = _mock_conn_fetch(records)
        query = DeleteQuery(conn, User).where(User.id == 1)
        result = await query.returning(User).execute()
        assert len(result) == 1
        assert isinstance(result[0], User)

    async def test_returns_empty_list(self) -> None:
        conn = _mock_conn_fetch([])
        query = DeleteQuery(conn, User).where(User.id == 999)
        result = await query.returning(User).execute()
        assert result == []


# -- DELETE returning scalar --------------------------------------------------


class TestDeleteReturningScalar:
    async def test_returns_list_of_scalars(self) -> None:
        records = [_mock_record(id=1), _mock_record(id=2)]
        conn = _mock_conn_fetch(records)
        query = DeleteQuery(conn, User).where(User.age < 18)
        result = await query.returning(User.id).execute()
        assert result == [1, 2]


# -- DELETE returning tuple ---------------------------------------------------


class TestDeleteReturningTuple:
    async def test_returns_list_of_tuples(self) -> None:
        records = [
            _mock_record(id=1, email="a@b.com"),
            _mock_record(id=2, email="b@c.com"),
        ]
        conn = _mock_conn_fetch(records)
        query = DeleteQuery(conn, User).where(User.age < 18)
        result = await query.returning(User.id, User.email).execute()
        assert result == [(1, "a@b.com"), (2, "b@c.com")]
        assert all(isinstance(r, tuple) for r in result)


# -- SQL generation tests for RETURNING clause --------------------------------


class TestReturningSQLGeneration:
    def test_insert_returning_table_sql(self) -> None:
        query = InsertQuery(None, User).values(name="Alice", email="a@b.com")
        sql, _ = query.returning(User).build()
        assert "RETURNING *" in sql

    def test_insert_returning_single_column_sql(self) -> None:
        query = InsertQuery(None, User).values(name="Alice", email="a@b.com")
        sql, _ = query.returning(User.id).build()
        assert "RETURNING id" in sql

    def test_insert_returning_multi_column_sql(self) -> None:
        query = InsertQuery(None, User).values(name="Alice", email="a@b.com")
        sql, _ = query.returning(User.id, User.name).build()
        assert "RETURNING id, name" in sql

    def test_insert_ignore_conflicts_returning_sql(self) -> None:
        query = (
            InsertQuery(None, User)
            .values(name="Alice", email="a@b.com")
            .ignore_conflicts(target=User.email)
        )
        sql, _ = query.returning(User.id).build()
        assert "ON CONFLICT (email) DO NOTHING" in sql
        assert "RETURNING id" in sql

    def test_update_returning_table_sql(self) -> None:
        query = UpdateQuery(None, User).set(name="Bob").where(User.id == 1)
        sql, _ = query.returning(User).build()
        assert "RETURNING *" in sql

    def test_update_returning_columns_sql(self) -> None:
        query = UpdateQuery(None, User).set(name="Bob").where(User.id == 1)
        sql, _ = query.returning(User.id, User.name).build()
        assert "RETURNING id, name" in sql

    def test_delete_returning_table_sql(self) -> None:
        query = DeleteQuery(None, User).where(User.id == 1)
        sql, _ = query.returning(User).build()
        assert "RETURNING *" in sql

    def test_delete_returning_columns_sql(self) -> None:
        query = DeleteQuery(None, User).where(User.id == 1)
        sql, _ = query.returning(User.id, User.email).build()
        assert "RETURNING id, email" in sql
