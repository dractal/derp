"""Tests for query builder."""

from datetime import datetime

from dribble import Field, Table, and_, eq, gt, in_, like, lt, or_
from dribble.fields import Integer, Serial, Timestamp, Varchar
from dribble.query.builder import DeleteQuery, InsertQuery, SelectQuery, UpdateQuery


class User(Table, table_name="users"):
    id: int = Field(Serial(), primary_key=True)
    name: str = Field(Varchar(255))
    email: str = Field(Varchar(255), unique=True)
    age: int = Field(Integer(), nullable=True)
    created_at: datetime = Field(Timestamp(), default="now()")


def test_select_all():
    """Test SELECT * query building."""
    query = SelectQuery[User](None, (User,))
    sql, params = query.build()

    assert sql == "SELECT users.* FROM users"
    assert params == []


def test_select_columns():
    """Test SELECT specific columns."""
    query = SelectQuery[User](None, (User.c.id, User.c.name))
    query.from_(User)
    sql, params = query.build()

    assert "SELECT users.id, users.name FROM users" == sql
    assert params == []


def test_select_where_eq():
    """Test SELECT with WHERE equality."""
    query = SelectQuery[User](None, (User,)).where(eq(User.c.id, 1))
    sql, params = query.build()

    assert "SELECT users.* FROM users WHERE" in sql
    assert "(users.id = $1)" in sql
    assert params == [1]


def test_select_where_comparison():
    """Test SELECT with comparison operators."""
    query = SelectQuery[User](None, (User,)).where(gt(User.c.age, 18))
    sql, params = query.build()

    assert "(users.age > $1)" in sql
    assert params == [18]


def test_select_where_and():
    """Test SELECT with AND condition."""
    query = SelectQuery[User](None, (User,)).where(
        and_(eq(User.c.name, "Alice"), gt(User.c.age, 18))
    )
    sql, params = query.build()

    assert "AND" in sql
    assert "(users.name = $1)" in sql
    assert "(users.age > $2)" in sql
    assert params == ["Alice", 18]


def test_select_where_or():
    """Test SELECT with OR condition."""
    query = SelectQuery[User](None, (User,)).where(
        or_(eq(User.c.name, "Alice"), eq(User.c.name, "Bob"))
    )
    sql, params = query.build()

    assert "OR" in sql
    assert params == ["Alice", "Bob"]


def test_select_where_in():
    """Test SELECT with IN clause."""
    query = SelectQuery[User](None, (User,)).where(in_(User.c.id, [1, 2, 3]))
    sql, params = query.build()

    assert "IN ($1, $2, $3)" in sql
    assert params == [1, 2, 3]


def test_select_where_like():
    """Test SELECT with LIKE pattern."""
    query = SelectQuery[User](None, (User,)).where(like(User.c.name, "%Alice%"))
    sql, params = query.build()

    assert "LIKE $1" in sql
    assert params == ["%Alice%"]


def test_select_order_by():
    """Test SELECT with ORDER BY."""
    query = SelectQuery[User](None, (User,)).order_by(User.c.name, "DESC")
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
        InsertQuery[User](None, User).values(name="Bob", email="bob@example.com").returning(User)
    )
    sql, params = query.build()

    assert "RETURNING *" in sql


def test_update():
    """Test UPDATE query building."""
    query = UpdateQuery[User](None, User).set(name="Robert").where(eq(User.c.id, 1))
    sql, params = query.build()

    assert "UPDATE users SET name = $1" in sql
    assert "WHERE (users.id = $2)" in sql
    assert params == ["Robert", 1]


def test_delete():
    """Test DELETE query building."""
    query = DeleteQuery[User](None, User).where(eq(User.c.id, 1))
    sql, params = query.build()

    assert "DELETE FROM users" in sql
    assert "WHERE (users.id = $1)" in sql
    assert params == [1]


def test_complex_where():
    """Test complex WHERE with nested conditions."""
    query = SelectQuery[User](None, (User,)).where(
        and_(
            or_(eq(User.c.name, "Alice"), eq(User.c.name, "Bob")),
            gt(User.c.age, 18),
            lt(User.c.age, 65),
        )
    )
    sql, params = query.build()

    assert "AND" in sql
    assert "OR" in sql
    assert params == ["Alice", "Bob", 18, 65]
