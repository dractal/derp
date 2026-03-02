"""Tests for Supabase-style string-based query API."""

from __future__ import annotations

from derp.orm.query.table_ref import (
    TableRef,
    UntypedDeleteQuery,
    UntypedInsertQuery,
    UntypedUpdateQuery,
)


def ref(table: str = "users") -> TableRef:
    """Create a TableRef with no pool for SQL-building tests."""
    return TableRef(table, None)


# =============================================================================
# SELECT Tests
# =============================================================================


def test_select_star():
    """Test SELECT * from a string table name."""
    query = ref().select("*")
    sql, params = query.build()
    assert sql == "SELECT * FROM users"
    assert params == []


def test_select_columns():
    """Test SELECT specific columns."""
    query = ref().select("name", "email")
    sql, params = query.build()
    assert sql == "SELECT name, email FROM users"
    assert params == []


def test_select_comma_separated():
    """Test SELECT with comma-separated column string."""
    query = ref().select("name, email")
    sql, params = query.build()
    assert sql == "SELECT name, email FROM users"
    assert params == []


def test_select_no_args_selects_star():
    """Test SELECT with no args defaults to *."""
    query = ref().select()
    sql, params = query.build()
    assert sql == "SELECT * FROM users"
    assert params == []


def test_select_eq():
    """Test SELECT with .eq() filter."""
    query = ref().select("*").eq("id", 1)
    sql, params = query.build()
    assert "(users.id = $1)" in sql
    assert params == [1]


def test_select_neq():
    """Test SELECT with .neq() filter."""
    query = ref().select("*").neq("name", "Alice")
    sql, params = query.build()
    assert "(users.name <> $1)" in sql
    assert params == ["Alice"]


def test_select_gt():
    """Test SELECT with .gt() filter."""
    query = ref().select("*").gt("age", 18)
    sql, params = query.build()
    assert "(users.age > $1)" in sql
    assert params == [18]


def test_select_gte():
    """Test SELECT with .gte() filter."""
    query = ref().select("*").gte("age", 18)
    sql, params = query.build()
    assert "(users.age >= $1)" in sql
    assert params == [18]


def test_select_lt():
    """Test SELECT with .lt() filter."""
    query = ref().select("*").lt("age", 65)
    sql, params = query.build()
    assert "(users.age < $1)" in sql
    assert params == [65]


def test_select_lte():
    """Test SELECT with .lte() filter."""
    query = ref().select("*").lte("age", 65)
    sql, params = query.build()
    assert "(users.age <= $1)" in sql
    assert params == [65]


def test_select_is_null():
    """Test SELECT with .is_null() filter."""
    query = ref().select("*").is_null("age")
    sql, params = query.build()
    assert "(users.age IS NULL)" in sql
    assert params == []


def test_select_is_not_null():
    """Test SELECT with .is_not_null() filter."""
    query = ref().select("*").is_not_null("email")
    sql, params = query.build()
    assert "(users.email IS NOT NULL)" in sql
    assert params == []


def test_select_in():
    """Test SELECT with .in_() filter."""
    query = ref().select("*").in_("id", [1, 2, 3])
    sql, params = query.build()
    assert "users.id" in sql
    assert "IN ($1, $2, $3)" in sql
    assert params == [1, 2, 3]


def test_select_not_in():
    """Test SELECT with .not_in() filter."""
    query = ref().select("*").not_in("id", [1, 2])
    sql, params = query.build()
    assert "NOT IN ($1, $2)" in sql
    assert params == [1, 2]


def test_select_like():
    """Test SELECT with .like() filter."""
    query = ref().select("*").like("name", "%Alice%")
    sql, params = query.build()
    assert "users.name" in sql
    assert "LIKE $1" in sql
    assert params == ["%Alice%"]


def test_select_ilike():
    """Test SELECT with .ilike() filter."""
    query = ref().select("*").ilike("name", "%alice%")
    sql, params = query.build()
    assert "ILIKE $1" in sql
    assert params == ["%alice%"]


def test_select_between():
    """Test SELECT with .between() filter."""
    query = ref().select("*").between("age", 18, 65)
    sql, params = query.build()
    assert "users.age" in sql
    assert "BETWEEN $1 AND $2" in sql
    assert params == [18, 65]


def test_select_chained_filters():
    """Test chaining multiple filters combines with AND."""
    query = ref().select("*").eq("name", "Alice").gt("age", 18).is_not_null("email")
    sql, params = query.build()
    assert "AND" in sql
    assert "(users.name = $1)" in sql
    assert "(users.age > $2)" in sql
    assert "(users.email IS NOT NULL)" in sql
    assert params == ["Alice", 18]


def test_select_order_by():
    """Test SELECT with order_by."""
    query = ref().select("*").order_by("name", asc=False)
    sql, params = query.build()
    assert "ORDER BY name DESC" in sql


def test_select_limit_offset():
    """Test SELECT with limit and offset."""
    query = ref().select("*").limit(10).offset(20)
    sql, params = query.build()
    assert "LIMIT 10" in sql
    assert "OFFSET 20" in sql


def test_select_count():
    """Test COUNT(*) on string-based select."""
    query = ref().select("*").eq("age", 25)
    sql, params = query.build_count()
    assert sql == "SELECT COUNT(*) FROM users WHERE (users.age = $1)"
    assert params == [25]


# =============================================================================
# INSERT Tests
# =============================================================================


def test_insert():
    """Test INSERT query building."""
    query = ref().insert({"name": "Bob", "email": "bob@example.com"})
    sql, params = query.build()
    assert "INSERT INTO users" in sql
    assert "(name, email)" in sql
    assert "VALUES ($1, $2)" in sql
    assert params == ["Bob", "bob@example.com"]


def test_insert_returning_star():
    """Test INSERT with RETURNING *."""
    query = ref().insert({"name": "Bob"}).returning("*")
    sql, params = query.build()
    assert "INSERT INTO users" in sql
    assert "RETURNING *" in sql
    assert params == ["Bob"]


def test_insert_returning_columns():
    """Test INSERT with RETURNING specific columns."""
    query = ref().insert({"name": "Bob"}).returning("id", "name")
    sql, params = query.build()
    assert "INSERT INTO users" in sql
    assert "RETURNING id, name" in sql


def test_insert_is_untyped_insert_query():
    """Verify insert() returns UntypedInsertQuery."""
    query = ref().insert({"name": "Bob"})
    assert isinstance(query, UntypedInsertQuery)


# =============================================================================
# UPDATE Tests
# =============================================================================


def test_update():
    """Test UPDATE query building."""
    query = ref().update({"name": "Robert"}).eq("id", 1)
    sql, params = query.build()
    assert "UPDATE users SET name = $1" in sql
    assert "WHERE (users.id = $2)" in sql
    assert params == ["Robert", 1]


def test_update_multiple_set():
    """Test UPDATE with multiple SET values."""
    query = ref().update({"name": "Robert", "email": "robert@example.com"}).eq("id", 1)
    sql, params = query.build()
    assert "UPDATE users SET" in sql
    assert "name = $1" in sql
    assert "email = $2" in sql
    assert "WHERE (users.id = $3)" in sql
    assert params == ["Robert", "robert@example.com", 1]


def test_update_returning_star():
    """Test UPDATE with RETURNING *."""
    query = ref().update({"name": "Robert"}).eq("id", 1).returning("*")
    sql, params = query.build()
    assert "RETURNING *" in sql


def test_update_returning_columns():
    """Test UPDATE with RETURNING specific columns."""
    query = ref().update({"name": "Robert"}).eq("id", 1).returning("id", "name")
    sql, params = query.build()
    assert "RETURNING id, name" in sql


def test_update_chained_filters():
    """Test UPDATE with chained filters."""
    query = ref().update({"name": "Robert"}).eq("active", True).gt("age", 18)
    sql, params = query.build()
    assert "AND" in sql
    assert "(users.active = $2)" in sql
    assert "(users.age > $3)" in sql


def test_update_is_untyped():
    """Verify update() returns UntypedUpdateQuery."""
    query = ref().update({"name": "Robert"})
    assert isinstance(query, UntypedUpdateQuery)


# =============================================================================
# DELETE Tests
# =============================================================================


def test_delete():
    """Test DELETE query building."""
    query = ref().delete().eq("id", 1)
    sql, params = query.build()
    assert "DELETE FROM users" in sql
    assert "WHERE (users.id = $1)" in sql
    assert params == [1]


def test_delete_complex_where():
    """Test DELETE with complex WHERE."""
    query = ref().delete().gt("age", 65).is_null("email")
    sql, params = query.build()
    assert "DELETE FROM users" in sql
    assert "AND" in sql
    assert "(users.age > $1)" in sql
    assert "(users.email IS NULL)" in sql
    assert params == [65]


def test_delete_returning_star():
    """Test DELETE with RETURNING *."""
    query = ref().delete().eq("id", 1).returning("*")
    sql, params = query.build()
    assert "DELETE FROM users" in sql
    assert "RETURNING *" in sql


def test_delete_returning_columns():
    """Test DELETE with RETURNING specific columns."""
    query = ref().delete().eq("id", 1).returning("id", "name")
    sql, params = query.build()
    assert "RETURNING id, name" in sql


def test_delete_is_untyped():
    """Verify delete() returns UntypedDeleteQuery."""
    query = ref().delete()
    assert isinstance(query, UntypedDeleteQuery)


# =============================================================================
# TableRef preserves pool/connection
# =============================================================================


def test_table_ref_passes_pool_to_select():
    """TableRef passes pool to SelectQuery."""
    sentinel = object()
    tr = TableRef("users", sentinel)  # type: ignore[arg-type]
    query = tr.select("*")
    assert query._pool is sentinel


def test_table_ref_passes_pool_to_insert():
    """TableRef passes pool to UntypedInsertQuery."""
    sentinel = object()
    tr = TableRef("users", sentinel)  # type: ignore[arg-type]
    query = tr.insert({"name": "Bob"})
    assert query._pool is sentinel


def test_table_ref_passes_pool_to_update():
    """TableRef passes pool to UntypedUpdateQuery."""
    sentinel = object()
    tr = TableRef("users", sentinel)  # type: ignore[arg-type]
    query = tr.update({"name": "Robert"})
    assert query._pool is sentinel


def test_table_ref_passes_pool_to_delete():
    """TableRef passes pool to UntypedDeleteQuery."""
    sentinel = object()
    tr = TableRef("users", sentinel)  # type: ignore[arg-type]
    query = tr.delete()
    assert query._pool is sentinel
