"""Microbenchmarks for query building performance.

This module benchmarks the SQL generation phase of query building,
which doesn't require a database connection.

Run with: python -m benchmarks.bench_query_building
Or with pytest-benchmark: pytest benchmarks/ --benchmark-only
"""

from __future__ import annotations

import statistics
import time
from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING, Any

from dribble import Field, Table, and_, eq, gt, in_, like, lt, lte, or_
from dribble.fields import Integer, Serial, Text, Timestamp, Varchar
from dribble.query.builder import DeleteQuery, InsertQuery, SelectQuery, UpdateQuery

if TYPE_CHECKING:
    from pytest_benchmark.fixture import BenchmarkFixture


# Sample table definitions for benchmarking
class User(Table, table_name="users"):
    id: int = Field(Serial(), primary_key=True)
    name: str = Field(Varchar(255))
    email: str = Field(Varchar(255), unique=True)
    age: int = Field(Integer(), nullable=True)
    bio: str = Field(Text(), nullable=True)
    created_at: datetime = Field(Timestamp(), default="now()")


class Post(Table, table_name="posts"):
    id: int = Field(Serial(), primary_key=True)
    title: str = Field(Varchar(255))
    content: str = Field(Text())
    author_id: int = Field(Integer())
    views: int = Field(Integer(), default=0)
    created_at: datetime = Field(Timestamp(), default="now()")


class Comment(Table, table_name="comments"):
    id: int = Field(Serial(), primary_key=True)
    post_id: int = Field(Integer())
    user_id: int = Field(Integer())
    content: str = Field(Text())
    created_at: datetime = Field(Timestamp(), default="now()")


def benchmark(func: Callable[[], Any], iterations: int = 10000) -> dict[str, Any]:
    """Run a benchmark and return timing statistics."""
    times: list[float] = []

    # Warmup
    for _ in range(100):
        func()

    # Actual benchmark
    for _ in range(iterations):
        start = time.perf_counter_ns()
        func()
        end = time.perf_counter_ns()
        times.append((end - start) / 1000)  # Convert to microseconds

    return {
        "iterations": iterations,
        "mean_us": statistics.mean(times),
        "median_us": statistics.median(times),
        "stdev_us": statistics.stdev(times) if len(times) > 1 else 0,
        "min_us": min(times),
        "max_us": max(times),
        "ops_per_sec": 1_000_000 / statistics.mean(times),
    }


# =============================================================================
# SELECT simple: SELECT * FROM users
# =============================================================================


def bench_select_simple() -> tuple[str, list[Any]]:
    """ORM: SELECT * FROM users"""
    query = SelectQuery[User](None, (User,))
    return query.build()


def bench_native_select_simple() -> tuple[str, list[Any]]:
    """Native: SELECT * FROM users"""
    sql = "SELECT users.* FROM users"
    params: list[Any] = []
    return sql, params


# =============================================================================
# SELECT columns: SELECT id, name, email FROM users
# =============================================================================


def bench_select_columns() -> tuple[str, list[Any]]:
    """ORM: SELECT id, name, email FROM users"""
    query = SelectQuery[User](None, (User.c.id, User.c.name, User.c.email))
    query.from_(User)
    return query.build()


# =============================================================================
# SELECT WHERE =: SELECT * FROM users WHERE id = $1
# =============================================================================


def bench_select_where_simple() -> tuple[str, list[Any]]:
    """ORM: SELECT * FROM users WHERE id = $1"""
    query = SelectQuery[User](None, (User,))
    query.where(eq(User.c.id, 1))
    return query.build()


def bench_native_select_where_simple() -> tuple[str, list[Any]]:
    """Native: SELECT * FROM users WHERE id = $1"""
    params: list[Any] = []
    params.append(1)
    sql = f"SELECT users.* FROM users WHERE (users.id = ${len(params)})"
    return sql, params


# =============================================================================
# SELECT WHERE AND: SELECT * FROM users WHERE name = $1 AND age > $2
# =============================================================================


def bench_select_where_and() -> tuple[str, list[Any]]:
    """ORM: SELECT * FROM users WHERE name = $1 AND age > $2"""
    query = SelectQuery[User](None, (User,))
    query.where(and_(eq(User.c.name, "Alice"), gt(User.c.age, 18)))
    return query.build()


def bench_native_select_where_and() -> tuple[str, list[Any]]:
    """Native: SELECT * FROM users WHERE name = $1 AND age > $2"""
    params: list[Any] = []
    params.append("Alice")
    p1 = f"${len(params)}"
    params.append(18)
    p2 = f"${len(params)}"
    sql = f"SELECT users.* FROM users WHERE ((users.name = {p1}) AND (users.age > {p2}))"
    return sql, params


# =============================================================================
# SELECT WHERE complex: nested AND/OR conditions
# =============================================================================


def bench_select_where_complex() -> tuple[str, list[Any]]:
    """ORM: SELECT with complex nested AND/OR conditions"""
    query = SelectQuery[User](None, (User,))
    query.where(
        and_(
            or_(eq(User.c.name, "Alice"), eq(User.c.name, "Bob")),
            gt(User.c.age, 18),
            lt(User.c.age, 65),
            or_(
                like(User.c.email, "%@gmail.com"),
                like(User.c.email, "%@yahoo.com"),
            ),
        )
    )
    return query.build()


def bench_native_select_where_complex() -> tuple[str, list[Any]]:
    """Native: SELECT with complex nested AND/OR conditions"""
    params: list[Any] = []
    params.append("Alice")
    p1 = f"${len(params)}"
    params.append("Bob")
    p2 = f"${len(params)}"
    params.append(18)
    p3 = f"${len(params)}"
    params.append(65)
    p4 = f"${len(params)}"
    params.append("%@gmail.com")
    p5 = f"${len(params)}"
    params.append("%@yahoo.com")
    p6 = f"${len(params)}"
    sql = (
        f"SELECT users.* FROM users WHERE "
        f"(((users.name = {p1}) OR (users.name = {p2})) AND "
        f"(users.age > {p3}) AND (users.age < {p4}) AND "
        f"((users.email LIKE {p5}) OR (users.email LIKE {p6})))"
    )
    return sql, params


# =============================================================================
# SELECT WHERE IN (10): SELECT * FROM users WHERE id IN ($1, ..., $10)
# =============================================================================


def bench_select_where_in() -> tuple[str, list[Any]]:
    """ORM: SELECT * FROM users WHERE id IN ($1, $2, ..., $10)"""
    query = SelectQuery[User](None, (User,))
    query.where(in_(User.c.id, list(range(1, 11))))
    return query.build()


def bench_native_select_where_in() -> tuple[str, list[Any]]:
    """Native: SELECT * FROM users WHERE id IN ($1, $2, ..., $10)"""
    params: list[Any] = []
    placeholders: list[str] = []
    for i in range(1, 11):
        params.append(i)
        placeholders.append(f"${len(params)}")
    sql = f"SELECT users.* FROM users WHERE (users.id IN ({', '.join(placeholders)}))"
    return sql, params


# =============================================================================
# SELECT WHERE IN (100): SELECT * FROM users WHERE id IN ($1, ..., $100)
# =============================================================================


def bench_select_where_in_large() -> tuple[str, list[Any]]:
    """ORM: SELECT * FROM users WHERE id IN ($1, $2, ..., $100)"""
    query = SelectQuery[User](None, (User,))
    query.where(in_(User.c.id, list(range(1, 101))))
    return query.build()


def bench_native_select_where_in_large() -> tuple[str, list[Any]]:
    """Native: SELECT * FROM users WHERE id IN ($1, $2, ..., $100)"""
    params: list[Any] = []
    placeholders: list[str] = []
    for i in range(1, 101):
        params.append(i)
        placeholders.append(f"${len(params)}")
    sql = f"SELECT users.* FROM users WHERE (users.id IN ({', '.join(placeholders)}))"
    return sql, params


# =============================================================================
# SELECT ORDER/LIMIT: SELECT * FROM users ORDER BY created_at DESC LIMIT 10 OFFSET 20
# =============================================================================


def bench_select_order_limit() -> tuple[str, list[Any]]:
    """ORM: SELECT * FROM users ORDER BY created_at DESC LIMIT 10 OFFSET 20"""
    query = SelectQuery[User](None, (User,))
    query.order_by(User.c.created_at, "DESC")
    query.limit(10)
    query.offset(20)
    return query.build()


# =============================================================================
# SELECT JOIN: SELECT with INNER JOIN
# =============================================================================


def bench_select_join() -> tuple[str, list[Any]]:
    """ORM: SELECT with INNER JOIN"""
    query = SelectQuery[Post](None, (Post, User.c.name))
    query.from_(Post)
    query.inner_join(User, eq(Post.c.author_id, User.c.id))
    return query.build()


def bench_native_select_join() -> tuple[str, list[Any]]:
    """Native: SELECT with INNER JOIN"""
    params: list[Any] = []
    sql = "SELECT posts.*, users.name FROM posts INNER JOIN users ON (posts.author_id = users.id)"
    return sql, params


# =============================================================================
# SELECT multi-JOIN: SELECT with multiple JOINs
# =============================================================================


def bench_select_multi_join() -> tuple[str, list[Any]]:
    """ORM: SELECT with multiple JOINs"""
    query = SelectQuery[Comment](None, (Comment, Post.c.title, User.c.name))
    query.from_(Comment)
    query.inner_join(Post, eq(Comment.c.post_id, Post.c.id))
    query.inner_join(User, eq(Comment.c.user_id, User.c.id))
    return query.build()


# =============================================================================
# SELECT full query: SELECT with WHERE, JOIN, ORDER BY, LIMIT
# =============================================================================


def bench_select_full() -> tuple[str, list[Any]]:
    """ORM: SELECT with WHERE, JOIN, ORDER BY, LIMIT"""
    query = SelectQuery[Post](None, (Post, User.c.name))
    query.from_(Post)
    query.inner_join(User, eq(Post.c.author_id, User.c.id))
    query.where(and_(gt(Post.c.views, 100), lte(Post.c.views, 10000)))
    query.order_by(Post.c.views, "DESC")
    query.limit(20)
    return query.build()


def bench_native_select_full() -> tuple[str, list[Any]]:
    """Native: SELECT with WHERE, JOIN, ORDER BY, LIMIT"""
    params: list[Any] = []
    params.append(100)
    p1 = f"${len(params)}"
    params.append(10000)
    p2 = f"${len(params)}"
    sql = (
        f"SELECT posts.*, users.name FROM posts "
        f"INNER JOIN users ON (posts.author_id = users.id) "
        f"WHERE ((posts.views > {p1}) AND (posts.views <= {p2})) "
        f"ORDER BY posts.views DESC LIMIT 20"
    )
    return sql, params


# =============================================================================
# INSERT simple: INSERT with 2 columns
# =============================================================================


def bench_insert_simple() -> tuple[str, list[Any]]:
    """ORM: INSERT with 2 columns"""
    query = InsertQuery[User](None, User)
    query.values(name="Alice", email="alice@example.com")
    return query.build()


def bench_native_insert_simple() -> tuple[str, list[Any]]:
    """Native: INSERT with 2 columns"""
    params: list[Any] = []
    params.append("Alice")
    params.append("alice@example.com")
    sql = "INSERT INTO users (name, email) VALUES ($1, $2)"
    return sql, params


# =============================================================================
# INSERT many columns: INSERT with all columns
# =============================================================================


def bench_insert_many_columns() -> tuple[str, list[Any]]:
    """ORM: INSERT with all columns"""
    query = InsertQuery[User](None, User)
    query.values(
        name="Alice",
        email="alice@example.com",
        age=30,
        bio="Software engineer",
    )
    return query.build()


def bench_native_insert_many_columns() -> tuple[str, list[Any]]:
    """Native: INSERT with all columns"""
    params: list[Any] = []
    params.extend(["Alice", "alice@example.com", 30, "Software engineer"])
    sql = "INSERT INTO users (name, email, age, bio) VALUES ($1, $2, $3, $4)"
    return sql, params


# =============================================================================
# INSERT RETURNING: INSERT ... RETURNING *
# =============================================================================


def bench_insert_returning() -> tuple[str, list[Any]]:
    """ORM: INSERT ... RETURNING *"""
    query = InsertQuery[User](None, User)
    query.values(name="Alice", email="alice@example.com")
    query.returning(User)
    return query.build()


# =============================================================================
# UPDATE simple: UPDATE with single SET and WHERE
# =============================================================================


def bench_update_simple() -> tuple[str, list[Any]]:
    """ORM: UPDATE with single SET and WHERE"""
    query = UpdateQuery[User](None, User)
    query.set(name="Bob")
    query.where(eq(User.c.id, 1))
    return query.build()


def bench_native_update_simple() -> tuple[str, list[Any]]:
    """Native: UPDATE with single SET and WHERE"""
    params: list[Any] = []
    params.append("Bob")
    p1 = f"${len(params)}"
    params.append(1)
    p2 = f"${len(params)}"
    sql = f"UPDATE users SET name = {p1} WHERE (users.id = {p2})"
    return sql, params


# =============================================================================
# UPDATE many columns: UPDATE with multiple SET columns
# =============================================================================


def bench_update_many_columns() -> tuple[str, list[Any]]:
    """ORM: UPDATE with multiple SET columns"""
    query = UpdateQuery[User](None, User)
    query.set(name="Bob", email="bob@example.com", age=25, bio="Updated bio")
    query.where(eq(User.c.id, 1))
    return query.build()


# =============================================================================
# UPDATE complex WHERE: UPDATE with complex WHERE
# =============================================================================


def bench_update_complex_where() -> tuple[str, list[Any]]:
    """ORM: UPDATE with complex WHERE"""
    query = UpdateQuery[User](None, User)
    query.set(age=0)
    query.where(
        and_(
            gt(User.c.age, 100),
            or_(eq(User.c.name, "Test"), like(User.c.email, "%@test.com")),
        )
    )
    return query.build()


# =============================================================================
# DELETE simple: DELETE with simple WHERE
# =============================================================================


def bench_delete_simple() -> tuple[str, list[Any]]:
    """ORM: DELETE with simple WHERE"""
    query = DeleteQuery[User](None, User)
    query.where(eq(User.c.id, 1))
    return query.build()


def bench_native_delete_simple() -> tuple[str, list[Any]]:
    """Native: DELETE with simple WHERE"""
    params: list[Any] = []
    params.append(1)
    sql = f"DELETE FROM users WHERE (users.id = ${len(params)})"
    return sql, params


# =============================================================================
# DELETE complex WHERE: DELETE with complex WHERE
# =============================================================================


def bench_delete_complex_where() -> tuple[str, list[Any]]:
    """ORM: DELETE with complex WHERE"""
    query = DeleteQuery[User](None, User)
    query.where(
        and_(
            lt(User.c.created_at, "2020-01-01"),
            or_(eq(User.c.age, None), lt(User.c.age, 0)),
        )
    )
    return query.build()


# =============================================================================
# DDL Generation
# =============================================================================


def bench_ddl_simple() -> str:
    """ORM: Generate DDL for simple table"""
    return User.to_ddl()


def bench_ddl_complex() -> str:
    """ORM: Generate DDL for table with more columns"""
    return Post.to_ddl()


# =============================================================================
# Main Runner
# =============================================================================


def run_benchmarks():
    """Run all benchmarks and print results."""
    benchmarks = [
        # SELECT benchmarks
        ("SELECT simple", bench_select_simple),
        ("SELECT columns", bench_select_columns),
        ("SELECT WHERE =", bench_select_where_simple),
        ("SELECT WHERE AND", bench_select_where_and),
        ("SELECT WHERE complex", bench_select_where_complex),
        ("SELECT WHERE IN (10)", bench_select_where_in),
        ("SELECT WHERE IN (100)", bench_select_where_in_large),
        ("SELECT ORDER/LIMIT", bench_select_order_limit),
        ("SELECT JOIN", bench_select_join),
        ("SELECT multi-JOIN", bench_select_multi_join),
        ("SELECT full query", bench_select_full),
        # INSERT benchmarks
        ("INSERT simple", bench_insert_simple),
        ("INSERT many cols", bench_insert_many_columns),
        ("INSERT RETURNING", bench_insert_returning),
        # UPDATE benchmarks
        ("UPDATE simple", bench_update_simple),
        ("UPDATE many cols", bench_update_many_columns),
        ("UPDATE complex WHERE", bench_update_complex_where),
        # DELETE benchmarks
        ("DELETE simple", bench_delete_simple),
        ("DELETE complex WHERE", bench_delete_complex_where),
        # DDL benchmarks
        ("DDL simple table", bench_ddl_simple),
        ("DDL complex table", bench_ddl_complex),
    ]

    print("=" * 80)
    print("Dribble ORM Query Building Benchmarks")
    print("=" * 80)
    print()
    print(f"{'Benchmark':<25} {'Mean (μs)':>12} {'Median (μs)':>12} {'Ops/sec':>12}")
    print("-" * 80)

    for name, func in benchmarks:
        results = benchmark(func, iterations=10000)
        print(
            f"{name:<25} {results['mean_us']:>12.2f} {results['median_us']:>12.2f} "
            f"{results['ops_per_sec']:>12,.0f}"
        )

    print("-" * 80)
    print()
    print("Legend: μs = microseconds, Ops/sec = operations per second")
    print()

    # Run comparison benchmarks
    run_comparison_benchmarks()


def run_comparison_benchmarks():
    """Run side-by-side comparison of ORM vs native SQL."""
    comparisons = [
        ("SELECT simple", bench_select_simple, bench_native_select_simple),
        ("SELECT WHERE =", bench_select_where_simple, bench_native_select_where_simple),
        ("SELECT WHERE AND", bench_select_where_and, bench_native_select_where_and),
        ("SELECT WHERE complex", bench_select_where_complex, bench_native_select_where_complex),
        ("SELECT WHERE IN (10)", bench_select_where_in, bench_native_select_where_in),
        ("SELECT WHERE IN (100)", bench_select_where_in_large, bench_native_select_where_in_large),
        ("SELECT JOIN", bench_select_join, bench_native_select_join),
        ("SELECT full query", bench_select_full, bench_native_select_full),
        ("INSERT simple", bench_insert_simple, bench_native_insert_simple),
        ("INSERT many cols", bench_insert_many_columns, bench_native_insert_many_columns),
        ("UPDATE simple", bench_update_simple, bench_native_update_simple),
        ("DELETE simple", bench_delete_simple, bench_native_delete_simple),
    ]

    print("=" * 95)
    print("ORM vs Native SQL Comparison")
    print("=" * 95)
    print()
    print(f"{'Benchmark':<25} {'ORM (μs)':>12} {'Native (μs)':>12} {'Overhead':>12} {'Ratio':>10}")
    print("-" * 95)

    for name, orm_func, native_func in comparisons:
        orm_results = benchmark(orm_func, iterations=10000)
        native_results = benchmark(native_func, iterations=10000)

        orm_mean = orm_results["mean_us"]
        native_mean = native_results["mean_us"]
        overhead = orm_mean - native_mean
        ratio = orm_mean / native_mean if native_mean > 0 else float("inf")

        print(
            f"{name:<25} {orm_mean:>12.2f} {native_mean:>12.2f} {overhead:>+12.2f} {ratio:>10.1f}x"
        )

    print("-" * 95)
    print()
    print("Legend: Overhead = ORM - Native (μs), Ratio = ORM / Native")
    print("        Lower overhead and ratio closer to 1.0x is better")
    print()


# =============================================================================
# Pytest-benchmark compatible functions
# =============================================================================

# ORM benchmarks


def test_select_simple(benchmark: BenchmarkFixture) -> None:
    benchmark(bench_select_simple)


def test_select_where_simple(benchmark: BenchmarkFixture) -> None:
    benchmark(bench_select_where_simple)


def test_select_where_complex(benchmark: BenchmarkFixture) -> None:
    benchmark(bench_select_where_complex)


def test_select_where_in_large(benchmark: BenchmarkFixture) -> None:
    benchmark(bench_select_where_in_large)


def test_select_join(benchmark: BenchmarkFixture) -> None:
    benchmark(bench_select_join)


def test_select_full(benchmark: BenchmarkFixture) -> None:
    benchmark(bench_select_full)


def test_insert_simple(benchmark: BenchmarkFixture) -> None:
    benchmark(bench_insert_simple)


def test_update_simple(benchmark: BenchmarkFixture) -> None:
    benchmark(bench_update_simple)


def test_delete_simple(benchmark: BenchmarkFixture) -> None:
    benchmark(bench_delete_simple)


def test_ddl_simple(benchmark: BenchmarkFixture) -> None:
    benchmark(bench_ddl_simple)


# Native SQL benchmarks (for comparison)


def test_native_select_simple(benchmark: BenchmarkFixture) -> None:
    benchmark(bench_native_select_simple)


def test_native_select_where_simple(benchmark: BenchmarkFixture) -> None:
    benchmark(bench_native_select_where_simple)


def test_native_select_where_complex(benchmark: BenchmarkFixture) -> None:
    benchmark(bench_native_select_where_complex)


def test_native_select_where_in_large(benchmark: BenchmarkFixture) -> None:
    benchmark(bench_native_select_where_in_large)


def test_native_select_join(benchmark: BenchmarkFixture) -> None:
    benchmark(bench_native_select_join)


def test_native_select_full(benchmark: BenchmarkFixture) -> None:
    benchmark(bench_native_select_full)


def test_native_insert_simple(benchmark: BenchmarkFixture) -> None:
    benchmark(bench_native_insert_simple)


def test_native_update_simple(benchmark: BenchmarkFixture) -> None:
    benchmark(bench_native_update_simple)


def test_native_delete_simple(benchmark: BenchmarkFixture) -> None:
    benchmark(bench_native_delete_simple)


if __name__ == "__main__":
    run_benchmarks()
