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

from dribble import Field, Table, and_, eq, gt, in_, like, lt, lte, or_
from dribble.fields import Integer, Serial, Text, Timestamp, Varchar
from dribble.query.builder import DeleteQuery, InsertQuery, SelectQuery, UpdateQuery


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


def benchmark(func: Callable[[], None], iterations: int = 10000) -> dict:
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
# SELECT Query Benchmarks
# =============================================================================


def bench_select_simple():
    """SELECT * FROM users"""
    query = SelectQuery[User](None, (User,))
    query.build()


def bench_select_columns():
    """SELECT id, name, email FROM users"""
    query = SelectQuery[User](None, (User.c.id, User.c.name, User.c.email))
    query.from_(User)
    query.build()


def bench_select_where_simple():
    """SELECT * FROM users WHERE id = $1"""
    query = SelectQuery[User](None, (User,))
    query.where(eq(User.c.id, 1))
    query.build()


def bench_select_where_and():
    """SELECT * FROM users WHERE name = $1 AND age > $2"""
    query = SelectQuery[User](None, (User,))
    query.where(and_(eq(User.c.name, "Alice"), gt(User.c.age, 18)))
    query.build()


def bench_select_where_complex():
    """SELECT with complex nested AND/OR conditions"""
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
    query.build()


def bench_select_where_in():
    """SELECT * FROM users WHERE id IN ($1, $2, ..., $10)"""
    query = SelectQuery[User](None, (User,))
    query.where(in_(User.c.id, list(range(1, 11))))
    query.build()


def bench_select_where_in_large():
    """SELECT * FROM users WHERE id IN ($1, $2, ..., $100)"""
    query = SelectQuery[User](None, (User,))
    query.where(in_(User.c.id, list(range(1, 101))))
    query.build()


def bench_select_order_limit():
    """SELECT * FROM users ORDER BY created_at DESC LIMIT 10 OFFSET 20"""
    query = SelectQuery[User](None, (User,))
    query.order_by(User.c.created_at, "DESC")
    query.limit(10)
    query.offset(20)
    query.build()


def bench_select_join():
    """SELECT with INNER JOIN"""
    query = SelectQuery[Post](None, (Post, User.c.name))
    query.from_(Post)
    query.inner_join(User, eq(Post.c.author_id, User.c.id))
    query.build()


def bench_select_multi_join():
    """SELECT with multiple JOINs"""
    query = SelectQuery[Comment](None, (Comment, Post.c.title, User.c.name))
    query.from_(Comment)
    query.inner_join(Post, eq(Comment.c.post_id, Post.c.id))
    query.inner_join(User, eq(Comment.c.user_id, User.c.id))
    query.build()


def bench_select_full():
    """SELECT with WHERE, JOIN, ORDER BY, LIMIT"""
    query = SelectQuery[Post](None, (Post, User.c.name))
    query.from_(Post)
    query.inner_join(User, eq(Post.c.author_id, User.c.id))
    query.where(and_(gt(Post.c.views, 100), lte(Post.c.views, 10000)))
    query.order_by(Post.c.views, "DESC")
    query.limit(20)
    query.build()


# =============================================================================
# INSERT Query Benchmarks
# =============================================================================


def bench_insert_simple():
    """INSERT with 2 columns"""
    query = InsertQuery[User](None, User)
    query.values(name="Alice", email="alice@example.com")
    query.build()


def bench_insert_many_columns():
    """INSERT with all columns"""
    query = InsertQuery[User](None, User)
    query.values(
        name="Alice",
        email="alice@example.com",
        age=30,
        bio="Software engineer",
    )
    query.build()


def bench_insert_returning():
    """INSERT ... RETURNING *"""
    query = InsertQuery[User](None, User)
    query.values(name="Alice", email="alice@example.com")
    query.returning(User)
    query.build()


# =============================================================================
# UPDATE Query Benchmarks
# =============================================================================


def bench_update_simple():
    """UPDATE with single SET and WHERE"""
    query = UpdateQuery[User](None, User)
    query.set(name="Bob")
    query.where(eq(User.c.id, 1))
    query.build()


def bench_update_many_columns():
    """UPDATE with multiple SET columns"""
    query = UpdateQuery[User](None, User)
    query.set(name="Bob", email="bob@example.com", age=25, bio="Updated bio")
    query.where(eq(User.c.id, 1))
    query.build()


def bench_update_complex_where():
    """UPDATE with complex WHERE"""
    query = UpdateQuery[User](None, User)
    query.set(age=0)
    query.where(
        and_(
            gt(User.c.age, 100),
            or_(eq(User.c.name, "Test"), like(User.c.email, "%@test.com")),
        )
    )
    query.build()


# =============================================================================
# DELETE Query Benchmarks
# =============================================================================


def bench_delete_simple():
    """DELETE with simple WHERE"""
    query = DeleteQuery[User](None, User)
    query.where(eq(User.c.id, 1))
    query.build()


def bench_delete_complex_where():
    """DELETE with complex WHERE"""
    query = DeleteQuery[User](None, User)
    query.where(
        and_(
            lt(User.c.created_at, "2020-01-01"),
            or_(eq(User.c.age, None), lt(User.c.age, 0)),
        )
    )
    query.build()


# =============================================================================
# DDL Generation Benchmarks
# =============================================================================


def bench_ddl_simple():
    """Generate DDL for simple table"""
    User.to_ddl()


def bench_ddl_complex():
    """Generate DDL for table with more columns"""
    Post.to_ddl()


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


# Pytest-benchmark compatible functions
def test_select_simple(benchmark):
    benchmark(bench_select_simple)


def test_select_where_simple(benchmark):
    benchmark(bench_select_where_simple)


def test_select_where_complex(benchmark):
    benchmark(bench_select_where_complex)


def test_select_where_in_large(benchmark):
    benchmark(bench_select_where_in_large)


def test_select_join(benchmark):
    benchmark(bench_select_join)


def test_select_full(benchmark):
    benchmark(bench_select_full)


def test_insert_simple(benchmark):
    benchmark(bench_insert_simple)


def test_update_simple(benchmark):
    benchmark(bench_update_simple)


def test_delete_simple(benchmark):
    benchmark(bench_delete_simple)


def test_ddl_simple(benchmark):
    benchmark(bench_ddl_simple)


if __name__ == "__main__":
    run_benchmarks()
