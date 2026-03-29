"""Isolated query compilation benchmark — no database round-trip.

Measures the time to build SQL strings and parameter lists from
the ORM query builder, compared to hand-written f-strings producing
the same SQL.

Run with: python -m benchmarks.bench_compile
"""

from __future__ import annotations

import statistics
import time
from typing import Any

from derp.orm import (
    Field,
    Integer,
    Nullable,
    Serial,
    Table,
    Text,
    Timestamp,
    Varchar,
)


class User(Table, table="users"):
    id: Serial = Field(primary=True)
    name: Varchar[255] = Field()
    email: Varchar[255] = Field()
    age: Nullable[Integer] = Field()
    bio: Nullable[Text] = Field()
    created_at: Timestamp = Field(default="now()")


class Post(Table, table="posts"):
    id: Serial = Field(primary=True)
    title: Varchar[255] = Field()
    content: Text = Field()
    author_id: Integer = Field()
    views: Integer = Field(default=0)
    created_at: Timestamp = Field(default="now()")


def benchmark_sync(
    func: Any,
    iterations: int = 10000,
    warmup: int = 100,
) -> dict[str, Any]:
    """Run a synchronous benchmark and return timing statistics."""
    times: list[float] = []

    for _ in range(warmup):
        func()

    for _ in range(iterations):
        start = time.perf_counter_ns()
        func()
        end = time.perf_counter_ns()
        times.append((end - start) / 1000)

    return {
        "iterations": iterations,
        "mean_us": statistics.mean(times),
        "median_us": statistics.median(times),
        "stdev_us": statistics.stdev(times) if len(times) > 1 else 0,
        "min_us": min(times),
        "max_us": max(times),
        "ops_per_sec": 1_000_000 / statistics.mean(times),
    }


# ── ORM query compilation ────────────────────────────────────────


def compile_select_simple() -> tuple[str, list[Any]]:
    """Build: SELECT users.* FROM users"""
    # DatabaseEngine not connected — build() doesn't need a pool.
    from derp.orm.query.builder import SelectQuery

    q: SelectQuery[User] = SelectQuery(None, (User,))
    q._from_table = User
    return q.build()


def compile_select_where() -> tuple[str, list[Any]]:
    from derp.orm.query.builder import SelectQuery

    q: SelectQuery[User] = SelectQuery(None, (User,))
    q._from_table = User
    q._where_clause = User.id == 1
    return q.build()


def compile_select_join() -> tuple[str, list[Any]]:
    from derp.orm.query.builder import (
        JoinClause,
        JoinType,
        SelectQuery,
    )

    q: SelectQuery[Any] = SelectQuery(None, (Post, User.name))
    q._from_table = Post
    q._joins.append(
        JoinClause(
            table=User,
            condition=Post.author_id == User.id,
            join_type=JoinType.INNER,
        )
    )
    return q.build()


def compile_insert() -> tuple[str, list[Any]]:
    from derp.orm.query.builder import InsertQuery

    q: InsertQuery[User] = InsertQuery(None, User)
    q._values = {"name": "Alice", "email": "alice@example.com"}
    return q.build()


# ── Hand-written f-string baselines ──────────────────────────────


def baseline_select_simple() -> tuple[str, list[Any]]:
    return "SELECT users.* FROM users", []


def baseline_select_where() -> tuple[str, list[Any]]:
    return "SELECT users.* FROM users WHERE (users.id = $1)", [1]


def baseline_select_join() -> tuple[str, list[Any]]:
    return (
        "SELECT posts.*, users.name FROM posts "
        "INNER JOIN users ON (posts.author_id = users.id)"
    ), []


def baseline_insert() -> tuple[str, list[Any]]:
    return (
        "INSERT INTO users (name, email) VALUES ($1, $2)",
        ["Alice", "alice@example.com"],
    )


# ── Runner ───────────────────────────────────────────────────────


def main() -> None:
    print("=" * 80)
    print("Query Compilation Benchmark (no DB round-trip)")
    print("=" * 80)
    print()
    print(f"{'Operation':<30} {'ORM (μs)':>12} {'f-string (μs)':>14} {'Ratio':>10}")
    print("-" * 80)

    pairs = [
        ("SELECT simple", compile_select_simple, baseline_select_simple),
        ("SELECT WHERE", compile_select_where, baseline_select_where),
        ("SELECT JOIN", compile_select_join, baseline_select_join),
        ("INSERT", compile_insert, baseline_insert),
    ]

    for name, orm_fn, baseline_fn in pairs:
        orm = benchmark_sync(orm_fn)
        base = benchmark_sync(baseline_fn)
        ratio = (
            orm["median_us"] / base["median_us"]
            if base["median_us"] > 0
            else float("inf")
        )
        print(
            f"{name:<30} {orm['median_us']:>12.3f} "
            f"{base['median_us']:>14.3f} {ratio:>10.2f}x"
        )

    print("-" * 80)
    print()


if __name__ == "__main__":
    main()
