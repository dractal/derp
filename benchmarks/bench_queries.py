"""Microbenchmarks for async query execution performance.

This module benchmarks the actual database query execution time,
comparing Derp ORM performance against direct asyncpg usage.

Run with: python -m benchmarks.bench_queries [--database-url DATABASE_URL]
If --database-url is not provided, a temporary database will be created and cleaned up.
"""

from __future__ import annotations

import argparse
import asyncio
import random
import statistics
import string
import time
from collections.abc import Callable
from typing import Any

import asyncpg
import testing.postgresql as tp

from derp.orm import (
    DatabaseEngine,
    Field,
    Integer,
    L,
    Nullable,
    Serial,
    Table,
    Text,
    Timestamp,
    Varchar,
)


# Sample table definitions for benchmarking
class User(Table, table="users"):
    id: Serial = Field(primary=True)
    name: Varchar[L[255]] = Field()
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


class Comment(Table, table="comments"):
    id: Serial = Field(primary=True)
    post_id: Integer = Field()
    user_id: Integer = Field()
    content: Text = Field()
    created_at: Timestamp = Field(default="now()")


# Database setup and teardown
async def create_temp_database(base_dsn: str) -> tuple[str, str]:
    """Create a temporary database for benchmarking."""
    base_conn_str = _parse_base_dsn(base_dsn)
    postgres_dsn = f"{base_conn_str}/postgres"
    try:
        conn = await asyncpg.connect(postgres_dsn)
    except (OSError, asyncpg.exceptions.InvalidPasswordError) as e:
        error_msg = (
            f"Failed to connect to PostgreSQL at {postgres_dsn}. "
            f"Please ensure PostgreSQL is running and the connection "
            f"details are correct. Original error: {e}"
        )
        raise ConnectionError(error_msg) from e

    try:
        random_suffix = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=8)
        )
        db_name = f"derp_bench_{random_suffix}"
        await conn.execute(f'CREATE DATABASE "{db_name}"')
        full_dsn = f"{base_conn_str}/{db_name}"
        return db_name, full_dsn
    finally:
        await conn.close()


async def drop_database(base_dsn: str, db_name: str) -> None:
    """Drop a database."""
    base_conn_str = _parse_base_dsn(base_dsn)
    postgres_dsn = f"{base_conn_str}/postgres"
    try:
        conn = await asyncpg.connect(postgres_dsn)
    except (OSError, asyncpg.exceptions.InvalidPasswordError) as e:
        print(f"Warning: Could not connect to drop database: {e}")
        return

    try:
        await conn.execute(
            f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{db_name}'
            AND pid <> pg_backend_pid()
        """
        )
        await conn.execute(f'DROP DATABASE "{db_name}"')
    finally:
        await conn.close()


def _parse_base_dsn(dsn: str) -> str:
    """Extract the base connection string (without database name) from a DSN."""
    if "://" not in dsn:
        raise ValueError(
            "DSN must be in format: postgresql://user:password@host:port/database"
        )
    scheme_end = dsn.find("://") + 3
    path_start = dsn.find("/", scheme_end)
    if path_start != -1:
        return dsn[:path_start]
    return dsn


async def setup_schema(db: DatabaseEngine) -> None:
    """Create tables and indexes for benchmarking."""
    async with db.pool.acquire() as conn:
        for ddl in [User.to_ddl(), Post.to_ddl(), Comment.to_ddl()]:
            statements = [s.strip() for s in ddl.split(";") if s.strip()]
            for statement in statements:
                await conn.execute(statement)


async def seed_data(
    db: DatabaseEngine,
    num_users: int = 1000,
    num_posts: int = 5000,
    num_comments: int = 10000,
) -> None:
    """Seed database with test data using bulk inserts."""
    # Insert users in bulk
    user_rows = [
        {
            "name": f"User {i}",
            "email": f"user{i}@example.com",
            "age": random.randint(18, 80),
            "bio": f"Bio for user {i}" if i % 2 == 0 else None,
        }
        for i in range(num_users)
    ]
    await db.insert(User).values_list(user_rows).execute()

    # Insert posts in bulk
    post_rows = [
        {
            "title": f"Post {i}",
            "content": f"Content for post {i}",
            "author_id": random.randint(1, num_users),
            "views": random.randint(0, 10000),
        }
        for i in range(num_posts)
    ]
    await db.insert(Post).values_list(post_rows).execute()

    # Insert comments in bulk
    comment_rows = [
        {
            "post_id": random.randint(1, num_posts),
            "user_id": random.randint(1, num_users),
            "content": f"Comment {i}",
        }
        for i in range(num_comments)
    ]
    await db.insert(Comment).values_list(comment_rows).execute()


async def benchmark_async(
    func: Callable[[], Any], iterations: int = 100, warmup: int = 10
) -> dict[str, Any]:
    """Run an async benchmark and return timing statistics."""
    times: list[float] = []

    # Warmup
    for _ in range(warmup):
        await func()

    # Actual benchmark
    for _ in range(iterations):
        start = time.perf_counter_ns()
        await func()
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
# SELECT benchmarks
# =============================================================================


async def bench_select_simple_orm(db: DatabaseEngine) -> None:
    """ORM: SELECT * FROM users"""
    _ = await db.select(User).execute()


async def bench_select_simple_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: SELECT * FROM users"""
    async with pool.acquire() as conn:
        _ = await conn.fetch("SELECT * FROM users")


async def bench_select_where_orm(db: DatabaseEngine) -> None:
    """ORM: SELECT * FROM users WHERE id = $1"""
    _ = await db.select(User).where(User.id == 1).execute()


async def bench_select_where_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: SELECT * FROM users WHERE id = $1"""
    async with pool.acquire() as conn:
        _ = await conn.fetch("SELECT * FROM users WHERE id = $1", 1)


async def bench_select_where_and_orm(db: DatabaseEngine) -> None:
    """ORM: SELECT * FROM users WHERE name = $1 AND age > $2"""
    _ = await db.select(User).where((User.name == "User 1") & (User.age > 18)).execute()


async def bench_select_where_and_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: SELECT * FROM users WHERE name = $1 AND age > $2"""
    async with pool.acquire() as conn:
        _ = await conn.fetch(
            "SELECT * FROM users WHERE name = $1 AND age > $2", "User 1", 18
        )


async def bench_select_where_complex_orm(db: DatabaseEngine) -> None:
    """ORM: SELECT with complex nested AND/OR conditions"""
    _ = await (
        db.select(User)
        .where(
            ((User.name == "User 1") | (User.name == "User 2"))
            & (User.age > 18)
            & (User.age < 65)
            & ((User.email.ilike("%@example.com")) | (User.email.ilike("%@test.com")))
        )
        .execute()
    )


async def bench_select_where_complex_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: SELECT with complex nested AND/OR conditions"""
    async with pool.acquire() as conn:
        query = (
            "SELECT * FROM users WHERE ((name = $1) OR (name = $2)) "
            "AND (age > $3) AND (age < $4) AND ((email LIKE $5) OR (email LIKE $6))"
        )
        _ = await conn.fetch(
            query, "User 1", "User 2", 18, 65, "%@example.com", "%@test.com"
        )


async def bench_select_where_in_orm(db: DatabaseEngine) -> None:
    """ORM: SELECT * FROM users WHERE id IN ($1, ..., $10)"""
    ids = list(range(1, 11))
    _ = await db.select(User).where(User.id.in_(ids)).execute()


async def bench_select_where_in_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: SELECT * FROM users WHERE id IN ($1, ..., $10)"""
    ids = list(range(1, 11))
    placeholders = ",".join(f"${i + 1}" for i in range(len(ids)))
    async with pool.acquire() as conn:
        _ = await conn.fetch(f"SELECT * FROM users WHERE id IN ({placeholders})", *ids)


async def bench_select_join_orm(db: DatabaseEngine) -> None:
    """ORM: SELECT with INNER JOIN"""
    _ = await (
        db.select(Post, User.name)
        .from_(Post)
        .inner_join(User, Post.author_id == User.id)
        .execute()
    )


async def bench_select_join_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: SELECT with INNER JOIN"""
    async with pool.acquire() as conn:
        query = (
            "SELECT posts.*, users.name FROM posts "
            "INNER JOIN users ON (posts.author_id = users.id)"
        )
        _ = await conn.fetch(query)


async def bench_select_full_orm(db: DatabaseEngine) -> None:
    """ORM: SELECT with WHERE, JOIN, ORDER BY, LIMIT"""
    _ = await (
        db.select(Post, User.name)
        .from_(Post)
        .inner_join(User, Post.author_id == User.id)
        .where((Post.views > 100) & (Post.views <= 10000))
        .order_by(Post.views, asc=False)
        .limit(20)
        .execute()
    )


async def bench_select_full_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: SELECT with WHERE, JOIN, ORDER BY, LIMIT"""
    async with pool.acquire() as conn:
        query = (
            "SELECT posts.*, users.name FROM posts "
            "INNER JOIN users ON (posts.author_id = users.id) "
            "WHERE ((posts.views > $1) AND (posts.views <= $2)) "
            "ORDER BY posts.views DESC LIMIT 20"
        )
        _ = await conn.fetch(query, 100, 10000)


# =============================================================================
# AGGREGATE benchmarks
# =============================================================================


async def bench_aggregate_count_orm(db: DatabaseEngine) -> None:
    """ORM: SELECT COUNT(*) FROM users WHERE age > $1"""
    _ = await db.select(User.id.count()).from_(User).where(User.age > 18).execute()


async def bench_aggregate_count_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: SELECT COUNT(*) FROM users WHERE age > $1"""
    async with pool.acquire() as conn:
        _ = await conn.fetch(
            "SELECT COUNT(users.id) FROM users WHERE (users.age > $1)", 18
        )


async def bench_aggregate_sum_group_orm(db: DatabaseEngine) -> None:
    """ORM: SELECT author_id, SUM(views) FROM posts GROUP BY author_id"""
    _ = await (
        db.select(Post.author_id, Post.views.sum())
        .from_(Post)
        .group_by(Post.author_id)
        .execute()
    )


async def bench_aggregate_sum_group_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: SELECT author_id, SUM(views) FROM posts GROUP BY author_id"""
    async with pool.acquire() as conn:
        _ = await conn.fetch(
            "SELECT posts.author_id, SUM(posts.views) "
            "FROM posts GROUP BY posts.author_id"
        )


# =============================================================================
# INSERT benchmarks
# =============================================================================


async def bench_insert_simple_orm(db: DatabaseEngine) -> None:
    """ORM: INSERT with 2 columns"""
    email = f"test{random.randint(1000000, 9999999)}@example.com"
    _ = await db.insert(User).values(name="Test User", email=email).execute()


async def bench_insert_simple_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: INSERT with 2 columns"""
    email = f"test{random.randint(1000000, 9999999)}@example.com"
    async with pool.acquire() as conn:
        _ = await conn.fetchrow(
            "INSERT INTO users (name, email) VALUES ($1, $2)", "Test User", email
        )


async def bench_insert_returning_orm(db: DatabaseEngine) -> None:
    """ORM: INSERT ... RETURNING *"""
    email = f"test{random.randint(10000, 99999)}@example.com"
    _ = await (
        db.insert(User).values(name="Test User", email=email).returning(User).execute()
    )


async def bench_insert_returning_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: INSERT ... RETURNING *"""
    async with pool.acquire() as conn:
        _ = await conn.fetchrow(
            "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING *",
            "Test User",
            f"test{random.randint(10000, 99999)}@example.com",
        )


# =============================================================================
# BULK INSERT benchmarks
# =============================================================================


async def bench_bulk_insert_orm(db: DatabaseEngine, n: int) -> None:
    """ORM: Bulk INSERT via values_list()"""
    rows = [
        {
            "name": f"Bulk {i}",
            "email": f"bulk{random.randint(1000000, 9999999)}_{i}@example.com",
        }
        for i in range(n)
    ]
    await db.insert(User).values_list(rows).execute()


async def bench_bulk_insert_asyncpg(pool: asyncpg.Pool, n: int) -> None:
    """AsyncPG: Bulk INSERT via single multi-row VALUES statement"""
    rows = [
        (f"Bulk {i}", f"bulk{random.randint(1000000, 9999999)}_{i}@example.com")
        for i in range(n)
    ]
    placeholders = ", ".join(f"(${i * 2 + 1}, ${i * 2 + 2})" for i in range(n))
    params = [v for row in rows for v in row]
    async with pool.acquire() as conn:
        await conn.execute(
            f"INSERT INTO users (name, email) VALUES {placeholders}", *params
        )


# =============================================================================
# UPDATE benchmarks
# =============================================================================


async def bench_update_simple_orm(db: DatabaseEngine) -> None:
    """ORM: UPDATE with single SET and WHERE"""
    _ = await db.update(User).set(name="Updated Name").where(User.id == 1).execute()


async def bench_update_simple_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: UPDATE with single SET and WHERE"""
    async with pool.acquire() as conn:
        _ = await conn.execute(
            "UPDATE users SET name = $1 WHERE (users.id = $2)", "Updated Name", 1
        )


async def bench_update_many_columns_orm(db: DatabaseEngine) -> None:
    """ORM: UPDATE with multiple SET columns"""
    email = f"updated{random.randint(10000, 99999)}@example.com"
    _ = await (
        db.update(User)
        .set(name="Updated Name", email=email, age=25, bio="Updated bio")
        .where(User.id == 1)
        .execute()
    )


async def bench_update_many_columns_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: UPDATE with multiple SET columns"""
    async with pool.acquire() as conn:
        query = (
            "UPDATE users SET name = $1, email = $2, age = $3, bio = $4 "
            "WHERE (users.id = $5)"
        )
        _ = await conn.execute(
            query,
            "Updated Name",
            f"updated{random.randint(10000, 99999)}@example.com",
            25,
            "Updated bio",
            1,
        )


# =============================================================================
# DELETE benchmarks
# =============================================================================


async def bench_insert_delete_simple_orm(db: DatabaseEngine) -> None:
    """ORM: DELETE with simple WHERE"""
    email = f"delete{random.randint(100000, 999999)}@example.com"
    user = (
        await db.insert(User)
        .values(name="Delete Me", email=email)
        .returning(User)
        .execute()
    )
    _ = await db.delete(User).where(User.id == user.id).execute()


async def bench_insert_delete_simple_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: DELETE with simple WHERE"""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
            "Delete Me",
            f"delete{random.randint(100000, 999999)}@example.com",
        )
        _ = await conn.execute("DELETE FROM users WHERE (users.id = $1)", row["id"])


# =============================================================================
# Main Runner
# =============================================================================


async def run_benchmarks(database_url: str | None = None) -> None:
    """Run all benchmarks and print results."""
    db: DatabaseEngine | None = None
    pg_temp: tp.Postgresql | None = None

    try:
        if database_url is None:
            pg_temp = tp.Postgresql(port=7654)
            database_url = pg_temp.url()

        db = DatabaseEngine(database_url)
        try:
            await db.connect()
        except (
            OSError,
            asyncpg.exceptions.InvalidPasswordError,
            asyncpg.exceptions.PostgresError,
        ) as e:
            print(f"Error connecting to database: {e}")
            print(f"\nFailed to connect to: {database_url}")
            print("Please check your database connection settings.")
            raise

        # Create schema
        print("Setting up schema...")
        await setup_schema(db)

        # Seed data
        print("Seeding data...")
        await seed_data(db)

        # Get asyncpg pool for baseline benchmarks
        pool = db.pool

        print("\n" + "=" * 95)
        print("Derp ORM vs AsyncPG Query Execution Benchmarks")
        print("=" * 95)
        print()
        print(
            f"{'Benchmark':<35} {'ORM (μs)':>12} {'AsyncPG (μs)':>14} "
            f"{'Overhead':>12} {'Ratio':>10}"
        )
        print("-" * 95)

        comparisons = [
            (
                "SELECT simple",
                lambda: bench_select_simple_orm(db),
                lambda: bench_select_simple_asyncpg(pool),
            ),
            (
                "SELECT WHERE =",
                lambda: bench_select_where_orm(db),
                lambda: bench_select_where_asyncpg(pool),
            ),
            (
                "SELECT WHERE AND",
                lambda: bench_select_where_and_orm(db),
                lambda: bench_select_where_and_asyncpg(pool),
            ),
            (
                "SELECT WHERE complex",
                lambda: bench_select_where_complex_orm(db),
                lambda: bench_select_where_complex_asyncpg(pool),
            ),
            (
                "SELECT WHERE IN (10)",
                lambda: bench_select_where_in_orm(db),
                lambda: bench_select_where_in_asyncpg(pool),
            ),
            (
                "SELECT JOIN",
                lambda: bench_select_join_orm(db),
                lambda: bench_select_join_asyncpg(pool),
            ),
            (
                "SELECT full query",
                lambda: bench_select_full_orm(db),
                lambda: bench_select_full_asyncpg(pool),
            ),
            (
                "AGG COUNT WHERE",
                lambda: bench_aggregate_count_orm(db),
                lambda: bench_aggregate_count_asyncpg(pool),
            ),
            (
                "AGG SUM GROUP BY",
                lambda: bench_aggregate_sum_group_orm(db),
                lambda: bench_aggregate_sum_group_asyncpg(pool),
            ),
            (
                "INSERT simple",
                lambda: bench_insert_simple_orm(db),
                lambda: bench_insert_simple_asyncpg(pool),
            ),
            (
                "INSERT RETURNING",
                lambda: bench_insert_returning_orm(db),
                lambda: bench_insert_returning_asyncpg(pool),
            ),
            (
                "UPDATE simple",
                lambda: bench_update_simple_orm(db),
                lambda: bench_update_simple_asyncpg(pool),
            ),
            (
                "UPDATE many cols",
                lambda: bench_update_many_columns_orm(db),
                lambda: bench_update_many_columns_asyncpg(pool),
            ),
            (
                "INSERT DELETE simple",
                lambda: bench_insert_delete_simple_orm(db),
                lambda: bench_insert_delete_simple_asyncpg(pool),
            ),
        ]

        for name, orm_func, asyncpg_func in comparisons:
            try:
                orm_results = await benchmark_async(orm_func, iterations=100)
                asyncpg_results = await benchmark_async(asyncpg_func, iterations=100)

                orm_median = orm_results["median_us"]
                asyncpg_median = asyncpg_results["median_us"]
                overhead = orm_median - asyncpg_median
                ratio = (
                    orm_median / asyncpg_median if asyncpg_median > 0 else float("inf")
                )

                print(
                    f"{name:<35} {orm_median:>12.2f} {asyncpg_median:>14.2f} "
                    f"{overhead:>+12.2f} {ratio:>10.2f}x"
                )
            except KeyboardInterrupt:
                print(f"\n\nBenchmark interrupted during: {name}")
                raise

        # Bulk insert benchmarks (separate section — different iteration count)
        print("-" * 95)
        print(
            f"\n{'Bulk Insert':<35} {'ORM (μs)':>12} {'AsyncPG (μs)':>14} "
            f"{'Overhead':>12} {'Ratio':>10}"
        )
        print("-" * 95)

        for n in [100, 1000]:
            try:
                orm_results = await benchmark_async(
                    lambda _n=n: bench_bulk_insert_orm(db, _n), iterations=20
                )
                asyncpg_results = await benchmark_async(
                    lambda _n=n: bench_bulk_insert_asyncpg(pool, _n), iterations=20
                )

                orm_median = orm_results["median_us"]
                asyncpg_median = asyncpg_results["median_us"]
                overhead = orm_median - asyncpg_median
                ratio = (
                    orm_median / asyncpg_median if asyncpg_median > 0 else float("inf")
                )
                label = f"BULK INSERT {n}"
                print(
                    f"{label:<35} {orm_median:>12.2f} "
                    f"{asyncpg_median:>14.2f} "
                    f"{overhead:>+12.2f} {ratio:>10.2f}x"
                )
            except KeyboardInterrupt:
                print(f"\n\nBenchmark interrupted during: BULK INSERT {n}")
                raise

        print("-" * 95)
        print()
        legend = (
            "Legend: μs = microseconds (median), Overhead = ORM - AsyncPG (μs), "
            "Ratio = ORM / AsyncPG"
        )
        print(legend)
        print("        Lower overhead and ratio closer to 1.0x is better")
        print()

    except KeyboardInterrupt:
        print("\n\nBenchmark interrupted by user (Ctrl+C)")
        raise
    except Exception as e:
        print(f"\n\nError during benchmark: {e}")
        raise
    finally:
        cleanup_errors = []
        if db is not None:
            try:
                await db.disconnect()
            except Exception as e:
                cleanup_errors.append(f"Database disconnect: {e}")

        if pg_temp is not None:
            pg_temp.stop()

        if cleanup_errors:
            print("\nCleanup warnings:")
            for error in cleanup_errors:
                print(f"  - {error}")


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Benchmark async query execution performance"
    )
    parser.add_argument(
        "--database-url",
        type=str,
        default=None,
        help=(
            "PostgreSQL connection URL. If not provided, a temporary database is used."
        ),
    )
    parser.add_argument(
        "--no-auto-postgres",
        action="store_true",
        help="Disable automatic PostgreSQL start/stop management",
    )
    args = parser.parse_args()

    asyncio.run(run_benchmarks(args.database_url))


if __name__ == "__main__":
    main()
