"""Microbenchmarks for async query execution performance.

This module benchmarks the actual database query execution time,
comparing Derp ORM performance against direct asyncpg usage.

Run with: python -m benchmarks.bench_query_execution [--database-url DATABASE_URL]
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
from datetime import datetime
from typing import Any

import asyncpg
import testing.postgresql as tp

from derp.orm import DatabaseClient, Table
from derp.orm.fields import Field, Integer, Serial, Text, Timestamp, Varchar


# Sample table definitions for benchmarking
class User(Table, table_name="users"):
    id: int = Field(Serial(), primary_key=True)
    name: str = Field(Varchar(255))
    email: str = Field(Varchar(255))
    age: int | None = Field(Integer(), nullable=True)
    bio: str | None = Field(Text(), nullable=True)
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


# Database setup and teardown
async def create_temp_database(base_dsn: str) -> tuple[str, str]:
    """Create a temporary database for benchmarking.

    Args:
        base_dsn: Connection string to postgres database
                  (with or without database name)

    Returns:
        Tuple of (database_name, full_dsn)
    """
    # Parse base DSN to get connection params
    # Format: postgresql://user:password@host:port/database
    if "://" not in base_dsn:
        raise ValueError(
            "DSN must be in format: postgresql://user:password@host:port/database"
        )

    # Extract database name if present
    # Split on '/' after the scheme (postgresql://) to separate
    # connection string from database name
    # Handle both postgresql://user:pass@host:port/db and
    # postgresql://user:pass@host:port
    scheme_end = base_dsn.find("://") + 3
    path_start = base_dsn.find("/", scheme_end)

    if path_start != -1:
        # Has a path component, extract base connection string
        base_conn_str = base_dsn[:path_start]
    else:
        # No path component, use as-is
        base_conn_str = base_dsn

    # Connect to postgres database to create new database
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
    """Drop a database.

    Args:
        base_dsn: Connection string (with or without database name)
        db_name: Name of database to drop
    """
    # Extract base connection string
    scheme_end = base_dsn.find("://") + 3
    path_start = base_dsn.find("/", scheme_end)

    if path_start != -1:
        base_conn_str = base_dsn[:path_start]
    else:
        base_conn_str = base_dsn

    # Terminate all connections to the database
    postgres_dsn = f"{base_conn_str}/postgres"
    try:
        conn = await asyncpg.connect(postgres_dsn)
    except (OSError, asyncpg.exceptions.InvalidPasswordError) as e:
        # If we can't connect, database might already be dropped or server is down
        # Just log and continue
        print(f"Warning: Could not connect to drop database: {e}")
        return

    try:
        # Terminate active connections
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


async def setup_schema(db: DatabaseClient) -> None:
    """Create tables and indexes for benchmarking."""
    async with db.pool.acquire() as conn:
        # Create tables - DDL may contain multiple statements
        for ddl in [User.to_ddl(), Post.to_ddl(), Comment.to_ddl()]:
            # Split by semicolon and execute each statement
            statements = [s.strip() for s in ddl.split(";") if s.strip()]
            for statement in statements:
                await conn.execute(statement)


async def seed_data(
    db: DatabaseClient,
    num_users: int = 1000,
    num_posts: int = 5000,
    num_comments: int = 10000,
) -> None:
    """Seed database with test data."""
    # Insert users
    user_data = []
    for i in range(num_users):
        user_data.append(
            {
                "name": f"User {i}",
                "email": f"user{i}@example.com",
                "age": random.randint(18, 80),
                "bio": f"Bio for user {i}" if i % 2 == 0 else None,
            }
        )

    for user in user_data:
        await db.insert(User).values(**user).execute()

    # Insert posts
    post_data = []
    for i in range(num_posts):
        post_data.append(
            {
                "title": f"Post {i}",
                "content": f"Content for post {i}",
                "author_id": random.randint(1, num_users),
                "views": random.randint(0, 10000),
            }
        )

    for post in post_data:
        await db.insert(Post).values(**post).execute()

    # Insert comments
    comment_data = []
    for i in range(num_comments):
        comment_data.append(
            {
                "post_id": random.randint(1, num_posts),
                "user_id": random.randint(1, num_users),
                "content": f"Comment {i}",
            }
        )

    for comment in comment_data:
        await db.insert(Comment).values(**comment).execute()


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


async def bench_select_simple_orm(db: DatabaseClient) -> None:
    """ORM: SELECT * FROM users"""
    await db.select(User).execute()


async def bench_select_simple_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: SELECT * FROM users"""
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM users")
        [User.model_validate(dict(row)) for row in rows]


async def bench_select_where_orm(db: DatabaseClient) -> None:
    """ORM: SELECT * FROM users WHERE id = $1"""
    await db.select(User).where(User.c.id == 1).execute()


async def bench_select_where_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: SELECT * FROM users WHERE id = $1"""
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM users WHERE id = $1", 1)
        [User.model_validate(dict(row)) for row in rows]


async def bench_select_where_and_orm(db: DatabaseClient) -> None:
    """ORM: SELECT * FROM users WHERE name = $1 AND age > $2"""
    await db.select(User).where((User.c.name == "User 1") & (User.c.age > 18)).execute()


async def bench_select_where_and_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: SELECT * FROM users WHERE name = $1 AND age > $2"""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM users WHERE name = $1 AND age > $2", "User 1", 18
        )
        [User.model_validate(dict(row)) for row in rows]


async def bench_select_where_complex_orm(db: DatabaseClient) -> None:
    """ORM: SELECT with complex nested AND/OR conditions"""
    await (
        db.select(User)
        .where(
            ((User.c.name == "User 1") | (User.c.name == "User 2"))
            & (User.c.age > 18)
            & (User.c.age < 65)
            & (
                (User.c.email.ilike("%@example.com"))
                | (User.c.email.ilike("%@test.com"))
            )
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
        rows = await conn.fetch(
            query, "User 1", "User 2", 18, 65, "%@example.com", "%@test.com"
        )
        [User.model_validate(dict(row)) for row in rows]


async def bench_select_where_in_orm(db: DatabaseClient) -> None:
    """ORM: SELECT * FROM users WHERE id IN ($1, ..., $10)"""
    ids = list(range(1, 11))
    await db.select(User).where(User.c.id.in_(ids)).execute()


async def bench_select_where_in_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: SELECT * FROM users WHERE id IN ($1, ..., $10)"""
    ids = list(range(1, 11))
    placeholders = ",".join(f"${i + 1}" for i in range(len(ids)))
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT * FROM users WHERE id IN ({placeholders})", *ids
        )
        [User.model_validate(dict(row)) for row in rows]


async def bench_select_join_orm(db: DatabaseClient) -> None:
    """ORM: SELECT with INNER JOIN"""
    await (
        db.select(Post, User.c.name)
        .from_(Post)
        .inner_join(User, Post.c.author_id == User.c.id)
        .execute()
    )


async def bench_select_join_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: SELECT with INNER JOIN"""
    async with pool.acquire() as conn:
        query = (
            "SELECT posts.*, users.name FROM posts "
            "INNER JOIN users ON (posts.author_id = users.id)"
        )
        rows = await conn.fetch(query)
        [dict(row) for row in rows]


async def bench_select_full_orm(db: DatabaseClient) -> None:
    """ORM: SELECT with WHERE, JOIN, ORDER BY, LIMIT"""
    await (
        db.select(Post, User.c.name)
        .from_(Post)
        .inner_join(User, Post.c.author_id == User.c.id)
        .where((Post.c.views > 100) & (Post.c.views <= 10000))
        .order_by(Post.c.views, asc=False)
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
        rows = await conn.fetch(query, 100, 10000)
        [dict(row) for row in rows]


# =============================================================================
# INSERT benchmarks
# =============================================================================


async def bench_insert_simple_orm(db: DatabaseClient) -> None:
    """ORM: INSERT with 2 columns"""
    email = f"test{random.randint(1000000, 9999999)}@example.com"
    await db.insert(User).values(name="Test User", email=email).execute()


async def bench_insert_simple_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: INSERT with 2 columns"""
    email = f"test{random.randint(1000000, 9999999)}@example.com"
    async with pool.acquire() as conn:
        await conn.fetchrow(
            "INSERT INTO users (name, email) VALUES ($1, $2)", "Test User", email
        )


async def bench_insert_returning_orm(db: DatabaseClient) -> None:
    """ORM: INSERT ... RETURNING *"""
    email = f"test{random.randint(10000, 99999)}@example.com"
    await (
        db.insert(User).values(name="Test User", email=email).returning(User).execute()
    )


async def bench_insert_returning_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: INSERT ... RETURNING *"""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING *",
            "Test User",
            f"test{random.randint(10000, 99999)}@example.com",
        )
        User.model_validate(dict(row))


# =============================================================================
# UPDATE benchmarks
# =============================================================================


async def bench_update_simple_orm(db: DatabaseClient) -> None:
    """ORM: UPDATE with single SET and WHERE"""
    await db.update(User).set(name="Updated Name").where(User.c.id == 1).execute()


async def bench_update_simple_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: UPDATE with single SET and WHERE"""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET name = $1 WHERE (users.id = $2)", "Updated Name", 1
        )


async def bench_update_many_columns_orm(db: DatabaseClient) -> None:
    """ORM: UPDATE with multiple SET columns"""
    email = f"updated{random.randint(10000, 99999)}@example.com"
    await (
        db.update(User)
        .set(name="Updated Name", email=email, age=25, bio="Updated bio")
        .where(User.c.id == 1)
        .execute()
    )


async def bench_update_many_columns_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: UPDATE with multiple SET columns"""
    async with pool.acquire() as conn:
        query = (
            "UPDATE users SET name = $1, email = $2, age = $3, bio = $4 "
            "WHERE (users.id = $5)"
        )
        await conn.execute(
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


async def bench_delete_simple_orm(db: DatabaseClient) -> None:
    """ORM: DELETE with simple WHERE"""
    # Insert a test user first to delete
    email = f"delete{random.randint(100000, 999999)}@example.com"
    user = (
        await db.insert(User)
        .values(name="Delete Me", email=email)
        .returning(User)
        .execute()
    )
    await db.delete(User).where(User.c.id == user.id).execute()


async def bench_delete_simple_asyncpg(pool: asyncpg.Pool) -> None:
    """AsyncPG: DELETE with simple WHERE"""
    async with pool.acquire() as conn:
        # Insert a test user first to delete
        row = await conn.fetchrow(
            "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
            "Delete Me",
            f"delete{random.randint(100000, 999999)}@example.com",
        )
        await conn.execute("DELETE FROM users WHERE (users.id = $1)", dict(row)["id"])


# =============================================================================
# Main Runner
# =============================================================================


async def run_benchmarks(database_url: str | None = None) -> None:
    """Run all benchmarks and print results."""
    db: DatabaseClient | None = None
    pg_temp: tp.Postgresql | None = None

    try:
        if database_url is None:
            pg_temp = tp.Postgresql(port=7654)
            database_url = pg_temp.url()

        db = DatabaseClient(database_url)
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
            ("SELECT simple", bench_select_simple_orm, bench_select_simple_asyncpg),
            ("SELECT WHERE =", bench_select_where_orm, bench_select_where_asyncpg),
            (
                "SELECT WHERE AND",
                bench_select_where_and_orm,
                bench_select_where_and_asyncpg,
            ),
            (
                "SELECT WHERE complex",
                bench_select_where_complex_orm,
                bench_select_where_complex_asyncpg,
            ),
            (
                "SELECT WHERE IN (10)",
                bench_select_where_in_orm,
                bench_select_where_in_asyncpg,
            ),
            ("SELECT JOIN", bench_select_join_orm, bench_select_join_asyncpg),
            ("SELECT full query", bench_select_full_orm, bench_select_full_asyncpg),
            ("INSERT simple", bench_insert_simple_orm, bench_insert_simple_asyncpg),
            (
                "INSERT RETURNING",
                bench_insert_returning_orm,
                bench_insert_returning_asyncpg,
            ),
            ("UPDATE simple", bench_update_simple_orm, bench_update_simple_asyncpg),
            (
                "UPDATE many cols",
                bench_update_many_columns_orm,
                bench_update_many_columns_asyncpg,
            ),
            ("DELETE simple", bench_delete_simple_orm, bench_delete_simple_asyncpg),
        ]

        for name, orm_func, asyncpg_func in comparisons:
            try:
                # Run ORM benchmark
                orm_results = await benchmark_async(
                    lambda: orm_func(db), iterations=100
                )

                # Run AsyncPG benchmark
                asyncpg_results = await benchmark_async(
                    lambda: asyncpg_func(pool), iterations=100
                )

                orm_mean = orm_results["mean_us"]
                asyncpg_mean = asyncpg_results["mean_us"]
                overhead = orm_mean - asyncpg_mean
                ratio = orm_mean / asyncpg_mean if asyncpg_mean > 0 else float("inf")

                print(
                    f"{name:<35} {orm_mean:>12.2f} {asyncpg_mean:>14.2f} "
                    f"{overhead:>+12.2f} {ratio:>10.2f}x"
                )
            except KeyboardInterrupt:
                print(f"\n\nBenchmark interrupted during: {name}")
                raise

        print("-" * 95)
        print()
        legend = (
            "Legend: μs = microseconds, Overhead = ORM - AsyncPG (μs), "
            "Ratio = ORM / AsyncPG"
        )
        print(legend)
        print("        Lower overhead and ratio closer to 1.0x is better")
        print()

    except KeyboardInterrupt:
        print("\n\nBenchmark interrupted by user (Ctrl+C)")
        # Cleanup will happen in finally block
        raise
    except Exception as e:
        print(f"\n\nError during benchmark: {e}")
        raise
    finally:
        # Always cleanup resources, even on interrupt or error
        cleanup_errors = []

        # Disconnect database if still connected
        if db is not None:
            try:
                await db.disconnect()
            except Exception as e:
                cleanup_errors.append(f"Database disconnect: {e}")

        # Cleanup temporary database if we created it.
        if pg_temp is not None:
            pg_temp.stop()

        # Report any cleanup errors
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
