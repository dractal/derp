"""Load test for Derp ORM — simulates concurrent API traffic.

Spawns N concurrent "users" that repeatedly hit different ORM operations
for a fixed duration, measuring throughput and latency distribution.

Run with::

    python -m benchmarks.bench_load [--database-url URL] [--concurrency 50]
"""

from __future__ import annotations

import argparse
import asyncio
import random
import statistics
import time

import asyncpg
import testing.postgresql as tp

from derp.orm import (
    Boolean,
    DatabaseEngine,
    Field,
    Integer,
    Nullable,
    Serial,
    Table,
    Text,
    Timestamp,
    Varchar,
)

# =============================================================================
# Schema
# =============================================================================


class User(Table, table="users"):
    id: Serial = Field(primary=True)
    name: Varchar[255] = Field()
    email: Varchar[255] = Field(unique=True)
    age: Nullable[Integer] = Field()
    is_active: Boolean = Field(default=True)
    created_at: Timestamp = Field(default="now()")


class Post(Table, table="posts"):
    id: Serial = Field(primary=True)
    title: Varchar[255] = Field()
    content: Text = Field()
    author_id: Integer = Field()
    views: Integer = Field(default=0)
    published: Boolean = Field(default=False)
    created_at: Timestamp = Field(default="now()")


# =============================================================================
# Setup
# =============================================================================


async def setup(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        for ddl in [User.to_ddl(), Post.to_ddl()]:
            for stmt in ddl.split(";"):
                stmt = stmt.strip()
                if stmt:
                    await conn.execute(stmt)

        for i in range(200):
            await conn.execute(
                "INSERT INTO users (name, email, age, is_active) "
                "VALUES ($1, $2, $3, $4)",
                f"User {i}",
                f"user{i}@example.com",
                random.randint(18, 80),
                i % 10 != 0,
            )
        for i in range(1000):
            await conn.execute(
                "INSERT INTO posts (title, content, author_id, views, "
                "published) VALUES ($1, $2, $3, $4, $5)",
                f"Post {i}",
                f"Content for post {i}. " * 3,
                random.randint(1, 200),
                random.randint(0, 50000),
                i % 3 != 0,
            )


# =============================================================================
# Workloads — each simulates a typical API handler
# =============================================================================


async def workload_read_user(db: DatabaseEngine) -> None:
    """GET /users/:id"""
    uid = random.randint(1, 200)
    await db.select(User).where(User.id == uid).first()


async def workload_list_posts(db: DatabaseEngine) -> None:
    """GET /posts?page=N"""
    offset = random.randint(0, 900)
    await (
        db.select(Post)
        .where(Post.published)
        .order_by(Post.created_at, asc=False)
        .limit(20)
        .offset(offset)
        .execute()
    )


async def workload_feed(db: DatabaseEngine) -> None:
    """GET /feed — join query"""
    await (
        db.select(Post, User.name)
        .from_(Post)
        .inner_join(User, Post.author_id == User.id)
        .where(Post.published)
        .order_by(Post.created_at, asc=False)
        .limit(20)
        .execute()
    )


async def workload_create_post(db: DatabaseEngine) -> None:
    """POST /posts"""
    uid = random.randint(1, 200)
    await (
        db.insert(Post)
        .values(
            title=f"Load test post {random.randint(0, 999999)}",
            content="Load test content " * 5,
            author_id=uid,
            views=0,
            published=True,
        )
        .execute()
    )


async def workload_update_views(db: DatabaseEngine) -> None:
    """PATCH /posts/:id/view"""
    pid = random.randint(1, 1000)
    await (
        db.update(Post)
        .set(views=random.randint(1, 100000))
        .where(Post.id == pid)
        .execute()
    )


async def workload_search(db: DatabaseEngine) -> None:
    """GET /search?q=..."""
    q = f"User {random.randint(0, 50)}"
    await db.select(User).where(User.name.ilike(f"%{q}%")).limit(10).execute()


# Weighted workload distribution — reads are more common than writes
WORKLOADS: list[tuple[str, float]] = [
    ("read_user", 0.25),
    ("list_posts", 0.25),
    ("feed", 0.20),
    ("search", 0.15),
    ("create_post", 0.10),
    ("update_views", 0.05),
]

WORKLOAD_FNS = {
    "read_user": workload_read_user,
    "list_posts": workload_list_posts,
    "feed": workload_feed,
    "search": workload_search,
    "create_post": workload_create_post,
    "update_views": workload_update_views,
}


def _pick_workload() -> str:
    r = random.random()
    cumulative = 0.0
    for name, weight in WORKLOADS:
        cumulative += weight
        if r < cumulative:
            return name
    return WORKLOADS[-1][0]


# =============================================================================
# Worker
# =============================================================================


async def worker(
    db: DatabaseEngine,
    duration_s: float,
    results: list[tuple[str, float, float]],
    errors: list[str],
    start_event: asyncio.Event,
) -> None:
    """Single concurrent worker — picks random workloads until time is up.

    Records (workload_name, total_us, pool_wait_us) per operation.
    """
    await start_event.wait()
    deadline = time.monotonic() + duration_s
    pool = db.pool

    while time.monotonic() < deadline:
        name = _pick_workload()
        fn = WORKLOAD_FNS[name]

        # Measure pool acquire time separately
        t0 = time.perf_counter_ns()
        conn = await pool.acquire()
        t1 = time.perf_counter_ns()
        await pool.release(conn)
        pool_wait_us = (t1 - t0) / 1000

        # Measure total operation time
        start = time.perf_counter_ns()
        try:
            await fn(db)
            elapsed_us = (time.perf_counter_ns() - start) / 1000
            results.append((name, elapsed_us, pool_wait_us))
        except Exception as e:
            errors.append(f"{name}: {e}")


# =============================================================================
# Runner
# =============================================================================


async def run_load_test(
    dsn: str,
    concurrency: int,
    duration_s: float,
    pool_size: int,
) -> None:
    pool = await asyncpg.create_pool(dsn, min_size=2, max_size=pool_size)
    db = DatabaseEngine(dsn, min_size=2, max_size=pool_size)
    await db.connect()

    print("Setting up schema and seeding data...")
    await setup(pool)
    await pool.close()

    print(
        f"Running load test: {concurrency} concurrent workers, "
        f"{duration_s}s duration, pool_size={pool_size}\n"
    )

    results: list[tuple[str, float, float]] = []
    errors: list[str] = []
    start_event = asyncio.Event()

    workers = [
        asyncio.create_task(worker(db, duration_s, results, errors, start_event))
        for _ in range(concurrency)
    ]

    start_time = time.monotonic()
    start_event.set()
    await asyncio.gather(*workers)
    wall_time = time.monotonic() - start_time

    await db.disconnect()

    # Analyze results
    total_ops = len(results)
    throughput = total_ops / wall_time

    print(f"{'=' * 65}")
    print(f"  Total operations:  {total_ops:,}")
    print(f"  Wall time:         {wall_time:.2f}s")
    print(f"  Throughput:        {throughput:,.0f} ops/sec")
    print(f"  Errors:            {len(errors)}")
    print(f"{'=' * 65}\n")

    if not results:
        print("No results collected.")
        return

    # Overall latency
    all_times = sorted(t for _, t, _ in results)
    all_waits = sorted(w for _, _, w in results)
    print("Overall latency:")
    _print_latency(all_times)
    print()
    print("Pool acquire wait:")
    _print_latency(all_waits)
    print()

    # Per-workload breakdown
    by_workload: dict[str, list[float]] = {}
    by_workload_wait: dict[str, list[float]] = {}
    for name, t, w in results:
        by_workload.setdefault(name, []).append(t)
        by_workload_wait.setdefault(name, []).append(w)

    print(
        f"{'Workload':<18} {'Count':>7} {'p50':>10} {'p95':>10} "
        f"{'p99':>10} {'Pool p50':>10}"
    )
    print("-" * 78)

    for name, _ in WORKLOADS:
        times = sorted(by_workload.get(name, []))
        waits = sorted(by_workload_wait.get(name, []))
        if not times:
            continue
        p50 = times[int(len(times) * 0.50)]
        p95 = times[int(len(times) * 0.95)]
        p99 = times[int(len(times) * 0.99)]
        wp50 = waits[int(len(waits) * 0.50)]
        print(
            f"{name:<18} {len(times):>7,} {_fmt(p50):>10} "
            f"{_fmt(p95):>10} {_fmt(p99):>10} {_fmt(wp50):>10}"
        )

    if errors:
        print("\nFirst 5 errors:")
        for e in errors[:5]:
            print(f"  {e}")


def _print_latency(times: list[float]) -> None:
    p50 = times[int(len(times) * 0.50)]
    p95 = times[int(len(times) * 0.95)]
    p99 = times[int(len(times) * 0.99)]
    avg = statistics.mean(times)
    print(f"  p50:  {_fmt(p50)}")
    print(f"  p95:  {_fmt(p95)}")
    print(f"  p99:  {_fmt(p99)}")
    print(f"  avg:  {_fmt(avg)}")
    print(f"  min:  {_fmt(times[0])}")
    print(f"  max:  {_fmt(times[-1])}")


def _fmt(us: float) -> str:
    if us >= 1000:
        return f"{us / 1000:.2f}ms"
    return f"{us:.0f}μs"


def main() -> None:
    parser = argparse.ArgumentParser(description="Derp ORM load test")
    parser.add_argument("--database-url", type=str, default=None)
    parser.add_argument(
        "--concurrency",
        type=int,
        default=50,
        help="Number of concurrent workers (default: 50)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=10,
        help="Test duration in seconds (default: 10)",
    )
    parser.add_argument(
        "--pool-size",
        type=int,
        default=20,
        help="Connection pool size (default: 20)",
    )
    args = parser.parse_args()

    if args.database_url:
        asyncio.run(
            run_load_test(
                args.database_url,
                args.concurrency,
                args.duration,
                args.pool_size,
            )
        )
    else:
        with tp.Postgresql() as pg:
            asyncio.run(
                run_load_test(
                    pg.url(),
                    args.concurrency,
                    args.duration,
                    args.pool_size,
                )
            )


if __name__ == "__main__":
    main()
