"""SQLAlchemy async benchmark — mirrors bench_queries.py operations.

Requires the ``benchmark`` optional dependency group::

    uv sync --extra benchmark

Run with: python -m benchmarks.bench_sqlalchemy [--database-url URL]
"""

from __future__ import annotations

import argparse
import asyncio
import random
import statistics
import time
from collections.abc import Callable
from typing import Any

try:
    from sqlalchemy import (
        Integer as SAInteger,
    )
    from sqlalchemy import String, func, select
    from sqlalchemy import (
        Text as SAText,
    )
    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        create_async_engine,
    )
    from sqlalchemy.orm import (
        DeclarativeBase,
        Mapped,
        mapped_column,
    )
except ImportError:
    print("SQLAlchemy not installed. Run: uv sync --extra benchmark")
    raise SystemExit(1)

import testing.postgresql as tp

# ── SQLAlchemy models ────────────────────────────────────────────


class Base(DeclarativeBase):
    pass


class SAUser(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    email: Mapped[str] = mapped_column(String(255))
    age: Mapped[int | None] = mapped_column(SAInteger, nullable=True)
    bio: Mapped[str | None] = mapped_column(SAText, nullable=True)


class SAPost(Base):
    __tablename__ = "posts"
    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(255))
    content: Mapped[str] = mapped_column(SAText)
    author_id: Mapped[int] = mapped_column(SAInteger)
    views: Mapped[int] = mapped_column(SAInteger, default=0)


class SAComment(Base):
    __tablename__ = "comments"
    id: Mapped[int] = mapped_column(primary_key=True)
    post_id: Mapped[int] = mapped_column(SAInteger)
    user_id: Mapped[int] = mapped_column(SAInteger)
    content: Mapped[str] = mapped_column(SAText)


# ── Benchmark harness ────────────────────────────────────────────


async def benchmark_async(
    func: Callable[[], Any],
    iterations: int = 100,
    warmup: int = 10,
) -> dict[str, Any]:
    times: list[float] = []
    for _ in range(warmup):
        await func()
    for _ in range(iterations):
        start = time.perf_counter_ns()
        await func()
        end = time.perf_counter_ns()
        times.append((end - start) / 1000)
    return {
        "iterations": iterations,
        "median_us": statistics.median(times),
        "mean_us": statistics.mean(times),
        "ops_per_sec": 1_000_000 / statistics.mean(times),
    }


# ── Benchmark functions ──────────────────────────────────────────


async def bench_select_simple(engine: Any) -> None:
    async with AsyncSession(engine) as session:
        result = await session.execute(select(SAUser))
        _ = result.scalars().all()


async def bench_select_where(engine: Any) -> None:
    async with AsyncSession(engine) as session:
        result = await session.execute(select(SAUser).where(SAUser.id == 1))
        _ = result.scalars().all()


async def bench_select_where_and(engine: Any) -> None:
    async with AsyncSession(engine) as session:
        result = await session.execute(
            select(SAUser).where(SAUser.name == "User 1", SAUser.age > 18)
        )
        _ = result.scalars().all()


async def bench_select_join(engine: Any) -> None:
    async with AsyncSession(engine) as session:
        result = await session.execute(
            select(SAPost, SAUser.name).join(SAUser, SAPost.author_id == SAUser.id)
        )
        _ = result.all()


async def bench_select_full(engine: Any) -> None:
    async with AsyncSession(engine) as session:
        result = await session.execute(
            select(SAPost, SAUser.name)
            .join(SAUser, SAPost.author_id == SAUser.id)
            .where(SAPost.views > 100, SAPost.views <= 10000)
            .order_by(SAPost.views.desc())
            .limit(20)
        )
        _ = result.all()


async def bench_aggregate_count(engine: Any) -> None:
    async with AsyncSession(engine) as session:
        result = await session.execute(
            select(func.count(SAUser.id)).where(SAUser.age > 18)
        )
        _ = result.scalar()


async def bench_aggregate_sum_group(engine: Any) -> None:
    async with AsyncSession(engine) as session:
        result = await session.execute(
            select(SAPost.author_id, func.sum(SAPost.views)).group_by(SAPost.author_id)
        )
        _ = result.all()


async def bench_insert_simple(engine: Any) -> None:
    async with AsyncSession(engine) as session:
        email = f"sa{random.randint(1000000, 9999999)}@test.com"
        session.add(SAUser(name="Test User", email=email))
        await session.commit()


async def bench_update_simple(engine: Any) -> None:
    async with AsyncSession(engine) as session:
        result = await session.execute(select(SAUser).where(SAUser.id == 1))
        user = result.scalar_one()
        user.name = "Updated Name"
        await session.commit()


# ── Runner ───────────────────────────────────────────────────────


async def run(database_url: str | None = None) -> None:
    pg_temp: tp.Postgresql | None = None
    try:
        if database_url is None:
            pg_temp = tp.Postgresql(port=7656)
            database_url = pg_temp.url()

        # asyncpg DSN format
        sa_url = database_url.replace("postgresql://", "postgresql+asyncpg://")
        engine = create_async_engine(sa_url, pool_size=10)

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        # Seed data
        async with AsyncSession(engine) as session:
            for i in range(1000):
                session.add(
                    SAUser(
                        name=f"User {i}",
                        email=f"user{i}@example.com",
                        age=random.randint(18, 80),
                        bio=f"Bio {i}" if i % 2 == 0 else None,
                    )
                )
            await session.commit()
            for i in range(5000):
                session.add(
                    SAPost(
                        title=f"Post {i}",
                        content=f"Content {i}",
                        author_id=random.randint(1, 1000),
                        views=random.randint(0, 10000),
                    )
                )
            await session.commit()

        print("=" * 60)
        print("SQLAlchemy Async Benchmark")
        print("=" * 60)
        print()
        print(f"{'Benchmark':<30} {'Median (μs)':>14}")
        print("-" * 60)

        benchmarks = [
            ("SELECT simple", lambda: bench_select_simple(engine)),
            ("SELECT WHERE =", lambda: bench_select_where(engine)),
            (
                "SELECT WHERE AND",
                lambda: bench_select_where_and(engine),
            ),
            ("SELECT JOIN", lambda: bench_select_join(engine)),
            ("SELECT full query", lambda: bench_select_full(engine)),
            (
                "AGG COUNT WHERE",
                lambda: bench_aggregate_count(engine),
            ),
            (
                "AGG SUM GROUP BY",
                lambda: bench_aggregate_sum_group(engine),
            ),
            ("INSERT simple", lambda: bench_insert_simple(engine)),
            ("UPDATE simple", lambda: bench_update_simple(engine)),
        ]

        for name, fn in benchmarks:
            try:
                result = await benchmark_async(fn, iterations=100)
                print(f"{name:<30} {result['median_us']:>14.2f}")
            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"{name:<30} {'ERROR':>14} — {e}")

        print("-" * 60)
        print()

        await engine.dispose()

    finally:
        if pg_temp is not None:
            pg_temp.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="SQLAlchemy async benchmark")
    parser.add_argument("--database-url", type=str, default=None)
    args = parser.parse_args()
    asyncio.run(run(args.database_url))


if __name__ == "__main__":
    main()
