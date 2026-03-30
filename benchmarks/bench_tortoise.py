"""Tortoise ORM benchmark — mirrors bench_queries.py operations.

Requires the ``benchmark`` optional dependency group::

    uv sync --extra benchmark

Run with: python -m benchmarks.bench_tortoise [--database-url URL]
"""

from __future__ import annotations

import argparse
import asyncio
import random
import statistics
import time
from collections.abc import Callable
from typing import Any

import testing.postgresql as tp
from tortoise import Tortoise, fields
from tortoise.functions import Sum
from tortoise.models import Model

# ── Tortoise models ──────────────────────────────────────────────


class TUser(Model):
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=255)
    email = fields.CharField(max_length=255)
    age = fields.IntField(null=True)
    bio = fields.TextField(null=True)

    class Meta:
        table = "tusers"


class TPost(Model):
    id = fields.IntField(primary_key=True)
    title = fields.CharField(max_length=255)
    content = fields.TextField()
    author = fields.ForeignKeyField("models.TUser", related_name="posts")
    views = fields.IntField(default=0)

    class Meta:
        table = "tposts"


class TComment(Model):
    id = fields.IntField(primary_key=True)
    post = fields.ForeignKeyField("models.TPost", related_name="comments")
    user = fields.ForeignKeyField("models.TUser", related_name="comments")
    content = fields.TextField()

    class Meta:
        table = "tcomments"


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


async def bench_select_simple() -> None:
    _ = await TUser.all()


async def bench_select_where() -> None:
    _ = await TUser.filter(id=1)


async def bench_select_where_and() -> None:
    _ = await TUser.filter(name="User 1", age__gt=18)


async def bench_select_join() -> None:
    _ = await TPost.all().select_related("author")


async def bench_select_full() -> None:
    _ = await (
        TPost.filter(views__gt=100, views__lte=10000)
        .select_related("author")
        .order_by("-views")
        .limit(20)
    )


async def bench_aggregate_count() -> None:
    _ = await TUser.filter(age__gt=18).count()


async def bench_aggregate_sum_group() -> None:
    _ = await (
        TPost.all()
        .group_by("author_id")
        .annotate(total_views=Sum("views"))
        .values("author_id", "total_views")
    )


async def bench_insert_simple() -> None:
    email = f"tort{random.randint(1000000, 9999999)}@test.com"
    _ = await TUser.create(name="Test User", email=email)


async def bench_update_simple() -> None:
    await TUser.filter(id=1).update(name="Updated Name")


# ── Runner ───────────────────────────────────────────────────────


async def run(database_url: str | None = None) -> None:
    pg_temp: tp.Postgresql | None = None
    try:
        if database_url is None:
            pg_temp = tp.Postgresql(port=7657)
            database_url = pg_temp.url()

        # Tortoise uses asyncpg:// scheme
        tort_url = database_url.replace("postgresql://", "asyncpg://")

        await Tortoise.init(
            db_url=tort_url,
            modules={"models": [__name__]},
        )
        await Tortoise.generate_schemas()

        # Seed data
        for i in range(1000):
            await TUser.create(
                name=f"User {i}",
                email=f"user{i}@example.com",
                age=random.randint(18, 80),
                bio=f"Bio {i}" if i % 2 == 0 else None,
            )
        users = await TUser.all().values_list("id", flat=True)
        for i in range(5000):
            await TPost.create(
                title=f"Post {i}",
                content=f"Content {i}",
                author_id=random.choice(users),
                views=random.randint(0, 10000),
            )

        print("=" * 60)
        print("Tortoise ORM Benchmark")
        print("=" * 60)
        print()
        print(f"{'Benchmark':<30} {'Median (μs)':>14}")
        print("-" * 60)

        benchmarks = [
            ("SELECT simple", bench_select_simple),
            ("SELECT WHERE =", bench_select_where),
            ("SELECT WHERE AND", bench_select_where_and),
            ("SELECT JOIN", bench_select_join),
            ("SELECT full query", bench_select_full),
            ("AGG COUNT WHERE", bench_aggregate_count),
            ("AGG SUM GROUP BY", bench_aggregate_sum_group),
            ("INSERT simple", bench_insert_simple),
            ("UPDATE simple", bench_update_simple),
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

        await Tortoise.close_connections()

    finally:
        if pg_temp is not None:
            pg_temp.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Tortoise ORM benchmark")
    parser.add_argument("--database-url", type=str, default=None)
    args = parser.parse_args()
    asyncio.run(run(args.database_url))


if __name__ == "__main__":
    main()
