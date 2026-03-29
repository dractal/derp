"""Isolated row hydration benchmark — no database in the measurement loop.

Fetches N rows from the database once, converts to dicts, then
benchmarks only the ``_from_row()`` / ``Table(...)`` instantiation
cost vs. baseline dict access.

Run with: python -m benchmarks.bench_hydration [--database-url URL]
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time
from typing import Any

import asyncpg
import testing.postgresql as tp

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


def benchmark_sync(
    func: Any,
    iterations: int = 1000,
    warmup: int = 50,
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


async def _fetch_dicts(dsn: str, n: int = 500) -> list[dict[str, Any]]:
    """Seed and fetch N rows as dicts for hydration benchmarking."""
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(User.to_ddl().split(";")[0])
        for i in range(n):
            await conn.execute(
                "INSERT INTO users (name, email, age, bio) VALUES ($1, $2, $3, $4)",
                f"User {i}",
                f"user{i}@example.com",
                18 + (i % 60),
                f"Bio for user {i}" if i % 2 == 0 else None,
            )
        rows = await conn.fetch("SELECT * FROM users")
        return [dict(r) for r in rows]
    finally:
        await conn.close()


def hydrate_from_row(dicts: list[dict[str, Any]]) -> list[User]:
    """Hydrate dicts into User instances via _from_row."""
    return [User._from_row(d) for d in dicts]


def baseline_dict_access(
    dicts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Baseline: access all columns from each dict."""
    out = []
    for d in dicts:
        _ = d["id"]
        _ = d["name"]
        _ = d["email"]
        _ = d["age"]
        _ = d["bio"]
        _ = d["created_at"]
        out.append(d)
    return out


async def run(database_url: str | None = None) -> None:
    pg_temp: tp.Postgresql | None = None
    try:
        if database_url is None:
            pg_temp = tp.Postgresql(port=7655)
            database_url = pg_temp.url()

        dicts = await _fetch_dicts(database_url, n=500)
        print(f"Fetched {len(dicts)} rows for hydration benchmark\n")

        print("=" * 70)
        print("Row Hydration Benchmark (no DB in loop)")
        print("=" * 70)
        print()
        print(f"{'Operation':<30} {'Time (μs)':>12} {'per row (ns)':>14} {'Ratio':>10}")
        print("-" * 70)

        orm_res = benchmark_sync(lambda: hydrate_from_row(dicts))
        base_res = benchmark_sync(lambda: baseline_dict_access(dicts))

        n = len(dicts)
        orm_per_row = orm_res["median_us"] * 1000 / n
        base_per_row = base_res["median_us"] * 1000 / n
        ratio = (
            orm_res["median_us"] / base_res["median_us"]
            if base_res["median_us"] > 0
            else float("inf")
        )

        print(
            f"{'_from_row()':<30} "
            f"{orm_res['median_us']:>12.2f} "
            f"{orm_per_row:>14.1f} {ratio:>10.2f}x"
        )
        print(
            f"{'dict access (baseline)':<30} "
            f"{base_res['median_us']:>12.2f} "
            f"{base_per_row:>14.1f} {'1.00':>10}x"
        )
        print("-" * 70)
        print()

    finally:
        if pg_temp is not None:
            pg_temp.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark row hydration performance")
    parser.add_argument("--database-url", type=str, default=None)
    args = parser.parse_args()
    asyncio.run(run(args.database_url))


if __name__ == "__main__":
    main()
