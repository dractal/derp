"""Full-stack load test exercising auth, storage, and database together.

Simulates a realistic app with user signups, logins, file uploads,
profile reads, and feed queries — all hitting real Postgres and Minio.

Requires:
  - PostgreSQL (auto via testing.postgresql, or --database-url)
  - Minio (auto-started if binary found, or --minio-url, or --no-storage)

Run with::

    python -m benchmarks.bench_fullstack
    python -m benchmarks.bench_fullstack --minio-url http://localhost:9000
    python -m benchmarks.bench_fullstack --no-storage
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import shutil
import socket
import statistics
import subprocess
import tempfile
import time
from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock, patch

import asyncpg
import testing.postgresql as tp

from derp.auth.models import AuthUser
from derp.config import (
    AuthConfig,
    DatabaseConfig,
    DerpConfig,
    EmailConfig,
    NativeAuthConfig,
    StorageConfig,
)
from derp.derp_client import DerpClient
from derp.orm import UUID, Boolean, Field, Serial, Table, Text, Timestamp, Varchar

# =============================================================================
# Extra tables
# =============================================================================


class Post(Table, table="posts"):
    id: Serial = Field(primary=True)
    title: Varchar[255] = Field()
    content: Text = Field()
    author_id: UUID = Field()
    published: Boolean = Field(default=True)
    created_at: Timestamp = Field(default="now()")


# =============================================================================
# Setup
# =============================================================================


async def create_tables(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                email VARCHAR(255) UNIQUE NOT NULL,
                email_confirmed_at TIMESTAMP WITH TIME ZONE,
                encrypted_password TEXT,
                first_name VARCHAR(255),
                last_name VARCHAR(255),
                username VARCHAR(255),
                image_url TEXT,
                provider VARCHAR(50) NOT NULL DEFAULT 'email',
                provider_id VARCHAR(255),
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                is_superuser BOOLEAN NOT NULL DEFAULT FALSE,
                role VARCHAR(50) NOT NULL DEFAULT 'default',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                last_sign_in_at TIMESTAMP WITH TIME ZONE
            );
            CREATE TABLE IF NOT EXISTS auth_sessions (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id UUID REFERENCES users(id) ON DELETE CASCADE,
                session_id UUID DEFAULT gen_random_uuid(),
                token VARCHAR(255) UNIQUE NOT NULL,
                role VARCHAR(50) NOT NULL DEFAULT 'default',
                revoked BOOLEAN NOT NULL DEFAULT FALSE,
                user_agent TEXT,
                ip_address VARCHAR(45),
                org_id UUID,
                not_after TIMESTAMP WITH TIME ZONE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS idx_sessions_user
                ON auth_sessions(user_id);
            CREATE INDEX IF NOT EXISTS idx_sessions_token
                ON auth_sessions(token);
        """)
        for stmt in Post.to_ddl().split(";"):
            stmt = stmt.strip()
            if stmt:
                await conn.execute(stmt)


def make_config(db_url: str, storage_cfg: dict[str, str] | None) -> DerpConfig:
    """Build DerpConfig inline — no derp.toml needed."""
    from derp.auth.jwt import JWTConfig

    storage = (
        StorageConfig(
            endpoint_url=storage_cfg["endpoint_url"],
            access_key_id=storage_cfg["access_key_id"],
            secret_access_key=storage_cfg["secret_access_key"],
            region="us-east-1",
        )
        if storage_cfg
        else None
    )
    return DerpConfig(
        database=DatabaseConfig(db_url=db_url, schema_path=__file__),
        email=EmailConfig(
            site_name="Bench",
            site_url="http://localhost",
            from_email="bench@test.com",
            smtp_host="localhost",
            smtp_port=587,
            smtp_user="test",
            smtp_password="test",
        ),
        auth=AuthConfig(
            native=NativeAuthConfig(
                jwt=JWTConfig(secret="bench-secret-key-long-enough-32!!"),
                enable_confirmation=False,
            )
        ),
        storage=storage,
    )


# =============================================================================
# Benchmark harness
# =============================================================================


async def bench(
    func: Callable[[], Any],
    iterations: int = 30,
    warmup: int = 3,
) -> dict[str, float]:
    for _ in range(warmup):
        await func()
    times: list[float] = []
    for _ in range(iterations):
        start = time.perf_counter_ns()
        await func()
        elapsed = (time.perf_counter_ns() - start) / 1000
        times.append(elapsed)
    return {
        "median": statistics.median(times),
        "p95": sorted(times)[int(len(times) * 0.95)],
        "mean": statistics.mean(times),
    }


class MockRequest:
    def __init__(self, token: str):
        self.headers = {"Authorization": f"Bearer {token}"}


# =============================================================================
# Runner
# =============================================================================


def _fmt(us: float) -> str:
    if us >= 1000:
        return f"{us / 1000:.2f}ms"
    return f"{us:.0f}μs"


def _row(name: str, stats: dict[str, float]) -> None:
    print(
        f"  {name:<40} {_fmt(stats['median']):>10} "
        f"{_fmt(stats['p95']):>10} {_fmt(stats['mean']):>10}"
    )


async def run(db_url: str, storage_cfg: dict[str, str] | None) -> None:
    # Create tables first
    pool = await asyncpg.create_pool(db_url, min_size=2, max_size=5)
    print("Setting up tables...")
    await create_tables(pool)
    await pool.close()

    config = make_config(db_url, storage_cfg)
    derp = DerpClient(config)
    with patch("aiosmtplib.send", AsyncMock()):
        await derp.connect()

    has_storage = derp._storage is not None
    if has_storage and storage_cfg:
        # Create bucket
        try:
            import aiobotocore.session

            session = aiobotocore.session.get_session()
            async with session.create_client(
                "s3",
                endpoint_url=storage_cfg["endpoint_url"],
                aws_access_key_id=storage_cfg["access_key_id"],
                aws_secret_access_key=storage_cfg["secret_access_key"],
                region_name="us-east-1",
            ) as s3:
                try:
                    await s3.create_bucket(Bucket="bench-bucket")
                except Exception:
                    pass
        except Exception:
            pass

    # -- Seed data --
    print("Seeding users...")
    emails: list[str] = []
    tokens: list[str] = []
    user_ids: list[str] = []

    with patch("aiosmtplib.send", AsyncMock()):
        for i in range(20):
            email = f"bench{i}_{random.randint(0, 99999)}@test.com"
            result = await derp.auth.sign_up(
                email=email,
                password="BenchPass123!",
                confirmation_url="http://localhost/confirm",
            )
            assert result is not None
            emails.append(email)
            tokens.append(result.tokens.access_token)
            user_ids.append(result.user.id)

    print("Seeding posts...")
    for _ in range(100):
        await (
            derp.db.insert(Post)
            .values(
                title=f"Post {random.randint(0, 999999)}",
                content="Seed content " * 10,
                author_id=random.choice(user_ids),
            )
            .execute()
        )

    if has_storage:
        print("Seeding files...")
        for i in range(10):
            await derp.storage.upload_file(
                bucket="bench-bucket",
                key=f"uploads/file_{i}.txt",
                data=os.urandom(1024 * 10),
            )

    # -- Run benchmarks --
    print("\nRunning benchmarks...\n")
    header = f"  {'Scenario':<40} {'p50':>10} {'p95':>10} {'Avg':>10}"
    print(header)
    print(f"  {'-' * 74}")

    # AUTH
    print("\n  AUTH")
    print(f"  {'─' * 74}")

    with patch("aiosmtplib.send", AsyncMock()):
        _row(
            "Sign up (new user)",
            await bench(
                lambda: derp.auth.sign_up(
                    email=f"load{random.randint(0, 9999999)}@t.com",
                    password="BenchPass123!",
                    confirmation_url=None,
                ),
                iterations=20,
                warmup=2,
            ),
        )

    _row(
        "Sign in (password)",
        await bench(
            lambda: derp.auth.sign_in_with_password(
                random.choice(emails), "BenchPass123!"
            ),
            iterations=30,
        ),
    )

    _row(
        "Authenticate (JWT verify)",
        await bench(
            lambda: derp.auth.authenticate(MockRequest(random.choice(tokens))),
            iterations=50,
        ),
    )

    _row(
        "Get user by ID",
        await bench(
            lambda: derp.auth.get_user(random.choice(user_ids)),
            iterations=50,
        ),
    )

    _row(
        "List users (limit 20)",
        await bench(
            lambda: derp.auth.list_users(limit=20),
            iterations=30,
        ),
    )

    # DATABASE
    print("\n  DATABASE")
    print(f"  {'─' * 74}")

    _row(
        "Create post (INSERT)",
        await bench(
            lambda: derp.db.insert(Post)
            .values(
                title="Bench post",
                content="Content " * 10,
                author_id=random.choice(user_ids),
            )
            .execute(),
            iterations=30,
        ),
    )

    _row(
        "Feed (JOIN + LIMIT 20)",
        await bench(
            lambda: derp.db.select(Post, AuthUser.email)
            .from_(Post)
            .inner_join(AuthUser, Post.author_id == AuthUser.id)
            .where(Post.published)
            .order_by(Post.created_at, asc=False)
            .limit(20)
            .execute(),
            iterations=30,
        ),
    )

    _row(
        "Select user by ID",
        await bench(
            lambda: derp.db.select(AuthUser)
            .where(AuthUser.id == random.choice(user_ids))
            .first(),
            iterations=50,
        ),
    )

    _row(
        "Update post (SET + WHERE)",
        await bench(
            lambda: derp.db.update(Post)
            .set(title="Updated title")
            .where(Post.id == random.randint(1, 100))
            .execute(),
            iterations=30,
        ),
    )

    # STORAGE
    if has_storage:
        print("\n  STORAGE")
        print(f"  {'─' * 74}")

        upload_data = os.urandom(1024 * 50)

        _row(
            "Upload file (50KB)",
            await bench(
                lambda: derp.storage.upload_file(
                    bucket="bench-bucket",
                    key=f"bench/{random.randint(0, 999999)}.bin",
                    data=upload_data,
                ),
                iterations=20,
                warmup=2,
            ),
        )

        _row(
            "Download file (10KB)",
            await bench(
                lambda: derp.storage.fetch_file(
                    bucket="bench-bucket",
                    key=f"uploads/file_{random.randint(0, 9)}.txt",
                ),
                iterations=30,
            ),
        )

        _row(
            "List files (prefix scan)",
            await bench(
                lambda: derp.storage.list_files(
                    bucket="bench-bucket", prefix="uploads/"
                ),
                iterations=30,
            ),
        )

        _row(
            "Check file exists",
            await bench(
                lambda: derp.storage.file_exists(
                    bucket="bench-bucket",
                    key=f"uploads/file_{random.randint(0, 9)}.txt",
                ),
                iterations=30,
            ),
        )

    print()
    await derp.disconnect()


# =============================================================================
# Minio auto-start
# =============================================================================


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _wait_for_port(host: str, port: int, timeout: float = 10) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.2)
            if s.connect_ex((host, port)) == 0:
                return
        time.sleep(0.05)
    raise RuntimeError(f"Service did not start on {host}:{port}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Full-stack derp benchmark (auth + db + storage)"
    )
    parser.add_argument("--database-url", type=str, default=None)
    parser.add_argument("--minio-url", type=str, default=None)
    parser.add_argument(
        "--no-storage",
        action="store_true",
        help="Skip storage benchmarks",
    )
    args = parser.parse_args()

    storage_cfg: dict[str, str] | None = None
    minio_proc = None

    if not args.no_storage:
        if args.minio_url:
            storage_cfg = {
                "endpoint_url": args.minio_url,
                "access_key_id": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
                "secret_access_key": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
            }
        elif shutil.which("minio"):
            tmpdir = tempfile.mkdtemp(prefix="derp_bench_minio_")
            port = _pick_free_port()
            console_port = _pick_free_port()
            minio_proc = subprocess.Popen(
                [
                    "minio",
                    "server",
                    tmpdir,
                    "--address",
                    f"127.0.0.1:{port}",
                    "--console-address",
                    f"127.0.0.1:{console_port}",
                    "--quiet",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env={
                    **os.environ,
                    "MINIO_ROOT_USER": "minioadmin",
                    "MINIO_ROOT_PASSWORD": "minioadmin",
                },
            )
            try:
                _wait_for_port("127.0.0.1", port)
                storage_cfg = {
                    "endpoint_url": f"http://127.0.0.1:{port}",
                    "access_key_id": "minioadmin",
                    "secret_access_key": "minioadmin",
                }
            except RuntimeError:
                minio_proc.kill()
                minio_proc = None
                print("Warning: minio failed to start")
        else:
            print(
                "Note: minio not found, skipping storage. "
                "Pass --minio-url or install minio."
            )

    try:
        if args.database_url:
            asyncio.run(run(args.database_url, storage_cfg))
        else:
            with tp.Postgresql() as pg:
                asyncio.run(run(pg.url(), storage_cfg))
    finally:
        if minio_proc:
            minio_proc.terminate()
            minio_proc.wait(timeout=5)


if __name__ == "__main__":
    main()
