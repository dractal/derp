"""Typer CLI commands for Derp ORM migrations."""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import tomllib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import asyncpg
import typer

from derp.orm.schema import (
    compare_schemas,
    generate_migration_sql,
    introspect_database,
    load_tables_from_module,
)

app = typer.Typer(
    name="derp",
    help="Derp ORM - A strongly-typed async Python ORM for PostgreSQL",
    no_args_is_help=True,
)

CONFIG_FILE = "derp.toml"
MIGRATIONS_TABLE = "_derp_migrations"
DEFAULT_DATABASE_URL_ENV = "DATABASE_URL"
DEFAULT_MIGRATIONS_DIR = "./migrations"


@dataclass
class DatabaseConfig:
    """Database configuration."""

    env: str = DEFAULT_DATABASE_URL_ENV

    def get_url(self) -> str:
        """Get database URL from the configured environment variable."""
        url = os.environ.get(self.env)
        if not url:
            typer.echo(
                f"Error: {self.env} environment variable not set.\n"
                f"Set it with: export {self.env}=postgresql://user:pass@localhost:5432/mydb",
                err=True,
            )
            raise typer.Exit(1)
        return url


@dataclass
class MigrationsConfig:
    """Migrations configuration."""

    dir: str = DEFAULT_MIGRATIONS_DIR
    schema: str | None = None

    @property
    def directory(self) -> Path:
        """Get migrations directory as Path."""
        return Path(self.dir)

    def get_schema_path(self) -> str:
        """Get schema module path, raising error if not configured."""
        if not self.schema:
            typer.echo(
                "Error: migrations.schema not configured in derp.toml", err=True
            )
            raise typer.Exit(1)
        return self.schema


@dataclass
class Config:
    """Derp configuration loaded from derp.toml."""

    database: DatabaseConfig
    migrations: MigrationsConfig

    @classmethod
    def load(cls) -> Config:
        """Load configuration from derp.toml."""
        config_path = Path(CONFIG_FILE)
        if not config_path.exists():
            typer.echo(
                f"Error: {CONFIG_FILE} not found in current directory.", err=True
            )
            typer.echo("Create a derp.toml with:", err=True)
            typer.echo(
                """
[database]
env = "DATABASE_URL"  # Environment variable containing the database URL

[migrations]
dir = "./migrations"
schema = "src/myapp/schema.py"
""",
                err=True,
            )
            raise typer.Exit(1)

        with open(config_path, "rb") as f:
            data = tomllib.load(f)

        db_data = data.get("database", {})
        migrations_data = data.get("migrations", {})

        return cls(
            database=DatabaseConfig(
                env=db_data.get("env", DEFAULT_DATABASE_URL_ENV),
            ),
            migrations=MigrationsConfig(
                dir=migrations_data.get("dir", DEFAULT_MIGRATIONS_DIR),
                schema=migrations_data.get("schema"),
            ),
        )


async def ensure_migrations_table(pool: asyncpg.Pool) -> None:
    """Ensure the migrations tracking table exists."""
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE,
                hash VARCHAR(64) NOT NULL,
                applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
            """
        )


async def get_applied_migrations(pool: asyncpg.Pool) -> dict[str, str]:
    """Get dict of applied migration names to their hashes."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT name, hash FROM {MIGRATIONS_TABLE} ORDER BY id"
        )
        return {row["name"]: row["hash"] for row in rows}


async def record_migration(pool: asyncpg.Pool, name: str, hash_: str) -> None:
    """Record a migration as applied."""
    async with pool.acquire() as conn:
        await conn.execute(
            f"INSERT INTO {MIGRATIONS_TABLE} (name, hash) VALUES ($1, $2)",
            name,
            hash_,
        )


async def remove_migration_record(pool: asyncpg.Pool, name: str) -> None:
    """Remove a migration record."""
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {MIGRATIONS_TABLE} WHERE name = $1", name)


def compute_hash(content: str) -> str:
    """Compute SHA256 hash of migration content."""
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def get_migration_files(migrations_dir: Path) -> list[tuple[str, Path]]:
    """Get sorted list of (name, path) for migration files."""
    if not migrations_dir.exists():
        return []

    migrations: list[tuple[str, Path]] = []
    for path in sorted(migrations_dir.glob("*.sql")):
        # Skip down migrations
        if path.stem.endswith(".down"):
            continue
        migrations.append((path.stem, path))
    return migrations


def get_next_migration_number(migrations_dir: Path) -> int:
    """Get the next migration number."""
    migrations = get_migration_files(migrations_dir)
    if not migrations:
        return 1

    last_name = migrations[-1][0]
    match = re.match(r"^(\d+)_", last_name)
    if match:
        return int(match.group(1)) + 1
    return len(migrations) + 1


@app.command()
def generate(
    name: Annotated[
        str, typer.Option("--name", "-n", help="Migration name")
    ] = "migration",
) -> None:
    """Generate a new migration from schema diff."""
    config = Config.load()
    db_url = config.database.get_url()
    migrations_dir = config.migrations.directory
    schema_path = config.migrations.get_schema_path()

    # Load tables from schema
    try:
        tables = load_tables_from_module(schema_path)
    except FileNotFoundError:
        typer.echo(f"Error: Schema file not found: {schema_path}", err=True)
        raise typer.Exit(1)

    if not tables:
        typer.echo(f"No Table classes found in {schema_path}", err=True)
        raise typer.Exit(1)

    typer.echo(f"Found {len(tables)} table(s): {', '.join(t.__name__ for t in tables)}")

    async def _generate() -> tuple[str, str]:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=2)
        try:
            await ensure_migrations_table(pool)
            db_tables = await introspect_database(pool)
            diff = compare_schemas(tables, db_tables)
            return generate_migration_sql(diff)
        finally:
            await pool.close()

    up_sql, down_sql = asyncio.run(_generate())

    if not up_sql.strip():
        typer.echo("No changes detected. Schema is up to date.")
        return

    # Create migrations directory if needed
    migrations_dir.mkdir(parents=True, exist_ok=True)

    # Generate migration file
    num = get_next_migration_number(migrations_dir)
    safe_name = re.sub(r"[^a-z0-9_]", "_", name.lower())
    filename = f"{num:04d}_{safe_name}"

    up_path = migrations_dir / f"{filename}.sql"
    up_path.write_text(
        f"-- Migration: {name}\n"
        f"-- Generated at: {datetime.now(UTC).isoformat()}\n"
        f"\n{up_sql}\n"
    )

    if down_sql.strip():
        down_path = migrations_dir / f"{filename}.down.sql"
        down_path.write_text(f"-- Rollback: {name}\n\n{down_sql}\n")
        typer.echo(f"Created: {up_path}")
        typer.echo(f"Created: {down_path}")
    else:
        typer.echo(f"Created: {up_path}")

    typer.echo("\nReview the migration and run 'derp migrate' to apply.")


@app.command()
def migrate() -> None:
    """Apply pending migrations."""
    config = Config.load()
    db_url = config.database.get_url()
    migrations_dir = config.migrations.directory

    migrations = get_migration_files(migrations_dir)
    if not migrations:
        typer.echo("No migrations found.")
        return

    async def _migrate() -> int:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=2)
        applied_count = 0
        try:
            await ensure_migrations_table(pool)
            applied = await get_applied_migrations(pool)

            for name, path in migrations:
                if name in applied:
                    continue

                typer.echo(f"Applying: {name}")
                sql = path.read_text()
                hash_ = compute_hash(sql)

                async with pool.acquire() as conn:
                    await conn.execute(sql)

                await record_migration(pool, name, hash_)
                applied_count += 1
                typer.echo(f"  Applied: {name}")

            return applied_count
        finally:
            await pool.close()

    count = asyncio.run(_migrate())
    if count == 0:
        typer.echo("No pending migrations.")
    else:
        typer.echo(f"\nApplied {count} migration(s).")


@app.command()
def push() -> None:
    """Push schema directly to database (dev mode, no migration files)."""
    config = Config.load()
    db_url = config.database.get_url()
    schema_path = config.migrations.get_schema_path()

    # Load tables from schema
    try:
        tables = load_tables_from_module(schema_path)
    except FileNotFoundError:
        typer.echo(f"Error: Schema file not found: {schema_path}", err=True)
        raise typer.Exit(1)

    if not tables:
        typer.echo(f"No Table classes found in {schema_path}", err=True)
        raise typer.Exit(1)

    typer.echo(f"Found {len(tables)} table(s): {', '.join(t.__name__ for t in tables)}")

    async def _push() -> None:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=2)
        try:
            db_tables = await introspect_database(pool)
            diff = compare_schemas(tables, db_tables)
            up_sql, _ = generate_migration_sql(diff)

            if not up_sql.strip():
                typer.echo("No changes detected. Schema is up to date.")
                return

            typer.echo("\nApplying changes:")
            typer.echo(up_sql)
            typer.echo("")

            async with pool.acquire() as conn:
                await conn.execute(up_sql)

            typer.echo("Schema pushed successfully.")
        finally:
            await pool.close()

    asyncio.run(_push())


@app.command()
def status() -> None:
    """Show migration status."""
    config = Config.load()
    db_url = config.database.get_url()
    migrations_dir = config.migrations.directory

    migrations = get_migration_files(migrations_dir)

    async def _status() -> dict[str, str]:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=2)
        try:
            await ensure_migrations_table(pool)
            return await get_applied_migrations(pool)
        finally:
            await pool.close()

    applied = asyncio.run(_status())

    if not migrations:
        typer.echo("No migrations found.")
        return

    typer.echo("Migration status:\n")
    pending_count = 0
    for name, path in migrations:
        if name in applied:
            typer.echo(f"  [x] {name}")
        else:
            typer.echo(f"  [ ] {name}")
            pending_count += 1

    typer.echo(f"\n{len(applied)} applied, {pending_count} pending")


@app.command()
def rollback(
    steps: Annotated[
        int, typer.Option("--steps", "-s", help="Number of migrations to rollback")
    ] = 1,
) -> None:
    """Rollback the last migration(s)."""
    config = Config.load()
    db_url = config.database.get_url()
    migrations_dir = config.migrations.directory

    async def _rollback() -> int:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=2)
        rolled_back = 0
        try:
            await ensure_migrations_table(pool)
            applied = await get_applied_migrations(pool)

            if not applied:
                typer.echo("No migrations to rollback.")
                return 0

            # Get migrations in reverse order
            applied_list = list(applied.keys())
            to_rollback = (
                applied_list[-steps:] if steps <= len(applied_list) else applied_list
            )
            to_rollback.reverse()

            for name in to_rollback:
                down_path = migrations_dir / f"{name}.down.sql"
                if not down_path.exists():
                    typer.echo(
                        f"Warning: No rollback file for {name}, skipping", err=True
                    )
                    continue

                typer.echo(f"Rolling back: {name}")
                sql = down_path.read_text()

                async with pool.acquire() as conn:
                    await conn.execute(sql)

                await remove_migration_record(pool, name)
                rolled_back += 1
                typer.echo(f"  Rolled back: {name}")

            return rolled_back
        finally:
            await pool.close()

    count = asyncio.run(_rollback())
    if count > 0:
        typer.echo(f"\nRolled back {count} migration(s).")


@app.command()
def init() -> None:
    """Initialize a new derp.toml configuration file."""
    config_path = Path(CONFIG_FILE)
    if config_path.exists():
        typer.echo(f"{CONFIG_FILE} already exists.")
        raise typer.Exit(1)

    default_config = """\
[database]
env = "DATABASE_URL"  # Environment variable containing the database URL

[migrations]
dir = "./migrations"
schema = "src/schema.py"
"""

    config_path.write_text(default_config)
    typer.echo(f"Created {CONFIG_FILE}")
    typer.echo("\nSet your database URL:")
    typer.echo(
        f"  export {DEFAULT_DATABASE_URL_ENV}=postgresql://user:pass@localhost:5432/dbname"
    )
    typer.echo("\nThen run:")
    typer.echo("  derp generate --name initial")
    typer.echo("  derp migrate")


if __name__ == "__main__":
    app()
