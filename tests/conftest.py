"""Pytest configuration and fixtures for tests."""

from __future__ import annotations

import asyncio
import os
import tempfile
from collections.abc import Generator
from pathlib import Path

import asyncpg
import pytest
import testing.postgresql

# Store the original working directory at module load time
_ORIGINAL_CWD = os.getcwd()


@pytest.fixture(autouse=True)
def restore_cwd() -> Generator[None, None, None]:
    """Restore the original working directory after each test.

    This is needed because some tests change directories to temp directories,
    and when those directories are deleted, the cwd becomes invalid.
    """
    yield
    try:
        os.getcwd()
    except (FileNotFoundError, OSError):
        # cwd was deleted, restore to original
        os.chdir(_ORIGINAL_CWD)


# Create a shared PostgreSQL instance for all tests in the session
@pytest.fixture(scope="session")
def postgresql_instance() -> Generator[testing.postgresql.Postgresql, None, None]:
    """Create a PostgreSQL instance for the test session."""
    with testing.postgresql.Postgresql() as postgresql:
        yield postgresql


@pytest.fixture(scope="function")
def database_url(postgresql_instance: testing.postgresql.Postgresql) -> str:
    """Get the database URL for tests."""
    return postgresql_instance.url()


@pytest.fixture(scope="function")
def clean_database(
    postgresql_instance: testing.postgresql.Postgresql,
) -> Generator[str, None, None]:
    """Provide a clean database for each test.

    Drops and recreates all tables after each test.
    """
    url = postgresql_instance.url()

    yield url

    # Clean up after test
    async def cleanup():
        conn = await asyncpg.connect(url)
        try:
            # Drop all tables in public schema
            await conn.execute(
                """
                DO $$ DECLARE
                    r RECORD;
                BEGIN
                    FOR r IN (SELECT tablename FROM pg_tables
                              WHERE schemaname = 'public') LOOP
                        EXECUTE 'DROP TABLE IF EXISTS ' ||
                                quote_ident(r.tablename) || ' CASCADE';
                    END LOOP;
                END $$;
                """
            )
            # Drop all types (enums)
            await conn.execute(
                """
                DO $$ DECLARE
                    r RECORD;
                BEGIN
                    FOR r IN (SELECT typname FROM pg_type t
                              JOIN pg_namespace n ON t.typnamespace = n.oid
                              WHERE n.nspname = 'public' AND t.typtype = 'e') LOOP
                        EXECUTE 'DROP TYPE IF EXISTS ' ||
                                quote_ident(r.typname) || ' CASCADE';
                    END LOOP;
                END $$;
                """
            )
        finally:
            await conn.close()

    asyncio.run(cleanup())


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def temp_migrations_dir(temp_dir: Path) -> Path:
    """Create a temporary migrations directory."""
    migrations_dir = temp_dir / "drizzle"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    return migrations_dir


@pytest.fixture
def temp_schema_file(temp_dir: Path) -> Path:
    """Create a temporary schema file with sample tables."""
    schema_content = '''"""Test schema for migrations."""

from datetime import datetime

from derp.orm import Table
from derp.orm.fields import (
    Field,
    ForeignKey,
    ForeignKeyAction,
    Integer,
    Serial,
    Timestamp,
    Varchar,
    Text,
    Boolean,
)


class User(Table, table="users"):
    id: int = Field(Serial(), primary_key=True)
    name: str = Field(Varchar(255))
    email: str = Field(Varchar(255), unique=True)
    is_active: bool = Field(Boolean(), default=True)
    created_at: datetime = Field(Timestamp(), default="now()")


class Post(Table, table="posts"):
    id: int = Field(Serial(), primary_key=True)
    title: str = Field(Varchar(255))
    content: str = Field(Text(), nullable=True)
    author_id: int = Field(
        Integer(),
        foreign_key=ForeignKey("users.id", on_delete=ForeignKeyAction.CASCADE),
        index=True,
    )
    published: bool = Field(Boolean(), default=False)
    created_at: datetime = Field(Timestamp(), default="now()")
'''
    schema_path = temp_dir / "schema.py"
    schema_path.write_text(schema_content)
    return schema_path


@pytest.fixture
def temp_config_file(
    temp_dir: Path,
    temp_schema_file: Path,
    temp_migrations_dir: Path,
) -> Path:
    """Create a temporary derp.toml config file."""
    config_content = f'''[database]
db_url = "$TEST_DATABASE_URL"
schema_path = "{temp_schema_file}"

[database.migrations]
dir = "{temp_migrations_dir}"

[database.introspect]
schemas = ["public"]
exclude_tables = ["_derp_migrations"]
'''
    config_path = temp_dir / "derp.toml"
    config_path.write_text(config_content)
    return config_path


@pytest.fixture
def cli_env(
    clean_database: str,
    temp_dir: Path,
    temp_config_file: Path,
) -> Generator[dict[str, str], None, None]:
    """Set up environment for CLI tests."""
    old_cwd = os.getcwd()
    old_env = os.environ.copy()

    try:
        os.chdir(temp_dir)
        os.environ["TEST_DATABASE_URL"] = clean_database
        yield {"TEST_DATABASE_URL": clean_database, "cwd": str(temp_dir)}
    finally:
        os.chdir(old_cwd)
        os.environ.clear()
        os.environ.update(old_env)


@pytest.fixture
def async_pool(clean_database: str):
    """Create an async connection pool for tests."""

    async def _create_pool() -> asyncpg.Pool:
        return await asyncpg.create_pool(clean_database, min_size=1, max_size=2)

    return _create_pool
