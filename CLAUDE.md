# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
uv sync

# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/test_query.py -v

# Run a specific test
uv run pytest tests/test_query.py::test_select_where -v

# Lint and format
uv run ruff check --select I --fix src/
uv run ruff format src/

# Type check
uv run ty check src/
```

## Architecture

Derp is an async Python ORM for PostgreSQL with Drizzle-inspired migrations.

### Core Layers

| Layer | Location | Purpose |
|-------|----------|---------|
| ORM | `src/derp/orm/` | Table definitions, query builder, field types |
| Migrations | `src/derp/orm/migrations/` | Snapshot-based migration system |
| CLI | `src/derp/cli/` | Typer commands (generate, migrate, push, pull, status, check, drop) |
| Auth | `src/derp/auth/` | Authentication client and tokens |
| Storage | `src/derp/storage/` | Storage client |

### Table System (src/derp/orm/table.py)

The `TableMeta` metaclass extracts `FieldInfo` objects BEFORE Pydantic processes them:
1. Stores column metadata in `__columns__` class variable
2. Creates `ColumnAccessor` at `Table.c` for query building
3. Use `User.c.id` (not `User.id`) for query building

### Query Builder (src/derp/orm/query/)

Builders use a fluent pattern:
- `build()` generates `(sql, params)` tuple with $N placeholders (asyncpg style)
- `execute()` runs the query and maps results to models
- Expressions form a tree that generates parameterized SQL

### Migration System (src/derp/orm/migrations/)

- **snapshot/**: Schema models and diffing logic
- **convertors/**: Convert PostgreSQL objects to SQL statements (column, constraint, enum, index, policy, table, etc.)
- **introspect/**: PostgreSQL schema introspection
- **journal.py**: Tracks applied migrations
- **safety.py**: Detects destructive operations

## Style Guidelines

- Python 3.12+ required (use `class Foo[T]:` syntax for generics)
- Use `from __future__ import annotations` in all files
- Prefer dataclasses for simple data containers
- Keep query builder methods chainable (return `self`)
- Fields default to NOT NULL (`nullable=False`)

## Configuration

CLI uses `derp.toml` with typed config in `src/derp/cli/config.py`:
- `[database]`: env var name for DATABASE_URL (never store URLs directly)
- `[migrations]`: migration directory, schema path
- `[introspect]`: schema selection, table filtering
