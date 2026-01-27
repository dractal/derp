# Derp ORM

A strongly-typed async Python ORM for PostgreSQL, inspired by [Drizzle ORM](https://orm.drizzle.team/) and [SQLModel](https://sqlmodel.tiangolo.com/).

## Features

- **Async-first** - Built on asyncpg for high-performance async PostgreSQL access
- **Type-safe** - Pydantic-based table definitions with full type hints
- **Fluent query builder** - Drizzle-style chainable API for SELECT, INSERT, UPDATE, DELETE
- **Pure SQL migrations** - Simple .sql migration files like Drizzle
- **CLI tooling** - Typer-based CLI for migration management

## Installation

```bash
pip install derp
```

## Quick Start

### Define Tables

```python
from datetime import datetime
from derp import Table, Field, ForeignKey
from derp.fields import Serial, Varchar, Integer, Timestamp

class User(Table, table_name="users"):
    id: int = Field(Serial(), primary_key=True)
    name: str = Field(Varchar(255))
    email: str = Field(Varchar(255), unique=True)
    created_at: datetime = Field(Timestamp(), default="now()")

class Post(Table, table_name="posts"):
    id: int = Field(Serial(), primary_key=True)
    title: str = Field(Varchar(255))
    author_id: int = Field(Integer(), foreign_key=ForeignKey("users.id"))
```

### Query Data

```python
from derp import Derp, eq, gt, and_

async with Derp("postgresql://user:pass@localhost:5432/mydb") as db:
    # SELECT
    users = await db.select(User).where(eq(User.c.name, "Alice")).execute()

    # SELECT with conditions
    active_users = await (
        db.select(User)
        .where(and_(gt(User.c.id, 5), eq(User.c.name, "Bob")))
        .order_by(User.c.created_at, "DESC")
        .limit(10)
        .execute()
    )

    # INSERT
    new_user = await (
        db.insert(User)
        .values(name="Charlie", email="charlie@example.com")
        .returning(User)
        .execute()
    )

    # UPDATE
    await (
        db.update(User)
        .set(name="Charles")
        .where(eq(User.c.id, 1))
        .execute()
    )

    # DELETE
    await db.delete(User).where(eq(User.c.id, 1)).execute()

    # JOINs
    posts_with_authors = await (
        db.select(Post, User.c.name)
        .from_(Post)
        .inner_join(User, eq(Post.c.author_id, User.c.id))
        .execute()
    )
```

### Transactions

```python
async with db.transaction():
    await db.insert(User).values(name="Alice", email="alice@example.com").execute()
    await db.update(Post).set(title="Updated").where(eq(Post.c.id, 1)).execute()
    # Automatically commits on success, rolls back on exception
```

## Field Types

| Type | PostgreSQL | Python |
|------|------------|--------|
| `Serial()` | SERIAL | int |
| `BigSerial()` | BIGSERIAL | int |
| `SmallInt()` | SMALLINT | int |
| `Integer()` | INTEGER | int |
| `BigInt()` | BIGINT | int |
| `Varchar(n)` | VARCHAR(n) | str |
| `Text()` | TEXT | str |
| `Char(n)` | CHAR(n) | str |
| `Boolean()` | BOOLEAN | bool |
| `Timestamp()` | TIMESTAMP | datetime |
| `Date()` | DATE | date |
| `Time()` | TIME | time |
| `Numeric(p, s)` | NUMERIC(p, s) | Decimal |
| `Real()` | REAL | float |
| `DoublePrecision()` | DOUBLE PRECISION | float |
| `UUID()` | UUID | uuid.UUID |
| `JSON()` | JSON | dict |
| `JSONB()` | JSONB | dict |
| `Array(T)` | T[] | list |

## Expression Operators

```python
from derp import eq, ne, gt, gte, lt, lte, and_, or_, not_
from derp import like, ilike, in_, not_in, is_null, is_not_null, between

# Comparison
eq(User.c.id, 1)           # id = 1
ne(User.c.id, 1)           # id <> 1
gt(User.c.age, 18)         # age > 18
gte(User.c.age, 18)        # age >= 18
lt(User.c.age, 65)         # age < 65
lte(User.c.age, 65)        # age <= 65

# Logical
and_(eq(User.c.name, "Alice"), gt(User.c.age, 18))
or_(eq(User.c.role, "admin"), eq(User.c.role, "moderator"))
not_(eq(User.c.active, False))

# Pattern matching
like(User.c.name, "%alice%")
ilike(User.c.email, "%@GMAIL.COM")

# Membership
in_(User.c.id, [1, 2, 3])
not_in(User.c.status, ["banned", "suspended"])

# Null checks
is_null(User.c.deleted_at)
is_not_null(User.c.email)

# Range
between(User.c.age, 18, 65)
```

## CLI Usage

### Configuration

Create a `derp.toml` in your project root:

```toml
[database]
env = "DATABASE_URL"  # Environment variable containing the database URL

[migrations]
dir = "./migrations"
schema = "src/schema.py"  # Path to your Table definitions
```

Set your database URL:

```bash
export DATABASE_URL=postgresql://user:pass@localhost:5432/mydb
```

### Commands

```bash
# Initialize configuration
derp init

# Generate migration from schema changes
derp generate --name add_users_table

# Apply pending migrations
derp migrate

# Push schema directly (dev mode, no migration files)
derp push

# Show migration status
derp status

# Rollback last migration
derp rollback

# Rollback multiple migrations
derp rollback --steps 3
```

### Migration Files

Migrations are pure SQL files:

```sql
-- migrations/0001_add_users_table.sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT now()
);
```

Rollback files (optional):

```sql
-- migrations/0001_add_users_table.down.sql
DROP TABLE users;
```

## Project Structure

```
src/derp/
├── __init__.py          # Public API exports
├── fields.py            # PostgreSQL type definitions
├── table.py             # Table base class (Pydantic-based)
├── engine.py            # Async query engine (asyncpg wrapper)
├── schema.py            # Schema introspection & DDL generation
├── query/
│   ├── __init__.py
│   ├── builder.py       # Query builders (SELECT, INSERT, UPDATE, DELETE)
│   ├── expressions.py   # WHERE clause operators
│   └── types.py         # Type definitions
└── cli/
    ├── __init__.py
    └── main.py          # Typer CLI commands
```

## Development

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest

# Lint and format
uv run ruff check src/
uv run ruff format src/

# Type check
uv run ty check src/
```

## License

MIT
