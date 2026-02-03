# Derp

Derp is a library for building Python backends for web applications. Derp consists of the following -

- A strongly-typed Python ORM inspired by [Drizzle ORM](https://orm.drizzle.team/).
- An asynchronous strongly typed storage client.
- An integrated authentication library built on JWT.

## Features

- **Async-first** - Built on asyncpg and aibotocore for high-performance async operations
- **Type-safe** - Pydantic-based table definitions with full type hints
- **Pure SQL migrations** - Simple .sql migration files like Drizzle
- **CLI** - Typer-based CLI for migration management

## Installation

```bash
uv add derp
```

## Quick Start

### Define Tables

```python
from datetime import datetime
from derp.orm import Table, Field, ForeignKey
from derp.orm.fields import Serial, Varchar, Integer, Timestamp

class User(Table, table="users"):
    id: int = Field(Serial(), primary_key=True)
    name: str = Field(Varchar(255))
    email: str = Field(Varchar(255), unique=True)
    created_at: datetime = Field(Timestamp(), default="now()")

class Post(Table, table="posts"):
    id: int = Field(Serial(), primary_key=True)
    title: str = Field(Varchar(255))
    author_id: int = Field(Integer(), foreign_key=ForeignKey(User))
```

### Query Data

```python
from derp import DatabaseConfig, DerpClient, DerpConfig


config = DerpConfig(
    database=DatabaseConfig(db_url="postgresql://user:pass@localhost:5432/mydb")
)
derp = DerpClient(config)

async with derp:
    # SELECT
    users = await derp.db.select(User).where(User.c.name == "Alice").execute()

    # SELECT with conditions
    active_users = await (
        derp.db.select(User)
        .where((User.c.id == 5) & (User.c.name == "Bob"))
        .order_by(User.c.created_at, asc=False)
        .limit(10)
        .execute()
    )

    # INSERT
    new_user = await (
        derp.db.insert(User)
        .values(name="Charlie", email="charlie@example.com")
        .returning(User)
        .execute()
    )

    # UPDATE
    await (
        derp.db.update(User)
        .set(name="Charles")
        .where(User.c.id == 1)
        .execute()
    )

    # DELETE
    await derp.db.delete(User).where(User.c.id == 1).execute()

    # JOINs
    posts_with_authors = await (
        derp.db.select(Post, User.c.name)
        .from_(Post)
        .inner_join(User, Post.c.author_id == User.c.id)
        .execute()
    )
```

### Transactions

```python
async with derp:
    async with derp.db.transaction():
        await derp.db.insert(User).values(name="Alice", email="alice@example.com").execute()
        await derp.db.update(Post).set(title="Updated").where(Post.c.id == 1).execute()
        # Automatically commits on success, rolls back on exception
```

## Storage and Authentication

The Derp client can be configured with storage and authentication.

```python
from derp import AuthConfig, DerpClient, DerpConfig, EmailConfig, JWTConfig, StorageConfig


config = DerpConfig(
    database=DatabaseConfig(db_url="postgresql://localhost:5432/mydb"),
    storage=StorageConfig(
        endpoint_url="http://localhost:9000",
        access_key_id="minioadmin",
        secret_access_key="minioadmin",
    )
    auth=AuthConfig[User](
        user_table_name="users",
        email=EmailConfig(
            site_name="My Website",
            site_url="http://localhost:3000",
            from_email="no-reply@example.com",
            smtp_host="smtp.example.com",
            smtp_port=587,
            smtp_user="smtp-user",
            smtp_password="smtp-password",
        ),
        jwt=JWTConfig(secret="my-secret"),
    ),
)
derp = DerpClient(config)
```

### Define tables

```python
from datetime import datetime
from derp.auth import BaseUser, AuthSession, AuthRefreshToken, AuthMagicLink  # Pre-defined tables
from derp.orm import Table, Field, ForeignKey
from derp.orm.fields import Serial, Varchar, Integer, Timestamp

class User(BaseUser, table="users"):
    avatar_url: str = Field(Varchar(255), default=None)

class Post(Table, table="posts"):
    id: int = Field(Serial(), primary_key=True)
    title: str = Field(Varchar(255))
    author_id: int = Field(Integer(), foreign_key=ForeignKey(User))
```

### Authenticate Users

```python
async with derp:
    user, _ = await derp.auth.sign_up(
        email="test@example.com", password="password123"
    )
    await derp.auth.confirm_email(user.confirmation_token)

    user, _ = await derp.auth.sign_in_with_password(
        email="test@example.com", password="password123"
    )
```

### Upload files

```python
async with derp:
    user = await derp.auth.get_user(email="test@example.com")

    if user.avatar_url
      await derp.storage.upload_file(
          bucket="avatars",
          key="test.txt",
          data=b"Hello, world!",
          content_type="text/plain",
      )
```

## Field Types

| Type                | PostgreSQL       | Python    |
| ------------------- | ---------------- | --------- |
| `Serial()`          | SERIAL           | int       |
| `BigSerial()`       | BIGSERIAL        | int       |
| `SmallInt()`        | SMALLINT         | int       |
| `Integer()`         | INTEGER          | int       |
| `BigInt()`          | BIGINT           | int       |
| `Varchar(n)`        | VARCHAR(n)       | str       |
| `Text()`            | TEXT             | str       |
| `Char(n)`           | CHAR(n)          | str       |
| `Boolean()`         | BOOLEAN          | bool      |
| `Timestamp()`       | TIMESTAMP        | datetime  |
| `Date()`            | DATE             | date      |
| `Time()`            | TIME             | time      |
| `Numeric(p, s)`     | NUMERIC(p, s)    | Decimal   |
| `Real()`            | REAL             | float     |
| `DoublePrecision()` | DOUBLE PRECISION | float     |
| `UUID()`            | UUID             | uuid.UUID |
| `JSON()`            | JSON             | dict      |
| `JSONB()`           | JSONB            | dict      |
| `Array(T)`          | T[]              | list      |

## Expression Operators

```python
# Field operators
User.c.id == 1
User.c.id != 1
User.c.age > 18
User.c.age >= 18
User.c.age < 65
User.c.age <= 65
~User.c.active

# Expression operators
(User.c.name == "Alice") & (User.c.age > 18)
(User.c.role == "admin") | (User.c.role == "moderator")
~(User.c.name == "Bob")

# Pattern matching
User.c.name.like("%alice%")
User.c.email.ilike("%@GMAIL.COM")

# Membership
User.c.id.in_([1, 2, 3])
User.c.status.not_in(["banned", "suspended"])

# Null checks
User.c.deleted_at.is_null()
User.c.email.is_not_null()

# Range
User.c.age.between(18, 65)
```

## CLI Usage

### Configuration

Create a `derp.toml` in your project root:

```toml
[database]
db_url = "$DATABASE_URL"  # Environment variable containing the database URL
schema_path = "src/schemas/*"  # Path to your Table definitions

[database.migrations]
dir = "./migrations"
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
