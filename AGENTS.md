# Agent Instructions for Derp ORM

This document provides guidance for AI agents working on the Derp ORM codebase.

## Project Overview

Derp is an async Python ORM for PostgreSQL with these core components:

| Component | File(s) | Purpose |
|-----------|---------|---------|
| Field Types | `src/derp/fields.py` | PostgreSQL column type definitions |
| Table Base | `src/derp/table.py` | Pydantic-based table metaclass |
| Query Builder | `src/derp/query/builder.py` | SELECT/INSERT/UPDATE/DELETE builders |
| Expressions | `src/derp/query/expressions.py` | WHERE clause operators |
| Engine | `src/derp/engine.py` | asyncpg connection pool wrapper |
| Schema | `src/derp/schema.py` | DDL generation & DB introspection |
| CLI | `src/derp/cli/main.py` | Typer migration commands |

## Architecture Patterns

### Table Definition Pattern

Tables use a custom metaclass (`TableMeta`) that:
1. Extracts `FieldInfo` objects from class attributes BEFORE Pydantic processes them
2. Stores column metadata in `__columns__` class variable
3. Creates a `ColumnAccessor` at `Table.c` for query building

```python
# User defines:
class User(Table, table_name="users"):
    id: int = Field(Serial(), primary_key=True)

# Metaclass creates:
# - User.__columns__ = {"id": FieldInfo(...)}
# - User.c = ColumnAccessor({"id": FieldInfo(...)})
# - User.c.id returns FieldInfo with _table_name="users", _field_name="id"
```

### Query Builder Pattern

Query builders are generic classes that:
1. Accept a connection pool and build SQL incrementally
2. Use `build()` to generate `(sql, params)` tuple with $N placeholders
3. Use `execute()` to run the query and map results to models

```python
# Pattern: Builder stores state, build() generates SQL, execute() runs it
query = SelectQuery[User](pool, (User,))
query.where(eq(User.c.id, 1))  # Mutates internal state
sql, params = query.build()     # Returns ("SELECT ... WHERE ...", [1])
results = await query.execute() # Runs query, returns list[User]
```

### Expression Pattern

Expressions form a tree that generates parameterized SQL:

```python
# eq(User.c.id, 1) creates:
BinaryOp(left=FieldInfo(...), operator="=", right=Literal(1))

# to_sql(params) generates:
# - Appends 1 to params list
# - Returns "(users.id = $1)"
```

## Common Modifications

### Adding a New Field Type

1. Add dataclass in `src/derp/fields.py`:
```python
@dataclass
class MyType(FieldType):
    """Description."""
    some_param: int = 0

    def sql_type(self) -> str:
        return f"MYTYPE({self.some_param})"
```

2. Export in `src/derp/__init__.py`
3. Add tests in `tests/test_fields.py`

### Adding a New Expression Operator

1. Add expression class in `src/derp/query/expressions.py`:
```python
@dataclass
class MyOp(Expression):
    column: Expression | FieldInfo
    value: Any

    def to_sql(self, params: list[Any]) -> str:
        col_sql = _expr_to_sql(self.column, params)
        params.append(self.value)
        return f"({col_sql} MY_OP ${len(params)})"

def my_op(column: FieldInfo | Expression, value: Any) -> MyOp:
    """My operation."""
    return MyOp(_to_expr(column), value)
```

2. Export in `src/derp/query/__init__.py`
3. Export in `src/derp/__init__.py`
4. Add tests in `tests/test_query.py`

### Adding a New Query Method

1. Add method to relevant builder class in `src/derp/query/builder.py`
2. Update `build()` method to include new clause in SQL generation
3. Add tests in `tests/test_query.py`

### Adding a New CLI Command

1. Add command in `src/derp/cli/main.py`:
```python
@app.command()
def my_command(
    arg: Annotated[str, typer.Option("--arg", "-a", help="Description")] = "default",
) -> None:
    """Command description."""
    config = Config.load()
    # Implementation
```

## Testing Guidelines

- All tests are in `tests/` directory
- Tests use pytest with pytest-asyncio for async tests
- Run tests: `uv run pytest tests/ -v`
- Tests should not require a real database (mock asyncpg or test build() output)

### Test Patterns

```python
# Testing query building (no DB needed)
def test_select_where():
    query = SelectQuery[User](None, (User,)).where(eq(User.c.id, 1))
    sql, params = query.build()
    assert "WHERE" in sql
    assert params == [1]

# Testing field types (no DB needed)
def test_varchar():
    assert Varchar(255).sql_type() == "VARCHAR(255)"
```

## Code Quality

Before committing, always run:

```bash
uv run ruff check src/       # Linting
uv run ruff format src/      # Formatting
uv run ty check src/         # Type checking
uv run pytest tests/         # Tests
```

### Style Guidelines

- Use Python 3.12+ features (type parameter syntax `class Foo[T]:`)
- Prefer dataclasses for simple data containers
- Use `from __future__ import annotations` in all files
- Fields default to NOT NULL (`nullable=False`)
- Keep query builder methods chainable (return `self`)

## Configuration

The CLI uses strongly-typed configuration:

```python
# src/derp/cli/main.py
@dataclass
class DatabaseConfig:
    env: str = "DATABASE_URL"  # Env var name, not the URL itself

@dataclass
class MigrationsConfig:
    dir: str = "./migrations"
    schema: str | None = None

@dataclass
class Config:
    database: DatabaseConfig
    migrations: MigrationsConfig
```

Database URLs are NEVER stored in config files - only the environment variable name.

## Key Files to Understand

1. **`table.py`** - The metaclass magic that makes table definitions work
2. **`query/expressions.py`** - How WHERE clauses become parameterized SQL
3. **`query/builder.py`** - How queries are built and executed
4. **`cli/main.py`** - CLI command structure and config loading

## Gotchas

1. **Pydantic Integration**: The metaclass must extract `FieldInfo` before Pydantic sees it, otherwise Pydantic will try to process it as a field default.

2. **Column Access**: Use `User.c.id` not `User.id` for query building. Direct attribute access doesn't work due to Pydantic's `__getattr__`.

3. **Parameter Numbering**: asyncpg uses `$1, $2, ...` placeholders (not `?` or `%s`). The `params` list is built incrementally as expressions are converted to SQL.

4. **Generic Classes**: Use Python 3.12 syntax `class Foo[T: Table]:` not `class Foo(Generic[T]):`.
