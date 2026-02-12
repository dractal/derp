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

## Style Guidelines

- Python 3.12+ required (use `class Foo[T]:` syntax for generics).
- Use `from __future__ import annotations` wherever possible.
- Prefer dataclasses for simple data containers.
- Keep query builder methods chainable (return `self`).
- Fields default to NOT NULL (`nullable=False`).

## Configuration

CLI uses `derp.toml` with typed config in `src/derp/config.py`:

## Code style

- Write comprehensive tests before you add specific functionality.
- Avoid using magic strings, use `enum.StrEnum` instead where possible.
- The final API exposed to the user should be simple and minimal.
