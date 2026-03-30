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
uv run ruff check --select I --fix src/ tests/ examples/ benchmarks/
uv run ruff format src/ tests/ examples/ benchmarks/

# Type check
uv run ty check src/ tests/ examples/ benchmarks/
```

## Style Guidelines

- Python 3.12+ required (use `class Foo[T]:` syntax for generics).
- Use `from __future__ import annotations` wherever possible.
- Prefer dataclasses for simple data containers.

## Configuration

CLI uses `derp.toml` with typed config in `src/derp/config.py`.

## Modules

- **ORM** (`src/derp/orm/`) — PostgreSQL query builder, migrations, schema diffing
- **Auth** (`src/derp/auth/`) — Native, Clerk, Cognito, Supabase backends with orgs/RBAC
- **Payments** (`src/derp/payments/`) — Stripe: checkout, Connect, payouts, webhooks
- **Storage** (`src/derp/storage/`) — S3: upload/download, signed URLs, batch delete, copy
- **KV** (`src/derp/kv/`) — Valkey: cache, stampede protection, idempotency, rate limiting
- **Queue** (`src/derp/queue/`) — Celery/Vercel: enqueue, schedules, cron
- **AI** (`src/derp/ai/`) — OpenAI, fal, Modal: chat, streaming, Vercel/TanStack protocol adapters

## Code style

- The final API exposed to the user should be simple, minimal, and non-duplicative.
- Write comprehensive tests before you add specific functionality.
- Avoid using magic strings, use `enum.StrEnum` instead where possible.
