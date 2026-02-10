# Derp

Derp is an async backend toolkit for Python web apps. It includes:

- A strongly typed PostgreSQL ORM inspired by [Drizzle ORM](https://orm.drizzle.team/).
- Built-in authentication (JWT + email/OAuth).
- S3-compatible object storage client.
- In memory KV storage support (Valkey backend).
- Integrated stripe payments client.

## Features

- Async-first clients and query execution.
- Typed models/configs with Pydantic.
- Unified lifecycle via one `DerpClient` session.
- Pure SQL migration workflow with Typer CLI.
- Optional modules through one config (`storage`, `auth`, `kv`, `payments`).

## Installation

```bash
uv add derp
```

## Quick Start

### Define Tables

```python
from datetime import datetime

from derp.orm import Table
from derp.orm.fields import Field, ForeignKey, Integer, Serial, Timestamp, Varchar


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
    database=DatabaseConfig(
        db_url="postgresql://user:pass@localhost:5432/mydb",
        schema_path="src/schema.py",
    )
)
derp = DerpClient(config)

async with derp:
    users = await derp.db.select(User).where(User.c.name == "Alice").execute()

    await (
        derp.db.insert(User)
        .values(name="Charlie", email="charlie@example.com")
        .execute()
    )
```

## One Client, Many Services

`DerpClient` manages the lifecycle for database and optional modules in one async
session.

```python
from derp import (
    AuthConfig,
    DatabaseConfig,
    DerpClient,
    DerpConfig,
    EmailConfig,
    JWTConfig,
    KVConfig,
    PaymentsConfig,
    StorageConfig,
)


config = DerpConfig(
    database=DatabaseConfig(
        db_url="postgresql://localhost:5432/mydb",
        schema_path="src/schema.py",
    ),
    storage=StorageConfig(
        endpoint_url="http://localhost:9000",
        access_key_id="minioadmin",
        secret_access_key="minioadmin",
        use_ssl=False,
        region="us-east-1",
    ),
    auth=AuthConfig(
        user_table_name="users",
        email=EmailConfig(
            site_name="My App",
            site_url="http://localhost:3000",
            from_email="no-reply@example.com",
            smtp_host="smtp.example.com",
            smtp_port=587,
            smtp_user="smtp-user",
            smtp_password="smtp-password",
        ),
        jwt=JWTConfig(secret="my-secret"),
    ),
    kv=KVConfig(
        valkey={"host": "localhost", "port": 6379},
    ),
    payments=PaymentsConfig(
        api_key="sk_test_...",
        webhook_secret="whsec_...",
    ),
)

derp = DerpClient(config)
```

## Payments (Stripe)

Payments are exposed via `derp.payments` when `payments` is configured.

### Create Customer + Checkout Session

```python
from derp.payments import CheckoutSessionMode


async with derp:
    customer = await derp.payments.create_customer(
        email="customer@example.com",
        name="Customer Name",
        metadata={"tenant_id": "tenant_123"},
    )

    session = await derp.payments.create_checkout_session(
        mode=CheckoutSessionMode.SUBSCRIPTION,
        success_url="https://app.example.com/billing/success",
        cancel_url="https://app.example.com/billing/cancel",
        line_items=[{"price_id": "price_123", "quantity": 1}],
        customer_id=customer.id,
        client_reference_id="org_42",
        allow_promotion_codes=True,
        idempotency_key="checkout-org-42-001",
    )

    checkout_url = session.url
```

### Verify Webhook Event

```python
payload = await request.body()
signature = request.headers["stripe-signature"]

async with derp:
    event = await derp.payments.verify_webhook_event(
        payload=payload,
        signature=signature,
    )

if event.type == "checkout.session.completed":
    # handle event.data_object
    ...
```

### Payments API Surface

- `create_customer(...)`
- `retrieve_customer(customer_id, *, expand=None)`
- `update_customer(customer_id, ...)`
- `create_checkout_session(...)`
- `retrieve_checkout_session(session_id, *, expand=None)`
- `expire_checkout_session(session_id)`
- `verify_webhook_event(...)`

All write APIs use function arguments (no input model objects).

## Authentication, Storage, and KV

```python
async with derp:
    user, _ = await derp.auth.sign_up(
        email="user@example.com",
        password="password123",
    )

    await derp.storage.upload_file(
        bucket="avatars",
        key=f"{user.id}.png",
        data=b"...",
        content_type="image/png",
    )

    await derp.kv.store.set(b"user:last_seen", b"2026-01-01T00:00:00Z")
```

## Configuration (`derp.toml`)

```toml
[database]
db_url = "$DATABASE_URL"
schema_path = "src/schema.py"
# replica_url = "$REPLICA_DATABASE_URL"

[database.migrations]
dir = "./migrations"

# [storage]
# endpoint_url = "http://localhost:9000"
# access_key_id = "$AWS_ACCESS_KEY_ID"
# secret_access_key = "$AWS_SECRET_ACCESS_KEY"
# region = "us-east-1"

# [auth]
# user_table_name = "users"

# [auth.email]
# site_name = "My App"
# site_url = "https://example.com"
# from_email = "no-reply@example.com"
# smtp_host = "smtp.example.com"
# smtp_port = 587
# smtp_user = "$SMTP_USER"
# smtp_password = "$SMTP_PASSWORD"

# [auth.jwt]
# secret = "$JWT_SECRET"

# [kv.valkey]
# host = "localhost"
# port = 6379

# [payments]
# api_key = "$STRIPE_SECRET_KEY"
# webhook_secret = "$STRIPE_WEBHOOK_SECRET"
# max_network_retries = 2
# timeout_seconds = 30.0
```

## CLI

```bash
derp init
derp generate --name add_users_table
derp migrate
derp push
derp pull
derp status
derp check
derp drop
derp studio
derp version
```

## Project Structure

```text
src/derp/
├── __init__.py
├── config.py
├── derp_client.py
├── orm/
├── auth/
├── storage/
├── kv/
├── payments/
└── cli/
```

## Development

```bash
uv sync
uv run pytest
uv run ruff check src/
uv run ruff format src/
uv run ty check src/
```

## License

MIT
