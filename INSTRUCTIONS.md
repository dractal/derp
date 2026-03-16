# Agent Instructions for Building Projects with Derp

Step-by-step instructions for an AI agent developing a project with `derp-py` — an async Python backend framework with ORM, auth, storage, payments, KV, and queues.

## Prerequisites

- Python 3.12+
- PostgreSQL database
- `uv` package manager

## Step 1: Project Setup

```bash
uv init my-project
cd my-project
uv add derp-py
derp init
```

This creates `derp.toml`. Set your database URL via environment variable:

```
# .env
DATABASE_URL=postgresql://user:pass@localhost:5432/mydb
```

Config uses `$ENV_VAR` syntax — never store credentials directly:

```toml
[database]
db_url = "$DATABASE_URL"
schema_path = "app/models.py"
```

### Recommended structure

```
my-project/
├── derp.toml
├── .env
├── app/
│   ├── models.py          # Table definitions
│   ├── main.py            # FastAPI entry point
│   ├── dependencies.py    # FastAPI dependencies
│   ├── schemas.py         # Pydantic request/response models
│   └── routers/
└── migrations/
```

Use `schema_path = "app/*"` to load tables from all files in a directory.

## Step 2: Define Tables

```python
from __future__ import annotations

import uuid
from datetime import datetime

from derp.orm import (
    Table, Field, UUID, Varchar, Text, Integer,
    Boolean, Timestamp, ForeignKey, ForeignKeyAction,
)


class User(Table, table="users"):
    id: uuid.UUID = Field(UUID(), primary_key=True, default="gen_random_uuid()")
    name: str = Field(Varchar(255))
    email: str = Field(Varchar(255), unique=True)
    age: int | None = Field(Integer(), nullable=True)
    is_active: bool = Field(Boolean(), default="true")
    created_at: datetime = Field(Timestamp(with_timezone=True), default="now()")


class Post(Table, table="posts"):
    id: uuid.UUID = Field(UUID(), primary_key=True, default="gen_random_uuid()")
    author_id: uuid.UUID = Field(
        UUID(),
        foreign_key=ForeignKey(User, on_delete=ForeignKeyAction.CASCADE),
        index=True,
    )
    title: str = Field(Varchar(255))
    content: str = Field(Text())
    published: bool = Field(Boolean(), default="false")
    created_at: datetime = Field(Timestamp(with_timezone=True), default="now()")
```

### Key rules

1. **Always use `from __future__ import annotations`**.
2. **Fields default to NOT NULL**. Use `nullable=True` for optional columns.
3. **Use `Table.c.column`** for queries, not `Table.column`.
4. **Foreign keys** accept table classes (`ForeignKey(User)`) or strings (`ForeignKey("users.id")`).
5. **Defaults can be SQL expressions**: `default="now()"`, `default="gen_random_uuid()"`.

### Field types

| Type                            | Python Type | Description                       |
| ------------------------------- | ----------- | --------------------------------- |
| `UUID()`                        | `uuid.UUID` | UUID                              |
| `Serial()`                      | `int`       | Auto-incrementing 4-byte integer  |
| `Integer()`                     | `int`       | 4-byte integer                    |
| `BigInt()`                      | `int`       | 8-byte integer                    |
| `SmallInt()`                    | `int`       | 2-byte integer                    |
| `Varchar(n)`                    | `str`       | Variable-length string with limit |
| `Text()`                        | `str`       | Unlimited length string           |
| `Boolean()`                     | `bool`      | Boolean                           |
| `Timestamp(with_timezone=True)` | `datetime`  | Timestamp                         |
| `Date()`                        | `date`      | Date                              |
| `Numeric(precision, scale)`     | `Decimal`   | Exact numeric                     |
| `JSON()`                        | `Any`       | JSON                              |
| `JSONB()`                       | `Any`       | Binary JSON (prefer over JSON)    |
| `Array(Integer())`              | `list[int]` | Array of another type             |
| `Enum(MyEnum)`                  | `StrEnum`   | PostgreSQL enum                   |

### Field options

```python
Field(
    field_type,           # Required: UUID(), Varchar(255), etc.
    primary_key=False,
    unique=False,
    nullable=False,       # Default: NOT NULL
    default=None,         # SQL expression as string
    foreign_key=None,
    index=False,
)
```

Foreign key actions: `CASCADE`, `SET_NULL`, `SET_DEFAULT`, `RESTRICT`, `NO_ACTION`.

## Step 3: Run Migrations

```bash
derp generate --name initial   # Generate migration from schema diff
derp migrate                   # Apply to database
```

For development, push directly (skips migration files):

```bash
derp push
```

Other commands:

```bash
derp status                          # Check migration status
derp check                           # Verify schema matches snapshot (CI)
derp pull --migration --name baseline  # Import existing database
derp generate --name seed --custom   # Empty migration for hand-written SQL
derp studio                          # Launch database browser UI
```

## Step 4: FastAPI Integration

### Lifecycle

```python
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI

from derp import DerpClient, DerpConfig

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    config = DerpConfig.load("derp.toml")
    derp = DerpClient(config)
    await derp.connect()
    app.state.derp = derp
    yield
    await derp.disconnect()


app = FastAPI(lifespan=lifespan)
```

### Dependencies

```python
from fastapi import Depends, Request
from derp import DerpClient


def get_derp(request: Request) -> DerpClient:
    return request.app.state.derp
```

### Route handlers

```python
from fastapi import APIRouter, Depends, HTTPException, status

router = APIRouter(prefix="/posts", tags=["posts"])


@router.get("")
async def list_posts(derp: DerpClient = Depends(get_derp)):
    return await (
        derp.db.select(Post)
        .where(Post.c.published == True)
        .order_by(Post.c.created_at, asc=False)
        .execute()
    )


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_post(
    title: str, content: str, derp: DerpClient = Depends(get_derp)
):
    return await (
        derp.db.insert(Post)
        .values(title=title, content=content, author_id=user_id)
        .returning(Post)
        .execute()
    )
```

## Step 5: Query Data

### SELECT

```python
db = derp.db

products = await db.select(Product).execute()

product = await db.select(Product).where(Product.c.id == pid).first_or_none()

results = await (
    db.select(Product)
    .where(Product.c.price > 1000)
    .where(Product.c.name.ilike("%phone%"))
    .where(Product.c.seller_id.in_([id1, id2]))
    .order_by(Product.c.price, asc=False)
    .limit(20)
    .offset(40)
    .execute()
)
```

### Operators

```python
User.c.id == 1                          # =
User.c.id != 1                          # !=
User.c.age > 18                         # >
User.c.age >= 18                        # >=
User.c.id.in_([1, 2, 3])               # IN
User.c.name.like("%Alice%")             # LIKE
User.c.name.ilike("%alice%")            # ILIKE
User.c.age.is_null()                    # IS NULL
User.c.age.between(18, 65)             # BETWEEN
(User.c.age > 18) & (User.c.age < 65)  # AND
(expr1) | (expr2)                       # OR
~(User.c.age > 18)                      # NOT
```

### INSERT

```python
user = await (
    db.insert(User)
    .values(name="Bob", email="bob@example.com")
    .returning(User)
    .execute()
)

# Bulk insert
await (
    db.insert(Product)
    .values_list([
        {"name": "Mouse", "price": 2500},
        {"name": "Keyboard", "price": 7500},
    ])
    .execute()
)

# Upsert
await (
    db.insert(User)
    .values(email="bob@example.com", name="Bob")
    .upsert(target=User.c.email, set={"name": "Bob Updated"})
    .execute()
)
```

### UPDATE

```python
updated = await (
    db.update(User)
    .set(name="Robert", age=30)
    .where(User.c.id == user_id)
    .returning(User)
    .execute()
)
```

### DELETE

```python
await db.delete(User).where(User.c.id == user_id).execute()
```

### JOINs

```python
results = await (
    db.select(User.c.name, Post.c.title)
    .from_(User)
    .inner_join(Post, User.c.id == Post.c.author_id)
    .where(Post.c.published == True)
    .execute()
)
```

Join methods: `inner_join`, `left_join`, `right_join`, `full_join`, `cross_join`.

### Aggregates

```python
stats = await (
    db.select(
        Product.c.price.sum().as_("total"),
        Product.c.price.avg().as_("average"),
        Product.c.id.count().as_("count"),
    )
    .from_(Product)
    .execute()
)

# Group by + having
sales = await (
    db.select(Product.c.seller_id, Product.c.price.sum().as_("revenue"))
    .from_(Product)
    .group_by(Product.c.seller_id)
    .having(Product.c.price.sum() > 100000)
    .execute()
)
```

### Subqueries and EXISTS

```python
active_sellers = db.select(User.c.id).where(User.c.is_active == True)
products = await (
    db.select(Product)
    .where(Product.c.seller_id.in_(active_sellers))
    .execute()
)

has_orders = db.select(Order).where(Order.c.user_id == User.c.id)
users = await db.select(User).where(has_orders.exists()).execute()
```

### Set Operations

```python
combined = await recent.union(popular).execute()
overlap = await recent.intersect(popular).execute()
only_new = await recent.except_(popular).execute()
```

### Transactions

```python
async with db.transaction() as txn:
    order = await (
        txn.insert(Order)
        .values(user_id=user_id, total=9900)
        .returning(Order)
        .execute()
    )
    await (
        txn.insert(OrderItem)
        .values(order_id=order.id, product_id=pid, qty=1)
        .execute()
    )
# Commits on success, rolls back on exception
```

### Raw SQL

```python
from derp.orm import sql

# SQL expression in queries
results = await (
    db.select(Product.c.name, sql("UPPER(name)").as_("upper_name"))
    .from_(Product)
    .execute()
)

# Raw execute
rows = await db.execute("SELECT * FROM products WHERE price > $1", [5000])
```

## Step 6: Authentication

### Config

```toml
[email]
site_name = "My App"
site_url = "https://example.com"
from_email = "noreply@example.com"
smtp_host = "smtp.example.com"
smtp_port = 587
smtp_user = "$SMTP_USER"
smtp_password = "$SMTP_PASSWORD"

[auth.native]
enable_signup = true
enable_confirmation = false
enable_magic_link = false

[auth.native.jwt]
secret = "$JWT_SECRET"
access_token_expire_minutes = 15
refresh_token_expire_days = 7
```

Native auth requires `[email]` for confirmation/magic link emails.

### Sign up / sign in

```python
user, tokens = await derp.auth.sign_up(
    email="alice@example.com",
    password="s3cur3passw0rd",
)
# tokens.access_token, tokens.refresh_token

user, tokens = await derp.auth.sign_in_with_password(
    "alice@example.com", "s3cur3passw0rd"
)

new_tokens = await derp.auth.refresh_token(tokens.refresh_token)
```

### Authenticate requests

```python
session = await derp.auth.authenticate(request)
# Returns SessionInfo or None
```

### Protected route dependency

```python
from derp import DerpClient
from derp.auth.models import UserInfo

async def get_current_user(
    request: Request, derp: DerpClient = Depends(get_derp)
) -> UserInfo:
    session = await derp.auth.authenticate(request)
    if session is None:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    user = await derp.auth.get_user(session.user_id)
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return user

@app.get("/orders")
async def list_orders(user: UserInfo = Depends(get_current_user)):
    ...
```

### OAuth

```toml
[auth.native.google_oauth]
client_id = "$GOOGLE_CLIENT_ID"
client_secret = "$GOOGLE_CLIENT_SECRET"
redirect_uri = "https://example.com/auth/callback/google"
```

```python
url = derp.auth.get_oauth_authorization_url("google", state=csrf_token)
user, tokens = await derp.auth.sign_in_with_oauth("google", code=auth_code)
```

### Magic links

Requires `enable_magic_link = true` in `[auth.native]`.

```python
await derp.auth.sign_in_with_magic_link(
    email="alice@example.com",
    magic_link_url="https://example.com/auth/magic",
)
user, tokens = await derp.auth.verify_magic_link(token)
```

### Organizations

```python
org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=user.id)
await derp.auth.add_org_member(org_id=org.id, user_id=other_id, role="member")
new_tokens = await derp.auth.set_active_org(
    session_id=session.session_id, org_id=org.id
)
orgs = await derp.auth.list_orgs(user_id=user.id)
```

### Clerk backend

Same interface, different config. Only one backend can be active at a time.

```toml
[auth.clerk]
secret_key = "$CLERK_SECRET_KEY"
```

## Step 7: Storage (S3)

### Config

```toml
[storage]
endpoint_url = "$STORAGE_ENDPOINT_URL"
access_key_id = "$STORAGE_ACCESS_KEY_ID"
secret_access_key = "$STORAGE_SECRET_ACCESS_KEY"
region = "us-east-1"
```

### Usage

```python
await derp.storage.upload_file(
    bucket="assets", key="avatars/user.jpg",
    data=file_bytes, content_type="image/jpeg",
)

data = await derp.storage.fetch_file(bucket="assets", key="avatars/user.jpg")

keys = await derp.storage.list_files(bucket="assets", prefix="avatars/")

await derp.storage.delete_file(bucket="assets", key="avatars/user.jpg")

url = derp.storage.get_url(bucket="assets", key="avatars/user.jpg")
```

## Step 8: Payments (Stripe)

### Config

```toml
[payments]
api_key = "$STRIPE_SECRET_KEY"
webhook_secret = "$STRIPE_WEBHOOK_SECRET"
```

### Usage

```python
customer = await derp.payments.create_customer(
    email="buyer@example.com", name="Alice",
)

session = await derp.payments.create_checkout_session(
    mode="payment",
    line_items=[{"price_id": "price_xxx", "quantity": 1}],
    success_url="https://example.com/success",
    cancel_url="https://example.com/cancel",
    customer_id=customer.id,
)

event = await derp.payments.verify_webhook_event(
    payload=body, signature=sig_header,
)

refund = await derp.payments.create_refund(payment_intent_id=intent.id)
```

## Step 9: KV Store (Valkey)

### Config

```toml
[kv.valkey]
addresses = [["localhost", 6379]]
```

### Usage

```python
await derp.kv.set(b"user:123", b'{"name":"Alice"}', ttl=3600)
data = await derp.kv.get(b"user:123")
await derp.kv.delete(b"user:123")

# Cache with stampede protection
result = await derp.kv.guarded_get(
    b"product:42",
    compute=lambda: fetch_from_db(42),
    ttl=300,
)

# Idempotent endpoint
body, status, is_replay = await derp.kv.idempotent_execute(
    key=idem_key,
    compute=lambda: create_order(data),
    status_code=201,
)

# Webhook dedup
if await derp.kv.already_processed(event_id=event["id"]):
    return {"status": "duplicate"}
```

## Step 10: Task Queue

### Config (Celery)

```toml
[queue.celery]
broker_url = "$CELERY_BROKER_URL"
result_backend = "$CELERY_RESULT_BACKEND"
```

### Config (Vercel)

```toml
[queue.vercel]
api_token = "$VERCEL_QUEUE_TOKEN"
team_id = "team_xxx"
project_id = "prj_xxx"
```

Only one backend can be configured at a time.

### Usage

```python
from datetime import timedelta

task_id = await derp.queue.enqueue(
    "send_welcome_email", payload={"user_id": str(user.id)},
)

task_id = await derp.queue.enqueue(
    "expire_reservation",
    payload={"reservation_id": str(res.id)},
    delay=timedelta(minutes=15),
)

status = await derp.queue.get_status(task_id)
```

### Schedules

```toml
[[queue.schedules]]
name = "cleanup"
task = "cleanup_expired_sessions"
cron = "0 */6 * * *"

[[queue.schedules]]
name = "sync"
task = "sync_inventory"
interval_seconds = 120
```

### Celery worker

```bash
celery -A 'derp.queue.celery:app' worker --loglevel=info
celery -A 'derp.queue.celery:app' beat --loglevel=info
```

## Common Gotchas

### Always use `.c` for queries

```python
# Correct
await db.select(User).where(User.c.id == 1).execute()

# Wrong — will not work
await db.select(User).where(User.id == 1).execute()
```

### UUID columns compared as strings

```python
user = await db.select(User).where(User.c.id == str(user_id)).first_or_none()
```

### No lazy-loading relationships

Fetch related data explicitly with queries or joins:

```python
posts = await db.select(Post).where(Post.c.author_id == user.id).execute()
```

### Prefer JSONB over JSON

`JSONB` supports indexing and is faster for queries.

### Use `enum.StrEnum` for string constants

```python
import enum

class Status(enum.StrEnum):
    ACTIVE = "active"
    INACTIVE = "inactive"
```

## Quick Reference: DerpClient Properties

| Property        | Type             | Config Section                       |
| --------------- | ---------------- | ------------------------------------ |
| `derp.db`       | `DatabaseEngine` | `[database]` (required)              |
| `derp.auth`     | `BaseAuthClient` | `[auth.native]` or `[auth.clerk]`    |
| `derp.storage`  | `StorageClient`  | `[storage]`                          |
| `derp.kv`       | `KVClient`       | `[kv.valkey]`                        |
| `derp.payments` | `PaymentsClient` | `[payments]`                         |
| `derp.queue`    | `QueueClient`    | `[queue.celery]` or `[queue.vercel]` |

Each property raises `ValueError` if the corresponding config section is missing.

## Quick Reference: CLI Commands

| Command                                | Description                          |
| -------------------------------------- | ------------------------------------ |
| `derp init`                            | Create `derp.toml`                   |
| `derp generate --name <name>`          | Generate migration from schema diff  |
| `derp generate --name <name> --custom` | Empty migration for custom SQL       |
| `derp migrate`                         | Apply pending migrations             |
| `derp migrate --dry-run`               | Show SQL without executing           |
| `derp push`                            | Push schema directly (dev only)      |
| `derp pull`                            | Import schema from existing database |
| `derp status`                          | Show migration status                |
| `derp check`                           | Verify schema matches snapshot (CI)  |
| `derp drop`                            | Remove migration files               |
| `derp studio`                          | Launch database browser UI           |
| `derp version`                         | Show version                         |
