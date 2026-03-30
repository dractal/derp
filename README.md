# Derp

[![PyPI](https://img.shields.io/pypi/v/derp-py?color=blue)](https://pypi.org/project/derp-py/)
[![Python](https://img.shields.io/pypi/pyversions/derp-py)](https://pypi.org/project/derp-py/)
[![License](https://img.shields.io/github/license/dractal/derp)](LICENSE)
[![Tests](https://img.shields.io/github/actions/workflow/status/dractal/derp/test.yml?label=tests)](https://github.com/dractal/derp/actions)
[![Docs](https://img.shields.io/readthedocs/derp)](https://derp.readthedocs.io/)

An async Python backend toolkit. One client, one config file.

**ORM** · **Auth** · **Payments** · **Storage** · **KV** · **Queues** · **AI** · **CLI** · **Studio**

> **Warning:** Derp is in alpha. The API is unstable and may change without notice before 1.0.

## Install

```bash
uv add derp-py
```

Requires Python 3.12+.

## Quick Start

Define a table:

```python
from derp.orm import Table, Field, Fn, UUID, Varchar, Integer, Boolean, TimestampTZ

class Product(Table, table="products"):
    id: UUID = Field(primary=True, default=Fn.gen_random_uuid())
    name: Varchar[255] = Field()
    price_cents: Integer = Field()
    is_active: Boolean = Field(default=True)
    created_at: TimestampTZ = Field(default=Fn.now())
```

Generate and apply a migration:

```bash
derp generate --name initial
derp migrate
```

Query data:

```python
from derp import DerpClient, DerpConfig
from app.models import Product

config = DerpConfig.load("derp.toml")
derp = DerpClient(config)
await derp.connect()

# Select
products = await (
    derp.db.select(Product)
    .where(Product.is_active)
    .order_by(Product.created_at, asc=False)
    .limit(10)
    .execute()
)

# Insert
product = await (
    derp.db.insert(Product)
    .values(name="Headphones", price_cents=4999)
    .returning(Product)
    .execute()
)

# Update
await (
    derp.db.update(Product)
    .set(price_cents=3999)
    .where(Product.id == product.id)
    .execute()
)
```

## Use with FastAPI

```python
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from fastapi import FastAPI, Request, Depends
from derp import DerpClient, DerpConfig

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    config = DerpConfig.load("derp.toml")
    derp = DerpClient(config)
    await derp.connect()
    app.state.derp = derp
    yield
    await derp.disconnect()

app = FastAPI(lifespan=lifespan)

def get_derp(request: Request) -> DerpClient:
    return request.app.state.derp

@app.get("/products")
async def list_products(derp: DerpClient = Depends(get_derp)):
    return await derp.db.select(Product).where(Product.is_active).execute()
```

## Configuration

Everything lives in `derp.toml`. Only `[database]` is required — add modules as you need them:

```toml
[database]
db_url = "$DATABASE_URL"
schema_path = "app/models.py"

[auth.native]
enable_signup = true
[auth.native.jwt]
secret = "$JWT_SECRET"

[storage]
endpoint_url = "$S3_ENDPOINT"
access_key_id = "$S3_KEY"
secret_access_key = "$S3_SECRET"

[kv.valkey]
addresses = [["localhost", 6379]]

[payments]
api_key = "$STRIPE_SECRET_KEY"

[queue.celery]
broker_url = "$CELERY_BROKER_URL"

[ai]
api_key = "$OPENAI_API_KEY"
# base_url = "https://api.openrouter.ai/v1"  # for other providers
```

Environment variables starting with `$` are resolved at load time.

## Modules

### Auth

Email/password, magic links, Google/GitHub OAuth, JWTs, organizations. Native or Clerk backend.

```python
user, tokens = await derp.auth.sign_up(email="alice@example.com", password="s3cure!")
session = await derp.auth.authenticate(request)  # from Bearer token
org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=user.id)
```

### Payments (Stripe)

```python
customer = await derp.payments.create_customer(email="buyer@example.com")
session = await derp.payments.create_checkout_session(
    mode="payment",
    line_items=[{"price_id": "price_xxx", "quantity": 1}],
    success_url="https://example.com/success",
    cancel_url="https://example.com/cancel",
)
event = await derp.payments.verify_webhook_event(payload=body, signature=sig)
```

### Storage (S3)

```python
await derp.storage.upload_file(bucket="assets", key="avatar.jpg", data=img, content_type="image/jpeg")
data = await derp.storage.fetch_file(bucket="assets", key="avatar.jpg")

# Signed URLs for direct client access
url = await derp.storage.signed_download_url(bucket="assets", key="avatar.jpg")
url = await derp.storage.signed_upload_url(bucket="assets", key="uploads/new.jpg", content_type="image/jpeg")

# Batch delete and server-side copy
await derp.storage.delete_files(bucket="assets", keys=["tmp/a.txt", "tmp/b.txt"])
await derp.storage.copy_file(src_bucket="uploads", src_key="tmp.jpg", dst_bucket="assets", dst_key="final.jpg")
```

### KV (Valkey)

```python
await derp.kv.set(b"user:123", b'{"name":"Alice"}', ttl=3600)
data = await derp.kv.get(b"user:123")

# Idempotent endpoints
body, status, is_replay = await derp.kv.idempotent_execute(
    key=idem_key, compute=lambda: create_order(data), status_code=201,
)

# Webhook dedup
if await derp.kv.already_processed(event_id=event["id"]):
    return {"status": "duplicate"}

# Rate limiting
result = await derp.kv.rate_limit(f"api:{user.id}", limit=100, window=3600)
if not result.allowed:
    raise HTTPException(429, headers={"Retry-After": str(result.retry_after)})
```

### AI (OpenAI / Fal / Modal)

```python
# Chat
response = await derp.ai.chat(model="gpt-4o-mini", messages=[{"role": "user", "content": "Hello"}])
print(response.content)

# Streaming with Vercel AI SDK format
async for chunk in derp.ai.stream_chat(model="gpt-4o-mini", messages=messages):
    for event in chunk.vercel_ai_json(message_id="msg-1"):
        yield event.dump()  # "data: {...}\n\n"

# Image generation via fal
request_id = await derp.ai.fal_call(application="fal-ai/flux", inputs={"prompt": "a cat"})
status = await derp.ai.fal_poll("fal-ai/flux", request_id)
```

### Queue (Celery / Vercel)

```python
task_id = await derp.queue.enqueue("send_email", payload={"user_id": str(user.id)})
status = await derp.queue.get_status(task_id)
```

Schedules in config:

```toml
[[queue.schedules]]
name = "cleanup"
task = "cleanup_sessions"
cron = "0 */6 * * *"
```

## CLI

```
derp init          Create derp.toml
derp generate      Generate migration from schema diff
derp migrate       Apply pending migrations
derp push          Push schema directly (dev only)
derp pull          Introspect database into snapshot
derp status        Show migration status
derp check         Verify schema matches snapshot (CI)
derp drop          Remove migration files
derp studio        Launch database browser UI
derp version       Show version
```

## Documentation

Full docs at [derp.readthedocs.io](https://derp.readthedocs.io/).

## Development

```bash
uv sync
uv run pytest
uv run ruff check src/
uv run ruff format src/
```

## License

MIT
