"""Derp Studio FastAPI application factory."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from datetime import time as dt_time
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, PlainTextResponse, Response
from fastapi.staticfiles import StaticFiles

from derp.config import DerpConfig
from derp.derp_client import DerpClient
from derp.orm.migrations.introspect.postgres import PostgresIntrospector

ARTIFICIAL_LATENCY = 0.2
_INT_TYPES = frozenset({"integer", "bigint", "smallint", "int2", "int4", "int8"})
_FLOAT_TYPES = frozenset(
    {"real", "double precision", "float4", "float8", "numeric", "decimal"}
)
_BOOL_TYPES = frozenset({"boolean", "bool"})
_UUID_TYPES = frozenset({"uuid"})


_JSON_TYPES = frozenset({"json", "jsonb"})


def _coerce_value(value: object, col_type: str) -> object:
    """Coerce a JSON-decoded value to the Python type asyncpg expects."""
    if value is None:
        return None

    col_lower = col_type.lower().strip()

    # asyncpg expects JSON/JSONB as a serialised string.
    if col_lower in _JSON_TYPES:
        return json.dumps(value) if not isinstance(value, str) else value

    if not isinstance(value, str):
        return value

    col_lower = col_type.lower().strip()
    if col_lower == "date":
        # Value may be a full ISO timestamp; extract date part.
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    if col_lower.startswith("timestamp"):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    if col_lower.startswith("time"):
        return dt_time.fromisoformat(value.replace("Z", "+00:00"))
    if col_lower in _INT_TYPES:
        return int(value)
    if col_lower in _FLOAT_TYPES or col_lower.startswith("numeric"):
        return Decimal(value)
    if col_lower in _BOOL_TYPES:
        return value.lower() in {"true", "t", "1", "yes"}
    if col_lower in _UUID_TYPES:
        return UUID(value)
    return value


def get_derp(request: Request) -> DerpClient:
    """Return the `DerpClient` from the request app state."""
    return request.app.state.derp_client


def _default_static_dir() -> Path:
    return Path(__file__).resolve().parent / "static"


def _missing_build_response(static_dir: Path) -> PlainTextResponse:
    message = (
        "Derp Studio frontend build is missing. "
        f"Expected: {static_dir / 'index.html'}. "
        f"Run `./scripts/build_studio.sh` from the project root."
    )
    return PlainTextResponse(message, status_code=503)


def _is_spa_path(path: str) -> bool:
    if path in {"", "api", "static"}:
        return False
    if path.startswith("api/") or path.startswith("static/"):
        return False
    return "." not in Path(path).name


def _serve_index(static_dir: Path) -> Response:
    index_path = static_dir / "index.html"
    if not index_path.exists():
        return _missing_build_response(static_dir)
    return FileResponse(index_path)


def create_app(
    *, static_dir: Path | None = None, enable_lifespan: bool = True
) -> FastAPI:
    """Create the Derp Studio FastAPI app."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        config = DerpConfig.load()
        derp_client = DerpClient(config)

        app.state.derp_client = derp_client

        try:
            await derp_client.connect()
        except Exception as exc:
            raise RuntimeError(
                "Failed to connect DerpClient during Studio startup."
            ) from exc

        yield

        await derp_client.disconnect()

    studio_static_dir = static_dir or _default_static_dir()

    app = FastAPI(
        title="Derp Studio",
        description="Derp Studio web interface",
        version="0.1.0",
        lifespan=lifespan if enable_lifespan else None,
    )

    @app.middleware("http")
    async def minimum_latency(request: Request, call_next):
        if ARTIFICIAL_LATENCY is not None:
            start = time.perf_counter()
            response = await call_next(request)
            elapsed = time.perf_counter() - start
            remaining = ARTIFICIAL_LATENCY - elapsed
            if remaining > 0:
                await asyncio.sleep(remaining)
            return response
        return await call_next(request)

    app.mount(
        "/static",
        StaticFiles(directory=str(studio_static_dir), check_dir=False),
        name="studio-static",
    )

    @app.get("/", include_in_schema=False)
    async def index() -> Response:
        return _serve_index(studio_static_dir)

    @app.get("/api/config")
    async def get_config(derp: DerpClient = Depends(get_derp)) -> dict:
        """Return loaded Derp configuration."""
        return derp.config.redacted_dump()

    @app.get("/api/storage/buckets")
    async def list_buckets(derp: DerpClient = Depends(get_derp)) -> dict:
        """List all S3 buckets."""
        if derp._storage is None:
            raise HTTPException(status_code=400, detail="Storage is not configured.")
        return {"buckets": await derp.storage.list_buckets()}

    @app.get("/api/storage/buckets/{bucket}/objects")
    async def list_objects(
        bucket: str,
        prefix: str = "",
        derp: DerpClient = Depends(get_derp),
    ) -> dict:
        """List objects in a bucket with prefix-based folder navigation."""
        if derp._storage is None:
            raise HTTPException(status_code=400, detail="Storage is not configured.")
        return await derp.storage.list_objects(bucket=bucket, prefix=prefix)

    @app.get("/api/storage/buckets/{bucket}/objects/info")
    async def get_object_info(
        bucket: str,
        key: str,
        derp: DerpClient = Depends(get_derp),
    ) -> dict:
        """Get object metadata (content type, size, etc.)."""
        if derp._storage is None:
            raise HTTPException(status_code=400, detail="Storage is not configured.")
        try:
            info = await derp.storage.head_object(bucket=bucket, key=key)
        except Exception as exc:
            raise HTTPException(
                status_code=404, detail=f"Object not found: {exc}"
            ) from exc
        return info

    @app.get("/api/storage/buckets/{bucket}/objects/content")
    async def get_object_content(
        bucket: str,
        key: str,
        derp: DerpClient = Depends(get_derp),
    ) -> Response:
        """Stream object content with its original content type."""
        if derp._storage is None:
            raise HTTPException(status_code=400, detail="Storage is not configured.")
        try:
            info = await derp.storage.head_object(bucket=bucket, key=key)
            data = await derp.storage.fetch_file(bucket=bucket, key=key)
        except Exception as exc:
            raise HTTPException(
                status_code=404, detail=f"Object not found: {exc}"
            ) from exc
        return Response(
            content=data,
            media_type=info["content_type"],
            headers={
                "Content-Length": str(len(data)),
                "Cache-Control": "private, max-age=60",
            },
        )

    @app.get("/api/email/templates")
    async def list_email_templates(derp: DerpClient = Depends(get_derp)) -> dict:
        """Return rendered previews for all configured email templates."""
        if derp._email is None or derp.config.email is None:
            raise HTTPException(status_code=400, detail="Email is not configured.")

        templates: list[dict[str, str]] = []
        for template_name, template in sorted(derp.email._templates.items()):
            if template_name == "base.html":
                continue

            content = derp.email._sources[template_name]
            if derp.email._base_template is not None:
                content = derp.email._base_template.render(
                    subject="<Email Subject>", content=content
                )
            templates.append({"name": template_name, "html": content})
        return {"templates": templates}

    # --- Database ---

    @app.get("/api/database/tables")
    async def list_tables(derp: DerpClient = Depends(get_derp)) -> dict:
        """List all database tables with column info and row counts."""
        introspector = PostgresIntrospector(derp.db.pool)
        snapshot = await introspector.introspect(
            schemas=derp.config.database.introspect_schemas,
            exclude_tables=derp.config.database.introspect_exclude_tables,
        )
        tables = []
        for table in snapshot.tables.values():
            columns = [
                {
                    "name": col.name,
                    "type": col.type,
                    "not_null": col.not_null,
                    "primary_key": col.primary_key,
                    "unique": col.unique,
                    "default": col.default,
                    "generated": col.generated or None,
                    "nullable": not col.not_null,
                }
                for col in table.columns.values()
            ]
            indexes = [
                {
                    "name": idx.name,
                    "columns": idx.columns,
                    "unique": idx.unique,
                    "method": idx.method,
                    "where": idx.where,
                }
                for idx in table.indexes.values()
            ]
            foreign_keys = [
                {
                    "name": fk.name,
                    "columns": fk.columns,
                    "references_table": fk.references_table,
                    "references_columns": fk.references_columns,
                    "references_schema": fk.references_schema,
                    "on_delete": fk.on_delete,
                    "on_update": fk.on_update,
                }
                for fk in table.foreign_keys.values()
            ]
            unique_constraints = [
                {
                    "name": uc.name,
                    "columns": uc.columns,
                }
                for uc in table.unique_constraints.values()
            ]
            check_constraints = [
                {
                    "name": cc.name,
                    "expression": cc.expression,
                }
                for cc in table.check_constraints.values()
            ]
            primary_key = (
                {"name": table.primary_key.name, "columns": table.primary_key.columns}
                if table.primary_key
                else None
            )
            qualified = (
                f'"{table.schema_name}"."{table.name}"'
                if table.schema_name != "public"
                else f'"{table.name}"'
            )
            row_count = await derp.db.table(qualified).select("*").count()
            tables.append(
                {
                    "name": table.name,
                    "schema": table.schema_name,
                    "columns": columns,
                    "row_count": row_count,
                    "indexes": indexes,
                    "foreign_keys": foreign_keys,
                    "unique_constraints": unique_constraints,
                    "check_constraints": check_constraints,
                    "primary_key": primary_key,
                }
            )
        return {"tables": tables}

    @app.get("/api/database/tables/{table}/rows")
    async def list_table_rows(
        table: str,
        limit: int = 50,
        offset: int = 0,
        schema: str = "public",
        derp: DerpClient = Depends(get_derp),
    ) -> dict:
        """Fetch rows from a table with pagination."""
        limit = min(max(limit, 1), 500)
        offset = max(offset, 0)

        qualified = f'"{schema}"."{table}"' if schema != "public" else f'"{table}"'
        total = await derp.db.table(qualified).select("*").count()
        rows = (
            await derp.db.table(qualified)
            .select("*")
            .limit(limit)
            .offset(offset)
            .execute()
        )
        return {"rows": rows, "total": total, "limit": limit, "offset": offset}

    @app.post("/api/database/tables/{table}/delete-rows")
    async def delete_table_rows(
        table: str,
        request: Request,
        schema: str = "public",
        derp: DerpClient = Depends(get_derp),
    ) -> dict:
        """Delete rows from a table by primary key values."""
        body = await request.json()
        row_keys: list[dict] = body.get("rows", [])
        if not row_keys:
            raise HTTPException(status_code=400, detail="No rows specified")

        # Resolve primary key columns from introspection
        introspector = PostgresIntrospector(derp.db.pool)
        snapshot = await introspector.introspect(
            schemas=derp.config.database.introspect_schemas,
            exclude_tables=derp.config.database.introspect_exclude_tables,
        )
        table_key = f"{schema}.{table}" if schema != "public" else table
        table_snapshot = snapshot.tables.get(table_key)
        if table_snapshot is None:
            raise HTTPException(status_code=404, detail=f"Table {table!r} not found")

        pk_columns = [
            col.name for col in table_snapshot.columns.values() if col.primary_key
        ]
        if not pk_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Table {table!r} has no primary key",
            )

        qualified = f'"{schema}"."{table}"' if schema != "public" else f'"{table}"'

        deleted = 0
        for row_key in row_keys:
            q = derp.db.table(qualified).delete()
            for pk_col in pk_columns:
                if pk_col not in row_key:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Missing primary key column {pk_col!r}",
                    )
                q = q.eq(pk_col, row_key[pk_col])
            result = await q.returning("*").execute()
            deleted += len(result)

        return {"deleted": deleted}

    @app.post("/api/database/tables/{table}/update-row")
    async def update_table_row(
        table: str,
        request: Request,
        schema: str = "public",
        derp: DerpClient = Depends(get_derp),
    ) -> dict:
        """Update a single row by primary key values."""
        body = await request.json()
        key: dict = body.get("key", {})
        values: dict = body.get("values", {})
        if not key:
            raise HTTPException(status_code=400, detail="No key specified")
        if not values:
            raise HTTPException(status_code=400, detail="No values specified")

        introspector = PostgresIntrospector(derp.db.pool)
        snapshot = await introspector.introspect(
            schemas=derp.config.database.introspect_schemas,
            exclude_tables=derp.config.database.introspect_exclude_tables,
        )
        table_key = f"{schema}.{table}" if schema != "public" else table
        table_snapshot = snapshot.tables.get(table_key)
        if table_snapshot is None:
            raise HTTPException(status_code=404, detail=f"Table {table!r} not found")

        pk_columns = [
            col.name for col in table_snapshot.columns.values() if col.primary_key
        ]
        if not pk_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Table {table!r} has no primary key",
            )

        qualified = f'"{schema}"."{table}"' if schema != "public" else f'"{table}"'

        col_types = {c.name: c.type for c in table_snapshot.columns.values()}

        coerced = {
            col: _coerce_value(val, col_types.get(col, "text"))
            for col, val in values.items()
        }

        q = derp.db.table(qualified).update(coerced)
        for pk_col in pk_columns:
            if pk_col not in key:
                raise HTTPException(
                    status_code=400,
                    detail=f"Missing primary key column {pk_col!r}",
                )
            q = q.eq(pk_col, key[pk_col])

        result = await q.returning("*").execute()
        updated = len(result)

        return {"updated": updated}

    # --- KV ---

    @app.get("/api/kv/keys")
    async def list_kv_keys(
        prefix: str = "",
        limit: int = 100,
        derp: DerpClient = Depends(get_derp),
    ) -> dict:
        """Scan KV keys with optional prefix filtering."""
        if derp._kv is None:
            raise HTTPException(status_code=400, detail="KV is not configured.")
        limit = min(max(limit, 1), 1000)
        prefix_bytes = prefix.encode() if prefix else None
        keys: list[str] = []
        async for key in derp.kv.scan(prefix=prefix_bytes, limit=limit):
            keys.append(key.decode("utf-8", errors="replace"))
        return {"keys": keys}

    @app.get("/api/kv/keys/info")
    async def get_kv_key_info(
        key: str,
        derp: DerpClient = Depends(get_derp),
    ) -> dict:
        """Get value and TTL for a single KV key."""
        if derp._kv is None:
            raise HTTPException(status_code=400, detail="KV is not configured.")
        key_bytes = key.encode()
        value = await derp.kv.get(key_bytes)
        if value is None:
            raise HTTPException(status_code=404, detail=f"Key not found: {key}")
        ttl = await derp.kv.ttl(key_bytes)
        return {
            "key": key,
            "value": value.decode("utf-8", errors="replace"),
            "ttl": ttl,
            "size": len(value),
        }

    @app.delete("/api/kv/keys")
    async def delete_kv_key(
        request: Request,
        derp: DerpClient = Depends(get_derp),
    ) -> dict:
        """Delete a single KV key."""
        if derp._kv is None:
            raise HTTPException(status_code=400, detail="KV is not configured.")
        body: dict[str, Any] = await request.json()
        key: str = body.get("key", "")
        if not key:
            raise HTTPException(status_code=400, detail="No key specified.")
        deleted = await derp.kv.delete(key.encode())
        return {"deleted": deleted}

    # --- Auth ---

    @app.get("/api/auth/users")
    async def list_auth_users(
        limit: int = 100,
        derp: DerpClient = Depends(get_derp),
    ) -> dict:
        """List auth users."""
        if derp._auth is None:
            raise HTTPException(status_code=400, detail="Auth is not configured.")
        limit = min(max(limit, 1), 500)
        users = await derp.auth.list_users(limit=limit)
        return {"users": users}

    @app.get("/api/auth/sessions")
    async def list_auth_sessions(
        limit: int = 100,
        derp: DerpClient = Depends(get_derp),
    ) -> dict:
        """List auth sessions."""
        if derp._auth is None:
            raise HTTPException(status_code=400, detail="Auth is not configured.")
        limit = min(max(limit, 1), 500)
        sessions = await derp.auth.list_sessions(limit=limit)
        return {"sessions": sessions}

    @app.get("/api/auth/organizations")
    async def list_auth_organizations(
        limit: int = 100,
        derp: DerpClient = Depends(get_derp),
    ) -> dict:
        """List auth organizations with member counts."""
        if derp._auth is None:
            raise HTTPException(status_code=400, detail="Auth is not configured.")
        limit = min(max(limit, 1), 500)
        orgs = await derp.auth.list_orgs(limit=limit)
        results = []
        for org in orgs:
            members = await derp.auth.list_org_members(org.id)
            results.append({
                "id": org.id,
                "name": org.name,
                "slug": org.slug,
                "member_count": len(members),
                "created_at": org.created_at.isoformat() if org.created_at else None,
                "updated_at": org.updated_at.isoformat() if org.updated_at else None,
            })
        return {"organizations": results}

    # --- Payments ---

    @app.get("/api/payments/customers")
    async def list_customers(
        limit: int = 25,
        starting_after: str | None = None,
        derp: DerpClient = Depends(get_derp),
    ) -> dict:
        """List Stripe customers."""
        if derp._payments is None:
            raise HTTPException(status_code=400, detail="Payments is not configured.")
        result = await derp.payments.list_customers(
            limit=limit, starting_after=starting_after
        )
        return {"data": result.data, "has_more": result.has_more}

    @app.get("/api/payments/products")
    async def list_products(
        limit: int = 25,
        starting_after: str | None = None,
        derp: DerpClient = Depends(get_derp),
    ) -> dict:
        """List Stripe products."""
        if derp._payments is None:
            raise HTTPException(status_code=400, detail="Payments is not configured.")
        result = await derp.payments.list_products(
            limit=limit, starting_after=starting_after
        )
        return {"data": result.data, "has_more": result.has_more}

    @app.get("/api/payments/subscriptions")
    async def list_subscriptions(
        limit: int = 25,
        starting_after: str | None = None,
        derp: DerpClient = Depends(get_derp),
    ) -> dict:
        """List Stripe subscriptions."""
        if derp._payments is None:
            raise HTTPException(status_code=400, detail="Payments is not configured.")
        result = await derp.payments.list_subscriptions(
            limit=limit, starting_after=starting_after
        )
        return {"data": result.data, "has_more": result.has_more}

    @app.get("/api/payments/invoices")
    async def list_invoices(
        limit: int = 25,
        starting_after: str | None = None,
        derp: DerpClient = Depends(get_derp),
    ) -> dict:
        """List Stripe invoices."""
        if derp._payments is None:
            raise HTTPException(status_code=400, detail="Payments is not configured.")
        result = await derp.payments.list_invoices(
            limit=limit, starting_after=starting_after
        )
        return {"data": result.data, "has_more": result.has_more}

    @app.get("/api/payments/charges")
    async def list_charges(
        limit: int = 25,
        starting_after: str | None = None,
        derp: DerpClient = Depends(get_derp),
    ) -> dict:
        """List Stripe charges."""
        if derp._payments is None:
            raise HTTPException(status_code=400, detail="Payments is not configured.")
        result = await derp.payments.list_charges(
            limit=limit, starting_after=starting_after
        )
        return {"data": result.data, "has_more": result.has_more}

    @app.get("/{path:path}", include_in_schema=False)
    async def spa_fallback(path: str) -> Response:
        if not _is_spa_path(path):
            raise HTTPException(status_code=404, detail="Not Found")
        return _serve_index(studio_static_dir)

    return app
