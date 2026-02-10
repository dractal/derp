"""Derp Studio FastAPI application factory."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, PlainTextResponse, Response
from fastapi.staticfiles import StaticFiles

from derp.config import DerpConfig
from derp.derp_client import DerpClient
from derp.orm.migrations.introspect.postgres import PostgresIntrospector


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
        return derp.config.model_dump(mode="json")

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
        introspect_cfg = derp.config.database.introspect
        introspector = PostgresIntrospector(derp.db.pool)
        snapshot = await introspector.introspect(
            schemas=introspect_cfg.schemas,
            exclude_tables=introspect_cfg.exclude_tables,
        )
        tables = []
        for table in snapshot.tables.values():
            columns = [
                {
                    "name": col.name,
                    "type": col.type,
                    "not_null": col.not_null,
                    "primary_key": col.primary_key,
                }
                for col in table.columns.values()
            ]
            qualified = (
                f'"{table.schema_name}"."{table.name}"'
                if table.schema_name != "public"
                else f'"{table.name}"'
            )
            count_rows = await derp.db.execute(
                f"SELECT count(*) AS cnt FROM {qualified}"  # noqa: S608
            )
            row_count = count_rows[0]["cnt"] if count_rows else 0
            tables.append(
                {
                    "name": table.name,
                    "schema": table.schema_name,
                    "columns": columns,
                    "row_count": row_count,
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
        count_rows = await derp.db.execute(f"SELECT count(*) AS cnt FROM {qualified}")
        total = count_rows[0]["cnt"] if count_rows else 0
        rows = await derp.db.execute(
            f"SELECT * FROM {qualified} LIMIT $1 OFFSET $2",
            [limit, offset],
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
        introspect_cfg = derp.config.database.introspect
        introspector = PostgresIntrospector(derp.db.pool)
        snapshot = await introspector.introspect(
            schemas=introspect_cfg.schemas,
            exclude_tables=introspect_cfg.exclude_tables,
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

        qualified = (
            f'"{schema}"."{table}"' if schema != "public" else f'"{table}"'
        )

        deleted = 0
        for row_key in row_keys:
            conditions = []
            params = []
            for i, pk_col in enumerate(pk_columns, 1):
                if pk_col not in row_key:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Missing primary key column {pk_col!r}",
                    )
                conditions.append(f'"{pk_col}" = ${i}')
                params.append(row_key[pk_col])
            where = " AND ".join(conditions)
            result = await derp.db.execute(
                f"DELETE FROM {qualified} WHERE {where}",  # noqa: S608
                params,
            )
            deleted += len(result) if result else 0

        return {"deleted": deleted}

    @app.get("/{path:path}", include_in_schema=False)
    async def spa_fallback(path: str) -> Response:
        if not _is_spa_path(path):
            raise HTTPException(status_code=404, detail="Not Found")
        return _serve_index(studio_static_dir)

    return app
