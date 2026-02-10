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

    @app.get("/{path:path}", include_in_schema=False)
    async def spa_fallback(path: str) -> Response:
        if not _is_spa_path(path):
            raise HTTPException(status_code=404, detail="Not Found")
        return _serve_index(studio_static_dir)

    return app
