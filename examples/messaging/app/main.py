"""FastAPI application entry point."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.models import User
from app.routers import auth, conversations, users
from derp import DerpClient, DerpConfig
from derp.auth import AuthConfig, EmailConfig, JWTConfig
from derp.orm import DatabaseConfig
from derp.storage import StorageConfig


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage DerpClient lifecycle."""
    config = DerpConfig(
        database=DatabaseConfig(db_url=settings.database_url),
        storage=StorageConfig(
            endpoint_url=settings.storage_endpoint_url,
            access_key_id=settings.storage_access_key_id,
            secret_access_key=settings.storage_secret_access_key,
            region=settings.storage_region,
        ),
        auth=AuthConfig[User](
            user_table=User,
            email=EmailConfig(
                site_name=settings.site_name,
                site_url=settings.site_url,
                confirm_email_url="{site_url}/auth/confirm",
                recovery_url="{site_url}/auth/recovery",
                magic_link_url="{site_url}/auth/magic-link",
                from_email=settings.smtp_from_email,
                from_name=settings.smtp_from_name,
                smtp_host=settings.smtp_host,
                smtp_port=settings.smtp_port,
                smtp_user=settings.smtp_user,
                smtp_password=settings.smtp_password,
                enable_signup=True,
                enable_confirmation=False,
                enable_magic_link=False,
            ),
            jwt=JWTConfig(
                secret=settings.jwt_secret,
                access_token_expire_minutes=15,
                refresh_token_expire_days=7,
            ),
        ),
    )

    client = DerpClient[User](config)
    await client.connect()

    app.state.derp_client = client

    yield

    await client.disconnect()


app = FastAPI(
    title="Messaging API",
    description="Example messaging application using Derp",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(auth.router)
app.include_router(users.router)
app.include_router(conversations.router)

# Serve static files
static_dir = Path(__file__).parent.parent / "static"
app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/")
async def index() -> FileResponse:
    """Serve the frontend."""
    return FileResponse(static_dir / "index.html")


@app.get("/health")
async def health_check() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok"}
