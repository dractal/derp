"""Studio command - launch the Derp Studio web interface."""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Annotated

import typer
import uvicorn

from derp.config import ConfigError, DerpConfig
from derp.studio.server import create_app


def _validate_config() -> None:
    try:
        DerpConfig.load()
    except ConfigError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


def _studio_ui_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "studio" / "ui"


def _terminate_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return

    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def studio(
    host: Annotated[
        str, typer.Option("--host", "-h", help="Host to bind to")
    ] = "127.0.0.1",
    port: Annotated[int, typer.Option("--port", "-p", help="Port to bind to")] = 4983,
) -> None:
    """Launch Derp Studio - a web UI for browsing your database."""
    _validate_config()

    old_url = os.environ.pop("PUBLIC_API_URL", None)
    old_node_env = os.environ.pop("NODE_ENV", None)
    os.environ["PUBLIC_API_URL"] = f"http://{host}:{port}"
    os.environ["NODE_ENV"] = "production"

    studio_app = create_app()
    uvicorn.run(studio_app, host=host, port=port)
    if old_url is not None:
        os.environ["PUBLIC_API_URL"] = old_url
    if old_node_env is not None:
        os.environ["NODE_ENV"] = old_node_env


def studio_dev(
    host: Annotated[
        str, typer.Option("--host", help="Host to bind both servers")
    ] = "127.0.0.1",
    backend_port: Annotated[
        int, typer.Option("--backend-port", help="Backend API port")
    ] = 4983,
    frontend_port: Annotated[
        int, typer.Option("--frontend-port", help="Frontend Vite dev port")
    ] = 5173,
) -> None:
    """Launch Derp Studio in development mode (backend + frontend)."""
    _validate_config()

    bun = shutil.which("bun")
    if bun is None:
        typer.echo(
            "Error: `bun` is required for `derp studio-dev` but was not found on PATH.",
            err=True,
        )
        typer.echo("Run `./scripts/build_studio.sh` after installing Bun.", err=True)
        raise typer.Exit(1)

    ui_dir = _studio_ui_dir()
    if not ui_dir.exists():
        typer.echo(f"Error: Studio UI directory not found: {ui_dir}", err=True)
        raise typer.Exit(1)

    frontend_cmd = [
        bun,
        "run",
        "dev",
        "--",
        "--host",
        host,
        "--port",
        str(frontend_port),
        "--strictPort",
    ]
    api_origin = f"http://{host}:{backend_port}"
    frontend_origin = f"http://{host}:{frontend_port}"
    env = os.environ.copy()
    env["NODE_ENV"] = "development"
    env["PUBLIC_API_URL"] = api_origin

    frontend_process: subprocess.Popen[bytes] | None = None
    try:
        frontend_process = subprocess.Popen(
            frontend_cmd,
            cwd=ui_dir,
            env=env,
        )

        time.sleep(0.3)
        return_code = frontend_process.poll()
        if return_code is not None:
            typer.echo(
                (
                    "Error: Frontend dev server exited early with code "
                    f"{return_code}. Run `./scripts/build_studio.sh`."
                ),
                err=True,
            )
            raise typer.Exit(1)

        typer.echo(f"Studio frontend: {frontend_origin}")
        typer.echo(f"Studio backend:  {api_origin}/api/config")
        typer.echo(f"Studio app URL:  {api_origin}")

        studio_app = create_app()
        uvicorn.run(studio_app, host=host, port=backend_port, reload=True)
    finally:
        if frontend_process is not None:
            _terminate_process(frontend_process)
