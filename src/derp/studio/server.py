"""Derp Studio FastAPI application factory."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request
from fastapi.responses import HTMLResponse

from derp.config import DerpConfig
from derp.derp_client import DerpClient


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


def get_derp(request: Request) -> DerpClient:
    """Return the `DerpClient` from the request app state."""
    return request.app.state.derp_client


app = FastAPI(
    title="Derp Studio",
    description="Minimal Derp Studio web interface",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    """Serve a minimal Studio UI."""
    return """\
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Derp Studio</title>
  </head>
  <body>
    <main>
      <h1>Derp Studio</h1>
      <p>Loaded configuration from <code>derp.toml</code></p>
      <pre id="config">Loading...</pre>
    </main>
    <script>
      fetch("/api/config")
        .then((response) => response.json())
        .then((data) => {
          const el = document.getElementById("config");
          if (el) {
            el.textContent = JSON.stringify(data, null, 2);
          }
        })
        .catch((error) => {
          const el = document.getElementById("config");
          if (el) {
            el.textContent = "Failed to load config: " + String(error);
          }
        });
    </script>
  </body>
</html>
"""


@app.get("/api/config")
async def get_config(derp: DerpClient = Depends(get_derp)) -> dict:
    """Return loaded Derp configuration."""
    return derp.config.model_dump(mode="json")
