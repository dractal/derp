"""Vercel queue client (REST-based)."""

from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any
from urllib.parse import quote

import httpx

from derp.config import VercelQueueConfig
from derp.queue.base import QueueClient, TaskState, TaskStatus
from derp.queue.exceptions import QueueNotConnectedError, QueueProviderError

VERCEL_API_BASE = "https://api.vercel.com"


class VercelQueueClient(QueueClient):
    """Queue client backed by Vercel Queues (REST API)."""

    supports_result = False
    supports_revoke = False
    supports_delay = True

    def __init__(self, config: VercelQueueConfig):
        self._config = config
        self._client: httpx.AsyncClient | None = None

    async def connect(self) -> None:
        if self._client is not None:
            return
        self._client = httpx.AsyncClient(
            base_url=VERCEL_API_BASE,
            headers={
                "Authorization": f"Bearer {self._config.api_token}",
                "Content-Type": "application/json",
            },
        )

    async def disconnect(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _build_params(self) -> dict[str, str]:
        params: dict[str, str] = {}
        if self._config.team_id is not None:
            params["teamId"] = self._config.team_id
        if self._config.project_id is not None:
            params["projectId"] = self._config.project_id
        return params

    async def enqueue(
        self,
        task_name: str,
        payload: dict[str, Any] | None = None,
        *,
        queue: str | None = None,
        delay: timedelta | None = None,
    ) -> str:
        if self._client is None:
            raise QueueNotConnectedError()

        queue_name = queue or self._config.default_queue
        task_id = uuid.uuid4().hex

        body: dict[str, Any] = {
            "task_name": task_name,
            "task_id": task_id,
            "payload": payload or {},
        }
        if delay is not None:
            body["delay_seconds"] = int(delay.total_seconds())

        try:
            resp = await self._client.post(
                f"/v1/queues/{quote(queue_name, safe='')}/messages",
                json=body,
                params=self._build_params(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise QueueProviderError(
                f"Vercel API error: {exc.response.status_code} {exc.response.text}",
                code="vercel_api_error",
            ) from exc
        except Exception as exc:
            raise QueueProviderError(str(exc) or "Failed to enqueue task") from exc

        return task_id

    async def get_status(self, task_id: str) -> TaskStatus:
        """Vercel queues do not expose per-message status."""
        return TaskStatus(
            task_id=task_id,
            state=TaskState.UNKNOWN,
        )
