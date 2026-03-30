"""Vercel queue client (REST-based)."""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import timedelta
from typing import Any
from urllib.parse import quote

import httpx

from derp.config import VercelQueueConfig
from derp.queue.base import QueueClient, Schedule, ScheduleType, TaskStatus
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
        self._schedules: list[Schedule] = []

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
        task_id: str | None = None,
        queue: str | None = None,
        delay: int | timedelta | None = None,
    ) -> str:
        if self._client is None:
            raise QueueNotConnectedError()

        queue_name = queue or self._config.default_queue
        if task_id is None:
            task_id = uuid.uuid4().hex

        body: dict[str, Any] = {
            "task_name": task_name,
            "task_id": task_id,
            "payload": payload or {},
        }
        if delay is not None:
            if isinstance(delay, timedelta):
                body["delay_seconds"] = int(delay.total_seconds())
            else:
                body["delay_seconds"] = delay

        try:
            resp = await self._client.post(
                f"/v1/queues/{quote(queue_name, safe='')}/messages",
                json=body,
                params=self._build_params(),
            )
        except Exception as exc:
            raise QueueProviderError(str(exc) or "Failed to enqueue task") from exc

        if resp.status_code != 200:
            raise QueueProviderError(
                f"Error connecting to Vercel API: {resp.status_code} {resp.text}",
            )

        return task_id

    async def get_status(self, task_id: str) -> TaskStatus:
        """Vercel queues do not expose per-message status."""
        raise NotImplementedError("Vercel queues do not expose per-message status.")

    def register_schedules(self, schedules: Sequence[Schedule]) -> None:
        """Register recurring schedules. Vercel only supports cron expressions."""
        for s in schedules:
            if s.type == ScheduleType.INTERVAL:
                raise QueueProviderError(
                    f"Schedule '{s.name}': Vercel cron only supports "
                    "cron expressions, not intervals.",
                )
        self._schedules = list(schedules)

    def get_schedules(self) -> list[Schedule]:
        """Return the currently registered schedules."""
        return self._schedules

    def generate_vercel_cron_config(self) -> list[dict[str, str]]:
        """Generate the ``crons`` section for vercel.json."""
        return [
            {
                "path": s.path or f"/api/cron/{s.name}",
                "schedule": s.cron or "",
            }
            for s in self._schedules
        ]
