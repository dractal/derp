"""Celery queue client."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import timedelta
from typing import Any

from etils import epy

from derp.config import CeleryConfig
from derp.queue.base import QueueClient, Schedule, ScheduleType, TaskState, TaskStatus
from derp.queue.exceptions import QueueNotConnectedError, QueueProviderError

with epy.lazy_imports():
    import celery
    import celery.result as celery_result
    import celery.schedules as celery_schedules


# Map Celery states to TaskState.
_CELERY_STATE_MAP: dict[str, TaskState] = {
    "PENDING": TaskState.PENDING,
    "STARTED": TaskState.STARTED,
    "SUCCESS": TaskState.SUCCESS,
    "FAILURE": TaskState.FAILURE,
    "REVOKED": TaskState.REVOKED,
    "RETRY": TaskState.STARTED,
    "RECEIVED": TaskState.PENDING,
}


class CeleryQueueClient(QueueClient):
    """Queue client backed by Celery."""

    supports_result = True
    supports_revoke = True
    supports_delay = True

    def __init__(self, config: CeleryConfig):
        self._config = config
        self._app: celery.Celery | None = None
        self._schedules: list[Schedule] = []

    @property
    def app(self) -> celery.Celery:
        """Expose the underlying Celery app for worker-side task registration."""
        if self._app is None:
            self._create_app()
        return self._app  # type: ignore[return-value]

    def _create_app(self) -> None:
        self._app = celery.Celery("derp")
        self._app.conf.update(
            broker_url=self._config.broker_url,
            result_backend=self._config.result_backend,
            task_serializer=self._config.task_serializer,
            result_serializer=self._config.result_serializer,
            task_default_queue=self._config.task_default_queue,
        )

    async def connect(self) -> None:
        if self._app is not None:
            return
        self._create_app()
        if self._schedules:
            self._apply_beat_schedule()

    async def disconnect(self) -> None:
        if self._app is not None:
            self._app.close()
            self._app = None

    async def enqueue(
        self,
        task_name: str,
        payload: dict[str, Any] | None = None,
        *,
        task_id: str | None = None,
        queue: str | None = None,
        delay: int | timedelta | None = None,
    ) -> str:
        del task_id
        if self._app is None:
            raise QueueNotConnectedError()

        kwargs: dict[str, Any] = {}
        if queue is not None:
            kwargs["queue"] = queue
        if delay is not None:
            if isinstance(delay, timedelta):
                kwargs["countdown"] = int(delay.total_seconds())
            else:
                kwargs["countdown"] = delay

        try:
            result = await asyncio.to_thread(
                self._app.send_task,
                task_name,
                kwargs=payload,
                **kwargs,
            )
        except Exception as exc:
            raise QueueProviderError(str(exc) or "Failed to enqueue task") from exc

        return str(result.id)

    async def get_status(self, task_id: str) -> TaskStatus:
        if self._app is None:
            raise QueueNotConnectedError()

        def _fetch_status() -> tuple[str, Any]:
            r = celery_result.AsyncResult(task_id, app=self._app)
            return r.state, r.result

        try:
            raw_state, raw_result = await asyncio.to_thread(_fetch_status)
            state = _CELERY_STATE_MAP.get(raw_state, TaskState.UNKNOWN)
            task_result = raw_result if state == TaskState.SUCCESS else None
            error = str(raw_result) if state == TaskState.FAILURE else None
        except Exception as exc:
            raise QueueProviderError(str(exc) or "Failed to get task status") from exc

        return TaskStatus(
            task_id=task_id,
            state=state,
            result=task_result,
            error=error,
        )

    def register_schedules(self, schedules: Sequence[Schedule]) -> None:
        """Register recurring schedules with Celery Beat."""
        self._schedules = list(schedules)
        if self._app is not None:
            self._apply_beat_schedule()

    def get_schedules(self) -> list[Schedule]:
        """Return the currently registered schedules."""
        return self._schedules

    def _apply_beat_schedule(self) -> None:
        """Write schedules into Celery's beat_schedule config."""
        assert self._app is not None
        beat_schedule: dict[str, Any] = {}
        for s in self._schedules:
            entry: dict[str, Any] = {"task": s.task}
            if s.type == ScheduleType.CRON and s.cron:
                parts = s.cron.split()
                entry["schedule"] = celery_schedules.crontab(
                    minute=parts[0] if len(parts) > 0 else "*",
                    hour=parts[1] if len(parts) > 1 else "*",
                    day_of_month=parts[2] if len(parts) > 2 else "*",
                    month_of_year=parts[3] if len(parts) > 3 else "*",
                    day_of_week=parts[4] if len(parts) > 4 else "*",
                )
            elif s.type == ScheduleType.INTERVAL and s.interval:
                entry["schedule"] = s.interval.total_seconds()
            if s.payload:
                entry["kwargs"] = s.payload
            if s.queue:
                entry["options"] = {"queue": s.queue}
            beat_schedule[s.name] = entry
        self._app.conf.beat_schedule = beat_schedule
