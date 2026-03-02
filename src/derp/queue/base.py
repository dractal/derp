"""Base interface for queue clients."""

from __future__ import annotations

import abc
import dataclasses
import enum
from datetime import timedelta
from typing import Any


class TaskState(enum.StrEnum):
    """State of a queued task."""

    PENDING = "pending"
    STARTED = "started"
    SUCCESS = "success"
    FAILURE = "failure"
    REVOKED = "revoked"
    UNKNOWN = "unknown"


@dataclasses.dataclass(slots=True)
class TaskStatus:
    """Status of a queued task."""

    task_id: str
    state: TaskState
    result: Any | None = None
    error: str | None = None


class QueueClient(abc.ABC):
    """Async producer-side queue client."""

    supports_result: bool
    supports_revoke: bool
    supports_delay: bool

    @abc.abstractmethod
    async def connect(self) -> None:
        """Connect to the queue backend."""

    @abc.abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the queue backend."""

    @abc.abstractmethod
    async def enqueue(
        self,
        task_name: str,
        payload: dict[str, Any] | None = None,
        *,
        queue: str | None = None,
        delay: timedelta | None = None,
    ) -> str:
        """Enqueue a task. Returns a task ID."""

    @abc.abstractmethod
    async def get_status(self, task_id: str) -> TaskStatus:
        """Get the status of a task by ID."""
