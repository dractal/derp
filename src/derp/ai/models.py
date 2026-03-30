"""Provider-agnostic AI response models with protocol adapters."""

from __future__ import annotations

import json
import time
import uuid
from enum import StrEnum
from typing import Any

from pydantic import BaseModel


class SSEEvent(dict[str, Any]):
    """A single SSE event. Behaves like a dict with a dump() method."""

    def dump(self) -> str:
        """Serialize as an SSE data line: ``data: {...}\\n\\n``."""
        return f"data: {json.dumps(self, separators=(',', ':'))}\n\n"


class SSEDone(SSEEvent):
    """Terminal SSE event signaling end of stream."""

    def dump(self) -> str:
        return "data: [DONE]\n\n"


class Usage(BaseModel):
    """Token usage statistics."""

    prompt_tokens: int
    completion_tokens: int
    total_tokens: int

    def vercel_ai_json(self) -> dict[str, int]:
        """Format as Vercel AI SDK usage object."""
        return {
            "promptTokens": self.prompt_tokens,
            "completionTokens": self.completion_tokens,
            "totalTokens": self.total_tokens,
        }

    def tanstack_ai_json(self) -> dict[str, int]:
        """Format as TanStack AG-UI usage object."""
        return {
            "promptTokens": self.prompt_tokens,
            "completionTokens": self.completion_tokens,
            "totalTokens": self.total_tokens,
        }


class ChatResponse(BaseModel):
    """Non-streaming chat completion response."""

    content: str
    role: str = "assistant"
    model: str
    usage: Usage | None = None
    finish_reason: str = "stop"

    def vercel_ai_json(self, *, message_id: str | None = None) -> list[SSEEvent]:
        """Format as Vercel AI SDK data stream protocol events."""
        mid = message_id or f"msg-{uuid.uuid4().hex}"
        finish_meta: dict[str, Any] = {
            "finishReason": self.finish_reason.replace("_", "-"),
        }
        if self.usage:
            finish_meta["usage"] = self.usage.vercel_ai_json()
        return [
            SSEEvent({"type": "start", "messageId": mid}),
            SSEEvent({"type": "text-start", "id": "text-1"}),
            SSEEvent({"type": "text-delta", "id": "text-1", "delta": self.content}),
            SSEEvent({"type": "text-end", "id": "text-1"}),
            SSEEvent({"type": "finish", "messageMetadata": finish_meta}),
            SSEDone(),
        ]

    def tanstack_ai_json(
        self,
        *,
        run_id: str | None = None,
        message_id: str | None = None,
    ) -> list[SSEEvent]:
        """Format as TanStack AG-UI protocol events."""
        rid = run_id or f"run-{uuid.uuid4().hex}"
        mid = message_id or f"msg-{uuid.uuid4().hex}"
        ts = int(time.time() * 1000)
        finish: dict[str, Any] = {
            "type": "RUN_FINISHED",
            "runId": rid,
            "model": self.model,
            "timestamp": ts,
            "finishReason": self.finish_reason,
        }
        if self.usage:
            finish["usage"] = self.usage.tanstack_ai_json()
        return [
            SSEEvent(
                {
                    "type": "RUN_STARTED",
                    "runId": rid,
                    "model": self.model,
                    "timestamp": ts,
                }
            ),
            SSEEvent(
                {
                    "type": "TEXT_MESSAGE_START",
                    "messageId": mid,
                    "role": self.role,
                    "model": self.model,
                    "timestamp": ts,
                }
            ),
            SSEEvent(
                {
                    "type": "TEXT_MESSAGE_CONTENT",
                    "messageId": mid,
                    "model": self.model,
                    "timestamp": ts,
                    "delta": self.content,
                }
            ),
            SSEEvent(
                {
                    "type": "TEXT_MESSAGE_END",
                    "messageId": mid,
                    "model": self.model,
                    "timestamp": ts,
                }
            ),
            SSEEvent(finish),
            SSEDone(),
        ]


class ChatChunk(BaseModel):
    """A single chunk from a streaming chat completion."""

    delta: str
    role: str = "assistant"
    model: str | None = None
    finish_reason: str | None = None
    usage: Usage | None = None
    is_first: bool = False
    is_last: bool = False

    def vercel_ai_json(
        self, *, message_id: str, stream_id: str = "text-1"
    ) -> list[SSEEvent]:
        """Format as Vercel AI SDK events.

        Includes lifecycle events when is_first/is_last are set.
        """
        events: list[SSEEvent] = []
        if self.is_first:
            events.append(SSEEvent({"type": "start", "messageId": message_id}))
            events.append(SSEEvent({"type": "text-start", "id": stream_id}))

        events.append(
            SSEEvent(
                {
                    "type": "text-delta",
                    "id": stream_id,
                    "delta": self.delta,
                }
            )
        )

        if self.is_last:
            events.append(SSEEvent({"type": "text-end", "id": stream_id}))
            finish_meta: dict[str, Any] = {
                "finishReason": (self.finish_reason or "stop").replace("_", "-"),
            }
            if self.usage:
                finish_meta["usage"] = self.usage.vercel_ai_json()
            events.append(
                SSEEvent(
                    {
                        "type": "finish",
                        "messageMetadata": finish_meta,
                    }
                )
            )
            events.append(SSEDone())

        return events

    def tanstack_ai_json(
        self, *, message_id: str, run_id: str | None = None
    ) -> list[SSEEvent]:
        """Format as TanStack AG-UI events.

        Includes lifecycle events when is_first/is_last are set.
        """
        rid = run_id or f"run-{uuid.uuid4().hex}"
        ts = int(time.time() * 1000)
        events: list[SSEEvent] = []

        if self.is_first:
            events.append(
                SSEEvent(
                    {
                        "type": "RUN_STARTED",
                        "runId": rid,
                        "model": self.model,
                        "timestamp": ts,
                    }
                )
            )
            events.append(
                SSEEvent(
                    {
                        "type": "TEXT_MESSAGE_START",
                        "messageId": message_id,
                        "role": self.role,
                        "model": self.model,
                        "timestamp": ts,
                    }
                )
            )

        content: dict[str, Any] = {
            "type": "TEXT_MESSAGE_CONTENT",
            "messageId": message_id,
            "delta": self.delta,
            "timestamp": ts,
        }
        if self.model:
            content["model"] = self.model
        events.append(SSEEvent(content))

        if self.is_last:
            events.append(
                SSEEvent(
                    {
                        "type": "TEXT_MESSAGE_END",
                        "messageId": message_id,
                        "model": self.model,
                        "timestamp": ts,
                    }
                )
            )
            run_finished: dict[str, Any] = {
                "type": "RUN_FINISHED",
                "runId": rid,
                "model": self.model,
                "timestamp": ts,
                "finishReason": self.finish_reason or "stop",
            }
            if self.usage:
                run_finished["usage"] = self.usage.tanstack_ai_json()
            events.append(SSEEvent(run_finished))
            events.append(SSEDone())

        return events


class CancelState(StrEnum):
    """Result of a job cancellation request."""

    CANCELLATION_REQUESTED = "cancellation_requested"
    ALREADY_COMPLETED = "already_completed"
    NOT_FOUND = "not_found"


class CancelResult(BaseModel):
    """Result of a cancel request with the job's state at cancellation time."""

    state: CancelState
    job_state: JobState

    @property
    def is_cancelled(self) -> bool:
        return self.state == CancelState.CANCELLATION_REQUESTED

    @property
    def is_already_completed(self) -> bool:
        return self.state == CancelState.ALREADY_COMPLETED

    @property
    def is_not_found(self) -> bool:
        return self.state == CancelState.NOT_FOUND

    @property
    def job_failed(self) -> bool:
        return self.job_state == JobState.FAILED

    @property
    def job_queued(self) -> bool:
        return self.job_state == JobState.QUEUED

    @property
    def job_in_progress(self) -> bool:
        return self.job_state == JobState.IN_PROGRESS

    @property
    def job_completed(self) -> bool:
        return self.job_state == JobState.COMPLETED


class JobState(StrEnum):
    """State of an async AI job."""

    QUEUED = "queued"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    UNKNOWN = "unknown"


class JobStatus(BaseModel):
    """Status of an async AI job (e.g. fal image generation).

    Wraps fal's Queued/InProgress/Completed statuses into a single model.
    """

    state: JobState
    position: int | None = None
    logs: list[dict[str, Any]] | None = None
    metrics: dict[str, Any] | None = None
    error: str | None = None
    error_type: str | None = None

    @property
    def is_queued(self) -> bool:
        return self.state == JobState.QUEUED

    @property
    def is_in_progress(self) -> bool:
        return self.state == JobState.IN_PROGRESS

    @property
    def is_completed(self) -> bool:
        return self.state == JobState.COMPLETED

    @property
    def is_failed(self) -> bool:
        return self.state == JobState.FAILED

    @property
    def is_done(self) -> bool:
        return self.state in (JobState.COMPLETED, JobState.FAILED)
