"""Provider-agnostic AI response models with protocol adapters."""

from __future__ import annotations

import abc
import json
import re
import time
import uuid
from collections.abc import Sequence
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


def _snake_case(name: str) -> str:
    """Convert CamelCase to snake_case."""
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)
    return re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", "_", s).lower()


class Tool(BaseModel, abc.ABC):
    """Base class for defining AI tools.

    Subclass with typed fields for parameters, a docstring for the
    description, and implement :meth:`run`::

        class GetWeather(Tool):
            \"\"\"Get the current weather for a city.\"\"\"
            city: str
            unit: str = "celsius"

            async def run(self) -> str:
                return f"22° {self.unit}"

    Pass the *class* (not an instance) to ``chat()`` or ``run()``::

        response = await ai.chat(model="gpt-4o", messages=msgs, tools=[GetWeather])
    """

    @abc.abstractmethod
    async def run(self) -> Any:
        """Execute the tool. Override in subclasses."""

    @classmethod
    def function_name(cls) -> str:
        """The function name sent to the LLM (snake_cased class name)."""
        return _snake_case(cls.__name__)

    @classmethod
    def openai_schema(cls) -> dict[str, Any]:
        """Generate the OpenAI function-tool JSON schema."""
        schema = cls.model_json_schema()
        # Remove pydantic metadata keys that OpenAI doesn't expect
        schema.pop("title", None)
        fn: dict[str, Any] = {
            "name": cls.function_name(),
            "parameters": schema,
        }
        if cls.__doc__:
            fn["description"] = cls.__doc__.strip()
        return {"type": "function", "function": fn}


class ToolCall(BaseModel):
    """A parsed tool call returned by the LLM."""

    id: str
    function_name: str
    arguments: str
    args: Any = None  # Raw JSON arguments from the API.

    async def run(self) -> Any:
        """Execute the tool call. Only works when *args* is a Tool instance."""
        if not isinstance(self.args, Tool):
            raise TypeError(
                f"Cannot run tool call '{self.function_name}': "
                "args were not parsed into a Tool instance."
            )
        return await self.args.run()


def _build_tool_map(
    tools: Sequence[type[Tool]],
) -> tuple[list[dict[str, Any]], dict[str, type[Tool]]]:
    """Build OpenAI tool schemas and a name→class lookup from Tool subclasses."""
    schemas: list[dict[str, Any]] = []
    name_map: dict[str, type[Tool]] = {}
    for tool_cls in tools:
        schemas.append(tool_cls.openai_schema())
        name_map[tool_cls.function_name()] = tool_cls
    return schemas, name_map


def _parse_tool_call(
    tc: Any,
    name_map: dict[str, type[Tool]] | None,
) -> ToolCall:
    """Parse a single OpenAI tool call object into a ToolCall model."""
    fn_name = tc.function.name
    raw_args = tc.function.arguments
    parsed: Any = None
    if name_map and fn_name in name_map:
        parsed = name_map[fn_name].model_validate_json(raw_args)
    return ToolCall(
        id=tc.id,
        function_name=fn_name,
        arguments=raw_args,
        args=parsed,
    )


class ChatResponse(BaseModel):
    """Non-streaming chat completion response."""

    content: str
    role: str = "assistant"
    model: str
    usage: Usage | None = None
    finish_reason: str = "stop"
    tool_calls: list[ToolCall] = []

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
    tool_calls: list[ToolCall] = []
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
