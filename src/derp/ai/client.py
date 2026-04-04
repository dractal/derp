"""AI client wrapping AsyncOpenAI."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any

import httpx
from etils import epy

from derp.ai.exceptions import (
    FalJobAlreadyCompletedError,
    FalJobNotFoundError,
    FalMissingCredentialsError,
    ModalNotConnectedError,
)
from derp.ai.models import (
    CancelResult,
    CancelState,
    ChatChunk,
    ChatResponse,
    JobState,
    JobStatus,
    Tool,
    ToolCall,
    Usage,
    _build_tool_map,
    _parse_tool_call,
)
from derp.config import AIConfig

with epy.lazy_imports():
    import fal_client
    import openai


class AIClient:
    """Async AI client wrapping several providers.

    Example::

        config = AIConfig(api_key="...")
        ai = AIClient(config)
        response = await ai.chat(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "Hello"}],
        )
    """

    def __init__(self, config: AIConfig):
        self._config = config
        self._openai_client: openai.AsyncOpenAI = openai.AsyncOpenAI(
            api_key=config.api_key,
            base_url=config.base_url,
        )
        self._fal_client: fal_client.AsyncClient | None = (
            fal_client.AsyncClient(
                key=config.fal_api_key,
            )
            if config.fal_api_key is not None
            else None
        )
        self._modal_client: httpx.AsyncClient | None = None

    async def connect(self) -> None:
        if self._config.modal_config is not None:
            self._modal_client = httpx.AsyncClient(
                headers={
                    "Modal-Key": self._config.modal_config.token_id,
                    "Modal-Secret": self._config.modal_config.token_secret,
                },
                base_url=self._config.modal_config.endpoint_url or "",
            )

    async def disconnect(self) -> None:
        if self._modal_client is not None:
            await self._modal_client.aclose()
            self._modal_client = None

    async def chat(
        self,
        model: str,
        *,
        messages: list[dict[str, Any]],
        tools: Sequence[type[Tool]] = (),
        **kwargs: Any,
    ) -> ChatResponse:
        """Create a chat completion.

        Args:
            model: Model ID to use.
            messages: List of message dicts.
            tools: Optional list of Tool subclasses.
            **kwargs: Additional arguments forwarded to the API.

        Returns:
            ChatResponse with content, usage, and protocol adapters.
        """
        name_map: dict[str, type[Tool]] | None = None
        if tools:
            schemas, name_map = _build_tool_map(tools)
            kwargs["tools"] = schemas
        completion = await self._openai_client.chat.completions.create(
            model=model,
            messages=messages,
            **kwargs,
        )
        choice = completion.choices[0]
        usage = None
        if completion.usage:
            usage = Usage(
                prompt_tokens=completion.usage.prompt_tokens,
                completion_tokens=completion.usage.completion_tokens,
                total_tokens=completion.usage.total_tokens,
            )
        parsed_tool_calls: list[ToolCall] = []
        if choice.message.tool_calls:
            parsed_tool_calls = [
                _parse_tool_call(tc, name_map) for tc in choice.message.tool_calls
            ]
        return ChatResponse(
            content=choice.message.content or "",
            role=choice.message.role,
            model=completion.model,
            usage=usage,
            finish_reason=choice.finish_reason or "stop",
            tool_calls=parsed_tool_calls,
        )

    async def stream_chat(
        self,
        model: str,
        *,
        messages: list[dict[str, Any]],
        tools: Sequence[type[Tool]] = (),
        **kwargs: Any,
    ) -> AsyncIterator[ChatChunk]:
        """Create a streaming chat completion.

        Args:
            model: Model ID to use.
            messages: List of message dicts.
            tools: Optional list of Tool subclasses.
            **kwargs: Additional arguments forwarded to the API.

        Yields:
            ChatChunk for each text delta. The final chunk includes
            parsed tool_calls when the model invokes tools.
        """
        name_map: dict[str, type[Tool]] | None = None
        if tools:
            schemas, name_map = _build_tool_map(tools)
            kwargs["tools"] = schemas
        kwargs.setdefault("stream_options", {"include_usage": True})
        stream = await self._openai_client.chat.completions.create(
            model=model,
            messages=messages,
            stream=True,
            **kwargs,
        )
        first = True
        finish_reason: str | None = None
        finish_model: str = model
        usage: Usage | None = None

        # Accumulate streamed tool call fragments: index -> [id, name, args]
        tc_acc: dict[int, list[Any]] = {}

        async for chunk in stream:
            if chunk.choices:
                choice = chunk.choices[0]
                if choice.delta.content:
                    yield ChatChunk(
                        delta=choice.delta.content,
                        role=choice.delta.role or "assistant",
                        model=getattr(chunk, "model", model),
                        is_first=first,
                    )
                    first = False
                # Accumulate tool call deltas
                if choice.delta.tool_calls:
                    for tc_delta in choice.delta.tool_calls:
                        idx = tc_delta.index
                        if idx not in tc_acc:
                            tc_acc[idx] = [
                                tc_delta.id or "",
                                (
                                    tc_delta.function.name or ""
                                    if tc_delta.function
                                    else ""
                                ),
                                "",
                            ]
                        else:
                            if tc_delta.id:
                                tc_acc[idx][0] = tc_delta.id
                            if tc_delta.function and tc_delta.function.name:
                                tc_acc[idx][1] = tc_delta.function.name
                        if tc_delta.function and tc_delta.function.arguments:
                            tc_acc[idx][2] += tc_delta.function.arguments
                if choice.finish_reason:
                    finish_reason = choice.finish_reason
                    finish_model = getattr(chunk, "model", model)
            if chunk.usage:
                usage = Usage(
                    prompt_tokens=chunk.usage.prompt_tokens,
                    completion_tokens=chunk.usage.completion_tokens,
                    total_tokens=chunk.usage.total_tokens,
                )

        # Build parsed tool calls from accumulated fragments
        parsed_tool_calls: list[ToolCall] = []
        for idx in sorted(tc_acc):
            tc_id, fn_name, raw_args = tc_acc[idx]
            parsed: Any = None
            if name_map and fn_name in name_map:
                parsed = name_map[fn_name].model_validate_json(raw_args)
            parsed_tool_calls.append(
                ToolCall(
                    id=tc_id,
                    function_name=fn_name,
                    arguments=raw_args,
                    args=parsed,
                )
            )

        if finish_reason:
            yield ChatChunk(
                delta="",
                model=finish_model,
                finish_reason=finish_reason,
                usage=usage,
                tool_calls=parsed_tool_calls,
                is_last=True,
            )

    async def stream_agent(
        self,
        model: str,
        *,
        messages: list[dict[str, Any]],
        tools: Sequence[type[Tool]] = (),
        max_turns: int = 10,
        **kwargs: Any,
    ) -> AsyncIterator[ChatChunk]:
        """Stream a chat completion loop, auto-executing tool calls.

        Streams via :meth:`stream_chat` in a loop. Text deltas are yielded
        as they arrive. When the model returns tool calls, each tool is
        executed via its :meth:`~Tool.run` method, results are appended
        as tool messages, and the next round starts automatically.

        The loop continues until the model returns a text response
        (no tool calls) or *max_turns* is reached.

        Args:
            model: Model ID to use.
            messages: List of message dicts (mutated in place).
            tools: Tool subclasses available to the model.
            max_turns: Maximum number of tool-call round-trips.
            **kwargs: Additional arguments forwarded to the API.

        Yields:
            ChatChunk for each text delta across all turns.
        """
        import json as _json

        for _ in range(max_turns):
            last_chunk: ChatChunk | None = None
            async for chunk in self.stream_chat(
                model=model, messages=messages, tools=tools, **kwargs
            ):
                last_chunk = chunk
                yield chunk

            if last_chunk is None or not last_chunk.tool_calls:
                return

            # Append the assistant message with tool calls
            messages.append(
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": tc.id,
                            "type": "function",
                            "function": {
                                "name": tc.function_name,
                                "arguments": tc.arguments,
                            },
                        }
                        for tc in last_chunk.tool_calls
                    ],
                }
            )

            # Execute each tool and append results
            for tc in last_chunk.tool_calls:
                result = await tc.run()
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": (
                            result if isinstance(result, str) else _json.dumps(result)
                        ),
                    }
                )

    async def fal_call(
        self, app: str, *, inputs: dict[str, Any], start_timeout: float = 10.0
    ) -> str:
        """Call a Fal application.

        Args:
            app: Fal application name.
            inputs: Inputs to the model.
            start_timeout: Start timeout in seconds. Default is 10 seconds.

        Returns:
            Request ID of the submitted task.
        """
        if self._fal_client is None:
            raise FalMissingCredentialsError()
        result = await self._fal_client.submit(
            app,
            arguments=inputs,
            start_timeout=start_timeout,
        )
        return result.request_id

    async def fal_poll(self, app: str, request_id: str) -> JobStatus:
        """Poll the status of a fal job.

        Args:
            app: Fal application name.
            request_id: Request ID returned by fal_call.

        Returns:
            JobStatus with the current state of the job.
        """
        if self._fal_client is None:
            raise FalMissingCredentialsError()
        handle = self._fal_client.get_handle(app, request_id)
        status = await handle.status()

        if isinstance(status, fal_client.Queued):
            return JobStatus(state=JobState.QUEUED, position=status.position)
        elif isinstance(status, fal_client.InProgress):
            return JobStatus(state=JobState.IN_PROGRESS, logs=status.logs)
        elif isinstance(status, fal_client.Completed):
            if status.error:
                state = JobState.FAILED
            else:
                state = JobState.COMPLETED
            return JobStatus(
                state=state,
                logs=status.logs,
                metrics=status.metrics,
                error=status.error,
                error_type=status.error_type,
            )
        return JobStatus(state=JobState.UNKNOWN)

    async def fal_get(self, app: str, request_id: str) -> dict[str, Any]:
        """Get the result of a fal job.

        Args:
            app: Fal application name.
            request_id: Request ID returned by fal_call.

        Returns:
            Result of the job as a dict.
        """
        if self._fal_client is None:
            raise FalMissingCredentialsError()
        handle = self._fal_client.get_handle(app, request_id)
        result = await handle.get()
        return result

    async def fal_cancel(self, app: str, request_id: str) -> CancelResult:
        """Cancel a fal job.

        Args:
            app: Fal application name.
            request_id: Request ID returned by fal_call.

        Returns:
            CancelResult with the cancellation state and job state.

        Raises:
            FalJobAlreadyCompletedError: If the job already completed.
            FalJobNotFoundError: If the job was not found.
        """
        if self._fal_client is None:
            raise FalMissingCredentialsError()

        handle = self._fal_client.get_handle(app, request_id)
        status = await handle.status()

        if isinstance(status, fal_client.Queued):
            job_state = JobState.QUEUED
        elif isinstance(status, fal_client.InProgress):
            job_state = JobState.IN_PROGRESS
        elif isinstance(status, fal_client.Completed):
            if status.error:
                job_state = JobState.FAILED
            else:
                job_state = JobState.COMPLETED
        else:
            job_state = JobState.UNKNOWN

        try:
            await handle.cancel()
        except fal_client.FalClientHTTPError as exc:
            if exc.status_code == 400:
                raise FalJobAlreadyCompletedError() from exc
            if exc.status_code == 404:
                raise FalJobNotFoundError() from exc
            raise exc
        return CancelResult(
            state=CancelState.CANCELLATION_REQUESTED,
            job_state=job_state,
        )

    async def modal_call(
        self, endpoint: str, *, inputs: dict[str, Any], timeout: float = 30.0
    ) -> dict[str, Any]:
        """Call a Modal endpoint.

        Args:
            endpoint: Modal endpoint name.
            inputs: Inputs to the endpoint.
            timeout: Timeout in seconds. Default is 30 seconds.

        Returns:
            Result of the endpoint as a dict.
        """
        if self._modal_client is None:
            raise ModalNotConnectedError()
        response = await self._modal_client.post(
            endpoint,
            json=inputs,
            timeout=timeout,
        )
        return response.json()
