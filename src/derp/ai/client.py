"""AI client wrapping AsyncOpenAI."""

from __future__ import annotations

from collections.abc import AsyncIterator
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
    Usage,
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
        self, *, model: str, messages: list[dict[str, Any]], **kwargs: Any
    ) -> ChatResponse:
        """Create a chat completion.

        Args:
            model: Model ID to use.
            messages: List of message dicts.
            **kwargs: Additional arguments forwarded to the API.

        Returns:
            ChatResponse with content, usage, and protocol adapters.
        """
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
        return ChatResponse(
            content=choice.message.content or "",
            role=choice.message.role,
            model=completion.model,
            usage=usage,
            finish_reason=choice.finish_reason or "stop",
        )

    async def stream_chat(
        self, *, model: str, messages: list[dict[str, Any]], **kwargs: Any
    ) -> AsyncIterator[ChatChunk]:
        """Create a streaming chat completion.

        Args:
            model: Model ID to use.
            messages: List of message dicts.
            **kwargs: Additional arguments forwarded to the API.

        Yields:
            ChatChunk for each text delta.
        """
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
                if choice.finish_reason:
                    finish_reason = choice.finish_reason
                    finish_model = getattr(chunk, "model", model)
            if chunk.usage:
                usage = Usage(
                    prompt_tokens=chunk.usage.prompt_tokens,
                    completion_tokens=chunk.usage.completion_tokens,
                    total_tokens=chunk.usage.total_tokens,
                )

        if finish_reason:
            yield ChatChunk(
                delta="",
                model=finish_model,
                finish_reason=finish_reason,
                usage=usage,
                is_last=True,
            )

    async def fal_call(
        self, *, application: str, inputs: dict[str, Any], start_timeout: float = 10.0
    ) -> str:
        """Call a Fal application.

        Args:
            application: Fal application name.
            inputs: Inputs to the model.
            start_timeout: Start timeout in seconds. Default is 10 seconds.

        Returns:
            Request ID of the submitted task.
        """
        if self._fal_client is None:
            raise FalMissingCredentialsError()
        result = await self._fal_client.submit(
            application,
            arguments=inputs,
            start_timeout=start_timeout,
        )
        return result.request_id

    async def fal_poll(self, application: str, request_id: str) -> JobStatus:
        """Poll the status of a fal job.

        Args:
            application: Fal application name.
            request_id: Request ID returned by fal_call.

        Returns:
            JobStatus with the current state of the job.
        """
        if self._fal_client is None:
            raise FalMissingCredentialsError()
        handle = self._fal_client.get_handle(application, request_id)
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

    async def fal_get(self, application: str, request_id: str) -> dict[str, Any]:
        """Get the result of a fal job.

        Args:
            application: Fal application name.
            request_id: Request ID returned by fal_call.

        Returns:
            Result of the job as a dict.
        """
        if self._fal_client is None:
            raise FalMissingCredentialsError()
        handle = self._fal_client.get_handle(application, request_id)
        result = await handle.get()
        return result

    async def fal_cancel(self, application: str, request_id: str) -> CancelResult:
        """Cancel a fal job.

        Args:
            application: Fal application name.
            request_id: Request ID returned by fal_call.

        Returns:
            CancelResult with the cancellation state and job state.

        Raises:
            FalJobAlreadyCompletedError: If the job already completed.
            FalJobNotFoundError: If the job was not found.
        """
        if self._fal_client is None:
            raise FalMissingCredentialsError()

        handle = self._fal_client.get_handle(application, request_id)
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
            raise
        return CancelResult(
            state=CancelState.CANCELLATION_REQUESTED,
            job_state=job_state,
        )

    async def modal_call(
        self, *, endpoint: str, inputs: dict[str, Any], timeout: float = 30.0
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
